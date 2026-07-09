package fileextract

// metrics_p1_test.go — P1 增量埋点最小断言覆盖（io_op / dlq_write_errors / tombstone /
// truncated / content_bytes）。只验证埋点被触发，阈值/分桶精度不管。

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// counterVal 从私有 registry 取某 counter series（可带 label）的当前值，取不到返回 -1。
func counterVal(t *testing.T, c *counters, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if m.Counter != nil && labelsMatch(m.Label, labels) {
				return m.Counter.GetValue()
			}
		}
	}
	return -1
}

// histSampleCount 从私有 registry 取某 histogram series 的样本数，取不到返回 -1。
func histSampleCount(t *testing.T, c *counters, name string, labels map[string]string) int64 {
	t.Helper()
	families, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if m.Histogram != nil && labelsMatch(m.Label, labels) {
				return int64(m.Histogram.GetSampleCount())
			}
		}
	}
	return -1
}

func labelsMatch(lps []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(lps))
	for _, lp := range lps {
		got[lp.GetName()] = lp.GetValue()
	}
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// TestMetrics_DLQWriteErrors DLQ 写重试耗尽走 escape（无论 spill 还是 hard stop）→
// dlq_write_errors_total inc。P0 告警依赖项。
func TestMetrics_DLQWriteErrors(t *testing.T) {
	m := newCounters()

	// hard stop 路径（未配 SpillDir）
	h := newTestDLQHandler(&fakeDLQSink{alwaysErr: true}, &recordAlerter{}, "")
	h.metrics = m
	if err := h.Send(context.Background(), sampleDLQRec()); err == nil {
		t.Fatal("expected errDLQHardStop on exhausted + no spill")
	}
	if got := counterVal(t, m, "fileextract_dlq_write_errors_total", nil); got != 1 {
		t.Fatalf("dlq_write_errors_total after hard stop: got %v want 1", got)
	}

	// spill 路径（配 SpillDir）也应 inc
	h2 := newTestDLQHandler(&fakeDLQSink{alwaysErr: true}, &recordAlerter{}, t.TempDir())
	h2.metrics = m
	if err := h2.Send(context.Background(), sampleDLQRec()); err != nil {
		t.Fatalf("escape to spill should return nil, got %v", err)
	}
	if got := counterVal(t, m, "fileextract_dlq_write_errors_total", nil); got != 2 {
		t.Fatalf("dlq_write_errors_total after spill: got %v want 2", got)
	}
}

// TestMetrics_DLQWriteErrors_NotIncOnSuccess DLQ 写首次成功不 inc dlq_write_errors_total。
func TestMetrics_DLQWriteErrors_NotIncOnSuccess(t *testing.T) {
	m := newCounters()
	h := newTestDLQHandler(&fakeDLQSink{}, &recordAlerter{}, "")
	h.metrics = m
	if err := h.Send(context.Background(), sampleDLQRec()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := counterVal(t, m, "fileextract_dlq_write_errors_total", nil); got != -1 && got != 0 {
		t.Fatalf("dlq_write_errors_total on success: got %v want 0", got)
	}
}

// TestMetrics_ExtractorHappyPath_IOAndContent 抽取全流程走通 → 三处 io_op_duration 各 observe
// 一次 + content_bytes observe 一次；io_op_errors 保持 0。
func TestMetrics_ExtractorHappyPath_IOAndContent(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pdf bytes")) //nolint:errcheck // test handler write
	}))
	defer cdn.Close()
	tika := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("extracted content 抽出内容")) //nolint:errcheck // test handler write
	}))
	defer tika.Close()
	os := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) //nolint:errcheck
		_, _ = w.Write([]byte(`{"_id":"42","result":"updated"}`)) //nolint:errcheck // test handler write
	}))
	defer os.Close()

	cfg := ServiceConfig{
		ESAddresses:            []string{os.URL},
		ESIndex:                "octo-message",
		TikaURL:                tika.URL,
		DownloadTimeout:        time.Second,
		ExtractTimeout:         time.Second,
		MaxFileSize:            1024 * 1024,
		MaxContentBytes:        1024 * 1024,
		HTTPRetries:            2,
		AllowedDownloadHosts:   []string{"127.0.0.1"},
		AllowedDownloadSchemes: []string{"http", "https"},
		SSRFAllowLoopback:      true,
	}
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor: %v", err)
	}
	m := newCounters()
	e.metrics = m

	fp := &filePayload{URL: cdn.URL + "/x.pdf", Name: "x.pdf", Extension: ".pdf", Size: 100}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if err != nil || reason != "" {
		t.Fatalf("ExtractAndWrite: reason=%q cause=%v err=%v", reason, cause, err)
	}

	for _, op := range []string{"download", "tika_extract", "os_update"} {
		if got := histSampleCount(t, m, "fileextract_io_op_duration_seconds", map[string]string{"op": op}); got != 1 {
			t.Errorf("io_op_duration_seconds{op=%q} sample count: got %d want 1", op, got)
		}
	}
	if got := histSampleCount(t, m, "fileextract_extract_content_bytes", nil); got != 1 {
		t.Errorf("extract_content_bytes sample count: got %d want 1", got)
	}
	// happy path 无 IO 错误
	for _, op := range []string{"download", "tika_extract", "os_update"} {
		if got := counterVal(t, m, "fileextract_io_op_errors_total", map[string]string{"op": op}); got != -1 && got != 0 {
			t.Errorf("io_op_errors_total{op=%q}: got %v want 0", op, got)
		}
	}
}

// TestMetrics_TombstoneOnPermanentFail permanent-fail (oversize) → WriteTombstone 成功后
// tombstone_total{reason} inc。
func TestMetrics_TombstoneOnPermanentFail(t *testing.T) {
	osSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) //nolint:errcheck
		_, _ = w.Write([]byte(`{"_id":"42","result":"updated"}`)) //nolint:errcheck // test handler write
	}))
	defer osSrv.Close()

	cfg := ServiceConfig{ESAddresses: []string{osSrv.URL}, ESIndex: "octo-message", MaxFileSize: 1024}
	osw, err := newOSWriter(cfg)
	if err != nil {
		t.Fatalf("newOSWriter: %v", err)
	}
	m := newCounters()
	e := &Extractor{os: osw, maxFileSize: 1024, extractorLabel: "tika/test", metrics: m}

	fp := &filePayload{URL: "http://x/y.pdf", Name: "y.pdf", Extension: ".pdf", Size: 2048}
	reason, _, err := e.ExtractAndWrite(context.Background(), "42", fp) //nolint:errcheck // cause 供 DLQ 记录用，测试只断言 reason/err
	if reason != ReasonOversize || err != nil {
		t.Fatalf("expected oversize DLQ, got reason=%q err=%v", reason, err)
	}
	if got := counterVal(t, m, "fileextract_tombstone_total", map[string]string{"reason": ReasonOversize}); got != 1 {
		t.Fatalf("tombstone_total{reason=oversize}: got %v want 1", got)
	}
}

// TestMetrics_NilSafe Extractor / dlqHandler 的 metrics 为 nil 时埋点跳过不 panic。
func TestMetrics_NilSafe(t *testing.T) {
	var c *counters
	c.ObserveIO("download", time.Millisecond)
	c.IncIOError("download")
	c.IncDLQWriteError()
	c.IncTombstone("oversize")
	c.IncTruncated()
	c.ObserveContentBytes(100)
}

// newTestExtractor 装配一个走真实 download/tika/os HTTP 链路的 Extractor：OS 返回 osStatus
// 状态码，tikaBody 为 Tika 抽出正文，maxContentBytes 控制截断阈值。返回 Extractor / counters
// 及 cdn 基址（供构造 filePayload.URL）。供 io_op_errors / truncated 失败路径断言复用。
func newTestExtractor(t *testing.T, osStatus int, tikaBody string, maxContentBytes int) (*Extractor, *counters, string) {
	t.Helper()
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pdf bytes")) //nolint:errcheck // test handler write
	}))
	t.Cleanup(cdn.Close)
	tika := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(tikaBody)) //nolint:errcheck // test handler write
	}))
	t.Cleanup(tika.Close)
	osSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) //nolint:errcheck
		if osStatus != http.StatusOK {
			w.WriteHeader(osStatus)
		}
		_, _ = w.Write([]byte(`{"_id":"42","result":"updated"}`)) //nolint:errcheck // test handler write
	}))
	t.Cleanup(osSrv.Close)

	cfg := ServiceConfig{
		ESAddresses:            []string{osSrv.URL},
		ESIndex:                "octo-message",
		TikaURL:                tika.URL,
		DownloadTimeout:        time.Second,
		ExtractTimeout:         time.Second,
		MaxFileSize:            1024 * 1024,
		MaxContentBytes:        maxContentBytes,
		HTTPRetries:            0,
		AllowedDownloadHosts:   []string{"127.0.0.1"},
		AllowedDownloadSchemes: []string{"http", "https"},
		SSRFAllowLoopback:      true,
	}
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor: %v", err)
	}
	m := newCounters()
	e.metrics = m
	return e, m, cdn.URL
}

// TestMetrics_IOError_OSUpdate os_update 返回非 errDocNotYet 的真错误（OS 4xx permanent）→
// io_op_errors_total{op="os_update"} 加 1；errDocNotYet（OS 404 时序竞态）→【不】加 1。
// 后者正好覆盖第 1 条修复。
func TestMetrics_IOError_OSUpdate(t *testing.T) {
	// 真错误：OS 400 → errOSPermanent（非 errDocNotYet）→ io_op_errors_total{os_update} 加 1
	e, m, cdnURL := newTestExtractor(t, http.StatusBadRequest, "extracted content 抽出内容", 1024*1024)
	fp := &filePayload{URL: cdnURL + "/x.pdf", Name: "x.pdf", Extension: ".pdf", Size: 100}
	if _, _, err := e.ExtractAndWrite(context.Background(), "42", fp); err == nil { //nolint:errcheck // cause 供 DLQ 记录用，测试只断言 err
		t.Fatal("expected OS permanent error, got nil")
	}
	if got := counterVal(t, m, "fileextract_io_op_errors_total", map[string]string{"op": "os_update"}); got != 1 {
		t.Fatalf("io_op_errors_total{op=os_update} on real error: got %v want 1", got)
	}

	// errDocNotYet：OS 404 时序竞态 → io_op_errors_total{os_update}【不】加 1（第 1 条修复）
	e2, m2, cdnURL2 := newTestExtractor(t, http.StatusNotFound, "extracted content 抽出内容", 1024*1024)
	fp2 := &filePayload{URL: cdnURL2 + "/x.pdf", Name: "x.pdf", Extension: ".pdf", Size: 100}
	if _, _, err := e2.ExtractAndWrite(context.Background(), "42", fp2); err == nil { //nolint:errcheck // cause 供 DLQ 记录用，测试只断言 err
		t.Fatal("expected errDocNotYet, got nil")
	}
	if got := counterVal(t, m2, "fileextract_io_op_errors_total", map[string]string{"op": "os_update"}); got != -1 && got != 0 {
		t.Fatalf("io_op_errors_total{op=os_update} on errDocNotYet: got %v want 0 (第1条修复)", got)
	}
}

// TestMetrics_Truncated Tika 抽出正文超过 MaxContentBytes → truncated=true → truncated_total 加 1。
func TestMetrics_Truncated(t *testing.T) {
	// MaxContentBytes=5，Tika 返回更长正文 → truncateContent 置 truncated=true；OS 200 成功回写
	e, m, cdnURL := newTestExtractor(t, http.StatusOK, "extracted content much longer than limit", 5)
	fp := &filePayload{URL: cdnURL + "/x.pdf", Name: "x.pdf", Extension: ".pdf", Size: 100}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if err != nil || reason != "" {
		t.Fatalf("ExtractAndWrite: reason=%q cause=%v err=%v", reason, cause, err)
	}
	if got := counterVal(t, m, "fileextract_truncated_total", nil); got != 1 {
		t.Fatalf("truncated_total: got %v want 1", got)
	}
}

