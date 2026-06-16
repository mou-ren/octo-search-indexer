// 结构化对账报告（YUJ-4682 步骤 5）：把 count 对账 + 抽样比对汇总成一个机检报告，
// 并提供回填 octo-server `search_recon_*` 只读 gauge 的载荷（PushPayload，逐字段对齐
// octo-server modules/messages_search/recon_metrics.go::ReconReport）。
//
// 失败阈值（机检口径，钉死在此 + Healthy()）：
//   - doc_drift（= ES doc-count − MySQL 行数）!= 0       → 不健康（>0 该删没删/越权可搜；<0 漏索引/搜不到）。
//   - sample_mismatch（抽样字段失配条数）!= 0            → 不健康（条数对得上但内容错位）。
//   - sample_missing（抽样 MySQL 有 ES 无）!= 0          → 不健康（少 doc 的字段级佐证）。
//   - exit code：Healthy → 0；不健康 → 2（CI / cron gate 失败信号）。
package recon

import "fmt"

// FullReport 汇总 count 对账 + 抽样比对。
type FullReport struct {
	Window struct {
		FromUnix int64 `json:"from_unix"`
		ToUnix   int64 `json:"to_unix"`
	} `json:"window"`
	Count  Report       `json:"count"`
	Sample SampleResult `json:"sample"`
	// RanAtUnixSeconds 是本次对账完成时刻（回填 search_recon_last_run_timestamp_seconds）。
	RanAtUnixSeconds int64 `json:"ran_at_unix_seconds"`
}

// Healthy 报告是否对平（count + 抽样均无 drift）。
func (r FullReport) Healthy() bool {
	return r.Count.OK && r.Sample.Mismatch == 0 && r.Sample.Missing == 0
}

// PushPayload 是回填 octo-server 只读 drift gauge 的载荷，**逐字段对齐**
// octo-server modules/messages_search/recon_metrics.go::ReconReport（半兼容会让 gauge 读错）。
type PushPayload struct {
	ESDocCount       int64 `json:"es_doc_count"`
	MySQLRowCount    int64 `json:"mysql_row_count"`
	SampleMismatch   int64 `json:"sample_mismatch"`
	RanAtUnixSeconds int64 `json:"ran_at_unix_seconds"`
}

// PushPayload 构造回填 octo-server 的载荷。SampleMismatch 计入「字段失配 + 缺 doc」两类
// （octo-server gauge 语义 = 抽样中与源失配的 doc 数）。
func (r FullReport) PushPayload() PushPayload {
	return PushPayload{
		ESDocCount:       r.Count.ESDocs,
		MySQLRowCount:    r.Count.SourceRows,
		SampleMismatch:   int64(r.Sample.Mismatch + r.Sample.Missing),
		RanAtUnixSeconds: r.RanAtUnixSeconds,
	}
}

// String 渲染人类可读摘要（cron / CI 日志用）。
func (r FullReport) String() string {
	status := "MISMATCH"
	if r.Healthy() {
		status = "OK"
	}
	return fmt.Sprintf(
		"recon %s | window=[%d,%d] %s | sample(sampled=%d mismatch=%d missing=%d)",
		status, r.Window.FromUnix, r.Window.ToUnix, r.Count.String(),
		r.Sample.Sampled, r.Sample.Mismatch, r.Sample.Missing,
	)
}
