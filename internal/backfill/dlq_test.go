package backfill

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestDLQSpill_WriteAndCount 写若干条 → 计数精确等于落盘行数。
func TestDLQSpill_WriteAndCount(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	for i := 0; i < 3; i++ {
		if err := s.Write(dlqRecord{ID: int64(i), MessageID: fmt.Sprintf("m%d", i)}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if s.Count() != 3 {
		t.Fatalf("count=%d want 3", s.Count())
	}
	data := mustRead(t, s.Path())
	if got := countLines(string(data)); got != 3 {
		t.Fatalf("file lines=%d want 3 (count must equal durable rows)", got)
	}
}

// TestDLQSpill_EmptyDirRejected dir 为空 → 拒绝构造（真异常不得静默消失）。
func TestDLQSpill_EmptyDirRejected(t *testing.T) {
	if _, err := OpenDLQSpill(""); err == nil {
		t.Fatalf("empty spill dir must be rejected (fail-closed)")
	}
}

// TestDLQSpill_AppendAcrossOpens 重开同目录 → append，不截断旧记录。
func TestDLQSpill_AppendAcrossOpens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s1, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s1.Write(dlqRecord{ID: 1, MessageID: "a"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if cerr := s1.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s2)
	if err := s2.Write(dlqRecord{ID: 2, MessageID: "b"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	data := mustRead(t, s2.Path())
	if got := countLines(string(data)); got != 2 {
		t.Fatalf("append across opens must keep both records, got %d lines", got)
	}
}

func countLines(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			n++
		}
	}
	return n
}

// TestDLQSpill_IdempotentByMessageID 同一 message_id 重复 Write → no-op，不重复 append/计数
// （codex P2-idempotency：批崩在 DLQ 写后、checkpoint 推进前，resume 重读同行不得膨胀 DLQ）。
func TestDLQSpill_IdempotentByMessageID(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	for i := 0; i < 5; i++ {
		if err := s.Write(dlqRecord{ID: 7, MessageID: "same", CreatedAt: 100}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if s.Count() != 1 {
		t.Fatalf("duplicate message_id must dedupe to 1, got %d", s.Count())
	}
	data := mustRead(t, s.Path())
	if got := countLines(string(data)); got != 1 {
		t.Fatalf("file must hold exactly 1 record, got %d lines", got)
	}
}

// TestDLQSpill_ReplayCountOnReopen 重开既有 spill → Count 从文件重建，不归零
// （codex P1：resume 后 Count 归零会让 inline reconcile 把已 DLQ 行当 ES 缺失 → false mismatch）。
func TestDLQSpill_ReplayCountOnReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s1, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s1.Write(dlqRecord{ID: int64(i), MessageID: fmt.Sprintf("m%d", i), CreatedAt: 100}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if cerr := s1.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mustCloseT(t, s2)
	if s2.Count() != 3 {
		t.Fatalf("reopen must rebuild count from spill file, got %d want 3", s2.Count())
	}
	// 重开后再写一条已存在的 → 仍幂等。
	if err := s2.Write(dlqRecord{ID: 1, MessageID: "m1", CreatedAt: 100}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s2.Count() != 3 {
		t.Fatalf("re-writing an existing key after reopen must stay 3, got %d", s2.Count())
	}
}

// TestDLQSpill_CountInWindow 仅数 created_at ∈ 窗的记录
// （codex P2-window：窗不覆盖整个 run 时，用全量 DLQ 会减掉窗外行 → false mismatch/OK）。
func TestDLQSpill_CountInWindow(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	writes := []dlqRecord{
		{ID: 1, MessageID: "before", CreatedAt: 50},
		{ID: 2, MessageID: "in1", CreatedAt: 150},
		{ID: 3, MessageID: "in2", CreatedAt: 200},
		{ID: 4, MessageID: "after", CreatedAt: 500},
	}
	for _, w := range writes {
		if err := s.Write(w); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if s.Count() != 4 {
		t.Fatalf("total count=%d want 4", s.Count())
	}
	if got := s.CountInWindow(100, 300); got != 2 {
		t.Fatalf("window [100,300] must count 2 (in1,in2), got %d", got)
	}
	// 边界含端点（gte/lte，与 recon range filter 一致）。
	if got := s.CountInWindow(50, 50); got != 1 {
		t.Fatalf("inclusive boundary must count 'before', got %d", got)
	}
}

// TestDLQSpill_Sync Sync 刷盘后记录可被另一个只读 reopen 看到（落盘可见性）。
func TestDLQSpill_Sync(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	if err := s.Write(dlqRecord{ID: 1, MessageID: "x", CreatedAt: 100}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// 另一个 reopen（replay）应已能看到这条记录（已落盘）。
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mustCloseT(t, s2)
	if s2.Count() != 1 {
		t.Fatalf("synced record must be visible to reopen, got %d", s2.Count())
	}
}
