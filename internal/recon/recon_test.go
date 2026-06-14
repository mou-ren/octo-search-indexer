package recon

import "testing"

// TestReconcile_AllIndexed 全部源行都进了 ES（含 raw_excluded doc），无 DLQ → 对平。
func TestReconcile_AllIndexed(t *testing.T) {
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 1000, RawExcluded: 50, DLQ: 0})
	if !r.OK || r.Diff != 0 || r.Expected != 1000 {
		t.Fatalf("expected OK diff=0 expected=1000, got %+v", r)
	}
}

// TestReconcile_DLQExcludedFromExpected DLQ 条目不写 ES → 期望 doc 数扣除 DLQ。
func TestReconcile_DLQExcludedFromExpected(t *testing.T) {
	// 1000 源行，3 条进 DLQ（未写 ES）→ 期望 997 doc。ES 实际 997 → 对平。
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 997, DLQ: 3})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if r.Expected != 997 {
		t.Fatalf("expected 997, got %d", r.Expected)
	}
}

// TestReconcile_RawExcludedStillCounts raw_excluded 仍是一条 ES doc → 不从期望扣除。
func TestReconcile_RawExcludedStillCounts(t *testing.T) {
	// 全部 1000 行都写入 ES（其中 200 条 raw_excluded，content=null 但占 doc）→ ES 1000 → 对平。
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 1000, RawExcluded: 200, DLQ: 0})
	if !r.OK {
		t.Fatalf("raw_excluded must NOT be subtracted from expected; got %+v", r)
	}
}

// TestReconcile_Shortfall ES 比期望少（漏灌/丢失）→ Diff<0，不对平（STOP 信号）。
func TestReconcile_Shortfall(t *testing.T) {
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 990, DLQ: 0})
	if r.OK || r.Diff != -10 {
		t.Fatalf("expected mismatch diff=-10, got %+v", r)
	}
}

// TestReconcile_Surplus ES 比期望多（越界/串窗）→ Diff>0，不对平。
func TestReconcile_Surplus(t *testing.T) {
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 1005, DLQ: 0})
	if r.OK || r.Diff != 5 {
		t.Fatalf("expected mismatch diff=+5, got %+v", r)
	}
}

// TestReconcile_DLQAndShortfall 综合：DLQ 扣除后仍缺口。
func TestReconcile_DLQAndShortfall(t *testing.T) {
	// 源 500，DLQ 10 → 期望 490；ES 只有 485 → 缺 5。
	r := Reconcile(Counts{SourceRows: 500, ESDocs: 485, DLQ: 10})
	if r.OK || r.Diff != -5 || r.Expected != 490 {
		t.Fatalf("expected expected=490 diff=-5, got %+v", r)
	}
}

func TestReport_StringRendersStatus(t *testing.T) {
	ok := Reconcile(Counts{SourceRows: 10, ESDocs: 10}).String()
	if !contains(ok, "OK") {
		t.Fatalf("OK report should say OK: %s", ok)
	}
	bad := Reconcile(Counts{SourceRows: 10, ESDocs: 9}).String()
	if !contains(bad, "MISMATCH") {
		t.Fatalf("mismatch report should say MISMATCH: %s", bad)
	}
}

func TestSafeTableName(t *testing.T) {
	for _, ok := range []string{"message", "message1", "msg_4"} {
		if !safeTableName(ok) {
			t.Fatalf("%q should be safe", ok)
		}
	}
	for _, bad := range []string{"", "msg;drop", "msg table", "msg`x", "msg-1"} {
		if safeTableName(bad) {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// ── DLQ accounting 加固（阶段 6 (f)）─────────────────────────────────────────────

// TestReconcileChecked_OK 自洽输入 → 校验通过并对平。
func TestReconcileChecked_OK(t *testing.T) {
	r, err := ReconcileChecked(Counts{SourceRows: 1000, ESDocs: 997, RawExcluded: 50, DLQ: 3})
	if err != nil {
		t.Fatalf("self-consistent input must pass: %v", err)
	}
	if !r.OK || r.Expected != 997 {
		t.Fatalf("want OK expected=997, got %+v", r)
	}
}

// TestReconcileChecked_DLQExceedsSource DLQ>源行数 → 拒绝（虚高 DLQ 会缩小 Expected 掩盖漏灌）。
func TestReconcileChecked_DLQExceedsSource(t *testing.T) {
	// 若不拦截：Expected=100-150=-50，ES=0 → diff=50≠0... 但更隐蔽的是用虚高 DLQ 把真实
	// 缺口算平。这里直接在自洽性层拦下。
	if _, err := ReconcileChecked(Counts{SourceRows: 100, ESDocs: 0, DLQ: 150}); err == nil {
		t.Fatalf("DLQ > source_rows must be rejected")
	}
}

// TestReconcileChecked_DLQMasksShortfall 虚高 DLQ 把真实漏灌算成 false OK → 必须被自洽性拦下。
func TestReconcileChecked_DLQMasksShortfall(t *testing.T) {
	// 源 1000，实际只灌了 900（漏 100），但谎报 DLQ=100 → Expected=900 == ES 900 → 朴素 Reconcile 会判 OK！
	naive := Reconcile(Counts{SourceRows: 1000, ESDocs: 900, DLQ: 100})
	if !naive.OK {
		t.Fatalf("precondition: naive reconcile would falsely pass; got %+v", naive)
	}
	// DLQ=100 <= source=1000，raw_excluded=0<=ES，这个例子自洽性是过的——说明自洽性层不是万能。
	// 真正防线是 DLQ 由 backfill 自己精确计数（不靠人工传），自洽性只挡明显不可能的输入。
	if _, err := ReconcileChecked(Counts{SourceRows: 1000, ESDocs: 900, DLQ: 100}); err != nil {
		t.Fatalf("this input IS self-consistent (defense is authoritative DLQ count, not validation): %v", err)
	}
}

// TestReconcileChecked_RawExcludedExceedsES raw_excluded>ES doc → 拒绝（raw_excluded 是 ES doc 子集）。
func TestReconcileChecked_RawExcludedExceedsES(t *testing.T) {
	if _, err := ReconcileChecked(Counts{SourceRows: 100, ESDocs: 50, RawExcluded: 60}); err == nil {
		t.Fatalf("raw_excluded > es_docs must be rejected")
	}
}

// TestReconcileChecked_Negative 负计数（采集错误）→ 拒绝。
func TestReconcileChecked_Negative(t *testing.T) {
	if _, err := ReconcileChecked(Counts{SourceRows: -1, ESDocs: 0}); err == nil {
		t.Fatalf("negative count must be rejected")
	}
}
