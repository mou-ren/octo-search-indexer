package consumer

import (
	"testing"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

func TestClassifyBulk(t *testing.T) {
	if ok, perm := classifyBulk(true, esindex.BulkItemResult{Status: 201, OK: true}); ok || !perm {
		t.Fatalf("schemaInvalid must be permanent regardless of result, got ok=%v perm=%v", ok, perm)
	}
	if ok, perm := classifyBulk(false, esindex.BulkItemResult{Status: 201, OK: true}); !ok || perm {
		t.Fatalf("201 must be ok")
	}
	if ok, perm := classifyBulk(false, esindex.BulkItemResult{Status: 400}); ok || !perm {
		t.Fatalf("400 must be permanent")
	}
	if ok, perm := classifyBulk(false, esindex.BulkItemResult{Status: 429}); ok || perm {
		t.Fatalf("429 must be transient (not ok, not permanent)")
	}
	if ok, perm := classifyBulk(false, esindex.BulkItemResult{Status: 503}); ok || perm {
		t.Fatalf("503 must be transient")
	}
	if ok, perm := classifyBulk(false, esindex.BulkItemResult{Status: 0}); ok || perm {
		t.Fatalf("batch-level (status 0) must be transient")
	}
}

func TestHasTransient(t *testing.T) {
	if hasTransient([]itemDisposition{dispOK, dispDLQResolved}) {
		t.Fatalf("no transient expected")
	}
	if !hasTransient([]itemDisposition{dispOK, dispTransient}) {
		t.Fatalf("transient expected")
	}
}

func part(p int, off int64) fetchedMessage {
	return fetchedMessage{Topic: "octo.message.v1", Partition: p, Offset: off}
}

// TestPartitionCommitPoints_SinglePartition 单分区：前缀到首个 transient 前一条。
func TestPartitionCommitPoints_SinglePartition(t *testing.T) {
	batch := []fetchedMessage{part(0, 10), part(0, 11), part(0, 12)}
	disp := []itemDisposition{dispOK, dispTransient, dispOK}
	pts := partitionCommitPoints(batch, disp)
	if len(pts) != 1 || pts[0].Offset != 10 {
		t.Fatalf("expected commit point at offset 10, got %+v", pts)
	}
}

// TestPartitionCommitPoints_HeadTransientNoPoint 队首 transient → 该分区无提交点。
func TestPartitionCommitPoints_HeadTransientNoPoint(t *testing.T) {
	batch := []fetchedMessage{part(0, 10), part(0, 11)}
	disp := []itemDisposition{dispTransient, dispOK}
	if pts := partitionCommitPoints(batch, disp); len(pts) != 0 {
		t.Fatalf("head transient → no commit point, got %+v", pts)
	}
}

// TestPartitionCommitPoints_DLQResolvedCounts dispDLQResolved 计入前缀。
func TestPartitionCommitPoints_DLQResolvedCounts(t *testing.T) {
	batch := []fetchedMessage{part(0, 10), part(0, 11), part(0, 12)}
	disp := []itemDisposition{dispOK, dispDLQResolved, dispOK}
	pts := partitionCommitPoints(batch, disp)
	if len(pts) != 1 || pts[0].Offset != 12 {
		t.Fatalf("expected commit point at 12 (all resolved), got %+v", pts)
	}
}

// TestPartitionCommitPoints_MultiPartitionIndependent 🔴 多分区各自独立推进：
// 分区0 全 OK → commit 到末；分区1 中间 transient → 只 commit 到 transient 前。
func TestPartitionCommitPoints_MultiPartitionIndependent(t *testing.T) {
	batch := []fetchedMessage{
		part(0, 100), part(1, 200), part(0, 101), part(1, 201), part(1, 202),
	}
	disp := []itemDisposition{
		dispOK,        // p0:100
		dispOK,        // p1:200
		dispOK,        // p0:101
		dispTransient, // p1:201 → 截断 p1 前缀
		dispOK,        // p1:202（不得越过 201）
	}
	pts := partitionCommitPoints(batch, disp)
	got := map[int]int64{}
	for _, p := range pts {
		got[p.Partition] = p.Offset
	}
	if got[0] != 101 {
		t.Fatalf("partition 0 should commit to 101, got %d", got[0])
	}
	if got[1] != 200 {
		t.Fatalf("partition 1 should commit only to 200 (not cross transient 201), got %d", got[1])
	}
}

// TestPartitionCommitPoints_OutOfOrderInput 乱序入参也按 offset 升序算前缀（防御）。
func TestPartitionCommitPoints_OutOfOrderInput(t *testing.T) {
	batch := []fetchedMessage{part(0, 12), part(0, 10), part(0, 11)}
	// 对应 offset 12,10,11 的处置：12=OK,10=OK,11=transient
	disp := []itemDisposition{dispOK, dispOK, dispTransient}
	pts := partitionCommitPoints(batch, disp)
	// 按 offset 序：10(OK),11(transient) → 前缀停在 10；12 虽 OK 但在 11 之后不算。
	if len(pts) != 1 || pts[0].Offset != 10 {
		t.Fatalf("expected commit point at 10 (sorted prefix), got %+v", pts)
	}
}
