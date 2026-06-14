package backfill

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCheckpoint_PersistAndReload Advance 后重开应载入同一水位。
func TestCheckpoint_PersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	cp, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := cp.Advance("message", 10); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := cp.Advance("message1", 5); err != nil {
		t.Fatalf("advance: %v", err)
	}
	cp2, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if cp2.Get("message") != 10 || cp2.Get("message1") != 5 {
		t.Fatalf("reload mismatch: %d %d", cp2.Get("message"), cp2.Get("message1"))
	}
}

// TestCheckpoint_Monotonic 回退 / 同值 Advance 被忽略（幂等重跑安全）。
func TestCheckpoint_Monotonic(t *testing.T) {
	cp, err := OpenCheckpoint(filepath.Join(t.TempDir(), "cp.json"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := cp.Advance("message", 10); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := cp.Advance("message", 5); err != nil { // 回退被忽略
		t.Fatalf("advance: %v", err)
	}
	if cp.Get("message") != 10 {
		t.Fatalf("monotonic violated: %d", cp.Get("message"))
	}
}

// TestCheckpoint_EmptyPathInMemory path 为空 → 内存态，不报错（不可续传）。
func TestCheckpoint_EmptyPathInMemory(t *testing.T) {
	cp, err := OpenCheckpoint("")
	if err != nil {
		t.Fatalf("empty path must be ok (in-memory): %v", err)
	}
	if err := cp.Advance("message", 3); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if cp.Get("message") != 3 {
		t.Fatalf("in-memory advance failed")
	}
}

// TestCheckpoint_MissingFileStartsEmpty 文件不存在 → 空起步，不报错。
func TestCheckpoint_MissingFileStartsEmpty(t *testing.T) {
	cp, err := OpenCheckpoint(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("missing file must start empty: %v", err)
	}
	if cp.Get("message") != 0 {
		t.Fatalf("missing file must yield 0 watermark")
	}
}

// TestCheckpoint_CreatesParentDir 🔴 P2-2：父目录不存在时 OpenCheckpoint 应建好目录（fail-fast），
// 之后 Advance 能直接持久化，不晚到第一次写才失败。
func TestCheckpoint_CreatesParentDir(t *testing.T) {
	// 多层尚不存在的父目录。
	path := filepath.Join(t.TempDir(), "nested", "deep", "cp.json")
	cp, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("OpenCheckpoint must create missing parent dirs: %v", err)
	}
	if err := cp.Advance("message", 5); err != nil {
		t.Fatalf("advance must persist after dir prepared: %v", err)
	}
	// 重开验证已落盘。
	cp2, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if cp2.Get("message") != 5 {
		t.Fatalf("checkpoint not persisted into created dir: %d", cp2.Get("message"))
	}
}

// TestCheckpoint_PersistLeavesDurableDir Advance 后持久化路径（写临时文件 → rename → fsync 父目录）
// 应是干净耐久状态：只剩最终 checkpoint 文件，无残留临时文件，且内容可重载。
// （父目录 fsync 本身在单测里不可直接观测，这里断言 persist 路径完整收尾且 rename 已生效——
//
//	与 DLQ writer 同一道 fsyncDir，让 rename 目录项变更落盘、续传可靠。）
func TestCheckpoint_PersistLeavesDurableDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	cp, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := cp.Advance("message", i); err != nil {
			t.Fatalf("advance %d: %v", i, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 || names[0] != "cp.json" {
		t.Fatalf("persist must leave only the final checkpoint (no stray temp files), got %v", names)
	}
	cp2, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if cp2.Get("message") != 3 {
		t.Fatalf("persisted watermark mismatch: %d", cp2.Get("message"))
	}
}
