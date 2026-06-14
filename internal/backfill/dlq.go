package backfill

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// dlqRecord 是 backfill 路径下「真异常 / ES 永久拒绝」消息的本地 DLQ 落地记录。
//
// 与实时 consumer 的区别：实时 indexer 有 Kafka DLQ topic 可投；backfill **绕开 Kafka**，
// 这两类「没进 ES 正文索引」的行无 topic 可投，故落本地 spill 文件并精确计数。该计数是阶段 6
// 对账门的权威 DLQ 输入（"ES 去重 + DLQ + 已知排除 == 源行数"）。
type dlqRecord struct {
	Reason    string `json:"reason"`     // backfill_payload_unparseable / permanent_es_reject
	Table     string `json:"table"`      // 源分表
	ID        int64  `json:"id"`         // 源自增 id
	MessageID string `json:"message_id"` // 源 message_id（= 本应的 ES _id；去重键）
	Payload   []byte `json:"payload"`    // 原始 payload 字节（供排查 / 回灌）
	CreatedAt int64  `json:"created_at"` // 源行 created_at（纪元秒），用于按窗对账
	SpilledAt int64  `json:"spilled_at"` // 落地时间（纪元秒）
}

// DLQSpill 把 backfill 的 DLQ 行可靠落地到本地文件并精确计数（对账门权威输入）。
//
// 设计（吸取 codex review 的 3 个 DLQ-accounting 缺陷）：
//   - **spill 文件是去重后的真相源**：以 message_id 为去重键（= ES _id，每条消息唯一）。
//     重开时**从既有文件重建去重集 + 计数**（修：resume 后 Count 归零会让 inline reconcile
//     把已 DLQ 的行当 ES 缺失，误报 mismatch）。
//   - **写入幂等**：同一 message_id 重复 Write 是 no-op（修：批在 DLQ 写之后、checkpoint
//     推进之前崩溃，resume 重读同一行会重复 append/计数，膨胀 DLQ）。这与「整条管线
//     `_id=message_id` 幂等」口径一致。
//   - **按窗计数**：CountInWindow 只数 created_at ∈ 窗的记录（修：reconcile 窗不覆盖整个 run
//     时，用全量 dlqCount 会把窗外的 DLQ 行也减掉 → false mismatch/false OK）。
//   - **fail-closed**：任一 spill 写盘失败立即返回错误，调用方须 STOP（真异常绝不静默消失）。
//
// DLQ 量级极小（真异常稀少；线上实测撤回都仅 0.21%、真不可解析的更罕见），故在内存保留全部
// 记录（去重键 → created_at）以支持按窗计数，开销可忽略。
type DLQSpill struct {
	path string

	mu      sync.Mutex
	f       *os.File
	seen    map[string]int64 // dedup key (message_id) -> created_at（按窗计数用）
	nowUnix func() int64
}

// OpenDLQSpill 打开（或创建）spill 文件，并从既有内容重建去重集 + 计数（resume 安全）。
// dir 为空表示禁用 spill——此时若 backfill 遇到 DLQ 行必须硬停（见 runner），绝不允许 DLQ 行
// 静默消失破坏对账。
func OpenDLQSpill(dir string) (*DLQSpill, error) {
	if dir == "" {
		return nil, fmt.Errorf("backfill: DLQ spill dir is required (DLQ rows must be durably accounted; refuse to silently drop)")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("backfill: mkdir spill dir: %w", err)
	}
	path := filepath.Join(dir, "backfill-dlq.ndjson")
	seen, err := loadSeen(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("backfill: open spill file: %w", err)
	}
	// fsync 父目录，让新建的 spill 文件「目录项」本身可崩溃恢复——否则 host 崩溃后 checkpoint
	// 可能存活而 backfill-dlq.ndjson 整个消失，replay 漏计该文件里所有已被游标越过的 DLQ 行。
	// 文件内容的 fsync 由 Sync()/Close() 负责；这里补的是目录项的 fsync（仅创建时一次性成本）。
	if err := fsyncDir(dir); err != nil {
		if cerr := f.Close(); cerr != nil {
			return nil, fmt.Errorf("backfill: %w (and close spill: %v)", err, cerr)
		}
		return nil, err
	}
	return &DLQSpill{path: path, f: f, seen: seen, nowUnix: func() int64 { return time.Now().Unix() }}, nil
}

// loadSeen 从既有 spill 文件重建「去重键 → created_at」集（resume 时计数不归零、写入幂等）。
func loadSeen(path string) (map[string]int64, error) {
	seen := map[string]int64{}
	f, err := os.Open(path) //nolint:gosec // path 来自运维配置，非用户输入
	if err != nil {
		if os.IsNotExist(err) {
			return seen, nil
		}
		return nil, fmt.Errorf("backfill: open spill for replay: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			// 只读句柄关闭失败不致命，但记一笔以免静默。
			fmt.Fprintf(os.Stderr, "backfill: close spill replay handle: %v\n", cerr)
		}
	}()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024) // 容忍大 payload 行
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec dlqRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("backfill: corrupt spill line during replay: %w", err)
		}
		seen[dedupKey(rec)] = rec.CreatedAt
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("backfill: scan spill during replay: %w", err)
	}
	return seen, nil
}

// dedupKey 以 message_id 为去重键（= ES _id，每条消息唯一）；空 message_id 退化为 table:id。
func dedupKey(rec dlqRecord) string {
	if rec.MessageID != "" {
		return rec.MessageID
	}
	return fmt.Sprintf("%s:%d", rec.Table, rec.ID)
}

// fsyncDir 打开并 fsync 一个目录，使其下新建 / 重命名的目录项可崩溃恢复。
func fsyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir 来自运维配置，非用户输入
	if err != nil {
		return fmt.Errorf("backfill: open spill dir for fsync: %w", err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("backfill: fsync spill dir: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("backfill: close spill dir after fsync: %w", closeErr)
	}
	return nil
}

// Write 幂等落地一条 DLQ 记录：同一去重键已存在则 no-op（不重复 append/计数）；否则 append
// 并记入去重集。写盘失败返回错误（fail-closed）。
func (s *DLQSpill) Write(rec dlqRecord) error {
	key := dedupKey(rec)
	rec.Reason = reasonOrDefault(rec.Reason)
	rec.SpilledAt = s.nowUnix()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[key]; ok {
		return nil // 幂等：该源行已记账，不重复
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("backfill: marshal dlq record (id=%d msg=%s): %w", rec.ID, rec.MessageID, err)
	}
	if _, err := s.f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("backfill: write dlq spill (id=%d msg=%s): %w", rec.ID, rec.MessageID, err)
	}
	s.seen[key] = rec.CreatedAt
	return nil
}

// reasonOrDefault 给未显式置 reason 的记录补默认（payload 不可解析）。
func reasonOrDefault(r string) string {
	if r == "" {
		return "backfill_payload_unparseable"
	}
	return r
}

// Count 返回已记账的去重 DLQ 记录总数（日志 / 全量对账用）。
func (s *DLQSpill) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.seen))
}

// CountInWindow 返回 created_at ∈ [fromUnix, toUnix] 的去重 DLQ 记录数（按窗对账门用）。
// 与 internal/recon 的 range filter（gte/lte）口径一致。
func (s *DLQSpill) CountInWindow(fromUnix, toUnix int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, createdAt := range s.seen {
		if createdAt >= fromUnix && createdAt <= toUnix {
			n++
		}
	}
	return n
}

// Sync 把已 append 的 DLQ 记录刷盘（fsync）。**必须在推进 checkpoint 前调用**：否则主机崩溃 /
// 延迟写回失败可能让 checkpoint 跳过某些 id，而它们的 DLQ 记录尚未落盘 → resume 后 DLQ 漏计、
// 该行不经手动回退 checkpoint 不可恢复（durability ordering：先让 DLQ 落盘，再推进游标）。
func (s *DLQSpill) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("backfill: sync dlq spill: %w", err)
	}
	return nil
}

// Path 返回 spill 文件路径（日志 / 运维用）。
func (s *DLQSpill) Path() string { return s.path }

// Close 把缓冲刷盘并关闭文件。
func (s *DLQSpill) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Sync()
	if cerr := s.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	s.f = nil
	return err
}
