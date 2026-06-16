package backfill

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// SourceReader 按 keyset 分页从某分表读一批源行（id>after 升序 LIMIT limit）。
// 抽成接口便于单测注入假数据源，并与具体存储解耦。
type SourceReader interface {
	// ReadBatch 读 table 中 id>after 的下一批（按 id 升序，至多 limit 行）。
	// 返回空切片表示该表已读尽。
	ReadBatch(ctx context.Context, table string, after int64, limit int) ([]*srcMessageRow, error)
}

// MySQLSource 用 database/sql 读 message 分表（keyset 分页，按主键 id 有序推进，不用 OFFSET）。
//
// 列集合与 octo-server searchetl readBatch 一致：抽取所需正文 + 可见性 + Signal 判定 +
// created_at（对账按窗 / DLQ 记录用）。
type MySQLSource struct {
	db *sql.DB
}

// NewMySQLSource 构造。
func NewMySQLSource(db *sql.DB) *MySQLSource {
	return &MySQLSource{db: db}
}

// ReadBatch 实现 keyset 分页：WHERE id>? ORDER BY id ASC LIMIT ?。
//
// 表名来自受控配置（非用户输入），仍用 safeTableName 白名单校验 + 反引号包裹防注入；
// 时间 / 分页参数走占位符。OFFSET 不用——keyset 分页在百万级深翻页 O(1) 定位，无 OFFSET 漂移。
func (s *MySQLSource) ReadBatch(ctx context.Context, table string, after int64, limit int) ([]*srcMessageRow, error) {
	if !safeTableName(table) {
		return nil, fmt.Errorf("backfill: unsafe table name %q", table)
	}
	q := fmt.Sprintf(
		"SELECT id, message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, "+
			"`timestamp`, UNIX_TIMESTAMP(created_at) AS created_unix, payload "+
			"FROM `%s` WHERE id>? ORDER BY id ASC LIMIT ?", table)
	rows, err := s.db.QueryContext(ctx, q, after, limit)
	if err != nil {
		return nil, fmt.Errorf("backfill: query %s: %w", table, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			log.Printf("backfill: close rows for %s: %v", table, cerr)
		}
	}()

	var out []*srcMessageRow
	for rows.Next() {
		var r srcMessageRow
		if err := rows.Scan(
			&r.ID, &r.MessageID, &r.MessageSeq, &r.FromUID, &r.ChannelID, &r.ChannelType,
			&r.Setting, &r.Signal, &r.Timestamp, &r.CreatedUnix, &r.Payload,
		); err != nil {
			return nil, fmt.Errorf("backfill: scan %s: %w", table, err)
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("backfill: iterate %s: %w", table, err)
	}
	return out, nil
}

// safeTableName 仅允许 [A-Za-z0-9_]（防御性，表名来自配置）。与 internal/recon 同口径。
func safeTableName(t string) bool {
	if t == "" {
		return false
	}
	for _, r := range t {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}
