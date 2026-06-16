package backfill

import (
	"encoding/json"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// docFromRow 把一行源消息抽取为 reader 可读的 esindex.Doc（backfill 富化路径）。
//
// 这是分叉 B 在 backfill 侧的核心：backfill 读**原始 MySQL payload**，因此能自源填全
// reader 必读但 Kafka 契约缺失的三字段：
//   - spaceId：从 payload.space_id 抽（octo-server 发送时由 enrichPayloadWithSpaceID 写入，
//     p2p/group/topic 均经服务端权威注入）。reader 对 p2p 缺 spaceId 走 fail-closed。
//   - visibles：从 payload.visibles 抽（群系统消息「仅管理员可见」白名单）。reader 缺则 fail-OPEN。
//   - messageSeq：从 message 表 message_seq 列（reader channel_offset「清空会话」gate）。
//
// 正文 outcome（OK/RawExcluded/DLQ）完全复用 extractMessage 的口径（与实时 producer 一致），
// 故 backfill 与实时增量对同一条消息产生**同形态 doc**（同 _id 幂等覆盖无害）。
func docFromRow(row *srcMessageRow) (esindex.Doc, extractOutcome, error) {
	msg, outcome := extractMessage(row)
	if outcome == outcomeDLQ {
		return esindex.Doc{}, outcome, nil
	}

	// 复用 esindex 的契约→Doc 转换（messageId 全精度 long / payload 嵌套 / 字段名对齐 reader）。
	doc, err := esindex.DocFromMessage(msg)
	if err != nil {
		// message_id 非数值：reader 读 long messageId 无法对齐 → 当真异常落 DLQ（不静默落 0）。
		return esindex.Doc{}, outcomeDLQ, err
	}

	// 富化 reader 必读的安全/正确性字段（仅 backfill 可做：见上）。
	doc.MessageSeq = uint64(row.MessageSeq)
	if row.MessageSeq < 0 {
		doc.MessageSeq = 0
	}
	// 加密消息 payload 是密文，不解析（与 extract 一致），spaceId/visibles 留空（reader fail-closed）。
	if !isSignalEncrypted(row) {
		spaceID, visibles := extractVisibility(row.Payload)
		doc.SpaceID = spaceID
		doc.Visibles = visibles
	}
	return doc, outcome, nil
}

// extractVisibility 从原始 payload 字节解析 reader 鉴权所需的可见性字段：
//   - space_id（string）：p2p space 召回过滤的真源。
//   - visibles（[]string，仅保留字符串元素，与 octo-server visiblesAllows 口径一致）：
//     群系统消息白名单。空/缺失 → 空切片（reader：无 gate）。
//
// 解析失败（payload 非 JSON 对象，如加密/损坏）→ 返回空，保守不附加可见性约束
// （与 reader/server visiblesAllows 对损坏 payload「不约束」一致，且 space 留空走 fail-closed）。
func extractVisibility(payload []byte) (spaceID string, visibles []string) {
	var v struct {
		SpaceID  string            `json:"space_id"`
		Visibles []json.RawMessage `json:"visibles"`
	}
	if err := json.Unmarshal(payload, &v); err != nil {
		return "", nil
	}
	spaceID = v.SpaceID
	if len(v.Visibles) == 0 {
		return spaceID, nil
	}
	out := make([]string, 0, len(v.Visibles))
	for _, raw := range v.Visibles {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			// 与 server visiblesAllows 一致：非字符串元素跳过，不当作约束。
			continue
		}
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return spaceID, nil
	}
	return spaceID, out
}
