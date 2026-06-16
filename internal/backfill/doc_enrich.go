package backfill

import (
	"bytes"
	"encoding/json"
	"fmt"

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
		spaceID, visibles, verr := extractVisibility(row.Payload)
		if verr != nil {
			// 🔴 fail-CLOSED：可见性是 access-control ACL。一旦它无法可信解析（payload 非 JSON 对象、
			// visibles 非数组等结构性损坏），**绝不**写空 visibles —— 否则 reader(doc.go:21) 把空
			// visibles 当 fail-OPEN（群系统消息「仅管理员可见」白名单门被移除，普通成员能搜到管理员
			// 可见消息）。这类行一律落 DLQ（永久跳过写入），把 ACL 解析失败显式暴露给对账门而非静默塌掉。
			//
			// 注意：space_id 的 JSON 类型（数字/对象/上游漂移）**不**触发此路径——extractVisibility 把
			// space_id 解成容忍类型，非字符串 space_id 退化为空 spaceId（reader p2p fail-closed，安全方向），
			// 绝不因一个字段类型怪异连累合法的 visibles 一并清空（V3b fail-OPEN 的根因）。
			return esindex.Doc{}, outcomeDLQ, verr
		}
		doc.SpaceID = spaceID
		doc.Visibles = visibles
	}
	return doc, outcome, nil
}

// extractVisibility 从原始 payload 字节解析 reader 鉴权所需的可见性字段：
//   - space_id：p2p space 召回过滤的真源。**容忍类型**：仅当 JSON 字符串时取值，其余 JSON 类型
//     （数字/对象/上游漂移）退化为空 spaceId（reader p2p fail-closed，安全方向）——绝不让 space_id
//     的怪异类型炸掉整条 payload 的解析、连累合法 visibles 被清空（V3b fail-OPEN 根因）。
//   - visibles（[]string，仅保留字符串元素，与 octo-server visiblesAllows 口径一致）：
//     群系统消息白名单。空/缺失 → 空切片（reader：无 gate）。
//
// 🔴 fail-CLOSED 返回值契约：
//   - payload 是合法 JSON 对象（即使含怪异字段类型）→ 返回解析出的 spaceID/visibles，err=nil。
//   - payload **不是** JSON 对象（顶层非 `{...}`，如损坏/截断/数组/标量）→ 返回 err≠nil。
//     调用方据此 fail-closed（落 DLQ），绝不把无法可信解析的行写成空 visibles（fail-OPEN）。
//
// 为何不再用 strict struct unmarshal：strict struct 里 `SpaceID string` 一旦遇非字符串 space_id
// 会令**整个 struct** Unmarshal 失败 → 旧实现返回 "",nil → 合法 visibles 被静默清空 → reader
// fail-OPEN。这正是本 PR 要堵的 V3b，却从 active backfill 路径重新引入。改用「先验顶层是对象，
// 再字段级容忍解析」彻底切断「单字段类型 → 全可见性塌掉」的耦合。
func extractVisibility(payload []byte) (spaceID string, visibles []string, err error) {
	// 先把 payload 解成顶层对象（容忍每个字段的 JSON 类型）。顶层不是对象（损坏/截断/数组/标量）
	// 才是真正「可见性不可信」→ fail-closed。space_id/visibles 字段本身的类型不在此判死。
	var top map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(payload))
	if derr := dec.Decode(&top); derr != nil {
		return "", nil, fmt.Errorf("backfill: visibility payload not a JSON object (fail-closed): %w", derr)
	}

	// space_id：容忍类型——仅 JSON 字符串取值，其余类型留空（不报错、不连累 visibles）。
	if raw, ok := top["space_id"]; ok {
		var s string
		if uerr := json.Unmarshal(raw, &s); uerr == nil {
			spaceID = s
		}
		// 非字符串 space_id（数字/对象/null）→ spaceID 留空（reader p2p fail-closed，安全方向）。
	}

	// visibles：必须是 JSON 数组（或缺失/null）。是别的类型（对象/标量/字符串）说明白名单结构损坏
	// → fail-closed（绝不当作「无 gate」放空，那是 fail-OPEN）。
	rawVis, ok := top["visibles"]
	if !ok || string(rawVis) == "null" {
		return spaceID, nil, nil
	}
	var elems []json.RawMessage
	if uerr := json.Unmarshal(rawVis, &elems); uerr != nil {
		return "", nil, fmt.Errorf("backfill: visibles is not a JSON array (fail-closed, refuse to drop ACL): %w", uerr)
	}
	out := make([]string, 0, len(elems))
	for _, raw := range elems {
		var s string
		if uerr := json.Unmarshal(raw, &s); uerr != nil {
			// 与 server visiblesAllows 一致：非字符串元素跳过，不当作约束。
			continue
		}
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return spaceID, nil, nil
	}
	return spaceID, out, nil
}
