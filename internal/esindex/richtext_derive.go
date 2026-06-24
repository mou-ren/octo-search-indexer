package esindex

import (
	"fmt"
)

// 富文本(type=14)内嵌媒体虚拟子文档（B2 方案，richtext-virtual-docs-indexer-dev.md）。
//
// 目标：让富文本里内嵌的 image/file 能被 reader 的 _search_media / _search_files 搜到。
// 对每个内嵌 image/file block，额外产出一个独立 OS doc，长得就像普通图片(type=2)/文件(type=8)
// 消息，带 virtual=true + parentMessageId。reader 媒体/文件端点白名单（payload.type∈{2,5,8}）
// 直接命中，octo-server 几乎不用改。
//
// 范围（本期保守）：
//   - 只派生 image(→type=2) / file(→type=8)。richText block 类型仅 text/image/file（对齐
//     octo-web RichTextBlockType / octo-lib common/richtext.go），不臆造 video block。
//   - 撤回/编辑联动**不做**（路线甲：reader 用 parentMessageId 回 MySQL join 判可见性/撤回）。
//   - 子文档继承父的标识/时序/可见性字段；ES _id = "<父messageId>-rt<i>"（i 按 blocks 顺序，从 0 起）。

// richTextDerivatives 从父 doc 的 RawPayload 解析 type=14 的 image/file block，产出虚拟子文档列表。
//
// 仅当父 doc 确为富文本（Payload.Type==14）且 RawPayload 非空时产出；否则返回 nil。
// 子文档幂等键 _id 用复合键，重跑 backfill/consumer 重投覆盖同一条，不重复创建。
func richTextDerivatives(parent Doc) []Doc {
	if parent.Payload == nil || parent.Payload.Type == nil || *parent.Payload.Type != payloadTypeRichText {
		return nil
	}
	if len(parent.PayloadRaw) == 0 {
		return nil
	}
	raw, ok := decodeObjectUseNumber(parent.PayloadRaw)
	if !ok {
		return nil
	}
	blocks := richTextBlocks(raw["content"])
	if len(blocks) == 0 {
		return nil
	}

	var out []Doc
	for i, blk := range blocks {
		t, _ := blk["type"].(string)
		var sub *Payload
		switch t {
		case "image":
			sub = &Payload{Type: intPtr(payloadTypeImage), Image: imageFromBlock(blk)}
		case "file":
			sub = &Payload{Type: intPtr(payloadTypeFile), File: fileFromBlock(blk)}
		default:
			continue // text / 未知 block：不派生媒体子文档
		}
		out = append(out, deriveChild(parent, sub, i))
	}
	return out
}

// deriveChild 构造一个虚拟子文档：继承父的标识/时序/可见性字段，挂上自身媒体 payload。
// ES _id = "<父messageId>-rt<i>"（字符串复合键，messageId 字段仍保留父值供 reader 游标排序）。
func deriveChild(parent Doc, sub *Payload, i int) Doc {
	return Doc{
		SchemaVersion: parent.SchemaVersion,
		MessageID:     parent.MessageID, // 与父相同（reader cursor 排序键 timestamp+messageId 不被打散）
		MessageSeq:    parent.MessageSeq,
		From:          parent.From,
		To:            parent.To,
		ChannelID:     parent.ChannelID,
		ChannelType:   parent.ChannelType,
		SpaceID:       parent.SpaceID,
		Visibles:      parent.Visibles,
		Timestamp:     parent.Timestamp,
		CreatedAt:     parent.CreatedAt,
		Source:        parent.Source,
		Payload:       sub,
		// 父子追踪
		ParentMessageID:   parent.MessageID,
		ParentPayloadType: payloadTypeRichText,
		Virtual:           true,
		// SubSeq 父独占 0，子从 1 递增（block 序号 i+1），保 (messageId, subSeq) 不跟父及兄弟撞。
		SubSeq: i + 1,
		// _id 复合键（字符串），不影响 messageId 字段保持 long
		idOverride: fmt.Sprintf("%d-rt%d", parent.MessageID, i),
	}
}

// imageFromBlock 把 richText image block 投影成 ImagePayload（与普通图片消息一致）。
func imageFromBlock(blk map[string]any) *ImagePayload {
	p := &ImagePayload{}
	if u, ok := blk["url"].(string); ok {
		p.URL = u
	}
	if c, ok := blk["caption"].(string); ok {
		p.Caption = c
	}
	if n, ok := blk["name"].(string); ok {
		p.Name = n
	}
	if w, ok := extractInt(blk, "width"); ok {
		p.Width = w
	}
	if h, ok := extractInt(blk, "height"); ok {
		p.Height = h
	}
	return p
}

// fileFromBlock 把 richText file block 投影成 FilePayload（与普通文件消息一致）。
func fileFromBlock(blk map[string]any) *FilePayload {
	p := &FilePayload{}
	if u, ok := blk["url"].(string); ok {
		p.URL = u
	}
	if n, ok := blk["name"].(string); ok {
		p.Name = n
	}
	if c, ok := blk["caption"].(string); ok {
		p.Caption = c
	}
	if sz, ok := extractInt64(blk, "size"); ok {
		p.Size = sz
	}
	if ext, ok := blk["extension"].(string); ok {
		p.Extension = ext
	}
	return p
}

func intPtr(v int) *int { return &v }
