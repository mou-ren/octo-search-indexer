package backfill

import (
	"encoding/json"
	"testing"
)

// payloadWith 构造一条带 space_id/visibles 的文本 payload（明文 JSON，模拟 enrichPayloadWithSpaceID 产物）。
func payloadWith(t *testing.T, content, spaceID string, visibles []string) []byte {
	t.Helper()
	m := map[string]interface{}{"type": contentTypeText, "content": content}
	if spaceID != "" {
		m["space_id"] = spaceID
	}
	if visibles != nil {
		m["visibles"] = visibles
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestDocFromRow_EnrichesSpaceIDVisiblesSeq 🔴 V1(b)+V3b 解红的根：backfill 从原始 MySQL
// payload 自源填 reader 必读的 spaceId/visibles + message_seq 列。
func TestDocFromRow_EnrichesSpaceIDVisiblesSeq(t *testing.T) {
	row := &srcMessageRow{
		ID: 1, MessageID: "1001", MessageSeq: 77, ChannelType: 1, // p2p
		Payload: payloadWith(t, "hello", "space-A", []string{"admin1", "admin2"}),
	}
	doc, outcome, err := docFromRow(row)
	if err != nil {
		t.Fatalf("docFromRow: %v", err)
	}
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK, got %v", outcome)
	}
	if doc.SpaceID != "space-A" {
		t.Fatalf("spaceId not enriched (V1b would stay red): %q", doc.SpaceID)
	}
	if len(doc.Visibles) != 2 || doc.Visibles[0] != "admin1" || doc.Visibles[1] != "admin2" {
		t.Fatalf("visibles not enriched (V3b would fail-OPEN): %+v", doc.Visibles)
	}
	if doc.MessageSeq != 77 {
		t.Fatalf("messageSeq not carried (reader channel_offset gate): %d", doc.MessageSeq)
	}
	if doc.MessageID != 1001 {
		t.Fatalf("messageId mismatch: %d", doc.MessageID)
	}
}

// TestDocFromRow_NoVisibilityFields payload 无 space_id/visibles → 字段留空（reader p2p fail-closed / 无 gate）。
func TestDocFromRow_NoVisibilityFields(t *testing.T) {
	row := &srcMessageRow{ID: 2, MessageID: "1002", ChannelType: 2, Payload: payloadWith(t, "hi", "", nil)}
	doc, outcome, err := docFromRow(row)
	if err != nil || outcome != outcomeOK {
		t.Fatalf("docFromRow: outcome=%v err=%v", outcome, err)
	}
	if doc.SpaceID != "" || len(doc.Visibles) != 0 {
		t.Fatalf("absent visibility fields must stay empty: spaceId=%q visibles=%+v", doc.SpaceID, doc.Visibles)
	}
}

// TestDocFromRow_SignalEncryptedNoVisibility 加密消息不解析 payload → spaceId/visibles 留空（fail-closed）。
func TestDocFromRow_SignalEncryptedNoVisibility(t *testing.T) {
	row := &srcMessageRow{ID: 3, MessageID: "1003", Signal: 1, ChannelType: 1, Payload: []byte("CIPHERTEXT")}
	doc, outcome, err := docFromRow(row)
	if err != nil {
		t.Fatalf("docFromRow: %v", err)
	}
	if outcome != outcomeRawExcluded {
		t.Fatalf("signal msg must be raw_excluded, got %v", outcome)
	}
	if doc.SpaceID != "" || len(doc.Visibles) != 0 {
		t.Fatalf("encrypted msg must not leak visibility fields: %+v", doc)
	}
}

// TestDocFromRow_NonNumericMessageIDToDLQ message_id 非数值 → outcomeDLQ + err（不静默落 0）。
func TestDocFromRow_NonNumericMessageIDToDLQ(t *testing.T) {
	row := &srcMessageRow{ID: 4, MessageID: "abc", ChannelType: 2, Payload: payloadWith(t, "hi", "", nil)}
	_, outcome, err := docFromRow(row)
	if outcome != outcomeDLQ || err == nil {
		t.Fatalf("non-numeric message_id must be DLQ with error: outcome=%v err=%v", outcome, err)
	}
}

// TestDocFromRow_BadJSONToDLQ payload 真异常 → outcomeDLQ（与 extract 口径一致）。
func TestDocFromRow_BadJSONToDLQ(t *testing.T) {
	row := &srcMessageRow{ID: 5, MessageID: "1005", ChannelType: 2, Payload: []byte("{not json")}
	_, outcome, err := docFromRow(row)
	if outcome != outcomeDLQ {
		t.Fatalf("bad json must be DLQ, got %v (err=%v)", outcome, err)
	}
}

// TestExtractVisibility_DropsNonStringElements visibles 非字符串元素跳过（与 server visiblesAllows 口径一致）。
func TestExtractVisibility_DropsNonStringElements(t *testing.T) {
	raw := []byte(`{"space_id":"s1","visibles":["u1",123,"u2",{"x":1}]}`)
	sid, vis := extractVisibility(raw)
	if sid != "s1" {
		t.Fatalf("space_id mismatch: %q", sid)
	}
	if len(vis) != 2 || vis[0] != "u1" || vis[1] != "u2" {
		t.Fatalf("non-string visibles elements must be dropped: %+v", vis)
	}
}
