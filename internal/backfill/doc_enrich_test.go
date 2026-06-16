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
	sid, vis, err := extractVisibility(raw)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sid != "s1" {
		t.Fatalf("space_id mismatch: %q", sid)
	}
	if len(vis) != 2 || vis[0] != "u1" || vis[1] != "u2" {
		t.Fatalf("non-string visibles elements must be dropped: %+v", vis)
	}
}

// TestExtractVisibility_NonStringSpaceIDKeepsVisibles 🔴 V3b fail-OPEN 根因回归门：
// 某行 payload 的 space_id 是**非字符串** JSON 类型（数字/对象/上游漂移），而 visibles 数据**合法**。
// 旧实现用 strict struct（SpaceID string）→ 整条 Unmarshal 失败 → 返回 "",nil → 合法 visibles 被
// 静默清空 → reader fail-OPEN（普通成员搜出群管才可见消息）。修后：space_id 怪异类型退化为空
// spaceId（reader p2p fail-closed，安全方向），但 visibles **必须**完整保留——绝不产出空 visibles。
func TestExtractVisibility_NonStringSpaceIDKeepsVisibles(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"space_id is number", `{"space_id":123,"visibles":["admin1","admin2"]}`},
		{"space_id is object", `{"space_id":{"nested":1},"visibles":["admin1","admin2"]}`},
		{"space_id is bool", `{"space_id":true,"visibles":["admin1","admin2"]}`},
		{"space_id is null", `{"space_id":null,"visibles":["admin1","admin2"]}`},
		{"space_id is array", `{"space_id":["x"],"visibles":["admin1","admin2"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sid, vis, err := extractVisibility([]byte(tc.payload))
			if err != nil {
				t.Fatalf("a weird space_id type must NOT fail visibility parse: %v", err)
			}
			if sid != "" {
				t.Fatalf("non-string space_id must degrade to empty spaceId (p2p fail-closed), got %q", sid)
			}
			if len(vis) != 2 || vis[0] != "admin1" || vis[1] != "admin2" {
				t.Fatalf("legitimate visibles must NOT be cleared by a weird space_id (V3b fail-OPEN): %+v", vis)
			}
		})
	}
}

// TestExtractVisibility_StringSpaceIDStillParsed 字符串 space_id 仍正常取值（容忍类型未破坏正常路径）。
func TestExtractVisibility_StringSpaceIDStillParsed(t *testing.T) {
	sid, vis, err := extractVisibility([]byte(`{"space_id":"space-A","visibles":["u1"]}`))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sid != "space-A" {
		t.Fatalf("string space_id must parse: %q", sid)
	}
	if len(vis) != 1 || vis[0] != "u1" {
		t.Fatalf("visibles: %+v", vis)
	}
}

// TestExtractVisibility_StructurallyBrokenFailClosed payload 顶层非对象 / visibles 非数组 → err（fail-closed）。
// 调用方据此落 DLQ，绝不写空 visibles。
func TestExtractVisibility_StructurallyBrokenFailClosed(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"not json", `{not json`},
		{"top-level array", `["a","b"]`},
		{"top-level scalar", `42`},
		{"visibles is object", `{"space_id":"s1","visibles":{"u1":true}}`},
		{"visibles is string", `{"space_id":"s1","visibles":"u1,u2"}`},
		{"visibles is number", `{"space_id":"s1","visibles":7}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := extractVisibility([]byte(tc.payload))
			if err == nil {
				t.Fatalf("structurally broken visibility must fail-closed (err), not silently empty: %q", tc.payload)
			}
		})
	}
}

// TestExtractVisibility_AbsentVisiblesNoError 缺 visibles / visibles=null → 空切片 + 无错（reader 无 gate，合法）。
func TestExtractVisibility_AbsentVisiblesNoError(t *testing.T) {
	for _, p := range []string{`{"space_id":"s1"}`, `{"space_id":"s1","visibles":null}`, `{}`} {
		sid, vis, err := extractVisibility([]byte(p))
		if err != nil {
			t.Fatalf("absent visibles is legitimate (no gate), must not fail-closed: %q err=%v", p, err)
		}
		if len(vis) != 0 {
			t.Fatalf("absent visibles must be empty: %+v", vis)
		}
		_ = sid
	}
}

// TestDocFromRow_NonStringSpaceIDKeepsVisibles 🔴 端到端 V3b：backfill doc 富化路径下，
// 非字符串 space_id 的合法行**绝不**产出空 visibles —— 要么保留 visibles，要么 fail-closed。
// 本用例锁定「保留 visibles」（visibles 合法时不该 fail-closed 把好行也丢了）。
func TestDocFromRow_NonStringSpaceIDKeepsVisibles(t *testing.T) {
	// 手工拼一条 space_id 为数字、visibles 合法的明文 payload。
	raw := []byte(`{"type":1,"content":"hi","space_id":12345,"visibles":["admin1","admin2"]}`)
	row := &srcMessageRow{ID: 9, MessageID: "2001", ChannelType: 2, Payload: raw}
	doc, outcome, err := docFromRow(row)
	if err != nil {
		t.Fatalf("legitimate visibles row must not error: %v", err)
	}
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK (visibles legit), got %v", outcome)
	}
	if len(doc.Visibles) != 2 || doc.Visibles[0] != "admin1" || doc.Visibles[1] != "admin2" {
		t.Fatalf("V3b fail-OPEN: weird space_id cleared legitimate visibles: %+v", doc.Visibles)
	}
	if doc.SpaceID != "" {
		t.Fatalf("non-string space_id must degrade to empty (p2p fail-closed): %q", doc.SpaceID)
	}
}

// TestDocFromRow_BrokenVisibilityFailClosed visibles 结构损坏（非数组）→ 该行 fail-closed 落 DLQ，
// 绝不写空 visibles（fail-OPEN）。
func TestDocFromRow_BrokenVisibilityFailClosed(t *testing.T) {
	raw := []byte(`{"type":1,"content":"hi","space_id":"s1","visibles":"u1,u2"}`)
	row := &srcMessageRow{ID: 10, MessageID: "2002", ChannelType: 2, Payload: raw}
	_, outcome, err := docFromRow(row)
	if outcome != outcomeDLQ || err == nil {
		t.Fatalf("broken visibles must fail-closed to DLQ (never empty visibles): outcome=%v err=%v", outcome, err)
	}
}
