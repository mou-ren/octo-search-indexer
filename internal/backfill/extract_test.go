package backfill

import (
	"encoding/json"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// 🔴 这些用例向量与 octo-server/modules/searchetl/payload_test.go **逐一对齐**：backfill 的
// payload 抽取必须与实时 producer 口径一致（否则 backfill 与实时增量对同一条消息产生不同 doc，
// 破坏「`_id=message_id` 幂等覆盖无害」前提）。任一侧改口径，本测试即应红。
//
// 常量对齐锚点：contentTypeText==1（octo-lib common.Text）、signal 位 == bit5（octo-lib
// config.Setting.Signal）。下面 TestExtract_ConstantsMatchOctoLib 显式钉住这两个常量值。

// textPayload 构造一条 type=Text 的明文 payload（int type，json.Unmarshal 后为 float64）。
func textPayload(t *testing.T, content string) []byte {
	t.Helper()
	return mustJSON(t, map[string]interface{}{"type": contentTypeText, "content": content})
}

// mustJSON marshals v or fails the test (keeps errcheck happy under check-blank).
func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// signalSettingByte 返回带 Signal 位（bit5）的 setting 字节，等价 config.Setting{Signal:true}.ToUint8()。
func signalSettingByte() uint8 { return 1 << 5 }

// TestExtract_ConstantsMatchOctoLib 钉住与 octo-lib 对齐的两个口径常量。
func TestExtract_ConstantsMatchOctoLib(t *testing.T) {
	if contentTypeText != 1 {
		t.Fatalf("contentTypeText must equal octo-lib common.Text(=1), got %d", contentTypeText)
	}
	if signalSettingMask != 32 {
		t.Fatalf("signalSettingMask must equal config Signal bit (1<<5=32), got %d", signalSettingMask)
	}
}

// TestExtract_Text 正常文本 → outcomeOK，content 取出，非 raw_excluded。
func TestExtract_Text(t *testing.T) {
	row := &srcMessageRow{MessageID: "m1", ChannelType: 2, Payload: textPayload(t, "hello 世界")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK, got %v", outcome)
	}
	if msg.RawExcluded {
		t.Fatalf("text msg must not be raw_excluded")
	}
	if msg.Content == nil || *msg.Content != "hello 世界" {
		t.Fatalf("content mismatch: %+v", msg.Content)
	}
	if msg.ContentType != contentTypeText {
		t.Fatalf("content_type=%d", msg.ContentType)
	}
	if msg.SchemaVersion != searchmsg.SchemaVersion || msg.Source != searchmsg.SourceETLMessageTable {
		t.Fatalf("contract metadata not set: %+v", msg)
	}
}

// TestExtract_SignalViaSetting Signal 加密（setting 位）→ raw_excluded，content=nil，不进 DLQ。
func TestExtract_SignalViaSetting(t *testing.T) {
	row := &srcMessageRow{MessageID: "m2", Setting: signalSettingByte(), Payload: []byte("ENCRYPTED-NOT-JSON")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded {
		t.Fatalf("want outcomeRawExcluded, got %v", outcome)
	}
	if !msg.RawExcluded || msg.Content != nil {
		t.Fatalf("signal msg must be raw_excluded with nil content: %+v", msg)
	}
}

// TestExtract_SignalViaColumn Signal 加密（signal 列）→ raw_excluded，即便 payload 恰为 JSON 也不解析。
func TestExtract_SignalViaColumn(t *testing.T) {
	row := &srcMessageRow{MessageID: "m3", Signal: 1, Payload: textPayload(t, "should be ignored")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded {
		t.Fatalf("want outcomeRawExcluded, got %v", outcome)
	}
	if !msg.RawExcluded || msg.Content != nil {
		t.Fatalf("signal-column msg must be raw_excluded: %+v", msg)
	}
}

// TestExtract_NonTextRawExcluded 非文本（媒体, type=2 Image）→ raw_excluded，不进 DLQ。
func TestExtract_NonTextRawExcluded(t *testing.T) {
	b := mustJSON(t, map[string]interface{}{"type": 2, "url": "http://x/y.png"})
	row := &srcMessageRow{MessageID: "m4", Payload: b}
	msg, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded {
		t.Fatalf("want outcomeRawExcluded for non-text, got %v", outcome)
	}
	if !msg.RawExcluded || msg.Content != nil {
		t.Fatalf("non-text must be raw_excluded: %+v", msg)
	}
}

// TestExtract_TextContentObjectRawExcluded type=Text 但 content 为 object（bot 误塞）→ 保守 raw_excluded。
func TestExtract_TextContentObjectRawExcluded(t *testing.T) {
	b := mustJSON(t, map[string]interface{}{"type": contentTypeText, "content": map[string]interface{}{"k": "v"}})
	row := &srcMessageRow{MessageID: "m5", Payload: b}
	msg, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded {
		t.Fatalf("want outcomeRawExcluded for text/object content, got %v", outcome)
	}
	if msg.Content != nil {
		t.Fatalf("content must be nil: %+v", msg.Content)
	}
}

// TestExtract_NonSignalBadJSON 非加密但 payload 非法 JSON（真异常）→ outcomeDLQ。
func TestExtract_NonSignalBadJSON(t *testing.T) {
	row := &srcMessageRow{MessageID: "m6", Payload: []byte("{not valid json")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeDLQ {
		t.Fatalf("want outcomeDLQ for bad json, got %v", outcome)
	}
	if msg.MessageID != "m6" {
		t.Fatalf("dlq msg must keep message_id for triage")
	}
}

// TestExtract_EmptyMapDLQ 空 JSON 对象（len(m)==0）→ DLQ（与 producer 一致）。
func TestExtract_EmptyMapDLQ(t *testing.T) {
	row := &srcMessageRow{MessageID: "m6b", Payload: []byte("{}")}
	if _, outcome := extractMessage(row); outcome != outcomeDLQ {
		t.Fatalf("empty map payload must be DLQ, got %v", outcome)
	}
}

// TestExtract_TypeAsFloat 兼容 json 反序列化把 type 解成 float64。
func TestExtract_TypeAsFloat(t *testing.T) {
	row := &srcMessageRow{MessageID: "m7", Payload: []byte(`{"type":1,"content":"x"}`)}
	msg, outcome := extractMessage(row)
	if outcome != outcomeOK || msg.Content == nil || *msg.Content != "x" {
		t.Fatalf("float64 type Text not handled: outcome=%v content=%v", outcome, msg.Content)
	}
}
