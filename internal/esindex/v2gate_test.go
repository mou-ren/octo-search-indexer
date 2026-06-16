package esindex

import (
	"reflect"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// safetyFieldNames 是 reader 必读的三个安全/正确性字段在 searchmsg.Message 上的预期字段名
// （契约升到 v2 时由 octo-lib 添加）。映射到 esindex.Doc 上的目标字段名。
var safetyFieldNames = []struct {
	msgField string // searchmsg.Message 上的字段名（v2 契约新增）
	docField string // esindex.Doc 上对应字段名
}{
	{"SpaceID", "SpaceID"},
	{"Visibles", "Visibles"},
	{"MessageSeq", "MessageSeq"},
}

// TestV2Gate_DocFromMessageWiresSafetyFields 🔴 v2-gate latent fail-open 防回归门（Jerry 两轮点名）。
//
// 背景：实时安全闸（consumer.Run / LiveContractCarriesSafetyFields）只看 searchmsg.SchemaVersion>=2
// 就解封实时写入。但 DocFromMessage 当前**不 copy** spaceId/visibles/messageSeq（v1 契约根本没这仨
// 字段）。隐患：将来有人把 octo-lib bump 到 v2（加字段 + 升 SchemaVersion）解了闸，却忘了同步给
// DocFromMessage 接线 —— 实时路径就会写出**空** visibles 的 doc，reader 直接 fail-OPEN（普通成员
// 搜出群管才可见的系统消息）。安全闸开了，但数据是 fail-open 的，比闸关着更危险。
//
// 本测试用**反射**实现，故在 v1（字段尚不存在）下能正常编译/通过；一旦契约升到 v2：
//   - 若 searchmsg.Message 没有这三字段 → 报错（契约 bump 不完整）。
//   - 若有字段但 DocFromMessage 没把它们 copy 进 Doc → 断言失败转红（这正是要拦的 latent fail-open）。
//
// 即「v2 bump 不接线就过不了 CI」。接线（DocFromMessage 填全三字段）后本测试自动转绿。
func TestV2Gate_DocFromMessageWiresSafetyFields(t *testing.T) {
	if !LiveContractCarriesSafetyFields() {
		// 契约仍是 v1（SchemaVersion<2）：这三字段尚未进 Kafka 契约，实时闸保持关闭（由
		// TestLiveContractSafetyGate 钉死），此 latent 隐患尚未被激活。本门在 v2 bump 时自动生效。
		t.Skipf("contract still v1 (SchemaVersion=%d < %d); v2-gate inactive until contract bump",
			searchmsg.SchemaVersion, SafetyFieldsSchemaVersion)
	}

	// ── 契约已升到 v2：强制要求 DocFromMessage 把三个安全字段从契约 copy 进 Doc ──
	msg := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     "123456789012345678",
		ChannelID:     "g_1",
		ChannelType:   2,
	}

	// 反射写入三个安全字段的哨兵值（字段在 v2 契约里才存在，故用反射，避免 v1 下编译失败）。
	mv := reflect.ValueOf(&msg).Elem()
	const (
		sentSpace = "space-sentinel"
		sentVis   = "vis-sentinel"
		sentSeq   = uint64(424242)
	)
	for _, f := range safetyFieldNames {
		fv := mv.FieldByName(f.msgField)
		if !fv.IsValid() {
			t.Fatalf("contract bumped to v2 (SchemaVersion=%d) but searchmsg.Message lacks field %q; "+
				"a v2 bump must add SpaceID/Visibles/MessageSeq to the contract", searchmsg.SchemaVersion, f.msgField)
		}
		setSentinel(t, fv, f.msgField, sentSpace, sentVis, sentSeq)
	}

	doc, err := DocFromMessage(msg)
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}

	// 逐字段断言 Doc 已被填全（未接线则为零值 → 转红）。
	if doc.SpaceID != sentSpace {
		t.Fatalf("v2-gate: DocFromMessage did not copy SpaceID from the contract (latent fail-closed/open): got %q", doc.SpaceID)
	}
	if len(doc.Visibles) != 1 || doc.Visibles[0] != sentVis {
		t.Fatalf("v2-gate: DocFromMessage did not copy Visibles from the contract — reader would fail-OPEN: got %+v", doc.Visibles)
	}
	if doc.MessageSeq != sentSeq {
		t.Fatalf("v2-gate: DocFromMessage did not copy MessageSeq from the contract: got %d", doc.MessageSeq)
	}
}

// TestV2Gate_DocRetainsSafetyFields 始终生效（v1 也跑）：esindex.Doc 必须保留三个安全字段为
// 反射门的目标字段。若有人重命名/删掉 Doc.SpaceID/Visibles/MessageSeq，本门立刻转红——否则上面
// 的 v2-gate 反射断言会因目标字段消失而**静默失效**（gate 烂掉而无人知）。
func TestV2Gate_DocRetainsSafetyFields(t *testing.T) {
	dt := reflect.TypeOf(Doc{})
	for _, f := range safetyFieldNames {
		if _, ok := dt.FieldByName(f.docField); !ok {
			t.Fatalf("esindex.Doc lost safety field %q; the v2 latent-fail-open gate depends on it", f.docField)
		}
	}
}

// setSentinel 按字段 kind 写入合适类型的哨兵值（string / []string / 无符号整数）。
func setSentinel(t *testing.T, fv reflect.Value, name, sentStr, sentVis string, sentSeq uint64) {
	t.Helper()
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(sentStr)
	case reflect.Slice:
		if fv.Type().Elem().Kind() != reflect.String {
			t.Fatalf("v2-gate: field %q expected []string, got %s", name, fv.Type())
		}
		fv.Set(reflect.ValueOf([]string{sentVis}))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fv.SetUint(sentSeq)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fv.SetInt(int64(sentSeq))
	default:
		t.Fatalf("v2-gate: field %q has unexpected kind %s", name, fv.Kind())
	}
}
