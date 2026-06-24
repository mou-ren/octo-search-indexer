package esindex

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// richTextRaw 构造一条 type=14 富文本原始 payload（含给定 content blocks）。
func richTextRaw(blocks string) string {
	return `{"type":14,"plain":"x","content":` + blocks + `}`
}

// TestRichTextDerivatives_ImageAndFile 富文本含 1 图 1 文件 → 派生 2 个子文档：
// image→type=2、file→type=8，各带 virtual=true + parentMessageId=父，_id 复合键。
func TestRichTextDerivatives_ImageAndFile(t *testing.T) {
	raw := richTextRaw(`[
		{"type":"text","text":"前言"},
		{"type":"image","url":"http://x/a.png","name":"a.png","caption":"图说","width":100,"height":200},
		{"type":"file","url":"http://x/b.pdf","name":"b.pdf","size":1234,"extension":"pdf"}
	]`)
	d, err := DocFromMessage(branchAMsg("2062443880774537216", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(d.Derivatives) != 2 {
		t.Fatalf("want 2 derivatives (image+file), got %d", len(d.Derivatives))
	}

	img := d.Derivatives[0]
	if img.Payload == nil || img.Payload.Type == nil || *img.Payload.Type != payloadTypeImage {
		t.Fatalf("derivative[0] want image type=2, got %+v", img.Payload)
	}
	if img.Payload.Image == nil || img.Payload.Image.URL != "http://x/a.png" ||
		img.Payload.Image.Name != "a.png" || img.Payload.Image.Caption != "图说" ||
		img.Payload.Image.Width != 100 || img.Payload.Image.Height != 200 {
		t.Fatalf("image projection mismatch: %+v", img.Payload.Image)
	}
	if !img.Virtual || img.ParentMessageID != d.MessageID || img.ParentPayloadType != payloadTypeRichText {
		t.Fatalf("image parent-tracking mismatch: virtual=%v parent=%d ptype=%d", img.Virtual, img.ParentMessageID, img.ParentPayloadType)
	}
	if img.MessageID != d.MessageID {
		t.Fatalf("derivative messageId must equal parent: got %d want %d", img.MessageID, d.MessageID)
	}
	// _id 复合键：image 是第 2 个 block（index=1）。
	if got := img.idString(); got != "2062443880774537216-rt1" {
		t.Fatalf("image _id want <parent>-rt1, got %q", got)
	}
	// subSeq = block序号 i+1（父独占 0）。image 在 index=1 → subSeq=2。
	if img.SubSeq != 2 {
		t.Fatalf("image subSeq want 2 (block idx 1 +1), got %d", img.SubSeq)
	}

	file := d.Derivatives[1]
	if file.Payload == nil || file.Payload.Type == nil || *file.Payload.Type != payloadTypeFile {
		t.Fatalf("derivative[1] want file type=8, got %+v", file.Payload)
	}
	if file.Payload.File == nil || file.Payload.File.URL != "http://x/b.pdf" ||
		file.Payload.File.Name != "b.pdf" || file.Payload.File.Size != 1234 ||
		file.Payload.File.Extension != "pdf" {
		t.Fatalf("file projection mismatch: %+v", file.Payload.File)
	}
	if got := file.idString(); got != "2062443880774537216-rt2" {
		t.Fatalf("file _id want <parent>-rt2, got %q", got)
	}
	// file 在 index=2 → subSeq=3。
	if file.SubSeq != 3 {
		t.Fatalf("file subSeq want 3 (block idx 2 +1), got %d", file.SubSeq)
	}
	// 父 doc subSeq 独占 0。
	if d.SubSeq != 0 {
		t.Fatalf("parent richtext doc subSeq want 0, got %d", d.SubSeq)
	}
	// 继承父可见性字段。
	if file.ChannelID != d.ChannelID || file.From != d.From || file.Timestamp != d.Timestamp {
		t.Fatalf("file did not inherit parent fields: %+v", file)
	}
}

// TestRichTextDerivatives_TextOnlyNoDerivatives 纯文本富文本（无内嵌媒体）→ 不派生子文档。
func TestRichTextDerivatives_TextOnlyNoDerivatives(t *testing.T) {
	raw := richTextRaw(`[{"type":"text","text":"只有文字"}]`)
	d, err := DocFromMessage(branchAMsg("100", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(d.Derivatives) != 0 {
		t.Fatalf("text-only richtext must have 0 derivatives, got %d", len(d.Derivatives))
	}
}

// TestRichTextDerivatives_NonRichTextNoDerivatives 普通图片消息(type=2)不派生子文档。
func TestRichTextDerivatives_NonRichTextNoDerivatives(t *testing.T) {
	raw := `{"type":2,"url":"http://x/a.png","name":"a.png"}`
	d, err := DocFromMessage(branchAMsg("101", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(d.Derivatives) != 0 {
		t.Fatalf("non-richtext must have 0 derivatives, got %d", len(d.Derivatives))
	}
	// 普通消息 doc subSeq 显式 = 0。
	if d.SubSeq != 0 {
		t.Fatalf("normal doc subSeq want 0, got %d", d.SubSeq)
	}
}

// TestEncodeBulkBody_ParentChildAdjacent 父 doc 后紧跟子 doc 编进同一 body（父子相邻 + 复合 _id）。
func TestEncodeBulkBody_ParentChildAdjacent(t *testing.T) {
	raw := richTextRaw(`[{"type":"image","url":"http://x/a.png","name":"a.png"}]`)
	parent, err := DocFromMessage(branchAMsg("500", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	body, err := encodeBulkBody([]Doc{parent})
	if err != nil {
		t.Fatalf("encodeBulkBody: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	// 父 action+doc + 子 action+doc = 4 行。
	if len(lines) != 4 {
		t.Fatalf("want 4 NDJSON lines (parent+child action/doc), got %d:\n%s", len(lines), string(body))
	}
	if !strings.Contains(lines[0], `"_id":"500"`) {
		t.Fatalf("line0 want parent action _id=500, got %q", lines[0])
	}
	if !strings.Contains(lines[2], `"_id":"500-rt0"`) {
		t.Fatalf("line2 want child action _id=500-rt0, got %q", lines[2])
	}
	// 子文档行带 virtual=true。
	if !bytes.Contains([]byte(lines[3]), []byte(`"virtual":true`)) {
		t.Fatalf("child doc line must carry virtual:true, got %q", lines[3])
	}
}

// TestMapBulkResults_ParentChildOnlyParentReported 父+N子混在响应里，结果只返回父；
// 子全 OK → 父 OK；任一子失败 → 父被判失败（父子原子）。
func TestMapBulkResults_ParentChildOnlyParentReported(t *testing.T) {
	raw := richTextRaw(`[{"type":"image","url":"http://x/a.png"},{"type":"file","url":"http://x/b.pdf"}]`)
	parent, err := DocFromMessage(branchAMsg("700", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(parent.Derivatives) != 2 {
		t.Fatalf("setup: want 2 derivatives, got %d", len(parent.Derivatives))
	}

	// 全成功：响应 3 项（父+2子）都 201。
	respAllOK := mkBulkResp(201, 201, 201)
	out := mapBulkResults([]Doc{parent}, respAllOK)
	if len(out) != 1 {
		t.Fatalf("result slice must be parent-count (1), got %d", len(out))
	}
	if !out[0].OK {
		t.Fatalf("all-ok bulk: parent must be OK, got %+v", out[0])
	}

	// 子失败（第 2 子 = 第 3 项 400）：父被判失败。
	respChildFail := mkBulkResp(201, 201, 400)
	out2 := mapBulkResults([]Doc{parent}, respChildFail)
	if out2[0].OK {
		t.Fatalf("child-failed bulk: parent must be reported failed (atomic), got OK")
	}
	if out2[0].Status != 400 {
		t.Fatalf("child-failed bulk: parent status want 400 (lifted from child), got %d", out2[0].Status)
	}
}

// TestSubSeq_NormalDocSerializedExplicitly 普通 doc subSeq=0 必须显式序列化出来
// （去 omitempty 的关键意图：reader 不赌「缺失=0」）。
func TestSubSeq_NormalDocSerializedExplicitly(t *testing.T) {
	raw := `{"type":2,"url":"http://x/a.png","name":"a.png"}`
	d, err := DocFromMessage(branchAMsg("900", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(b, []byte(`"subSeq":0`)) {
		t.Fatalf("normal doc must serialize subSeq:0 explicitly (no omitempty), got %s", b)
	}
}

// mkBulkResp 构造一个 _bulk 响应（每个 status 一项 index action）。
func mkBulkResp(statuses ...int) *opensearchapi.BulkResp {
	resp := &opensearchapi.BulkResp{}
	for _, st := range statuses {
		item := opensearchapi.BulkRespItem{Status: st}
		resp.Items = append(resp.Items, map[string]opensearchapi.BulkRespItem{"index": item})
	}
	return resp
}
