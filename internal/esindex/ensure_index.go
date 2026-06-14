package esindex

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// mappingJSON 是 octo-message 索引的规范 mapping + analyzer（中文分词），编译进二进制。
// 它是 octo-deployment 协调变更引用的单一真源——托管 OpenSearch 模板须与本文件同契约版本对齐。
//
//go:embed mapping/octo-message.json
var mappingJSON []byte

// IndexMappingJSON 返回内嵌的规范 mapping 字节（供 backfill / 部署校验复用）。
func IndexMappingJSON() []byte {
	out := make([]byte, len(mappingJSON))
	copy(out, mappingJSON)
	return out
}

// EnsureIndex 幂等创建目标索引：若不存在则用内嵌 mapping 创建；已存在则不动（不覆盖既有
// mapping，避免运行期破坏）。供 es-indexer 启动时与 backfill job 调用。
func (w *osWriter) EnsureIndex(ctx context.Context) error {
	return ensureIndex(ctx, w.client, w.index)
}

// ensureIndex 抽出便于测试（注入 mock transport 的 client）。
func ensureIndex(ctx context.Context, client *opensearchapi.Client, index string) error {
	resp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{index}})
	// Exists 在 404 时返回 (resp, err)；据状态码判定，而非仅看 err。
	if resp != nil && resp.StatusCode == http.StatusOK {
		return nil // 已存在，不覆盖
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		// 非 200 / 非 404 的异常（如 5xx / 鉴权失败）：上报，不盲目建索引。
		return fmt.Errorf("esindex: index exists check for %q returned status %d: %w", index, resp.StatusCode, err)
	}
	// 404（或无 resp 但 err 提示不存在）→ 创建。
	if _, cerr := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: index,
		Body:  bytes.NewReader(mappingJSON),
	}); cerr != nil {
		return fmt.Errorf("esindex: create index %q: %w", index, cerr)
	}
	return nil
}
