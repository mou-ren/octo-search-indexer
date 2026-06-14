// Package esindex 是 es-indexer 的可复用写入器包：把 octo-im 消息检索契约
// (octo-lib contract/searchmsg.Message) 批量写入 OpenSearch/ES。
//
// 设计纪律（YUJ-4530 v4 / YUJ-4534）：
//   - 写入器与 Kafka consumer **解耦**。consumer（cmd/es-indexer）负责拉取/offset
//     提交/DLQ 路由；本包只负责「一批契约消息 → ES bulk upsert」。这样阶段 6 的
//     存量 backfill job 可以直接 import 本包复用同一套写入逻辑（读 message 表 →
//     转契约 → Writer.Bulk），无需复制 ES 写入代码。
//   - ES doc `_id = MessageID`（= Kafka key），保证 at-least-once 上线下的
//     effectively-once：重复投递走 upsert 幂等，不产生重复文档。
//   - 撤回/删除态**绝不**写入 ES（路线甲：读时回 MySQL join 过滤）。本包只索引
//     正文 + 查询侧鉴权所需可见性字段，与 searchmsg.Message 契约一致。
package esindex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// Writer 把检索契约消息批量幂等写入 ES/OpenSearch。
//
// 实现须满足：
//   - bulk upsert（index 动作，幂等），doc _id = msg.MessageID（幂等 sink）。
//   - 逐项回报 per-item status，便于 consumer 区分 transient(429/5xx 重试) 与
//     permanent(4xx 进 DLQ)，并据「连续成功前缀」推进 offset（C4）。
//   - 中文分词由索引 mapping/analyzer 承担（mapping 配置在 octo-deployment 协调变更）。
type Writer interface {
	// EnsureIndex 幂等创建目标索引（不存在则用内嵌 mapping 创建，已存在则不动）。
	// 供服务启动与 backfill job 复用同一套 mapping bootstrap。
	EnsureIndex(ctx context.Context) error
	// Bulk 幂等写入一批消息，返回与入参等长、顺序对齐的 per-item 结果切片。
	Bulk(ctx context.Context, msgs []searchmsg.Message) ([]BulkItemResult, error)
	// Close 释放底层 ES 客户端资源。
	Close() error
}

// BulkItemResult 是单条文档的写入结果，供 consumer 做 offset / DLQ 决策。
type BulkItemResult struct {
	// MessageID 对应 searchmsg.Message.MessageID（= ES _id）。
	MessageID string
	// OK 为 true 表示该文档已成功 upsert。
	OK bool
	// Status 是底层 ES 返回的 HTTP 状态码（成功为 2xx）。批级失败时为 0。
	Status int
	// Err 携带写入失败的原因（OK=true 时为 nil）。
	Err error
}

// Permanent 报告该条失败是否为永久错误（4xx，除 429）——consumer 据此进 DLQ；
// 否则为 transient（429/5xx/网络），consumer 原地退避重试、不推进 offset。
func (r BulkItemResult) Permanent() bool {
	return isPermanentStatus(r.Status)
}

// isPermanentStatus 判定 HTTP 状态码是否为「永久错误」（毒丸，进 DLQ）。
// 4xx 表示请求本身有问题（如 mapping 冲突、文档格式非法），重试无意义 → permanent。
// 例外：429 Too Many Requests 是限流，属 transient（退避重试）。
// 5xx / 0（网络/批级失败）属 transient。
func isPermanentStatus(status int) bool {
	if status == http.StatusTooManyRequests { // 429
		return false
	}
	return status >= 400 && status < 500
}

// Config 是 Writer 的构造配置。
type Config struct {
	// Addresses 是 OpenSearch/ES 节点地址列表。
	Addresses []string
	// Index 是目标索引名。
	Index string
	// Username/Password 为 HTTP Basic 认证（可空）。
	Username string
	Password string
	// Transport 可选：注入自定义 http.RoundTripper（测试用 mock；生产留空走默认）。
	Transport http.RoundTripper
}

// osWriter 是基于 opensearch-go v3 的 Writer 实现。
type osWriter struct {
	client *opensearchapi.Client
	index  string
}

// NewWriter 构造一个 OpenSearch 写入器。
func NewWriter(cfg Config) (Writer, error) {
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("esindex: at least one OpenSearch address is required")
	}
	if cfg.Index == "" {
		return nil, errors.New("esindex: target index is required")
	}
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: cfg.Addresses,
			Username:  cfg.Username,
			Password:  cfg.Password,
			Transport: cfg.Transport,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("esindex: new opensearch client: %w", err)
	}
	return &osWriter{client: client, index: cfg.Index}, nil
}

// bulkActionLine / bulkDocLine 构成 _bulk NDJSON 的每条两行：动作行 + 文档行。
// 用 index 动作（非 create）实现 upsert：同 _id 重复投递覆盖同一 doc，幂等去重。
type bulkActionLine struct {
	Index struct {
		ID string `json:"_id"`
	} `json:"index"`
}

// Bulk 把一批契约消息以 _bulk 幂等写入 ES。返回与 msgs 等长、按 MessageID 对齐的结果。
//
// 错误语义：
//   - 批级失败（网络/非 2xx 整体响应）→ 返回 error，且每条结果 OK=false、Status=0（transient，
//     consumer 整批退避重试不推进 offset）。
//   - 批级成功但部分条目失败 → error=nil，逐条 OK/Status/Err 反映 per-item 状态，consumer 据此
//     按「连续成功前缀」推进 offset、permanent 条进 DLQ。
func (w *osWriter) Bulk(ctx context.Context, msgs []searchmsg.Message) ([]BulkItemResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	body, err := encodeBulkBody(msgs)
	if err != nil {
		return nil, err
	}

	resp, err := w.client.Bulk(ctx, opensearchapi.BulkReq{
		Index: w.index,
		Body:  bytes.NewReader(body),
	})
	if err != nil {
		// 批级失败（网络/超时/非 2xx）：全部标 transient，让 consumer 整批退避重试。
		return batchFailure(msgs, err), fmt.Errorf("esindex: bulk request failed: %w", err)
	}

	return mapBulkResults(msgs, resp), nil
}

// encodeBulkBody 把消息序列化为 _bulk NDJSON（每条：index 动作行 + 文档行）。
func encodeBulkBody(msgs []searchmsg.Message) ([]byte, error) {
	var buf bytes.Buffer
	for i := range msgs {
		var action bulkActionLine
		action.Index.ID = msgs[i].MessageID
		actionJSON, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("esindex: marshal bulk action for %s: %w", msgs[i].MessageID, err)
		}
		docJSON, err := json.Marshal(msgs[i])
		if err != nil {
			return nil, fmt.Errorf("esindex: marshal doc for %s: %w", msgs[i].MessageID, err)
		}
		buf.Write(actionJSON)
		buf.WriteByte('\n')
		buf.Write(docJSON)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// batchFailure 在批级失败时把每条标为 transient（Status=0, OK=false）。
func batchFailure(msgs []searchmsg.Message, err error) []BulkItemResult {
	out := make([]BulkItemResult, len(msgs))
	for i := range msgs {
		out[i] = BulkItemResult{MessageID: msgs[i].MessageID, OK: false, Status: 0, Err: err}
	}
	return out
}

// mapBulkResults 把 _bulk 响应逐项映射回结果切片（顺序与入参一致）。
// _bulk 响应 Items 顺序与请求顺序一致，按下标对齐；防御性校验长度与 _id。
func mapBulkResults(msgs []searchmsg.Message, resp *opensearchapi.BulkResp) []BulkItemResult {
	out := make([]BulkItemResult, len(msgs))
	for i := range msgs {
		res := BulkItemResult{MessageID: msgs[i].MessageID}
		if i >= len(resp.Items) {
			// 响应条目缺失（理论上不应发生）：标 transient 让整批重试，绝不静默当成功。
			res.OK = false
			res.Status = 0
			res.Err = fmt.Errorf("esindex: missing bulk response item for %s", msgs[i].MessageID)
			out[i] = res
			continue
		}
		item, ok := resp.Items[i]["index"]
		if !ok {
			res.OK = false
			res.Status = 0
			res.Err = fmt.Errorf("esindex: bulk response item for %s has no index action", msgs[i].MessageID)
			out[i] = res
			continue
		}
		res.Status = item.Status
		if item.Status >= 200 && item.Status < 300 {
			res.OK = true
		} else {
			res.OK = false
			if item.Error != nil {
				res.Err = fmt.Errorf("esindex: bulk item %s status=%d type=%s reason=%s",
					msgs[i].MessageID, item.Status, item.Error.Type, item.Error.Reason)
			} else {
				res.Err = fmt.Errorf("esindex: bulk item %s status=%d", msgs[i].MessageID, item.Status)
			}
		}
		out[i] = res
	}
	return out
}

// Close 释放底层 ES 客户端资源。opensearch-go 客户端无显式 Close；预留接口对齐。
func (w *osWriter) Close() error {
	return nil
}
