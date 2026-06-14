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
//
// 本文件目前是阶段 4 施工前的骨架（stub），仅建立可复用写入器的包边界与签名，
// 保证 `go build ./...` 通过。真实 OpenSearch bulk + 中文分词索引落在阶段 4。
package esindex

import (
	"context"
	"errors"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// ErrNotImplemented 标记阶段 4 待实现的写入路径。骨架阶段返回该错误以避免
// 静默成功（false-green）。
var ErrNotImplemented = errors.New("esindex: writer not implemented (phase 4)")

// Writer 把检索契约消息批量幂等写入 ES/OpenSearch。
//
// 实现须满足（阶段 4）：
//   - bulk upsert，doc _id = msg.MessageID（幂等 sink）。
//   - 逐项回报 per-item status，便于 consumer 区分 transient(429/5xx 重试) 与
//     permanent(4xx 进 DLQ)，并据「连续成功前缀」推进 offset（C4）。
//   - 中文分词由索引 mapping/analyzer 承担（mapping 配置在 octo-deployment 协调变更）。
type Writer interface {
	// Bulk 幂等写入一批消息，返回与入参等长的 per-item 结果切片。
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
	// Status 是底层 ES 返回的 HTTP 状态码（成功为 2xx）。
	Status int
	// Err 携带写入失败的原因（OK=true 时为 nil）。
	Err error
}

// Config 是 Writer 的构造配置（骨架占位，阶段 4 补全 endpoint/index/auth/分词等）。
type Config struct {
	// Addresses 是 OpenSearch/ES 节点地址列表。
	Addresses []string
	// Index 是目标索引名。
	Index string
}

// NewWriter 构造一个 ES 写入器。阶段 4 前为骨架：返回 ErrNotImplemented。
func NewWriter(_ Config) (Writer, error) {
	return nil, ErrNotImplemented
}
