// Command es-indexer 是 octo-im 消息检索管线的独立索引器服务（YUJ-4530 v4 / YUJ-4534
// 阶段 4）。它消费 Kafka topic `octo.message.v1`（契约 octo-lib contract/searchmsg），
// 经可复用写入器 internal/esindex 幂等 bulk 写入 OpenSearch（doc _id = message_id）。
//
// 在 9 阶段管线中的位置：
//
//	message 5 分表 → searchetl(producer, octo-server) → Kafka octo.message.v1
//	  → 【es-indexer 本服务: consumer + bulk + 中文分词】 → OpenSearch
//	  → 读路径(octo-server 查询侧 join 撤回/删除过滤 + 鉴权 fail-CLOSED)
//
// 设计纪律：
//   - consumer（offset 提交/DLQ 路由）与写入器（internal/esindex.Writer）解耦，
//     以便阶段 6 backfill job 复用同一写入器。
//   - schema_version 校验：收到未知契约版本进 DLQ，不静默吃。
//   - offset 仅推进到「连续成功前缀」；transient(429/5xx) 退避重试，permanent(4xx)
//     进 DLQ（C4）。
//
// 本文件目前是阶段 4 施工前的入口骨架（stub），仅建立 binary 边界与优雅退出，
// 保证 `go build ./...` 通过。真实 Kafka consumer + bulk 落在阶段 4。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Printf("es-indexer starting (scaffold; phase 4 implements Kafka consumer + ES bulk writer)")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Printf("es-indexer exited with error: %v", err)
		os.Exit(1)
	}
	log.Printf("es-indexer stopped")
}

// run 是服务主循环挂点。骨架阶段仅阻塞到收到终止信号后干净退出；阶段 4 在此
// 启动 Kafka consumer、构造 esindex.Writer、起 /metrics 抓取端点。
func run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
