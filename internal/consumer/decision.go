// Package consumer 实现 es-indexer 的 Kafka 消费侧（YUJ-4534 阶段 4，硬条件 C4）：
// FetchMessage 手动提交（禁 ReadMessage 自动提交）→ esindex.Writer bulk → 按「每分区连续成功
// 前缀」推进 offset；transient(429/5xx) **原地重试同一批不再拉新 offset**；permanent(4xx) 进 DLQ；
// 未知 schema_version 进 DLQ；DLQ 写自身 transient 失败有终态逃逸（不允许前缀永久卡死）。
//
// 本文件是**纯决策逻辑**（无 IO），把处置归类与「每分区连续成功前缀提交点」算清楚，便于单测穷举 C4 分支。
package consumer

import (
	"sort"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// itemDisposition 是单条消息经 bulk（+ DLQ 路由）后的最终处置类别。
type itemDisposition int

const (
	// dispOK 成功写入 ES。
	dispOK itemDisposition = iota
	// dispDLQResolved 永久失败（4xx 毒丸 / 未知 schema_version）且已成功落 DLQ（或终态逃逸已落地）
	// → 视为「已处理」，offset 可越过。
	dispDLQResolved
	// dispTransient 暂时失败（429/5xx/网络/批级失败）→ 原地重试，offset 不越过此条。
	dispTransient
)

// classifyBulk 把单条 bulk 结果（或预判的 schema 错误）归类为「初判」处置。
// schemaInvalid=true 表示 schema_version 校验未过（permanent，需进 DLQ）；此时 result 被忽略。
// 返回 (ok, permanent)：permanent=true 表示毒丸需走 DLQ 路由；ok=true 表示成功；二者皆 false=transient。
func classifyBulk(schemaInvalid bool, result esindex.BulkItemResult) (ok bool, permanent bool) {
	if schemaInvalid {
		return false, true
	}
	if result.OK {
		return true, false
	}
	if result.Permanent() {
		return false, true
	}
	return false, false
}

// hasTransient 报告 dispositions 中是否含 transient（需原地重试、本批不再拉新 offset）。
func hasTransient(dispositions []itemDisposition) bool {
	for _, disp := range dispositions {
		if disp == dispTransient {
			return true
		}
	}
	return false
}

// partitionCommitPoints 计算**每分区**「连续可越过前缀」的提交点（C4 顺序提交核心，多分区正确）。
//
// kafka offset 是 per-partition 的，消费组单 Reader 可同时被分配多个分区、FetchMessage 交错返回。
// 因此连续前缀必须**按分区**算：每个分区内按 offset 升序，从最低 offset 起，dispOK/dispDLQResolved
// 计入前缀，遇到第一个 dispTransient 立即停。返回每个分区前缀末的那条消息（供 CommitMessages 提交，
// kafka 单调高水位语义：commit 该条 = 确认该分区 0..该 offset）。某分区队首即 transient 则不产生
// 提交点（该分区不前进，靠原地重试）。
//
// 这杜绝「A 分区某 offset transient 未确认，却因 B 分区或更高 offset 成功 commit 把它隐式越过」造成丢消息。
func partitionCommitPoints(batch []fetchedMessage, dispositions []itemDisposition) []fetchedMessage {
	type pkey struct {
		topic     string
		partition int
	}
	idxByPart := make(map[pkey][]int)
	for i := range batch {
		k := pkey{batch[i].Topic, batch[i].Partition}
		idxByPart[k] = append(idxByPart[k], i)
	}

	var points []fetchedMessage
	for _, idxs := range idxByPart {
		// 按 offset 升序（FetchMessage 单分区本就有序，排序以防御乱序入参）。
		sort.Slice(idxs, func(a, b int) bool {
			return batch[idxs[a]].Offset < batch[idxs[b]].Offset
		})
		lastPrefix := -1
		for _, i := range idxs {
			if dispositions[i] == dispTransient {
				break // 该分区前缀在首个 transient 处截断
			}
			lastPrefix = i
		}
		if lastPrefix >= 0 {
			points = append(points, batch[lastPrefix])
		}
	}
	return points
}
