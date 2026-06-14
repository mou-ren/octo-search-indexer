package esindex

import (
	"context"
	"errors"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// TestNewWriterStubReturnsNotImplemented 锁定骨架阶段语义：写入器尚未实现时
// 必须显式报错而非静默成功（防 false-green）。阶段 4 实现后替换该断言。
func TestNewWriterStubReturnsNotImplemented(t *testing.T) {
	w, err := NewWriter(Config{Addresses: []string{"http://127.0.0.1:9200"}, Index: "octo-message"})
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented, got w=%v err=%v", w, err)
	}
	if w != nil {
		t.Fatalf("expected nil writer in stub, got %v", w)
	}
}

// TestContractImportWired 确认本包能引用 octo-lib searchmsg 契约（编译期保障
// 阶段 4 写入器拿到的就是单一真源契约，避免字段错位静默吃数据）。
func TestContractImportWired(t *testing.T) {
	_ = searchmsg.Message{SchemaVersion: searchmsg.SchemaVersion}
	_ = context.Background()
}
