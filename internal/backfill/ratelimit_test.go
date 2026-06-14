package backfill

import (
	"context"
	"testing"
	"time"
)

// TestRateLimiter_Unlimited docsPerSec<=0 → 不阻塞。
func TestRateLimiter_Unlimited(t *testing.T) {
	rl := newRateLimiter(0, 100)
	if err := rl.Wait(context.Background(), 1000); err != nil {
		t.Fatalf("unlimited must not error: %v", err)
	}
}

// TestRateLimiter_Throttles 用假时钟验证：超出突发量需等待对应时长。
func TestRateLimiter_Throttles(t *testing.T) {
	now := time.Unix(0, 0)
	var slept time.Duration
	rl := newRateLimiter(1000, 1000) // 1000 docs/s, burst 1000
	rl.now = func() time.Time { return now }
	rl.sleep = func(_ context.Context, d time.Duration) error {
		slept += d
		now = now.Add(d) // 推进假时钟，模拟睡眠后令牌补充
		return nil
	}
	// 先耗尽突发 1000，再要 1000 → 需等约 1s。
	if err := rl.Wait(context.Background(), 1000); err != nil {
		t.Fatalf("wait1: %v", err)
	}
	if err := rl.Wait(context.Background(), 1000); err != nil {
		t.Fatalf("wait2: %v", err)
	}
	if slept < 900*time.Millisecond {
		t.Fatalf("expected ~1s throttle for 2nd batch, slept=%s", slept)
	}
}

// TestRateLimiter_CtxCancel ctx 取消 → Wait 返回错误。
func TestRateLimiter_CtxCancel(t *testing.T) {
	rl := newRateLimiter(1, 1) // 极慢，必然进入等待
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rl.Wait(ctx, 1000); err == nil {
		t.Fatalf("cancelled ctx must error")
	}
}
