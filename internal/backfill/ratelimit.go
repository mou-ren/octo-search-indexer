package backfill

import (
	"context"
	"math/rand"
	"time"
)

// randInt63n 是 full-jitter 取样的随机源（非安全用途）。
func randInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int63n(n) //nolint:gosec // 抖动非安全用途
}

// rateLimiter 限制 backfill 的文档摄入速率（阶段 6 (b) 限速，默认 ≤5k docs/s），
// 避免压垮 ES / 源 DB。简单令牌桶：按 docs/s 匀速补充令牌，每写一批前等到足够令牌。
//
// docsPerSec<=0 表示不限速。
type rateLimiter struct {
	docsPerSec float64
	burst      float64

	tokens float64
	last   time.Time

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// newRateLimiter 构造。burst 为令牌桶容量（允许的瞬时突发量），<=0 时取 1 秒的量。
func newRateLimiter(docsPerSec float64, burst int) *rateLimiter {
	b := float64(burst)
	if b <= 0 {
		b = docsPerSec
	}
	if b <= 0 {
		b = 1
	}
	return &rateLimiter{
		docsPerSec: docsPerSec,
		burst:      b,
		tokens:     b,
		last:       time.Now(),
		now:        time.Now,
		sleep:      sleepCtx,
	}
}

// Wait 阻塞直到可以摄入 n 个文档（消耗 n 个令牌）。不限速时立即返回。
// ctx 取消时返回 ctx.Err()。
func (r *rateLimiter) Wait(ctx context.Context, n int) error {
	if r.docsPerSec <= 0 || n <= 0 {
		return ctx.Err()
	}
	need := float64(n)
	for {
		now := r.now()
		elapsed := now.Sub(r.last).Seconds()
		r.last = now
		r.tokens += elapsed * r.docsPerSec
		if r.tokens > r.burst {
			r.tokens = r.burst
		}
		if r.tokens >= need {
			r.tokens -= need
			return ctx.Err()
		}
		// 还差 (need-tokens) 个令牌，按补充速率算需等待的时长。
		deficit := need - r.tokens
		wait := time.Duration(deficit / r.docsPerSec * float64(time.Second))
		if wait <= 0 {
			wait = time.Millisecond
		}
		if err := r.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// sleepCtx 在 ctx 取消时尽早返回（与 consumer.sleepCtx 同语义；本包独立一份避免跨包耦合）。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// maxBackoff 限制单次退避上限。
const maxBackoff = 30 * time.Second

// expJitterBackoff 计算第 attempt 次（1-based）指数退避 + 满抖动时长（与 consumer 同语义；
// 本包独立一份避免跨包耦合）。base*2^(attempt-1) 截到 maxBackoff，再在 [0, d] 上随机取样。
func expJitterBackoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	return time.Duration(randInt63n(int64(d) + 1))
}
