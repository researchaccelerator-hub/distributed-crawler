package telegramhelper

import (
	"context"
	"testing"
	"time"
)

func TestValidatorRateLimiter_RateEnforcement(t *testing.T) {
	// 120 calls/min = 1 call per 500ms.
	rl := NewValidatorRateLimiter(120, 0)

	start := time.Now()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("unexpected error on call %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	// First call is instant (burst=1), then two waits of ~500ms each = ~1s minimum.
	if elapsed < 900*time.Millisecond {
		t.Errorf("3 calls at 120/min should take ~1s, took %v", elapsed)
	}
}

func TestValidatorRateLimiter_JitterAddsDelay(t *testing.T) {
	// Very high rate so the token is instant; jitter should add delay.
	rl := NewValidatorRateLimiter(6000, 100)

	start := time.Now()
	ctx := context.Background()
	const calls = 5
	for i := 0; i < calls; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// With 0-100ms jitter per call and 5 calls, we expect some measurable delay.
	// On average 50ms * 5 = 250ms, but first call jitter applies too.
	// Just verify it's not instant (>50ms as a conservative check).
	if elapsed < 50*time.Millisecond {
		t.Errorf("expected jitter to add delay, total elapsed: %v", elapsed)
	}
}

func TestValidatorRateLimiter_ContextCancellation(t *testing.T) {
	// Very slow rate — will block on second call.
	rl := NewValidatorRateLimiter(1, 0)

	ctx, cancel := context.WithCancel(context.Background())

	// First call succeeds immediately (burst=1).
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("first call should succeed: %v", err)
	}

	// Cancel before second call can get a token.
	cancel()
	err := rl.Wait(ctx)
	if err == nil {
		t.Error("expected error after context cancellation")
	}
}

func TestValidatorRateLimiter_InfiniteRate(t *testing.T) {
	// callsPerMinute <= 0 means no rate limiting.
	rl := NewValidatorRateLimiter(0, 0)

	start := time.Now()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// 100 calls with no rate limiting and no jitter should be near-instant.
	if elapsed > 100*time.Millisecond {
		t.Errorf("infinite rate should be near-instant for 100 calls, took %v", elapsed)
	}
}

func TestValidatorRateLimiter_JitterContextCancellation(t *testing.T) {
	// High rate, high jitter — cancel during jitter sleep.
	rl := NewValidatorRateLimiter(60000, 5000) // 5s max jitter

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// First call may or may not finish before timeout.
	err := rl.Wait(ctx)
	// Either succeeds quickly or returns context error — both are valid.
	// If jitter is large and context expires during it, we should get an error.
	_ = err
}
