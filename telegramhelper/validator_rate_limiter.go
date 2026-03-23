package telegramhelper

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

// ValidatorRateLimiter controls the rate of HTTP requests to t.me for channel
// validation. Uses the same token-bucket + jitter pattern as the TDLib rate
// limiters, but is independent of any Telegram account.
type ValidatorRateLimiter struct {
	limiter     *rate.Limiter
	jitterMaxMs int
}

// NewValidatorRateLimiter creates a rate limiter for HTTP validation requests.
// callsPerMinute controls the sustained rate; jitterMaxMs adds a random delay
// (0–jitterMaxMs) after each token acquisition to reduce request fingerprinting.
func NewValidatorRateLimiter(callsPerMinute float64, jitterMaxMs int) *ValidatorRateLimiter {
	var lim rate.Limit
	if callsPerMinute <= 0 {
		lim = rate.Inf
	} else {
		lim = rate.Every(time.Duration(float64(time.Minute) / callsPerMinute))
	}
	return &ValidatorRateLimiter{
		limiter:     rate.NewLimiter(lim, 1),
		jitterMaxMs: jitterMaxMs,
	}
}

// Wait blocks until the rate limiter grants a token, then sleeps a random
// jitter. Returns ctx.Err() if the context is cancelled while waiting.
func (v *ValidatorRateLimiter) Wait(ctx context.Context) error {
	if err := v.limiter.Wait(ctx); err != nil {
		return err
	}
	jitterMs := 0
	if v.jitterMaxMs > 0 {
		jitterMs = rand.IntN(v.jitterMaxMs + 1)
	}
	log.Debug().Int("jitter_ms", jitterMs).Msg("validator-rate-limit: wait complete")
	if jitterMs > 0 {
		select {
		case <-time.After(time.Duration(jitterMs) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
