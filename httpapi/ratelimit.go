package httpapi

import (
	"sync"
	"time"
)

// Per-tenant rate-limit defaults for POST /v1/actions. Deliberately generous —
// the real coalescing/budget lives downstream (controller + CloudPress); this is
// only a node-level abuse bound so one tenant cannot flood the shared control.db
// write path and starve the tenant-auth read path for every other tenant.
const (
	actionsBurst        = 20  // bucket capacity (tokens)
	actionsRefillPerSec = 2.0 // tokens/second (~120/min sustained)
)

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// rateLimiter is a per-key token-bucket limiter. Keys are project ids, so the
// bucket map is naturally bounded by the number of tenants provisioned on the
// node — no janitor/eviction goroutine is needed.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	burst   float64
	refill  float64 // tokens per second
}

func newRateLimiter(burst, refillPerSec float64) *rateLimiter {
	return &rateLimiter{
		buckets: make(map[string]*tokenBucket),
		burst:   burst,
		refill:  refillPerSec,
	}
}

// allow refills key's bucket by the elapsed time, then deducts one token.
// Returns false when the bucket is empty (the caller should answer 429).
func (rl *rateLimiter) allow(key string) bool {
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	b := rl.buckets[key]
	if b == nil {
		b = &tokenBucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(rl.burst, b.tokens+elapsed*rl.refill)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
