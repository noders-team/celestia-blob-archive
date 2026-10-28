package ingest

import (
	"context"
	"slices"
	"sync"
	"time"
)

const (
	// latencyWindow is the number of recent calls the p95 is computed over.
	latencyWindow = 100
	// growEvery is how long the limit must go without a halving to grow by one.
	growEvery = time.Minute
)

// Throttle is an AIMD limit on concurrent source calls: it halves when a call
// fails or the p95 latency of the last 100 calls exceeds the target, and grows
// by one after every full minute without a halving. The limit stays within
// [1, max]. The tail uses AcquireTail, which is served before waiting backfill
// callers, so a saturated backfill cannot starve it. It is safe for concurrent
// use.
type Throttle struct {
	target time.Duration
	max    int
	clock  func() time.Time

	mu          sync.Mutex
	limit       int
	inFlight    int
	lastChange  time.Time       // last halving, or last growth step
	lat         []time.Duration // ring buffer of recent latencies
	next        int             // ring position of the next sample
	wake        chan struct{}   // closed and replaced when capacity may have freed
	tailWaiting int             // AcquireTail callers waiting for a slot
}

// NewThrottle returns a throttle starting at initial, clamped to [1, maxLimit].
// clock may be nil for time.Now.
func NewThrottle(initial, maxLimit int, target time.Duration, clock func() time.Time) *Throttle {
	if clock == nil {
		clock = time.Now
	}
	maxLimit = max(maxLimit, 1)
	return &Throttle{
		target:     target,
		max:        maxLimit,
		clock:      clock,
		limit:      min(max(initial, 1), maxLimit),
		lastChange: clock(),
		lat:        make([]time.Duration, 0, latencyWindow),
		wake:       make(chan struct{}),
	}
}

// Limit returns the current concurrency limit.
func (t *Throttle) Limit() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.limit
}

// Acquire blocks until a call slot is free or ctx is done. It yields to
// AcquireTail: while a tail caller waits, Acquire does not take a slot.
func (t *Throttle) Acquire(ctx context.Context) error { return t.acquire(ctx, false) }

// AcquireTail is Acquire for the tail. It takes the next free slot ahead of
// any waiting Acquire caller, so the tail is never starved by queued backfill.
// It does not exceed the limit: it may still wait for an in-flight call.
func (t *Throttle) AcquireTail(ctx context.Context) error { return t.acquire(ctx, true) }

func (t *Throttle) acquire(ctx context.Context, tail bool) error {
	t.mu.Lock()
	if tail {
		t.tailWaiting++
	}
	for {
		t.growLocked()
		if t.inFlight < t.limit && (tail || t.tailWaiting == 0) {
			t.inFlight++
			if tail {
				t.leaveTailLocked()
			}
			t.mu.Unlock()
			return nil
		}
		wake := t.wake
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			t.mu.Lock()
			if tail {
				t.leaveTailLocked()
			}
			t.mu.Unlock()
			return ctx.Err()
		case <-wake:
		}
		t.mu.Lock()
	}
}

// leaveTailLocked drops a waiting tail caller and wakes backfill callers that
// may have been holding back for it.
func (t *Throttle) leaveTailLocked() {
	t.tailWaiting--
	if t.tailWaiting == 0 {
		t.broadcastLocked()
	}
}

func (t *Throttle) broadcastLocked() {
	close(t.wake)
	t.wake = make(chan struct{})
}

// Release frees the slot taken by Acquire and records the call's outcome.
// failed must be true only for source failures, not for cancellation.
func (t *Throttle) Release(latency time.Duration, failed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight--
	if failed {
		t.halveLocked()
	} else {
		t.recordLocked(latency)
	}
	t.growLocked()
	t.broadcastLocked()
}

func (t *Throttle) recordLocked(d time.Duration) {
	if len(t.lat) < latencyWindow {
		t.lat = append(t.lat, d)
	} else {
		t.lat[t.next] = d
	}
	t.next = (t.next + 1) % latencyWindow
	if len(t.lat) == latencyWindow && t.p95Locked() > t.target {
		t.halveLocked()
	}
}

func (t *Throttle) p95Locked() time.Duration {
	s := slices.Clone(t.lat)
	slices.Sort(s)
	return s[(len(s)*95+99)/100-1]
}

// halveLocked halves the limit and restarts the latency window and the
// healthy-minute timer, so old samples cannot trigger another halving.
func (t *Throttle) halveLocked() {
	t.limit = max(t.limit/2, 1)
	t.lastChange = t.clock()
	t.lat = t.lat[:0]
	t.next = 0
}

func (t *Throttle) growLocked() {
	now := t.clock()
	if t.limit >= t.max {
		t.lastChange = now
		return
	}
	for t.limit < t.max && now.Sub(t.lastChange) >= growEvery {
		t.limit++
		t.lastChange = t.lastChange.Add(growEvery)
	}
}
