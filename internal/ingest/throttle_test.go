package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestThrottle(initial, max int) (*Throttle, *testClock) {
	c := newTestClock()
	return NewThrottle(initial, max, 100*time.Millisecond, c.Now), c
}

func call(t *testing.T, th *Throttle, lat time.Duration, failed bool) {
	t.Helper()
	if err := th.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	th.Release(lat, failed)
}

func TestThrottleHalvesOnErrorDownToOne(t *testing.T) {
	th, _ := newTestThrottle(8, 8)
	for _, want := range []int{4, 2, 1, 1} {
		call(t, th, time.Millisecond, true)
		if got := th.Limit(); got != want {
			t.Fatalf("limit %d, want %d", got, want)
		}
	}
}

func TestThrottleHalvesOnHighP95(t *testing.T) {
	th, _ := newTestThrottle(8, 8)
	// 95 fast + 5 slow: p95 is fast.
	for i := 0; i < 95; i++ {
		call(t, th, 10*time.Millisecond, false)
	}
	for i := 0; i < 5; i++ {
		call(t, th, time.Second, false)
	}
	if got := th.Limit(); got != 8 {
		t.Fatalf("limit %d after healthy p95, want 8", got)
	}
	// One more slow call pushes p95 of the last 100 over the target.
	call(t, th, time.Second, false)
	if got := th.Limit(); got != 4 {
		t.Fatalf("limit %d after slow p95, want 4", got)
	}
	// The window restarts after a halving: a few slow calls do not halve again.
	for i := 0; i < 10; i++ {
		call(t, th, time.Second, false)
	}
	if got := th.Limit(); got != 4 {
		t.Fatalf("limit %d, want 4 (window restarted)", got)
	}
}

func TestThrottleGrowsOnePerHealthyMinuteUpToMax(t *testing.T) {
	th, clk := newTestThrottle(2, 5)
	clk.Advance(59 * time.Second)
	call(t, th, time.Millisecond, false)
	if got := th.Limit(); got != 2 {
		t.Fatalf("limit %d before a full minute, want 2", got)
	}
	clk.Advance(time.Second)
	call(t, th, time.Millisecond, false)
	if got := th.Limit(); got != 3 {
		t.Fatalf("limit %d after one minute, want 3", got)
	}
	clk.Advance(150 * time.Second)
	call(t, th, time.Millisecond, false)
	if got := th.Limit(); got != 5 {
		t.Fatalf("limit %d after 3.5 minutes, want 5", got)
	}
	clk.Advance(time.Hour)
	call(t, th, time.Millisecond, false)
	if got := th.Limit(); got != 5 {
		t.Fatalf("limit %d, want capped at 5", got)
	}
	// A halving restarts the healthy-minute timer.
	clk.Advance(30 * time.Second)
	call(t, th, time.Millisecond, true)
	clk.Advance(59 * time.Second)
	call(t, th, time.Millisecond, false)
	if got := th.Limit(); got != 2 {
		t.Fatalf("limit %d, want 2 (timer restarted by halving)", got)
	}
	clk.Advance(time.Second)
	call(t, th, time.Millisecond, false)
	if got := th.Limit(); got != 3 {
		t.Fatalf("limit %d, want 3", got)
	}
}

func TestThrottleBoundsInitial(t *testing.T) {
	if got := NewThrottle(0, 4, time.Second, nil).Limit(); got != 1 {
		t.Fatalf("limit %d, want clamped to 1", got)
	}
	if got := NewThrottle(10, 4, time.Second, nil).Limit(); got != 4 {
		t.Fatalf("limit %d, want clamped to 4", got)
	}
}

func TestThrottleBlocksAtLimit(t *testing.T) {
	th, _ := newTestThrottle(1, 2)
	if err := th.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := th.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire at limit: %v, want deadline exceeded", err)
	}
	got := make(chan error, 1)
	go func() { got <- th.Acquire(context.Background()) }()
	th.Release(time.Millisecond, false)
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire not woken by Release")
	}
}

// With limit 1 and backfill callers queued, a tail caller gets the next free
// slot ahead of them.
func TestThrottleTailBeforeQueuedBackfill(t *testing.T) {
	for round := 0; round < 20; round++ {
		th, _ := newTestThrottle(1, 1)
		ctx := context.Background()
		if err := th.Acquire(ctx); err != nil { // backfill holds the only slot
			t.Fatal(err)
		}
		order := make(chan string, 6)
		for range 5 {
			go func() {
				if th.Acquire(ctx) == nil {
					order <- "backfill"
					th.Release(time.Millisecond, false)
				}
			}()
		}
		time.Sleep(5 * time.Millisecond) // let backfill callers queue
		go func() {
			if th.AcquireTail(ctx) == nil {
				order <- "tail"
				th.Release(time.Millisecond, false)
			}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			th.mu.Lock()
			w := th.tailWaiting
			th.mu.Unlock()
			if w == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("tail caller never queued")
			}
			time.Sleep(100 * time.Microsecond)
		}
		th.Release(time.Millisecond, false)
		if first := <-order; first != "tail" {
			t.Fatalf("round %d: %s got the slot before the tail", round, first)
		}
		for range 5 {
			<-order
		}
	}
}

func TestThrottleTailWaiterCancelUnblocksBackfill(t *testing.T) {
	th, _ := newTestThrottle(2, 2)
	ctx := context.Background()
	if err := th.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := th.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	tctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- th.AcquireTail(tctx) }()
	for {
		th.mu.Lock()
		w := th.tailWaiting
		th.mu.Unlock()
		if w == 1 {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireTail = %v", err)
	}
	th.Release(time.Millisecond, false)
	actx, acancel := context.WithTimeout(ctx, 2*time.Second)
	defer acancel()
	if err := th.Acquire(actx); err != nil {
		t.Fatalf("backfill blocked after tail waiter left: %v", err)
	}
}
