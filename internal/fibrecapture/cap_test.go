package fibrecapture

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

func TestCapReservedBeforeDownloadAndSurvivesRestart(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)}

	// (a) concurrent reservations never overshoot the limit.
	c := NewCap(100, "2026-10-27", 0, clk.now)
	var ok atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.Reserve(20) {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 5 {
		t.Fatalf("(a) %d reservations succeeded, want 5", ok.Load())
	}

	// (b) Commit moves the reservation into the actual size.
	c.Commit(20, 7)
	for range 4 {
		c.Release(20)
	}
	if day, committed := c.Snapshot(); day != "2026-10-27" || committed != 7 {
		t.Fatalf("(b) Snapshot = %q,%d, want 2026-10-27,7", day, committed)
	}

	// (c) a restarted Cap from the snapshot keeps the committed bytes.
	day, committed := c.Snapshot()
	r := NewCap(100, day, committed, clk.now)
	if r.Reserve(94) {
		t.Fatal("(c) Reserve(94) accepted with 7 committed of 100")
	}
	if !r.Reserve(93) {
		t.Fatal("(c) Reserve(93) refused with 7 committed of 100")
	}

	// (d) committed resets at UTC midnight, whatever the clock's zone.
	msk := time.FixedZone("UTC+3", 3*3600)
	for _, loc := range []*time.Location{time.UTC, msk} {
		clk.set(time.Date(2026, 10, 27, 23, 59, 59, 0, time.UTC).In(loc))
		d := NewCap(100, "2026-10-27", 90, clk.now)
		if d.Reserve(11) {
			t.Fatalf("(d %s) Reserve(11) accepted before midnight", loc)
		}
		if !d.Reserve(5) {
			t.Fatalf("(d %s) Reserve(5) refused before midnight", loc)
		}
		if day, n := d.Snapshot(); day != "2026-10-27" || n != 90 {
			t.Fatalf("(d %s) Snapshot before midnight = %q,%d", loc, day, n)
		}
		clk.set(time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC).In(loc))
		// The new day resets committed and keeps the 5 reserved bytes.
		if d.Reserve(96) {
			t.Fatalf("(d %s) Reserve(96) accepted with 5 reserved after midnight", loc)
		}
		if !d.Reserve(95) {
			t.Fatalf("(d %s) Reserve(95) refused after midnight", loc)
		}
		if day, n := d.Snapshot(); day != "2026-10-28" || n != 0 {
			t.Fatalf("(d %s) Snapshot after midnight = %q,%d, want 2026-10-28,0", loc, day, n)
		}
	}

	// A snapshot from a past day does not count against today.
	clk.set(time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC))
	if !NewCap(100, "2026-10-27", 100, clk.now).Reserve(100) {
		t.Fatal("yesterday's committed bytes counted against today")
	}

	// (e) Release frees the reservation.
	e := NewCap(100, "2026-10-28", 0, clk.now)
	if !e.Reserve(60) || e.Reserve(60) {
		t.Fatal("(e) setup: want first Reserve(60) ok, second refused")
	}
	e.Release(60)
	if !e.Reserve(60) {
		t.Fatal("(e) Reserve(60) refused after Release")
	}
	if _, n := e.Snapshot(); n != 0 {
		t.Fatalf("(e) committed = %d after reserve/release only, want 0", n)
	}
}

func TestCapReserveNoOverflow(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)}
	c := NewCap(100, "2026-10-27", 50, clk.now)
	if c.Reserve(^uint64(0) - 10) {
		t.Fatal("Reserve accepted a size that wraps around uint64")
	}
}
