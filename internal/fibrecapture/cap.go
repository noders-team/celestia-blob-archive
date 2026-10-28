package fibrecapture

import (
	"sync"
	"time"
)

const dayLayout = "2006-01-02"

// Cap enforces the daily download budget. Bytes are reserved before a
// download starts, so concurrent workers never overshoot the limit; the
// committed bytes of the UTC day are persisted in State (Day, DayBytes),
// while reservations live only in memory.
type Cap struct {
	mu        sync.Mutex
	limit     uint64
	day       string // YYYY-MM-DD UTC
	committed uint64
	reserved  uint64
	now       func() time.Time
}

// NewCap restores a cap from a persisted snapshot. A day other than today's
// is rolled over (committed reset) on first use.
func NewCap(limit uint64, day string, committed uint64, now func() time.Time) *Cap {
	return &Cap{limit: limit, day: day, committed: committed, now: now}
}

// rollover resets committed when the UTC date of now() differs from day.
// Reservations are kept: they are for downloads already in flight.
func (c *Cap) rollover() {
	if today := c.now().UTC().Format(dayLayout); today != c.day {
		c.day = today
		c.committed = 0
	}
}

// Reserve claims n bytes of today's budget; it accepts iff
// committed + reserved + n ≤ limit.
func (c *Cap) Reserve(n uint64) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollover()
	used := c.committed + c.reserved
	if used < c.committed || used > c.limit || n > c.limit-used {
		return false
	}
	c.reserved += n
	return true
}

// Commit turns a reservation into actual committed bytes of today.
func (c *Cap) Commit(reserved, actual uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollover()
	c.reserved -= min(reserved, c.reserved)
	c.committed += actual
}

// Release returns n reserved bytes after a failed attempt.
func (c *Cap) Release(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reserved -= min(n, c.reserved)
}

// Snapshot returns today's date and committed bytes, for State.Day and
// State.DayBytes.
func (c *Cap) Snapshot() (day string, committed uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollover()
	return c.day, c.committed
}
