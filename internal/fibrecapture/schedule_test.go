package fibrecapture

import (
	"testing"
	"time"
)

func TestDeadline(t *testing.T) {
	block := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		retention time.Duration
		want      time.Duration
	}{
		{4 * time.Hour, 3*time.Hour + 50*time.Minute},
		{10 * time.Minute, 7*time.Minute + 30*time.Second},
		{40 * time.Minute, 30 * time.Minute}, // min(10m, 10m)
	} {
		if got := Deadline(block, tc.retention); !got.Equal(block.Add(tc.want)) {
			t.Errorf("Deadline(R=%v) = %v, want block+%v", tc.retention, got, tc.want)
		}
	}
}

func TestNextAttemptSchedule(t *testing.T) {
	scanned := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	want := []time.Duration{0, time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute,
		20 * time.Minute, 30 * time.Minute, 60 * time.Minute, 90 * time.Minute, 120 * time.Minute}
	for n, w := range want {
		if got := NextAttempt(scanned, n); !got.Equal(scanned.Add(w)) {
			t.Errorf("NextAttempt(%d) = +%v, want +%v", n, got.Sub(scanned), w)
		}
	}
	if got := NextAttempt(scanned, -1); !got.Equal(scanned) {
		t.Errorf("NextAttempt(-1) = +%v, want +0", got.Sub(scanned))
	}
}

func TestAttemptTimeout(t *testing.T) {
	for _, tc := range []struct {
		size uint32
		want time.Duration
	}{
		{256 << 10, 90 * time.Second},
		{128 << 20, 154 * time.Second},
		{1 << 30, 10 * time.Minute},    // 90s + 512s = 602s, capped
		{^uint32(0), 10 * time.Minute}, // cap holds at the type's max
	} {
		if got := AttemptTimeout(tc.size); got != tc.want {
			t.Errorf("AttemptTimeout(%d) = %v, want %v", tc.size, got, tc.want)
		}
	}
}

func queueEntry(height int64, block, scanned time.Time, retention time.Duration) *Entry {
	return &Entry{
		Item:        Item{Height: height, BlockTime: block},
		Deadline:    Deadline(block, retention),
		NextAttempt: NextAttempt(scanned, 0),
	}
}

func TestQueueDueAndExpired(t *testing.T) {
	t0 := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	const r = 40 * time.Minute // deadline = block + 30m
	var q Queue
	if _, ok := q.NearestDeadline(); ok {
		t.Fatal("empty queue reports a deadline")
	}
	if q.Due(t0) != nil {
		t.Fatal("empty queue returned a due entry")
	}

	old := queueEntry(1, t0.Add(-time.Hour), t0, r)       // deadline t0-30m: already expired
	a := queueEntry(2, t0, t0, r)                         // due at t0, deadline t0+30m
	b := queueEntry(3, t0.Add(5*time.Minute), t0, r)      // due at t0, deadline t0+35m
	later := queueEntry(4, t0, t0.Add(10*time.Minute), r) // due at t0+10m
	for _, e := range []*Entry{later, b, old, a} {
		q.Push(e)
	}
	if q.Len() != 4 {
		t.Fatalf("Len = %d, want 4", q.Len())
	}
	if d, ok := q.NearestDeadline(); !ok || !d.Equal(t0.Add(-30*time.Minute)) {
		t.Fatalf("NearestDeadline = %v,%v, want t0-30m", d, ok)
	}

	// The expired entry ties on NextAttempt and has the earliest deadline, yet
	// Due skips it; a and b tie on NextAttempt and a's deadline is earlier.
	if got := q.Due(t0); got != a {
		t.Fatalf("Due #1 = %+v, want height 2", got)
	}
	if got := q.Due(t0); got != b {
		t.Fatalf("Due #2 = %+v, want height 3", got)
	}
	if got := q.Due(t0); got != nil {
		t.Fatalf("Due #3 = height %d, want nil (only expired and future left)", got.Height)
	}
	if q.Len() != 2 {
		t.Fatalf("Len after Due = %d, want 2 (old and later stay queued)", q.Len())
	}

	exp := q.Expired(t0)
	if len(exp) != 1 || exp[0] != old {
		t.Fatalf("Expired = %v, want [old]", exp)
	}
	if exp := q.Expired(t0); len(exp) != 0 {
		t.Fatalf("second Expired = %d entries, want 0", len(exp))
	}
	if d, ok := q.NearestDeadline(); !ok || !d.Equal(t0.Add(30*time.Minute)) {
		t.Fatalf("NearestDeadline = %v, want t0+30m", d)
	}

	// Exactly at the deadline an entry is expired, never due.
	at := t0.Add(30 * time.Minute)
	if got := q.Due(at); got != nil {
		t.Fatalf("Due at deadline = height %d, want nil", got.Height)
	}
	if got := q.Due(at.Add(-time.Nanosecond)); got != later {
		t.Fatalf("Due just before deadline = %v, want later", got)
	}
	q.Push(later)

	// Retention shrinks to 20m: deadline = block + 15m.
	q.Reschedule(20 * time.Minute)
	if !later.Deadline.Equal(t0.Add(15 * time.Minute)) {
		t.Fatalf("rescheduled deadline = %v, want t0+15m", later.Deadline)
	}
	if d, _ := q.NearestDeadline(); !d.Equal(t0.Add(15 * time.Minute)) {
		t.Fatalf("NearestDeadline after Reschedule = %v, want t0+15m", d)
	}
	if got := q.Due(t0.Add(15 * time.Minute)); got != nil {
		t.Fatal("Due returned an entry past its rescheduled deadline")
	}
	if exp := q.Expired(t0.Add(15 * time.Minute)); len(exp) != 1 || exp[0] != later {
		t.Fatalf("Expired after Reschedule = %v, want [later]", exp)
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}
}
