package fibrecapture

import (
	"container/heap"
	"sort"
	"time"
)

const (
	maxDeadlineMargin = 10 * time.Minute
	retryEvery        = 30 * time.Minute
	attemptBase       = 90 * time.Second
	attemptPerSecond  = 2 << 20 // bytes of blob per extra second of timeout
	maxAttemptTimeout = 10 * time.Minute
)

// retryDelays are the offsets from scan time of the first attempts; after
// the list, attempts follow every retryEvery.
var retryDelays = []time.Duration{0, time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute}

// Deadline is the last moment a download may start: shards are pruned at
// blockTime + retention, and the margin min(10m, retention/4) leaves room for
// the attempt itself.
func Deadline(blockTime time.Time, retention time.Duration) time.Time {
	return blockTime.Add(retention - min(maxDeadlineMargin, retention/4))
}

// NextAttempt is when attempt number attempts (0-based) may start. The delays
// are cumulative offsets from the scan time, not from the previous attempt:
// +0, +1m, +2m, +5m, +10m, +20m, +30m, then +60m, +90m, … every 30m.
func NextAttempt(scanned time.Time, attempts int) time.Time {
	attempts = max(attempts, 0)
	if attempts < len(retryDelays) {
		return scanned.Add(retryDelays[attempts])
	}
	last := retryDelays[len(retryDelays)-1]
	return scanned.Add(last + time.Duration(attempts-len(retryDelays)+1)*retryEvery)
}

// AttemptTimeout bounds one download: 90s plus one second per whole 2 MiB of
// blob, capped at 10m.
func AttemptTimeout(blobSize uint32) time.Duration {
	return min(attemptBase+time.Duration(blobSize/attemptPerSecond)*time.Second, maxAttemptTimeout)
}

// Queue holds pending entries as a min-heap by NextAttempt, ties broken by
// Deadline and then record key. It is not safe for concurrent use.
type Queue struct {
	h entryHeap
}

// Push adds e to the queue.
func (q *Queue) Push(e *Entry) { heap.Push(&q.h, e) }

// Len is the number of queued entries.
func (q *Queue) Len() int { return len(q.h) }

// Due removes and returns the earliest entry with NextAttempt ≤ now and
// now < Deadline, or nil. Entries past their deadline stay queued for
// Expired. The caller pushes the entry back if the attempt is retried.
func (q *Queue) Due(now time.Time) *Entry {
	var skipped []*Entry
	defer func() {
		for _, e := range skipped {
			heap.Push(&q.h, e)
		}
	}()
	for len(q.h) > 0 {
		top := q.h[0]
		if top.NextAttempt.After(now) {
			return nil
		}
		e := heap.Pop(&q.h).(*Entry)
		if now.Before(e.Deadline) {
			return e
		}
		skipped = append(skipped, e)
	}
	return nil
}

// Expired removes and returns every entry with now ≥ Deadline, ordered by
// deadline and then record key.
func (q *Queue) Expired(now time.Time) []*Entry {
	var out []*Entry
	kept := q.h[:0]
	for _, e := range q.h {
		if now.Before(e.Deadline) {
			kept = append(kept, e)
		} else {
			out = append(out, e)
		}
	}
	clear(q.h[len(kept):])
	q.h = kept
	if len(out) > 0 {
		heap.Init(&q.h)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Deadline.Equal(out[j].Deadline) {
			return out[i].Deadline.Before(out[j].Deadline)
		}
		return out[i].Key() < out[j].Key()
	})
	return out
}

// Remove deletes e from the queue; it reports whether e was queued.
func (q *Queue) Remove(e *Entry) bool {
	for i, x := range q.h {
		if x == e {
			heap.Remove(&q.h, i)
			return true
		}
	}
	return false
}

// NearestDeadline is the earliest deadline in the queue; false if empty.
func (q *Queue) NearestDeadline() (time.Time, bool) {
	if len(q.h) == 0 {
		return time.Time{}, false
	}
	d := q.h[0].Deadline
	for _, e := range q.h[1:] {
		if e.Deadline.Before(d) {
			d = e.Deadline
		}
	}
	return d, true
}

// Reschedule recomputes every deadline from the entry's block time after a
// shard_retention change.
func (q *Queue) Reschedule(retention time.Duration) {
	for _, e := range q.h {
		e.Deadline = Deadline(e.BlockTime, retention)
	}
	heap.Init(&q.h)
}

type entryHeap []*Entry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if !a.NextAttempt.Equal(b.NextAttempt) {
		return a.NextAttempt.Before(b.NextAttempt)
	}
	if !a.Deadline.Equal(b.Deadline) {
		return a.Deadline.Before(b.Deadline)
	}
	return a.Key() < b.Key()
}

func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x any) { *h = append(*h, x.(*Entry)) }

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}
