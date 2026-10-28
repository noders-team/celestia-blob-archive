package cachestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// memStore is an in-memory store.Store that counts reads per key.
type memStore struct {
	mu     sync.Mutex
	objs   map[string][]byte
	gets   map[string]int
	ranges map[string]int
	delay  time.Duration
	gate   chan struct{} // if set, Get waits for it to close
}

func newMem() *memStore {
	return &memStore{objs: map[string][]byte{}, gets: map[string]int{}, ranges: map[string]int{}}
}

func (m *memStore) set(key string, b []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = b
}

func (m *memStore) counts(key string) (gets, ranges int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gets[key], m.ranges[key]
}

func (m *memStore) Get(ctx context.Context, key string) ([]byte, error) {
	time.Sleep(m.delay)
	if m.gate != nil {
		<-m.gate
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets[key]++
	b, ok := m.objs[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(b), nil
}

func (m *memStore) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ranges[key]++
	b, ok := m.objs[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	if off < 0 || n <= 0 || off+n > int64(len(b)) {
		return nil, store.ErrOutOfRange
	}
	return bytes.Clone(b[off : off+n]), nil
}

func (m *memStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objs[key]
	return ok, nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *memStore) PutIfAbsent(context.Context, string, []byte, ...store.PutOptions) error {
	return errors.New("memStore: read-only")
}

func (m *memStore) Put(context.Context, string, []byte, ...store.PutOptions) error {
	return errors.New("memStore: read-only")
}

func (m *memStore) Delete(context.Context, string) error { return errors.New("memStore: read-only") }

// fakeClock is a settable clock for Options.Now.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func fixed(v int64) func() int64 { return func() int64 { return v } }

// dirBytes is a directory-sized object that starts with prefix.
func dirBytes(prefix string) []byte {
	b := make([]byte, dirSize)
	copy(b, prefix)
	return b
}

func mustRange(t *testing.T, st store.Store, key string, off, n int64) []byte {
	t.Helper()
	b, err := st.GetRange(context.Background(), key, off, n)
	if err != nil {
		t.Fatalf("GetRange(%s, %d, %d): %v", key, off, n, err)
	}
	return b
}

func TestCacheNeverHoldsSegOrStaleOpenBucket(t *testing.T) {
	ctx := context.Background()
	seg := archive.SegmentKey(1, 100)
	dir0 := archive.DirKey(0)
	t.Run("seg passes through", func(t *testing.T) {
		base := newMem()
		base.set(seg, bytes.Repeat([]byte{1}, 100))
		c := New(base, fixed(1<<40), Options{})
		mustRange(t, c, seg, 10, 20)
		mustRange(t, c, seg, 10, 20)
		if _, r := base.counts(seg); r != 2 {
			t.Fatalf("%d base range reads of the segment, want 2", r)
		}
		for _, k := range []string{archive.ManifestKey, "tombstones.json", "ns/x", "op/y"} {
			base.set(k, []byte("abcdef"))
			mustRange(t, c, k, 0, 3)
			mustRange(t, c, k, 0, 3)
			if _, r := base.counts(k); r != 2 {
				t.Fatalf("%s: %d base range reads, want 2", k, r)
			}
		}
	})
	t.Run("open bucket kept for OpenTTL only", func(t *testing.T) {
		base := newMem()
		clk := &fakeClock{t: time.Unix(1000, 0)}
		base.set(dir0, dirBytes("old"))
		c := New(base, fixed(9999), Options{Now: clk.now})
		if got := mustRange(t, c, dir0, 0, 3); string(got) != "old" {
			t.Fatalf("first read %q", got)
		}
		base.set(dir0, dirBytes("new"))
		clk.advance(5 * time.Second)
		if got := mustRange(t, c, dir0, 0, 3); string(got) != "old" {
			t.Fatalf("read after 5s %q, want old", got)
		}
		clk.advance(2 * time.Second)
		if got := mustRange(t, c, dir0, 0, 3); string(got) != "new" {
			t.Fatalf("read after 7s %q, want new", got)
		}
	})
	t.Run("unfrozen bucket never kept beyond OpenTTL", func(t *testing.T) {
		base := newMem()
		clk := &fakeClock{t: time.Unix(1000, 0)}
		base.set(dir0, dirBytes("v0"))
		var until atomic.Int64
		until.Store(9999) // (0+1)*10000 > 9999: open
		c := New(base, until.Load, Options{Now: clk.now})
		for i := 0; i < 5; i++ {
			mustRange(t, c, dir0, 0, 2)
			clk.advance(6*time.Second + time.Nanosecond)
		}
		if g, _ := base.counts(dir0); g != 5 {
			t.Fatalf("%d base Gets over 5 TTLs, want 5", g)
		}
		// The bucket freezes within the TTL of an open copy holding v0:
		// that copy is not reused.
		if got := mustRange(t, c, dir0, 0, 2); string(got) != "v0" {
			t.Fatalf("open read %q, want v0", got)
		}
		base.set(dir0, dirBytes("v1"))
		until.Store(10000)
		if got := mustRange(t, c, dir0, 0, 2); string(got) != "v1" {
			t.Fatalf("frozen read %q, want the bucket fetched after freezing", got)
		}
		if g, _ := base.counts(dir0); g != 7 {
			t.Fatalf("%d base Gets, want 7", g)
		}
	})
	t.Run("missing bucket retried after OpenTTL", func(t *testing.T) {
		base := newMem()
		clk := &fakeClock{t: time.Unix(1000, 0)}
		c := New(base, fixed(0), Options{Now: clk.now})
		if _, err := c.GetRange(ctx, dir0, 0, 2); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("missing bucket: %v", err)
		}
		base.set(dir0, dirBytes("he"))
		clk.advance(5 * time.Second)
		if _, err := c.GetRange(ctx, dir0, 0, 2); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("missing bucket within OpenTTL: %v", err)
		}
		clk.advance(2 * time.Second)
		if got := mustRange(t, c, dir0, 0, 2); string(got) != "he" {
			t.Fatalf("after OpenTTL %q", got)
		}
		if g, _ := base.counts(dir0); g != 2 {
			t.Fatalf("%d base Gets, want 2", g)
		}
	})
}

func TestFrozenBucketOutOfRange(t *testing.T) {
	base := newMem()
	dir0 := archive.DirKey(0)
	base.set(dir0, dirBytes(""))
	c := New(base, fixed(10000), Options{})
	if _, err := c.GetRange(context.Background(), dir0, dirSize-10, 24); !errors.Is(err, store.ErrOutOfRange) {
		t.Fatalf("range past the cached frozen bucket: %v", err)
	}
	if _, err := c.GetRange(context.Background(), dir0, -1, 4); !errors.Is(err, store.ErrOutOfRange) {
		t.Fatalf("negative offset: %v", err)
	}
	if got := mustRange(t, c, dir0, dirSize-24, 24); len(got) != 24 {
		t.Fatalf("got %d bytes", len(got))
	}
	if g, _ := base.counts(dir0); g != 1 {
		t.Fatalf("%d base Gets, want 1", g)
	}
}

// A dir object of any length but the fixed bucket size is served but never
// kept, open or frozen.
func TestWrongSizeDirNotCached(t *testing.T) {
	for _, size := range []int{100, dirSize - 1, dirSize + 1, 4 << 20} {
		for _, until := range []int64{0, 10000} {
			base := newMem()
			dir0 := archive.DirKey(0)
			base.set(dir0, make([]byte, size))
			c := New(base, fixed(until), Options{})
			for i := 0; i < 2; i++ {
				_, err := c.GetRange(context.Background(), dir0, 76, 24)
				if err != nil {
					t.Fatalf("size %d until %d: %v", size, until, err)
				}
			}
			if _, err := c.GetRange(context.Background(), dir0, int64(size)-10, 24); !errors.Is(err, store.ErrOutOfRange) {
				t.Fatalf("size %d until %d: range past the end: %v", size, until, err)
			}
			if g, _ := base.counts(dir0); g != 3 {
				t.Fatalf("size %d until %d: %d base Gets, want 3", size, until, g)
			}
			if b := c.Stats().Bytes; b != 0 {
				t.Fatalf("size %d until %d: cache holds %d bytes", size, until, b)
			}
		}
	}
}

// A caller that gives up does not fail the fetch it started for the others.
func TestCancelledCallerDoesNotFailFlight(t *testing.T) {
	base := newMem()
	base.gate = make(chan struct{})
	dir0 := archive.DirKey(0)
	base.set(dir0, dirBytes("data"))
	c := New(base, fixed(10000), Options{})

	ctxA, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.GetRange(ctxA, dir0, 0, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller A: %v, want context.Canceled", err)
	}
	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := c.GetRange(context.Background(), dir0, 0, 4)
		done <- result{b, err}
	}()
	// Let B join A's flight, still blocked on the gate, before releasing it.
	for c.Stats().Misses[ClassDirFrozen] < 2 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(base.gate)
	r := <-done
	if r.err != nil || string(r.b) != "data" {
		t.Fatalf("caller B: %q, %v", r.b, r.err)
	}
	if g, _ := base.counts(dir0); g != 1 {
		t.Fatalf("%d base Gets, want 1", g)
	}
}

func TestNewRejectsNilContiguousUntil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "contiguousUntil") {
			t.Fatalf("recovered %v, want a panic naming contiguousUntil", r)
		}
	}()
	New(newMem(), nil, Options{})
}

func TestFrozenBucketFetchedOnce(t *testing.T) {
	base := newMem()
	base.delay = 20 * time.Millisecond
	var es [archive.DirBucket]archive.DirEntry
	for i := range es {
		es[i] = archive.DirEntry{SegFirst: 1, SegLast: archive.DirBucket, RowStart: uint32(i), RowCount: 1}
	}
	base.set(archive.DirKey(0), archive.EncodeDir(0, &es))
	c := New(base, fixed(archive.DirBucket), Options{})

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(h int64) {
			defer wg.Done()
			e, err := archiveread.Locate(context.Background(), c, h)
			if err == nil && e.RowStart != uint32(h-1) {
				err = errors.New("wrong entry")
			}
			errs <- err
		}(int64(i*97 + 1))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if g, r := base.counts(archive.DirKey(0)); g != 1 || r != 0 {
		t.Fatalf("base Gets %d, range reads %d; want 1 and 0", g, r)
	}
	if s := c.Stats(); s.Misses["dir_frozen"] == 0 || s.Hits["dir_frozen"]+s.Misses["dir_frozen"] != 100 {
		t.Fatalf("stats %+v", s)
	}
}

func TestIdx2RangeCached(t *testing.T) {
	base := newMem()
	key := archive.IndexKey(1, 100)
	base.set(key, bytes.Repeat([]byte{7}, 4096))
	c := New(base, fixed(0), Options{MaxEntry: 1024})
	mustRange(t, c, key, 16, 512)
	mustRange(t, c, key, 16, 512)
	if _, r := base.counts(key); r != 1 {
		t.Fatalf("%d base range reads, want 1", r)
	}
	mustRange(t, c, key, 32, 512) // another range is another entry
	if _, r := base.counts(key); r != 2 {
		t.Fatalf("%d base range reads, want 2", r)
	}
	mustRange(t, c, key, 0, 2048) // above MaxEntry: never kept
	mustRange(t, c, key, 0, 2048)
	if _, r := base.counts(key); r != 4 {
		t.Fatalf("%d base range reads, want 4", r)
	}
	if _, err := c.GetRange(context.Background(), key, 4000, 512); !errors.Is(err, store.ErrOutOfRange) {
		t.Fatalf("range past the end: %v", err)
	}
	// Misses: the two cached ranges and the failed one.
	if s := c.Stats(); s.Hits["idx2"] != 1 || s.Misses["idx2"] != 3 {
		t.Fatalf("stats %+v", s)
	}
}

func TestEvictionBoundsBytes(t *testing.T) {
	const maxBytes = 8192
	base := newMem()
	key := archive.IndexKey(1, 100)
	base.set(key, make([]byte, 4*maxBytes))
	c := New(base, fixed(0), Options{MaxBytes: maxBytes, MaxEntry: 1024})
	for off := int64(0); off < 3*maxBytes; off += 512 {
		mustRange(t, c, key, off, 512)
		if b := c.Stats().Bytes; b > maxBytes {
			t.Fatalf("cache holds %d bytes, more than %d", b, maxBytes)
		}
	}
	if b := c.Stats().Bytes; b == 0 {
		t.Fatal("cache holds nothing")
	}
	// The most recent entry survived, the oldest did not.
	_, before := base.counts(key)
	mustRange(t, c, key, 3*maxBytes-512, 512)
	mustRange(t, c, key, 0, 512)
	if _, after := base.counts(key); after != before+1 {
		t.Fatalf("%d base range reads, want %d", after, before+1)
	}
}

func TestWritesReadOnly(t *testing.T) {
	c := New(newMem(), fixed(0), Options{})
	ctx := context.Background()
	if err := c.Put(ctx, "k", nil); !errors.Is(err, client.ErrReadOnly) {
		t.Fatalf("Put: %v", err)
	}
	if err := c.PutIfAbsent(ctx, "k", nil); !errors.Is(err, client.ErrReadOnly) {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if err := c.Delete(ctx, "k"); !errors.Is(err, client.ErrReadOnly) {
		t.Fatalf("Delete: %v", err)
	}
}

// rangeCounter counts range reads per key on top of a real store.
type rangeCounter struct {
	store.Store
	mu     sync.Mutex
	gets   int
	ranges []string
}

func (r *rangeCounter) Get(ctx context.Context, key string) ([]byte, error) {
	r.mu.Lock()
	r.gets++
	r.mu.Unlock()
	return r.Store.Get(ctx, key)
}

func (r *rangeCounter) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	r.mu.Lock()
	r.ranges = append(r.ranges, key)
	r.mu.Unlock()
	return r.Store.GetRange(ctx, key, off, n)
}

func TestArchivereadThroughCache(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
	archivetest.Build(t, fs, blocks, 1, 7, 3, nil)
	_, recs, err := archiveread.ReadHeight(ctx, fs, 3)
	if err != nil || len(recs) < 2 {
		t.Fatalf("ReadHeight: %d records, %v", len(recs), err)
	}
	b := recs[1].GetBlob()
	wantHR, wantRec, err := archiveread.ReadBlob(ctx, fs, 3, b.Namespace, b.Commitment)
	if err != nil {
		t.Fatal(err)
	}

	base := &rangeCounter{Store: fs}
	c := New(base, fixed(7), Options{})
	for i := 0; i < 2; i++ {
		base.mu.Lock()
		base.gets, base.ranges = 0, nil
		base.mu.Unlock()
		hr, rec, err := archiveread.ReadBlob(ctx, c, 3, b.Namespace, b.Commitment)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(hr, wantHR) || !proto.Equal(rec, wantRec) {
			t.Fatalf("call %d: result differs from reading without the cache", i)
		}
	}
	if base.gets != 0 || len(base.ranges) != 2 {
		t.Fatalf("second call: %d Gets, range reads %v; want only 2 .seg range reads", base.gets, base.ranges)
	}
	for _, k := range base.ranges {
		if !strings.HasSuffix(k, ".seg") {
			t.Fatalf("second call read %s from the base", k)
		}
	}
}
