// Package cachestore is a bounded in-memory read cache over a store.Store for
// the read API. It keeps only objects that are safe to keep: frozen height
// directory buckets (immutable) without expiry, open buckets (rewritten in
// place by the ingester) for a short TTL, and .idx2 row ranges (immutable).
// Segments are rewritten in place by takedowns and everything else is
// mutable or not on the hot path, so all other reads pass through.
package cachestore

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// Cache classes, as reported in Stats.
const (
	ClassDirFrozen = "dir_frozen"
	ClassDirOpen   = "dir_open"
	ClassIdx2      = "idx2"
)

// entryOverhead is charged per entry on top of its bytes, so that entries
// without data (a cached missing bucket) still count against MaxBytes.
const entryOverhead = 128

// Options tune the cache; zero fields take the defaults.
type Options struct {
	// MaxBytes bounds the cached bytes (default 512 MiB).
	MaxBytes int64
	// OpenTTL is how long an open directory bucket, or its absence, is kept
	// (default 6 s).
	OpenTTL time.Duration
	// MaxEntry is the largest .idx2 range that is cached (default 1 MiB).
	MaxEntry int64
	// Now is the clock (default time.Now).
	Now func() time.Time
}

// Stats is a snapshot of the cache: the bytes it holds and hits and misses
// per class (dir_frozen, dir_open, idx2).
type Stats struct {
	Bytes        int64
	Hits, Misses map[string]uint64
}

type entry struct {
	key     string
	data    []byte
	missing bool      // the object did not exist
	expires time.Time // zero: kept until evicted
	size    int64
	noKeep  bool // served to the callers of this fetch, never cached
}

// dirSize is the length of every well-formed directory bucket.
const dirSize = archive.DirHeaderSize + archive.DirBucket*archive.DirEntrySize

// Store is a read-only store.Store that caches reads of its base.
type Store struct {
	base            store.Store
	contiguousUntil func() int64
	o               Options
	sf              singleflight.Group

	mu     sync.Mutex
	lru    *list.List // of *entry, most recent first
	items  map[string]*list.Element
	bytes  int64
	hits   map[string]uint64
	misses map[string]uint64
}

var _ store.Store = (*Store)(nil)

// New returns a cache over base. contiguousUntil reports the height up to
// which the archive has no gaps (manifest.contiguous_until); a directory
// bucket b is frozen, and kept without expiry, once (b+1)*10000 is at most
// that height. A nil base or contiguousUntil is a programming error and
// panics.
func New(base store.Store, contiguousUntil func() int64, o Options) *Store {
	if base == nil || contiguousUntil == nil {
		panic("cachestore: New needs a base store and a contiguousUntil func")
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = 512 << 20
	}
	if o.OpenTTL <= 0 {
		o.OpenTTL = 6 * time.Second
	}
	if o.MaxEntry <= 0 {
		o.MaxEntry = 1 << 20
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Store{
		base: base, contiguousUntil: contiguousUntil, o: o,
		lru: list.New(), items: map[string]*list.Element{},
		hits: map[string]uint64{}, misses: map[string]uint64{},
	}
}

// Stats returns a snapshot of the cache counters.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Bytes: s.bytes, Hits: maps.Clone(s.hits), Misses: maps.Clone(s.misses)}
}

// GetRange serves directory buckets and small .idx2 ranges from the cache and
// passes every other read through.
func (s *Store) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if b, ok := dirBucket(key); ok {
		return s.dirRange(ctx, key, b, off, n)
	}
	if strings.HasPrefix(key, "segments/") && strings.HasSuffix(key, ".idx2") && n > 0 && n <= s.o.MaxEntry {
		return s.idx2Range(ctx, key, off, n)
	}
	return s.base.GetRange(ctx, key, off, n)
}

// dirBucket returns the bucket of a key written by archive.DirKey.
func dirBucket(key string) (int64, bool) {
	num, ok := strings.CutPrefix(key, "dir/")
	if !ok {
		return 0, false
	}
	if num, ok = strings.CutSuffix(num, ".dir"); !ok {
		return 0, false
	}
	b, err := strconv.ParseInt(num, 10, 64)
	if err != nil || b < 0 || archive.DirKey(b) != key {
		return 0, false
	}
	return b, true
}

func (s *Store) dirRange(ctx context.Context, key string, b, off, n int64) ([]byte, error) {
	// A bucket cached while open may have been incomplete, so a frozen
	// bucket is a separate entry that is fetched again once.
	frozen := b < (1<<63-1)/archive.DirBucket && (b+1)*archive.DirBucket <= s.contiguousUntil()
	class, ck := ClassDirOpen, "o:"+key
	if frozen {
		class, ck = ClassDirFrozen, "f:"+key
	}
	e, err := s.load(ctx, class, ck, func(ctx context.Context) (*entry, error) {
		fetched := s.o.Now()
		data, err := s.base.Get(ctx, key)
		e := &entry{data: data}
		switch {
		case errors.Is(err, store.ErrNotFound):
			e.missing, e.data = true, nil
		case err != nil:
			return nil, err
		case len(data) != dirSize:
			// Not a bucket EncodeDir writes: serve it so the reader
			// reports it, but never keep it.
			e.noKeep = true
		}
		if !frozen || e.missing {
			e.expires = fetched.Add(s.o.OpenTTL)
		}
		return e, nil
	})
	if err != nil {
		return nil, err
	}
	if e.missing {
		return nil, fmt.Errorf("%s: %w", key, store.ErrNotFound)
	}
	if off < 0 || n <= 0 || off > int64(len(e.data))-n {
		return nil, fmt.Errorf("%s range [%d,+%d) of %d bytes: %w", key, off, n, len(e.data), store.ErrOutOfRange)
	}
	return bytes.Clone(e.data[off : off+n]), nil
}

func (s *Store) idx2Range(ctx context.Context, key string, off, n int64) ([]byte, error) {
	ck := fmt.Sprintf("i:%s@%d+%d", key, off, n)
	// Errors are not cached: an .idx2 that is missing now may be written
	// later, and an existing one never changes.
	e, err := s.load(ctx, ClassIdx2, ck, func(ctx context.Context) (*entry, error) {
		data, err := s.base.GetRange(ctx, key, off, n)
		if err != nil {
			return nil, err
		}
		return &entry{data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return bytes.Clone(e.data), nil
}

// load returns the live entry under ck or fetches it once for all
// concurrent callers. The fetch runs detached from the caller's
// cancellation (the base store bounds it with its own timeout) so that one
// caller giving up does not fail the others; each caller still returns when
// its own context is done.
func (s *Store) load(ctx context.Context, class, ck string, fetch func(context.Context) (*entry, error)) (*entry, error) {
	s.mu.Lock()
	if el, ok := s.items[ck]; ok {
		e := el.Value.(*entry)
		if e.expires.IsZero() || s.o.Now().Before(e.expires) {
			s.lru.MoveToFront(el)
			s.hits[class]++
			s.mu.Unlock()
			return e, nil
		}
		s.removeLocked(el)
	}
	s.misses[class]++
	s.mu.Unlock()

	detached := context.WithoutCancel(ctx)
	ch := s.sf.DoChan(ck, func() (any, error) {
		e, err := fetch(detached)
		if err != nil {
			return nil, err
		}
		e.key = ck
		s.add(e)
		return e, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*entry), nil
	}
}

// add inserts e and evicts least recently used entries until the cache is
// within MaxBytes. An entry larger than MaxBytes is served but not kept.
func (s *Store) add(e *entry) {
	e.size = int64(len(e.data)) + int64(len(e.key)) + entryOverhead
	if e.noKeep || e.size > s.o.MaxBytes {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[e.key]; ok {
		s.removeLocked(el)
	}
	s.items[e.key] = s.lru.PushFront(e)
	s.bytes += e.size
	for s.bytes > s.o.MaxBytes {
		s.removeLocked(s.lru.Back())
	}
}

func (s *Store) removeLocked(el *list.Element) {
	e := s.lru.Remove(el).(*entry)
	delete(s.items, e.key)
	s.bytes -= e.size
}

// Get passes through uncached.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.base.Get(ctx, key)
}

// GetLimit passes through uncached (see store.GetLimit).
func (s *Store) GetLimit(ctx context.Context, key string, max int64) ([]byte, error) {
	return store.GetLimit(ctx, s.base, key, max)
}

// Exists passes through uncached.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	return s.base.Exists(ctx, key)
}

// List passes through uncached.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	return s.base.List(ctx, prefix)
}

// Put returns client.ErrReadOnly.
func (s *Store) Put(context.Context, string, []byte, ...store.PutOptions) error {
	return client.ErrReadOnly
}

// PutIfAbsent returns client.ErrReadOnly.
func (s *Store) PutIfAbsent(context.Context, string, []byte, ...store.PutOptions) error {
	return client.ErrReadOnly
}

// Delete returns client.ErrReadOnly.
func (s *Store) Delete(context.Context, string) error {
	return client.ErrReadOnly
}
