package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

func hr(h uint64) *archivev1.HeightRecord { return &archivev1.HeightRecord{Height: h} }
func br(h uint64, c byte) *archivev1.BlobRecord {
	return &archivev1.BlobRecord{Height: h, Namespace: make([]byte, 29), Commitment: bytes.Repeat([]byte{c}, 32)}
}

func mkRef(t *testing.T, first, last int64, extra byte) SegmentRef {
	t.Helper()
	b := NewBuilder(nil)
	for h := first; h <= last; h++ {
		var bl []*archivev1.BlobRecord
		if extra != 0 {
			bl = append(bl, br(uint64(h), extra))
		}
		if err := b.Add(hr(uint64(h)), bl); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := b.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return *ref
}

func readManifest(t *testing.T, st store.Store) Manifest {
	t.Helper()
	b, err := st.Get(context.Background(), ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func newCommitter(t *testing.T, st store.Store) *Committer {
	t.Helper()
	c, err := NewCommitter(context.Background(), st, "mocha-4")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBuilderClosesBySizeAndTime(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBuilder(func() time.Time { return now })
	if b.ShouldClose(now.Add(24 * time.Hour)) {
		t.Fatal("empty builder must not close")
	}
	if err := b.Add(hr(5), nil); err != nil {
		t.Fatal(err)
	}
	if b.ShouldClose(now.Add(time.Hour)) {
		t.Fatal("exactly 1h must not close")
	}
	if !b.ShouldClose(now.Add(time.Hour + time.Second)) {
		t.Fatal("over 1h must close")
	}
	// size: a big blob pushes past 64 MiB
	b2 := NewBuilder(func() time.Time { return now })
	big := br(1, 1)
	big.Data = make([]byte, MaxSegmentSize)
	if err := b2.Add(hr(1), []*archivev1.BlobRecord{big}); err != nil {
		t.Fatal(err)
	}
	if !b2.ShouldClose(now) {
		t.Fatal("size >= 64MiB must close")
	}
}

func TestBuilderOrderIndexAndGap(t *testing.T) {
	b := NewBuilder(nil)
	if err := b.Add(hr(10), []*archivev1.BlobRecord{br(10, 1), br(10, 2)}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(hr(12), nil); err == nil {
		t.Fatal("gap must error")
	}
	if err := b.Add(hr(11), []*archivev1.BlobRecord{br(11, 3)}); err != nil {
		t.Fatal(err)
	}
	ref, err := b.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ref.First != 10 || ref.Last != 11 || ref.Key != "segments/000000000010-000000000011.seg" ||
		IndexKey(ref.First, ref.Last) != "segments/000000000010-000000000011.idx2" || ref.Size != int64(len(ref.seg)) {
		t.Fatalf("bad ref %+v", ref)
	}
	first, last, rows, err := segment.DecodeRows(ref.idx)
	if err != nil {
		t.Fatal(err)
	}
	if first != 10 || last != 11 || !slices.Equal(rows, ref.rows) {
		t.Fatalf("idx2 [%d,%d] rows %+v, ref rows %+v", first, last, rows, ref.rows)
	}
	want := []struct {
		h    uint64
		kind uint32
	}{{10, 1}, {10, 2}, {10, 2}, {11, 1}, {11, 2}}
	if len(rows) != len(want) {
		t.Fatalf("rows %d", len(rows))
	}
	for i, e := range rows {
		if e.Height != want[i].h || e.Kind != want[i].kind {
			t.Fatalf("row %d: %+v", i, e)
		}
		r, err := segment.ReadAt(ref.seg, e.Offset, e.Length)
		if err != nil {
			t.Fatal(err)
		}
		if (e.Kind == segment.KindHeight) != (r.GetHeight() != nil) {
			t.Fatalf("row %d kind mismatch", i)
		}
	}
	scanned, err := segment.ScanRows(ref.seg)
	if err != nil || !slices.Equal(scanned, rows) {
		t.Fatalf("ScanRows = %+v, %v; want the builder's rows", scanned, err)
	}
	if _, err := b.Flush(context.Background()); err == nil {
		t.Fatal("flush on reset builder must error")
	}
	if err := b.Add(hr(500), nil); err != nil {
		t.Fatalf("builder must accept a new start after flush: %v", err)
	}
}

func TestCommitOrderAndContiguity(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := store.NewFS(root)
	c := newCommitter(t, st)
	for _, s := range [][2]int64{{1, 100}, {201, 300}} {
		if err := c.Commit(ctx, mkRef(t, s[0], s[1], 1)); err != nil {
			t.Fatal(err)
		}
	}
	m := readManifest(t, st)
	if m.ContiguousUntil != 100 || m.HighestHeight != 300 || m.FormatVersion != FormatVersion || m.ChainID != "mocha-4" {
		t.Fatalf("manifest %+v", m)
	}
	if err := c.Commit(ctx, mkRef(t, 101, 200, 1)); err != nil {
		t.Fatal(err)
	}
	if m := readManifest(t, st); m.ContiguousUntil != 300 {
		t.Fatalf("manifest %+v", m)
	}
	for _, k := range []string{SegmentKey(1, 100), IndexKey(1, 100), DirKey(0), CatalogKey(0)} {
		if ok, _ := st.Exists(ctx, k); !ok {
			t.Fatalf("missing %s", k)
		}
	}
	checkDirMatchesCatalog(t, root)
}

func TestRecommitSameSegmentIsNoop(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	c := newCommitter(t, st)
	ref := mkRef(t, 1, 10, 1)
	for i := 0; i < 2; i++ {
		if err := c.Commit(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := st.Get(ctx, CatalogKey(0))
	var ch CatalogChunk
	if err := json.Unmarshal(b, &ch); err != nil || len(ch.Segments) != 1 {
		t.Fatalf("catalog %s err %v", b, err)
	}
}

func TestRecommitDifferentBytesFails(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	c := newCommitter(t, st)
	empty, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyTombstones(ctx, empty); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, mkRef(t, 1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, mkRef(t, 1, 10, 2)); !errors.Is(err, store.ErrExists) {
		t.Fatalf("want ErrExists, got %v", err)
	}
}

func TestConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := store.NewFS(root)
	c := newCommitter(t, st)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			first := int64(i*10 + 1)
			if err := c.Commit(ctx, mkRef(t, first, first+9, 1)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	b, _ := st.Get(ctx, CatalogKey(0))
	var ch CatalogChunk
	if err := json.Unmarshal(b, &ch); err != nil || len(ch.Segments) != 8 {
		t.Fatalf("catalog %s err %v", b, err)
	}
	for i, r := range ch.Segments {
		if r.First != int64(i*10+1) {
			t.Fatalf("not sorted/unique: %+v", ch.Segments)
		}
	}
	if m := readManifest(t, st); m.ContiguousUntil != 80 || m.HighestHeight != 80 {
		t.Fatalf("manifest %+v", m)
	}
	checkDirMatchesCatalog(t, root)
}

func TestNewCommitterRestoresState(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	c := newCommitter(t, st)
	for _, s := range [][2]int64{{1, 5}, {6, 10}, {21, 30}} {
		if err := c.Commit(ctx, mkRef(t, s[0], s[1], 0)); err != nil {
			t.Fatal(err)
		}
	}
	c2 := newCommitter(t, st)
	if got := c2.ContiguousUntil(); got != 10 {
		t.Fatalf("restored %d", got)
	}
	if err := c2.Commit(ctx, mkRef(t, 11, 20, 0)); err != nil {
		t.Fatal(err)
	}
	if m := readManifest(t, st); m.ContiguousUntil != 30 || m.HighestHeight != 30 {
		t.Fatalf("manifest %+v", m)
	}
	if _, err := NewCommitter(ctx, st, "other"); err == nil {
		t.Fatal("chain mismatch must error")
	}
}

// memStore is an in-memory store.Store for tests that commit many times;
// every commit rewrites a 240 KB directory bucket, which is slow with fsync.
type memStore struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newMemStore() *memStore { return &memStore{objs: map[string][]byte{}} }

func (m *memStore) PutIfAbsent(_ context.Context, key string, b []byte, _ ...store.PutOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.objs[key]; ok {
		if bytes.Equal(old, b) {
			return nil
		}
		return store.ErrExists
	}
	m.objs[key] = bytes.Clone(b)
	return nil
}

func (m *memStore) Put(_ context.Context, key string, b []byte, _ ...store.PutOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = bytes.Clone(b)
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(b), nil
}

func (m *memStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if off < 0 || n < 0 || off+n > int64(len(b)) {
		return nil, store.ErrOutOfRange
	}
	return b[off : off+n], nil
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
	slices.Sort(keys)
	return keys, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

func TestCatalogChunkFreezes(t *testing.T) {
	ctx := context.Background()
	st := newOptSpy(newMemStore())
	c := newCommitter(t, st)
	for i := int64(0); i < ChunkSize+1; i++ {
		if err := c.Commit(ctx, mkRef(t, i+1, i+1, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if m := readManifest(t, st); m.LastCatalogChunk != 1 || m.ContiguousUntil != ChunkSize+1 {
		t.Fatalf("manifest %+v", m)
	}
	b, _ := st.Get(ctx, CatalogKey(0))
	var ch CatalogChunk
	_ = json.Unmarshal(b, &ch)
	if len(ch.Segments) != ChunkSize {
		t.Fatalf("chunk0 has %d", len(ch.Segments))
	}
	if got := st.get(CatalogKey(0)); got != wantFrozen {
		t.Fatalf("frozen chunk headers %+v", got)
	}
	if got := st.get(CatalogKey(1)); got != wantMutable {
		t.Fatalf("open chunk headers %+v", got)
	}
	before, _ := st.Get(ctx, CatalogKey(0))
	c2 := newCommitter(t, st)
	if err := c2.Commit(ctx, mkRef(t, ChunkSize+2, ChunkSize+2, 0)); err != nil {
		t.Fatal(err)
	}
	if got := c2.ContiguousUntil(); got != ChunkSize+2 {
		t.Fatalf("got %d", got)
	}
	after, _ := st.Get(ctx, CatalogKey(0))
	if !bytes.Equal(before, after) {
		t.Fatal("frozen chunk 0 changed")
	}
}

func writeChunk(t *testing.T, st store.Store, n int, refs ...SegmentRef) {
	t.Helper()
	b, err := json.Marshal(CatalogChunk{Segments: refs})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), CatalogKey(n), b); err != nil {
		t.Fatal(err)
	}
}

func TestNewCommitterRecoversCatalogWithoutManifest(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	writeChunk(t, st, 0, mkRef(t, 1, 10, 0)) // crash before first manifest
	c := newCommitter(t, st)
	if got := c.ContiguousUntil(); got != 10 {
		t.Fatalf("restored %d", got)
	}
	if err := c.Commit(ctx, mkRef(t, 11, 20, 0)); err != nil {
		t.Fatal(err)
	}
	b, _ := st.Get(ctx, CatalogKey(0))
	var ch CatalogChunk
	if err := json.Unmarshal(b, &ch); err != nil || len(ch.Segments) != 2 {
		t.Fatalf("catalog %s err %v", b, err)
	}
	if m := readManifest(t, st); m.ContiguousUntil != 20 {
		t.Fatalf("manifest %+v", m)
	}
}

func TestNewCommitterRecoversChunkBeyondManifest(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	c := newCommitter(t, st)
	if err := c.Commit(ctx, mkRef(t, 1, 5, 0)); err != nil {
		t.Fatal(err)
	}
	// Crash after chunk 1 was written but before the manifest moved on.
	writeChunk(t, st, 1, mkRef(t, 6, 10, 0))
	c2 := newCommitter(t, st)
	if got := c2.ContiguousUntil(); got != 10 {
		t.Fatalf("restored %d", got)
	}
	if err := c2.Commit(ctx, mkRef(t, 11, 15, 0)); err != nil {
		t.Fatal(err)
	}
	b, _ := st.Get(ctx, CatalogKey(1))
	var ch CatalogChunk
	if err := json.Unmarshal(b, &ch); err != nil || len(ch.Segments) != 2 {
		t.Fatalf("chunk 1 %s err %v", b, err)
	}
	if m := readManifest(t, st); m.LastCatalogChunk != 1 || m.ContiguousUntil != 15 {
		t.Fatalf("manifest %+v", m)
	}
}

func TestCommitRejectsOverlap(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	c := newCommitter(t, st)
	if err := c.Commit(ctx, mkRef(t, 1, 100, 0)); err != nil {
		t.Fatal(err)
	}
	bad := mkRef(t, 1, 150, 0)
	if err := c.Commit(ctx, bad); !errors.Is(err, ErrOverlap) {
		t.Fatalf("want ErrOverlap, got %v", err)
	}
	for _, k := range []string{bad.Key, IndexKey(bad.First, bad.Last)} {
		if ok, _ := st.Exists(ctx, k); ok {
			t.Fatalf("%s written for rejected segment", k)
		}
	}
	b, _ := st.Get(ctx, CatalogKey(0))
	var ch CatalogChunk
	_ = json.Unmarshal(b, &ch)
	if len(ch.Segments) != 1 || readManifest(t, st).HighestHeight != 100 {
		t.Fatalf("state changed: %s", b)
	}
	// Overlap is also detected after a restart.
	c2 := newCommitter(t, st)
	if err := c2.Commit(ctx, mkRef(t, 100, 120, 0)); !errors.Is(err, ErrOverlap) {
		t.Fatalf("want ErrOverlap after restart, got %v", err)
	}
	// Adjacent is fine.
	if err := c2.Commit(ctx, mkRef(t, 101, 120, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedRangesAndHighest(t *testing.T) {
	st := store.NewFS(t.TempDir())
	c := newCommitter(t, st)
	if got := c.CommittedRanges(); len(got) != 0 || c.HighestHeight() != 0 {
		t.Fatalf("empty committer: ranges %v highest %d", got, c.HighestHeight())
	}
	ctx := context.Background()
	for _, r := range [][2]int64{{10, 12}, {1, 3}, {5, 6}} {
		if err := c.Commit(ctx, mkRef(t, r[0], r[1], 0)); err != nil {
			t.Fatal(err)
		}
	}
	want := [][2]int64{{1, 3}, {5, 6}, {10, 12}}
	check := func(c *Committer) {
		t.Helper()
		got := c.CommittedRanges()
		if len(got) != len(want) {
			t.Fatalf("ranges %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("ranges %v, want %v", got, want)
			}
		}
		if c.HighestHeight() != 12 {
			t.Fatalf("highest %d, want 12", c.HighestHeight())
		}
	}
	check(c)
	// The returned slice is a copy.
	c.CommittedRanges()[0] = [2]int64{100, 200}
	check(c)
	check(newCommitter(t, st))
}

// optSpy records the PutOptions of the last write to each key.
type optSpy struct {
	store.Store
	mu   sync.Mutex
	opts map[string]store.PutOptions
}

func newOptSpy(st store.Store) *optSpy {
	return &optSpy{Store: st, opts: map[string]store.PutOptions{}}
}

func (s *optSpy) record(key string, opts []store.PutOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var o store.PutOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	s.opts[key] = o
}

func (s *optSpy) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.record(key, opts)
	return s.Store.Put(ctx, key, b, opts...)
}

func (s *optSpy) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.record(key, opts)
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

func (s *optSpy) get(key string) store.PutOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opts[key]
}

var (
	wantSegment = store.PutOptions{CacheControl: "public, max-age=86400", ContentType: "application/octet-stream"}
	wantIndex   = store.PutOptions{CacheControl: "public, max-age=31536000, immutable", ContentType: "application/octet-stream"}
	wantFrozen  = store.PutOptions{CacheControl: "public, max-age=31536000, immutable", ContentType: "application/json"}
	wantMutable = store.PutOptions{CacheControl: "no-cache", ContentType: "application/json"}
	wantOpenDir = store.PutOptions{CacheControl: "no-cache", ContentType: "application/octet-stream"}
)

func TestCommitObjectHeaders(t *testing.T) {
	spy := newOptSpy(store.NewFS(t.TempDir()))
	c := newCommitter(t, spy)
	ref := mkRef(t, 1, 3, 7)
	if err := c.Commit(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]store.PutOptions{
		ref.Key: wantSegment, IndexKey(ref.First, ref.Last): wantIndex,
		DirKey(0):     wantOpenDir,
		CatalogKey(0): wantMutable, ManifestKey: wantMutable,
	} {
		if got := spy.get(key); got != want {
			t.Errorf("%s: headers %+v, want %+v", key, got, want)
		}
	}
}

func TestBuilderRejectsMalformedBlobKeys(t *testing.T) {
	b := NewBuilder(nil)
	short := br(1, 1)
	short.Commitment = short.Commitment[:1]
	if err := b.Add(hr(1), []*archivev1.BlobRecord{short}); err == nil {
		t.Fatal("1-byte commitment must be rejected")
	}
	short = br(1, 1)
	short.Namespace = short.Namespace[:28]
	if err := b.Add(hr(1), []*archivev1.BlobRecord{short}); err == nil {
		t.Fatal("28-byte namespace must be rejected")
	}
	if !b.Empty() || b.Size() != 0 {
		t.Fatal("rejected Add changed the builder")
	}
}

var errInjected = errors.New("injected store failure")

// guardStore records object headers and enforces the directory freeze rule:
// a dir/ bucket may be written immutable only while the stored catalog covers
// all of its heights, and is never written again afterwards. It can also fail
// selected writes.
type guardStore struct {
	*optSpy
	mu         sync.Mutex
	frozen     map[string]bool
	violations []string
	// failCatalog fails the first Put of catalog/00000000.json that follows
	// a dir/ write.
	failCatalog, sawDir bool
	// failFreeze fails this many immutable dir/ writes.
	failFreeze int
}

func newGuardStore(root string) *guardStore {
	return &guardStore{optSpy: newOptSpy(store.NewFS(root)), frozen: map[string]bool{}}
}

func (g *guardStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if strings.HasPrefix(key, "dir/") {
		if g.frozen[key] {
			g.violations = append(g.violations, "rewrote frozen "+key)
		}
		if len(opts) > 0 && opts[0].CacheControl == store.CacheImmutable {
			if g.failFreeze > 0 {
				g.failFreeze--
				return errInjected
			}
			var bucket int64
			if _, err := fmt.Sscanf(key, "dir/%d.dir", &bucket); err != nil {
				return err
			}
			if !g.catalogCovers(ctx, bucket*DirBucket+1, (bucket+1)*DirBucket) {
				g.violations = append(g.violations, "froze "+key+" before the catalog covers it")
			}
			g.frozen[key] = true
		}
		g.sawDir = true
	}
	if key == CatalogKey(0) && g.failCatalog && g.sawDir {
		g.failCatalog = false
		return errInjected
	}
	return g.optSpy.Put(ctx, key, b, opts...)
}

func (g *guardStore) catalogCovers(ctx context.Context, lo, hi int64) bool {
	var refs []SegmentRef
	for i := 0; ; i++ {
		b, err := g.optSpy.Get(ctx, CatalogKey(i))
		if err != nil {
			break
		}
		var ch CatalogChunk
		if err := json.Unmarshal(b, &ch); err != nil {
			return false
		}
		refs = append(refs, ch.Segments...)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].First < refs[j].First })
	until := lo - 1
	for _, r := range refs {
		if r.First > until+1 {
			break
		}
		until = max(until, r.Last)
	}
	return until >= hi
}

func (g *guardStore) arm() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failCatalog, g.sawDir = true, false
}

func (g *guardStore) check(t *testing.T) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, v := range g.violations {
		t.Error(v)
	}
}

// readDir returns the decoded entries of directory bucket b.
func readDir(t *testing.T, st store.Store, b int64) *[DirBucket]DirEntry {
	t.Helper()
	raw, err := st.Get(context.Background(), DirKey(b))
	if err != nil {
		t.Fatal(err)
	}
	got, e, err := DecodeDir(raw)
	if err != nil || got != b {
		t.Fatalf("%s: bucket %d, %v", DirKey(b), got, err)
	}
	return e
}

// checkDirMatchesCatalog decodes every .idx2 and dir/ object under root and
// checks that every non-empty directory entry points at a catalogued segment
// and at exactly its height's rows, and that every catalogued height has
// such an entry.
func checkDirMatchesCatalog(t *testing.T, root string) {
	t.Helper()
	ctx := context.Background()
	st := store.NewFS(root)
	refs := map[[2]int64]SegmentRef{}
	for i := 0; ; i++ {
		b, err := st.Get(ctx, CatalogKey(i))
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var ch CatalogChunk
		if err := json.Unmarshal(b, &ch); err != nil {
			t.Fatal(err)
		}
		for _, r := range ch.Segments {
			refs[[2]int64{r.First, r.Last}] = r
		}
	}
	rows := map[[2]int64][]segment.Row{}
	idx2, _ := filepath.Glob(filepath.Join(root, "segments", "*.idx2"))
	for _, p := range idx2 {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		first, last, rs, err := segment.DecodeRows(b)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if filepath.Base(IndexKey(int64(first), int64(last))) != filepath.Base(p) {
			t.Fatalf("%s holds rows of [%d,%d]", p, first, last)
		}
		rows[[2]int64{int64(first), int64(last)}] = rs
	}
	dirs := map[int64]*[DirBucket]DirEntry{}
	files, _ := filepath.Glob(filepath.Join(root, "dir", "*.dir"))
	for _, p := range files {
		var bucket int64
		if _, err := fmt.Sscanf(filepath.Base(p), "%d.dir", &bucket); err != nil {
			t.Fatal(err)
		}
		dirs[bucket] = readDir(t, st, bucket)
	}
	for b, es := range dirs {
		for i, e := range es {
			if e == (DirEntry{}) {
				continue
			}
			h := b*DirBucket + int64(i) + 1
			if _, ok := refs[[2]int64{e.SegFirst, e.SegLast}]; !ok {
				t.Fatalf("height %d points at uncatalogued segment [%d,%d]", h, e.SegFirst, e.SegLast)
			}
			rs := rows[[2]int64{e.SegFirst, e.SegLast}]
			end := int(e.RowStart) + int(e.RowCount)
			if end > len(rs) {
				t.Fatalf("height %d: rows [%d,%d) past %d rows", h, e.RowStart, end, len(rs))
			}
			group := rs[e.RowStart:end]
			if group[0].Kind != segment.KindHeight ||
				(e.RowStart > 0 && rs[e.RowStart-1].Height == uint64(h)) ||
				(end < len(rs) && rs[end].Height == uint64(h)) {
				t.Fatalf("height %d: rows [%d,%d) are not its whole row group", h, e.RowStart, end)
			}
			for _, r := range group {
				if r.Height != uint64(h) {
					t.Fatalf("height %d: row of height %d in its group", h, r.Height)
				}
			}
		}
	}
	for _, r := range refs {
		for h := r.First; h <= r.Last; h++ {
			b, off := DirSlot(h)
			es := dirs[b]
			if es == nil {
				t.Fatalf("catalogued height %d: no %s", h, DirKey(b))
			}
			if e := es[(off-DirHeaderSize)/DirEntrySize]; e.SegFirst != r.First || e.SegLast != r.Last {
				t.Fatalf("catalogued height %d in %s: dir entry %+v", h, r.Key, e)
			}
		}
	}
}

func TestCommitWritesDirectory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := newGuardStore(root)
	c := newCommitter(t, st)
	if err := c.Commit(ctx, mkRef(t, 1, 100, 1)); err != nil {
		t.Fatal(err)
	}
	// Height 1: one height row and one blob row, at the start of the .idx2.
	if e := readDir(t, st, 0)[0]; e != (DirEntry{1, 100, 0, 2}) {
		t.Fatalf("height 1: %+v", e)
	}
	if got := st.get(DirKey(0)); got != wantOpenDir {
		t.Fatalf("open bucket headers %+v", got)
	}
	if err := c.Commit(ctx, mkRef(t, 101, 10000, 1)); err != nil {
		t.Fatal(err)
	}
	es := readDir(t, st, 0)
	if es[0] != (DirEntry{1, 100, 0, 2}) || es[9999] != (DirEntry{101, 10000, 2 * 9899, 2}) {
		t.Fatalf("height 1: %+v, height 10000: %+v", es[0], es[9999])
	}
	if got := st.get(DirKey(0)); got.CacheControl != store.CacheImmutable || got.ContentType != store.TypeBinary {
		t.Fatalf("completed bucket headers %+v", got)
	}
	c.mu.Lock()
	open := len(c.open)
	c.mu.Unlock()
	if open != 0 {
		t.Fatalf("%d buckets still in memory after freezing", open)
	}
	checkDirMatchesCatalog(t, root)
	st.check(t)
}

func TestSegmentSpanningTwoBuckets(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := newGuardStore(root)
	c := newCommitter(t, st)
	if err := c.Commit(ctx, mkRef(t, 9990, 10010, 1)); err != nil {
		t.Fatal(err)
	}
	for b := int64(0); b <= 1; b++ {
		es := readDir(t, st, b)
		for i, e := range es {
			h := b*DirBucket + int64(i) + 1
			want := h >= 9990 && h <= 10010
			if got := e.SegFirst == 9990 && e.SegLast == 10010; got != want || (!want && e != (DirEntry{})) {
				t.Fatalf("height %d: %+v", h, e)
			}
		}
		if got := st.get(DirKey(b)); got != wantOpenDir {
			t.Fatalf("%s headers %+v", DirKey(b), got)
		}
	}
	checkDirMatchesCatalog(t, root)
	st.check(t)
}

// Review Focus 4: a crash between the directory write and the catalog write
// must not leave a frozen bucket or a pointer at an uncatalogued segment once
// the heights are re-committed with different boundaries.
func TestDirectoryConsistentAfterCrashBeforeCatalog(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := newGuardStore(root)
	c := newCommitter(t, st)
	if err := c.Commit(ctx, mkRef(t, 1, 9989, 0)); err != nil {
		t.Fatal(err)
	}
	st.arm()
	if err := c.Commit(ctx, mkRef(t, 9990, 10000, 0)); !errors.Is(err, errInjected) {
		t.Fatalf("commit with failing catalog: %v", err)
	}
	if got := st.get(DirKey(0)); got != wantOpenDir {
		t.Fatalf("bucket written %+v before the catalog covers it", got)
	}
	if e := readDir(t, st, 0)[9999]; e.SegFirst != 9990 {
		t.Fatalf("test proves nothing: the failed commit left no directory entry (%+v)", e)
	}

	c2 := newCommitter(t, st)
	if err := c2.Commit(ctx, mkRef(t, 9990, 9994, 0)); err != nil {
		t.Fatal(err)
	}
	checkDirMatchesCatalog(t, root)
	if got := st.get(DirKey(0)); got != wantOpenDir {
		t.Fatalf("bucket headers %+v with 9995-10000 uncatalogued", got)
	}
	if err := c2.Commit(ctx, mkRef(t, 9995, 10000, 0)); err != nil {
		t.Fatal(err)
	}
	checkDirMatchesCatalog(t, root)
	if got := st.get(DirKey(0)); got.CacheControl != store.CacheImmutable {
		t.Fatalf("complete bucket headers %+v", got)
	}
	if got := c2.ContiguousUntil(); got != 10000 {
		t.Fatalf("contiguous until %d", got)
	}
	st.check(t)
}

// A failed freeze does not fail Commit: it is reported, the manifest is
// written, the bucket stays no-cache and the next Commit freezes it.
func TestDirectoryFreezeRetried(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := newGuardStore(root)
	st.failFreeze = 1
	c := newCommitter(t, st)
	var failed []string
	c.SetFreezeErrorHook(func(key string, err error) {
		if !errors.Is(err, errInjected) {
			t.Errorf("freeze error %v", err)
		}
		failed = append(failed, key)
	})
	if err := c.Commit(ctx, mkRef(t, 1, 10000, 0)); err != nil {
		t.Fatalf("commit with failing freeze: %v", err)
	}
	if len(failed) != 1 || failed[0] != DirKey(0) {
		t.Fatalf("freeze errors reported: %v", failed)
	}
	if m := readManifest(t, st); m.HighestHeight != 10000 || m.ContiguousUntil != 10000 {
		t.Fatalf("manifest %+v", m)
	}
	if got := st.get(DirKey(0)); got != wantOpenDir {
		t.Fatalf("bucket headers %+v after failed freeze", got)
	}
	if err := c.Commit(ctx, mkRef(t, 10001, 10005, 0)); err != nil {
		t.Fatal(err)
	}
	if got := st.get(DirKey(0)); got.CacheControl != store.CacheImmutable {
		t.Fatalf("bucket headers %+v after the next commit", got)
	}
	if len(failed) != 1 {
		t.Fatalf("freeze errors reported: %v", failed)
	}
	checkDirMatchesCatalog(t, root)
	st.check(t)
}

func TestCommitterRejectsV1Manifest(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	if err := st.Put(ctx, ManifestKey, []byte(`{"format_version":1,"chain_id":"mocha-4"}`)); err != nil {
		t.Fatal(err)
	}
	_, err := NewCommitter(ctx, st, "mocha-4")
	if err == nil || !strings.Contains(err.Error(), "cba-ingester migrate-v2") {
		t.Fatalf("NewCommitter on a v1 manifest: %v", err)
	}
}
