package opbackfill_test

import (
	"context"
	"encoding/hex"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/nsindex"
	"github.com/noders-team/celestia-blob-archive/internal/opbackfill"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// In mocha-1834013 the namespace v0NS holds one share-version-0 blob and v1NS
// two share-version-1 blobs.
const (
	v0NS = "00000000000000000000000000000000000000893f79d6e2c81a4d08c4"
	v1NS = "00000000000000000000000000000000000000636c6235656637626138"
)

// blobHeights carry the fixture; with segments of 2 heights, each of the
// three segments [1,2], [3,4], [5,6] holds one v0NS blob.
var blobHeights = []int64{2, 4, 6}

func nsOf(t *testing.T, s string) [29]byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 29 {
		t.Fatalf("namespace %q: %v", s, err)
	}
	return [29]byte(b)
}

type fixture struct {
	st     store.Store
	blocks map[int64]*source.Block
	blobs  map[int64][]*archivev1.BlobRecord // as processed, by height
}

// build archives heights 1..6 in three segments; the namespace index covers
// the first two only. Then v0NS is configured. mut may be nil.
func build(t *testing.T, st store.Store, mut archivetest.Mutate) *fixture {
	t.Helper()
	ctx := context.Background()
	at := map[int64]string{}
	for _, h := range blobHeights {
		at[h] = "mocha-1834013"
	}
	f := &fixture{st: st, blocks: archivetest.Blocks(t, 7, at), blobs: map[int64][]*archivev1.BlobRecord{}}
	for _, h := range blobHeights {
		_, blobs, err := process.Process(f.blocks[h], f.blocks[h+1])
		if err != nil {
			t.Fatal(err)
		}
		f.blobs[h] = blobs
	}
	archivetest.Build(t, st, f.blocks, 1, 4, 2, mut)
	if err := nsindex.Update(ctx, st); err != nil {
		t.Fatal(err)
	}
	archivetest.Build(t, st, f.blocks, 5, 6, 2, mut)
	b, err := st.Get(ctx, nsindex.ProgressKey)
	if err != nil || !strings.Contains(string(b), `"last_height":4`) {
		t.Fatalf("premise: namespace index progress %s, %v", b, err)
	}
	if err := oplayer.WriteConfig(ctx, st, [][29]byte{nsOf(t, v0NS)}, time.Now()); err != nil {
		t.Fatal(err)
	}
	return f
}

// v0Blob returns the v0NS blob of height h.
func (f *fixture) v0Blob(t *testing.T, h int64) *archivev1.BlobRecord {
	t.Helper()
	for _, b := range f.blobs[h] {
		if hex.EncodeToString(b.Namespace) == v0NS {
			return b
		}
	}
	t.Fatalf("no v0NS blob at height %d", h)
	return nil
}

func compactKey(t *testing.T, b *archivev1.BlobRecord) string {
	t.Helper()
	ob, err := oplayer.FromRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	k, err := ob.Keys()
	if err != nil {
		t.Fatal(err)
	}
	return k[0]
}

// opObjects returns every object under op/ except the config, with its bytes.
func opObjects(t *testing.T, st store.Store) map[string]string {
	t.Helper()
	ctx := context.Background()
	keys, err := st.List(ctx, "op/")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, k := range keys {
		if k == oplayer.ConfigKey {
			continue
		}
		b, err := st.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		out[k] = string(b)
	}
	return out
}

func addTombstone(t *testing.T, st store.Store, b *archivev1.BlobRecord) {
	t.Helper()
	err := tombstone.Add(context.Background(), st, tombstone.Entry{
		Height: int64(b.Height), Namespace: hex.EncodeToString(b.Namespace),
		Commitment: hex.EncodeToString(b.Commitment), Date: "2026-10-27", Reason: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBackfillWritesHistory(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	f := build(t, st, nil)

	var progress int
	stats, err := opbackfill.Backfill(ctx, st, nsOf(t, v0NS), opbackfill.BackfillOptions{
		Workers:  2,
		Progress: func(opbackfill.BackfillStats) { progress++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	var bytes int64
	for _, h := range blobHeights {
		b := f.v0Blob(t, h)
		want[compactKey(t, b)] = string(b.Data)
		bytes += int64(len(b.Data))
	}
	got := opObjects(t, st)
	if len(got) != len(want) {
		t.Fatalf("op objects %v, want the %d compact keys of v0NS", slices.Sorted(mapKeys(got)), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: missing or wrong bytes", k)
		}
	}
	wantStats := opbackfill.BackfillStats{Blobs: 3, Written: 3, Bytes: bytes, Heights: [2]int64{1, 6}}
	if stats != wantStats {
		t.Fatalf("stats %+v, want %+v", stats, wantStats)
	}
	if progress == 0 {
		t.Error("Progress never called")
	}

	// A namespace with only share-version-1 blobs writes nothing.
	if err := oplayer.WriteConfig(ctx, st, [][29]byte{nsOf(t, v0NS), nsOf(t, v1NS)}, time.Now()); err != nil {
		t.Fatal(err)
	}
	stats, err = opbackfill.Backfill(ctx, st, nsOf(t, v1NS), opbackfill.BackfillOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blobs != 6 || stats.Skipped != 6 || stats.Written != 0 {
		t.Fatalf("v1NS stats %+v, want 6 blobs all skipped", stats)
	}
	if n := len(opObjects(t, st)); n != 3 {
		t.Fatalf("%d op objects after the v1NS run, want 3", n)
	}
}

func mapKeys(m map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func TestBackfillRefusesUnconfiguredNamespace(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	build(t, st, nil)
	_, err := opbackfill.Backfill(ctx, st, nsOf(t, v1NS), opbackfill.BackfillOptions{})
	want := "namespace " + v1NS + " is not in op/_namespaces.json: add it to CBA_OP_NAMESPACES and restart the ingester first"
	if err == nil || err.Error() != want {
		t.Fatalf("err %v, want %q", err, want)
	}
	if n := len(opObjects(t, st)); n != 0 {
		t.Fatalf("%d op objects written", n)
	}
}

func TestBackfillSkipsWithdrawnAndTombstoneRecords(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	f := build(t, st, nil)
	// Height 2: listed in tombstones.json, segment not yet scrubbed.
	addTombstone(t, st, f.v0Blob(t, 2))
	// Height 6: scrubbed in the segment (above the namespace index).
	if n := archivetest.Withdraw(t, st, archive.SegmentKey(5, 6), func(b *archivev1.BlobRecord) bool {
		return hex.EncodeToString(b.Namespace) == v0NS
	}); n != 1 {
		t.Fatalf("withdrew %d records", n)
	}
	stats, err := opbackfill.Backfill(ctx, st, nsOf(t, v0NS), opbackfill.BackfillOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blobs != 3 || stats.Withdrawn != 2 || stats.Written != 1 || stats.Deleted != 0 {
		t.Fatalf("stats %+v", stats)
	}
	got := opObjects(t, st)
	if _, ok := got[compactKey(t, f.v0Blob(t, 4))]; !ok || len(got) != 1 {
		t.Fatalf("op objects %v, want only height 4", slices.Sorted(mapKeys(got)))
	}
}

// hookStore runs after once a PutIfAbsent of key has succeeded.
type hookStore struct {
	store.Store
	key   string
	once  sync.Once
	after func()
}

func (s *hookStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if err := s.Store.PutIfAbsent(ctx, key, b, opts...); err != nil {
		return err
	}
	if key == s.key {
		s.once.Do(s.after)
	}
	return nil
}

func TestBackfillTakedownDuringRun(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	f := build(t, fs, nil)
	target := f.v0Blob(t, 4)
	// The hook runs on a worker goroutine, so it must not call t.Fatal.
	hs := &hookStore{Store: fs, key: compactKey(t, target), after: func() {
		err := tombstone.Add(ctx, fs, tombstone.Entry{
			Height: int64(target.Height), Namespace: hex.EncodeToString(target.Namespace),
			Commitment: hex.EncodeToString(target.Commitment), Date: "2026-10-27", Reason: "test",
		})
		if err != nil {
			t.Error(err)
		}
	}}

	stats, err := opbackfill.Backfill(ctx, hs, nsOf(t, v0NS), opbackfill.BackfillOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Deleted != 1 {
		t.Fatalf("Deleted = %d, want 1 (stats %+v)", stats.Deleted, stats)
	}
	got := opObjects(t, fs)
	if _, ok := got[compactKey(t, target)]; ok {
		t.Fatal("withdrawn blob's OP object is still public")
	}
	if len(got) != 2 {
		t.Fatalf("op objects %v, want heights 2 and 6", slices.Sorted(mapKeys(got)))
	}
}

// failingTombstoneStore fails every Get of tombstones.json after the first.
type failingTombstoneStore struct {
	store.Store
	gets atomic.Int64
}

func (s *failingTombstoneStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == tombstone.Key && s.gets.Add(1) > 1 {
		return nil, errors.New("injected: R2 unavailable")
	}
	return s.Store.Get(ctx, key)
}

// addTombstoneHook returns a hook that lists b, for use on a worker goroutine.
func addTombstoneHook(t *testing.T, st store.Store, b *archivev1.BlobRecord) func() {
	return func() {
		err := tombstone.Add(context.Background(), st, tombstone.Entry{
			Height: int64(b.Height), Namespace: hex.EncodeToString(b.Namespace),
			Commitment: hex.EncodeToString(b.Commitment), Date: "2026-10-27", Reason: "test",
		})
		if err != nil {
			t.Error(err)
		}
	}
}

func TestBackfillFailedCleanupRecoveredByRerun(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	f := build(t, fs, nil)
	target := f.v0Blob(t, 4)
	key := compactKey(t, target)
	fail := &failingTombstoneStore{Store: fs}
	hs := &hookStore{Store: fail, key: key, after: addTombstoneHook(t, fs, target)}

	_, err := opbackfill.Backfill(ctx, hs, nsOf(t, v0NS), opbackfill.BackfillOptions{Workers: 1})
	if err == nil || !strings.Contains(err.Error(), "run the backfill again") {
		t.Fatalf("err %v, want the failed reload reported", err)
	}
	if _, ok := opObjects(t, fs)[key]; !ok {
		t.Fatal("premise: the withdrawn blob's key should survive the failed cleanup")
	}

	// The rerun the error asks for removes it.
	stats, err := opbackfill.Backfill(ctx, fs, nsOf(t, v0NS), opbackfill.BackfillOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := opObjects(t, fs)[key]; ok {
		t.Fatal("rerun left the withdrawn blob's OP object public")
	}
	if stats.Withdrawn != 1 || stats.Present != 2 {
		t.Fatalf("rerun stats %+v", stats)
	}
}

// ctxStore fails reads and deletes on a done context, as R2 requests do (the
// FS store ignores ctx).
type ctxStore struct{ store.Store }

func (s ctxStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Store.Get(ctx, key)
}

func (s ctxStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func (s ctxStore) Exists(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.Store.Exists(ctx, key)
}

func (s ctxStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.Delete(ctx, key)
}

func TestBackfillCleanupRunsAfterCancel(t *testing.T) {
	fs := store.NewFS(t.TempDir())
	f := build(t, fs, nil)
	target := f.v0Blob(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	add := addTombstoneHook(t, fs, target)
	// A takedown lands, then the operator presses Ctrl-C.
	hs := &hookStore{Store: ctxStore{fs}, key: compactKey(t, target), after: func() { add(); cancel() }}

	stats, err := opbackfill.Backfill(ctx, hs, nsOf(t, v0NS), opbackfill.BackfillOptions{Workers: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", err)
	}
	if stats.Deleted != 1 {
		t.Fatalf("Deleted = %d, want 1", stats.Deleted)
	}
	if _, ok := opObjects(t, fs)[compactKey(t, target)]; ok {
		t.Fatal("withdrawn blob's OP object is still public after a cancelled run")
	}
}

func TestBackfillIdempotentAndResumable(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	f := build(t, st, nil)
	if _, err := opbackfill.Backfill(ctx, st, nsOf(t, v0NS), opbackfill.BackfillOptions{}); err != nil {
		t.Fatal(err)
	}
	first := opObjects(t, st)
	stats, err := opbackfill.Backfill(ctx, st, nsOf(t, v0NS), opbackfill.BackfillOptions{})
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second := opObjects(t, st); !maps.Equal(first, second) {
		t.Fatalf("second run changed the object set: %v → %v", slices.Sorted(maps.Keys(first)), slices.Sorted(maps.Keys(second)))
	}
	if stats.Written != 0 || stats.Present != 3 || stats.Bytes != 0 {
		t.Fatalf("second run stats %+v, want 3 present and nothing written", stats)
	}

	// -from skips lower heights.
	st2 := store.NewFS(t.TempDir())
	build(t, st2, nil)
	stats, err = opbackfill.Backfill(ctx, st2, nsOf(t, v0NS), opbackfill.BackfillOptions{From: 3, To: 6})
	if err != nil {
		t.Fatal(err)
	}
	got := opObjects(t, st2)
	if _, ok := got[compactKey(t, f.v0Blob(t, 2))]; ok || len(got) != 2 {
		t.Fatalf("op objects %v, want heights 4 and 6", slices.Sorted(mapKeys(got)))
	}
	if stats.Heights != [2]int64{3, 6} || stats.Written != 2 {
		t.Fatalf("stats %+v", stats)
	}
	if _, err := opbackfill.Backfill(ctx, st2, nsOf(t, v0NS), opbackfill.BackfillOptions{From: 5, To: 4}); err == nil {
		t.Fatal("from > to accepted")
	}
}

// optsStore records the PutOptions of each conditional write.
type optsStore struct {
	store.Store
	mu   sync.Mutex
	opts map[string]store.PutOptions
}

func (s *optsStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	if len(opts) > 0 {
		s.opts[key] = opts[0]
	}
	s.mu.Unlock()
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

// TestBackfillObjectHeaders: OP objects get the short OP max-age, so a
// takedown reaches downstream caches within minutes.
func TestBackfillObjectHeaders(t *testing.T) {
	fs := store.NewFS(t.TempDir())
	build(t, fs, nil)
	rec := &optsStore{Store: fs, opts: map[string]store.PutOptions{}}
	if _, err := opbackfill.Backfill(context.Background(), rec, nsOf(t, v0NS), opbackfill.BackfillOptions{}); err != nil {
		t.Fatal(err)
	}
	keys := opObjects(t, fs)
	if len(keys) == 0 {
		t.Fatal("no op objects written")
	}
	for k := range keys {
		if o := rec.opts[k]; o.CacheControl != store.CacheOP || o.ContentType != store.TypeBinary {
			t.Errorf("%s: headers %+v", k, o)
		}
	}
}

// countingStore counts writes.
type countingStore struct {
	store.Store
	puts atomic.Int64
}

func (s *countingStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.puts.Add(1)
	return s.Store.Put(ctx, key, b, opts...)
}

func (s *countingStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.puts.Add(1)
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

func (s *countingStore) Delete(ctx context.Context, key string) error {
	s.puts.Add(1)
	return s.Store.Delete(ctx, key)
}

func TestBackfillDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	f := build(t, fs, nil)
	cs := &countingStore{Store: fs}
	stats, err := opbackfill.Backfill(ctx, cs, nsOf(t, v0NS), opbackfill.BackfillOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if n := cs.puts.Load(); n != 0 {
		t.Fatalf("dry run made %d writes", n)
	}
	var bytes int64
	for _, h := range blobHeights {
		bytes += int64(len(f.v0Blob(t, h).Data))
	}
	if stats.Written != 3 || stats.Bytes != bytes {
		t.Fatalf("stats %+v, want 3 writes of %d bytes", stats, bytes)
	}
}

func TestBackfillRejectsForgedRecord(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	// The v0NS blob at height 4 is stored with one data byte changed.
	forge := func(hr *archivev1.HeightRecord, blobs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		if hr.Height != 4 {
			return hr, blobs
		}
		out := make([]*archivev1.BlobRecord, len(blobs))
		for i, b := range blobs {
			if hex.EncodeToString(b.Namespace) == v0NS {
				b = proto.Clone(b).(*archivev1.BlobRecord)
				b.Data[0] ^= 0xff
			}
			out[i] = b
		}
		return hr, out
	}
	f := build(t, st, forge)
	_, err := opbackfill.Backfill(ctx, st, nsOf(t, v0NS), opbackfill.BackfillOptions{Workers: 1})
	if err == nil || !strings.Contains(err.Error(), "height 4") {
		t.Fatalf("err %v, want one naming height 4", err)
	}
	if _, ok := opObjects(t, st)[compactKey(t, f.v0Blob(t, 4))]; ok {
		t.Fatal("forged blob was written")
	}
}
