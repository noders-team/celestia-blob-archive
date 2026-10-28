package archive_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

const blobHeight = 3

// takedownBlocks returns blocks 1..6 with the mocha-1834013 blobs at height
// 3, and those blob records.
func takedownBlocks(t *testing.T) (map[int64]*source.Block, []*archivev1.BlobRecord) {
	t.Helper()
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: "mocha-1834013"})
	_, blobs, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) < 2 {
		t.Fatalf("fixture has %d blobs, want at least 2", len(blobs))
	}
	return blocks, blobs
}

// withdraw adds b to the tombstone file of st and returns the loaded set.
func withdraw(t *testing.T, st store.Store, b *archivev1.BlobRecord) *tombstone.Set {
	t.Helper()
	ctx := context.Background()
	err := tombstone.Add(ctx, st, tombstone.Entry{
		Height: int64(b.Height), Namespace: hex.EncodeToString(b.Namespace),
		Commitment: hex.EncodeToString(b.Commitment), Date: "2026-10-26", Reason: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// loadSet loads the tombstone set of st.
func loadSet(t *testing.T, st store.Store) *tombstone.Set {
	t.Helper()
	s, err := tombstone.Load(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// buildRef builds one segment of heights first..last.
func buildRef(t *testing.T, blocks map[int64]*source.Block, first, last int64) archive.SegmentRef {
	t.Helper()
	b := archive.NewBuilder(nil)
	for h := first; h <= last; h++ {
		hr, blobs, err := process.Process(blocks[h], blocks[h+1])
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Add(hr, blobs); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := b.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return *ref
}

// checkScrubbed fails unless the segment under key holds a tombstone record
// in place of withdrawn and still holds every other blob.
func checkScrubbed(t *testing.T, st store.Store, key string, withdrawn *archivev1.BlobRecord, others []*archivev1.BlobRecord) {
	t.Helper()
	seg, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(seg, withdrawn.Data) {
		t.Fatalf("%s still holds the withdrawn blob data", key)
	}
	for i, o := range others {
		if !bytes.Contains(seg, o.Data) {
			t.Fatalf("%s lost blob %d", key, i)
		}
	}
	rows, err := segment.ScanRows(seg)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Kind != segment.KindBlob || r.Height != withdrawn.Height || !bytes.Equal(r.Commitment[:], withdrawn.Commitment) {
			continue
		}
		rec, err := segment.ReadAt(seg, r.Offset, r.Length)
		if err != nil {
			t.Fatal(err)
		}
		if rec.GetTombstone() == nil {
			t.Fatalf("%s: record of the withdrawn blob is %T, want a tombstone", key, rec.Kind)
		}
		return
	}
	t.Fatalf("%s has no row for the withdrawn blob", key)
}

// snapshot returns every file under root by key.
func snapshot(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		out[filepath.ToSlash(rel)] = b
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// countStore counts Puts and Deletes, can hide keys from List and fail
// catalog writes and the next failDeletes deletes. It records the options of
// the last Put per key.
type countStore struct {
	store.Store
	mu          sync.Mutex
	puts        int
	deletes     int
	failDeletes int
	putOpts     map[string]store.PutOptions
	hide        string
	failCatalog bool
}

var (
	errCatalog = errors.New("r2: 503 on catalog")
	errDelete  = errors.New("r2: 503 on delete")
)

func (s *countStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deletes++
	fail := s.failDeletes > 0
	if fail {
		s.failDeletes--
	}
	s.mu.Unlock()
	if fail {
		return errDelete
	}
	return s.Store.Delete(ctx, key)
}

func (s *countStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	s.puts++
	if s.putOpts == nil {
		s.putOpts = map[string]store.PutOptions{}
	}
	s.putOpts[key] = store.PutOptions{}
	if len(opts) > 0 {
		s.putOpts[key] = opts[0]
	}
	fail := s.failCatalog && strings.HasPrefix(key, "catalog/")
	s.mu.Unlock()
	if fail {
		return errCatalog
	}
	return s.Store.Put(ctx, key, b, opts...)
}

func (s *countStore) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := s.Store.List(ctx, prefix)
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.DeleteFunc(keys, func(k string) bool { return k == s.hide }), err
}

func (s *countStore) set(f func(s *countStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

// checkRewriteHeaders fails unless the last Put of key, a scrub rewrite,
// carried the short segment cache lifetime.
func (s *countStore) checkRewriteHeaders(t *testing.T, key string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	want := store.PutOptions{CacheControl: store.CacheSegment, ContentType: store.TypeBinary}
	if got, ok := s.putOpts[key]; !ok || got != want {
		t.Fatalf("scrub rewrite of %s: headers %+v (put %v), want %+v", key, got, ok, want)
	}
}

func TestTakedownScrubsCatalogedAndOrphanSegments(t *testing.T) {
	ctx := context.Background()
	blocks, blobs := takedownBlocks(t)
	root := t.TempDir()
	st := store.NewFS(root)
	archivetest.Build(t, st, blocks, 1, 5, 2, nil) // segments [1,2] [3,4] [5,5]

	// An orphan: a .seg of height 3 alone that no catalog or directory
	// references, as left by a crash before the catalog write.
	orphanDir := t.TempDir()
	archivetest.Build(t, store.NewFS(orphanDir), blocks, 3, 3, 1, nil)
	orphanKey := archive.SegmentKey(3, 3)
	orphan, err := os.ReadFile(filepath.Join(orphanDir, orphanKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, orphanKey, orphan); err != nil {
		t.Fatal(err)
	}
	catKey := archive.SegmentKey(3, 4)
	before := snapshot(t, root)

	set := withdraw(t, st, blobs[0])
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.ApplyTombstones(ctx, set)
	if err != nil || n != 2 {
		t.Fatalf("ApplyTombstones = %d, %v; want 2 records scrubbed", n, err)
	}
	for _, key := range []string{catKey, orphanKey} {
		checkScrubbed(t, st, key, blobs[0], blobs[1:])
	}
	after := snapshot(t, root)
	for k, b := range before {
		if k == catKey || k == orphanKey {
			continue
		}
		if !bytes.Equal(after[k], b) {
			t.Errorf("%s changed", k)
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok && k != tombstone.Key {
			t.Errorf("unexpected new object %s", k)
		}
	}
}

func TestApplyTombstonesNoopWhenApplied(t *testing.T) {
	ctx := context.Background()
	blocks, blobs := takedownBlocks(t)
	st := &countStore{Store: store.NewFS(t.TempDir())}
	archivetest.Build(t, st, blocks, 1, 5, 2, nil)
	set := withdraw(t, st, blobs[0])
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.ApplyTombstones(ctx, set); err != nil || n != 1 {
		t.Fatalf("first ApplyTombstones = %d, %v", n, err)
	}
	st.checkRewriteHeaders(t, archive.SegmentKey(3, 4))
	st.set(func(s *countStore) { s.puts = 0 })
	if n, err := c.ApplyTombstones(ctx, set); err != nil || n != 0 {
		t.Fatalf("second ApplyTombstones = %d, %v; want 0", n, err)
	}
	if st.puts != 0 {
		t.Fatalf("second ApplyTombstones wrote %d objects, want 0", st.puts)
	}
}

func TestTombstoneAddedDuringCommit(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		name := "listed"
		if hidden {
			// The in-flight .seg may not be listed yet (a PUT still running):
			// the commit must re-scrub it after its catalog write.
			name = "not listed"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			blocks, blobs := takedownBlocks(t)
			st := &countStore{Store: store.NewFS(t.TempDir())}
			ref := buildRef(t, blocks, 3, 4)
			if hidden {
				st.hide = ref.Key
			}
			c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
			if err != nil {
				t.Fatal(err)
			}
			archive.SetBeforeCatalog(c, func() {
				if _, err := c.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
					t.Error(err)
				}
			})
			if err := c.Commit(ctx, ref); err != nil {
				t.Fatal(err)
			}
			checkScrubbed(t, st, ref.Key, blobs[0], blobs[1:])
			st.checkRewriteHeaders(t, ref.Key)
		})
	}
}

// A commit that meets its own unscrubbed orphan (the takedown missed it)
// replaces it with the scrubbed version instead of failing with ErrExists;
// a segment that differs in anything but tombstones still fails.
func TestRecommitOverUnscrubbedOrphan(t *testing.T) {
	ctx := context.Background()
	blocks, blobs := takedownBlocks(t)
	st := &countStore{Store: store.NewFS(t.TempDir()), failCatalog: true}
	ref := buildRef(t, blocks, 3, 4)
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, ref); !errors.Is(err, errCatalog) {
		t.Fatalf("Commit = %v, want the catalog error", err)
	}
	st.set(func(s *countStore) { s.failCatalog, s.hide = false, ref.Key })
	if _, err := c.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, ref); err != nil {
		t.Fatalf("recommit: %v", err)
	}
	checkScrubbed(t, st, ref.Key, blobs[0], blobs[1:])
	st.checkRewriteHeaders(t, ref.Key)

	// Same key, but height 4 now carries the blobs of height 3 relabelled:
	// not explained by tombstones.
	other := archive.NewBuilder(nil)
	for h := int64(3); h <= 4; h++ {
		hr, bl, err := process.Process(blocks[h], blocks[h+1])
		if err != nil {
			t.Fatal(err)
		}
		if h == 4 {
			bl = []*archivev1.BlobRecord{{Height: 4, Namespace: blobs[1].Namespace, Commitment: blobs[1].Commitment, Data: []byte("x")}}
			hr.BlobCount = 1
		}
		if err := other.Add(hr, bl); err != nil {
			t.Fatal(err)
		}
	}
	oref, err := other.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, *oref); !errors.Is(err, store.ErrExists) {
		t.Fatalf("different segment: %v, want ErrExists", err)
	}
}

// opKeys returns the compact and full OP keys of b.
func opKeys(t *testing.T, b *archivev1.BlobRecord) [2]string {
	t.Helper()
	ob, err := oplayer.FromRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := ob.Keys()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// plantOP writes b's data under both its OP keys, as if both had been
// written (the writer writes the compact one only).
func plantOP(t *testing.T, st store.Store, b *archivev1.BlobRecord) {
	t.Helper()
	for _, k := range opKeys(t, b) {
		if err := st.Put(context.Background(), k, b.Data); err != nil {
			t.Fatal(err)
		}
	}
}

// opPresent returns how many of b's OP keys exist.
func opPresent(t *testing.T, st store.Store, b *archivev1.BlobRecord) int {
	t.Helper()
	n := 0
	for _, k := range opKeys(t, b) {
		ok, err := st.Exists(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			n++
		}
	}
	return n
}

// opWriter returns an OP writer for the namespace of b.
func opWriter(st store.Store, b *archivev1.BlobRecord) *oplayer.Writer {
	return oplayer.NewWriter(st, [][29]byte{[29]byte(b.Namespace)})
}

// Review Focus 1: a withdrawn blob's OP objects are deleted wherever its
// segment is (catalogued, in flight, orphaned) and whatever the configured
// namespaces are. The crash before the .seg write is covered by
// ingest.TestRunOPTakedownAfterCrashBeforeSegment.
func TestOPTakedownCoversInFlightCrashedAndUnconfigured(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, st *countStore, blocks map[int64]*source.Block, blobs []*archivev1.BlobRecord)
	}{
		{"committed", func(t *testing.T, st *countStore, blocks map[int64]*source.Block, blobs []*archivev1.BlobRecord) {
			ctx := context.Background()
			archivetest.Build(t, st, blocks, 1, 5, 2, nil)
			c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
			if err != nil {
				t.Fatal(err)
			}
			c.SetOPWriter(opWriter(st, blobs[0]))
			if _, err := c.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
				t.Fatal(err)
			}
		}},
		{"in flight", func(t *testing.T, st *countStore, blocks map[int64]*source.Block, blobs []*archivev1.BlobRecord) {
			ctx := context.Background()
			ref := buildRef(t, blocks, 3, 4)
			// Not listed yet: only the commit itself can find the blob.
			st.hide = ref.Key
			c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
			if err != nil {
				t.Fatal(err)
			}
			c.SetOPWriter(opWriter(st, blobs[0]))
			if _, err := c.ApplyTombstones(ctx, loadSet(t, st)); err != nil {
				t.Fatal(err)
			}
			archive.SetBeforeCatalog(c, func() {
				if _, err := c.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
					t.Error(err)
				}
				if opPresent(t, st, blobs[0]) == 0 {
					t.Error("the takedown found the uncatalogued segment; test proves nothing")
				}
			})
			if err := c.Commit(ctx, ref); err != nil {
				t.Fatal(err)
			}
		}},
		{"unconfigured", func(t *testing.T, st *countStore, blocks map[int64]*source.Block, blobs []*archivev1.BlobRecord) {
			ctx := context.Background()
			archivetest.Build(t, st, blocks, 1, 5, 2, nil)
			// Restarted with the blob's namespace removed from the OP list.
			c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
			if err != nil {
				t.Fatal(err)
			}
			c.SetOPWriter(opWriter(st, blobs[1]))
			if _, err := c.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
				t.Fatal(err)
			}
		}},
		{"orphan", func(t *testing.T, st *countStore, blocks map[int64]*source.Block, blobs []*archivev1.BlobRecord) {
			ctx := context.Background()
			ref := buildRef(t, blocks, 3, 3)
			st.failCatalog = true
			c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.ApplyTombstones(ctx, loadSet(t, st)); err != nil {
				t.Fatal(err)
			}
			if err := c.Commit(ctx, ref); !errors.Is(err, errCatalog) {
				t.Fatalf("Commit = %v, want the catalog error", err)
			}
			st.set(func(s *countStore) { s.failCatalog = false })
			c2, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c2.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blocks, blobs := takedownBlocks(t)
			st := &countStore{Store: store.NewFS(t.TempDir())}
			plantOP(t, st, blobs[0])
			plantOP(t, st, blobs[1])
			tc.run(t, st, blocks, blobs)
			if n := opPresent(t, st, blobs[0]); n != 0 {
				t.Fatalf("%d OP keys of the withdrawn blob left", n)
			}
			if n := opPresent(t, st, blobs[1]); n != 2 {
				t.Fatalf("%d OP keys of the other blob left, want 2", n)
			}
		})
	}
}

// A crashed height that is never re-committed has no segment at all; its
// compact OP key still goes, derived from the tombstone set alone.
func TestCompactKeyDeletedWithoutSegment(t *testing.T) {
	ctx := context.Background()
	_, blobs := takedownBlocks(t)
	st := store.NewFS(t.TempDir())
	for _, b := range blobs[:2] {
		if err := st.Put(ctx, opKeys(t, b)[0], b.Data); err != nil {
			t.Fatal(err)
		}
	}
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyTombstones(ctx, withdraw(t, st, blobs[0])); err != nil {
		t.Fatal(err)
	}
	if n := opPresent(t, st, blobs[0]); n != 0 {
		t.Fatalf("%d OP keys of the withdrawn blob left", n)
	}
	if n := opPresent(t, st, blobs[1]); n != 1 {
		t.Fatalf("%d OP keys of the other blob left, want its compact key", n)
	}
}

func TestApplyTombstonesDeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	blocks, blobs := takedownBlocks(t)
	st := &countStore{Store: store.NewFS(t.TempDir())}
	archivetest.Build(t, st, blocks, 1, 5, 2, nil)
	plantOP(t, st, blobs[0])
	set := withdraw(t, st, blobs[0])
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := c.ApplyTombstones(ctx, set); err != nil {
			t.Fatalf("apply %d: %v", i+1, err)
		}
		if n := opPresent(t, st, blobs[0]); n != 0 {
			t.Fatalf("apply %d: %d OP keys left", i+1, n)
		}
	}
	if st.deletes != 4 {
		t.Fatalf("%d deletes, want 2 per apply", st.deletes)
	}
}

// A failed delete fails ApplyTombstones (so the runner does not mark the set
// applied) and the next call deletes; a failed delete fails Commit too.
func TestApplyTombstonesDeleteError(t *testing.T) {
	ctx := context.Background()
	blocks, blobs := takedownBlocks(t)
	st := &countStore{Store: store.NewFS(t.TempDir()), failDeletes: 1}
	archivetest.Build(t, st, blocks, 1, 5, 2, nil)
	plantOP(t, st, blobs[0])
	set := withdraw(t, st, blobs[0])
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyTombstones(ctx, set); !errors.Is(err, errDelete) {
		t.Fatalf("ApplyTombstones = %v, want the delete error", err)
	}
	if opPresent(t, st, blobs[0]) == 0 {
		t.Fatal("the failed delete did not stop the takedown; test proves nothing")
	}
	if _, err := c.ApplyTombstones(ctx, set); err != nil {
		t.Fatal(err)
	}
	if n := opPresent(t, st, blobs[0]); n != 0 {
		t.Fatalf("%d OP keys left after the retry", n)
	}

	st2 := &countStore{Store: store.NewFS(t.TempDir())}
	c2, err := archive.NewCommitter(ctx, st2, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.ApplyTombstones(ctx, withdraw(t, st2, blobs[0])); err != nil {
		t.Fatal(err)
	}
	plantOP(t, st2, blobs[0])
	st2.set(func(s *countStore) { s.failDeletes = 1 })
	ref3 := buildRef(t, blocks, 3, 3)
	if err := c2.Commit(ctx, ref3); !errors.Is(err, errDelete) {
		t.Fatalf("Commit = %v, want the delete error", err)
	}
	if err := c2.Commit(ctx, ref3); err != nil {
		t.Fatalf("recommit: %v", err)
	}
	if n := opPresent(t, st2, blobs[0]); n != 0 {
		t.Fatalf("%d OP keys left after the recommit", n)
	}
}

// Without a tombstone set a committer cannot tell a scrubbed orphan from a
// different segment: the conflict is retryable, never ErrExists (which stops
// the ingester and invites deleting the scrubbed copy).
func TestReconcileWithoutTombstoneSetIsRetryable(t *testing.T) {
	ctx := context.Background()
	blocks, blobs := takedownBlocks(t)
	st := &countStore{Store: store.NewFS(t.TempDir()), failCatalog: true}
	ref := buildRef(t, blocks, 3, 4)
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	set := withdraw(t, st, blobs[0])
	if _, err := c.ApplyTombstones(ctx, set); err != nil {
		t.Fatal(err)
	}
	// A scrubbed orphan, as left by a commit whose catalog write failed.
	if err := c.Commit(ctx, ref); !errors.Is(err, errCatalog) {
		t.Fatalf("Commit = %v, want the catalog error", err)
	}
	checkScrubbed(t, st, ref.Key, blobs[0], blobs[1:])
	st.set(func(s *countStore) { s.failCatalog = false })

	c2, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	err = c2.Commit(ctx, ref)
	if errors.Is(err, store.ErrExists) || !errors.Is(err, archive.ErrNoTombstoneSet) {
		t.Fatalf("Commit without a tombstone set = %v, want ErrNoTombstoneSet and not ErrExists", err)
	}
	checkScrubbed(t, st, ref.Key, blobs[0], blobs[1:])
	if _, err := c2.ApplyTombstones(ctx, set); err != nil {
		t.Fatal(err)
	}
	if err := c2.Commit(ctx, ref); err != nil {
		t.Fatalf("Commit once the set is loaded: %v", err)
	}
	checkScrubbed(t, st, ref.Key, blobs[0], blobs[1:])
}
