package archive_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/nsindex"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

func v1FixtureDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "archive-v1")
}

// readTree returns every file under dir by slash-separated relative path.
func readTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = b
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// copyV1Fixture copies testdata/archive-v1 into a fresh directory.
func copyV1Fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for k, b := range readTree(t, v1FixtureDir()) {
		p := filepath.Join(dir, filepath.FromSlash(k))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// v1FixtureBlocks returns the blocks testdata/archive-v1 was built from.
func v1FixtureBlocks(t *testing.T) map[int64]*source.Block {
	return archivetest.Blocks(t, 10, map[int64]string{
		2: "mocha-1834013",
		5: "mocha-1834076",
		8: "mocha-2035911-fibre",
	})
}

type headers map[int64]*source.Block

func (h headers) Block(_ context.Context, height int64) (*source.Block, error) {
	b, ok := h[height]
	if !ok {
		return nil, fmt.Errorf("no block %d", height)
	}
	return b, nil
}

// v1Blobs returns the blob entries of every v1 .idx of the fixture, in segment
// order.
func v1Blobs(t *testing.T, tree map[string][]byte) []nsindex.Entry {
	t.Helper()
	var out []nsindex.Entry
	for _, seg := range []string{"000000000001-000000000003", "000000000004-000000000006", "000000000007-000000000009"} {
		var idx archivev1.SegmentIndex
		if err := proto.Unmarshal(tree["segments/"+seg+".idx"], &idx); err != nil {
			t.Fatal(err)
		}
		for _, e := range idx.GetEntries() {
			if e.GetKind() != segment.KindBlob {
				continue
			}
			out = append(out, nsindex.Entry{
				Height: int64(e.GetHeight()), Commitment: hex.EncodeToString(e.GetCommitment()),
				SegmentKey: "segments/" + seg + ".seg", Offset: e.GetOffset(), Length: e.GetLength(),
			})
		}
	}
	return out
}

func TestMigrateV2Fixture(t *testing.T) {
	ctx := context.Background()
	dir := copyV1Fixture(t)
	st := store.NewFS(dir)
	before := readTree(t, dir)

	stats, err := archive.MigrateV2(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (archive.MigrateStats{Segments: 3, Buckets: 1}) {
		t.Fatalf("stats %+v", stats)
	}
	after := readTree(t, dir)

	// .seg and v1 .idx are untouched; the v1 manifest and catalog are backed up.
	for k, b := range before {
		switch filepath.Ext(k) {
		case ".seg", ".idx":
			if !bytes.Equal(after[k], b) {
				t.Errorf("%s changed", k)
			}
		}
		if k == archive.ManifestKey || filepath.Dir(k) == "catalog" {
			if !bytes.Equal(after["migrate-v1/"+k], b) {
				t.Errorf("migrate-v1/%s is not the v1 object", k)
			}
		}
	}
	m, err := archiveread.LoadManifest(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if m.FormatVersion != 2 || m.ChainID != archivetest.ChainID || m.HighestHeight != 9 || m.ContiguousUntil != 9 {
		t.Fatalf("manifest %+v", m)
	}
	if _, err := archive.NewCommitter(ctx, st, archivetest.ChainID); err != nil {
		t.Fatalf("v2 committer on migrated archive: %v", err)
	}

	// Each .idx2 is what the v2 builder would write for the segment.
	refs, err := archiveread.LoadCatalog(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range refs {
		rows, err := segment.ScanRows(after[r.Key])
		if err != nil {
			t.Fatal(err)
		}
		want, err := segment.EncodeRows(uint64(r.First), uint64(r.Last), rows)
		if err != nil {
			t.Fatal(err)
		}
		if got := after[archive.IndexKey(r.First, r.Last)]; !bytes.Equal(got, want) {
			t.Errorf("%s differs from the rows of %s", archive.IndexKey(r.First, r.Last), r.Key)
		}
	}
	if _, es, err := archive.DecodeDir(after[archive.DirKey(0)]); err != nil {
		t.Fatal(err)
	} else if es[9] != (archive.DirEntry{}) || es[8].RowCount == 0 {
		t.Fatalf("dir/00000000.dir: slot 9 %+v, slot 8 %+v", es[9], es[8])
	}
	assertV1Readable(t, st)

	// Every blob reads and verifies through the directory.
	blobs := v1Blobs(t, before)
	if len(blobs) < 3 {
		t.Fatalf("fixture has %d blobs", len(blobs))
	}
	hs := headers(v1FixtureBlocks(t))
	for _, e := range blobs {
		rec, err := segment.ReadAt(before[e.SegmentKey], e.Offset, e.Length)
		if err != nil {
			t.Fatal(err)
		}
		b := rec.GetBlob()
		_, got, err := archiveread.ReadBlob(ctx, st, e.Height, b.GetNamespace(), b.GetCommitment())
		if err != nil {
			t.Fatalf("read blob at %d: %v", e.Height, err)
		}
		if !proto.Equal(got.GetBlob(), b) {
			t.Fatalf("blob at %d differs", e.Height)
		}
		p := verify.Params{Height: e.Height, Namespace: b.GetNamespace(), Commitment: b.GetCommitment()}
		if err := verify.Verify(ctx, st, hs, p); err != nil {
			t.Fatalf("verify blob at %d: %v", e.Height, err)
		}
	}

	// The namespace index holds the same entries v1 pointed at.
	if err := nsindex.Update(ctx, st); err != nil {
		t.Fatal(err)
	}
	got := map[nsindex.Entry]bool{}
	for k, b := range readTree(t, dir) {
		if filepath.Dir(filepath.Dir(k)) != "ns" || filepath.Base(k) == "head.json" {
			continue
		}
		var ch nsindex.Chunk
		if err := json.Unmarshal(b, &ch); err != nil {
			t.Fatal(err)
		}
		for _, e := range ch.Entries {
			got[e] = true
		}
	}
	if len(got) != len(blobs) {
		t.Fatalf("namespace index has %d entries, v1 index %d", len(got), len(blobs))
	}
	for _, e := range blobs {
		if !got[e] {
			t.Errorf("namespace index lacks %+v", e)
		}
	}

	// The v2 ingester commits on top of the migrated catalog and drops
	// index_key when it rewrites the open chunk.
	more := archivetest.Blocks(t, 11, map[int64]string{
		2: "mocha-1834013",
		5: "mocha-1834076",
		8: "mocha-2035911-fibre",
	})
	archivetest.Build(t, st, more, 10, 10, 1, nil)
	cb, err := st.Get(ctx, archive.CatalogKey(0))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(cb, []byte("index_key")) || !bytes.Contains(cb, []byte(archive.SegmentKey(10, 10))) {
		t.Fatalf("catalog after a v2 commit: %s", cb)
	}
}

// assertV1Readable checks the v1 reader path on the live catalog: every ref
// names its v1 .idx, which exists and covers the ref's range.
func assertV1Readable(t *testing.T, st store.Store) {
	t.Helper()
	ctx := context.Background()
	refs := 0
	for i := 0; ; i++ {
		b, err := st.Get(ctx, archive.CatalogKey(i))
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var ch struct {
			Segments []struct {
				First    int64  `json:"first"`
				Last     int64  `json:"last"`
				IndexKey string `json:"index_key"`
			} `json:"segments"`
		}
		if err := json.Unmarshal(b, &ch); err != nil {
			t.Fatalf("v1 decode of catalog chunk %d: %v", i, err)
		}
		for _, r := range ch.Segments {
			if r.IndexKey == "" {
				t.Fatalf("catalog chunk %d: ref [%d,%d] has no index_key", i, r.First, r.Last)
			}
			raw, err := st.Get(ctx, r.IndexKey)
			if err != nil {
				t.Fatalf("v1 index %s: %v", r.IndexKey, err)
			}
			var idx archivev1.SegmentIndex
			if err := proto.Unmarshal(raw, &idx); err != nil {
				t.Fatal(err)
			}
			if idx.GetFirstHeight() != uint64(r.First) || idx.GetLastHeight() != uint64(r.Last) || len(idx.GetEntries()) == 0 {
				t.Fatalf("v1 index %s does not cover [%d,%d]", r.IndexKey, r.First, r.Last)
			}
			refs++
		}
	}
	if refs == 0 {
		t.Fatal("no catalog refs")
	}
}

// writeStore counts writes and, once limit writes succeeded, fails every
// further one. With after set, the failing write reaches the store first (a
// lost acknowledgement).
type writeStore struct {
	store.Store
	mu     sync.Mutex
	writes int
	limit  int // -1: no limit
	after  bool
	opts   map[string]store.PutOptions
}

var errInjected = errors.New("injected write failure")

func (w *writeStore) write(key string, opts []store.PutOptions, do func() error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.limit >= 0 && w.writes >= w.limit {
		if w.after {
			_ = do()
		}
		return errInjected
	}
	w.writes++
	if w.opts != nil && len(opts) > 0 {
		w.opts[key] = opts[0]
	}
	return do()
}

func (w *writeStore) Put(ctx context.Context, key string, body []byte, opts ...store.PutOptions) error {
	return w.write(key, opts, func() error { return w.Store.Put(ctx, key, body, opts...) })
}

func (w *writeStore) PutIfAbsent(ctx context.Context, key string, body []byte, opts ...store.PutOptions) error {
	return w.write(key, opts, func() error { return w.Store.PutIfAbsent(ctx, key, body, opts...) })
}

func TestMigrateV2Idempotent(t *testing.T) {
	ctx := context.Background()
	dir := copyV1Fixture(t)
	if _, err := archive.MigrateV2(ctx, store.NewFS(dir), archivetest.ChainID); err != nil {
		t.Fatal(err)
	}
	want := readTree(t, dir)
	ws := &writeStore{Store: store.NewFS(dir), limit: -1}
	stats, err := archive.MigrateV2(ctx, ws, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if ws.writes != 0 || stats != (archive.MigrateStats{AlreadyV2: true}) {
		t.Fatalf("second run: %d writes, stats %+v", ws.writes, stats)
	}
	got := readTree(t, dir)
	if len(got) != len(want) {
		t.Fatalf("second run changed the object set")
	}
}

// Review Focus 5: a migration interrupted at any write finishes on rerun to
// the same bytes, and the manifest stays v1 until everything else exists.
func TestMigrateV2Interrupted(t *testing.T) {
	ctx := context.Background()
	refDir := copyV1Fixture(t)
	ref := &writeStore{Store: store.NewFS(refDir), limit: -1}
	if _, err := archive.MigrateV2(ctx, ref, archivetest.ChainID); err != nil {
		t.Fatal(err)
	}
	k := ref.writes
	want := readTree(t, refDir)
	v1Manifest := readTree(t, v1FixtureDir())[archive.ManifestKey]

	for _, after := range []bool{false, true} {
		for n := 0; n < k; n++ {
			t.Run(fmt.Sprintf("after=%v/n=%d", after, n), func(t *testing.T) {
				dir := copyV1Fixture(t)
				ws := &writeStore{Store: store.NewFS(dir), limit: n, after: after}
				if _, err := archive.MigrateV2(ctx, ws, archivetest.ChainID); !errors.Is(err, errInjected) {
					t.Fatalf("interrupted run: %v", err)
				}
				// Only the last write is the manifest; a lost ack of it is a
				// finished migration.
				if m := readTree(t, dir)[archive.ManifestKey]; !(after && n == k-1) && !bytes.Equal(m, v1Manifest) {
					t.Fatalf("manifest changed by a failed run: %s", m)
				}
				// Including after the catalog writes: the bucket stays v1.
				assertV1Readable(t, store.NewFS(dir))
				if _, err := archive.MigrateV2(ctx, store.NewFS(dir), archivetest.ChainID); err != nil {
					t.Fatalf("rerun: %v", err)
				}
				got := readTree(t, dir)
				if len(got) != len(want) {
					t.Fatalf("rerun: %d objects, want %d", len(got), len(want))
				}
				for key, b := range want {
					if !bytes.Equal(got[key], b) {
						t.Errorf("rerun: %s differs from an uninterrupted run", key)
					}
				}
			})
		}
	}
}

// writeV1 stores a synthetic v1 archive whose segments have one height row
// per height (the migration never reads .seg).
func writeV1(t *testing.T, st store.Store, ranges [][2]int64) {
	t.Helper()
	ctx := context.Background()
	type v1Ref struct {
		First    int64  `json:"first"`
		Last     int64  `json:"last"`
		Key      string `json:"key"`
		IndexKey string `json:"index_key"`
		Size     int64  `json:"size"`
	}
	var refs []v1Ref
	var highest int64
	for _, r := range ranges {
		idx := &archivev1.SegmentIndex{FirstHeight: uint64(r[0]), LastHeight: uint64(r[1])}
		for h := r[0]; h <= r[1]; h++ {
			idx.Entries = append(idx.Entries, &archivev1.IndexEntry{
				Height: uint64(h), Offset: uint64(h-r[0])*10 + 1, Length: 9, Kind: segment.KindHeight,
			})
		}
		b, err := proto.Marshal(idx)
		if err != nil {
			t.Fatal(err)
		}
		ik := fmt.Sprintf("segments/%012d-%012d.idx", r[0], r[1])
		if err := st.Put(ctx, ik, b); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, v1Ref{r[0], r[1], archive.SegmentKey(r[0], r[1]), ik, (r[1] - r[0] + 1) * 10})
		highest = max(highest, r[1])
	}
	cb, err := json.Marshal(map[string]any{"segments": refs})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, archive.CatalogKey(0), cb); err != nil {
		t.Fatal(err)
	}
	mb, err := json.Marshal(map[string]any{
		"format_version": 1, "chain_id": archivetest.ChainID, "highest_height": highest,
		"contiguous_until": highest, "last_catalog_chunk": 0, "updated_at": "2026-10-26T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, archive.ManifestKey, mb); err != nil {
		t.Fatal(err)
	}
}

// A bucket the catalog covers completely is written immutable; a partly
// covered one stays no-cache. Segments may span buckets.
func TestMigrateV2FreezesCoveredBuckets(t *testing.T) {
	ctx := context.Background()
	fsst := store.NewFS(t.TempDir())
	writeV1(t, fsst, [][2]int64{{1, 9998}, {9999, 10002}, {10003, 10005}})
	ws := &writeStore{Store: fsst, limit: -1, opts: map[string]store.PutOptions{}}
	stats, err := archive.MigrateV2(ctx, ws, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (archive.MigrateStats{Segments: 3, Buckets: 2}) {
		t.Fatalf("stats %+v", stats)
	}
	if got := ws.opts[archive.DirKey(0)].CacheControl; got != store.CacheImmutable {
		t.Errorf("dir bucket 0: Cache-Control %q, want immutable", got)
	}
	if got := ws.opts[archive.DirKey(1)].CacheControl; got != store.CacheNoCache {
		t.Errorf("dir bucket 1: Cache-Control %q, want no-cache", got)
	}
	if got := ws.opts[archive.IndexKey(1, 9998)].CacheControl; got != store.CacheImmutable {
		t.Errorf(".idx2: Cache-Control %q, want immutable", got)
	}
	if got := ws.opts[archive.ManifestKey].CacheControl; got != store.CacheNoCache {
		t.Errorf("manifest: Cache-Control %q, want no-cache", got)
	}
	for _, h := range []int64{1, 9998, 9999, 10000, 10001, 10002, 10005} {
		e, err := archiveread.Locate(ctx, fsst, h)
		if err != nil {
			t.Fatalf("locate %d: %v", h, err)
		}
		if h < e.SegFirst || h > e.SegLast || e.RowCount != 1 || int64(e.RowStart) != h-e.SegFirst {
			t.Fatalf("locate %d: %+v", h, e)
		}
	}
	if _, err := archiveread.Locate(ctx, fsst, 10006); !errors.Is(err, archiveread.ErrNotArchived) {
		t.Fatalf("locate 10006: %v, want not archived", err)
	}
}

func TestMigrateV2Rejects(t *testing.T) {
	ctx := context.Background()
	t.Run("chain id", func(t *testing.T) {
		st := store.NewFS(copyV1Fixture(t))
		if _, err := archive.MigrateV2(ctx, st, "other-chain"); err == nil {
			t.Fatal("migrated an archive of another chain")
		}
	})
	t.Run("no manifest", func(t *testing.T) {
		if _, err := archive.MigrateV2(ctx, store.NewFS(t.TempDir()), archivetest.ChainID); err == nil {
			t.Fatal("migrated an empty store")
		}
	})
	t.Run("index does not match catalog", func(t *testing.T) {
		dir := copyV1Fixture(t)
		st := store.NewFS(dir)
		other, err := st.Get(ctx, "segments/000000000004-000000000006.idx")
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Put(ctx, "segments/000000000001-000000000003.idx", other); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.MigrateV2(ctx, st, archivetest.ChainID); err == nil {
			t.Fatal("migrated an index of another segment")
		}
		if m := readTree(t, dir)[archive.ManifestKey]; !bytes.Contains(m, []byte(`"format_version":1`)) {
			t.Fatalf("manifest changed: %s", m)
		}
	})
}
