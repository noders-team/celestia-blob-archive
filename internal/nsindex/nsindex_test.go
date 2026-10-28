package nsindex

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// putSpy records the keys written with Put and the options of the last write.
type putSpy struct {
	store.Store
	mu   sync.Mutex
	puts []string
	opts map[string]store.PutOptions
}

func (s *putSpy) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	if s.opts == nil {
		s.opts = map[string]store.PutOptions{}
	}
	var o store.PutOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	s.opts[key] = o
	s.mu.Unlock()
	return s.Store.Put(ctx, key, b, opts...)
}

func (s *putSpy) reset() { s.puts = nil }

func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	base := filepath.Join(root, "ns")
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func readJSON(t *testing.T, st store.Store, key string, v any) {
	t.Helper()
	b, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
}

// expected lists, per ns hex, the (height, commitment) of every blob of the
// archived heights first..last, in archive order.
func expected(t *testing.T, st store.Store, first, last int64) map[string][][2]string {
	t.Helper()
	ctx := context.Background()
	out := map[string][][2]string{}
	for h := first; h <= last; h++ {
		_, recs, err := archiveread.ReadHeight(ctx, st, h)
		if err != nil {
			t.Fatalf("height %d: %v", h, err)
		}
		for _, r := range recs {
			b := r.GetBlob()
			if b == nil {
				t.Fatalf("height %d: tombstone record", h)
			}
			ns := hex.EncodeToString(b.Namespace)
			out[ns] = append(out[ns], [2]string{itoa(h), hex.EncodeToString(b.Commitment)})
		}
	}
	return out
}

func itoa(h int64) string { b, _ := json.Marshal(h); return string(b) }

// readNS reads every chunk of a namespace through head.json.
func readNS(t *testing.T, st store.Store, ns string) (Head, [][]Entry) {
	t.Helper()
	var h Head
	readJSON(t, st, HeadKey(ns), &h)
	var chunks [][]Entry
	for i := 0; i <= h.LastSeq; i++ {
		var c Chunk
		readJSON(t, st, ChunkKey(ns, i), &c)
		chunks = append(chunks, c.Entries)
	}
	return h, chunks
}

func sortedKeys[V any](m map[string]V) []string {
	var k []string
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}

func TestNsIndexOnlyContiguousAndIdempotent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := store.NewFS(root)
	blocks := archivetest.Blocks(t, 13, map[int64]string{
		2: "mocha-1834013", 4: "mainnet-1500004", 11: "mocha-1834013",
	})
	archivetest.Build(t, st, blocks, 1, 6, 2, nil)
	archivetest.Build(t, st, blocks, 9, 12, 2, nil) // gap at 7,8: contiguous_until stays 6

	if err := Update(ctx, st); err != nil {
		t.Fatal(err)
	}
	snap1 := snapshot(t, root)
	if err := Update(ctx, st); err != nil {
		t.Fatal(err)
	}
	if snap2 := snapshot(t, root); !reflect.DeepEqual(snap1, snap2) {
		t.Fatal("second Update changed objects")
	}

	var prog Progress
	readJSON(t, st, ProgressKey, &prog)
	if prog.LastHeight != 6 {
		t.Fatalf("progress %+v, want last_height 6", prog)
	}
	if !strings.Contains(snap1[ProgressKey], `"last_height":6`) {
		t.Fatalf("progress not snake_case: %s", snap1[ProgressKey])
	}

	want := expected(t, st, 1, 6)
	if len(want) < 2 {
		t.Fatalf("fixture namespaces: %d, want at least 2", len(want))
	}
	nsDirs := map[string]bool{}
	for k := range snap1 {
		if strings.HasPrefix(k, "ns/") && k != ProgressKey {
			nsDirs[strings.Split(k, "/")[1]] = true
		}
	}
	if !reflect.DeepEqual(sortedKeys(nsDirs), sortedKeys(want)) {
		t.Fatalf("namespaces %v, want %v", sortedKeys(nsDirs), sortedKeys(want))
	}
	for ns, w := range want {
		if len(ns) != 58 || ns != strings.ToLower(ns) {
			t.Fatalf("ns_hex %q", ns)
		}
		h, chunks := readNS(t, st, ns)
		if h.Blobs != len(w) || h.LastSeq != 0 || len(chunks) != 1 {
			t.Fatalf("%s head %+v", ns, h)
		}
		var got [][2]string
		for _, e := range chunks[0] {
			got = append(got, [2]string{itoa(e.Height), e.Commitment})
			if e.SegmentKey == "" || e.Length == 0 {
				t.Fatalf("entry %+v", e)
			}
		}
		if !reflect.DeepEqual(got, w) {
			t.Fatalf("%s entries %v, want %v", ns, got, w)
		}
		if h.FirstHeight != int64(atoi(w[0][0])) || h.LastHeight != int64(atoi(w[len(w)-1][0])) {
			t.Fatalf("%s head heights %+v", ns, h)
		}
	}
	if !strings.Contains(snap1[HeadKey(sortedKeys(want)[0])], `"first_height"`) {
		t.Fatal("head.json not snake_case")
	}

	// Fill the gap: the later heights now get indexed.
	archivetest.Build(t, st, blocks, 7, 8, 2, nil)
	if err := Update(ctx, st); err != nil {
		t.Fatal(err)
	}
	readJSON(t, st, ProgressKey, &prog)
	if prog.LastHeight != 12 {
		t.Fatalf("progress %+v, want 12", prog)
	}
	snap3 := snapshot(t, root)
	want = expected(t, st, 1, 12)
	for ns, w := range want {
		h, chunks := readNS(t, st, ns)
		var got [][2]string
		for _, e := range chunks[0] {
			got = append(got, [2]string{itoa(e.Height), e.Commitment})
		}
		if h.Blobs != len(w) || !reflect.DeepEqual(got, w) {
			t.Fatalf("%s after fill: head %+v entries %v want %v", ns, h, got, w)
		}
	}

	// Crash between namespace writes and the progress write: roll progress and
	// one head back to the previous state; a rerun must converge to snap3.
	for k, v := range snap1 {
		if k == ProgressKey || strings.HasSuffix(k, "head.json") {
			if err := st.Put(ctx, k, []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := Update(ctx, st); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, root); !reflect.DeepEqual(got, snap3) {
		t.Fatal("rerun after simulated crash did not converge to the same objects")
	}
}

func atoi(s string) int {
	var n int
	_ = json.Unmarshal([]byte(s), &n)
	return n
}

func TestNsIndexNoManifest(t *testing.T) {
	if err := Update(context.Background(), store.NewFS(t.TempDir())); err != nil {
		t.Fatalf("empty archive: %v", err)
	}
}

// A row index that does not decode, or covers other heights than its
// catalog entry, is an archive defect: ErrCorrupt.
func TestNsIndexCorruptIndexIsErrCorrupt(t *testing.T) {
	ctx := context.Background()
	key := "segments/000000000001-000000000002.idx2"
	for name, body := range map[string]func([]byte) []byte{
		"garbage":   func([]byte) []byte { return []byte{0xff, 0xff, 0xff, 0xff} },
		"truncated": func(b []byte) []byte { return b[:len(b)-1] },
		"other range": func(b []byte) []byte {
			first, last, rows, err := segment.DecodeRows(b)
			if err != nil {
				t.Fatal(err)
			}
			out, err := segment.EncodeRows(first, last+1, rows)
			if err != nil {
				t.Fatal(err)
			}
			return out
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := store.NewFS(t.TempDir())
			archivetest.Build(t, st, archivetest.Blocks(t, 5, nil), 1, 4, 2, nil)
			b, err := st.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Put(ctx, key, body(b)); err != nil {
				t.Fatal(err)
			}
			if err := Update(ctx, st); !errors.Is(err, archiveread.ErrCorrupt) {
				t.Fatalf("Update = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestNsIndexFrozenChunkNotRewritten(t *testing.T) {
	old := chunkEntries
	chunkEntries = 3
	t.Cleanup(func() { chunkEntries = old })

	ctx := context.Background()
	spy := &putSpy{Store: store.NewFS(t.TempDir())}
	// mocha-1834013 carries two blobs in one namespace.
	blocks := archivetest.Blocks(t, 14, map[int64]string{
		2: "mocha-1834013", 6: "mocha-1834013", 9: "mocha-1834013", 12: "mocha-1834013",
	})
	ns := "00000000000000000000000000000000000000636c6235656637626138"

	steps := []struct {
		first, last int64
		blobs, seq  int
		sizes       []int
		notPut      []int // chunks that must not be written in this step
	}{
		{1, 3, 2, 0, []int{2}, nil},
		{4, 7, 4, 1, []int{3, 1}, nil},       // chunk 0 is filled by a rewrite and frozen
		{8, 10, 6, 1, []int{3, 3}, []int{0}}, // chunk 1 is filled
		{11, 13, 8, 2, []int{3, 3, 2}, []int{0, 1}},
	}
	for i, s := range steps {
		archivetest.Build(t, spy, blocks, s.first, s.last, 3, nil)
		spy.reset()
		if err := Update(ctx, spy); err != nil {
			t.Fatal(err)
		}
		h, chunks := readNS(t, spy, ns)
		if h.Blobs != s.blobs || h.LastSeq != s.seq || len(chunks) != len(s.sizes) {
			t.Fatalf("step %d: head %+v, %d chunks", i, h, len(chunks))
		}
		for j, n := range s.sizes {
			if len(chunks[j]) != n {
				t.Fatalf("step %d: chunk %d has %d entries, want %d", i, j, len(chunks[j]), n)
			}
		}
		immutable := store.PutOptions{CacheControl: store.CacheImmutable, ContentType: store.TypeJSON}
		mutable := store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}
		for _, k := range spy.puts {
			// A chunk written full is frozen; everything else is mutable.
			want := mutable
			if !strings.HasSuffix(k, "/head.json") && k != ProgressKey {
				var c Chunk
				if b, err := spy.Get(ctx, k); err != nil || json.Unmarshal(b, &c) != nil {
					t.Fatalf("read %s: %v", k, err)
				}
				if len(c.Entries) == chunkEntries {
					want = immutable
				}
			}
			if got := spy.opts[k]; got != want {
				t.Fatalf("step %d: %s headers %+v, want %+v", i, k, got, want)
			}
			for _, c := range s.notPut {
				if k == ChunkKey(ns, c) {
					t.Fatalf("step %d: frozen chunk %s was rewritten", i, k)
				}
			}
		}
	}
}

// errStore fails every Get.
type errStore struct{ store.Store }

func (errStore) Get(context.Context, string) ([]byte, error) { return nil, errors.New("r2: 503") }

func TestNsIndexMetrics(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	tick(ctx, st, m) // empty archive: nothing indexed, no error
	if e, h := testutil.ToFloat64(m.Errors), testutil.ToFloat64(m.LastHeight); e != 0 || h != 0 {
		t.Fatalf("empty: errors %v last_height %v", e, h)
	}

	blocks := archivetest.Blocks(t, 7, map[int64]string{2: "mocha-1834013"})
	archivetest.Build(t, st, blocks, 1, 6, 2, nil)
	tick(ctx, st, m)
	if e, h := testutil.ToFloat64(m.Errors), testutil.ToFloat64(m.LastHeight); e != 0 || h != 6 {
		t.Fatalf("after update: errors %v last_height %v", e, h)
	}

	tick(ctx, errStore{st}, m)
	if e, h := testutil.ToFloat64(m.Errors), testutil.ToFloat64(m.LastHeight); e != 1 || h != 6 {
		t.Fatalf("after failure: errors %v last_height %v", e, h)
	}

	names := map[string]bool{}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	if !names["cba_nsindex_errors_total"] || !names["cba_nsindex_last_height"] {
		t.Fatalf("registered metrics: %v", names)
	}
}
