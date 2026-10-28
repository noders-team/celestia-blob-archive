package ingest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/pkg/da"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

var (
	emptyHashOnce sync.Once
	emptyHash     []byte
)

// emptyDataHash is the data root of an empty app-version-10 square.
func emptyDataHash(t *testing.T) []byte {
	t.Helper()
	emptyHashOnce.Do(func() {
		eds, err := da.ConstructEDS(nil, 10, -1)
		if err != nil {
			panic(err)
		}
		dah, err := da.NewDataAvailabilityHeader(eds)
		if err != nil {
			panic(err)
		}
		emptyHash = dah.Hash()
	})
	return emptyHash
}

// mkBlocks returns empty blocks 1..n.
func mkBlocks(t *testing.T, n int64) map[int64]*source.Block {
	t.Helper()
	dh := emptyDataHash(t)
	base := time.Unix(1_700_000_000, 0).UTC()
	m := make(map[int64]*source.Block, n)
	for h := int64(1); h <= n; h++ {
		m[h] = &source.Block{
			Height:     h,
			Time:       base.Add(time.Duration(h) * 6 * time.Second),
			AppVersion: 10,
			DataHash:   dh,
		}
	}
	archivetest.Link(t, m)
	return m
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock { return &testClock{now: time.Unix(1_800_000_000, 0)} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// hookSource calls onBlock before every Block call.
type hookSource struct {
	source.Source
	onBlock func(h int64)
}

func (s *hookSource) Block(ctx context.Context, h int64) (*source.Block, error) {
	s.onBlock(h)
	return s.Source.Block(ctx, h)
}

func testConfig() Config {
	return Config{
		ChainID:          "mocha-test",
		Workers:          3,
		MaxWorkers:       4,
		TailDelay:        2,
		RangeSize:        40,
		LatencyTarget:    time.Second,
		HeadPollInterval: 2 * time.Millisecond,
		BackoffBase:      time.Millisecond,
		BackoffMax:       4 * time.Millisecond,
	}
}

type env struct {
	root  string
	st    store.Store
	clock *testClock
	m     *Metrics
}

func newEnv(t *testing.T) *env {
	root := t.TempDir()
	return &env{root: root, st: store.NewFS(root), clock: newTestClock(), m: NewMetrics(prometheus.NewRegistry())}
}

func (e *env) deps(src source.Source) Deps {
	return Deps{Source: src, Store: e.st, Clock: e.clock.Now, Metrics: e.m}
}

func (e *env) manifest(t *testing.T) (archive.Manifest, bool) {
	t.Helper()
	b, err := e.st.Get(context.Background(), archive.ManifestKey)
	if errors.Is(err, store.ErrNotFound) {
		return archive.Manifest{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var m archive.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m, true
}

// runUntil runs the ingester until the manifest reports ContiguousUntil ==
// target, advancing the clock so open segments close, then stops it.
// onTick runs on every poll.
func (e *env) runUntil(t *testing.T, cfg Config, src source.Source, target int64, onTick func()) {
	t.Helper()
	e.runUntilStep(t, cfg, src, target, onTick, 2*time.Hour)
}

// runUntilStep is runUntil advancing the clock by step per poll.
func (e *env) runUntilStep(t *testing.T, cfg Config, src source.Source, target int64, onTick func(), step time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, e.deps(src)) }()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("Run returned early: %v", err)
		case <-deadline:
			m, _ := e.manifest(t)
			t.Fatalf("timeout: contiguous_until %d, want %d", m.ContiguousUntil, target)
		case <-time.After(5 * time.Millisecond):
		}
		if onTick != nil {
			onTick()
		}
		e.clock.Advance(step)
		if m, ok := e.manifest(t); ok && m.ContiguousUntil == target {
			break
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after cancel: %v, want context.Canceled", err)
	}
}

// heightRecords scans every .idx2 and returns the height record bytes by
// height, failing if a height appears twice.
func (e *env) heightRecords(t *testing.T) map[int64][]byte {
	t.Helper()
	out := map[int64][]byte{}
	for _, idxKey := range e.indexKeys(t) {
		b, err := e.st.Get(context.Background(), idxKey)
		if err != nil {
			t.Fatal(err)
		}
		_, _, rows, err := segment.DecodeRows(b)
		if err != nil {
			t.Fatalf("%s: %v", idxKey, err)
		}
		segKey := strings.TrimSuffix(idxKey, ".idx2") + ".seg"
		for _, en := range rows {
			if en.Kind != segment.KindHeight {
				continue
			}
			h := int64(en.Height)
			if _, dup := out[h]; dup {
				t.Fatalf("height %d archived twice (again in %s)", h, idxKey)
			}
			rec, err := e.st.GetRange(context.Background(), segKey, int64(en.Offset), int64(en.Length))
			if err != nil {
				t.Fatal(err)
			}
			out[h] = rec
		}
	}
	return out
}

func (e *env) indexKeys(t *testing.T) []string {
	t.Helper()
	var keys []string
	dir := filepath.Join(e.root, "segments")
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(p, ".idx2") {
			keys = append(keys, "segments/"+d.Name())
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return keys
}

func checkExactly(t *testing.T, recs map[int64][]byte, last int64) {
	t.Helper()
	for h := int64(1); h <= last; h++ {
		if _, ok := recs[h]; !ok {
			t.Fatalf("height %d missing", h)
		}
	}
	if len(recs) != int(last) {
		t.Fatalf("archived %d heights, want %d", len(recs), last)
	}
}

func TestBackfillAndTailFillAllHeights(t *testing.T) {
	e := newEnv(t)
	src := source.NewFake(mkBlocks(t, 520), 500)
	moved := false
	e.runUntil(t, testConfig(), src, 518, func() {
		if _, ok := e.manifest(t); ok && !moved {
			src.SetHead(520)
			moved = true
		}
	})
	m, _ := e.manifest(t)
	if m.ContiguousUntil != 518 || m.HighestHeight != 518 {
		t.Fatalf("manifest %+v, want contiguous/highest 518", m)
	}
	checkExactly(t, e.heightRecords(t), 518)
	if got := testutil.ToFloat64(e.m.ContiguousUntil); got != 518 {
		t.Fatalf("cba_contiguous_until %v", got)
	}
	if got := testutil.ToFloat64(e.m.ChainHead); got != 520 {
		t.Fatalf("cba_chain_head_height %v", got)
	}
	if got := testutil.ToFloat64(e.m.BlocksProcessed); got < 518 {
		t.Fatalf("cba_blocks_processed_total %v", got)
	}
}

func TestResumeAfterCrash(t *testing.T) {
	const n, target = 300, 298
	blocks := mkBlocks(t, n)

	ref := newEnv(t)
	ref.runUntil(t, testConfig(), source.NewFake(blocks, n), target, nil)
	want := ref.heightRecords(t)

	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	src := &hookSource{Source: source.NewFake(blocks, n), onBlock: func(h int64) {
		if h == 150 {
			once.Do(cancel)
		}
	}}
	if err := Run(ctx, testConfig(), e.deps(src)); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted Run: %v, want context.Canceled", err)
	}
	if m, ok := e.manifest(t); ok && m.ContiguousUntil >= target {
		t.Fatalf("interrupted run already complete (%d); test proves nothing", m.ContiguousUntil)
	}

	e.runUntil(t, testConfig(), source.NewFake(blocks, n), target, nil)
	got := e.heightRecords(t)
	checkExactly(t, got, target)
	for h, w := range want {
		if !bytes.Equal(got[h], w) {
			t.Fatalf("height %d record differs from uninterrupted run", h)
		}
	}
	gm, _ := e.manifest(t)
	wm, _ := ref.manifest(t)
	if gm.ContiguousUntil != wm.ContiguousUntil || gm.HighestHeight != wm.HighestHeight {
		t.Fatalf("manifest %+v, want %+v", gm, wm)
	}
}

func TestTransportErrorsNeverSkip(t *testing.T) {
	e := newEnv(t)
	src := source.NewFake(mkBlocks(t, 120), 120)
	src.FailNext(77, 3)
	e.runUntil(t, testConfig(), src, 118, nil)
	recs := e.heightRecords(t)
	if _, ok := recs[77]; !ok {
		t.Fatal("height 77 not archived")
	}
	checkExactly(t, recs, 118)
	if got := testutil.ToFloat64(e.m.SourceErrors); got != 3 {
		t.Fatalf("cba_source_errors_total %v, want 3", got)
	}
}

func TestDataRootMismatchStops(t *testing.T) {
	e := newEnv(t)
	blocks := mkBlocks(t, 150)
	bad := *blocks[90]
	bad.DataHash = bytes.Clone(bad.DataHash)
	bad.DataHash[0] ^= 0xFF
	blocks[90] = &bad

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := Run(ctx, testConfig(), e.deps(source.NewFake(blocks, 150)))
	if !errors.Is(err, process.ErrDataRootMismatch) || !errors.Is(err, ErrFatalBlock) {
		t.Fatalf("Run: %v, want ErrDataRootMismatch and ErrFatalBlock", err)
	}
	for _, k := range e.indexKeys(t) {
		b, err := e.st.Get(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		first, last, _, err := segment.DecodeRows(b)
		if err != nil {
			t.Fatal(err)
		}
		if first <= 90 && 90 <= last {
			t.Fatalf("segment %s covers height 90", k)
		}
	}
	if got := testutil.ToFloat64(e.m.DataRootMismatch); got != 1 {
		t.Fatalf("cba_data_root_mismatch_total %v, want 1", got)
	}
}

func TestMissingRanges(t *testing.T) {
	cases := []struct {
		name      string
		committed [][2]int64
		upTo      int64
		size      int64
		want      [][2]int64
	}{
		{"empty", nil, 25, 10, [][2]int64{{1, 10}, {11, 20}, {21, 25}}},
		{"nothing to do", nil, 0, 10, nil},
		{"gaps", [][2]int64{{1, 3}, {6, 8}, {12, 30}}, 20, 3, [][2]int64{{4, 5}, {9, 11}}},
		{"committed beyond", [][2]int64{{5, 100}}, 50, 10, [][2]int64{{1, 4}}},
		{"tail gap", [][2]int64{{1, 10}}, 14, 10, [][2]int64{{11, 14}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := missingRanges(c.committed, c.upTo, c.size)
			if len(got) != len(c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("got %v, want %v", got, c.want)
				}
			}
		})
	}
}

// inFlightSource records the maximum number of concurrent Block calls.
type inFlightSource struct {
	source.Source
	mu       sync.Mutex
	cur, max int
}

func (s *inFlightSource) Block(ctx context.Context, h int64) (*source.Block, error) {
	s.mu.Lock()
	s.cur++
	s.max = max(s.max, s.cur)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.cur--
		s.mu.Unlock()
	}()
	time.Sleep(500 * time.Microsecond) // let calls overlap
	return s.Source.Block(ctx, h)
}

func TestConcurrencyGrowsBeyondWorkers(t *testing.T) {
	e := newEnv(t)
	cfg := testConfig()
	cfg.Workers, cfg.MaxWorkers, cfg.RangeSize = 1, 4, 20
	src := &inFlightSource{Source: source.NewFake(mkBlocks(t, 300), 300)}
	e.runUntil(t, cfg, src, 298, nil)
	checkExactly(t, e.heightRecords(t), 298)
	if got := testutil.ToFloat64(e.m.Workers); got <= 1 {
		t.Fatalf("cba_workers %v, want > 1", got)
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.max <= 1 {
		t.Fatalf("max concurrent Block calls %d, want > 1", src.max)
	}
	if src.max > cfg.MaxWorkers {
		t.Fatalf("max concurrent Block calls %d exceeds MaxWorkers %d", src.max, cfg.MaxWorkers)
	}
}

// flakyStore fails the first n PutIfAbsent calls for the first .seg key seen.
type flakyStore struct {
	store.Store
	mu    sync.Mutex
	key   string
	fails int
}

var errFlaky = errors.New("flaky store: 503")

func (s *flakyStore) PutIfAbsent(ctx context.Context, key string, body []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	if strings.HasSuffix(key, ".seg") && s.key == "" {
		s.key = key
	}
	if key == s.key && s.fails > 0 {
		s.fails--
		s.mu.Unlock()
		return errFlaky
	}
	s.mu.Unlock()
	return s.Store.PutIfAbsent(ctx, key, body, opts...)
}

func TestCommitRetriesTransientStoreErrors(t *testing.T) {
	e := newEnv(t)
	inner := e.st
	e.st = &flakyStore{Store: inner, fails: 2}
	e.runUntil(t, testConfig(), source.NewFake(mkBlocks(t, 120), 120), 118, nil)
	e.st = inner
	checkExactly(t, e.heightRecords(t), 118)
	if got := testutil.ToFloat64(e.m.CommitRetries); got != 2 {
		t.Fatalf("cba_commit_retries_total %v, want 2", got)
	}
}

func TestProcessErrorIsFatalBlock(t *testing.T) {
	e := newEnv(t)
	errBad := errors.New("cannot build square")
	deps := e.deps(source.NewFake(mkBlocks(t, 150), 150))
	deps.Process = func(cur, next *source.Block) (*archivev1.HeightRecord, []*archivev1.BlobRecord, error) {
		if cur.Height == 70 {
			return nil, nil, errBad
		}
		return process.Process(cur, next)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := Run(ctx, testConfig(), deps)
	if !errors.Is(err, ErrFatalBlock) || !errors.Is(err, errBad) {
		t.Fatalf("Run: %v, want ErrFatalBlock wrapping the process error", err)
	}
	if errors.Is(err, process.ErrDataRootMismatch) {
		t.Fatalf("Run: %v must not be a mismatch", err)
	}
	for _, k := range e.indexKeys(t) {
		b, err := e.st.Get(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		first, last, _, err := segment.DecodeRows(b)
		if err != nil {
			t.Fatal(err)
		}
		if first <= 70 && 70 <= last {
			t.Fatalf("segment %s covers height 70", k)
		}
	}
	if got := testutil.ToFloat64(e.m.DataRootMismatch); got != 0 {
		t.Fatalf("cba_data_root_mismatch_total %v, want 0", got)
	}
}

// A commit that does not match the previous header is an inconsistent source
// response, like a header that does not match its block id: Run stops with an
// ordinary (restartable) error, not ErrFatalBlock.
func TestCommitMismatchIsNotFatal(t *testing.T) {
	e := newEnv(t)
	blocks := mkBlocks(t, 150)
	bad := *blocks[71]
	bad.LastCommitProto = blocks[72].LastCommitProto // commits to block 71, not 70
	blocks[71] = &bad

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := Run(ctx, testConfig(), e.deps(source.NewFake(blocks, 150)))
	if !errors.Is(err, process.ErrCommitMismatch) || errors.Is(err, ErrFatalBlock) {
		t.Fatalf("Run: %v, want ErrCommitMismatch without ErrFatalBlock", err)
	}
	for _, k := range e.indexKeys(t) {
		b, err := e.st.Get(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		first, last, _, err := segment.DecodeRows(b)
		if err != nil {
			t.Fatal(err)
		}
		if first <= 70 && 70 <= last {
			t.Fatalf("segment %s covers height 70", k)
		}
	}
}

// blobBlocks returns blocks 1..n with the mocha-1834013 blobs at height at,
// and those blob records.
func blobBlocks(t *testing.T, n, at int64) (map[int64]*source.Block, []*archivev1.BlobRecord) {
	t.Helper()
	blocks := archivetest.Blocks(t, n, map[int64]string{at: "mocha-1834013"})
	_, blobs, err := process.Process(blocks[at], blocks[at+1])
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) < 2 {
		t.Fatalf("fixture has %d blobs, want at least 2", len(blobs))
	}
	return blocks, blobs
}

func withdraw(t *testing.T, st store.Store, b *archivev1.BlobRecord) {
	t.Helper()
	err := tombstone.Add(context.Background(), st, tombstone.Entry{
		Height: int64(b.Height), Namespace: hex.EncodeToString(b.Namespace),
		Commitment: hex.EncodeToString(b.Commitment), Date: "2026-10-26", Reason: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
}

// segmentOf returns the key and bytes of the catalogued segment of height h.
func (e *env) segmentOf(t *testing.T, h int64) (string, []byte) {
	t.Helper()
	ctx := context.Background()
	d, err := archiveread.Locate(ctx, e.st, h)
	if err != nil {
		t.Fatal(err)
	}
	key := archive.SegmentKey(d.SegFirst, d.SegLast)
	b, err := e.st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return key, b
}

// withdrawn reports whether seg holds a tombstone record in place of b; it
// fails if seg has no row for b.
func withdrawn(t *testing.T, seg []byte, b *archivev1.BlobRecord) bool {
	t.Helper()
	rows, err := segment.ScanRows(seg)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Kind == segment.KindBlob && r.Height == b.Height && bytes.Equal(r.Commitment[:], b.Commitment) {
			rec, err := segment.ReadAt(seg, r.Offset, r.Length)
			if err != nil {
				t.Fatal(err)
			}
			return rec.GetTombstone() != nil && !bytes.Contains(seg, b.Data)
		}
	}
	t.Fatalf("no row for blob at height %d", b.Height)
	return false
}

func TestRunAppliesTombstoneToFutureHeight(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 120, 90)
	withdraw(t, e.st, blobs[0])
	e.runUntil(t, testConfig(), source.NewFake(blocks, 120), 118, nil)
	_, seg := e.segmentOf(t, 90)
	if !withdrawn(t, seg, blobs[0]) {
		t.Fatal("height 90 was archived with the withdrawn blob")
	}
	for _, o := range blobs[1:] {
		if withdrawn(t, seg, o) {
			t.Fatal("a blob that is not tombstoned was scrubbed")
		}
	}
	if got := testutil.ToFloat64(e.m.Tombstones); got != 1 {
		t.Fatalf("cba_tombstones %v, want 1", got)
	}
}

func TestRunPollsTombstones(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 120, 40)
	cfg := testConfig()
	cfg.TombstonePoll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, e.deps(source.NewFake(blocks, 120))) }()

	// wait polls cond, advancing the clock so open segments close.
	wait := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.After(20 * time.Second)
		for !cond() {
			select {
			case err := <-done:
				t.Fatalf("Run returned early: %v", err)
			case <-deadline:
				t.Fatalf("timeout waiting for %s", what)
			case <-time.After(5 * time.Millisecond):
			}
			e.clock.Advance(2 * time.Hour)
		}
	}
	wait("height 40 archived", func() bool { m, ok := e.manifest(t); return ok && m.ContiguousUntil >= 40 })
	withdraw(t, e.st, blobs[0])
	wait("the poll to scrub height 40", func() bool { _, seg := e.segmentOf(t, 40); return withdrawn(t, seg, blobs[0]) })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after cancel: %v", err)
	}
	if got := testutil.ToFloat64(e.m.TombstoneScrubs); got != 1 {
		t.Fatalf("cba_tombstone_scrubs_total %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.m.Tombstones); got != 1 {
		t.Fatalf("cba_tombstones %v, want 1", got)
	}
}

// tombLoadStore fails the first fails Gets of tombstones.json and records
// whether a .seg was written before a Get of it succeeded.
type tombLoadStore struct {
	store.Store
	mu          sync.Mutex
	fails       int
	loaded      bool
	earlyWrites int
}

func (s *tombLoadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key != tombstone.Key {
		return s.Store.Get(ctx, key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fails > 0 {
		s.fails--
		return nil, errFlaky
	}
	b, err := s.Store.Get(ctx, key)
	s.loaded = true
	return b, err
}

func (s *tombLoadStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	if strings.HasSuffix(key, ".seg") && !s.loaded {
		s.earlyWrites++
	}
	s.mu.Unlock()
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

// A failed tombstone load at start is retried before any segment is written,
// so the takedown applies to the first write.
func TestStartupTombstoneLoadRetried(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 60, 30)
	withdraw(t, e.st, blobs[0])
	st := &tombLoadStore{Store: e.st, fails: 2}
	e.st = st
	e.runUntil(t, testConfig(), source.NewFake(blocks, 60), 58, nil)
	st.mu.Lock()
	early := st.earlyWrites
	st.mu.Unlock()
	if early != 0 {
		t.Fatalf("%d segments written before the tombstone set was loaded", early)
	}
	if _, seg := e.segmentOf(t, 30); !withdrawn(t, seg, blobs[0]) {
		t.Fatal("height 30 was archived with the withdrawn blob")
	}
	if got := testutil.ToFloat64(e.m.TombstoneErrors); got != 2 {
		t.Fatalf("cba_tombstone_errors_total %v, want 2", got)
	}
}

// segGetStore counts Gets of .seg keys and fails the next fails of them.
type segGetStore struct {
	store.Store
	mu          sync.Mutex
	gets, fails int
}

func (s *segGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, ".seg") {
		s.mu.Lock()
		s.gets++
		fail := s.fails > 0
		if fail {
			s.fails--
		}
		s.mu.Unlock()
		if fail {
			return nil, errFlaky
		}
	}
	return s.Store.Get(ctx, key)
}

func (s *segGetStore) segGets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// takedownRunner archives heights 1..5 (blobs at 3) into st, withdraws the
// first blob of height 3 and returns a runner, whose applyTombstones is one
// poll, and the blobs of height 3.
func takedownRunner(t *testing.T, e *env, st *segGetStore) (*runner, []*archivev1.BlobRecord) {
	t.Helper()
	blocks, blobs := blobBlocks(t, 6, 3)
	archivetest.Build(t, st, blocks, 1, 5, 2, nil)
	withdraw(t, st, blobs[0])
	com, err := archive.NewCommitter(context.Background(), st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	r := &runner{m: e.m, com: com, st: st, log: slog.Default()}
	com.SetTombstoneHook(r.onScrub)
	return r, blobs
}

// A takedown cut short by a store error is not marked applied: the next poll
// with the same tombstones.json runs it again.
func TestTakedownRetriedAfterStoreError(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	st := &segGetStore{Store: e.st}
	r, blobs := takedownRunner(t, e, st)

	st.mu.Lock()
	st.fails = 1
	st.mu.Unlock()
	r.applyTombstones(ctx)
	if _, seg := e.segmentOf(t, 3); withdrawn(t, seg, blobs[0]) {
		t.Fatal("the failed Get did not stop the scrub; test proves nothing")
	}
	if got := testutil.ToFloat64(e.m.TombstoneErrors); got != 1 {
		t.Fatalf("cba_tombstone_errors_total %v, want 1", got)
	}
	r.applyTombstones(ctx)
	if _, seg := e.segmentOf(t, 3); !withdrawn(t, seg, blobs[0]) {
		t.Fatal("the next poll did not retry the takedown")
	}
	if got := testutil.ToFloat64(e.m.TombstoneScrubs); got != 1 {
		t.Fatalf("cba_tombstone_scrubs_total %v, want 1", got)
	}
}

// A segment that cannot be scrubbed is counted once; the takedown counts as
// applied, so the next poll with the same version downloads nothing.
func TestScrubErrorNotRetried(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	st := &segGetStore{Store: e.st}
	r, blobs := takedownRunner(t, e, st)
	// An orphan covering height 3 that does not parse: Scrub fails on it the
	// same way on every attempt.
	if err := st.Put(ctx, archive.SegmentKey(3, 3), []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}

	r.applyTombstones(ctx)
	if _, seg := e.segmentOf(t, 3); !withdrawn(t, seg, blobs[0]) {
		t.Fatal("the catalogued segment was not scrubbed")
	}
	if got := testutil.ToFloat64(e.m.TombstoneErrors); got != 1 {
		t.Fatalf("cba_tombstone_errors_total %v, want 1", got)
	}
	gets := st.segGets()
	r.applyTombstones(ctx)
	if got := st.segGets(); got != gets {
		t.Fatalf("the next poll read %d segments, want 0", got-gets)
	}
	if got := testutil.ToFloat64(e.m.TombstoneErrors); got != 1 {
		t.Fatalf("cba_tombstone_errors_total %v after the next poll, want 1", got)
	}
}

// failCatalogStore fails every catalog write.
type failCatalogStore struct{ store.Store }

func (s failCatalogStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if strings.HasPrefix(key, "catalog/") {
		return errFlaky
	}
	return s.Store.Put(ctx, key, b, opts...)
}

func TestReingestOfScrubbedRangeIsIdempotent(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 60, 30)
	cfg := testConfig()
	cfg.RangeSize = 50

	// Commit 1..50 with the catalog write failing: an orphan .seg.
	c, err := archive.NewCommitter(ctx, failCatalogStore{e.st}, cfg.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	b := archive.NewBuilder(nil)
	for h := int64(1); h <= 50; h++ {
		hr, bl, err := process.Process(blocks[h], blocks[h+1])
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Add(hr, bl); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := b.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(ctx, *ref); !errors.Is(err, errFlaky) {
		t.Fatalf("Commit = %v, want the catalog error", err)
	}
	orig, err := e.st.Get(ctx, ref.Key)
	if err != nil {
		t.Fatal(err)
	}

	withdraw(t, e.st, blobs[0])
	set, err := tombstone.Load(ctx, e.st)
	if err != nil {
		t.Fatal(err)
	}
	want, n, err := tombstone.Scrub(orig, set)
	if err != nil || n != 1 {
		t.Fatalf("Scrub: %d, %v", n, err)
	}
	// Head 53: backfill rebuilds exactly [1,50]; the tail starts at 51. The
	// clock stands still so the backfill segment does not split by age.
	e.runUntilStep(t, cfg, source.NewFake(blocks, 53), 50, nil, 0)
	key, got := e.segmentOf(t, 30)
	if key != ref.Key {
		t.Fatalf("height 30 is in %s, want the rebuilt %s", key, ref.Key)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("final segment differs from Scrub(original)")
	}
}

// failingStore fails every write once fail is set and counts the failures.
type failingStore struct {
	store.Store
	fail   atomic.Bool
	failed atomic.Int64
}

var errStoreDown = errors.New("store down")

func (s *failingStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if s.fail.Load() {
		s.failed.Add(1)
		return errStoreDown
	}
	return s.Store.Put(ctx, key, b, opts...)
}

func (s *failingStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if s.fail.Load() {
		s.failed.Add(1)
		return errStoreDown
	}
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

// runWithBufferedTail starts Run with head 60 (backfill [1,57], tail from 58),
// waits for the backfill to commit, moves head to 96 so the tail buffers the
// 37 heights 58..94 without closing the segment (the clock stands still),
// calls beforeCancel, cancels Run and returns its error.
func runWithBufferedTail(t *testing.T, e *env, beforeCancel func()) error {
	t.Helper()
	const backfillLast, tailLast = 57, 94
	src := source.NewFake(mkBlocks(t, 100), 60)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testConfig(), e.deps(src)) }()
	deadline := time.After(20 * time.Second)
	for moved := false; ; {
		select {
		case err := <-done:
			t.Fatalf("Run returned early: %v", err)
		case <-deadline:
			t.Fatalf("timeout: %v blocks processed", testutil.ToFloat64(e.m.BlocksProcessed))
		case <-time.After(5 * time.Millisecond):
		}
		m, ok := e.manifest(t)
		if !moved && ok && m.ContiguousUntil == backfillLast {
			src.SetHead(tailLast + 2)
			moved = true
		}
		if moved && testutil.ToFloat64(e.m.BlocksProcessed) == tailLast {
			break
		}
	}
	if m, _ := e.manifest(t); m.HighestHeight != backfillLast {
		t.Fatalf("tail committed before shutdown (highest %d); test proves nothing", m.HighestHeight)
	}
	if beforeCancel != nil {
		beforeCancel()
	}
	cancel()
	return <-done
}

func TestShutdownFlushesTail(t *testing.T) {
	e := newEnv(t)
	if err := runWithBufferedTail(t, e, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after cancel: %v, want context.Canceled", err)
	}
	m, _ := e.manifest(t)
	if m.ContiguousUntil != 94 || m.HighestHeight != 94 {
		t.Fatalf("manifest contiguous %d highest %d, want 94/94", m.ContiguousUntil, m.HighestHeight)
	}
	checkExactly(t, e.heightRecords(t), 94)
	if key, _ := e.segmentOf(t, 94); key != archive.SegmentKey(58, 94) {
		t.Fatalf("height 94 is in %s, want %s", key, archive.SegmentKey(58, 94))
	}
}

func TestShutdownFlushFailureIsNotFatal(t *testing.T) {
	e := newEnv(t)
	fstore := &failingStore{Store: e.st}
	e.st = fstore
	err := runWithBufferedTail(t, e, func() { fstore.fail.Store(true) })
	if !errors.Is(err, context.Canceled) || errors.Is(err, errStoreDown) {
		t.Fatalf("Run after cancel: %v, want context.Canceled only", err)
	}
	if fstore.failed.Load() == 0 {
		t.Fatal("no final commit was attempted")
	}
	if m, _ := e.manifest(t); m.HighestHeight != 57 {
		t.Fatalf("manifest highest %d, want 57 (final commit failed)", m.HighestHeight)
	}
}

// opNS is the namespace of the one share-version-0 blob in mocha-1834013.
const opNS = "00000000000000000000000000000000000000893f79d6e2c81a4d08c4"

func opNamespaces(t *testing.T, csv string) [][29]byte {
	t.Helper()
	nss, err := oplayer.ParseNamespaces(csv)
	if err != nil {
		t.Fatal(err)
	}
	return nss
}

func opConfig(t *testing.T) Config {
	cfg := testConfig()
	cfg.OPNamespaces = opNamespaces(t, opNS)
	return cfg
}

// opKeyOf returns the compact OP key of the opNS blob among blobs.
func opKeyOf(t *testing.T, blobs []*archivev1.BlobRecord) string {
	t.Helper()
	for _, b := range blobs {
		if hex.EncodeToString(b.Namespace) != opNS {
			continue
		}
		ob, err := oplayer.FromRecord(b)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := ob.Keys()
		if err != nil {
			t.Fatal(err)
		}
		return keys[0]
	}
	t.Fatalf("no blob of %s", opNS)
	return ""
}

// opOrderStore checks, whenever a segment covering height is written, that
// opKey already exists.
type opOrderStore struct {
	store.Store
	height int64
	opKey  string
	checks atomic.Int64
	early  atomic.Int64
}

func (s *opOrderStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	first, last, ok := archive.ParseSegmentKey(key)
	if ok && strings.HasSuffix(key, ".seg") && first <= s.height && s.height <= last {
		s.checks.Add(1)
		if ok, err := s.Store.Exists(ctx, s.opKey); err != nil || !ok {
			s.early.Add(1)
		}
	}
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

func TestRunWritesOPBeforeCommit(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 60, 30)
	st := &opOrderStore{Store: e.st, height: 30, opKey: opKeyOf(t, blobs)}
	e.st = st
	e.runUntil(t, opConfig(t), source.NewFake(blocks, 60), 58, nil)
	if st.checks.Load() == 0 {
		t.Fatal("no segment covering height 30 was written")
	}
	if n := st.early.Load(); n != 0 {
		t.Fatalf("%d segment writes for height 30 before its OP object", n)
	}
	if got := testutil.ToFloat64(e.m.OPObjectsWritten); got != 1 {
		t.Fatalf("cba_op_objects_written_total %v, want 1", got)
	}
}

func TestRunCountsOPSkips(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 60, 30)
	withdraw(t, e.st, blobs[1]) // the opNS blob
	if hex.EncodeToString(blobs[1].Namespace) != opNS {
		t.Fatalf("blob 1 is in %x, want %s", blobs[1].Namespace, opNS)
	}
	cfg := testConfig()
	cfg.OPNamespaces = opNamespaces(t, opNS+","+hex.EncodeToString(blobs[0].Namespace))
	e.runUntil(t, cfg, source.NewFake(blocks, 60), 58, nil)
	for reason, want := range map[string]float64{"share_version": 2, "withdrawn": 1} {
		if got := testutil.ToFloat64(e.m.OPBlobsSkipped.WithLabelValues(reason)); got != want {
			t.Errorf("cba_op_blobs_skipped_total{reason=%q} %v, want %v", reason, got, want)
		}
	}
	if got := testutil.ToFloat64(e.m.OPObjectsWritten); got != 0 {
		t.Errorf("cba_op_objects_written_total %v, want 0", got)
	}
}

// opFailStore fails the first fails PutIfAbsent calls of op/ keys.
type opFailStore struct {
	store.Store
	fails atomic.Int64
}

func (s *opFailStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if strings.HasPrefix(key, "op/") && s.fails.Add(-1) >= 0 {
		return errFlaky
	}
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

func TestRunRetriesOPWriteErrors(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 60, 30)
	inner := e.st
	st := &opFailStore{Store: inner}
	st.fails.Store(3)
	e.st = st
	e.runUntil(t, opConfig(t), source.NewFake(blocks, 60), 58, nil)
	e.st = inner
	checkExactly(t, e.heightRecords(t), 58)
	if ok, err := inner.Exists(context.Background(), opKeyOf(t, blobs)); err != nil || !ok {
		t.Fatalf("OP object missing: %v, %v", ok, err)
	}
	if got := testutil.ToFloat64(e.m.OPErrors); got != 3 {
		t.Fatalf("cba_op_errors_total %v, want 3", got)
	}
}

// plantOP writes b's data under both its OP keys (the writer writes only the
// compact one; a takedown must delete both) and returns the keys.
func plantOP(t *testing.T, st store.Store, b *archivev1.BlobRecord) [2]string {
	t.Helper()
	ob, err := oplayer.FromRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := ob.Keys()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if err := st.Put(context.Background(), k, b.Data); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

// opLeft returns the keys of keys that exist in st.
func opLeft(t *testing.T, st store.Store, keys [2]string) []string {
	t.Helper()
	var left []string
	for _, k := range keys {
		ok, err := st.Exists(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			left = append(left, k)
		}
	}
	return left
}

// segFailStore fails every PutIfAbsent of a .seg covering height.
type segFailStore struct {
	store.Store
	height int64
}

func (s *segFailStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if first, last, ok := archive.ParseSegmentKey(key); ok && strings.HasSuffix(key, ".seg") && first <= s.height && s.height <= last {
		return errFlaky
	}
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

// Review Focus 1, crash case: the OP object of a height was written, the
// ingester died before its .seg was stored, and the blob was withdrawn before
// the restart. No stored segment holds the blob, so only the restarted
// commit of that height can find it.
func TestRunOPTakedownAfterCrashBeforeSegment(t *testing.T) {
	e := newEnv(t)
	blocks, blobs := blobBlocks(t, 60, 30)
	src := source.NewFake(blocks, 60)
	opKey := opKeyOf(t, blobs)

	inner := e.st
	e.st = &segFailStore{Store: inner, height: 30}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := Run(ctx, opConfig(t), e.deps(src)); err == nil || ctx.Err() != nil {
		t.Fatalf("Run = %v, want the segment write error", err)
	}
	e.st = inner
	if ok, err := inner.Exists(context.Background(), opKey); err != nil || !ok {
		t.Fatalf("OP object not written before the crash: %v, %v", ok, err)
	}
	segs, err := inner.List(context.Background(), "segments/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range segs {
		if first, last, ok := archive.ParseSegmentKey(k); ok && first <= 30 && 30 <= last {
			t.Fatalf("%s covers height 30; the crash must leave none", k)
		}
	}
	keys := plantOP(t, inner, blobs[1])
	if keys[0] != opKey {
		t.Fatalf("blob 1 is not the OP blob")
	}
	withdraw(t, inner, blobs[1])

	// The startup takedown deletes the compact key from the set (the first
	// failure); the full key needs the segment, so the commit of height 30
	// deletes both (the second failure, named and retried).
	e.st = &deleteFailStore{Store: inner, fails: 2}
	e.m = NewMetrics(prometheus.NewRegistry())
	e.runUntil(t, opConfig(t), src, 58, nil)
	e.st = inner
	if left := opLeft(t, inner, keys); len(left) != 0 {
		t.Fatalf("OP keys of the withdrawn blob left: %v", left)
	}
	if got := testutil.ToFloat64(e.m.OPErrors); got != 1 {
		t.Fatalf("cba_op_errors_total %v, want 1 (the failed delete in commit)", got)
	}
	if got := testutil.ToFloat64(e.m.OPBlobsSkipped.WithLabelValues("withdrawn")); got != 1 {
		t.Fatalf("cba_op_blobs_skipped_total{reason=withdrawn} %v, want 1", got)
	}
}

// deleteFailStore fails the next fails Deletes.
type deleteFailStore struct {
	store.Store
	mu    sync.Mutex
	fails int
}

func (s *deleteFailStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	fail := s.fails > 0
	if fail {
		s.fails--
	}
	s.mu.Unlock()
	if fail {
		return errFlaky
	}
	return s.Store.Delete(ctx, key)
}

// A takedown whose OP delete failed is not marked applied: the next poll
// with the same tombstones.json deletes.
func TestDeleteErrorRetriedOnNextPoll(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	st := &segGetStore{Store: &deleteFailStore{Store: e.st, fails: 1}}
	r, blobs := takedownRunner(t, e, st)
	keys := plantOP(t, e.st, blobs[0])

	r.applyTombstones(ctx)
	if len(opLeft(t, e.st, keys)) == 0 {
		t.Fatal("the failed delete did not stop the takedown; test proves nothing")
	}
	if got := testutil.ToFloat64(e.m.TombstoneErrors); got != 1 {
		t.Fatalf("cba_tombstone_errors_total %v, want 1", got)
	}
	r.applyTombstones(ctx)
	if left := opLeft(t, e.st, keys); len(left) != 0 {
		t.Fatalf("the next poll left %v", left)
	}
	if got := testutil.ToFloat64(e.m.TombstoneErrors); got != 1 {
		t.Fatalf("cba_tombstone_errors_total %v after the retry, want 1", got)
	}
}

// A withdrawn blob whose only stored segment cannot be parsed still loses its
// compact OP key (derived from the set), and the set counts as applied only
// once the deletes succeeded.
func TestUnreadableSegmentCompactKeyDeleted(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	del := &deleteFailStore{Store: e.st, fails: 1}
	st := &segGetStore{Store: del}
	r, blobs := takedownRunner(t, e, st)
	segKey := archive.SegmentKey(3, 4)
	if err := e.st.Put(ctx, segKey, []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	compact := plantOP(t, e.st, blobs[0])[0]
	exists := func() bool {
		ok, err := e.st.Exists(ctx, compact)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	r.applyTombstones(ctx)
	if !exists() {
		t.Fatal("the failed delete did not stop the takedown; test proves nothing")
	}
	if r.tombApplied {
		t.Fatal("set marked applied although a delete failed")
	}
	r.applyTombstones(ctx)
	if exists() {
		t.Fatal("compact OP key of a blob in an unreadable segment survived the takedown")
	}
	if !r.tombApplied {
		t.Fatal("set not marked applied after the deletes succeeded")
	}
}

// Run writes the OP config at start even when the list is empty, replacing
// a stale one.
func TestRunWritesOPConfig(t *testing.T) {
	ctx := context.Background()
	stale := opNamespaces(t, "00000000000000000000000000000000000000636c6235656637626138")
	for name, cfg := range map[string]Config{"configured": opConfig(t), "empty": testConfig()} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			if err := oplayer.WriteConfig(ctx, e.st, stale, time.Unix(0, 0)); err != nil {
				t.Fatal(err)
			}
			e.runUntil(t, cfg, source.NewFake(mkBlocks(t, 20), 20), 18, nil)
			got, err := oplayer.LoadConfig(ctx, e.st)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(cfg.OPNamespaces) || (len(got) == 1 && got[0] != cfg.OPNamespaces[0]) {
				t.Fatalf("config %x, want %x", got, cfg.OPNamespaces)
			}
			if g := testutil.ToFloat64(e.m.OPNamespaces); g != float64(len(cfg.OPNamespaces)) {
				t.Fatalf("cba_op_namespaces %v, want %d", g, len(cfg.OPNamespaces))
			}
		})
	}
}
