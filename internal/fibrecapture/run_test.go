package fibrecapture

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/fibre"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// threeHeights is the standard scenario: h1 a PFF of data, h2 a timeout of
// the same promise, h3 a malformed PFF.
func threeHeights(t *testing.T, data []byte) (map[int64]*source.Block, [33]byte, uint32) {
	t.Helper()
	tx, id, size := pffFor(t, data)
	return map[int64]*source.Block{
		1: blockAt(1, hStart, tx),
		2: blockAt(2, hStart, timeoutTx(t, id, size)),
		3: blockAt(3, hStart, malformedPFF(t)),
	}, id, size
}

func addFibreTombstone(t *testing.T, st store.Store, id [33]byte) {
	t.Helper()
	err := tombstone.AddFibre(context.Background(), st, tombstone.FibreEntry{
		BlobID: hex.EncodeToString(id[:]), Date: "2026-10-27", Reason: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCaptureHappyPath(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	blocks, id, _ := threeHeights(t, data)
	h := newHarness(t, blocks, 3)
	h.dl.set(id, data)
	if err := h.runUntil(h.drained(3), nil); err != nil {
		t.Fatal(err)
	}

	got, err := h.st.Store.Get(context.Background(), fibrearchive.DataKey(id))
	if err != nil || string(got) != string(data) {
		t.Fatalf("fibre/data: %v", err)
	}
	want := map[string]fibrearchive.Status{
		fibrearchive.RecordKey(1, 0, 0): fibrearchive.StatusArchived,
		fibrearchive.RecordKey(2, 0, 0): fibrearchive.StatusArchived,
		fibrearchive.RecordKey(3, 0, 0): fibrearchive.StatusUnrecoverable,
	}
	st := h.statuses()
	if len(st) != len(want) {
		t.Fatalf("records %v", st)
	}
	for k, s := range want {
		if st[k] != s {
			t.Errorf("%s: %v, want %v", k, st[k], s)
		}
	}
	if r := h.records()[fibrearchive.RecordKey(3, 0, 0)]; r.Error == "" {
		t.Error("unrecoverable record without error")
	}
	if n := len(h.dl.callLog()); n != 1 {
		t.Errorf("downloader called %d times, want 1", n)
	}
	ents := h.indexEntries()
	if len(ents) != 3 {
		t.Fatalf("index %+v", ents)
	}
	for i, e := range ents {
		if e.Height != int64(i+1) {
			t.Errorf("index entry %d at height %d", i, e.Height)
		}
	}
	m, err := fibrearchive.LoadManifest(context.Background(), h.st.Store)
	if err != nil {
		t.Fatal(err)
	}
	if m.Counts["archived"] != 2 || m.Counts["unrecoverable"] != 1 || len(m.Counts) != 2 {
		t.Errorf("manifest counts %v", m.Counts)
	}
	if m.BytesArchived != uint64(len(data)) {
		t.Errorf("bytes archived %d, want %d", m.BytesArchived, len(data))
	}
	if v := testutil.ToFloat64(h.m.Pending); v != 0 {
		t.Errorf("pending gauge %v", v)
	}
	if v := testutil.ToFloat64(h.m.Records.WithLabelValues("timeout", "archived")); v != 1 {
		t.Errorf("records{timeout,archived} = %v", v)
	}
	if files := h.spoolFiles(); len(files) != 0 {
		t.Errorf("spool left %v", files)
	}
}

func TestRetryThenExpire(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	tx, id, _ := pffFor(t, data)
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, tx)}, 1)
	h.params.set(time.Hour) // deadline = block time + 50m
	h.dl.err = fibre.ErrNotFound
	key := fibrearchive.RecordKey(1, 0, 0)
	const step = 15 * time.Second
	offsets := []time.Duration{0, time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute}
	// The clock moves one step at a time; once it reaches the next expected
	// attempt it waits (up to 2s of real time) for that attempt, so a slow
	// loop cannot be overtaken by the clock.
	var waiting time.Time
	err := h.runUntil(func() bool { return h.statuses()[key] != 0 && h.drained(1)() }, func() {
		now := h.clock.now()
		if !now.Before(hStart.Add(55 * time.Minute)) {
			return
		}
		if n := len(h.dl.callLog()); n < len(offsets) && !now.Before(hStart.Add(offsets[n])) {
			if waiting.IsZero() {
				waiting = time.Now()
			}
			if time.Since(waiting) < 2*time.Second {
				return
			}
		}
		waiting = time.Time{}
		time.Sleep(2 * time.Millisecond)
		h.clock.Advance(step)
	})
	if err != nil {
		t.Fatal(err)
	}
	if s := h.statuses()[key]; s != fibrearchive.StatusExpired {
		t.Fatalf("status %v, want expired", s)
	}
	calls := h.dl.callLog()
	if len(calls) != len(offsets) {
		t.Fatalf("%d attempts, want %d: %v", len(calls), len(offsets), calls)
	}
	for i, c := range calls {
		lo := hStart.Add(offsets[i])
		if c.id != id || c.at.Before(lo) || !c.at.Before(lo.Add(step)) {
			t.Errorf("attempt %d at +%v, want +%v", i, c.at.Sub(hStart), offsets[i])
		}
	}
	if v := testutil.ToFloat64(h.m.Records.WithLabelValues("pff", "expired")); v != 1 {
		t.Errorf("records{pff,expired} = %v", v)
	}
	if v := testutil.ToFloat64(h.m.DownloadAttempts.WithLabelValues("not_found")); v != float64(len(offsets)) {
		t.Errorf("attempts{not_found} = %v", v)
	}
}

// blobOfSize returns random data whose upload size is exactly size.
func blobOfSize(t *testing.T, size uint32) []byte {
	t.Helper()
	cfg, err := fibre.BlobConfigForVersion(0)
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := 1, int(size)
	for lo < hi { // largest n with UploadSize(n) <= size
		mid := (lo + hi + 1) / 2
		if cfg.UploadSize(mid) <= int(size) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if cfg.UploadSize(lo) != int(size) {
		t.Fatalf("no payload length has upload size %d", size)
	}
	data, _, got := testBlob(t, lo)
	if got != size {
		t.Fatalf("upload size %d", got)
	}
	return data
}

func TestCapSkip(t *testing.T) {
	d1, d2 := blobOfSize(t, 262144), blobOfSize(t, 262144)
	tx1, id1, _ := pffFor(t, d1)
	tx2, id2, _ := pffFor(t, d2)
	h := newHarness(t, map[int64]*source.Block{
		1: blockAt(1, hStart, tx1),
		2: blockAt(2, hStart.Add(6*time.Second), tx2),
	}, 2)
	h.cfg.DailyCapBytes = 300 << 10
	h.cfg.DownloadWorkers = 1
	h.dl.set(id1, d1)
	h.dl.set(id2, d2)
	if err := h.runUntil(h.drained(2), nil); err != nil {
		t.Fatal(err)
	}
	st := h.statuses()
	if st[fibrearchive.RecordKey(1, 0, 0)] != fibrearchive.StatusArchived || st[fibrearchive.RecordKey(2, 0, 0)] != fibrearchive.StatusSkippedCap {
		t.Fatalf("statuses %v", st)
	}
	for _, c := range h.dl.callLog() {
		if c.id == id2 {
			t.Error("skipped blob was downloaded")
		}
	}
	if h.hasData(id2) {
		t.Error("skipped blob has data")
	}
	// The cap reserves blob_size but charges the bytes written.
	if s := h.state(); s.DayBytes != uint64(len(d1)) || s.Day != "2026-10-27" {
		t.Errorf("day %s bytes %d, want %d", s.Day, s.DayBytes, len(d1))
	}
}

func TestWithdrawnBeforeCapture(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	blocks, id, _ := threeHeights(t, data)
	h := newHarness(t, blocks, 3)
	h.dl.set(id, data)
	addFibreTombstone(t, h.st.Store, id)
	if err := h.runUntil(h.drained(3), nil); err != nil {
		t.Fatal(err)
	}
	st := h.statuses()
	if st[fibrearchive.RecordKey(1, 0, 0)] != fibrearchive.StatusWithdrawn || st[fibrearchive.RecordKey(2, 0, 0)] != fibrearchive.StatusWithdrawn {
		t.Fatalf("statuses %v", st)
	}
	if n := len(h.dl.callLog()); n != 0 {
		t.Errorf("downloader called %d times", n)
	}
	if h.hasData(id) {
		t.Error("withdrawn blob has data")
	}
}

func TestMismatchedDownloadNeverWritten(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	id, size := blobIDOf(t, data)
	other, _, _ := testBlob(t, 1000)
	cases := map[string]struct {
		blobSize uint32
		payload  []byte
	}{
		"(a) bytes of another blob": {size, other},
		"(b) blob_size off by 4096": {size + 4096, data},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, pffTx(t, id, c.blobSize))}, 1)
			h.dl.set(id, c.payload)
			if err := h.runUntil(h.drained(1), nil); err != nil {
				t.Fatal(err)
			}
			if h.hasData(id) {
				t.Error("mismatched bytes written to fibre/data")
			}
			if files := h.spoolFiles(); len(files) != 0 {
				t.Errorf("spool left %v", files)
			}
			r := h.records()[fibrearchive.RecordKey(1, 0, 0)]
			if r == nil || fibrearchive.Status(r.DataStatus) != fibrearchive.StatusUnrecoverable || r.Error == "" {
				t.Fatalf("record %+v", r)
			}
			if n := len(h.dl.callLog()); n != 1 {
				t.Errorf("downloader called %d times, want 1 (no retry)", n)
			}
		})
	}

	t.Run("(c) stored object holds other bytes", func(t *testing.T) {
		h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, pffTx(t, id, size))}, 1)
		h.dl.set(id, data)
		if err := h.st.Store.Put(context.Background(), fibrearchive.DataKey(id), other); err != nil {
			t.Fatal(err)
		}
		err := h.runUntil(func() bool { return false }, nil)
		if !errors.Is(err, store.ErrExists) {
			t.Fatalf("Run = %v, want an error wrapping store.ErrExists", err)
		}
		if got, _ := h.st.Store.Get(context.Background(), fibrearchive.DataKey(id)); string(got) != string(other) {
			t.Error("stored object was replaced")
		}
	})
}

// killPoint arms h so that the run dies right after the step it names: the
// hook marks store and dir dead (all later writes fail) and cancels Run.
type killPoint struct {
	name string
	arm  func(h *harness, id [33]byte, kill func())
}

// deadFS makes every write of dir fail once dead is set, and calls
// afterRename after each successful rename.
func deadFS(h *harness, dead *atomic.Bool, afterRename func(dst string)) {
	h.dir.fs = fsys{
		writeFile: func(name string, b []byte, perm os.FileMode) error {
			if dead.Load() {
				return errDead
			}
			return writeFileSync(name, b, perm)
		},
		rename: func(oldpath, newpath string) error {
			if dead.Load() {
				return errDead
			}
			if err := os.Rename(oldpath, newpath); err != nil {
				return err
			}
			if afterRename != nil {
				afterRename(newpath)
			}
			return nil
		},
	}
}

func stateHook(h *harness, cond func(*State) bool, kill func()) func(string) {
	return func(dst string) {
		if filepath.Base(dst) != stateName {
			return
		}
		s, err := h.dir.Load()
		if err == nil && s != nil && cond(s) {
			kill()
		}
	}
}

func storeHook(h *harness, match func(op, key string) bool, kill func()) {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	h.st.after = func(op, key string, _ store.PutOptions) {
		if match(op, key) {
			kill()
		}
	}
}

var pffKey = fibrearchive.RecordKey(1, 0, 0)

// pffIndexed reports whether the index holds the PFF at height 1.
func (h *harness) pffIndexed() bool {
	for _, e := range h.indexEntries() {
		if e.Height == 1 {
			return true
		}
	}
	return false
}

var killPoints = []killPoint{
	{"after scan save", func(h *harness, _ [33]byte, kill func()) {
		h.dirHook = stateHook(h, func(s *State) bool { return s.Entries[pffKey] != nil }, kill)
	}},
	{"after spool write", func(h *harness, id [33]byte, kill func()) {
		want := hex.EncodeToString(id[:])
		h.dirHook = func(dst string) {
			if filepath.Base(dst) == want {
				kill()
			}
		}
	}},
	{"after the data PUT", func(h *harness, _ [33]byte, kill func()) {
		storeHook(h, func(op, key string) bool { return strings.HasPrefix(key, fibrearchive.DataPrefix) }, kill)
	}},
	{"after the record PUT", func(h *harness, _ [33]byte, kill func()) {
		storeHook(h, func(op, key string) bool { return key == pffKey }, kill)
	}},
	{"after the final save", func(h *harness, _ [33]byte, kill func()) {
		h.dirHook = stateHook(h, func(s *State) bool { e := s.Entries[pffKey]; return e != nil && e.Phase == PhaseFinal }, kill)
	}},
	// The index and manifest kill points are the writes of the flush that
	// indexes the PFF, not an earlier flush racing its download.
	{"after the index write", func(h *harness, _ [33]byte, kill func()) {
		storeHook(h, func(op, key string) bool { return strings.HasPrefix(key, fibrearchive.IndexPrefix) && h.pffIndexed() }, kill)
	}},
	{"after the manifest write", func(h *harness, _ [33]byte, kill func()) {
		storeHook(h, func(op, key string) bool { return key == fibrearchive.ManifestKey && h.pffIndexed() }, kill)
	}},
}

// crashThenRestart runs once until the kill point fires, then restarts on the
// same dir, store and source with healthy hooks. before runs between the
// two runs.
func crashThenRestart(t *testing.T, h *harness, kp killPoint, id [33]byte, before func()) {
	t.Helper()
	var dead atomic.Bool
	killed := make(chan struct{})
	var once sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	kill := func() {
		once.Do(func() {
			dead.Store(true)
			h.st.setDead(true)
			cancel()
			close(killed)
		})
	}
	kp.arm(h, id, kill)
	deadFS(h, &dead, func(dst string) {
		if h.dirHook != nil {
			h.dirHook(dst)
		}
	})
	done := make(chan error, 1)
	d := h.deps()
	go func() { done <- Run(ctx, h.cfg, d) }()
	select {
	case <-killed:
	case err := <-done:
		cancel()
		t.Fatalf("first run returned %v before the kill point", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("kill point never reached")
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, errDead) {
			t.Logf("first run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("killed run did not return")
	}

	// Restart healthy.
	h.st.setDead(false)
	h.st.mu.Lock()
	h.st.after = nil
	h.st.mu.Unlock()
	h.dirHook = nil
	h.dir.fs = osFS
	if before != nil {
		before()
	}
	if err := h.runUntil(h.drained(3), nil); err != nil {
		t.Fatal(err)
	}
}

func TestCrashAtEveryStepLosesNothing(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	for _, kp := range killPoints {
		t.Run(kp.name, func(t *testing.T) {
			blocks, id, _ := threeHeights(t, data)
			h := newHarness(t, blocks, 3)
			h.dl.set(id, data)
			crashThenRestart(t, h, kp, id, nil)

			// (a) exactly one record and one index entry per item.
			want := map[string]fibrearchive.Status{
				fibrearchive.RecordKey(1, 0, 0): fibrearchive.StatusArchived,
				fibrearchive.RecordKey(2, 0, 0): fibrearchive.StatusArchived,
				fibrearchive.RecordKey(3, 0, 0): fibrearchive.StatusUnrecoverable,
			}
			st := h.statuses()
			if len(st) != 3 {
				t.Fatalf("records %v", st)
			}
			for k, s := range want {
				if st[k] != s {
					t.Errorf("%s: %v, want %v", k, st[k], s)
				}
			}
			seen := map[int64]int{}
			for _, e := range h.indexEntries() {
				seen[e.Height]++
			}
			if len(seen) != 3 || seen[1] != 1 || seen[2] != 1 || seen[3] != 1 {
				t.Errorf("index entries by height %v", seen)
			}
			// (b) data for every archived item.
			if !h.hasData(id) {
				t.Error("archived blob has no data object")
			}
			// (c) no extra download: the crash comes before the download or
			// after the spool write.
			if n := len(h.dl.callLog()); n != 1 {
				t.Errorf("downloader called %d times, want 1", n)
			}
			// (d) the day's bytes are counted once.
			if s := h.state(); s.DayBytes != uint64(len(data)) {
				t.Errorf("day bytes %d, want %d", s.DayBytes, len(data))
			}
			if files := h.spoolFiles(); len(files) != 0 {
				t.Errorf("spool left %v", files)
			}
			m, err := fibrearchive.LoadManifest(context.Background(), h.st.Store)
			if err != nil {
				t.Fatal(err)
			}
			if m.Counts["archived"] != 2 || m.Counts["unrecoverable"] != 1 || m.BytesArchived != uint64(len(data)) {
				t.Errorf("manifest counts %v bytes %d", m.Counts, m.BytesArchived)
			}
		})
	}
}

func TestRestartDoesNotReplayWithdrawnSpool(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	blocks, id, _ := threeHeights(t, data)
	h := newHarness(t, blocks, 3)
	h.dl.set(id, data)
	var dataPut atomic.Bool
	var spoolAtRecord atomic.Int64
	spoolAtRecord.Store(-1)
	crashThenRestart(t, h, killPoints[1], id, func() {
		if files := h.spoolFiles(); len(files) != 1 {
			t.Fatalf("spool after the crash %v, want the payload", files)
		}
		addFibreTombstone(t, h.st.Store, id)
		h.st.mu.Lock()
		defer h.st.mu.Unlock()
		h.st.before = func(op, key string) {
			if op != "delete" && key == fibrearchive.DataKey(id) { // the takedown's delete is expected
				dataPut.Store(true)
			}
		}
		// The withdrawn payload must leave the disk at start, before its
		// record is written, not as the usual cleanup after the final save.
		h.st.after = func(_, key string, _ store.PutOptions) {
			if key == pffKey {
				ents, _ := os.ReadDir(filepath.Join(h.dir.path, spoolDirName))
				spoolAtRecord.Store(int64(len(ents)))
			}
		}
	})
	if dataPut.Load() {
		t.Error("restart attempted a PUT of the withdrawn fibre/data object")
	}
	if n := spoolAtRecord.Load(); n != 0 {
		t.Errorf("%d spool files when the withdrawn record was written, want 0", n)
	}
	if h.hasData(id) {
		t.Fatal("withdrawn spool replayed into fibre/data")
	}
	if files := h.spoolFiles(); len(files) != 0 {
		t.Errorf("spool left %v", files)
	}
	if s := h.statuses()[pffKey]; s != fibrearchive.StatusWithdrawn {
		t.Errorf("record %v, want withdrawn", s)
	}
}

// countingSource counts Block calls.
type countingSource struct {
	source.Source
	n atomic.Int64
}

func (c *countingSource) Block(ctx context.Context, h int64) (*source.Block, error) {
	c.n.Add(1)
	return c.Source.Block(ctx, h)
}

func TestFirstStartBeginsAtRetentionWindow(t *testing.T) {
	const head = 10_000
	blocks := make(map[int64]*source.Block, head)
	for i := int64(1); i <= head; i++ {
		blocks[i] = blockAt(i, hStart.Add(time.Duration(i-head)*6*time.Second))
	}
	src := source.NewFake(blocks, head)
	cs := &countingSource{Source: src}
	first, err := FirstHeightAfter(context.Background(), cs, hStart.Add(-4*time.Hour), head)
	if err != nil {
		t.Fatal(err)
	}
	if first < head-2400-1 || first > head-2400+1 {
		t.Errorf("first height %d, want %d±1", first, head-2400)
	}
	if n := cs.n.Load(); n > 15 {
		t.Errorf("%d Block calls, want ≤ 15", n)
	}
	if got, _ := FirstHeightAfter(context.Background(), src, hStart.Add(time.Hour), head); got != head {
		t.Errorf("every block older: %d, want head", got)
	}

	// The first start begins the deadline margin (10m of the 4h retention)
	// plus startSlack (5m) inside the window, so even the first scanned block
	// keeps 5m before its deadline while the scan reaches it.
	const start = head - 2250 // block time hStart − 3h45m
	pffAt := func(height int64, n int) ([]byte, [33]byte) {
		data, _, _ := testBlob(t, n)
		tx, id, _ := pffFor(t, data)
		blocks[height] = blockAt(height, blocks[height].Time, tx)
		return data, id
	}
	pffAt(head-2390, 1000) // in the retention window, past its deadline
	pffAt(head-2300, 1001) // deadline exactly at the start
	data, id := pffAt(start, 1002)
	h := newHarness(t, blocks, head)
	// Every block read takes 10ms of the fake clock: the scan of the first
	// 1000 heights takes 10s before their PFFs are dispatched.
	cs2 := &clockSource{countingSource: countingSource{Source: h.src}, clock: h.clock, step: 10 * time.Millisecond}
	h.cfg.HeadPoll = 10 * time.Millisecond
	h.source = cs2
	h.dl.set(id, data)
	if err := h.runUntil(h.drained(1), nil); err != nil {
		t.Fatal(err)
	}
	s := h.state()
	if s.FirstHeight != start {
		t.Errorf("state first height %d, want %d", s.FirstHeight, start)
	}
	if n := cs2.n.Load(); n > 15+(head-s.FirstHeight+1) {
		t.Errorf("%d Block calls for %d scanned heights: search used more than 15", n, head-s.FirstHeight+1)
	}
	if v := testutil.ToFloat64(h.m.Records.WithLabelValues("pff", "expired")); v != 0 {
		t.Errorf("first start wrote %v expired pff records", v)
	}
	st := h.statuses()
	if len(st) != 1 || st[fibrearchive.RecordKey(start, 0, 0)] != fibrearchive.StatusArchived {
		t.Fatalf("records %v, want only the PFF at the first scanned height, archived", st)
	}
	if !h.hasData(id) {
		t.Error("the first scanned PFF has no data")
	}
}

// clockSource advances a fake clock by step on every Block call.
type clockSource struct {
	countingSource
	clock *fakeClock
	step  time.Duration
}

func (c *clockSource) Block(ctx context.Context, h int64) (*source.Block, error) {
	c.clock.Advance(c.step)
	return c.countingSource.Block(ctx, h)
}

func TestResumeAfterLongOutageWritesExpired(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	tx, id, _ := pffFor(t, data)
	h := newHarness(t, map[int64]*source.Block{
		1: blockAt(1, hStart.Add(-5*time.Hour), tx),
		2: blockAt(2, hStart),
	}, 2)
	h.dl.set(id, data)
	if err := h.dir.Save(&State{ChainID: "mocha-5", FirstHeight: 1, ScannedUntil: 0, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	if err := h.runUntil(h.drained(1), nil); err != nil {
		t.Fatal(err)
	}
	if s := h.statuses()[pffKey]; s != fibrearchive.StatusExpired {
		t.Errorf("status %v, want expired", s)
	}
	if n := len(h.dl.callLog()); n != 0 {
		t.Errorf("downloader called %d times", n)
	}
}

func TestParamsChangeReschedules(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	tx, _, _ := pffFor(t, data)
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, tx)}, 1)
	h.dl.err = fibre.ErrNotFound
	deadlineIs := func(want time.Time) func() bool {
		return func() bool {
			s, err := h.dir.Load()
			if err != nil || s == nil || s.Entries[pffKey] == nil {
				return false
			}
			return s.Entries[pffKey].Deadline.Equal(want)
		}
	}
	_, stop := h.start()
	defer stop()
	waitFor(t, deadlineIs(hStart.Add(4*time.Hour-10*time.Minute)))
	h.params.set(time.Hour)
	waitFor(t, deadlineIs(hStart.Add(50*time.Minute)))
	if v := testutil.ToFloat64(h.m.ShardRetention); v != 3600 {
		t.Errorf("shard retention gauge %v", v)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 30s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestChainMismatchIsFatal(t *testing.T) {
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart)}, 1)
	if err := h.dir.Save(&State{ChainID: "celestia", FirstHeight: 1, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	err := h.runUntil(func() bool { return false }, nil)
	if !errors.Is(err, ErrChainMismatch) {
		t.Fatalf("Run = %v, want ErrChainMismatch", err)
	}
}

func TestMetricsGauges(t *testing.T) {
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart)}, 1)
	_, stop := h.start()
	waitFor(t, func() bool { return testutil.ToFloat64(h.m.ScannedHeight) == 1 })
	if v := testutil.ToFloat64(h.m.NearestDeadline); !math.IsNaN(v) {
		t.Errorf("nearest deadline with nothing pending = %v, want NaN", v)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	data, _, _ := testBlob(t, 1000)
	tx, _, _ := pffFor(t, data)
	h = newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, tx)}, 1)
	h.dl.err = fibre.ErrNotFound
	_, stop = h.start()
	defer stop()
	want := (4*time.Hour - 10*time.Minute).Seconds()
	waitFor(t, func() bool { return testutil.ToFloat64(h.m.NearestDeadline) == want })
	if v := testutil.ToFloat64(h.m.Pending); v != 1 {
		t.Errorf("pending %v", v)
	}
	if v := testutil.ToFloat64(h.m.DailyCap); v != 50e9 {
		t.Errorf("daily cap gauge %v", v)
	}
}

// TestNearestDeadlineCountsPFFOnly: an un-uploaded promise timeout is expected
// to wait until its deadline, so only PFF entries drive the deadline gauge.
func TestNearestDeadlineCountsPFFOnly(t *testing.T) {
	d1, _, _ := testBlob(t, 1000)
	d2, _, _ := testBlob(t, 1001)
	id1, size1 := blobIDOf(t, d1)
	tx2, _, _ := pffFor(t, d2)
	h := newHarness(t, map[int64]*source.Block{
		1: blockAt(1, hStart.Add(-time.Hour), timeoutTx(t, id1, size1)),
		2: blockAt(2, hStart, tx2),
	}, 2)
	h.dl.err = fibre.ErrNotFound
	_, stop := h.start()
	defer stop()
	want := (4*time.Hour - 10*time.Minute).Seconds() // the PFF's, not the timeout's 2h50m
	waitFor(t, func() bool {
		return testutil.ToFloat64(h.m.Pending) == 2 && testutil.ToFloat64(h.m.NearestDeadline) == want
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	// A pending timeout alone leaves the gauge at NaN.
	h = newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, timeoutTx(t, id1, size1))}, 1)
	h.dl.err = fibre.ErrNotFound
	_, stop = h.start()
	defer stop()
	waitFor(t, func() bool { return testutil.ToFloat64(h.m.Pending) == 1 })
	time.Sleep(20 * time.Millisecond) // a few more loop steps
	if v := testutil.ToFloat64(h.m.NearestDeadline); !math.IsNaN(v) {
		t.Errorf("nearest deadline with only a timeout pending = %v, want NaN", v)
	}
}

// TestIndexBucketFrozenOnceScanPasses: a bucket indexed while open is frozen
// later, exactly once, when the scan passes its end.
func TestIndexBucketFrozenOnceScanPasses(t *testing.T) {
	blocks := map[int64]*source.Block{}
	for i := int64(9_998); i <= 10_002; i++ {
		blocks[i] = blockAt(i, hStart)
	}
	blocks[9_999] = blockAt(9_999, hStart, malformedPFF(t))
	h := newHarness(t, blocks, 9_999)
	if err := h.dir.Save(&State{ChainID: "mocha-5", FirstHeight: 1, ScannedUntil: 9_998, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	key0 := fibrearchive.IndexKey(0)
	puts := func(cc string) int {
		n := 0
		for _, op := range h.st.ops() {
			if op == "put "+key0+" "+cc {
				n++
			}
		}
		return n
	}
	_, stop := h.start()
	defer stop()
	waitFor(t, h.drained(1))
	if n := puts(store.CacheNoCache); n == 0 {
		t.Fatal("bucket 0 not written open")
	}
	if n := puts(store.CacheImmutable); n != 0 {
		t.Fatalf("bucket 0 frozen before the scan passed it")
	}
	h.src.SetHead(10_002)
	waitFor(t, func() bool { s, _ := h.dir.Load(); return s != nil && s.FrozenThrough == 0 && s.ScannedUntil == 10_002 })
	time.Sleep(30 * time.Millisecond) // several more flushes
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := puts(store.CacheImmutable); n != 1 {
		t.Errorf("bucket 0 frozen %d times, want 1", n)
	}
	if ok, _ := h.st.Store.Exists(context.Background(), fibrearchive.IndexKey(1)); ok {
		t.Error("empty bucket 1 written")
	}
}

// TestStoreOutageSpansDeadline: an R2 outage does not stop downloads. The
// entry is spooled before its deadline and archived once the store is back.
func TestStoreOutageSpansDeadline(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	tx, id, _ := pffFor(t, data)
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart, tx)}, 1)
	h.params.set(time.Hour) // deadline = block time + 50m
	h.dl.set(id, data)
	h.st.outage.Store(true)
	phase := func() Phase {
		s, err := h.dir.Load()
		if err != nil || s == nil || s.Entries[pffKey] == nil {
			return ""
		}
		return s.Entries[pffKey].Phase
	}
	_, stop := h.start()
	defer stop()
	waitFor(t, func() bool { return phase() == PhaseSpooled })
	h.clock.Advance(51 * time.Minute) // past the deadline, store still down
	time.Sleep(50 * time.Millisecond)
	if p := phase(); p != PhaseSpooled {
		t.Fatalf("phase %q after the deadline during the outage, want spooled", p)
	}
	h.st.outage.Store(false)
	waitFor(t, h.drained(1))
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if s := h.statuses()[pffKey]; s != fibrearchive.StatusArchived {
		t.Errorf("status %v, want archived", s)
	}
	if !h.hasData(id) {
		t.Error("no data object after the store recovered")
	}
	if n := len(h.dl.callLog()); n != 1 {
		t.Errorf("downloader called %d times, want 1", n)
	}
	if v := testutil.ToFloat64(h.m.StoreErrors); v == 0 {
		t.Error("store errors not counted")
	}
}

// TestSourceErrorsWaitForHeadTick: a failing Block is retried on the head
// tick, not in a tight loop.
func TestSourceErrorsWaitForHeadTick(t *testing.T) {
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart), 2: blockAt(2, hStart)}, 2)
	if err := h.dir.Save(&State{ChainID: "mocha-5", FirstHeight: 1, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	h.src.FailNext(1, 1<<30)
	cs := &countingSource{Source: h.src}
	h.source = cs
	h.cfg.HeadPoll = 20 * time.Millisecond
	_, stop := h.start()
	time.Sleep(300 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	// ~15 head ticks in 300ms; a spinning loop makes thousands of calls.
	if n := cs.n.Load(); n < 2 || n > 30 {
		t.Errorf("%d Block calls in 300ms with a 20ms head poll", n)
	}
	if v := testutil.ToFloat64(h.m.SourceErrors); v > 30 {
		t.Errorf("source errors %v", v)
	}
}

// hangingSource blocks in Block(from) until ctx ends, then returns a wrapped
// ctx error, as the gRPC source does.
type hangingSource struct {
	source.Source
	from    int64
	entered chan struct{}
	once    sync.Once
}

func (s *hangingSource) Block(ctx context.Context, h int64) (*source.Block, error) {
	if h < s.from {
		return s.Source.Block(ctx, h)
	}
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return nil, fmt.Errorf("get block %d: %w", h, ctx.Err())
}

// TestCancelDuringScanReturnsNil: a signal while the scan waits for a block
// ends Run with nil, so the process exits 0.
func TestCancelDuringScanReturnsNil(t *testing.T) {
	h := newHarness(t, map[int64]*source.Block{1: blockAt(1, hStart), 2: blockAt(2, hStart)}, 2)
	if err := h.dir.Save(&State{ChainID: "mocha-5", FirstHeight: 1, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	hs := &hangingSource{Source: h.src, from: 2, entered: make(chan struct{})}
	h.source = hs
	done, stop := h.start()
	select {
	case <-hs.entered:
	case err := <-done:
		t.Fatalf("Run returned %v before the scan reached height 2", err)
	case <-time.After(30 * time.Second):
		t.Fatal("scan never reached height 2")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run = %v after cancel, want nil", err)
	}
}

// slowSource takes a millisecond per block, so a catch-up lasts long.
type slowSource struct{ source.Source }

func (s slowSource) Block(ctx context.Context, h int64) (*source.Block, error) {
	time.Sleep(time.Millisecond)
	return s.Source.Block(ctx, h)
}

// TestPollsRunDuringCatchUp: tombstones, params and flushes keep running
// while the scan is far behind head.
func TestPollsRunDuringCatchUp(t *testing.T) {
	const head = 40_000
	blocks := make(map[int64]*source.Block, head)
	for i := int64(1); i <= head; i++ {
		blocks[i] = blockAt(i, hStart)
	}
	h := newHarness(t, blocks, head)
	h.source = slowSource{h.src}
	if err := h.dir.Save(&State{ChainID: "mocha-5", FirstHeight: 1, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, withdrawnID, _ := testBlob(t, 100)
	if err := h.st.Store.Put(ctx, fibrearchive.DataKey(withdrawnID), []byte("published")); err != nil {
		t.Fatal(err)
	}
	_, stop := h.start()
	defer stop()
	waitFor(t, func() bool { return testutil.ToFloat64(h.m.ScannedHeight) > 0 })
	addFibreTombstone(t, h.st.Store, withdrawnID)
	h.params.set(time.Hour)
	waitFor(t, func() bool {
		m, _ := fibrearchive.LoadManifest(ctx, h.st.Store)
		return m != nil && m.ScannedUntil > 0 && !h.hasData(withdrawnID) &&
			testutil.ToFloat64(h.m.ShardRetention) == 3600
	})
	if v := testutil.ToFloat64(h.m.ScannedHeight); v >= head {
		t.Fatalf("polls ran only after the catch-up (scanned %v)", v)
	}
}

// TestFreezeAboveOpenBucket: buckets above a bucket still open (an item
// retrying in it) are frozen, whether by the flush that indexes them or
// later, and none is frozen twice once the lower bucket closes.
func TestFreezeAboveOpenBucket(t *testing.T) {
	data, _, _ := testBlob(t, 1000)
	tx, _, _ := pffFor(t, data)
	blocks := map[int64]*source.Block{}
	for i := int64(9_999); i <= 30_001; i++ {
		blocks[i] = blockAt(i, hStart)
	}
	blocks[9_999] = blockAt(9_999, hStart, tx)                // bucket 0, retrying
	blocks[10_001] = blockAt(10_001, hStart, malformedPFF(t)) // bucket 1
	blocks[20_001] = blockAt(20_001, hStart, malformedPFF(t)) // bucket 2
	h := newHarness(t, blocks, 20_001)
	h.params.set(time.Hour)
	h.dl.err = fibre.ErrNotFound
	if err := h.dir.Save(&State{ChainID: "mocha-5", FirstHeight: 1, ScannedUntil: 9_998, FrozenThrough: -1}); err != nil {
		t.Fatal(err)
	}
	frozenPuts := func(b int64) int {
		n := 0
		for _, op := range h.st.ops() {
			if op == "put "+fibrearchive.IndexKey(b)+" "+store.CacheImmutable {
				n++
			}
		}
		return n
	}
	st := func() *State { s, _ := h.dir.Load(); return s }
	_, stop := h.start()
	defer stop()
	waitFor(t, func() bool { s := st(); return s != nil && slices.Contains(s.Frozen, 1) && s.ScannedUntil == 20_001 })
	if s := st(); s.FrozenThrough != -1 || frozenPuts(0) != 0 {
		t.Fatalf("bucket 0 frozen while open: through %d", s.FrozenThrough)
	}
	h.src.SetHead(30_001)
	waitFor(t, func() bool { s := st(); return s != nil && slices.Contains(s.Frozen, 2) })
	h.clock.Advance(51 * time.Minute) // bucket 0's item expires and closes it
	waitFor(t, func() bool { s := st(); return s != nil && s.FrozenThrough == 2 && len(s.Frozen) == 0 })
	time.Sleep(30 * time.Millisecond) // several more flushes
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	for b := int64(0); b <= 2; b++ {
		if n := frozenPuts(b); n != 1 {
			t.Errorf("bucket %d frozen %d times, want 1", b, n)
		}
	}
	if s := h.statuses()[fibrearchive.RecordKey(9_999, 0, 0)]; s != fibrearchive.StatusExpired {
		t.Errorf("bucket 0 item %v, want expired", s)
	}
}

func TestLoadDefaultsFrozenThrough(t *testing.T) {
	dir, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"version":1,"chain_id":"mocha-5","first_height":1,"scanned_until":5,"counts":{},"entries":{}}`)
	if err := os.WriteFile(filepath.Join(dir.path, stateName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := dir.Load()
	if err != nil {
		t.Fatal(err)
	}
	if s.FrozenThrough != -1 || s.ScannedUntil != 5 {
		t.Errorf("frozen_through %d scanned_until %d", s.FrozenThrough, s.ScannedUntil)
	}
}
