package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/cachestore"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// fakeClock is a manual clock whose tickers fire, like time.Ticker with a
// one-tick buffer, when Advance passes their next deadline.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

type fakeTicker struct {
	d    time.Duration
	next time.Time
	c    chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Tick(d time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tk := &fakeTicker{d: d, next: c.now.Add(d), c: make(chan time.Time, 1)}
	c.tickers = append(c.tickers, tk)
	return tk.c, func() {}
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, tk := range c.tickers {
		for !tk.next.After(c.now) {
			select {
			case tk.c <- c.now:
			default:
			}
			tk.next = tk.next.Add(tk.d)
		}
	}
}

// eventually fails t unless cond holds within 10 s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// start runs Start on a server over st with a fake clock and serves it.
func start(t *testing.T, st store.Store, cfg Config) (*testServer, *fakeClock) {
	t.Helper()
	if cfg.ChainID == "" {
		cfg.ChainID = archivetest.ChainID
	}
	s, err := New(st, cfg, NewMetrics(prometheus.NewRegistry()), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	clk := newFakeClock()
	s.clock = clk
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(hs.Close)
	return &testServer{Server: s, url: hs.URL}, clk
}

// countGets counts Gets of one key and can fail them; like a network store
// it fails them on a done context.
type countGets struct {
	store.Store
	key  string
	n    atomic.Int64
	fail atomic.Bool
}

func (c *countGets) Get(ctx context.Context, key string) ([]byte, error) {
	if key == c.key {
		c.n.Add(1)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.fail.Load() {
			return nil, errors.New("injected upstream failure")
		}
	}
	return c.Store.Get(ctx, key)
}

func getFixture(t *testing.T) (params []any, ns, cm []byte) {
	t.Helper()
	call := loadCall(t, "blob.Get-1500004")
	return fixtureParams(t, "blob.Get-1500004", hOne), param[[]byte](t, call, 1), param[[]byte](t, call, 2)
}

func isWithdrawn(r rpcResponse) bool { return r.Error != nil && r.Error.Message == errWithdrawn.msg }

// TestWithdrawnAfterTombstoneReload (Review Focus 4): a tombstone added
// after Start is honoured after one TombstonePoll, and a scrubbed record
// the loaded set does not list is withdrawn on the first request.
func TestWithdrawnAfterTombstoneReload(t *testing.T) {
	params, ns, cm := getFixture(t)

	t.Run("listed", func(t *testing.T) {
		st := buildArchive(t, nil)
		ts, clk := start(t, st, Config{})
		okResult(t, ts.call(t, "blob.Get", params...))
		addTombstone(t, st, hOne, ns, cm)
		clk.Advance(ts.cfg.TombstonePoll - time.Second)
		// No tombstone tick yet, only manifest ticks; give them time to run.
		time.Sleep(50 * time.Millisecond)
		okResult(t, ts.call(t, "blob.Get", params...))
		clk.Advance(time.Second)
		eventually(t, "withdrawn after the tombstone poll", func() bool {
			return isWithdrawn(ts.call(t, "blob.Get", params...))
		})
		if n := testutil.ToFloat64(ts.m.Tombstones); n != 1 {
			t.Errorf("cba_api_tombstones = %v, want 1", n)
		}
	})

	t.Run("scrubbed, not in the loaded set", func(t *testing.T) {
		base := buildArchive(t, nil)
		st := &countGets{Store: base, key: tombstone.Key}
		ts, _ := start(t, st, Config{})
		okResult(t, ts.call(t, "blob.Get", params...))
		addTombstone(t, base, hOne, ns, cm)
		withdraw(t, base, segOneKey, cm)
		loads := st.n.Load()
		if r := ts.call(t, "blob.Get", params...); !isWithdrawn(r) {
			t.Fatalf("first request after the scrub: %+v", r.Error)
		}
		if got := st.n.Load() - loads; got != 1 {
			t.Errorf("immediate reloads = %d, want 1", got)
		}
		if !ts.Server.state().tombstones().Has(hOne, ns, cm) {
			t.Error("the immediate reload did not install the new set")
		}
		// Listed now: no further reload.
		if r := ts.call(t, "blob.Get", params...); !isWithdrawn(r) {
			t.Fatalf("second request: %+v", r.Error)
		}
		if got := st.n.Load() - loads; got != 1 {
			t.Errorf("reloads after a listed request = %d, want 1", got)
		}
	})

	t.Run("immediate reloads are rate-limited", func(t *testing.T) {
		st := &countGets{Store: buildArchive(t, nil), key: tombstone.Key}
		ts, clk := start(t, st, Config{TombstonePoll: time.Hour})
		ls := ts.Server.state().(*liveState)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // a cancelled request still gets a reload
		loads := st.n.Load()
		ls.reloadTombstones(ctx)
		ls.reloadTombstones(ctx)
		clk.Advance(reloadEvery - time.Second)
		ls.reloadTombstones(ctx)
		if got := st.n.Load() - loads; got != 1 {
			t.Fatalf("reloads within %s = %d, want 1", reloadEvery, got)
		}
		clk.Advance(time.Second)
		ls.reloadTombstones(ctx)
		if got := st.n.Load() - loads; got != 2 {
			t.Fatalf("reloads after %s = %d, want 2", reloadEvery, got)
		}
		if n := testutil.ToFloat64(ts.m.StateErrors); n != 0 {
			t.Errorf("state errors %v: the reload ran on the cancelled context", n)
		}
	})
}

func TestStartRejectsWrongChain(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name, chain, want string
		mut               func(t *testing.T, st store.Store)
	}{
		{name: "wrong chain", chain: "mocha-5", want: `chain_id "test-chain" does not match "mocha-5"`},
		{name: "format_version 1", want: "format_version 1", mut: func(t *testing.T, st store.Store) {
			putJSON(t, st, archive.ManifestKey, archive.Manifest{FormatVersion: 1, ChainID: archivetest.ChainID, HighestHeight: 6})
		}},
		{name: "no manifest", want: "read manifest", mut: func(t *testing.T, st store.Store) {
			if err := st.Delete(ctx, archive.ManifestKey); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "bad tombstones", want: "tombstone", mut: func(t *testing.T, st store.Store) {
			if err := st.Put(ctx, tombstone.Key, []byte(`{"tombstones":[{"height":0}]}`)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "tombstones unreadable", want: "injected", mut: nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			var st store.Store = buildArchive(t, nil)
			if c.mut != nil {
				c.mut(t, st)
			}
			if c.name == "tombstones unreadable" {
				cg := &countGets{Store: st, key: tombstone.Key}
				cg.fail.Store(true)
				st = cg
			}
			chain := c.chain
			if chain == "" {
				chain = archivetest.ChainID
			}
			s, err := New(st, Config{ChainID: chain}, nil, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			err = s.Start(ctx)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Start: %v, want an error containing %q", err, c.want)
			}
			if s.state() != nil {
				t.Error("state set after a failed Start")
			}
		})
	}
}

func putJSON(t *testing.T, st store.Store, key string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), key, b); err != nil {
		t.Fatal(err)
	}
}

func getHealth(t *testing.T, url string, want int) health {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	var h health
	if err := json.Unmarshal([]byte(httpBody(t, req, want)), &h); err != nil {
		t.Fatal(err)
	}
	return h
}

// TestHealthStale: after the manifest upstream has failed for 61 s, /health
// answers 503 while the previous state still serves.
func TestHealthStale(t *testing.T) {
	st := &countGets{Store: buildArchive(t, nil), key: archive.ManifestKey}
	// No tombstone reload within the test, so its age grows with the clock.
	ts, clk := start(t, st, Config{TombstonePoll: time.Hour})
	params, _, _ := getFixture(t)

	want := health{ChainID: archivetest.ChainID, HighestHeight: hHighest, ContiguousUntil: hContig}
	if h := getHealth(t, ts.url, http.StatusOK); h != want {
		t.Fatalf("health %+v, want %+v", h, want)
	}

	st.fail.Store(true)
	for range 9 {
		clk.Advance(ts.cfg.ManifestPoll)
	}
	clk.Advance(5 * time.Second)
	eventually(t, "a counted manifest failure", func() bool { return testutil.ToFloat64(ts.m.StateErrors) >= 1 })
	if h := getHealth(t, ts.url, http.StatusOK); h.ManifestAgeSeconds != 59 {
		t.Errorf("health at 59 s: %+v", h)
	}
	clk.Advance(2 * time.Second)
	want.ManifestAgeSeconds, want.TombstonesAgeSeconds = 61, 61
	if h := getHealth(t, ts.url, http.StatusServiceUnavailable); h != want {
		t.Errorf("stale health %+v, want %+v", h, want)
	}
	okResult(t, ts.call(t, "blob.Get", params...))
	// The gauges are set on manifest ticks, from the clock when the tick is
	// handled; with the clock held at the next tick (66 s) both read 66.
	clk.Advance(5 * time.Second)
	eventually(t, "age gauges at 66 s", func() bool {
		return testutil.ToFloat64(ts.m.ManifestAge) == 66 && testutil.ToFloat64(ts.m.TombstonesAge) == 66
	})

	st.fail.Store(false)
	clk.Advance(ts.cfg.ManifestPoll)
	eventually(t, "healthy after the upstream recovers", func() bool {
		return getHealthStatus(t, ts.url) == http.StatusOK
	})

	req, err := http.NewRequest(http.MethodPost, ts.url+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	httpBody(t, req, http.StatusMethodNotAllowed)
}

func getHealthStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestHealthBeforeStart(t *testing.T) {
	s, err := New(buildArchive(t, nil), Config{ChainID: archivetest.ChainID}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s.Handler())
	defer hs.Close()
	if h := getHealth(t, hs.URL, http.StatusServiceUnavailable); h.ManifestAgeSeconds != -1 || h.TombstonesAgeSeconds != -1 {
		t.Errorf("health before Start %+v", h)
	}
}

// TestStateErrorKeepsPreviousSet: a reload that fails (bad tombstones.json,
// a manifest of another chain) is counted and keeps the loaded state.
func TestStateErrorKeepsPreviousSet(t *testing.T) {
	ctx := context.Background()
	st := buildArchive(t, nil)
	params, ns, cm := getFixture(t)
	addTombstone(t, st, hOne, ns, cm)
	ts, clk := start(t, st, Config{})
	if r := ts.call(t, "blob.Get", params...); !isWithdrawn(r) {
		t.Fatalf("listed blob: %+v", r.Error)
	}

	// Only the tombstone reload can fail here: the manifest is still good.
	if err := st.Put(ctx, tombstone.Key, []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(ts.cfg.TombstonePoll)
	eventually(t, "the tombstone reload failure counted", func() bool { return testutil.ToFloat64(ts.m.StateErrors) >= 1 })
	putJSON(t, st, archive.ManifestKey, archive.Manifest{FormatVersion: archive.FormatVersion, ChainID: "mocha-5", HighestHeight: 100, ContiguousUntil: 100})
	n := testutil.ToFloat64(ts.m.StateErrors)
	clk.Advance(ts.cfg.ManifestPoll)
	eventually(t, "the manifest reload failure counted", func() bool { return testutil.ToFloat64(ts.m.StateErrors) > n })

	if r := ts.call(t, "blob.Get", params...); !isWithdrawn(r) {
		t.Fatalf("after a failed reload: %+v", r.Error)
	}
	if !ts.Server.state().tombstones().Has(hOne, ns, cm) {
		t.Error("previous tombstone set dropped")
	}
	if h, c := ts.Server.state().heights(); h != hHighest || c != hContig {
		t.Errorf("heights %d/%d after a wrong-chain manifest, want %d/%d", h, c, hHighest, hContig)
	}
	if n := testutil.ToFloat64(ts.m.Tombstones); n != 1 {
		t.Errorf("cba_api_tombstones = %v, want 1", n)
	}
	if n := testutil.ToFloat64(ts.m.HighestHeight); n != hHighest {
		t.Errorf("cba_api_archive_highest_height = %v, want %d", n, hHighest)
	}
}

// putPadded writes v as JSON followed by whitespace up to size bytes: valid
// JSON that only the size cap refuses.
func putPadded(t *testing.T, st store.Store, key string, v any, size int) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, bytes.Repeat([]byte{' '}, size-len(b))...)
	if err := st.Put(context.Background(), key, b); err != nil {
		t.Fatal(err)
	}
}

// TestStateOversizeKeepsPreviousState: a manifest.json or tombstones.json
// over maxStateObject fails the reload and keeps the loaded state.
func TestStateOversizeKeepsPreviousState(t *testing.T) {
	st := buildArchive(t, nil)
	_, ns, cm := getFixture(t)
	addTombstone(t, st, hOne, ns, cm)
	ts, clk := start(t, st, Config{})

	// An empty set that would un-withdraw the blob if it were loaded.
	putPadded(t, st, tombstone.Key, tombstone.File{}, maxStateObject+1)
	clk.Advance(ts.cfg.TombstonePoll)
	eventually(t, "the oversize tombstones reload counted", func() bool { return testutil.ToFloat64(ts.m.StateErrors) >= 1 })
	if !ts.Server.state().tombstones().Has(hOne, ns, cm) {
		t.Error("oversize tombstones.json replaced the set")
	}

	putPadded(t, st, archive.ManifestKey, archive.Manifest{FormatVersion: archive.FormatVersion,
		ChainID: archivetest.ChainID, HighestHeight: hHighest + 10, ContiguousUntil: hContig}, maxStateObject+1)
	n := testutil.ToFloat64(ts.m.StateErrors)
	clk.Advance(ts.cfg.ManifestPoll)
	eventually(t, "the oversize manifest reload counted", func() bool { return testutil.ToFloat64(ts.m.StateErrors) > n })
	if h, c := ts.Server.state().heights(); h != hHighest || c != hContig {
		t.Errorf("heights %d/%d after an oversize manifest, want %d/%d", h, c, hHighest, hContig)
	}

	// At the cap both load.
	putPadded(t, st, tombstone.Key, tombstone.File{}, maxStateObject)
	clk.Advance(ts.cfg.TombstonePoll)
	eventually(t, "a tombstones.json at the cap loaded", func() bool { return !ts.Server.state().tombstones().Has(hOne, ns, cm) })
}

// TestContiguousUntilFeedsCache: the cache built over Server.ContiguousUntil
// sees the loaded manifest, and reading it, or serving, while Start runs is
// not a data race (-race).
func TestContiguousUntilFeedsCache(t *testing.T) {
	st := buildArchive(t, nil)
	var srv atomic.Pointer[Server]
	cache := cachestore.New(st, func() int64 { return srv.Load().ContiguousUntil() }, cachestore.Options{})
	s, err := New(cache, Config{ChainID: archivetest.ChainID}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	srv.Store(s)
	if got := s.ContiguousUntil(); got != 0 {
		t.Fatalf("ContiguousUntil before Start = %d", got)
	}
	hs := httptest.NewServer(s.Handler())
	defer hs.Close()
	params, _, _ := getFixture(t)
	body := request(t, "blob.Get", params...)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.ContiguousUntil()
			for _, do := range []func() (*http.Response, error){
				func() (*http.Response, error) { return http.Post(hs.URL, "application/json", bytes.NewReader(body)) },
				func() (*http.Response, error) { return http.Get(hs.URL + "/health") },
			} {
				if resp, err := do(); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	})
	err = s.Start(t.Context())
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := s.ContiguousUntil(); got != hContig {
		t.Fatalf("ContiguousUntil = %d, want %d", got, hContig)
	}
}

// TestStartOnce: a second Start fails; a Start that failed may be retried.
func TestStartOnce(t *testing.T) {
	ctx := t.Context()
	st := &countGets{Store: buildArchive(t, nil), key: archive.ManifestKey}
	s, err := New(st, Config{ChainID: archivetest.ChainID}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	st.fail.Store(true)
	if err := s.Start(ctx); err == nil {
		t.Fatal("Start with the manifest down succeeded")
	}
	st.fail.Store(false)
	var wg sync.WaitGroup
	var ok atomic.Int32
	for range 8 {
		wg.Go(func() {
			if s.Start(ctx) == nil {
				ok.Add(1)
			}
		})
	}
	wg.Wait()
	if n := ok.Load(); n != 1 {
		t.Fatalf("%d concurrent Starts succeeded, want 1", n)
	}
	if err := s.Start(ctx); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("second Start: %v", err)
	}
}

// blockGets blocks Gets of one key, ignoring the context, while armed: an
// upstream that hangs.
type blockGets struct {
	store.Store
	key     string
	armed   atomic.Bool
	release chan struct{}
	blocked chan struct{} // gets a value when a Get blocks
}

func (b *blockGets) Get(ctx context.Context, key string) ([]byte, error) {
	if key == b.key && b.armed.Load() {
		select {
		case b.blocked <- struct{}{}:
		default:
		}
		<-b.release
	}
	return b.Store.Get(ctx, key)
}

// TestImmediateReloadBoundedByRequest: a request whose immediate tombstone
// reload has to wait for a hung scheduled load still answers within its
// RequestTimeout.
func TestImmediateReloadBoundedByRequest(t *testing.T) {
	base := buildArchive(t, nil)
	st := &blockGets{Store: base, key: tombstone.Key, release: make(chan struct{}), blocked: make(chan struct{}, 1)}
	t.Cleanup(func() { close(st.release) })
	const timeout = 300 * time.Millisecond
	ts, clk := start(t, st, Config{RequestTimeout: timeout})
	params, ns, cm := getFixture(t)

	// The scheduled load hangs in the upstream, holding the load slot.
	st.armed.Store(true)
	clk.Advance(ts.cfg.TombstonePoll)
	select {
	case <-st.blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("the scheduled tombstones load did not start")
	}

	// A scrubbed, unlisted record triggers an immediate reload.
	addTombstone(t, base, hOne, ns, cm)
	withdraw(t, base, segOneKey, cm)
	began := time.Now()
	if r := ts.call(t, "blob.Get", params...); !isWithdrawn(r) {
		t.Fatalf("scrubbed record: %+v", r.Error)
	}
	if took := time.Since(began); took > timeout+time.Second {
		t.Fatalf("call took %s with RequestTimeout %s", took, timeout)
	}
	eventually(t, "the timed-out immediate reload counted", func() bool { return testutil.ToFloat64(ts.m.StateErrors) >= 1 })
}

// TestImmediateReloadKeepsCallerDeadline: the reload ends at the caller's
// remaining deadline, not a fresh RequestTimeout, even when the caller's
// context is already cancelled.
func TestImmediateReloadKeepsCallerDeadline(t *testing.T) {
	ts, _ := start(t, buildArchive(t, nil), Config{RequestTimeout: time.Minute})
	ls := ts.Server.state().(*liveState)
	ls.tombSem <- struct{}{} // a load in progress
	defer func() { <-ls.tombSem }()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	began := time.Now()
	ls.reloadTombstones(ctx)
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("reload took %s with 100ms left on the caller's deadline", took)
	}
}
