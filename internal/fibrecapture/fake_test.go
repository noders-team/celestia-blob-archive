package fibrecapture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/fibre"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"github.com/prometheus/client_golang/prometheus"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// Advance moves the fake clock (cap_test.go) forward.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type dlCall struct {
	id [33]byte
	at time.Time
}

// fakeDownloader serves payloads by blob ID and records every call.
type fakeDownloader struct {
	mu    sync.Mutex
	data  map[[33]byte][]byte
	err   error // returned for every call when set
	now   func() time.Time
	calls []dlCall
}

func newFakeDownloader(now func() time.Time) *fakeDownloader {
	return &fakeDownloader{data: map[[33]byte][]byte{}, now: now}
}

func (f *fakeDownloader) set(id [33]byte, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[id] = data
}

func (f *fakeDownloader) Download(ctx context.Context, id [33]byte, _ uint64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, dlCall{id: id, at: f.now()})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	d, ok := f.data[id]
	if !ok {
		return nil, fibre.ErrNotFound
	}
	return bytes.Clone(d), nil
}

func (f *fakeDownloader) callLog() []dlCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]dlCall(nil), f.calls...)
}

// fakeParams returns settable Fibre parameters.
type fakeParams struct {
	mu        sync.Mutex
	retention time.Duration
}

func (p *fakeParams) Fibre(context.Context) (time.Duration, time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retention, time.Hour, nil
}

func (p *fakeParams) set(r time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retention = r
}

var errDead = errors.New("process is dead")

// crashStore wraps a store: after reports every completed write, and once
// dead every write fails, as if the process had died.
type crashStore struct {
	store.Store
	mu    sync.Mutex
	dead  bool
	after func(op, key string, opts store.PutOptions)
	// before, when set, runs before every write.
	before func(op, key string)
	log    []string // "<op> <key> <cache-control>"
	// outage makes every call on a fibre/ key fail, like an R2 outage.
	outage atomic.Bool
}

var errOutage = errors.New("store outage")

func (s *crashStore) down(key string) bool {
	return s.outage.Load() && strings.HasPrefix(key, "fibre/")
}

func (s *crashStore) pre(op, key string) error {
	s.mu.Lock()
	before := s.before
	s.mu.Unlock()
	if before != nil {
		before(op, key)
	}
	if s.isDead() {
		return errDead
	}
	if s.down(key) {
		return errOutage
	}
	return nil
}

func (s *crashStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.down(key) {
		return nil, errOutage
	}
	return s.Store.Get(ctx, key)
}

func (s *crashStore) Exists(ctx context.Context, key string) (bool, error) {
	if s.down(key) {
		return false, errOutage
	}
	return s.Store.Exists(ctx, key)
}

func (s *crashStore) isDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

func (s *crashStore) setDead(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = v
}

func (s *crashStore) done(op, key string, opts []store.PutOptions) {
	o := store.PutOptions{}
	if len(opts) > 0 {
		o = opts[0]
	}
	s.mu.Lock()
	s.log = append(s.log, op+" "+key+" "+o.CacheControl)
	after := s.after
	s.mu.Unlock()
	if after != nil {
		after(op, key, o)
	}
}

func (s *crashStore) ops() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

func (s *crashStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if err := s.pre("putifabsent", key); err != nil {
		return err
	}
	if err := s.Store.PutIfAbsent(ctx, key, b, opts...); err != nil {
		return err
	}
	s.done("putifabsent", key, opts)
	return nil
}

func (s *crashStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	if err := s.pre("put", key); err != nil {
		return err
	}
	if err := s.Store.Put(ctx, key, b, opts...); err != nil {
		return err
	}
	s.done("put", key, opts)
	return nil
}

func (s *crashStore) Delete(ctx context.Context, key string) error {
	if err := s.pre("delete", key); err != nil {
		return err
	}
	return s.Store.Delete(ctx, key)
}

// pffTx builds a PFF tx around the real fixture promise with the given blob
// ID and blob_size.
func pffTx(t *testing.T, id [33]byte, blobSize uint32) []byte {
	t.Helper()
	_, msg := fixturePFF(t)
	msg.PaymentPromise.Commitment = bytes.Clone(id[1:])
	msg.PaymentPromise.BlobVersion = uint32(id[0])
	msg.PaymentPromise.BlobSize = blobSize
	return buildTx(t, anyMsg(t, fibretypes.MsgPayForFibreTypeURL, msg))
}

// timeoutTx builds a payment-promise-timeout tx for the same promise as
// pffTx(id, blobSize).
func timeoutTx(t *testing.T, id [33]byte, blobSize uint32) []byte {
	t.Helper()
	_, msg := fixturePFF(t)
	msg.PaymentPromise.Commitment = bytes.Clone(id[1:])
	msg.PaymentPromise.BlobVersion = uint32(id[0])
	msg.PaymentPromise.BlobSize = blobSize
	timeout := &fibretypes.MsgPaymentPromiseTimeout{Signer: msg.Signer, PaymentPromise: msg.PaymentPromise}
	return buildTx(t, anyMsg(t, timeoutTypeURL, timeout))
}

// malformedPFF is a PFF whose promise carries a 31-byte commitment.
func malformedPFF(t *testing.T) []byte {
	t.Helper()
	_, msg := fixturePFF(t)
	msg.PaymentPromise.Commitment = msg.PaymentPromise.Commitment[:31]
	return buildTx(t, anyMsg(t, fibretypes.MsgPayForFibreTypeURL, msg))
}

// pffFor returns a PFF tx for data, its blob ID and blob_size.
func pffFor(t *testing.T, data []byte) ([]byte, [33]byte, uint32) {
	t.Helper()
	id, size := blobIDOf(t, data)
	return pffTx(t, id, size), id, size
}

func blobIDOf(t *testing.T, data []byte) (id [33]byte, size uint32) {
	t.Helper()
	cfg, err := fibre.BlobConfigForVersion(0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fibre.NewBlob(bytes.Clone(data), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Free()
	copy(id[:], b.ID())
	return id, uint32(cfg.UploadSize(len(data)))
}

// harness wires Run to fakes over a temp state dir and an FS store.
type harness struct {
	t      *testing.T
	src    *source.Fake
	st     *crashStore
	dir    *Dir
	dl     *fakeDownloader
	params *fakeParams
	clock  *fakeClock
	m      *Metrics
	cfg    Config
	// source, when set, replaces src in Deps.
	source source.Source
	// dirHook, when set, runs after each rename in the state dir (see deadFS).
	dirHook func(dst string)
}

var hStart = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

func newHarness(t *testing.T, blocks map[int64]*source.Block, head int64) *harness {
	t.Helper()
	dir, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: hStart}
	h := &harness{
		t:      t,
		src:    source.NewFake(blocks, head),
		st:     &crashStore{Store: store.NewFS(t.TempDir())},
		dir:    dir,
		dl:     newFakeDownloader(clock.now),
		params: &fakeParams{retention: 4 * time.Hour},
		clock:  clock,
	}
	h.cfg = Config{
		ChainID: "mocha-5", DailyCapBytes: 50_000_000_000, DownloadWorkers: 2,
		HeadPoll: time.Millisecond, ParamsPoll: 5 * time.Millisecond, TombstonePoll: 5 * time.Millisecond,
		FlushEvery: 5 * time.Millisecond, Now: clock.now,
	}
	return h
}

func (h *harness) deps() Deps {
	h.m = NewMetrics(prometheus.NewRegistry())
	var src source.Source = h.src
	if h.source != nil {
		src = h.source
	}
	return Deps{Source: src, Downloader: h.dl, Params: h.params, Store: h.st, Dir: h.dir, Metrics: h.m}
}

// start runs Run in the background; stop cancels it and returns its error.
func (h *harness) start() (done <-chan error, stop func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	d := h.deps()
	go func() { ch <- Run(ctx, h.cfg, d) }()
	var once sync.Once
	var err error
	return ch, func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-ch:
			case <-time.After(30 * time.Second):
				h.t.Fatal("Run did not return after cancel")
			}
		})
		return err
	}
}

// runUntil runs Run until cond holds (or Run returns by itself) and returns
// Run's error. tick, if set, runs between polls (e.g. to advance the clock).
func (h *harness) runUntil(cond func() bool, tick func()) error {
	h.t.Helper()
	done, stop := h.start()
	deadline := time.After(30 * time.Second)
	for !cond() {
		select {
		case err := <-done:
			return err
		case <-deadline:
			_ = stop()
			h.t.Fatal("condition not reached within 30s")
		case <-time.After(2 * time.Millisecond):
		}
		if tick != nil {
			tick()
		}
	}
	return stop()
}

// drained reports whether the manifest shows head scanned, nothing pending
// and n indexed entries.
func (h *harness) drained(n int) func() bool {
	return func() bool {
		m, err := fibrearchive.LoadManifest(context.Background(), h.st.Store)
		if err != nil || m == nil {
			return false
		}
		head, _ := h.src.Head(context.Background())
		return m.Pending == 0 && m.ScannedUntil == head && len(h.indexEntries()) == n
	}
}

func (h *harness) indexEntries() []fibrearchive.IndexEntry {
	ctx := context.Background()
	keys, err := h.st.Store.List(ctx, fibrearchive.IndexPrefix)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []fibrearchive.IndexEntry
	for _, k := range keys {
		raw, err := h.st.Store.Get(ctx, k)
		if err != nil {
			h.t.Fatal(err)
		}
		var b fibrearchive.IndexBucket
		if err := json.Unmarshal(raw, &b); err != nil {
			h.t.Fatal(err)
		}
		out = append(out, b.Entries...)
	}
	return out
}

// records returns the decoded records by key.
func (h *harness) records() map[string]*archivev1.FibreRecord {
	ctx := context.Background()
	keys, err := h.st.Store.List(ctx, fibrearchive.RecordPrefix)
	if err != nil {
		h.t.Fatal(err)
	}
	out := map[string]*archivev1.FibreRecord{}
	for _, k := range keys {
		raw, err := h.st.Store.Get(ctx, k)
		if err != nil {
			h.t.Fatal(err)
		}
		r, err := fibrearchive.DecodeRecord(raw)
		if err != nil {
			h.t.Fatal(err)
		}
		out[k] = r
	}
	return out
}

func (h *harness) statuses() map[string]fibrearchive.Status {
	out := map[string]fibrearchive.Status{}
	for k, r := range h.records() {
		out[k] = fibrearchive.Status(r.DataStatus)
	}
	return out
}

func (h *harness) state() *State {
	h.t.Helper()
	s, err := h.dir.Load()
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}

func (h *harness) spoolFiles() []string {
	ents, err := os.ReadDir(filepath.Join(h.dir.path, spoolDirName))
	if err != nil {
		h.t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func (h *harness) hasData(id [33]byte) bool {
	ok, err := h.st.Store.Exists(context.Background(), fibrearchive.DataKey(id))
	if err != nil {
		h.t.Fatal(err)
	}
	return ok
}

func blockAt(height int64, t time.Time, txs ...[]byte) *source.Block {
	return &source.Block{Height: height, Time: t, Txs: txs}
}
