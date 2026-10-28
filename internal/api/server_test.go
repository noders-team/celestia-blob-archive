package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// Heights of the test archive: 1-2 and 5-6 are archived (3-4 are a gap),
// 7 is above the highest archived height.
const (
	hOne     = 2 // mainnet-1500004
	hTwo     = 6 // mainnet-15300000
	hGap     = 3
	hFuture  = 7
	hHighest = 6
	hContig  = 2
	fixOne   = "mainnet-1500004"
	fixTwo   = "mainnet-15300000"
)

var segOneKey = archive.SegmentKey(1, 2)

// buildArchive archives heights 1-2 and 5-6 into a fresh FS store; mut may
// change the records before they are archived.
func buildArchive(t *testing.T, mut archivetest.Mutate) store.Store {
	t.Helper()
	blocks := archivetest.Blocks(t, 7, map[int64]string{hOne: fixOne, hTwo: fixTwo})
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 1, 2, 2, mut)
	archivetest.Build(t, st, blocks, 5, 6, 2, mut)
	return st
}

// testState is the archive state the server reads: fixed heights and a
// tombstone set that reloadTombstones re-reads from the store.
type testState struct {
	st                  store.Store
	highest, contiguous int64

	mu      sync.Mutex
	set     *tombstone.Set
	reloads int
}

func newTestState(t *testing.T, st store.Store) *testState {
	t.Helper()
	set, err := tombstone.Load(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	return &testState{st: st, highest: hHighest, contiguous: hContig, set: set}
}

func (f *testState) heights() (int64, int64) { return f.highest, f.contiguous }

func (f *testState) manifestAge() time.Duration { return 0 }

func (f *testState) tombstonesAge() time.Duration { return 0 }

func (f *testState) tombstones() *tombstone.Set {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.set
}

func (f *testState) reloadTombstones(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reloads++
	if s, err := tombstone.Load(ctx, f.st); err == nil {
		f.set = s
	}
}

func (f *testState) reloadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reloads
}

type testServer struct {
	*Server
	url   string
	state *testState
}

// serve starts the API over st (state is read from base, which defaults to
// st) and returns its URL.
func serve(t *testing.T, st store.Store, cfg Config, base ...store.Store) *testServer {
	t.Helper()
	stateStore := st
	if len(base) > 0 {
		stateStore = base[0]
	}
	if cfg.ChainID == "" {
		cfg.ChainID = archivetest.ChainID
	}
	s, err := New(st, cfg, NewMetrics(prometheus.NewRegistry()), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	state := newTestState(t, stateStore)
	s.setState(state)
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(hs.Close)
	return &testServer{Server: s, url: hs.URL, state: state}
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *JSONRPCError   `json:"error"`
}

func post(t *testing.T, url string, body []byte) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

func request(t *testing.T, method string, params ...any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (ts *testServer) call(t *testing.T, method string, params ...any) rpcResponse {
	t.Helper()
	return ts.raw(t, request(t, method, params...))
}

func (ts *testServer) raw(t *testing.T, body []byte) rpcResponse {
	t.Helper()
	resp, b := post(t, ts.url, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %s: %s", resp.Status, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type %q", ct)
	}
	var r rpcResponse
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	if r.JSONRPC != "2.0" {
		t.Fatalf("jsonrpc %q in %s", r.JSONRPC, b)
	}
	return r
}

// fixtureParams returns the params of a captured node request with the
// height replaced by h.
func fixtureParams(t *testing.T, name string, h int64) []any {
	t.Helper()
	c := loadCall(t, name)
	out := []any{h}
	for _, p := range c.Params[1:] {
		out = append(out, p)
	}
	return out
}

func mustErr(t *testing.T, r rpcResponse) *JSONRPCError {
	t.Helper()
	if r.Error == nil {
		t.Fatalf("no error, result %s", r.Result)
	}
	return r.Error
}

// faultStore overrides GetRange of its base store.
type faultStore struct {
	store.Store
	getRange func(ctx context.Context, key string, off, n int64) ([]byte, error)
}

func (f faultStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return f.getRange(ctx, key, off, n)
}

// testNamespace returns a usable, non-reserved version 0 namespace.
func testNamespace(i int) []byte {
	ns := make([]byte, 29)
	ns[20] = 0x42
	ns[28] = byte(i)
	return ns
}

var blobMethods = []string{"blob.Get", "blob.GetAll", "blob.GetProof", "blob.Included"}

// methodParams returns the params of method for the blob of the
// blob.Get-1500004 fixture at height h, with namespace ns if not nil.
func methodParams(t *testing.T, method string, h, ns any) []any {
	t.Helper()
	get := loadCall(t, "blob.Get-1500004")
	if ns == nil {
		ns = get.Params[1]
	}
	cm := get.Params[2]
	switch method {
	case "blob.Get", "blob.GetProof":
		return []any{h, ns, cm}
	case "blob.GetAll":
		return []any{h, []any{ns}}
	case "blob.Included":
		return []any{h, ns, loadCall(t, "blob.Included-1500004").Params[2], cm}
	}
	t.Fatalf("no params for %s", method)
	return nil
}

// httpBody sends req and returns the body of a response with status want.
func httpBody(t *testing.T, req *http.Request, want int) string {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Errorf("status %s, want %d", resp.Status, want)
	}
	return string(b)
}

// TestErrorsNeverClaimNotFound (Review Focus 3): only an archived height
// without the blob may say "not found"; op-alt-da and ev-node treat that
// substring as final.
func TestErrorsNeverClaimNotFound(t *testing.T) {
	good := buildArchive(t, nil)
	nsB, cmB := param[[]byte](t, loadCall(t, "blob.Get-1500004"), 1), param[[]byte](t, loadCall(t, "blob.Get-1500004"), 2)

	upstream500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	t.Cleanup(upstream500.Close)
	down := faultStore{good, func(context.Context, string, int64, int64) ([]byte, error) {
		return nil, errors.New("connection refused")
	}}

	// Each case runs over all four blob methods, for the fixture blob with
	// the height h (and namespace ns, if set).
	cases := []struct {
		name string
		ts   func(t *testing.T) *testServer
		h    any
		ns   any
		want string
	}{
		{"above highest", func(t *testing.T) *testServer { return serve(t, good, Config{}) }, hFuture, nil, "given height is from the future"},
		// Answered from the manifest, without an upstream read.
		{"above highest, upstream down", func(t *testing.T) *testServer { return serve(t, down, Config{}, good) }, hFuture, nil, "given height is from the future"},
		{"above int64", func(t *testing.T) *testServer { return serve(t, good, Config{}) }, uint64(1) << 63, nil, "given height is from the future"},
		{"gap", func(t *testing.T) *testServer { return serve(t, good, Config{}) }, hGap, nil, "not archived"},
		{"height 0", func(t *testing.T) *testServer { return serve(t, good, Config{}) }, 0, nil, "header: height is equal to 0"},
		{"withdrawn listed", func(t *testing.T) *testServer {
			st := buildArchive(t, nil)
			addTombstone(t, st, hOne, nsB, cmB)
			return serve(t, st, Config{})
		}, hOne, nil, "blob withdrawn from this archive"},
		{"withdrawn scrubbed", func(t *testing.T) *testServer {
			st := buildArchive(t, nil)
			withdraw(t, st, segOneKey, cmB)
			return serve(t, st, Config{})
		}, hOne, nil, "blob withdrawn from this archive"},
		{"upstream 500", func(t *testing.T) *testServer { return serve(t, client.New(upstream500.URL), Config{}, good) }, hOne, nil, "archive: upstream unavailable"},
		{"upstream down", func(t *testing.T) *testServer { return serve(t, down, Config{}, good) }, hOne, nil, "archive: upstream unavailable"},
		{"upstream timeout", func(t *testing.T) *testServer {
			return serve(t, faultStore{good, func(ctx context.Context, _ string, _, _ int64) ([]byte, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}}, Config{RequestTimeout: 50 * time.Millisecond}, good)
		}, hOne, nil, "archive: upstream unavailable"},
		{"corrupt row", func(t *testing.T) *testServer {
			return serve(t, faultStore{good, func(ctx context.Context, key string, off, n int64) ([]byte, error) {
				b, err := good.GetRange(ctx, key, off, n)
				if strings.HasSuffix(key, ".idx2") && err == nil {
					b = bytes.Repeat([]byte{0xff}, len(b))
				}
				return b, err
			}}, Config{}, good)
		}, hOne, nil, "failed verification"},
		{"record fails CheckRecord", func(t *testing.T) *testServer { return serve(t, buildArchive(t, flipData(cmB)), Config{}) },
			hOne, nil, "archive: stored record for height 2 failed verification"},
		{"bad params", func(t *testing.T) *testServer { return serve(t, good, Config{}) }, hOne, "AAAA", "invalid params"},
	}
	check := func(t *testing.T, msg, want string) {
		t.Helper()
		if strings.Contains(msg, "not found") {
			t.Errorf("message %q claims not found", msg)
		}
		if !strings.Contains(msg, want) {
			t.Errorf("message %q does not contain %q", msg, want)
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts := c.ts(t)
			for _, m := range blobMethods {
				t.Run(m, func(t *testing.T) {
					check(t, mustErr(t, ts.call(t, m, methodParams(t, m, c.h, c.ns)...)).Message, c.want)
				})
			}
		})
	}

	ts := serve(t, good, Config{})
	t.Run("unsupported method", func(t *testing.T) {
		check(t, mustErr(t, ts.call(t, "blob.Subscribe", nsB)).Message, "unsupported method blob.Subscribe")
	})
	t.Run("websocket", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		check(t, httpBody(t, req, http.StatusBadRequest), "websocket is not supported; use HTTP POST")
	})
	t.Run("unknown path", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, ts.url+"/x", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		check(t, httpBody(t, req, http.StatusNotFound), "unknown path")
	})

	// The exact texts of the global constraints.
	for h, want := range map[int64]string{
		hFuture: "archive: height 7 not archived yet (header: given height is from the future: highest archived height 6)",
		hGap:    "archive: height 3 not archived (gap in the archive; contiguous up to 2, highest archived 6)",
	} {
		for _, m := range blobMethods {
			if e := mustErr(t, ts.call(t, m, methodParams(t, m, h, nil)...)); e.Code != 1 || e.Message != want {
				t.Errorf("%s height %d: %+v, want code 1 %q", m, h, e, want)
			}
		}
	}
	e := mustErr(t, ts.call(t, "blob.Subscribe"))
	if e.Code != -32601 || e.Message != "unsupported method blob.Subscribe: the archive API serves blob.Get, blob.GetAll, blob.GetProof, blob.Included, header.NetworkHead, header.GetByHeight" {
		t.Errorf("unsupported method: %+v", e)
	}
}

func addTombstone(t *testing.T, st store.Store, h int64, ns, cm []byte) {
	t.Helper()
	err := tombstone.Add(context.Background(), st, tombstone.Entry{
		Height: h, Namespace: fmt.Sprintf("%x", ns), Commitment: fmt.Sprintf("%x", cm),
		Date: "2026-10-27", Reason: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProtocol(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	get := fixtureParams(t, "blob.Get-1500004", hOne)

	t.Run("id echoed", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "abc", "method": "blob.Get", "params": get})
		if r := ts.raw(t, body); string(r.ID) != `"abc"` || r.Error != nil {
			t.Fatalf("id %s error %+v", r.ID, r.Error)
		}
	})
	t.Run("batch in order without notifications", func(t *testing.T) {
		calls := []map[string]any{
			{"jsonrpc": "2.0", "id": 1, "method": "blob.Get", "params": get},
			{"jsonrpc": "2.0", "method": "blob.Get", "params": get},
			{"jsonrpc": "2.0", "id": 3, "method": "nope"},
			{"jsonrpc": "2.0", "id": 4, "method": "blob.Get", "params": []any{0, get[1], get[2]}},
		}
		body, _ := json.Marshal(calls)
		resp, b := post(t, ts.url, body)
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("%s %q", resp.Status, resp.Header.Get("Content-Type"))
		}
		var rs []rpcResponse
		if err := json.Unmarshal(b, &rs); err != nil {
			t.Fatal(err)
		}
		if len(rs) != 3 || string(rs[0].ID) != "1" || string(rs[1].ID) != "3" || string(rs[2].ID) != "4" {
			t.Fatalf("batch responses %s", b)
		}
		if rs[0].Error != nil || rs[1].Error.Code != -32601 || rs[2].Error.Code != 1 {
			t.Fatalf("batch responses %s", b)
		}
	})
	t.Run("notification only", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "blob.Get", "params": get})
		resp, b := post(t, ts.url, body)
		if resp.StatusCode != http.StatusNoContent || len(b) != 0 {
			t.Fatalf("%s %q", resp.Status, b)
		}
	})
	for name, c := range map[string]struct {
		body string
		code int
	}{
		"parse error":     {`{"jsonrpc":`, -32700},
		"empty batch":     {`[]`, -32600},
		"no method":       {`{"jsonrpc":"2.0","id":1}`, -32600},
		"wrong version":   {`{"jsonrpc":"1.0","id":1,"method":"blob.Get"}`, -32600},
		"object params":   {`{"jsonrpc":"2.0","id":1,"method":"blob.Get","params":{"height":2}}`, -32602},
		"param count":     {`{"jsonrpc":"2.0","id":1,"method":"blob.Get","params":[2]}`, -32602},
		"negative height": {`{"jsonrpc":"2.0","id":1,"method":"blob.Get","params":[-2,"AA==","AA=="]}`, -32602},
		"string height":   {`{"jsonrpc":"2.0","id":1,"method":"blob.Get","params":["2","AA==","AA=="]}`, -32602},
	} {
		t.Run(name, func(t *testing.T) {
			if e := mustErr(t, ts.raw(t, []byte(c.body))); e.Code != c.code || strings.Contains(e.Message, "not found") {
				t.Fatalf("%+v, want code %d", e, c.code)
			}
		})
	}
	t.Run("reserved namespace", func(t *testing.T) {
		tx := make([]byte, 29)
		tx[28] = 1 // the tx namespace
		if e := mustErr(t, ts.call(t, "blob.Get", hOne, tx, get[2])); e.Code != -32602 {
			t.Fatalf("%+v", e)
		}
	})
	t.Run("other paths and methods", func(t *testing.T) {
		resp, err := http.Get(ts.url)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("GET /: %s", resp.Status)
		}
		resp, err = http.Post(ts.url+"/x", "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("POST /x: %s", resp.Status)
		}
	})
}

func TestLimits(t *testing.T) {
	st := buildArchive(t, nil)
	get := fixtureParams(t, "blob.Get-1500004", hOne)

	t.Run("body", func(t *testing.T) {
		ts := serve(t, st, Config{})
		body := append([]byte(`[`), bytes.Repeat([]byte(" "), 1<<20)...)
		resp, _ := post(t, ts.url, body)
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status %s", resp.Status)
		}
		// Exactly 1 MiB is accepted.
		ok := append(request(t, "blob.Get", get...), bytes.Repeat([]byte(" "), 1<<20)...)
		ok = ok[:1<<20]
		resp, b := post(t, ts.url, ok)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("1 MiB body: %s %s", resp.Status, b)
		}
	})
	t.Run("batch", func(t *testing.T) {
		ts := serve(t, st, Config{})
		var calls []json.RawMessage
		for range 17 {
			calls = append(calls, request(t, "blob.Get", get...))
		}
		body, _ := json.Marshal(calls)
		if e := mustErr(t, ts.raw(t, body)); e.Code != -32600 {
			t.Fatalf("%+v", e)
		}
		body, _ = json.Marshal(calls[:16])
		resp, b := post(t, ts.url, body)
		var rs []rpcResponse
		if err := json.Unmarshal(b, &rs); err != nil || resp.StatusCode != http.StatusOK || len(rs) != 16 {
			t.Fatalf("16 calls: %s %s", resp.Status, b)
		}
	})
	t.Run("namespaces", func(t *testing.T) {
		ts := serve(t, st, Config{})
		var nss [][]byte
		for i := range 17 {
			nss = append(nss, testNamespace(i+1))
		}
		if e := mustErr(t, ts.call(t, "blob.GetAll", hOne, nss)); e.Code != -32602 {
			t.Fatalf("17 namespaces: %+v", e)
		}
		if r := ts.call(t, "blob.GetAll", hOne, nss[:16]); r.Error != nil {
			t.Fatalf("16 namespaces: %+v", r.Error)
		}
		if e := mustErr(t, ts.call(t, "blob.GetAll", hOne, [][]byte{nss[0], nss[1], nss[0]})); e.Code != -32602 {
			t.Fatalf("duplicate: %+v", e)
		}
	})
	t.Run("slow upstream", func(t *testing.T) {
		slow := faultStore{st, func(ctx context.Context, key string, off, n int64) ([]byte, error) {
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return st.GetRange(ctx, key, off, n)
		}}
		ts := serve(t, slow, Config{RequestTimeout: 100 * time.Millisecond}, st)
		for _, r := range []rpcResponse{
			ts.call(t, "blob.Get", get...),
			ts.call(t, "blob.GetAll", fixtureParams(t, "blob.GetAll-1500004-one", hOne)...),
		} {
			if e := mustErr(t, r); e.Code != 1 || e.Message != "archive: upstream unavailable" {
				t.Fatalf("%+v", e)
			}
		}
	})
	// The raw segment bytes one call reads, from the archiveread hooks.
	ns, cm := param[[]byte](t, loadCall(t, "blob.Get-1500004"), 1), param[[]byte](t, loadCall(t, "blob.Get-1500004"), 2)
	var rawAll, rawOne int64
	if _, _, err := archiveread.ReadNamespaces(context.Background(), st, hOne, [][29]byte{ns29(t, ns)},
		func(n int64) (func(), error) { rawAll = n; return func() {}, nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := archiveread.ReadBlobReserve(context.Background(), st, hOne, ns, cm,
		func(n int64) (func(), error) { rawOne = n; return func() {}, nil }); err != nil {
		t.Fatal(err)
	}
	for _, m := range blobMethods {
		raw := rawOne
		if m == "blob.GetAll" {
			raw = rawAll
		}
		// A call reserves 3× its raw bytes: a budget of exactly that is one
		// call's worth.
		t.Run("in-flight bytes "+m, func(t *testing.T) { testInflight(t, st, m, 3*raw) })
	}
	t.Run("batch holds its reservations until the reply", func(t *testing.T) { testBatchInflight(t, st, 3*rawOne) })
}

// testBatchInflight: with a budget of one call's reservation, a batch of two
// such calls cannot pass, because the first keeps its bytes until the
// whole reply is written. Its second call waits and fails as upstream
// unavailable, and the budget is whole again afterwards.
func testBatchInflight(t *testing.T, st store.Store, budget int64) {
	ts := serve(t, st, Config{MaxInflightBytes: budget, RequestTimeout: 300 * time.Millisecond})
	one := request(t, "blob.Get", methodParams(t, "blob.Get", hOne, nil)...)
	body, err := json.Marshal([]json.RawMessage{one, one})
	if err != nil {
		t.Fatal(err)
	}
	resp, b := post(t, ts.url, body)
	var rs []rpcResponse
	if err := json.Unmarshal(b, &rs); err != nil || resp.StatusCode != http.StatusOK || len(rs) != 2 {
		t.Fatalf("batch: %s %s", resp.Status, b)
	}
	var ok, upstream int
	for _, r := range rs {
		switch {
		case r.Error == nil:
			ok++
		case r.Error.Code == 1 && r.Error.Message == "archive: upstream unavailable":
			upstream++
		default:
			t.Fatalf("batch call: %+v", r.Error)
		}
	}
	if ok != 1 || upstream != 1 {
		t.Fatalf("%d calls served, %d upstream unavailable; want 1 and 1: %.300s", ok, upstream, b)
	}
	// Every reservation was returned after the reply.
	if !ts.sem.TryAcquire(budget) {
		t.Fatal("budget not released after the batch reply")
	}
	ts.sem.Release(budget)
}

// testInflight checks that with a budget of one call's reservation a second
// concurrent call of method waits for the first, then succeeds, and that it
// fails as upstream unavailable if the budget is held past its timeout.
func testInflight(t *testing.T, st store.Store, method string, budget int64) {
	body := request(t, method, methodParams(t, method, hOne, nil)...)
	{
		var segReads atomic.Int32
		entered, release := make(chan struct{}), make(chan struct{})
		gated := faultStore{st, func(ctx context.Context, key string, off, n int64) ([]byte, error) {
			if strings.HasSuffix(key, ".seg") && segReads.Add(1) == 1 {
				close(entered)
				<-release
			}
			return st.GetRange(ctx, key, off, n)
		}}
		ts := serve(t, gated, Config{MaxInflightBytes: budget, RequestTimeout: 10 * time.Second}, st)
		first := callAsync(ts.url, body)
		<-entered
		second := callAsync(ts.url, body)
		// Unblock the first call if the test fails before it does.
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		select {
		case r := <-second:
			t.Fatalf("second call did not wait: error %v, rpc error %+v", r.err, r.resp.Error)
		case <-time.After(300 * time.Millisecond):
		}
		if n := segReads.Load(); n != 1 {
			t.Fatalf("second call read segments while the budget was taken (%d reads)", n)
		}
		close(release)
		for _, ch := range []chan asyncResult{first, second} {
			if r := <-ch; r.err != nil || r.resp.Error != nil {
				t.Fatalf("%v %+v", r.err, r.resp.Error)
			}
		}
	}

	// With the budget held past the timeout, the waiter fails as
	// upstream unavailable, never as not found.
	{
		var held atomic.Int32
		entered2, release2 := make(chan struct{}), make(chan struct{})
		gated2 := faultStore{st, func(ctx context.Context, key string, off, n int64) ([]byte, error) {
			if strings.HasSuffix(key, ".seg") && held.Add(1) == 1 {
				close(entered2)
				<-release2
			}
			return st.GetRange(ctx, key, off, n)
		}}
		ts2 := serve(t, gated2, Config{MaxInflightBytes: budget, RequestTimeout: 300 * time.Millisecond}, st)
		holder := callAsync(ts2.url, body)
		<-entered2
		e := mustErr(t, ts2.raw(t, body))
		close(release2)
		<-holder
		if e.Message != "archive: upstream unavailable" {
			t.Fatalf("%+v", e)
		}
	}
}

// TestPanicInCallIsContained: a call that panics is answered as upstream
// unavailable and counted; the other calls of its batch and later requests
// are served.
func TestPanicInCallIsContained(t *testing.T) {
	good := buildArchive(t, nil)
	boom := faultStore{good, func(ctx context.Context, key string, off, n int64) ([]byte, error) {
		if key == segOneKey {
			panic("injected")
		}
		return good.GetRange(ctx, key, off, n)
	}}
	ts := serve(t, boom, Config{}, good)
	two := fixtureParams(t, "blob.GetAll-15300000-two", hTwo)

	body, _ := json.Marshal([]json.RawMessage{
		request(t, "blob.Get", methodParams(t, "blob.Get", hOne, nil)...),
		request(t, "blob.GetAll", two...),
	})
	resp, b := post(t, ts.url, body)
	var rs []rpcResponse
	if err := json.Unmarshal(b, &rs); err != nil || resp.StatusCode != http.StatusOK || len(rs) != 2 {
		t.Fatalf("batch: %s %s", resp.Status, b)
	}
	if rs[0].Error == nil || rs[0].Error.Code != 1 || rs[0].Error.Message != "archive: upstream unavailable" {
		t.Fatalf("panicking call: %s", b)
	}
	if rs[1].Error != nil {
		t.Fatalf("other call: %+v", rs[1].Error)
	}
	// The single-call path recovers too, and the server keeps serving.
	if e := mustErr(t, ts.call(t, "blob.GetProof", methodParams(t, "blob.GetProof", hOne, nil)...)); e.Message != "archive: upstream unavailable" {
		t.Fatalf("%+v", e)
	}
	okResult(t, ts.call(t, "blob.GetAll", two...))
	if n := testutil.ToFloat64(ts.m.Panics); n != 2 {
		t.Fatalf("cba_api_panics_total = %v, want 2", n)
	}
	if n := testutil.ToFloat64(ts.m.VerifyFailures); n != 0 {
		t.Fatalf("cba_api_verify_failures_total = %v, want 0", n)
	}
	// The panicking calls had reserved their bytes; they are returned too.
	if !ts.sem.TryAcquire(ts.cfg.MaxInflightBytes) {
		t.Fatal("in-flight bytes of a panicked call not released")
	}
	ts.sem.Release(ts.cfg.MaxInflightBytes)
}

type asyncResult struct {
	resp rpcResponse
	err  error
}

// callAsync posts body from another goroutine, which must not use t.
func callAsync(url string, body []byte) chan asyncResult {
	ch := make(chan asyncResult, 1)
	go func() {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			ch <- asyncResult{err: err}
			return
		}
		defer resp.Body.Close()
		var r asyncResult
		r.err = json.NewDecoder(resp.Body).Decode(&r.resp)
		ch <- r
	}()
	return ch
}

func TestNewDefaults(t *testing.T) {
	if _, err := New(store.NewFS(t.TempDir()), Config{}, nil, nil); err == nil {
		t.Fatal("New accepted an empty chain id")
	}
	s, err := New(store.NewFS(t.TempDir()), Config{ChainID: "celestia"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Config{ChainID: "celestia", MaxBodyBytes: 1 << 20, MaxBatch: 16, MaxNamespaces: 16,
		RequestTimeout: 30 * time.Second, MaxInflightBytes: 1 << 30, ManifestPoll: 6 * time.Second, TombstonePoll: time.Minute}
	if s.cfg != want {
		t.Fatalf("defaults %+v, want %+v", s.cfg, want)
	}
}
