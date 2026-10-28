package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	gproto "google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/cachestore"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// headerHeight returns header.height of a trimmed ExtendedHeader and checks
// that validator_set and dah are null.
func headerHeight(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var eh struct {
		Header struct {
			Height string `json:"height"`
		} `json:"header"`
		Commit struct {
			Height string `json:"height"`
		} `json:"commit"`
		ValidatorSet json.RawMessage `json:"validator_set"`
		DAH          json.RawMessage `json:"dah"`
	}
	if err := json.Unmarshal(raw, &eh); err != nil {
		t.Fatal(err)
	}
	if string(eh.ValidatorSet) != "null" || string(eh.DAH) != "null" {
		t.Errorf("validator_set %s, dah %s; want null", eh.ValidatorSet, eh.DAH)
	}
	if eh.Header.Height != eh.Commit.Height {
		t.Errorf("header height %s, commit height %s", eh.Header.Height, eh.Commit.Height)
	}
	return eh.Header.Height
}

// Real heights of the block fixtures: their headers and commits are the
// chain's, unlike the synthetic blocks of buildArchive.
const (
	realOne = 1500004
	realTwo = 15300000
)

// buildReal archives the block fixture name at its real height into st.
func buildReal(t *testing.T, st store.Store, name string, h int64, mut archivetest.Mutate) {
	t.Helper()
	cur, next := blockFixture(t, name)
	archivetest.Build(t, st, map[int64]*source.Block{h: cur, h + 1: next}, h, h, 1, mut)
}

// realArchive archives both block fixtures at their real heights.
func realArchive(t *testing.T, mut archivetest.Mutate) store.Store {
	t.Helper()
	st := store.NewFS(t.TempDir())
	buildReal(t, st, fixOne, realOne, mut)
	buildReal(t, st, fixTwo, realTwo, nil)
	return st
}

// serveReal serves the realArchive st with fixed heights (testState).
func serveReal(t *testing.T, st store.Store) *testServer {
	t.Helper()
	ts := serve(t, st, Config{ChainID: archivetest.ChainID})
	ts.state.highest, ts.state.contiguous = realTwo, 0
	return ts
}

func TestNetworkHeadFollowsManifest(t *testing.T) {
	st := store.NewFS(t.TempDir())
	buildReal(t, st, fixOne, realOne, nil)
	ts, clk := start(t, st, Config{})

	head := okResult(t, ts.call(t, "header.NetworkHead"))
	if h := headerHeight(t, head); h != strconv.Itoa(realOne) {
		t.Fatalf("NetworkHead height %s, want %d", h, realOne)
	}
	eq(t, "NetworkHead = GetByHeight(highest)", head, okResult(t, ts.call(t, "header.GetByHeight", realOne)))

	buildReal(t, st, fixTwo, realTwo, nil)
	// Not reloaded yet: still the old head, and the new height is in the future.
	if h := headerHeight(t, okResult(t, ts.call(t, "header.NetworkHead"))); h != strconv.Itoa(realOne) {
		t.Fatalf("NetworkHead before the manifest poll %s, want %d", h, realOne)
	}
	if e := mustErr(t, ts.call(t, "header.GetByHeight", realTwo)); !strings.Contains(e.Message, "given height is from the future") {
		t.Fatalf("new height before the poll: %+v", e)
	}
	clk.Advance(ts.cfg.ManifestPoll)
	eventually(t, "NetworkHead at the new highest height", func() bool {
		r := ts.call(t, "header.NetworkHead")
		return r.Error == nil && headerHeight(t, r.Result) == strconv.Itoa(realTwo)
	})
	if n := testutil.ToFloat64(ts.m.HighestHeight); n != realTwo {
		t.Errorf("cba_api_archive_highest_height = %v, want %d", n, realTwo)
	}
}

func TestHeaderGetByHeight(t *testing.T) {
	ts := serveReal(t, realArchive(t, nil))
	res := okResult(t, ts.call(t, "header.GetByHeight", realOne))
	if got := headerHeight(t, res); got != strconv.Itoa(realOne) {
		t.Errorf("GetByHeight height %s", got)
	}
	var got, want map[string]json.RawMessage
	if err := json.Unmarshal(res, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(loadCall(t, "header.GetByHeight-1500004").Result, &want); err != nil {
		t.Fatal(err)
	}
	eq(t, "header", compact(t, got["header"]), compact(t, want["header"]))
	if h := headerHeight(t, okResult(t, ts.call(t, "header.GetByHeight", realTwo))); h != strconv.Itoa(realTwo) {
		t.Errorf("GetByHeight(%d) height %s", realTwo, h)
	}

	for _, c := range []struct {
		h    any
		code int
		want string
	}{
		{realOne + 1, 1, "archive: height 1500005 not archived (gap in the archive; contiguous up to 0, highest archived 15300000)"},
		{realTwo + 1, 1, "header: given height is from the future"},
		{0, 1, "header: height is equal to 0"},
		{"2", -32602, "invalid params"},
	} {
		e := mustErr(t, ts.call(t, "header.GetByHeight", c.h))
		if e.Code != c.code || !strings.Contains(e.Message, c.want) || strings.Contains(e.Message, "not found") {
			t.Errorf("GetByHeight(%v): %+v, want code %d and %q", c.h, e, c.code, c.want)
		}
	}
	if e := mustErr(t, ts.call(t, "header.NetworkHead", 1)); e.Code != -32602 {
		t.Errorf("NetworkHead with a param: %+v", e)
	}
}

// TestHeaderRecordMismatchFails: a height record whose commit does not sign
// its header is never served.
func TestHeaderRecordMismatchFails(t *testing.T) {
	cur, next := blockFixture(t, fixTwo)
	hrTwo, _, err := process.Process(cur, next)
	if err != nil {
		t.Fatal(err)
	}
	// Block 15300000's commit decodes and encodes fine, so only the check
	// against the header can refuse it.
	mut := func(hr *archivev1.HeightRecord, blobs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		hr = gproto.Clone(hr).(*archivev1.HeightRecord)
		hr.Commit = hrTwo.Commit
		return hr, blobs
	}
	ts := serveReal(t, realArchive(t, mut))
	e := mustErr(t, ts.call(t, "header.GetByHeight", realOne))
	if e.Message != "archive: stored record for height 1500004 failed verification" {
		t.Errorf("mismatched commit: %+v", e)
	}
	if n := testutil.ToFloat64(ts.m.VerifyFailures); n != 1 {
		t.Errorf("verify failures %v, want 1", n)
	}
}

// TestHeaderDataHashMismatchFails: blob proofs validate against the record's
// data_hash, so a header whose DataHash differs is never served.
func TestHeaderDataHashMismatchFails(t *testing.T) {
	mut := func(hr *archivev1.HeightRecord, blobs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		hr = gproto.Clone(hr).(*archivev1.HeightRecord)
		hr.DataHash = bytes.Clone(hr.DataHash)
		hr.DataHash[0] ^= 1
		return hr, blobs
	}
	ts := serveReal(t, realArchive(t, mut))
	e := mustErr(t, ts.call(t, "header.GetByHeight", realOne))
	if e.Message != "archive: stored record for height 1500004 failed verification" {
		t.Errorf("mismatched data hash: %+v", e)
	}
	if n := testutil.ToFloat64(ts.m.VerifyFailures); n != 1 {
		t.Errorf("verify failures %v, want 1", n)
	}
}

func httpRequest(t *testing.T, method, url string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestMetricsOutcomes(t *testing.T) {
	params, ns, cm := getFixture(t)
	st := buildArchive(t, nil)
	addTombstone(t, st, hTwo, ns, cm)
	ts := serve(t, st, Config{})
	wrong := append([]any{}, params...)
	wrong[2] = make([]byte, 32)
	listed := append([]any{}, params...)
	listed[0] = hTwo

	ts.call(t, "blob.Get", params...)
	ts.call(t, "blob.Get", wrong...)
	ts.call(t, "blob.Get", listed...)
	ts.call(t, "blob.Get", hFuture, params[1], params[2])
	ts.call(t, "blob.Get", "x", params[1], params[2])
	ts.call(t, "header.GetByHeight", hGap)
	real := serveReal(t, realArchive(t, nil))
	real.call(t, "header.GetByHeight", realOne)
	ts.call(t, "blob.Subscribe")
	post(t, ts.url, []byte("{"))
	post(t, ts.url, []byte(`{"jsonrpc":"2.0","method":"blob.Get","params":[]}`)) // notification: not counted
	// Refused before JSON-RPC: counted as method="http"; GET /health is not counted.
	for _, req := range []*http.Request{
		httpRequest(t, http.MethodGet, ts.url+"/", nil),
		httpRequest(t, http.MethodPost, ts.url+"/other", nil),
		httpRequest(t, http.MethodPut, ts.url+"/health", nil),
		httpRequest(t, http.MethodPost, ts.url+"/", bytes.Repeat([]byte(" "), int(ts.cfg.MaxBodyBytes)+1)),
	} {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	getHealth(t, ts.url, http.StatusOK)

	broken := serve(t, buildArchive(t, flipData(cm)), Config{})
	broken.call(t, "blob.Get", params...)
	down := serve(t, faultStore{Store: buildArchive(t, nil), getRange: func(context.Context, string, int64, int64) ([]byte, error) {
		return nil, errors.New("injected")
	}}, Config{})
	down.call(t, "blob.Get", params...)

	for _, c := range []struct {
		ts              *testServer
		method, outcome string
		want            float64
	}{
		{ts, "blob.Get", outcomeOK, 1},
		{ts, "blob.Get", outcomeNotFound, 1},
		{ts, "blob.Get", outcomeWithdrawn, 1},
		{ts, "blob.Get", outcomeNotArchived, 1},
		{ts, "blob.Get", outcomeBadRequest, 1},
		{ts, "header.GetByHeight", outcomeNotArchived, 1},
		{real, "header.GetByHeight", outcomeOK, 1},
		{ts, methodOther, outcomeBadRequest, 2},
		{ts, methodHTTP, outcomeBadRequest, 4},
		{broken, "blob.Get", outcomeVerifyFailed, 1},
		{down, "blob.Get", outcomeUpstream, 1},
	} {
		if n := testutil.ToFloat64(c.ts.m.Requests.WithLabelValues(c.method, c.outcome)); n != c.want {
			t.Errorf("cba_api_requests_total{%s,%s} = %v, want %v", c.method, c.outcome, n, c.want)
		}
	}
	if n := testutil.CollectAndCount(ts.m.RequestSeconds); n != 2 {
		t.Errorf("request_seconds series %d, want 2 (blob.Get, header.GetByHeight)", n)
	}

	t.Run("upstream", func(t *testing.T) {
		m := NewMetrics(prometheus.NewRegistry())
		up := m.CountUpstream(faultStore{Store: store.NewFS(t.TempDir()), getRange: func(context.Context, string, int64, int64) ([]byte, error) {
			return nil, errors.New("injected")
		}})
		ctx := context.Background()
		if err := up.Put(ctx, "a", []byte("x")); err != nil {
			t.Fatal(err)
		}
		_, _ = up.Get(ctx, "a")
		_, _ = up.Get(ctx, "missing")
		_, _ = up.GetRange(ctx, "a", 0, 1)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, _ = m.CountUpstream(faultStore{Store: up, getRange: func(ctx context.Context, _ string, _, _ int64) ([]byte, error) {
			return nil, ctx.Err()
		}}).GetRange(cctx, "a", 0, 1)
		for outcome, want := range map[string]float64{"ok": 1, "not_found": 1, "error": 1, "canceled": 1} {
			if n := testutil.ToFloat64(m.Upstream.WithLabelValues(outcome)); n != want {
				t.Errorf("upstream{%s} = %v, want %v", outcome, n, want)
			}
		}
	})

	t.Run("cache", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)
		if n := testutil.CollectAndCount(cacheCollector{m}); n != 0 {
			t.Fatalf("cache series before WatchCache: %d", n)
		}
		m.WatchCache(func() cachestore.Stats {
			return cachestore.Stats{Bytes: 4096,
				Hits: map[string]uint64{cachestore.ClassIdx2: 3}, Misses: map[string]uint64{cachestore.ClassDirFrozen: 2}}
		})
		want := `
# HELP cba_api_cache_bytes Bytes held by the read cache.
# TYPE cba_api_cache_bytes gauge
cba_api_cache_bytes 4096
`
		if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "cba_api_cache_bytes"); err != nil {
			t.Error(err)
		}
		if n := testutil.CollectAndCount(cacheCollector{m}, "cba_api_cache_hits_total"); n != 3 {
			t.Errorf("hit series %d, want 3", n)
		}
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for _, mf := range mfs {
			for _, mt := range mf.GetMetric() {
				if c := mt.GetCounter(); c != nil && strings.HasPrefix(mf.GetName(), "cba_api_cache_") {
					got[mf.GetName()+"/"+mt.GetLabel()[0].GetValue()] = c.GetValue()
				}
			}
		}
		if got["cba_api_cache_hits_total/idx2"] != 3 || got["cba_api_cache_misses_total/dir_frozen"] != 2 || got["cba_api_cache_misses_total/idx2"] != 0 {
			t.Errorf("cache counters %v", got)
		}
	})

	t.Run("nil safe", func(t *testing.T) {
		var nilM *Metrics
		nilM.panicked()
		nilM.request("blob.Get", outcomeOK, 1)
		(&Metrics{}).verifyFailed()
		(&Metrics{}).request("blob.Get", outcomeOK, 1)
	})
}
