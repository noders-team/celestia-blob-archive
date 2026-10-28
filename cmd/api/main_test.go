package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

const fixtures = "../../testdata/celestia-node/v0.34.3"

func TestFlagsDefaults(t *testing.T) {
	t.Setenv("CBA_API_ARCHIVE", "https://archive.example")
	t.Setenv("CBA_CHAIN_ID", "celestia")
	c, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		archive: "https://archive.example", chainID: "celestia",
		listen: "127.0.0.1:8471", metricsAddr: "127.0.0.1:9469",
		cacheBytes: 512 << 20, openTTL: 6 * time.Second,
		manifestPoll: 6 * time.Second, tombstonePoll: time.Minute,
		maxBody: 1 << 20, maxBatch: 16, maxNamespaces: 16,
		requestTimeout: 30 * time.Second, maxInflightBytes: 1 << 30,
	}
	if c != want {
		t.Fatalf("defaults\n got %+v\nwant %+v", c, want)
	}

	c, err = parseFlags([]string{"-archive", "https://other.example", "-chain-id", "mocha-5",
		"-cache-bytes", "64KiB", "-max-inflight-bytes", "2GiB", "-max-body", "4096"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.archive != "https://other.example" || c.chainID != "mocha-5" ||
		c.cacheBytes != 64<<10 || c.maxInflightBytes != 2<<30 || c.maxBody != 4096 {
		t.Fatalf("flags over env: %+v", c)
	}

	t.Setenv("CBA_API_ARCHIVE", "")
	if _, err := parseFlags(nil, io.Discard); err == nil {
		t.Fatal("no archive URL accepted")
	}
	t.Setenv("CBA_API_ARCHIVE", "https://archive.example")
	t.Setenv("CBA_CHAIN_ID", "")
	if _, err := parseFlags(nil, io.Discard); err == nil {
		t.Fatal("no chain id accepted")
	}
	t.Setenv("CBA_CHAIN_ID", "celestia")
	for _, bad := range []string{"0", "-1MiB", "12XB", "MiB", "9999999999GiB"} {
		if _, err := parseFlags([]string{"-cache-bytes", bad}, io.Discard); err == nil {
			t.Errorf("-cache-bytes %q accepted", bad)
		}
	}
	if _, err := parseFlags([]string{"extra"}, io.Discard); err == nil {
		t.Error("positional argument accepted")
	}
}

func TestUsageAndExitCodes(t *testing.T) {
	t.Setenv("CBA_API_ARCHIVE", "https://archive.example")
	t.Setenv("CBA_CHAIN_ID", "celestia")
	cases := []struct {
		args      []string
		wantCode  int
		wantUsage bool
	}{
		{[]string{"-h"}, 0, true},
		{[]string{"-no-such-flag"}, 2, true},
		{[]string{"-cache-bytes", "12XB"}, 2, true},
		{[]string{"extra"}, 2, true},
	}
	for _, c := range cases {
		var out bytes.Buffer
		_, err := parseFlags(c.args, &out)
		if err == nil {
			t.Fatalf("%v: accepted", c.args)
		}
		if got := exitCode(err); got != c.wantCode {
			t.Errorf("%v: exit %d, want %d (err %v)", c.args, got, c.wantCode, err)
		}
		if got := strings.Contains(out.String(), "-max-inflight-bytes"); got != c.wantUsage {
			t.Errorf("%v: usage printed %v, want %v: %q", c.args, got, c.wantUsage, out.String())
		}
	}
	// A start error (here a missing chain id) is status 1.
	t.Setenv("CBA_CHAIN_ID", "")
	if _, err := parseFlags(nil, io.Discard); exitCode(err) != 1 {
		t.Errorf("missing chain id: exit %d, want 1", exitCode(err))
	}
	if exitCode(nil) != 0 {
		t.Error("exitCode(nil) != 0")
	}
}

func TestAPIEndToEnd(t *testing.T) {
	root := t.TempDir()
	blocks := archivetest.Blocks(t, 3, map[int64]string{2: "mainnet-1500004"})
	archivetest.Build(t, store.NewFS(root), blocks, 1, 2, 2, nil)
	upstream := httptest.NewServer(http.FileServer(http.Dir(root)))
	defer upstream.Close()

	type addrs struct{ api, metrics string }
	ready := make(chan addrs, 1)
	done := make(chan error, 1)
	go func() {
		done <- run([]string{"-archive", upstream.URL, "-chain-id", archivetest.ChainID,
			"-listen", "127.0.0.1:0", "-metrics-addr", "127.0.0.1:0"},
			func(api, metrics string) { ready <- addrs{api, metrics} })
	}()
	var a addrs
	select {
	case a = <-ready:
	case err := <-done:
		t.Fatalf("run returned before serving: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("run did not start")
	}

	// blob.Get of the fixture blob, archived at height 2.
	req := readFixture(t, "req-blob.Get-1500004.json")
	req = bytes.Replace(req, []byte("[1500004,"), []byte("[2,"), 1)
	resp, err := http.Post("http://"+a.api+"/", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatal(err)
	}
	var got, want struct {
		Result json.RawMessage `json:"result"`
		Error  any             `json:"error"`
	}
	decode(t, resp, &got)
	if err := json.Unmarshal(readFixture(t, "resp-blob.Get-1500004.json"), &want); err != nil {
		t.Fatal(err)
	}
	if got.Error != nil || !jsonEqual(t, got.Result, want.Result) {
		t.Fatalf("blob.Get: error %v, result equal to the fixture: %v", got.Error, jsonEqual(t, got.Result, want.Result))
	}

	resp, err = http.Get("http://" + a.api + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health: %s", resp.Status)
	}
	var h struct {
		ChainID       string `json:"chain_id"`
		HighestHeight int64  `json:"highest_height"`
	}
	decode(t, resp, &h)
	if h.ChainID != archivetest.ChainID || h.HighestHeight != 2 {
		t.Fatalf("/health: %+v", h)
	}

	resp, err = http.Get("http://" + a.metrics + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, m := range []string{
		`cba_api_requests_total{method="blob.Get",outcome="ok"} 1`,
		"cba_api_upstream_requests_total",
		"cba_api_cache_bytes",
		"cba_api_archive_highest_height 2",
		"go_goroutines",
	} {
		if !strings.Contains(string(body), m) {
			t.Errorf("/metrics lacks %q", m)
		}
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run after SIGTERM: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop on SIGTERM")
	}
	if _, err := http.Get("http://" + a.api + "/health"); err == nil {
		t.Error("API still serving after SIGTERM")
	}
}

func TestRunRejectsWrongChain(t *testing.T) {
	root := t.TempDir()
	blocks := archivetest.Blocks(t, 2, nil)
	archivetest.Build(t, store.NewFS(root), blocks, 1, 1, 1, nil)
	upstream := httptest.NewServer(http.FileServer(http.Dir(root)))
	defer upstream.Close()
	err := run([]string{"-archive", upstream.URL, "-chain-id", "celestia",
		"-listen", "127.0.0.1:0", "-metrics-addr", ""}, func(string, string) { t.Error("served") })
	if err == nil || !strings.Contains(err.Error(), "chain_id") {
		t.Fatalf("run against another chain: %v", err)
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(fixtures + "/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return bytes.Equal(xa, ya)
}
