package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/fibrecapture"
	"github.com/noders-team/celestia-blob-archive/internal/ingest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

func TestExitCode(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{nil, 0},
		{errors.New("boom"), 1},
		{context.DeadlineExceeded, 1},
		{fmt.Errorf("ingest: height 90: %w", process.ErrDataRootMismatch), exitStop},
		{fmt.Errorf("ingest: commit: %w", archive.ErrOverlap), exitStop},
		{fmt.Errorf("ingest: %w: height 7: %w", ingest.ErrFatalBlock, errors.New("bad square")), exitStop},
		{fmt.Errorf("ingest: commit x: %w", store.ErrNotFound), 1},
		{fmt.Errorf("ingest: put segments/x.seg: %w", store.ErrExists), exitStop},
	}
	for _, c := range cases {
		if got := exitCode(c.err); got != c.want {
			t.Errorf("exitCode(%v) = %d, want %d", c.err, got, c.want)
		}
	}
	if exitStop != 3 {
		t.Fatalf("exitStop %d must match RestartPreventExitStatus=3 in the unit", exitStop)
	}
}

func TestTombstoneAdd(t *testing.T) {
	ctx := context.Background()
	blocks := archivetest.Blocks(t, 6, map[int64]string{3: "mocha-1834013"})
	_, blobs, err := process.Process(blocks[3], blocks[4])
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	st := store.NewFS(root)
	archivetest.Build(t, st, blocks, 1, 5, 2, nil)
	// An orphan covering height 3 is affected too.
	orphan := archive.SegmentKey(3, 3)
	if err := st.Put(ctx, orphan, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	segKey := archive.SegmentKey(3, 4)
	seg, err := st.Get(ctx, segKey)
	if err != nil {
		t.Fatal(err)
	}
	b := blobs[0]
	args := []string{"add", "-fs-root", root, "-public-url", "https://example.test/",
		"-height", "3", "-namespace", strings.ToUpper(hex.EncodeToString(b.Namespace)),
		"-commitment", hex.EncodeToString(b.Commitment), "-reason", "court order", "-date", "2026-10-26"}
	var out bytes.Buffer
	if err := runTombstone(args, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"https://example.test/" + tombstone.Key,
		"https://example.test/" + segKey,
		"https://example.test/" + orphan,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %s:\n%s", want, out.String())
		}
	}
	set, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if !set.Has(3, b.Namespace, b.Commitment) || set.Len() != 1 {
		t.Fatalf("tombstones.json does not list the blob (len %d)", set.Len())
	}
	if got, _ := st.Get(ctx, segKey); !bytes.Equal(got, seg) {
		t.Fatal("tombstone add touched the segment")
	}

	// A height that is not archived yet is fine: the ingester scrubs it on write.
	out.Reset()
	args = []string{"add", "-fs-root", root, "-height", "90", "-namespace", hex.EncodeToString(b.Namespace),
		"-commitment", hex.EncodeToString(b.Commitment), "-reason", "court order"}
	if err := runTombstone(args, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "not archived yet") {
		t.Errorf("output for an unarchived height:\n%s", out.String())
	}
	if err := runTombstone(args, &out); !errors.Is(err, tombstone.ErrDuplicate) {
		t.Fatalf("duplicate add: %v", err)
	}
	for _, bad := range [][]string{
		nil,
		{"remove"},
		{"add", "-fs-root", root, "-height", "3"},
		{"add", "-fs-root", root, "extra"},
	} {
		if err := runTombstone(bad, &out); err == nil {
			t.Errorf("runTombstone(%q) succeeded", bad)
		}
	}
}

func TestTombstoneAddFibreDeletesData(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st := store.NewFS(root)
	var id [fibrearchive.BlobIDSize]byte
	for i := range id {
		id[i] = byte(i + 1)
	}
	dataKey := fibrearchive.DataKey(id)
	if err := st.Put(ctx, dataKey, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	args := []string{"add-fibre", "-fs-root", root, "-public-url", "https://example.test/",
		"-blob-id", strings.ToUpper(hex.EncodeToString(id[:])), "-reason", "court order", "-date", "2026-10-27",
		"-height", "1497140", "-namespace", strings.Repeat("00", 28) + "01"}
	var out bytes.Buffer
	if err := runTombstone(args, &out); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, dataKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("%s still present: %v", dataKey, err)
	}
	set, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if !set.HasFibre(id[:]) || set.FibreLen() != 1 || set.Len() != 0 {
		t.Fatalf("tombstones.json: HasFibre %v FibreLen %d Len %d", set.HasFibre(id[:]), set.FibreLen(), set.Len())
	}
	for _, want := range []string{
		"https://example.test/" + tombstone.Key,
		"https://example.test/" + dataKey,
		"the running fibre-capture also deletes it within 1m and never writes it again",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %s:\n%s", want, out.String())
		}
	}

	// A rerun of the same takedown is not an error: the entry is kept and
	// the delete is attempted again.
	out.Reset()
	if err := runTombstone(args, &out); err != nil {
		t.Fatalf("duplicate add-fibre: %v", err)
	}
	if !strings.Contains(out.String(), "already") {
		t.Errorf("duplicate output:\n%s", out.String())
	}
	for _, bad := range [][]string{
		{"add-fibre", "-fs-root", root, "-reason", "x"},
		{"add-fibre", "-fs-root", root, "-blob-id", "abcd", "-reason", "x"},
		{"add-fibre", "-fs-root", root, "-blob-id", hex.EncodeToString(id[:]), "-reason", "x", "extra"},
		{"add-fibre", "-fs-root", root, "-blob-id", hex.EncodeToString(id[:]), "-reason", "x", "-height", "5"},
	} {
		if err := runTombstone(bad, &out); err == nil {
			t.Errorf("runTombstone(%q) succeeded", bad)
		}
	}
}

// deleteFails is a store whose Delete always fails.
type deleteFails struct{ store.Store }

func (deleteFails) Delete(context.Context, string) error { return errors.New("delete refused") }

// deadlineCloser records how long Close was given.
type deadlineCloser struct{ budget time.Duration }

func (c *deadlineCloser) Close(ctx context.Context) error {
	d, ok := ctx.Deadline()
	if !ok {
		return errors.New("no deadline")
	}
	c.budget = time.Until(d)
	return nil
}

// TestCloseDownloaderBounded: the fibre client gets at most 10 s to stop, so
// the whole stop path fits TimeoutStopSec=60.
func TestCloseDownloaderBounded(t *testing.T) {
	var c deadlineCloser
	closeDownloader(&c)
	if c.budget <= 0 || c.budget > 10*time.Second {
		t.Fatalf("Close given %v, want ≤ 10s", c.budget)
	}
}

// TestTombstoneAddFibreRerunRetriesDelete: after a failed delete, a rerun
// finds the entry saved (ErrDuplicate) and still deletes the payload.
func TestTombstoneAddFibreRerunRetriesDelete(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	var id [fibrearchive.BlobIDSize]byte
	id[0] = 7
	dataKey := fibrearchive.DataKey(id)
	if err := fs.Put(ctx, dataKey, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	e := tombstone.FibreEntry{BlobID: hex.EncodeToString(id[:]), Date: "2026-10-27", Reason: "x"}
	var out bytes.Buffer
	if err := addFibre(ctx, deleteFails{fs}, publicLink("", "fs", ""), e, &out); err == nil {
		t.Fatal("first run: delete failure not reported")
	}

	out.Reset()
	if err := addFibre(ctx, fs, publicLink("", "fs", ""), e, &out); err != nil {
		t.Fatalf("rerun = %v, output:\n%s", err, out.String())
	}
	if _, err := fs.Get(ctx, dataKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("%s still present after the rerun: %v", dataKey, err)
	}
	if !strings.Contains(out.String(), "already") || !strings.Contains(out.String(), "deleted "+dataKey) {
		t.Errorf("rerun output:\n%s", out.String())
	}
	if set, err := tombstone.Load(ctx, fs); err != nil || set.FibreLen() != 1 {
		t.Fatalf("tombstones.json: %v", err)
	}

	// A rerun whose delete fails again still fails.
	if err := addFibre(ctx, deleteFails{fs}, publicLink("", "fs", ""), e, &out); err == nil {
		t.Error("rerun with a failing delete succeeded")
	}
}

func TestTombstoneAddFibreDeleteErrorAfterSave(t *testing.T) {
	ctx := context.Background()
	st := deleteFails{store.NewFS(t.TempDir())}
	id := strings.Repeat("ab", fibrearchive.BlobIDSize)
	var out bytes.Buffer
	err := addFibre(ctx, st, publicLink("", "fs", ""), tombstone.FibreEntry{BlobID: id, Date: "2026-10-27", Reason: "x"}, &out)
	if err == nil || !strings.Contains(out.String(), "delete refused") {
		t.Fatalf("addFibre = %v, output:\n%s", err, out.String())
	}
	set, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if set.FibreLen() != 1 {
		t.Fatal("the entry was not saved before the failed delete")
	}
}

func TestMigrateCommand(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join("..", "..", "testdata", "archive-v1")
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CBA_CHAIN_ID", "")
	var out bytes.Buffer
	if err := runMigrate([]string{"-fs-root", root, "-chain-id", archivetest.ChainID}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "3 segments, 1 directory buckets") {
		t.Fatalf("output %q", out.String())
	}
	if _, err := archive.NewCommitter(ctx, store.NewFS(root), archivetest.ChainID); err != nil {
		t.Fatalf("committer after migrate-v2: %v", err)
	}
	out.Reset()
	if err := runMigrate([]string{"-fs-root", root, "-chain-id", archivetest.ChainID}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "nothing to migrate") {
		t.Fatalf("second run output %q", out.String())
	}
	for _, bad := range [][]string{
		{"-fs-root", root},
		{"-fs-root", root, "-chain-id", "other"},
		{"-fs-root", root, "-chain-id", archivetest.ChainID, "extra"},
	} {
		if err := runMigrate(bad, &out); err == nil {
			t.Errorf("runMigrate(%q) succeeded", bad)
		}
	}
}

func TestOPBackfillFlags(t *testing.T) {
	ctx := context.Background()
	const v0NS = "00000000000000000000000000000000000000893f79d6e2c81a4d08c4"
	blocks := archivetest.Blocks(t, 4, map[int64]string{2: "mocha-1834013"})
	root := t.TempDir()
	st := store.NewFS(root)
	archivetest.Build(t, st, blocks, 1, 3, 2, nil)
	ns, err := hex.DecodeString(v0NS)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CBA_FS_ROOT", "")
	t.Setenv("CBA_BUCKET", "")

	var out bytes.Buffer
	// Not configured yet: refused before anything is read or written.
	err = runOPBackfill([]string{"-fs-root", root, "-namespace", v0NS}, &out)
	if err == nil || !strings.Contains(err.Error(), "CBA_OP_NAMESPACES") {
		t.Fatalf("unconfigured namespace: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("summary printed for a run that never started:\n%s", out.String())
	}
	if err := oplayer.WriteConfig(ctx, st, [][29]byte{[29]byte(ns)}, time.Now()); err != nil {
		t.Fatal(err)
	}

	out.Reset()
	if err := runOPBackfill([]string{"-fs-root", root, "-namespace", v0NS, "-dry-run", "-workers", "2"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"dry run", "heights 1-3", "written 1", "PUTs = 1", "cost ≈ $0.0000045", "storage = "} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("dry-run output lacks %q:\n%s", want, out.String())
		}
	}
	if keys, _ := st.List(ctx, "op/"+v0NS+"/"); len(keys) != 0 {
		t.Fatalf("dry run wrote %v", keys)
	}

	out.Reset()
	if err := runOPBackfill([]string{"-fs-root", root, "-namespace", v0NS, "-from", "2", "-to", "2"}, &out); err != nil {
		t.Fatal(err)
	}
	if keys, _ := st.List(ctx, "op/"+v0NS+"/"); len(keys) != 1 {
		t.Fatalf("op objects %v, want 1", keys)
	}
	if !strings.Contains(out.String(), "heights 2-2") || strings.Contains(out.String(), "dry run") {
		t.Errorf("output:\n%s", out.String())
	}
	// A rerun writes nothing and estimates no PUTs.
	out.Reset()
	if err := runOPBackfill([]string{"-fs-root", root, "-namespace", v0NS}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"written 0", "already present 1", "PUTs = 0"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("rerun output lacks %q:\n%s", want, out.String())
		}
	}

	for _, bad := range [][]string{
		{"-fs-root", root},
		{"-fs-root", root, "-namespace", "zz"},
		{"-fs-root", root, "-namespace", v0NS + "," + v0NS},
		{"-fs-root", root, "-namespace", v0NS, "-workers", "-1"},
		{"-fs-root", root, "-namespace", v0NS, "-from", "-1"},
		{"-fs-root", root, "-namespace", v0NS, "-from", "3", "-to", "2"},
		{"-fs-root", root, "-namespace", v0NS, "extra"},
		{"-namespace", v0NS}, // no store
	} {
		if err := runOPBackfill(bad, &out); err == nil {
			t.Errorf("runOPBackfill(%q) succeeded", bad)
		}
	}
}

func TestEnvBoolDefault(t *testing.T) {
	const key = "CBA_TEST_ENV_BOOL"
	cases := []struct {
		val     string
		def     bool
		want    bool
		wantErr bool
	}{
		{"", true, true, false},
		{"", false, false, false},
		{"false", true, false, false},
		{"1", false, true, false},
		{"maybe", true, false, true},
	}
	for _, c := range cases {
		t.Setenv(key, c.val)
		got, err := envBool(key, c.def)
		if (err != nil) != c.wantErr || (err == nil && got != c.want) {
			t.Errorf("envBool(%q, %v) = %v, %v; want %v, err=%v", c.val, c.def, got, err, c.want, c.wantErr)
		}
	}
}

func TestFibreCaptureFlags(t *testing.T) {
	for _, k := range []string{"CBA_GRPC", "CBA_GRPC_TLS", "CBA_CHAIN_ID", "CBA_FS_ROOT", "CBA_BUCKET",
		"CBA_CONDITIONAL_PUT", "CBA_FIBRE_STATE_DIR", "STATE_DIRECTORY", "CBA_FIBRE_DAILY_CAP_BYTES"} {
		t.Setenv(k, "")
	}
	base := []string{"-grpc", "127.0.0.1:12690", "-chain-id", "mocha-5"}
	with := func(extra ...string) []string { return slices.Concat(base, extra) }

	if _, err := parseFibreCaptureFlags(base); err == nil || !strings.Contains(err.Error(), "-state-dir") {
		t.Fatalf("no state dir: err %v, want a -state-dir error", err)
	}

	t.Setenv("STATE_DIRECTORY", "/var/lib/cba-fibre-mocha")
	o, err := parseFibreCaptureFlags(base)
	if err != nil {
		t.Fatal(err)
	}
	want := fibreCaptureOpts{
		grpcAddr: "127.0.0.1:12690", chainID: "mocha-5", condPut: true,
		stateDir: "/var/lib/cba-fibre-mocha", dailyCapBytes: 50_000_000_000,
		downloadWorkers: 2, metricsAddr: "127.0.0.1:9470",
	}
	if o != want {
		t.Fatalf("defaults\n got %+v\nwant %+v", o, want)
	}

	t.Setenv("CBA_FIBRE_STATE_DIR", "/srv/fibre")
	t.Setenv("CBA_FIBRE_DAILY_CAP_BYTES", "1000")
	if o, err = parseFibreCaptureFlags(base); err != nil {
		t.Fatal(err)
	}
	if o.stateDir != "/srv/fibre" || o.dailyCapBytes != 1000 {
		t.Fatalf("env: got %+v", o)
	}
	const tlsErr = "-grpc-tls is not supported by fibre-capture: the fibre client dials the app gRPC in plaintext"
	if _, err := parseFibreCaptureFlags(with("-grpc-tls")); err == nil || err.Error() != tlsErr {
		t.Errorf("-grpc-tls: err %v, want %q", err, tlsErr)
	}
	t.Setenv("CBA_GRPC_TLS", "true")
	if _, err := parseFibreCaptureFlags(base); err == nil || err.Error() != tlsErr {
		t.Errorf("CBA_GRPC_TLS=true: err %v, want %q", err, tlsErr)
	}
	t.Setenv("CBA_GRPC_TLS", "false")
	if _, err := parseFibreCaptureFlags(base); err != nil {
		t.Errorf("CBA_GRPC_TLS=false: %v", err)
	}
	o, err = parseFibreCaptureFlags(with("-state-dir", "/x", "-daily-cap-bytes", "7",
		"-download-workers", "3", "-metrics-addr", "127.0.0.1:9471"))
	if err != nil {
		t.Fatal(err)
	}
	if o.stateDir != "/x" || o.dailyCapBytes != 7 || o.downloadWorkers != 3 || o.metricsAddr != "127.0.0.1:9471" {
		t.Fatalf("flags: got %+v", o)
	}

	for _, bad := range [][]string{
		{"-chain-id", "mocha-5"},
		{"-grpc", "127.0.0.1:12690"},
		with("-download-workers", "0"),
		with("-daily-cap-bytes", "0"),
		with("extra"),
	} {
		if _, err := parseFibreCaptureFlags(bad); err == nil {
			t.Errorf("parseFibreCaptureFlags(%q) succeeded", bad)
		}
	}
	t.Setenv("CBA_FIBRE_DAILY_CAP_BYTES", "50GB")
	if _, err := parseFibreCaptureFlags(base); err == nil {
		t.Error("CBA_FIBRE_DAILY_CAP_BYTES=50GB accepted")
	}
}

// headerSource serves one head block whose header is hdr, after failing
// fails Block calls.
func headerSource(t *testing.T, hdr []byte, fails int) *source.Fake {
	t.Helper()
	src := source.NewFake(map[int64]*source.Block{5: {Height: 5, HeaderProto: hdr}}, 5)
	if fails > 0 {
		src.FailNext(5, fails)
	}
	return src
}

func TestCheckHeadChainID(t *testing.T) {
	ctx := context.Background()
	hdr := cmtproto.Header{ChainID: "mocha-5", Height: 5}
	good, err := hdr.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkHeadChainID(ctx, headerSource(t, good, 0), "mocha-5"); err != nil {
		t.Fatalf("matching chain: %v", err)
	}
	if err := checkHeadChainID(ctx, headerSource(t, good, 0), "celestia"); !errors.Is(err, fibrecapture.ErrChainMismatch) {
		t.Fatalf("other chain: err %v, want ErrChainMismatch", err)
	}
	// A transport error is retried (one 1s backoff).
	if err := checkHeadChainID(ctx, headerSource(t, good, 1), "mocha-5"); err != nil {
		t.Fatalf("after a transport error: %v", err)
	}
	// An undecodable header is returned at once, not retried.
	tctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = checkHeadChainID(tctx, headerSource(t, []byte{0xff, 0xff, 0xff}, 0), "mocha-5")
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, fibrecapture.ErrChainMismatch) {
		t.Fatalf("bad header: err %v, want a decode error at once", err)
	}
	if got := exitCode(err); got != 1 {
		t.Errorf("bad header exit code %d, want 1", got)
	}
}

// TestFibreCaptureServesMetricsBeforeNode: with the app node unreachable,
// /metrics already serves cba_fibre_* while the fibre client retries, and
// SIGINT then ends the command with nil.
func TestFibreCaptureServesMetricsBeforeNode(t *testing.T) {
	for _, k := range []string{"CBA_GRPC_TLS", "CBA_CONDITIONAL_PUT", "CBA_FIBRE_DAILY_CAP_BYTES", "CBA_BUCKET"} {
		t.Setenv(k, "")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr := ln.Addr().String()
	ln.Close()
	args := []string{"-grpc", "127.0.0.1:1", "-chain-id", "mocha-5", "-fs-root", t.TempDir(),
		"-state-dir", t.TempDir(), "-metrics-addr", metricsAddr}
	done := make(chan error, 1)
	go func() { done <- runFibreCapture(args) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("/metrics never served cba_fibre_ metrics")
		}
		select {
		case err := <-done:
			t.Fatalf("runFibreCapture returned early: %v", err)
		default:
		}
		resp, err := http.Get("http://" + metricsAddr + "/metrics")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if strings.Contains(string(body), "cba_fibre_") {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	// signal.NotifyContext in runFibreCapture is registered by now (it is
	// set up before the metrics server), so SIGINT is caught.
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runFibreCapture after SIGINT = %v, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("runFibreCapture did not return after SIGINT")
	}
}

func TestFibreCaptureExitCodes(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("start: %w: state.json has a, want b", fibrecapture.ErrChainMismatch),
		fmt.Errorf("record x: %w", fibrecapture.ErrRecordConflict),
		fmt.Errorf("put fibre/data/x: %w", store.ErrExists),
	} {
		if got := exitCode(err); got != exitStop {
			t.Errorf("exitCode(%v) = %d, want %d", err, got, exitStop)
		}
	}
}
