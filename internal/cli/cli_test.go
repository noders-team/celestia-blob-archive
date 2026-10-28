package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/nsindex"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

const (
	fixtureNS = "AAAAAAAAAAAAAAAAAAAAAAAAAGNsYjVlZjdiYTg="
	blobAt    = 3
)

type env struct {
	dir     string
	url     string
	st      store.Store
	genuine []*archivev1.BlobRecord // blobs of fixtureNS at blobAt
	ns      []byte
}

func newEnv(t *testing.T) *env {
	t.Helper()
	blocks := archivetest.Blocks(t, 10, map[int64]string{blobAt: "mocha-1834013"})
	dir := t.TempDir()
	fsst := store.NewFS(dir)
	archivetest.Build(t, fsst, blocks, 1, 5, 2, nil)
	if err := nsindex.Update(context.Background(), fsst); err != nil {
		t.Fatal(err)
	}
	ns, _ := base64.StdEncoding.DecodeString(fixtureNS)
	_, all, err := process.Process(blocks[blobAt], blocks[blobAt+1])
	if err != nil {
		t.Fatal(err)
	}
	var g []*archivev1.BlobRecord
	for _, b := range all {
		if bytes.Equal(b.Namespace, ns) {
			g = append(g, b)
		}
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(srv.Close)
	return &env{dir: dir, url: srv.URL, st: client.New(srv.URL), genuine: g, ns: ns}
}

func readLines(t *testing.T, path string) []IndexLine {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []IndexLine
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var x IndexLine
		if err := json.Unmarshal([]byte(l), &x); err != nil {
			t.Fatal(err)
		}
		out = append(out, x)
	}
	return out
}

func TestExportFixtureNamespace(t *testing.T) {
	e := newEnv(t)
	if len(e.genuine) != 2 {
		t.Fatalf("fixture has %d blobs in namespace, want 2", len(e.genuine))
	}
	out := filepath.Join(t.TempDir(), "out")
	n, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: out})
	if err != nil || n != 2 {
		t.Fatalf("Export: %d %v", n, err)
	}
	ents, _ := os.ReadDir(out)
	if len(ents) != 3 { // 2 blobs + index.jsonl
		t.Fatalf("%d files in out", len(ents))
	}
	lines := readLines(t, filepath.Join(out, "index.jsonl"))
	if len(lines) != 2 {
		t.Fatalf("%d index lines", len(lines))
	}
	for i, b := range e.genuine {
		name := "3-" + hex.EncodeToString(b.Commitment) + ".bin"
		got, err := os.ReadFile(filepath.Join(out, name))
		if err != nil || !bytes.Equal(got, b.Data) {
			t.Fatalf("blob %d file %s: err=%v equal=%v", i, name, err, bytes.Equal(got, b.Data))
		}
		l := lines[i]
		if l.File != name || l.Height != 3 || l.Namespace != fixtureNS ||
			l.Commitment != base64.StdEncoding.EncodeToString(b.Commitment) || l.ShareVersion != b.ShareVersion ||
			l.Signer != base64.StdEncoding.EncodeToString(b.Signer) {
			t.Fatalf("index line %d: %+v", i, l)
		}
	}
	// A range without the blobs exports nothing.
	n, err = Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 4, To: 5, Out: t.TempDir()})
	if err != nil || n != 0 {
		t.Fatalf("empty range: %d %v", n, err)
	}
}

func TestExportSkipsTombstones(t *testing.T) {
	e := newEnv(t)
	writeTombstones(t, e.dir, tombstone.Entry{
		Height: blobAt, Namespace: hex.EncodeToString(e.ns), Commitment: hex.EncodeToString(e.genuine[0].Commitment),
		Date: "2026-10-26", Reason: "test",
	})
	out := t.TempDir()
	n, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: out})
	if err != nil || n != 1 {
		t.Fatalf("Export: %d %v", n, err)
	}
	lines := readLines(t, filepath.Join(out, "index.jsonl"))
	if len(lines) != 1 || !strings.Contains(lines[0].File, hex.EncodeToString(e.genuine[1].Commitment)) {
		t.Fatalf("lines: %+v", lines)
	}
	if _, err := os.Stat(filepath.Join(out, "3-"+hex.EncodeToString(e.genuine[0].Commitment)+".bin")); err == nil {
		t.Fatal("tombstoned blob exported")
	}
}

// withdraw scrubs blob b out of the segment holding height blobAt.
func withdraw(t *testing.T, e *env, b *archivev1.BlobRecord) {
	t.Helper()
	if n := archivetest.Withdraw(t, store.NewFS(e.dir), archive.SegmentKey(3, 4), func(r *archivev1.BlobRecord) bool {
		return bytes.Equal(r.Commitment, b.Commitment)
	}); n != 1 {
		t.Fatalf("withdrew %d records, want 1", n)
	}
}

func tombstoneOf(e *env, b *archivev1.BlobRecord) tombstone.Entry {
	return tombstone.Entry{
		Height: blobAt, Namespace: hex.EncodeToString(e.ns), Commitment: hex.EncodeToString(b.Commitment),
		Date: "2026-10-26", Reason: "test",
	}
}

// A blob scrubbed and listed after Export loaded tombstones.json reaches the
// read as a tombstone record; one reload finds it listed and it is skipped.
// (A blob listed before the load is skipped without a read, see
// TestExportSkipsTombstones.)
func TestExportSkipsScrubbedBlob(t *testing.T) {
	e := newEnv(t)
	writeTombstones(t, e.dir, tombstoneOf(e, e.genuine[0]))
	withdraw(t, e, e.genuine[0])
	st := &staleTombstones{Store: e.st}
	out := t.TempDir()
	n, err := Export(context.Background(), st, ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: out})
	if err != nil || n != 1 {
		t.Fatalf("Export: %d %v", n, err)
	}
	if st.gets != 2 {
		t.Fatalf("tombstones.json read %d times, want 2 (one reload)", st.gets)
	}
	lines := readLines(t, filepath.Join(out, "index.jsonl"))
	if len(lines) != 1 || !strings.Contains(lines[0].File, hex.EncodeToString(e.genuine[1].Commitment)) {
		t.Fatalf("lines: %+v", lines)
	}
	if _, err := os.Stat(filepath.Join(out, "3-"+hex.EncodeToString(e.genuine[0].Commitment)+".bin")); err == nil {
		t.Fatal("withdrawn blob exported")
	}
}

func TestExportErrorsOnUnlistedTombstone(t *testing.T) {
	e := newEnv(t)
	withdraw(t, e, e.genuine[0])
	_, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "archive holds an unlisted tombstone") || !strings.Contains(err.Error(), "run the export again") {
		t.Fatalf("Export = %v, want an unlisted tombstone error", err)
	}
}

// staleTombstones serves no tombstones.json on the first Get, as if the
// takedown landed after Export loaded the file.
type staleTombstones struct {
	store.Store
	gets int
}

func (s *staleTombstones) Get(ctx context.Context, key string) ([]byte, error) {
	if key == tombstone.Key {
		if s.gets++; s.gets == 1 {
			return nil, store.ErrNotFound
		}
	}
	return s.Store.Get(ctx, key)
}

func TestExitStatusWithdrawn(t *testing.T) {
	code, msg := ExitStatus(fmt.Errorf("height 3, namespace 00, commitment 01: %w", verify.ErrWithdrawn))
	if code != ExitVerifyFailed || msg != "verification FAILED: height 3, namespace 00, commitment 01: blob withdrawn, see tombstones.json" {
		t.Fatalf("ExitStatus = %d %q", code, msg)
	}
}

func writeTombstones(t *testing.T, dir string, ts ...tombstone.Entry) {
	t.Helper()
	b, _ := json.Marshal(tombstone.File{Tombstones: ts})
	if err := os.WriteFile(filepath.Join(dir, tombstone.Key), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// Validation and hex case are tested in internal/tombstone; this only checks
// that Export fails on a malformed file instead of exporting everything.
func TestExportRejectsMalformedTombstones(t *testing.T) {
	e := newEnv(t)
	writeTombstones(t, e.dir, tombstone.Entry{Height: 0, Namespace: hex.EncodeToString(e.ns), Commitment: strings.Repeat("ab", 32)})
	if _, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "tombstone") {
		t.Fatalf("Export = %v, want a tombstone error", err)
	}
}

func TestExportRefusesOverwriteWithoutForce(t *testing.T) {
	e := newEnv(t)
	out := t.TempDir()
	opts := ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: out}
	if _, err := Export(context.Background(), e.st, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := Export(context.Background(), e.st, opts); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("second export: %v", err)
	}
	// An existing blob file (but no index) is also refused and left intact.
	out2 := t.TempDir()
	name := filepath.Join(out2, "3-"+hex.EncodeToString(e.genuine[0].Commitment)+".bin")
	if err := os.WriteFile(name, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts.Out = out2
	if _, err := Export(context.Background(), e.st, opts); err == nil {
		t.Fatal("overwrote existing blob file")
	}
	if b, _ := os.ReadFile(name); string(b) != "precious" {
		t.Fatalf("file changed: %q", b)
	}
	opts.Force = true
	if n, err := Export(context.Background(), e.st, opts); err != nil || n != 2 {
		t.Fatalf("forced export: %d %v", n, err)
	}
	if b, _ := os.ReadFile(name); !bytes.Equal(b, e.genuine[0].Data) {
		t.Fatal("force did not overwrite")
	}
}

func TestExportUnknownNamespace(t *testing.T) {
	e := newEnv(t)
	ns := bytes.Repeat([]byte{1}, 29)
	_, err := Export(context.Background(), e.st, ExportOptions{Namespace: ns, From: 1, To: 5, Out: t.TempDir()})
	if err == nil {
		t.Fatal("unknown namespace accepted")
	}
	if !strings.Contains(err.Error(), "up to 5") {
		t.Fatalf("error does not name the indexed height: %v", err)
	}
}

func TestExportRefusesHeightsAboveIndex(t *testing.T) {
	e := newEnv(t) // archive and namespace index cover 1-5
	out := filepath.Join(t.TempDir(), "out")
	_, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 6, Out: out})
	if err == nil || !strings.Contains(err.Error(), "up to 5") || !strings.Contains(err.Error(), "--to 6") {
		t.Fatalf("Export = %v, want an index-coverage error", err)
	}
	if code, _ := ExitStatus(err); code != ExitError {
		t.Fatalf("exit status %d, want %d", code, ExitError)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("out dir created: %v", err)
	}
	// --to equal to the indexed height is fine.
	if n, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 5, Out: out}); err != nil || n != 2 {
		t.Fatalf("Export to indexed height: %d %v", n, err)
	}
}

func TestExportWithoutIndexProgress(t *testing.T) {
	e := newEnv(t)
	if err := os.Remove(filepath.Join(e.dir, nsindex.ProgressKey)); err != nil {
		t.Fatal(err)
	}
	_, err := Export(context.Background(), e.st, ExportOptions{Namespace: e.ns, From: 1, To: 1, Out: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "up to 0") {
		t.Fatalf("Export = %v, want an index-coverage error", err)
	}
}

type fakeHead int64

func (f fakeHead) Head(context.Context) (int64, error) { return int64(f), nil }

func TestInspectDetectsGaps(t *testing.T) {
	blocks := archivetest.Blocks(t, 10, nil)
	dir := t.TempDir()
	st := store.NewFS(dir)
	archivetest.Build(t, st, blocks, 1, 4, 2, nil)
	archivetest.Build(t, st, blocks, 7, 8, 2, nil)
	var buf bytes.Buffer
	if err := Inspect(context.Background(), st, fakeHead(100), &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"chain_id:           " + archivetest.ChainID,
		"format_version:     2",
		"highest_height:     8",
		"contiguous_until:   4",
		"segments:           3",
		"committed ranges:   1-4, 7-8",
		"gaps:               5-6",
		"92 blocks behind highest",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestRanges(t *testing.T) {
	committed, gaps := archiveread.Ranges(nil)
	if len(committed) != 0 || len(gaps) != 0 {
		t.Fatal("empty")
	}
}

func TestRootExportAndInspectCommands(t *testing.T) {
	e := newEnv(t)
	out := filepath.Join(t.TempDir(), "o")
	root := NewRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{"export", "--archive", e.url, "--namespace", fixtureNS, "--from", "1", "--to", "5", "--out", out})
	if err := root.Execute(); err != nil || !strings.Contains(buf.String(), "exported 2 blobs") {
		t.Fatalf("export: %v %q", err, buf.String())
	}
	root = NewRoot()
	buf.Reset()
	root.SetOut(&buf)
	root.SetArgs([]string{"inspect", "--archive", e.url})
	if err := root.Execute(); err != nil || !strings.Contains(buf.String(), "committed ranges:   1-5") {
		t.Fatalf("inspect: %v %q", err, buf.String())
	}
}
