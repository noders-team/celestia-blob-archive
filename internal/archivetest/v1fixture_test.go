package archivetest

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

var update = flag.Bool("update", false, "regenerate testdata/archive-v1 (only valid with format v1 code)")

func testdataDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata")
}

// fixtureSums lists "<sha256 hex>  <path>" for every file under dir, sorted by
// path (the sha256sum format).
func fixtureSums(tb testing.TB, dir string) string {
	tb.Helper()
	var sb strings.Builder
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		fmt.Fprintf(&sb, "%x  %s\n", sha256.Sum256(b), filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return sb.String()
}

// TestV1Fixture freezes a format v1 archive built from the mocha fixtures:
// two blobs of one namespace (1834013), a real block without blobs (1834076)
// and a square with a PayForFibre system blob (2035911). The migration test
// reads it as is, so its bytes are pinned in testdata/archive-v1.sha256 and it
// must never be regenerated once the format has changed.
func TestV1Fixture(t *testing.T) {
	dir := filepath.Join(testdataDir(), "archive-v1")
	sumsFile := filepath.Join(testdataDir(), "archive-v1.sha256")
	if *update {
		if archive.FormatVersion != 1 {
			t.Fatalf("refusing -update: this code writes format_version %d, the fixture must be format 1", archive.FormatVersion)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		blocks := Blocks(t, 10, map[int64]string{
			2: "mocha-1834013",
			5: "mocha-1834076",
			8: "mocha-2035911-fibre",
		})
		Build(t, store.NewFS(dir), blocks, 1, 9, 3, nil)
		if err := os.WriteFile(sumsFile, []byte(fixtureSums(t, dir)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(sumsFile)
	if err != nil {
		t.Fatalf("v1 fixture pin missing (generate with -update on format v1 code): %v", err)
	}
	if got := fixtureSums(t, dir); got != string(want) {
		t.Fatalf("v1 fixture bytes differ from %s; it is frozen, restore it from git\ngot:\n%s", sumsFile, got)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		FormatVersion int   `json:"format_version"`
		HighestHeight int64 `json:"highest_height"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.FormatVersion != 1 || m.HighestHeight != 9 {
		t.Fatalf("v1 fixture manifest: format_version %d, highest_height %d", m.FormatVersion, m.HighestHeight)
	}
}
