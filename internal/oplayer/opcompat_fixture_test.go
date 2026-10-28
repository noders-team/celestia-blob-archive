package oplayer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

var updateOpcompat = flag.Bool("update-opcompat", false, "regenerate testdata/opcompat")

// The share-version-0 namespaces of the opcompat fixture: five blobs of
// opcompatNS1 over two heights, one two-share blob of opcompatNS2.
const (
	opcompatNS1 = "0000000000000000000000000000000000000048ebd3411d6431afa0c5"
	opcompatNS2 = "000000000000000000000000000000000000000a312b9016d7ca3339b6"
)

// opcompatVector is one entry of testdata/opcompat/vectors.json.
type opcompatVector struct {
	Namespace  string `json:"namespace"`
	Commitment string `json:"commitment"`
	DataSHA256 string `json:"data_sha256"`
}

func opcompatDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "opcompat")
}

// buildOpcompat writes the OP objects of the fixture blobs to dir with the
// Writer and returns the vectors.json bytes. The blocks keep their real
// mainnet heights, so the IDs are the ones op-alt-da posts for them.
func buildOpcompat(t *testing.T, dir string) []byte {
	t.Helper()
	ctx := context.Background()
	w := oplayer.NewWriter(store.NewFS(dir), [][29]byte{ns(t, opcompatNS1), ns(t, opcompatNS2)})
	w.SetTombstones(tombSet(t))
	vectors := []opcompatVector{}
	for _, f := range []struct {
		name   string
		height uint64
	}{{"mainnet-10000000", 10_000_000}, {"mainnet-15300000", 15_300_000}} {
		blocks := archivetest.Blocks(t, 2, map[int64]string{1: f.name})
		_, blobs, err := process.Process(blocks[1], blocks[2])
		if err != nil {
			t.Fatal(err)
		}
		var in []*archivev1.BlobRecord
		for _, b := range blobs {
			if !w.Configured(b.Namespace) {
				continue
			}
			if b.ShareVersion != 0 {
				t.Fatalf("%s: blob of %x is share version %d", f.name, b.Namespace, b.ShareVersion)
			}
			b.Height = f.height
			in = append(in, b)
			ob, err := oplayer.FromRecord(b)
			if err != nil {
				t.Fatal(err)
			}
			id, err := ob.ID()
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(b.Data)
			vectors = append(vectors, opcompatVector{
				Namespace:  hex.EncodeToString(b.Namespace),
				Commitment: hex.EncodeToString(oplayer.Commitment(id.Compact())),
				DataSHA256: hex.EncodeToString(sum[:]),
			})
		}
		if n, skipped, err := w.WriteHeight(ctx, in); err != nil || n != len(in) || skipped != (oplayer.Skipped{}) {
			t.Fatalf("%s: WriteHeight = %d, %+v, %v; want %d, none, nil", f.name, n, skipped, err, len(in))
		}
	}
	if len(vectors) != 6 {
		t.Fatalf("fixture has %d blobs, want 6", len(vectors))
	}
	raw, err := json.MarshalIndent(vectors, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

// readTree returns every file under dir by slash path relative to dir.
func readTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
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
		out[filepath.ToSlash(rel)] = b
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestOpcompatFixture builds testdata/opcompat, which tools/opcompat serves
// to op-alt-da's own S3 fallback provider: the op/ tree the Writer writes
// and vectors.json listing each blob's compact commitment. With -update-opcompat it
// rewrites the fixture; otherwise the committed fixture must match.
func TestOpcompatFixture(t *testing.T) {
	tmp := t.TempDir()
	vectors := buildOpcompat(t, tmp)
	if err := os.WriteFile(filepath.Join(tmp, "vectors.json"), vectors, 0o644); err != nil {
		t.Fatal(err)
	}
	dir := opcompatDir()
	if *updateOpcompat {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.CopyFS(dir, os.DirFS(tmp)); err != nil {
			t.Fatal(err)
		}
	}
	want, got := readTree(t, tmp), readTree(t, dir)
	for k, b := range want {
		if !bytes.Equal(got[k], b) {
			t.Errorf("testdata/opcompat/%s differs from the Writer's output (regenerate with -update-opcompat)", k)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("testdata/opcompat/%s is not written by the Writer", k)
		}
	}
}
