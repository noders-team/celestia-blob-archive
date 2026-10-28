package oplayer_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// fixtureSegment returns a segment of heights 1..3 with the mocha-1834013
// blobs at height 2, that height's record and its blobs.
func fixtureSegment(t *testing.T) ([]byte, *archivev1.HeightRecord, []*archivev1.BlobRecord) {
	t.Helper()
	blocks := archivetest.Blocks(t, 4, map[int64]string{2: "mocha-1834013"})
	var w segment.Writer
	var hr2 *archivev1.HeightRecord
	var blobs2 []*archivev1.BlobRecord
	for h := int64(1); h <= 3; h++ {
		hr, blobs, err := process.Process(blocks[h], blocks[h+1])
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.Append(&archivev1.Record{Kind: &archivev1.Record_Height{Height: hr}}); err != nil {
			t.Fatal(err)
		}
		for _, b := range blobs {
			if _, _, err := w.Append(&archivev1.Record{Kind: &archivev1.Record_Blob{Blob: b}}); err != nil {
				t.Fatal(err)
			}
		}
		if h == 2 {
			hr2, blobs2 = hr, blobs
		}
	}
	if len(blobs2) < 2 {
		t.Fatalf("fixture has %d blobs, want at least 2", len(blobs2))
	}
	return w.Bytes(), hr2, blobs2
}

func TestWithdrawnFromScrubbedAndRawSegment(t *testing.T) {
	raw, hr, blobs := fixtureSegment(t)
	set := tombSet(t, blobs[0], blobs[2])
	scrubbed, n, err := tombstone.Scrub(raw, set)
	if err != nil || n != 2 {
		t.Fatalf("Scrub = %d, %v; want 2 records", n, err)
	}

	want := make([]oplayer.Blob, 0, 2)
	for _, b := range []*archivev1.BlobRecord{blobs[0], blobs[2]} {
		ob, err := oplayer.FromRecord(b)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, ob)
	}
	for name, seg := range map[string][]byte{"raw": raw, "scrubbed": scrubbed} {
		got, err := oplayer.Withdrawn(seg, set)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, b := range got {
			if b.SquareSize != hr.SquareSize {
				t.Errorf("%s: square size %d, want %d", name, b.SquareSize, hr.SquareSize)
			}
		}
		if len(got) != len(want) || !slices.ContainsFunc(got, func(b oplayer.Blob) bool { return b == want[0] }) ||
			!slices.ContainsFunc(got, func(b oplayer.Blob) bool { return b == want[1] }) {
			t.Fatalf("%s: Withdrawn = %+v, want %+v", name, got, want)
		}
	}
	if got, err := oplayer.Withdrawn(raw, tombSet(t)); err != nil || len(got) != 0 {
		t.Fatalf("empty set: %+v, %v", got, err)
	}
}

func TestSetKeysAreCompactKeys(t *testing.T) {
	blobs := fixtureBlobs(t)
	got := oplayer.SetKeys(tombSet(t, blobs...))
	var want []string
	for _, b := range blobs {
		want = append(want, keysOf(t, b)[0])
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("SetKeys = %v, want the compact keys %v", got, want)
	}
}

// deleteStore records deleted keys and fails the first fails deletes.
type deleteStore struct {
	store.Store
	deleted []string
	fails   int
}

var errDelete = errors.New("r2: 503 on delete")

func (s *deleteStore) Delete(ctx context.Context, key string) error {
	if s.fails > 0 {
		s.fails--
		return errDelete
	}
	s.deleted = append(s.deleted, key)
	return s.Store.Delete(ctx, key)
}

func TestDeleteBothKeys(t *testing.T) {
	ctx := context.Background()
	blobs := fixtureBlobs(t)
	st := &deleteStore{Store: store.NewFS(t.TempDir())}
	var bs []oplayer.Blob
	var want []string
	for _, b := range blobs[:2] {
		ob, err := oplayer.FromRecord(b)
		if err != nil {
			t.Fatal(err)
		}
		bs = append(bs, ob)
		keys := keysOf(t, b)
		want = append(want, keys[:]...)
		// Only the compact key exists; the missing full key is no error.
		if err := st.Put(ctx, keys[0], b.Data); err != nil {
			t.Fatal(err)
		}
	}
	n, err := oplayer.Delete(ctx, st, bs)
	if err != nil || n != 4 {
		t.Fatalf("Delete = %d, %v; want 4, nil", n, err)
	}
	if !slices.Equal(st.deleted, want) {
		t.Fatalf("deleted %v, want %v", st.deleted, want)
	}
	if got := listOP(t, st); len(got) != 0 {
		t.Fatalf("op objects left: %v", got)
	}

	st.fails, st.deleted = 1, nil
	n, err = oplayer.Delete(ctx, st, bs)
	if !errors.Is(err, errDelete) || n != 3 {
		t.Fatalf("with a failing delete: %d, %v; want 3 and the error", n, err)
	}
}
