package audit

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

const blobHeight = 3

func setup(t *testing.T, mut archivetest.Mutate) (*source.Fake, store.Store) {
	t.Helper()
	blocks := archivetest.Blocks(t, 7, map[int64]string{blobHeight: "mocha-1834013"})
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 1, 6, 2, mut)
	return source.NewFake(blocks, 7), st
}

func TestAuditOK(t *testing.T) {
	src, st := setup(t, nil)
	for h := int64(1); h <= 6; h++ {
		if err := Check(context.Background(), src, st, h); err != nil {
			t.Fatalf("Check(%d): %v", h, err)
		}
	}
}

func TestAuditDetectsMissingBlob(t *testing.T) {
	src, st := setup(t, func(hr *archivev1.HeightRecord, b []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		if hr.Height == blobHeight {
			return hr, b[:1]
		}
		return hr, b
	})
	err := Check(context.Background(), src, st, blobHeight)
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("Check = %v, want ErrMismatch", err)
	}
	if err := Check(context.Background(), src, st, 1); err != nil {
		t.Fatalf("untouched height: %v", err)
	}

	// Run counts the failure on the metric.
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, src, st, time.Millisecond, m) }()
	deadline := time.Now().Add(10 * time.Second)
	for testutil.ToFloat64(m.Failures) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no audit failure counted")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if testutil.ToFloat64(m.Checks) == 0 {
		t.Fatal("checks not counted")
	}
}

func TestAuditDetectsBadProof(t *testing.T) {
	src, st := setup(t, func(hr *archivev1.HeightRecord, b []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		if hr.Height != blobHeight {
			return hr, b
		}
		out := make([]*archivev1.BlobRecord, len(b))
		for i, br := range b {
			br = proto.Clone(br).(*archivev1.BlobRecord)
			var sp proof.ShareProof
			if err := sp.Unmarshal(br.ShareProof); err != nil {
				t.Fatal(err)
			}
			sp.Data[0][len(sp.Data[0])-1] ^= 1
			bad, err := sp.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			br.ShareProof = bad
			out[i] = br
		}
		return hr, out
	})
	err := Check(context.Background(), src, st, blobHeight)
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("Check = %v, want ErrMismatch", err)
	}
}

func TestAuditTransportErrorIsNotFailure(t *testing.T) {
	src, st := setup(t, nil)
	src.FailNext(2, 1)
	err := Check(context.Background(), src, st, 2)
	if err == nil || errors.Is(err, ErrMismatch) {
		t.Fatalf("Check = %v, want a non-mismatch error", err)
	}
	if err := Check(context.Background(), src, st, 2); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

// A next block whose commit does not sign the audited header is an
// inconsistent source response, not evidence against the archive.
func TestAuditCommitMismatchIsNotFailure(t *testing.T) {
	blocks := archivetest.Blocks(t, 7, map[int64]string{blobHeight: "mocha-1834013"})
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 1, 6, 2, nil)
	bad := *blocks[blobHeight+1]
	bad.LastCommitProto = blocks[blobHeight+2].LastCommitProto
	blocks[blobHeight+1] = &bad
	err := Check(context.Background(), source.NewFake(blocks, 7), st, blobHeight)
	if err == nil || errors.Is(err, ErrMismatch) {
		t.Fatalf("Check = %v, want a non-mismatch error", err)
	}
}

// withdrawFirst lists (if listed) and scrubs the first blob at blobHeight,
// with fake changing the tombstone's copy of the blob. It returns the blob.
func withdrawFirst(t *testing.T, src *source.Fake, st store.Store, listed bool, fake func(*archivev1.BlobRecord)) *archivev1.BlobRecord {
	t.Helper()
	ctx := context.Background()
	cur, _ := src.Block(ctx, blobHeight)
	next, _ := src.Block(ctx, blobHeight+1)
	_, blobs, err := process.Process(cur, next)
	if err != nil || len(blobs) < 2 {
		t.Fatalf("process: %d blobs, %v", len(blobs), err)
	}
	b := blobs[0]
	if listed {
		if err := tombstone.Add(ctx, st, tombstone.Entry{
			Height: blobHeight, Namespace: hex.EncodeToString(b.Namespace), Commitment: hex.EncodeToString(b.Commitment),
			Date: "2026-10-26", Reason: "test",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n := archivetest.Withdraw(t, st, archive.SegmentKey(3, 4), func(r *archivev1.BlobRecord) bool {
		if !bytes.Equal(r.Commitment, b.Commitment) {
			return false
		}
		if fake != nil {
			fake(r)
		}
		return true
	}); n != 1 {
		t.Fatalf("withdrew %d records, want 1", n)
	}
	return b
}

func TestAuditAcceptsListedTombstone(t *testing.T) {
	src, st := setup(t, nil)
	withdrawFirst(t, src, st, true, nil)
	for h := int64(1); h <= 6; h++ {
		if err := Check(context.Background(), src, st, h); err != nil {
			t.Fatalf("Check(%d): %v", h, err)
		}
	}
}

// A listed blob whose segment is not scrubbed yet still passes.
func TestAuditAcceptsListedUnscrubbedBlob(t *testing.T) {
	src, st := setup(t, nil)
	ctx := context.Background()
	cur, _ := src.Block(ctx, blobHeight)
	next, _ := src.Block(ctx, blobHeight+1)
	_, blobs, err := process.Process(cur, next)
	if err != nil || len(blobs) == 0 {
		t.Fatalf("process: %d blobs, %v", len(blobs), err)
	}
	if err := tombstone.Add(ctx, st, tombstone.Entry{
		Height: blobHeight, Namespace: hex.EncodeToString(blobs[0].Namespace), Commitment: hex.EncodeToString(blobs[0].Commitment),
		Date: "2026-10-26", Reason: "test",
	}); err != nil {
		t.Fatal(err)
	}
	if err := Check(context.Background(), src, st, blobHeight); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestAuditFlagsUnlistedTombstone(t *testing.T) {
	src, st := setup(t, nil)
	withdrawFirst(t, src, st, false, nil)
	if err := Check(context.Background(), src, st, blobHeight); !errors.Is(err, ErrMismatch) || !strings.Contains(err.Error(), "not listed") {
		t.Fatalf("Check = %v, want an unlisted tombstone mismatch", err)
	}
}

func TestAuditFlagsTombstoneWithWrongShareRange(t *testing.T) {
	for name, fake := range map[string]func(*archivev1.BlobRecord){
		"start": func(b *archivev1.BlobRecord) { b.ShareStart++ },
		"end":   func(b *archivev1.BlobRecord) { b.ShareEnd++ },
	} {
		t.Run(name, func(t *testing.T) {
			src, st := setup(t, nil)
			withdrawFirst(t, src, st, true, fake)
			if err := Check(context.Background(), src, st, blobHeight); !errors.Is(err, ErrMismatch) || !strings.Contains(err.Error(), "differs from the chain's blob") {
				t.Fatalf("Check = %v, want a tombstone mismatch", err)
			}
		})
	}
}

// A tombstones.json that cannot be loaded says nothing about the archive.
func TestAuditBadTombstoneFileIsNotFailure(t *testing.T) {
	src, st := setup(t, nil)
	withdrawFirst(t, src, st, true, nil)
	if err := st.Put(context.Background(), tombstone.Key, []byte("{")); err != nil {
		t.Fatal(err)
	}
	if err := Check(context.Background(), src, st, blobHeight); err == nil || errors.Is(err, ErrMismatch) {
		t.Fatalf("Check = %v, want a non-mismatch error", err)
	}
}

func TestAuditDetectsUncoveredHeight(t *testing.T) {
	src, st := setup(t, nil)
	if err := Check(context.Background(), src, st, 7); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Check(7) = %v, want ErrMismatch", err)
	}
}

// flakyStore fails GetRange with a generic (transient) error.
type flakyStore struct{ store.Store }

var errBlip = errors.New("r2: 503 slow down")

func (flakyStore) GetRange(context.Context, string, int64, int64) ([]byte, error) {
	return nil, errBlip
}

func TestAuditStoreBlipIsNotFailure(t *testing.T) {
	src, st := setup(t, nil)
	fst := flakyStore{st}
	err := Check(context.Background(), src, fst, 2)
	if err == nil || errors.Is(err, ErrMismatch) || !errors.Is(err, errBlip) {
		t.Fatalf("Check = %v, want the transport error", err)
	}

	m := NewMetrics(prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, src, fst, time.Millisecond, m) }()
	deadline := time.Now().Add(10 * time.Second)
	for testutil.ToFloat64(m.Errors) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no audit error counted")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if f := testutil.ToFloat64(m.Failures); f != 0 {
		t.Fatalf("failures = %v, want 0", f)
	}
}

func TestAuditDetectsMissingAndCorruptIndex(t *testing.T) {
	blocks := archivetest.Blocks(t, 7, map[int64]string{blobHeight: "mocha-1834013"})
	root := t.TempDir()
	st := store.NewFS(root)
	archivetest.Build(t, st, blocks, 1, 6, 2, nil)
	src := source.NewFake(blocks, 7)
	ctx := context.Background()
	// Segments are [1,2] [3,4] [5,6].
	if err := st.Put(ctx, "segments/000000000003-000000000004.idx2", []byte{0xff, 0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if err := Check(ctx, src, st, 3); !errors.Is(err, ErrMismatch) {
		t.Fatalf("corrupt idx: %v, want ErrMismatch", err)
	}
	if err := os.Remove(filepath.Join(root, "segments/000000000005-000000000006.idx2")); err != nil {
		t.Fatal(err)
	}
	if err := Check(ctx, src, st, 5); !errors.Is(err, ErrMismatch) {
		t.Fatalf("missing idx: %v, want ErrMismatch", err)
	}
}

// An .idx entry pointing past the end of its segment is an archive defect
// (counted as a failure), not a transient store error.
func TestAuditIndexEntryPastEOFIsFailure(t *testing.T) {
	src, st := setup(t, nil)
	ctx := context.Background()
	// Segments are [1,2] [3,4] [5,6]; blobHeight=3 is in the second.
	key := "segments/000000000003-000000000004.idx2"
	b, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	first, last, rows, err := segment.DecodeRows(b)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].Height == blobHeight && rows[i].Kind == segment.KindBlob {
			rows[i].Offset += 1 << 30 // the last blob row of the height stays last
			break
		}
	}
	if b, err = segment.EncodeRows(first, last, rows); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, key, b); err != nil {
		t.Fatal(err)
	}
	if err := Check(ctx, src, st, blobHeight); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Check = %v, want ErrMismatch", err)
	}
	m := NewMetrics(prometheus.NewRegistry())
	sample(ctx, src, st, m) // samples a random height; repeat until it hits 3 or 4
	deadline := time.Now().Add(10 * time.Second)
	for testutil.ToFloat64(m.Failures) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no audit failure counted")
		}
		sample(ctx, src, st, m)
	}
	if e := testutil.ToFloat64(m.Errors); e != 0 {
		t.Fatalf("errors = %v, want 0", e)
	}
}
