package verify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/celestiaorg/celestia-app/v10/pkg/da"
	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/go-square/v4/share"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

const blobHeight = 3

type fakeHeaders map[int64]*source.Block

func (f fakeHeaders) Block(_ context.Context, h int64) (*source.Block, error) {
	b, ok := f[h]
	if !ok {
		return nil, fmt.Errorf("no block %d", h)
	}
	return b, nil
}

// setup archives heights 1..5 (blobs at height 3), applies mut, and returns a
// store reading it over HTTP, the node's blocks and the genuine blob records.
func setup(t *testing.T, mut archivetest.Mutate) (store.Store, fakeHeaders, []*archivev1.BlobRecord) {
	t.Helper()
	st, h, g := setupFixture(t, "mocha-1834013", mut)
	if len(g) < 2 {
		t.Fatalf("fixture has %d blobs", len(g))
	}
	return st, h, g
}

func setupFixture(t *testing.T, fixture string, mut archivetest.Mutate) (store.Store, fakeHeaders, []*archivev1.BlobRecord) {
	t.Helper()
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: fixture})
	_, genuine, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil || len(genuine) < 1 {
		t.Fatalf("process: %d blobs, %v", len(genuine), err)
	}
	dir := t.TempDir()
	archivetest.Build(t, store.NewFS(dir), blocks, 1, 5, 2, mut)
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(srv.Close)
	return client.New(srv.URL), fakeHeaders(blocks), genuine
}

func params(b *archivev1.BlobRecord) Params {
	return Params{Height: int64(b.Height), Namespace: b.Namespace, Commitment: b.Commitment}
}

func onBlob(i int, f func(*archivev1.BlobRecord)) archivetest.Mutate {
	return func(hr *archivev1.HeightRecord, bs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		if len(bs) > i {
			f(bs[i])
		}
		return hr, bs
	}
}

func TestVerifyOK(t *testing.T) {
	st, hdrs, genuine := setup(t, nil)
	for i, b := range genuine {
		if err := Verify(context.Background(), st, hdrs, params(b)); err != nil {
			t.Fatalf("blob %d: %v", i, err)
		}
	}
}

func TestVerifyDetectsTampering(t *testing.T) {
	flipProof := func(b *archivev1.BlobRecord) {
		var sp proof.ShareProof
		if err := sp.Unmarshal(b.ShareProof); err != nil {
			t.Fatal(err)
		}
		sp.RowProof.Proofs[0].Aunts[0][0] ^= 1
		out, err := sp.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		b.ShareProof = out
	}
	tests := []struct {
		name    string
		mut     archivetest.Mutate
		nodeFix func(fakeHeaders)
		req     func(g []*archivev1.BlobRecord) Params
		want    error
	}{
		{
			name: "flipped data byte in stored blob",
			mut:  onBlob(0, func(b *archivev1.BlobRecord) { b.Data = bytes.Clone(b.Data); b.Data[0] ^= 1 }),
			want: ErrSharesMismatch,
		},
		{
			name: "stored commitment forged, request matches it",
			mut:  onBlob(0, func(b *archivev1.BlobRecord) { b.Commitment = bytes.Repeat([]byte{7}, 32) }),
			req: func(g []*archivev1.BlobRecord) Params {
				p := params(g[0])
				p.Commitment = bytes.Repeat([]byte{7}, 32)
				return p
			},
			want: ErrCommitmentMismatch,
		},
		{
			name: "flipped proof node",
			mut:  onBlob(0, flipProof),
			want: ErrProofInvalid,
		},
		{
			// The stored root is compared with the node before the proof is
			// checked, so a forged root is reported as a root mismatch.
			name: "stored data_hash flipped, node honest",
			mut: func(hr *archivev1.HeightRecord, bs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
				if hr.Height == blobHeight {
					hr.DataHash = bytes.Clone(hr.DataHash)
					hr.DataHash[0] ^= 1
				}
				return hr, bs
			},
			want: ErrDataRootMismatch,
		},
		{
			name: "flipped NMT proof node",
			mut: onBlob(0, func(b *archivev1.BlobRecord) {
				var sp proof.ShareProof
				if err := sp.Unmarshal(b.ShareProof); err != nil {
					t.Fatal(err)
				}
				sp.ShareProofs[0].Nodes[0][len(sp.ShareProofs[0].Nodes[0])-1] ^= 1
				out, err := sp.Marshal()
				if err != nil {
					t.Fatal(err)
				}
				b.ShareProof = out
			}),
			want: ErrProofInvalid,
		},
		{
			// Archive is self-consistent (proof validates against its own
			// root) but the node's header says otherwise.
			name: "archive consistent, node reports other data hash",
			nodeFix: func(h fakeHeaders) {
				b := *h[blobHeight]
				b.DataHash = bytes.Clone(b.DataHash)
				b.DataHash[31] ^= 1
				h[blobHeight] = &b
			},
			want: ErrDataRootMismatch,
		},
		{
			name: "proof of the other blob paired with this record",
			mut: func(hr *archivev1.HeightRecord, bs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
				if len(bs) >= 2 {
					bs[0].ShareProof = bs[1].ShareProof
				}
				return hr, bs
			},
			want: ErrSharesMismatch,
		},
		{
			name: "unknown commitment",
			req: func(g []*archivev1.BlobRecord) Params {
				p := params(g[0])
				p.Commitment = bytes.Repeat([]byte{9}, 32)
				return p
			},
			want: ErrNotInArchive,
		},
		{
			name: "height without blobs",
			req: func(g []*archivev1.BlobRecord) Params {
				p := params(g[0])
				p.Height = 2
				return p
			},
			want: ErrNotInArchive,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, hdrs, genuine := setup(t, tc.mut)
			if tc.nodeFix != nil {
				tc.nodeFix(hdrs)
			}
			p := params(genuine[0])
			if tc.req != nil {
				p = tc.req(genuine)
			}
			err := Verify(context.Background(), st, hdrs, p)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			for _, other := range []error{ErrNotInArchive, ErrSharesMismatch, ErrCommitmentMismatch, ErrProofInvalid, ErrDataRootMismatch, ErrProofLayout, ErrReservedNamespace} {
				if other != tc.want && errors.Is(err, other) {
					t.Fatalf("error also matches %v", other)
				}
			}
		})
	}
}

// A height outside the committed ranges is not a failed check: the archive
// simply does not hold it (yet).
func TestVerifyHeightNotArchived(t *testing.T) {
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: "mocha-1834013"})
	_, genuine, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil || len(genuine) == 0 {
		t.Fatalf("process: %d blobs, %v", len(genuine), err)
	}
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 2, 2, 2, nil)
	archivetest.Build(t, st, blocks, 4, 5, 2, nil)
	for _, h := range []int64{1, blobHeight, 6} { // below first, gap, above highest
		t.Run(fmt.Sprint(h), func(t *testing.T) {
			p := params(genuine[0])
			p.Height = h
			err := Verify(context.Background(), st, fakeHeaders(blocks), p)
			if !errors.Is(err, ErrNotArchived) || IsFailure(err) {
				t.Fatalf("got %v, want ErrNotArchived and not a failure", err)
			}
			want := fmt.Sprintf("height %d is not archived (committed ranges: 2, 4-5)", h)
			if err.Error() != want {
				t.Fatalf("message %q, want %q", err, want)
			}
		})
	}
}

// countingStore counts full Gets and range reads.
type countingStore struct {
	store.Store
	mu     sync.Mutex
	gets   []string
	ranges int
}

func (c *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	c.gets = append(c.gets, key)
	c.mu.Unlock()
	return c.Store.Get(ctx, key)
}

func (c *countingStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.mu.Lock()
	c.ranges++
	c.mu.Unlock()
	return c.Store.GetRange(ctx, key, off, n)
}

// A verify reads only the directory entry, the rows of the height and the
// two records it needs, all with range requests.
func TestVerifyUsesRangeReads(t *testing.T) {
	st, hdrs, genuine := setup(t, nil)
	for i, b := range genuine {
		cs := &countingStore{Store: st}
		if err := Verify(context.Background(), cs, hdrs, params(b)); err != nil {
			t.Fatalf("blob %d: %v", i, err)
		}
		if len(cs.gets) != 0 {
			t.Fatalf("blob %d: full Gets of %v, want none", i, cs.gets)
		}
		if cs.ranges > 4 {
			t.Fatalf("blob %d: %d range reads, want at most 4", i, cs.ranges)
		}
	}
}

// A height the directory does not hold is reported through the directory
// lookup, with the committed ranges for context, and is not a failure.
func TestVerifyNotArchivedHeight(t *testing.T) {
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: "mocha-1834013"})
	_, genuine, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil || len(genuine) == 0 {
		t.Fatalf("process: %d blobs, %v", len(genuine), err)
	}
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 4, 5, 2, nil)
	for _, h := range []int64{0, blobHeight, 6, 20000} { // invalid, below first, above highest, missing bucket
		t.Run(fmt.Sprint(h), func(t *testing.T) {
			p := params(genuine[0])
			p.Height = h
			err := Verify(context.Background(), st, fakeHeaders(blocks), p)
			if !errors.Is(err, ErrNotArchived) || !errors.Is(err, archiveread.ErrNotArchived) || IsFailure(err) {
				t.Fatalf("got %v, want ErrNotArchived from the directory and not a failure", err)
			}
			want := fmt.Sprintf("height %d is not archived (committed ranges: 4-5)", h)
			if err.Error() != want {
				t.Fatalf("message %q, want %q", err, want)
			}
		})
	}
}

// An archived height without the requested blob is a failed check.
func TestVerifyBlobNotInArchivedHeight(t *testing.T) {
	st, hdrs, genuine := setup(t, nil)
	for name, c := range map[string][]byte{
		"unknown": bytes.Repeat([]byte{0xab}, 32),
		"short":   genuine[0].Commitment[:31],
	} {
		p := params(genuine[0])
		p.Commitment = c
		err := Verify(context.Background(), st, hdrs, p)
		if !errors.Is(err, ErrNotInArchive) || errors.Is(err, ErrNotArchived) {
			t.Fatalf("%s commitment: got %v, want ErrNotInArchive", name, err)
		}
	}
}

// A height that is not archived wins over a malformed commitment (exit 1,
// not 2), as with the catalog lookup before the height directory.
func TestVerifyNotArchivedBeforeBadCommitment(t *testing.T) {
	st, hdrs, genuine := setup(t, nil)
	short := genuine[0].Commitment[:31]
	for _, tc := range []struct {
		name       string
		height     int64
		commitment []byte
		want       error
	}{
		{"archived, short commitment", blobHeight, short, ErrNotInArchive},
		{"not archived, short commitment", 6, short, ErrNotArchived},
		{"not archived, good commitment", 6, genuine[0].Commitment, ErrNotArchived},
		{"not archived, unknown commitment", 6, bytes.Repeat([]byte{0xab}, 32), ErrNotArchived},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := params(genuine[0])
			p.Height, p.Commitment = tc.height, tc.commitment
			err := Verify(context.Background(), st, hdrs, p)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if other := map[error]error{ErrNotInArchive: ErrNotArchived, ErrNotArchived: ErrNotInArchive}[tc.want]; errors.Is(err, other) {
				t.Fatalf("got %v, also wraps %v", err, other)
			}
		})
	}
}

// A blob whose record was replaced by a tombstone fails with ErrWithdrawn;
// the other blobs of the height still verify.
func TestVerifyWithdrawn(t *testing.T) {
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: "mocha-1834013"})
	_, genuine, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil || len(genuine) < 2 {
		t.Fatalf("process: %d blobs, %v", len(genuine), err)
	}
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 1, 5, 2, nil)
	if n := archivetest.Withdraw(t, st, archive.SegmentKey(3, 4), func(b *archivev1.BlobRecord) bool {
		return bytes.Equal(b.Commitment, genuine[0].Commitment)
	}); n != 1 {
		t.Fatalf("withdrew %d records, want 1", n)
	}
	err = Verify(context.Background(), st, fakeHeaders(blocks), params(genuine[0]))
	if !errors.Is(err, ErrWithdrawn) || !IsFailure(err) || errors.Is(err, ErrNotInArchive) || errors.Is(err, ErrNotArchived) {
		t.Fatalf("got %v, want ErrWithdrawn only", err)
	}
	if want := fmt.Sprintf("height %d, namespace %x, commitment %x: blob withdrawn, see tombstones.json",
		blobHeight, genuine[0].Namespace, genuine[0].Commitment); err.Error() != want {
		t.Fatalf("message %q, want %q", err, want)
	}
	if err := Verify(context.Background(), st, fakeHeaders(blocks), params(genuine[1])); err != nil {
		t.Fatalf("other blob: %v", err)
	}
}

// The decoded record must itself carry the requested height, namespace and
// commitment, whatever its row said.
func TestMatchRecordChecksRecord(t *testing.T) {
	_, _, genuine := setup(t, nil)
	b := genuine[0]
	otherNS := bytes.Clone(b.Namespace)
	otherNS[len(otherNS)-1] ^= 1
	for name, mut := range map[string]func(*archivev1.BlobRecord){
		"namespace":  func(r *archivev1.BlobRecord) { r.Namespace = otherNS },
		"commitment": func(r *archivev1.BlobRecord) { r.Commitment = bytes.Repeat([]byte{1}, 32) },
		"height":     func(r *archivev1.BlobRecord) { r.Height++ },
	} {
		t.Run(name, func(t *testing.T) {
			r := proto.Clone(b).(*archivev1.BlobRecord)
			mut(r)
			_, err := matchRecord(params(b), &archivev1.Record{Kind: &archivev1.Record_Blob{Blob: r}})
			if !errors.Is(err, ErrNotInArchive) {
				t.Fatalf("got %v, want ErrNotInArchive", err)
			}
		})
	}
	other := &archivev1.TombstoneRecord{Height: b.Height, Namespace: b.Namespace, Commitment: bytes.Repeat([]byte{1}, 32)}
	if _, err := matchRecord(params(b), &archivev1.Record{Kind: &archivev1.Record_Tombstone{Tombstone: other}}); !errors.Is(err, ErrNotInArchive) {
		t.Fatalf("tombstone of another blob: got %v, want ErrNotInArchive", err)
	}
	got, err := matchRecord(params(b), &archivev1.Record{Kind: &archivev1.Record_Blob{Blob: b}})
	if err != nil || got != b {
		t.Fatalf("genuine record: %v, %v", got, err)
	}
}

// multiRow returns the index of the blob in g whose proof spans several rows.
func multiRow(t *testing.T, g []*archivev1.BlobRecord) int {
	t.Helper()
	for i, b := range g {
		var sp proof.ShareProof
		if err := sp.Unmarshal(b.ShareProof); err != nil {
			t.Fatal(err)
		}
		if len(sp.ShareProofs) > 1 {
			return i
		}
	}
	t.Fatal("no multi-row blob in fixture")
	return 0
}

func TestVerifyLayoutTampering(t *testing.T) {
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: "mainnet-1500004"})
	_, g, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil {
		t.Fatal(err)
	}
	idx := multiRow(t, g)
	edit := func(f func(sp *proof.ShareProof, b *archivev1.BlobRecord)) archivetest.Mutate {
		return onBlob(idx, func(b *archivev1.BlobRecord) {
			var sp proof.ShareProof
			if err := sp.Unmarshal(b.ShareProof); err != nil {
				t.Fatal(err)
			}
			f(&sp, b)
			out, err := sp.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			b.ShareProof = out
		})
	}
	tests := map[string]archivetest.Mutate{
		"rows swapped (non-consecutive index)": edit(func(sp *proof.ShareProof, _ *archivev1.BlobRecord) {
			p := sp.RowProof
			p.Proofs[0], p.Proofs[1] = p.Proofs[1], p.Proofs[0]
			p.RowRoots[0], p.RowRoots[1] = p.RowRoots[1], p.RowRoots[0]
			sp.ShareProofs[0], sp.ShareProofs[1] = sp.ShareProofs[1], sp.ShareProofs[0]
		}),
		"row duplicated": edit(func(sp *proof.ShareProof, _ *archivev1.BlobRecord) {
			p := sp.RowProof
			p.Proofs[1], p.RowRoots[1], sp.ShareProofs[1] = p.Proofs[0], p.RowRoots[0], sp.ShareProofs[0]
		}),
		"later row does not start at 0": edit(func(sp *proof.ShareProof, _ *archivev1.BlobRecord) {
			sp.ShareProofs[len(sp.ShareProofs)-1].Start = 1
		}),
		"interior row is not full": edit(func(sp *proof.ShareProof, _ *archivev1.BlobRecord) {
			sp.ShareProofs[0].End--
		}),
		"column root index": edit(func(sp *proof.ShareProof, _ *archivev1.BlobRecord) {
			sp.RowProof.Proofs[0].Index += sp.RowProof.Proofs[0].Total / 2
		}),
		"record share_start changed": onBlob(idx, func(b *archivev1.BlobRecord) { b.ShareStart++ }),
	}
	for name, mut := range tests {
		t.Run(name, func(t *testing.T) {
			st, hdrs, genuine := setupFixture(t, "mainnet-1500004", mut)
			err := Verify(context.Background(), st, hdrs, params(genuine[idx]))
			if !errors.Is(err, ErrProofLayout) {
				t.Fatalf("got %v, want ErrProofLayout", err)
			}
		})
	}
	t.Run("untampered", func(t *testing.T) {
		st, hdrs, genuine := setupFixture(t, "mainnet-1500004", nil)
		if err := Verify(context.Background(), st, hdrs, params(genuine[idx])); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLayoutAcceptsHonestProofsOfAllFixtures(t *testing.T) {
	names := []string{"mainnet-10000000", "mainnet-1500004", "mainnet-15300000", "mainnet-4499999", "mocha-1834013", "mocha-1834076"}
	n := 0
	for _, name := range names {
		blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: name})
		_, recs, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for i, r := range recs {
			var sp proof.ShareProof
			if err := sp.Unmarshal(r.ShareProof); err != nil {
				t.Fatal(err)
			}
			if err := checkLayout(&sp, r); err != nil {
				t.Fatalf("%s blob %d: %v", name, i, err)
			}
			n++
		}
	}
	if n == 0 {
		t.Fatal("no blobs")
	}
}

// TestVerifyRejectsTxNamespacePrefix forges a "blob" from the proven, honestly
// generated compact (tx or PFB namespace) shares at the start of the square: the compact shares
// parse as one sparse sequence, so shares, commitment, layout and proof all
// check out. Only the reserved-namespace check stops it.
func TestVerifyRejectsTxNamespacePrefix(t *testing.T) {
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: "mocha-1834013"})
	blk := blocks[blobHeight]
	eds, err := da.ConstructEDS(blk.Txs, blk.AppVersion, -1)
	if err != nil {
		t.Fatal(err)
	}
	ods := eds.FlattenedODS()
	var raw []share.Share
	var firstNS share.Namespace
	for i, r := range ods {
		s, err := share.NewShare(r)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstNS = s.Namespace()
			if !firstNS.IsReserved() {
				t.Fatalf("first share namespace %x is not reserved", firstNS.Bytes())
			}
		}
		if !s.Namespace().Equals(firstNS) {
			break
		}
		raw = append(raw, s)
	}
	if len(raw) == 0 {
		t.Fatal("no tx shares")
	}
	blobs, err := share.ParseBlobs(raw)
	if err != nil || len(blobs) != 1 {
		t.Fatalf("forgery premise: parse gave %d blobs, err %v", len(blobs), err)
	}
	fb := blobs[0]
	commitment, err := process.BlobCommitment(fb.Namespace().Bytes(), fb.Data(), uint32(fb.ShareVersion()), fb.Signer())
	if err != nil {
		t.Fatalf("forgery premise: commitment: %v", err)
	}
	sp, err := proof.NewShareInclusionProofFromEDS(eds, fb.Namespace(), share.NewRange(0, len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	spb, err := sp.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	forged := &archivev1.BlobRecord{
		Height: blobHeight, Namespace: fb.Namespace().Bytes(), ShareVersion: uint32(fb.ShareVersion()),
		Data: fb.Data(), Commitment: commitment, ShareStart: 0, ShareEnd: uint32(len(raw)), ShareProof: spb,
	}
	mut := func(hr *archivev1.HeightRecord, bs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		if hr.Height == blobHeight {
			return hr, []*archivev1.BlobRecord{forged}
		}
		return hr, bs
	}
	st, hdrs, _ := setupFixture(t, "mocha-1834013", mut)
	err = Verify(context.Background(), st, hdrs, Params{Height: blobHeight, Namespace: forged.Namespace, Commitment: commitment})
	if !errors.Is(err, ErrReservedNamespace) {
		t.Fatalf("got %v, want ErrReservedNamespace", err)
	}
}

// find matches the record on namespace bytes, so through Verify the record's
// namespace always equals the (already checked) request namespace. The record
// check is defence in depth; exercise it directly with a normal request
// namespace and an archived record in a reserved one.
func TestVerifyRecordReservedNamespace(t *testing.T) {
	st, hdrs, genuine := setup(t, nil)
	p := params(genuine[0])
	ctx := context.Background()
	rec, hr, err := find(ctx, st, p)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRecord(ctx, hdrs, p, rec, hr); err != nil {
		t.Fatalf("genuine record: %v", err)
	}
	bad := proto.Clone(rec).(*archivev1.BlobRecord)
	bad.Namespace = share.TxNamespace.Bytes()
	if err := verifyRecord(ctx, hdrs, p, bad, hr); !errors.Is(err, ErrReservedNamespace) {
		t.Fatalf("got %v, want ErrReservedNamespace", err)
	}
}

// fixtureRecords processes the fixture as height blobHeight and returns its
// height record and blob records, as the archive stores them.
func fixtureRecords(t *testing.T, fixture string) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
	t.Helper()
	blocks := archivetest.Blocks(t, 6, map[int64]string{blobHeight: fixture})
	hr, recs, err := process.Process(blocks[blobHeight], blocks[blobHeight+1])
	if err != nil {
		t.Fatalf("%s: %v", fixture, err)
	}
	return hr, recs
}

func TestCheckRecordOK(t *testing.T) {
	files, err := filepath.Glob("../../testdata/blocks/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("fixtures: %v, %v", files, err)
	}
	n := 0
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".json")
		hr, recs := fixtureRecords(t, name)
		for i, r := range recs {
			if err := CheckRecord(r, hr); err != nil {
				t.Fatalf("%s blob %d: %v", name, i, err)
			}
			n++
		}
	}
	if n == 0 {
		t.Fatal("no blobs")
	}
}

// A tombstone has no BlobRecord; CheckRecord(rec.GetBlob()) must fail, not
// panic.
func TestCheckRecordNilRecord(t *testing.T) {
	hr, _ := fixtureRecords(t, "mocha-1834013")
	tomb := &archivev1.Record{Kind: &archivev1.Record_Tombstone{Tombstone: &archivev1.TombstoneRecord{Height: blobHeight}}}
	if err := CheckRecord(tomb.GetBlob(), hr); !errors.Is(err, ErrNotInArchive) {
		t.Fatalf("got %v, want ErrNotInArchive", err)
	}
}

// TestCheckRecordDetectsTampering: CheckRecord needs no node, so a forged
// stored data root fails the proof check instead of the node comparison.
func TestCheckRecordDetectsTampering(t *testing.T) {
	editProof := func(f func(sp *proof.ShareProof)) func(*archivev1.BlobRecord, []*archivev1.BlobRecord) {
		return func(b *archivev1.BlobRecord, _ []*archivev1.BlobRecord) {
			var sp proof.ShareProof
			if err := sp.Unmarshal(b.ShareProof); err != nil {
				t.Fatal(err)
			}
			f(&sp)
			out, err := sp.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			b.ShareProof = out
		}
	}
	tests := []struct {
		name string
		mut  func(b *archivev1.BlobRecord, all []*archivev1.BlobRecord)
		hr   func(hr *archivev1.HeightRecord)
		want error
	}{
		{
			name: "flipped data byte",
			mut:  func(b *archivev1.BlobRecord, _ []*archivev1.BlobRecord) { b.Data[0] ^= 1 },
			want: ErrSharesMismatch,
		},
		{
			name: "spliced share (proof of the other blob)",
			mut:  func(b *archivev1.BlobRecord, all []*archivev1.BlobRecord) { b.ShareProof = all[1].ShareProof },
			want: ErrSharesMismatch,
		},
		{
			name: "forged commitment",
			mut:  func(b *archivev1.BlobRecord, _ []*archivev1.BlobRecord) { b.Commitment = bytes.Repeat([]byte{7}, 32) },
			want: ErrCommitmentMismatch,
		},
		{
			name: "flipped row proof node",
			mut:  editProof(func(sp *proof.ShareProof) { sp.RowProof.Proofs[0].Aunts[0][0] ^= 1 }),
			want: ErrProofInvalid,
		},
		{
			name: "flipped NMT proof node",
			mut: editProof(func(sp *proof.ShareProof) {
				sp.ShareProofs[0].Nodes[0][len(sp.ShareProofs[0].Nodes[0])-1] ^= 1
			}),
			want: ErrProofInvalid,
		},
		{
			name: "record share_start changed",
			mut:  func(b *archivev1.BlobRecord, _ []*archivev1.BlobRecord) { b.ShareStart++ },
			want: ErrProofLayout,
		},
		{
			name: "stored data root flipped",
			hr:   func(hr *archivev1.HeightRecord) { hr.DataHash[0] ^= 1 },
			want: ErrProofInvalid,
		},
		{
			// square_size feeds the served index; only the proof width is
			// bound to the data root.
			name: "height record square_size doubled",
			hr:   func(hr *archivev1.HeightRecord) { hr.SquareSize *= 2 },
			want: ErrProofLayout,
		},
		{
			name: "reserved namespace",
			mut:  func(b *archivev1.BlobRecord, _ []*archivev1.BlobRecord) { b.Namespace = share.TxNamespace.Bytes() },
			want: ErrReservedNamespace,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hr0, recs0 := fixtureRecords(t, "mocha-1834013")
			if len(recs0) < 2 {
				t.Fatalf("fixture has %d blobs", len(recs0))
			}
			hr := proto.Clone(hr0).(*archivev1.HeightRecord)
			recs := make([]*archivev1.BlobRecord, len(recs0))
			for i, r := range recs0 {
				recs[i] = proto.Clone(r).(*archivev1.BlobRecord)
			}
			if tc.mut != nil {
				tc.mut(recs[0], recs)
			}
			if tc.hr != nil {
				tc.hr(hr)
			}
			err := CheckRecord(recs[0], hr)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			for _, other := range []error{ErrNotInArchive, ErrSharesMismatch, ErrCommitmentMismatch, ErrProofInvalid, ErrDataRootMismatch, ErrProofLayout, ErrReservedNamespace} {
				if other != tc.want && errors.Is(err, other) {
					t.Fatalf("error also matches %v", other)
				}
			}
		})
	}
}
