package square_test

import (
	"testing"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"

	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/square"
)

func TestEDSIndex(t *testing.T) {
	for _, c := range []struct {
		i    uint32
		w    uint64
		want uint64
	}{
		{0, 4, 0},
		{3, 4, 3},
		{4, 4, 8},
		{15, 4, 27},
		{0, 1, 0},
		{1<<16 - 1, 1 << 8, 255*512 + 255},
		{1<<32 - 1, 1 << 32, 1<<32 - 1}, // no overflow at huge widths
	} {
		got, err := square.EDSIndex(c.i, c.w)
		if err != nil || got != c.want {
			t.Errorf("EDSIndex(%d, %d) = %d, %v; want %d", c.i, c.w, got, err, c.want)
		}
	}
	for _, c := range []struct {
		i uint32
		w uint64
	}{
		{0, 0},
		{0, 3},
		{16, 4},
		{1 << 16, 1 << 8},
	} {
		if got, err := square.EDSIndex(c.i, c.w); err == nil {
			t.Errorf("EDSIndex(%d, %d) = %d, want an error", c.i, c.w, got)
		}
	}
}

func TestWidthFromProofMatchesSquareSize(t *testing.T) {
	checked := 0
	for _, name := range []string{
		"mocha-1834013", "mocha-1834076",
		"mainnet-1500004", "mainnet-4499999", "mainnet-10000000", "mainnet-15300000",
	} {
		t.Run(name, func(t *testing.T) {
			blocks := archivetest.Blocks(t, 2, map[int64]string{1: name})
			hr, blobs, err := process.Process(blocks[1], blocks[2])
			if err != nil {
				t.Fatal(err)
			}
			checked += len(blobs)
			for i, b := range blobs {
				var sp proof.ShareProof
				if err := sp.Unmarshal(b.ShareProof); err != nil {
					t.Fatal(err)
				}
				w, err := square.WidthFromProof(&sp)
				if err != nil || w != hr.SquareSize {
					t.Errorf("blob %d: width %d, %v; square size %d", i, w, err, hr.SquareSize)
				}
			}
		})
	}
	if checked == 0 {
		t.Fatal("the fixtures have no blobs")
	}
	t.Logf("checked %d blobs", checked)
}

func TestWidthFromProofRejectsBadTotal(t *testing.T) {
	for _, c := range []struct {
		name string
		sp   *proof.ShareProof
	}{
		{"no row proof", &proof.ShareProof{}},
		{"no proofs", &proof.ShareProof{RowProof: &proof.RowProof{}}},
		{"nil proof", &proof.ShareProof{RowProof: &proof.RowProof{Proofs: []*proof.Proof{nil}}}},
		{"zero", rowTotal(0)},
		{"negative", rowTotal(-4)},
		{"not 4w", rowTotal(6)},
		{"not a power of two", rowTotal(12)},
	} {
		if w, err := square.WidthFromProof(c.sp); err == nil {
			t.Errorf("%s: width %d, want an error", c.name, w)
		}
	}
}

func rowTotal(total int64) *proof.ShareProof {
	return &proof.ShareProof{RowProof: &proof.RowProof{Proofs: []*proof.Proof{{Total: total}}}}
}
