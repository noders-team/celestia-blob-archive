package process

import (
	"bytes"
	"testing"

	"github.com/celestiaorg/celestia-app/v10/pkg/da"
	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/go-square/v4/share"
	"github.com/celestiaorg/rsmt2d"
)

// The per-block prover must produce byte-identical proofs to celestia-app's
// NewShareInclusionProofFromEDS: stored proofs are part of the archive format.
func TestShareProverMatchesUpstream(t *testing.T) {
	multiRow := 0
	for _, path := range fixturePaths(t) {
		cur, next := loadFixture(t, path)
		_, blobs, err := Process(cur, next)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		eds, p := proverFor(t, cur.Txs, cur.AppVersion)
		k := int(eds.Width() / 2)
		for i, br := range blobs {
			ns, err := share.NewNamespaceFromBytes(br.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			assertSameProof(t, eds, p, ns, share.NewRange(int(br.ShareStart), int(br.ShareEnd)))
			if t.Failed() {
				t.Fatalf("%s blob %d", path, i)
			}
			if (int(br.ShareEnd)-1)/k != int(br.ShareStart)/k {
				multiRow++
			}
		}
	}
	if multiRow == 0 {
		t.Fatal("no fixture blob spans several rows; the test does not cover multi-row ranges")
	}
}

// Every range of the smallest fixture square, so row-aligned ranges, the full
// square and the last share are covered too, not only real blob ranges.
func TestShareProverMatchesUpstreamAllRanges(t *testing.T) {
	var smallest struct {
		eds *rsmt2d.ExtendedDataSquare
		p   *shareProver
	}
	for _, path := range fixturePaths(t) {
		cur, _ := loadFixture(t, path)
		eds, p := proverFor(t, cur.Txs, cur.AppVersion)
		if eds.Width() >= 4 && (smallest.eds == nil || eds.Width() < smallest.eds.Width()) {
			smallest.eds, smallest.p = eds, p
		}
	}
	if smallest.eds == nil {
		t.Fatal("no fixture with a square of at least 2x2")
	}
	k := int(smallest.eds.Width() / 2)
	if k > 16 {
		t.Fatalf("smallest fixture square is %dx%d; an exhaustive sweep would be too slow", k, k)
	}
	t.Logf("sweeping all ranges of a %dx%d square", k, k)
	ns := share.TxNamespace // namespace only labels the proof; any is fine
	for s := 0; s < k*k; s++ {
		for e := s + 1; e <= k*k; e++ {
			assertSameProof(t, smallest.eds, smallest.p, ns, share.NewRange(s, e))
			if t.Failed() {
				t.Fatalf("k=%d range [%d,%d)", k, s, e)
			}
		}
	}
}

func proverFor(t *testing.T, txs [][]byte, appVersion uint64) (*rsmt2d.ExtendedDataSquare, *shareProver) {
	t.Helper()
	eds, err := da.ConstructEDS(txs, appVersion, -1)
	if err != nil {
		t.Fatal(err)
	}
	p, err := newShareProver(eds, eds.FlattenedODS())
	if err != nil {
		t.Fatal(err)
	}
	return eds, p
}

func assertSameProof(t *testing.T, eds *rsmt2d.ExtendedDataSquare, p *shareProver, ns share.Namespace, rng share.Range) {
	t.Helper()
	want, err := proof.NewShareInclusionProofFromEDS(eds, ns, rng)
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.prove(ns, rng)
	if err != nil {
		t.Fatal(err)
	}
	wb, err := want.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	gb, err := got.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wb, gb) {
		t.Error("proof differs from upstream")
	}
}
