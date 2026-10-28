package process

import (
	"bytes"
	"fmt"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/celestia-app/v10/pkg/wrapper"
	"github.com/celestiaorg/go-square/v4/share"
	"github.com/celestiaorg/rsmt2d"
	"github.com/cometbft/cometbft/crypto/merkle"
)

// shareProver builds share inclusion proofs for the blobs of one block. It
// produces the same proofs as celestia-app's proof.NewShareInclusionProofFromEDS,
// which on every call copies the whole ODS (only to learn the square size),
// rebuilds the row/column-root Merkle proofs, copies every row it touches and
// rebuilds that row's NMT. A block with tens of thousands of small blobs then
// spends over 15 minutes on it; here that work is done once per block and
// once per row.
//
// Returned proofs share hashes with the prover and with each other: marshal
// them, never modify one in place.
type shareProver struct {
	eds        *rsmt2d.ExtendedDataSquare
	ods        [][]byte // eds.FlattenedODS(), row-major k×k
	squareSize int
	rowRoots   [][]byte
	rootProofs []*merkle.Proof // over row roots then column roots, as upstream
	rows       map[int]*proverRow
}

type proverRow struct {
	tree *wrapper.ErasuredNamespacedMerkleTree
}

// ods must be eds.FlattenedODS().
func newShareProver(eds *rsmt2d.ExtendedDataSquare, ods [][]byte) (*shareProver, error) {
	rowRoots, err := eds.RowRoots()
	if err != nil {
		return nil, err
	}
	colRoots, err := eds.ColRoots()
	if err != nil {
		return nil, err
	}
	_, rootProofs := merkle.ProofsFromByteSlices(append(append([][]byte{}, rowRoots...), colRoots...))
	return &shareProver{
		eds:        eds,
		ods:        ods,
		squareSize: int(eds.Width() / 2),
		rowRoots:   rowRoots,
		rootProofs: rootProofs,
		rows:       make(map[int]*proverRow),
	}, nil
}

// prove mirrors proof.NewShareInclusionProofFromEDS (celestia-app v10.5.0).
func (p *shareProver) prove(namespace share.Namespace, shareRange share.Range) (proof.ShareProof, error) {
	k := p.squareSize
	if shareRange.Start < 0 || shareRange.End <= shareRange.Start || shareRange.End > k*k {
		return proof.ShareProof{}, fmt.Errorf("share range [%d,%d) outside square of %d shares", shareRange.Start, shareRange.End, k*k)
	}
	startRow := shareRange.Start / k
	endRow := (shareRange.End - 1) / k
	startLeaf := shareRange.Start % k
	endLeaf := (shareRange.End - 1) % k

	rowProofs := make([]*proof.Proof, endRow-startRow+1)
	rowRoots := make([][]byte, endRow-startRow+1)
	shareProofs := make([]*proof.NMTProof, 0, endRow-startRow+1)
	var rawShares [][]byte
	for i := startRow; i <= endRow; i++ {
		rp := p.rootProofs[i]
		rowProofs[i-startRow] = &proof.Proof{
			Total:    rp.Total,
			Index:    rp.Index,
			LeafHash: rp.LeafHash,
			Aunts:    rp.Aunts,
		}
		rowRoots[i-startRow] = p.rowRoots[i]
		row, err := p.row(i)
		if err != nil {
			return proof.ShareProof{}, err
		}
		// As proof.CreateShareToRowRootProofs: the first row starts at
		// startLeaf, the last ends at endLeaf, rows between are whole.
		from, to := startLeaf, endLeaf
		if i > startRow {
			from = 0
		}
		if i < endRow {
			to = k - 1
		}
		// The first k shares of extended row i are ODS row i.
		rawShares = append(rawShares, p.ods[i*k+from:i*k+to+1]...)
		np, err := row.tree.ProveRange(from, to+1)
		if err != nil {
			return proof.ShareProof{}, fmt.Errorf("row %d: prove range [%d,%d]: %w", i, from, to, err)
		}
		shareProofs = append(shareProofs, &proof.NMTProof{
			Start:    int32(np.Start()),
			End:      int32(np.End()),
			Nodes:    np.Nodes(),
			LeafHash: np.LeafHash(),
		})
	}
	return proof.ShareProof{
		RowProof: &proof.RowProof{
			RowRoots: rowRoots,
			Proofs:   rowProofs,
			StartRow: uint32(startRow),
			EndRow:   uint32(endRow),
		},
		Data:             rawShares,
		ShareProofs:      shareProofs,
		NamespaceId:      namespace.ID(),
		NamespaceVersion: uint32(namespace.Version()),
	}, nil
}

// row returns the extended row i and its NMT, built once per block. The tree
// must reproduce the square's row root, as upstream checks on every proof.
func (p *shareProver) row(i int) (*proverRow, error) {
	if r, ok := p.rows[i]; ok {
		return r, nil
	}
	tree := wrapper.NewErasuredNamespacedMerkleTree(uint64(p.squareSize), uint(i))
	for _, s := range p.eds.Row(uint(i)) {
		if err := tree.Push(s); err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
	}
	root, err := tree.Root()
	if err != nil {
		return nil, fmt.Errorf("row %d: %w", i, err)
	}
	if !bytes.Equal(root, p.rowRoots[i]) {
		return nil, fmt.Errorf("row %d: tree root differs from the square's row root", i)
	}
	r := &proverRow{tree: &tree}
	p.rows[i] = r
	return r, nil
}
