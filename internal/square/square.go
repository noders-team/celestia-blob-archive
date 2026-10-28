// Package square maps share positions between the original data square (ODS)
// and the extended data square (EDS) and derives the square width from a
// share proof.
package square

import (
	"errors"
	"fmt"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
)

// EDSIndex returns the EDS index of the ODS share at odsIndex in a square of
// the given width: row i/w of the EDS has 2w shares and the ODS fills its
// first w. This is the index celestia-node reports as Blob.Index().
func EDSIndex(odsIndex uint32, width uint64) (uint64, error) {
	if width == 0 || width&(width-1) != 0 {
		return 0, fmt.Errorf("square width %d is not a power of two", width)
	}
	// A width above 2^16 holds every uint32 index (and width*width could
	// overflow).
	if width <= 1<<16 && uint64(odsIndex) >= width*width {
		return 0, fmt.Errorf("share %d outside a square of width %d", odsIndex, width)
	}
	i := uint64(odsIndex)
	return (i/width)*2*width + i%width, nil
}

// WidthFromProof returns the ODS width fixed by sp's row proofs. The data
// root is over 2w row roots then 2w column roots, and Total is bound by the
// merkle path, so Total/4 is the width; it must be a power of two.
func WidthFromProof(sp *proof.ShareProof) (uint64, error) {
	rp := sp.RowProof
	if rp == nil || len(rp.Proofs) == 0 || rp.Proofs[0] == nil {
		return 0, errors.New("no row proof")
	}
	total := rp.Proofs[0].Total
	if total <= 0 || total%4 != 0 {
		return 0, fmt.Errorf("row proof total %d is not 4*width", total)
	}
	w := uint64(total / 4)
	if w&(w-1) != 0 {
		return 0, fmt.Errorf("square width %d is not a power of two", w)
	}
	return w, nil
}
