package process

import (
	"bytes"
	"fmt"

	"github.com/celestiaorg/celestia-app/v10/pkg/appconsts"
	v5 "github.com/celestiaorg/celestia-app/v10/pkg/appconsts/v5"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	squarev2 "github.com/celestiaorg/go-square/v2"
	squarev3 "github.com/celestiaorg/go-square/v3"
	squarev4 "github.com/celestiaorg/go-square/v4"
	"github.com/celestiaorg/go-square/v4/share"
)

// blobRangeFinder returns the half-open ODS share range [start, end) of the
// blob identified by (txIndex, blobIndex), where txIndex is the index into the
// block's transaction list.
type blobRangeFinder func(txIndex, blobIndex int) (start, end int, err error)

// newBlobRangeFinder returns a finder backed by go-square's own square
// builder, the same major version that da.ConstructEDS uses for appVersion:
//
//	app versions 1-5: go-square v2, SubtreeRootThreshold/SquareSizeUpperBound of appconsts/v5
//	app versions 6-7: go-square v3
//	app versions 8+ : go-square v4 (txs classified with fibre types, as ConstructEDS does)
//
// The builder is the one behind go-square's BlobShareRange: it places blobs
// in the order the square builder chose, which is not necessarily tx order,
// and reports the resulting start index and length per (tx, blob). It is
// built once per block instead of once per blob (BlobShareRange rebuilds it
// on every call); the lookups are the same FindBlobStartingIndex and
// BlobShareLength calls.
//
// No ODS scan is needed, but every range is still checked against the actual
// shares by verifyBlobShares, so a disagreement between the builder and the
// square is an error rather than a wrong proof.
func newBlobRangeFinder(txs [][]byte, appVersion uint64) (blobRangeFinder, error) {
	switch appVersion {
	case 0:
		return nil, fmt.Errorf("app version cannot be 0")
	case 1, 2, 3, 4, 5:
		b, err := squarev2.NewBuilder(v5.SquareSizeUpperBound, v5.SubtreeRootThreshold, txs...)
		if err != nil {
			return nil, fmt.Errorf("go-square v2 builder: %w", err)
		}
		return func(txIndex, blobIndex int) (int, int, error) {
			start, err := b.FindBlobStartingIndex(txIndex, blobIndex)
			if err != nil {
				return 0, 0, err
			}
			n, err := b.BlobShareLength(txIndex, blobIndex)
			return start, start + n, err
		}, nil
	case 6, 7:
		b, err := squarev3.NewBuilder(appconsts.SquareSizeUpperBound, appconsts.SubtreeRootThreshold, txs...)
		if err != nil {
			return nil, fmt.Errorf("go-square v3 builder: %w", err)
		}
		return func(txIndex, blobIndex int) (int, int, error) {
			start, err := b.FindBlobStartingIndex(txIndex, blobIndex)
			if err != nil {
				return 0, 0, err
			}
			n, err := b.BlobShareLength(txIndex, blobIndex)
			return start, start + n, err
		}, nil
	default:
		classified, err := fibretypes.ClassifyTxs(txs)
		if err != nil {
			return nil, fmt.Errorf("classify txs: %w", err)
		}
		b, err := squarev4.NewBuilder(appconsts.SquareSizeUpperBound, appconsts.SubtreeRootThreshold, classified...)
		if err != nil {
			return nil, fmt.Errorf("go-square v4 builder: %w", err)
		}
		return func(txIndex, blobIndex int) (int, int, error) {
			start, err := b.FindBlobStartingIndex(txIndex, blobIndex)
			if err != nil {
				return 0, 0, err
			}
			n, err := b.BlobShareLength(txIndex, blobIndex)
			return start, start + n, err
		}, nil
	}
}

// verifyBlobShares checks that ods[start:end] is exactly the blob: the first
// share starts a sequence in the blob's namespace and share version, carries
// the blob's sequence length (and signer for share version 1), and the
// concatenated raw data of the range equals the blob data.
func verifyBlobShares(ods [][]byte, start, end int, blob *share.Blob) error {
	if start < 0 || end <= start || end > len(ods) {
		return fmt.Errorf("share range [%d,%d) outside square of %d shares", start, end, len(ods))
	}
	data := make([]byte, 0, blob.DataLen())
	for i := start; i < end; i++ {
		s, err := share.NewShare(ods[i])
		if err != nil {
			return fmt.Errorf("share %d: %w", i, err)
		}
		if !bytes.Equal(s.Namespace().Bytes(), blob.Namespace().Bytes()) {
			return fmt.Errorf("share %d: namespace %x, want %x", i, s.Namespace().Bytes(), blob.Namespace().Bytes())
		}
		if (i == start) != s.IsSequenceStart() {
			return fmt.Errorf("share %d: unexpected sequence-start flag %t", i, s.IsSequenceStart())
		}
		if i == start {
			if s.Version() != blob.ShareVersion() {
				return fmt.Errorf("share %d: share version %d, want %d", i, s.Version(), blob.ShareVersion())
			}
			if int(s.SequenceLen()) != blob.DataLen() {
				return fmt.Errorf("share %d: sequence length %d, want %d", i, s.SequenceLen(), blob.DataLen())
			}
			if s.ContainsSigner() {
				if !bytes.Equal(share.GetSigner(s), blob.Signer()) {
					return fmt.Errorf("share %d: signer %x, want %x", i, share.GetSigner(s), blob.Signer())
				}
			} else if blob.HasSigner() {
				return fmt.Errorf("share %d: blob has a signer but the share does not carry one", i)
			}
		}
		data = append(data, s.RawData()...)
	}
	if len(data) < blob.DataLen() {
		return fmt.Errorf("range holds %d bytes, blob has %d", len(data), blob.DataLen())
	}
	// Anything after DataLen must be sequence padding (zeros).
	if !bytes.Equal(data[:blob.DataLen()], blob.Data()) {
		return fmt.Errorf("share data does not match blob data")
	}
	for _, c := range data[blob.DataLen():] {
		if c != 0 {
			return fmt.Errorf("non-zero padding after blob data")
		}
	}
	return nil
}
