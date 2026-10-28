package process

import (
	"fmt"

	blobtxv2 "github.com/celestiaorg/go-square/v2/tx"
	blobtxv3 "github.com/celestiaorg/go-square/v3/tx"
	"github.com/celestiaorg/go-square/v4/share"
	blobtxv4 "github.com/celestiaorg/go-square/v4/tx"
)

// blobTx is a decoded BlobTx with its blobs in go-square v4's type, the common
// representation used for share verification and commitments.
type blobTx struct {
	Tx    []byte
	Blobs []*share.Blob
}

// decodeBlobTx decodes raw with the go-square major version that
// da.ConstructEDS uses to build the square for appVersion, so that "is this a
// BlobTx" and the blob list always agree with the square (v4 additionally
// rejects non-canonical and nested BlobTxs, which v2/v3 accept):
// app 1-5 -> v2, 6-7 -> v3, 8+ -> v4. Like the square builders, the bool
// reports whether raw is a BlobTx at all; err with true means a malformed one.
func decodeBlobTx(raw []byte, appVersion uint64) (*blobTx, bool, error) {
	switch appVersion {
	case 1, 2, 3, 4, 5:
		btx, ok, err := blobtxv2.UnmarshalBlobTx(raw)
		if err != nil || !ok {
			return nil, ok, err
		}
		out := &blobTx{Tx: btx.Tx}
		for i, b := range btx.Blobs {
			nb, err := convertBlob(b.Namespace().Bytes(), b.Data(), b.ShareVersion(), b.Signer())
			if err != nil {
				return nil, true, fmt.Errorf("blob %d: %w", i, err)
			}
			out.Blobs = append(out.Blobs, nb)
		}
		return out, true, nil
	case 6, 7:
		btx, ok, err := blobtxv3.UnmarshalBlobTx(raw)
		if err != nil || !ok {
			return nil, ok, err
		}
		out := &blobTx{Tx: btx.Tx}
		for i, b := range btx.Blobs {
			nb, err := convertBlob(b.Namespace().Bytes(), b.Data(), b.ShareVersion(), b.Signer())
			if err != nil {
				return nil, true, fmt.Errorf("blob %d: %w", i, err)
			}
			out.Blobs = append(out.Blobs, nb)
		}
		return out, true, nil
	case 0:
		return nil, false, fmt.Errorf("app version cannot be 0")
	default:
		btx, ok, err := blobtxv4.UnmarshalBlobTx(raw)
		if err != nil || !ok {
			return nil, ok, err
		}
		return &blobTx{Tx: btx.Tx, Blobs: btx.Blobs}, true, nil
	}
}

func convertBlob(ns, data []byte, shareVersion uint8, signer []byte) (*share.Blob, error) {
	n, err := share.NewNamespaceFromBytes(ns)
	if err != nil {
		return nil, err
	}
	return share.NewBlob(n, data, shareVersion, signer)
}
