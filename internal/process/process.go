// Package process rebuilds a block's data square, checks it against the
// header's data root and produces the archive records for the block.
package process

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/celestiaorg/celestia-app/v10/pkg/appconsts"
	"github.com/celestiaorg/celestia-app/v10/pkg/da"
	blobtypes "github.com/celestiaorg/celestia-app/v10/x/blob/types"
	"github.com/celestiaorg/go-square/v4/inclusion"
	"github.com/celestiaorg/go-square/v4/share"
	"github.com/celestiaorg/rsmt2d"
	"github.com/cometbft/cometbft/crypto/merkle"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmttypes "github.com/cometbft/cometbft/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	"google.golang.org/protobuf/types/known/timestamppb"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/source"
)

// ErrDataRootMismatch means the data root computed from the block's
// transactions differs from the one in its header.
var ErrDataRootMismatch = errors.New("data root mismatch")

// ErrCommitMismatch means next's LastCommit does not commit to cur's header:
// the source returned blocks that do not chain, like a header that does not
// match its block id. It is not a defect of the block, so it is retryable.
var ErrCommitMismatch = errors.New("commit does not match header")

// Process turns block cur into one HeightRecord and one BlobRecord per blob.
// next must be block cur.Height+1: its LastCommit is the commit for cur.
//
// It never returns records together with an error. Every returned
// ShareProof has been validated against cur.DataHash.
func Process(cur, next *source.Block) (*archivev1.HeightRecord, []*archivev1.BlobRecord, error) {
	if cur == nil || next == nil {
		return nil, nil, errors.New("process: nil block")
	}
	if next.Height != cur.Height+1 {
		return nil, nil, fmt.Errorf("process: next block height %d is not %d+1", next.Height, cur.Height)
	}
	if len(next.LastCommitProto) == 0 {
		return nil, nil, fmt.Errorf("process: block %d has no last commit", next.Height)
	}
	if len(cur.DataHash) != 32 {
		return nil, nil, fmt.Errorf("process: block %d data hash is %d bytes, want 32", cur.Height, len(cur.DataHash))
	}
	if err := checkCommit(cur.HeaderProto, next.LastCommitProto); err != nil {
		return nil, nil, fmt.Errorf("process: block %d: %w", cur.Height, err)
	}

	eds, err := da.ConstructEDS(cur.Txs, cur.AppVersion, -1)
	if err != nil {
		return nil, nil, fmt.Errorf("process: block %d (app version %d): construct square: %w", cur.Height, cur.AppVersion, err)
	}
	dah, err := da.NewDataAvailabilityHeader(eds)
	if err != nil {
		return nil, nil, fmt.Errorf("process: block %d: data availability header: %w", cur.Height, err)
	}
	if got := dah.Hash(); !bytes.Equal(got, cur.DataHash) {
		return nil, nil, fmt.Errorf("%w: block %d app version %d: computed %x, header %x",
			ErrDataRootMismatch, cur.Height, cur.AppVersion, got, cur.DataHash)
	}

	blobs, err := buildBlobRecords(cur, eds)
	if err != nil {
		return nil, nil, fmt.Errorf("process: block %d (app version %d): %w", cur.Height, cur.AppVersion, err)
	}
	hr := &archivev1.HeightRecord{
		Height:     uint64(cur.Height),
		Time:       timestamppb.New(cur.Time),
		AppVersion: cur.AppVersion,
		DataHash:   cur.DataHash,
		SquareSize: cur.SquareSize,
		BlobCount:  uint32(len(blobs)),
		Header:     cur.HeaderProto,
		Commit:     next.LastCommitProto,
	}
	return hr, blobs, nil
}

// checkCommit requires the block id hash of commit (a marshaled
// tendermint.types.Commit) to equal the CometBFT hash of header (a marshaled
// tendermint.types.Header), so HeightRecord.commit signs HeightRecord.header.
func checkCommit(header, commit []byte) error {
	var hp cmtproto.Header
	if err := hp.Unmarshal(header); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}
	hdr, err := cmttypes.HeaderFromProto(&hp)
	if err != nil {
		return fmt.Errorf("invalid header: %w", err)
	}
	var c cmtproto.Commit
	if err := c.Unmarshal(commit); err != nil {
		return fmt.Errorf("decode last commit: %w", err)
	}
	got := hdr.Hash()
	if len(got) == 0 || !bytes.Equal(got, c.BlockID.Hash) {
		return fmt.Errorf("%w: header hash %X, commit block id %X", ErrCommitMismatch, []byte(got), c.BlockID.Hash)
	}
	return nil
}

func buildBlobRecords(cur *source.Block, eds *rsmt2d.ExtendedDataSquare) ([]*archivev1.BlobRecord, error) {
	var (
		records []*archivev1.BlobRecord
		find    blobRangeFinder
		ods     [][]byte
		prover  *shareProver
	)
	for txIndex, raw := range cur.Txs {
		btx, isBlobTx, err := decodeBlobTx(raw, cur.AppVersion)
		if !isBlobTx {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("tx %d: decode BlobTx: %w", txIndex, err)
		}
		commitments, err := payForBlobsCommitments(btx)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", txIndex, err)
		}
		if len(commitments) != len(btx.Blobs) {
			return nil, fmt.Errorf("tx %d: %d share commitments for %d blobs", txIndex, len(commitments), len(btx.Blobs))
		}
		if find == nil {
			if find, err = newBlobRangeFinder(cur.Txs, cur.AppVersion); err != nil {
				return nil, err
			}
			ods = eds.FlattenedODS()
			if prover, err = newShareProver(eds, ods); err != nil {
				return nil, fmt.Errorf("share prover: %w", err)
			}
		}
		for blobIndex, blob := range btx.Blobs {
			start, end, err := find(txIndex, blobIndex)
			if err != nil {
				return nil, fmt.Errorf("tx %d blob %d: locate shares: %w", txIndex, blobIndex, err)
			}
			if err := verifyBlobShares(ods, start, end, blob); err != nil {
				return nil, fmt.Errorf("tx %d blob %d: shares [%d,%d): %w", txIndex, blobIndex, start, end, err)
			}
			commitment, err := checkCommitment(blob, commitments[blobIndex])
			if err != nil {
				return nil, fmt.Errorf("tx %d blob %d: %w", txIndex, blobIndex, err)
			}
			sp, err := prover.prove(blob.Namespace(), share.NewRange(start, end))
			if err != nil {
				return nil, fmt.Errorf("tx %d blob %d: build share proof: %w", txIndex, blobIndex, err)
			}
			if err := sp.Validate(cur.DataHash); err != nil {
				return nil, fmt.Errorf("tx %d blob %d: share proof does not verify against data root: %w", txIndex, blobIndex, err)
			}
			spBytes, err := sp.Marshal()
			if err != nil {
				return nil, fmt.Errorf("tx %d blob %d: marshal share proof: %w", txIndex, blobIndex, err)
			}
			records = append(records, &archivev1.BlobRecord{
				Height:       uint64(cur.Height),
				TxIndex:      uint32(txIndex),
				BlobIndex:    uint32(blobIndex),
				Namespace:    blob.Namespace().Bytes(),
				ShareVersion: uint32(blob.ShareVersion()),
				Data:         blob.Data(),
				Commitment:   commitment,
				Signer:       blob.Signer(),
				ShareStart:   uint32(start),
				ShareEnd:     uint32(end),
				ShareProof:   spBytes,
			})
		}
	}
	return records, nil
}

// payForBlobsCommitments extracts MsgPayForBlobs.share_commitments from the
// BlobTx's inner Cosmos SDK transaction.
func payForBlobsCommitments(btx *blobTx) ([][]byte, error) {
	var tx sdktx.Tx
	if err := tx.Unmarshal(btx.Tx); err != nil {
		return nil, fmt.Errorf("decode inner tx: %w", err)
	}
	if tx.Body == nil {
		return nil, errors.New("inner tx has no body")
	}
	var pfb *blobtypes.MsgPayForBlobs
	for _, m := range tx.Body.Messages {
		if m.TypeUrl != blobtypes.URLMsgPayForBlobs {
			continue
		}
		if pfb != nil {
			return nil, errors.New("inner tx has several MsgPayForBlobs")
		}
		pfb = &blobtypes.MsgPayForBlobs{}
		if err := pfb.Unmarshal(m.Value); err != nil {
			return nil, fmt.Errorf("decode MsgPayForBlobs: %w", err)
		}
	}
	if pfb == nil {
		return nil, errors.New("inner tx has no MsgPayForBlobs")
	}
	return pfb.ShareCommitments, nil
}

// checkCommitment recomputes the blob's share commitment and requires it to
// equal the one the PayForBlobs message declared. The subtree root threshold
// is 64 for every app version (appconsts/v5 and appconsts agree), and the
// commitment algorithm is the same across go-square v2-v4, so go-square v4's
// implementation is used for all versions.
func checkCommitment(blob *share.Blob, declared []byte) ([]byte, error) {
	got, err := inclusion.CreateCommitment(blob, merkle.HashFromByteSlices, appconsts.SubtreeRootThreshold)
	if err != nil {
		return nil, fmt.Errorf("recompute commitment: %w", err)
	}
	if !bytes.Equal(got, declared) {
		return nil, fmt.Errorf("commitment mismatch: recomputed %x, MsgPayForBlobs %x", got, declared)
	}
	return got, nil
}

// BlobCommitment recomputes the share commitment of a blob from its
// namespace, data, share version and (for share version 1) signer. It is the
// same computation Process uses to validate MsgPayForBlobs commitments.
func BlobCommitment(namespace, data []byte, shareVersion uint32, signer []byte) ([]byte, error) {
	if shareVersion > 255 {
		return nil, fmt.Errorf("share version %d out of range", shareVersion)
	}
	ns, err := share.NewNamespaceFromBytes(namespace)
	if err != nil {
		return nil, fmt.Errorf("namespace: %w", err)
	}
	blob, err := share.NewBlob(ns, data, uint8(shareVersion), signer)
	if err != nil {
		return nil, fmt.Errorf("blob: %w", err)
	}
	c, err := inclusion.CreateCommitment(blob, merkle.HashFromByteSlices, appconsts.SubtreeRootThreshold)
	if err != nil {
		return nil, fmt.Errorf("recompute commitment: %w", err)
	}
	return c, nil
}
