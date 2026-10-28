// Package api serves archived blobs over celestia-node's JSON-RPC methods.
// This file holds the wire types; their encoding is pinned byte for byte by
// responses captured from celestia-node (testdata/celestia-node).
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/nmt"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmttypes "github.com/cometbft/cometbft/types"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/square"
)

// Blob is celestia-node's blob.Blob as it appears on the wire.
type Blob struct {
	Namespace    []byte `json:"namespace"`
	Data         []byte `json:"data"`
	ShareVersion uint8  `json:"share_version"`
	Commitment   []byte `json:"commitment"`
	Signer       []byte `json:"signer,omitempty"`
	// Index is the blob's first share in the extended data square.
	Index int `json:"index"`
}

// JSONRPCError is the error object of a JSON-RPC 2.0 response.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// BlobFrom converts an archived blob record of a height whose original data
// square is squareSize wide.
func BlobFrom(rec *archivev1.BlobRecord, squareSize uint64) (Blob, error) {
	if rec == nil {
		return Blob{}, errors.New("no blob record")
	}
	if rec.ShareVersion > math.MaxUint8 {
		return Blob{}, fmt.Errorf("share version %d out of range", rec.ShareVersion)
	}
	idx, err := square.EDSIndex(rec.ShareStart, squareSize)
	if err != nil {
		return Blob{}, fmt.Errorf("blob index: %w", err)
	}
	// celestia-node's Blob.index is an int; keep it decodable by 32-bit clients.
	if idx > math.MaxInt32 {
		return Blob{}, fmt.Errorf("blob index %d out of range", idx)
	}
	return Blob{
		Namespace:    rec.Namespace,
		Data:         rec.Data,
		ShareVersion: uint8(rec.ShareVersion),
		Commitment:   rec.Commitment,
		Signer:       rec.Signer,
		Index:        int(idx),
	}, nil
}

// ProofFrom returns celestia-node's blob.Proof for a share proof: one NMT
// inclusion proof per row, in row order.
func ProofFrom(sp *proof.ShareProof) ([]*nmt.Proof, error) {
	if sp == nil || len(sp.ShareProofs) == 0 {
		return nil, errors.New("share proof has no row proofs")
	}
	out := make([]*nmt.Proof, len(sp.ShareProofs))
	for i, p := range sp.ShareProofs {
		if p == nil {
			return nil, fmt.Errorf("row proof %d is missing", i)
		}
		if len(p.LeafHash) != 0 {
			// An absence proof is not a blob inclusion proof; never pass it off as one.
			return nil, fmt.Errorf("row proof %d is an absence proof", i)
		}
		np := nmt.NewInclusionProof(int(p.Start), int(p.End), p.Nodes, true)
		out[i] = &np
	}
	return out, nil
}

// ProofsEqual reports whether a and b hold the same row proofs: start, end,
// nodes, leaf hash and the ignore-max-namespace flag.
func ProofsEqual(a, b []*nmt.Proof) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		p, q := a[i], b[i]
		if p == nil || q == nil {
			if p != q {
				return false
			}
			continue
		}
		if p.Start() != q.Start() || p.End() != q.End() ||
			p.IsMaxNamespaceIDIgnored() != q.IsMaxNamespaceIDIgnored() ||
			!bytes.Equal(p.LeafHash(), q.LeafHash()) ||
			len(p.Nodes()) != len(q.Nodes()) {
			return false
		}
		for j, n := range p.Nodes() {
			if !bytes.Equal(n, q.Nodes()[j]) {
				return false
			}
		}
	}
	return true
}

// HeaderFrom returns the ExtendedHeader of hr as header.GetByHeight encodes
// it, with only the parts the archive keeps: header and commit. The archive
// stores neither the validator set nor the data availability header, so
// validator_set and dah are null. The commit is the canonical one (block
// h+1's LastCommit); celestia-node may serve the commit its bridge saw first,
// which commits the same block with a different set of signatures.
func HeaderFrom(hr *archivev1.HeightRecord) (json.RawMessage, error) {
	if hr == nil {
		return nil, errors.New("no height record")
	}
	var hp cmtproto.Header
	if err := hp.Unmarshal(hr.Header); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	h, err := cmttypes.HeaderFromProto(&hp)
	if err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}
	var cp cmtproto.Commit
	if err := cp.Unmarshal(hr.Commit); err != nil {
		return nil, fmt.Errorf("decode commit: %w", err)
	}
	c, err := cmttypes.CommitFromProto(&cp)
	if err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	hj, err := cmtjson.Marshal(&h)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	cj, err := cmtjson.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encode commit: %w", err)
	}
	return json.Marshal(struct {
		Header       json.RawMessage `json:"header"`
		Commit       json.RawMessage `json:"commit"`
		ValidatorSet json.RawMessage `json:"validator_set"`
		DAH          json.RawMessage `json:"dah"`
	}{hj, cj, json.RawMessage("null"), json.RawMessage("null")})
}
