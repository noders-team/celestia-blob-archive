// Package oplayer builds the op-alt-da blob IDs of archived blobs and the
// object keys under which op-alt-da's S3 fallback provider looks them up.
//
// op-alt-da (fallback/s3) reads key path.Join(prefix, hex(keccak256(c))),
// where c is the commitment an OP rollup posts on L1:
// 0x01 (generic commitment) ‖ 0x0c ‖ blob ID. A compact ID is
// height u64 LE ‖ commitment[32] (40 B); a full ID appends
// share_offset u32 LE ‖ share_size u32 LE (48 B), where share_offset is
// celestia-node's Blob.Index() (the EDS index of the first share) and
// share_size its Blob.Length() (a number of shares).
package oplayer

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"golang.org/x/crypto/sha3"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/nsrules"
	"github.com/noders-team/celestia-blob-archive/internal/square"
)

// ConfigKey is the object listing the namespaces the archive publishes OP
// objects for.
const ConfigKey = "op/_namespaces.json"

const (
	namespaceSize  = 29
	commitmentSize = 32
	compactIDSize  = 8 + commitmentSize
	fullIDSize     = compactIDSize + 4 + 4
)

// BlobID is an op-alt-da Celestia blob ID.
type BlobID struct {
	Height      uint64
	Commitment  [commitmentSize]byte
	ShareOffset uint32 // EDS index of the blob's first share
	ShareSize   uint32 // number of shares
}

// Compact returns the 40-byte ID: height u64 LE ‖ commitment.
func (id BlobID) Compact() []byte {
	b := make([]byte, compactIDSize, fullIDSize)
	binary.LittleEndian.PutUint64(b, id.Height)
	copy(b[8:], id.Commitment[:])
	return b
}

// Full returns the 48-byte ID: the compact ID ‖ share_offset u32 LE ‖
// share_size u32 LE.
func (id BlobID) Full() []byte {
	b := id.Compact()
	b = binary.LittleEndian.AppendUint32(b, id.ShareOffset)
	return binary.LittleEndian.AppendUint32(b, id.ShareSize)
}

// Commitment returns the L1 commitment of a blob ID: 0x01 (generic
// commitment type) ‖ 0x0c (Celestia) ‖ id.
func Commitment(id []byte) []byte {
	return append([]byte{0x01, 0x0c}, id...)
}

// Key returns the object key of commitment under namespace ns:
// "op/<ns hex>/<keccak256(commitment) hex>", lowercase, no 0x.
func Key(ns [namespaceSize]byte, commitment []byte) string {
	h := sha3.NewLegacyKeccak256()
	h.Write(commitment)
	return "op/" + hex.EncodeToString(ns[:]) + "/" + hex.EncodeToString(h.Sum(nil))
}

// Blob is what the OP keys of an archived (or withdrawn) blob derive from.
type Blob struct {
	Height     uint64
	Namespace  [namespaceSize]byte
	Commitment [commitmentSize]byte
	// ShareStart and ShareEnd are the blob's ODS share range [start, end).
	ShareStart, ShareEnd uint32
	// SquareSize is the ODS width.
	SquareSize uint64
}

// ID returns the blob's op-alt-da ID, with the ODS start mapped to the EDS.
func (b Blob) ID() (BlobID, error) {
	if b.ShareEnd <= b.ShareStart {
		return BlobID{}, fmt.Errorf("blob at height %d: empty share range [%d,%d)", b.Height, b.ShareStart, b.ShareEnd)
	}
	if _, err := square.EDSIndex(b.ShareEnd-1, b.SquareSize); err != nil {
		return BlobID{}, fmt.Errorf("blob at height %d: last share: %w", b.Height, err)
	}
	off, err := square.EDSIndex(b.ShareStart, b.SquareSize)
	if err != nil {
		return BlobID{}, fmt.Errorf("blob at height %d: %w", b.Height, err)
	}
	if off > math.MaxUint32 {
		return BlobID{}, fmt.Errorf("blob at height %d: EDS index %d exceeds uint32", b.Height, off)
	}
	return BlobID{
		Height:      b.Height,
		Commitment:  b.Commitment,
		ShareOffset: uint32(off),
		ShareSize:   b.ShareEnd - b.ShareStart,
	}, nil
}

// Keys returns the object keys of the blob's compact ID and full ID, in that
// order. Only the compact key is written; a takedown deletes both.
func (b Blob) Keys() ([2]string, error) {
	id, err := b.ID()
	if err != nil {
		return [2]string{}, err
	}
	return [2]string{
		Key(b.Namespace, Commitment(id.Compact())),
		Key(b.Namespace, Commitment(id.Full())),
	}, nil
}

// FromRecord returns the Blob of an archived record. The square width comes
// from the record's share proof.
func FromRecord(r *archivev1.BlobRecord) (Blob, error) {
	if len(r.Namespace) != namespaceSize || len(r.Commitment) != commitmentSize {
		return Blob{}, fmt.Errorf("blob at height %d: namespace is %d bytes, commitment %d", r.Height, len(r.Namespace), len(r.Commitment))
	}
	var sp proof.ShareProof
	if err := sp.Unmarshal(r.ShareProof); err != nil {
		return Blob{}, fmt.Errorf("blob at height %d: decode share proof: %w", r.Height, err)
	}
	w, err := square.WidthFromProof(&sp)
	if err != nil {
		return Blob{}, fmt.Errorf("blob at height %d: %w", r.Height, err)
	}
	b := Blob{Height: r.Height, ShareStart: r.ShareStart, ShareEnd: r.ShareEnd, SquareSize: w}
	copy(b.Namespace[:], r.Namespace)
	copy(b.Commitment[:], r.Commitment)
	return b, nil
}

// FromTombstone returns the Blob a tombstone withdraws; squareSize is the
// ODS width of its height. A tombstone does not keep the share proof, so the
// width comes from the height record. It errors on a namespace or commitment
// of the wrong length, an invalid square size or a share range outside the
// square: keys derived from them would let a takedown "succeed" while the
// object stays public.
func FromTombstone(t *archivev1.TombstoneRecord, squareSize uint64) (Blob, error) {
	if len(t.Namespace) != namespaceSize || len(t.Commitment) != commitmentSize {
		return Blob{}, fmt.Errorf("tombstone at height %d: namespace is %d bytes, commitment %d", t.Height, len(t.Namespace), len(t.Commitment))
	}
	b := Blob{Height: t.Height, ShareStart: t.ShareStart, ShareEnd: t.ShareEnd, SquareSize: squareSize}
	copy(b.Namespace[:], t.Namespace)
	copy(b.Commitment[:], t.Commitment)
	if _, err := b.ID(); err != nil {
		return Blob{}, fmt.Errorf("tombstone: %w", err)
	}
	return b, nil
}

// ParseNamespaces parses a comma-separated list of 29-byte hex namespaces
// (spaces around entries are ignored; an empty list gives none). It rejects
// bad hex, wrong lengths, reserved namespaces and duplicates.
func ParseNamespaces(csv string) ([][namespaceSize]byte, error) {
	if strings.TrimSpace(csv) == "" {
		return nil, nil
	}
	var out [][namespaceSize]byte
	seen := make(map[[namespaceSize]byte]bool)
	for i, f := range strings.Split(csv, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			return nil, fmt.Errorf("namespace %d is empty", i+1)
		}
		raw, err := hex.DecodeString(f)
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", f, err)
		}
		if len(raw) != namespaceSize {
			return nil, fmt.Errorf("namespace %q is %d bytes, want %d", f, len(raw), namespaceSize)
		}
		if err := nsrules.CheckBlobNamespace(raw); err != nil {
			return nil, fmt.Errorf("namespace %q: %w", f, err)
		}
		ns := [namespaceSize]byte(raw)
		if seen[ns] {
			return nil, fmt.Errorf("namespace %q is listed twice", f)
		}
		seen[ns] = true
		out = append(out, ns)
	}
	return out, nil
}
