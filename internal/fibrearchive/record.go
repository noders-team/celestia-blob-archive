package fibrearchive

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
)

// Status is FibreRecord.data_status.
type Status uint32

// Data statuses; every status in a written record is final.
const (
	StatusArchived      Status = 1
	StatusExpired       Status = 2
	StatusUnrecoverable Status = 3
	StatusWithdrawn     Status = 4
	StatusSkippedCap    Status = 5
)

var statusNames = [...]string{
	StatusArchived:      "archived",
	StatusExpired:       "expired",
	StatusUnrecoverable: "unrecoverable",
	StatusWithdrawn:     "withdrawn",
	StatusSkippedCap:    "skipped_cap",
}

// Valid reports whether s is one of the five statuses.
func (s Status) Valid() bool { return s >= StatusArchived && s <= StatusSkippedCap }

// String returns the status name used in the index and manifest counts.
func (s Status) String() string {
	if !s.Valid() {
		return fmt.Sprintf("status(%d)", uint32(s))
	}
	return statusNames[s]
}

// Kind is FibreRecord.kind.
type Kind uint32

// Message kinds.
const (
	KindPayForFibre Kind = 1 // MsgPayForFibre
	KindTimeout     Kind = 2 // MsgPaymentPromiseTimeout
)

// Valid reports whether k is one of the two kinds.
func (k Kind) Valid() bool { return k == KindPayForFibre || k == KindTimeout }

// String returns the kind name used in the index.
func (k Kind) String() string {
	switch k {
	case KindPayForFibre:
		return "pff"
	case KindTimeout:
		return "timeout"
	}
	return fmt.Sprintf("kind(%d)", uint32(k))
}

// EncodeRecord marshals r deterministically. It does not validate r.
func EncodeRecord(r *archivev1.FibreRecord) ([]byte, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal fibre record: %w", err)
	}
	return b, nil
}

// DecodeRecord unmarshals and validates a record: kind 1 or 2, a 33-byte
// blob_id, data_status 1..5, and data_size non-zero exactly when archived.
func DecodeRecord(b []byte) (*archivev1.FibreRecord, error) {
	var r archivev1.FibreRecord
	if err := proto.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("unmarshal fibre record: %w", err)
	}
	if err := validate(&r); err != nil {
		return nil, fmt.Errorf("fibre record at height %d tx %d msg %d: %w", r.Height, r.TxIndex, r.MsgIndex, err)
	}
	return &r, nil
}

func validate(r *archivev1.FibreRecord) error {
	if !Kind(r.Kind).Valid() {
		return fmt.Errorf("kind %d, want 1 or 2", r.Kind)
	}
	if len(r.BlobId) != BlobIDSize {
		return fmt.Errorf("blob_id is %d bytes, want %d", len(r.BlobId), BlobIDSize)
	}
	s := Status(r.DataStatus)
	if !s.Valid() {
		return fmt.Errorf("data_status %d, want 1..5", r.DataStatus)
	}
	if (r.DataSize > 0) != (s == StatusArchived) {
		return fmt.Errorf("data_size %d with status %s", r.DataSize, s)
	}
	return nil
}

// SameMessage reports whether a and b record the same message: same height,
// tx_index, msg_index, kind and tx bytes. A record PUT that finds a record of
// the same message is an idempotent rewrite.
func SameMessage(a, b *archivev1.FibreRecord) bool {
	return a.Height == b.Height && a.TxIndex == b.TxIndex && a.MsgIndex == b.MsgIndex &&
		a.Kind == b.Kind && bytes.Equal(a.Tx, b.Tx)
}
