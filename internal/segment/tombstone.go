package segment

import (
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
)

var (
	// ErrTombstoneTooLarge means the replaced record is shorter than the
	// smallest tombstone (no padding, no filler).
	ErrTombstoneTooLarge = errors.New("segment: tombstone does not fit the record length")
	// ErrTombstoneUnreachable means no canonical tombstone has the requested
	// length. The only such length at or above the minimum is minimum+1,
	// since every added field costs at least two bytes; a blob record is
	// always far longer than that.
	ErrTombstoneUnreachable = errors.New("segment: no canonical tombstone of the record length")
)

// Field 3 (Record.tombstone) and 15 (Record.filler, TombstoneRecord.padding)
// all have one-byte tags.
const tagSize = 1

// TombstoneFor returns the canonical (deterministic proto) bytes of a Record
// holding a TombstoneRecord for b that are exactly length bytes long, so it
// can overwrite b's record in place. The result depends only on b's height,
// namespace, commitment, share range and on length; padding is zero bytes.
//
// The length is matched with TombstoneRecord.padding and, where padding
// alone cannot hit it (the Record's own length prefix skips a length at every
// varint boundary), with Record.filler. The smallest filler is preferred, then
// the smallest padding.
func TombstoneFor(b *archivev1.BlobRecord, length uint32) ([]byte, error) {
	t := &archivev1.TombstoneRecord{
		Height: b.GetHeight(), Namespace: b.GetNamespace(), Commitment: b.GetCommitment(),
		ShareStart: b.GetShareStart(), ShareEnd: b.GetShareEnd(),
	}
	base := proto.Size(t)
	if int(length) < tagSize+protowire.SizeVarint(uint64(base))+base {
		return nil, fmt.Errorf("%w: %d bytes", ErrTombstoneTooLarge, length)
	}
	for _, filler := range fillers {
		fsize := 0
		if filler > 0 {
			fsize = tagSize + protowire.SizeVarint(filler)
		}
		inner, ok := sizeWithPrefix(int(length) - fsize - tagSize)
		if !ok || inner < base {
			continue
		}
		p, ok := paddingFor(inner - base)
		if !ok {
			continue
		}
		t.Padding = make([]byte, p)
		rec := &archivev1.Record{Kind: &archivev1.Record_Tombstone{Tombstone: t}, Filler: filler}
		out, err := proto.MarshalOptions{Deterministic: true}.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("marshal tombstone: %w", err)
		}
		if len(out) != int(length) {
			return nil, fmt.Errorf("segment: tombstone is %d bytes, want %d", len(out), length)
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: %d bytes", ErrTombstoneUnreachable, length)
}

// fillers lists Record.filler candidates in increasing order: absent (0),
// then the smallest value whose varint is k bytes, for k = 1..10.
var fillers = func() []uint64 {
	f := []uint64{0, 1}
	for k := 2; k <= binary.MaxVarintLen64; k++ {
		f = append(f, 1<<(7*(k-1)))
	}
	return f
}()

// sizeWithPrefix returns the n for which varint(n) followed by n bytes takes
// exactly total bytes.
func sizeWithPrefix(total int) (int, bool) {
	for k := 1; k <= binary.MaxVarintLen64; k++ {
		if n := total - k; n >= 0 && protowire.SizeVarint(uint64(n)) == k {
			return n, true
		}
	}
	return 0, false
}

// paddingFor returns the padding length p for which the padding field takes
// exactly extra bytes (0 means no padding field).
func paddingFor(extra int) (int, bool) {
	if extra == 0 {
		return 0, true
	}
	p, ok := sizeWithPrefix(extra - tagSize)
	return p, ok && p > 0
}
