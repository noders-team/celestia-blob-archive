// Package segment implements the segment framing of the archive: each record
// is an unsigned varint length followed by the protobuf bytes of a Record.
package segment

import (
	"encoding/binary"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
)

// Record kinds as stored in IndexEntry.Kind.
const (
	KindHeight uint32 = 1
	KindBlob   uint32 = 2
)

// Writer accumulates framed records in memory.
type Writer struct {
	buf []byte
}

// Append frames r and returns the offset and length of its protobuf bytes
// (i.e. after the length varint) within the segment.
func (w *Writer) Append(r *archivev1.Record) (offset uint64, length uint32, err error) {
	b, err := proto.Marshal(r)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal record: %w", err)
	}
	if uint64(len(b)) > math.MaxUint32 {
		return 0, 0, fmt.Errorf("record too large: %d bytes", len(b))
	}
	w.buf = binary.AppendUvarint(w.buf, uint64(len(b)))
	offset = uint64(len(w.buf))
	w.buf = append(w.buf, b...)
	return offset, uint32(len(b)), nil
}

// Bytes returns the segment contents. The slice is owned by the Writer.
func (w *Writer) Bytes() []byte { return w.buf }

// Size returns the current segment size in bytes.
func (w *Writer) Size() int { return len(w.buf) }

// ReadAt decodes the Record whose protobuf bytes occupy seg[off:off+n].
func ReadAt(seg []byte, off uint64, n uint32) (*archivev1.Record, error) {
	end := off + uint64(n)
	if end < off || end > uint64(len(seg)) {
		return nil, fmt.Errorf("record range [%d,+%d) out of bounds of segment of %d bytes", off, n, len(seg))
	}
	r := &archivev1.Record{}
	if err := proto.Unmarshal(seg[off:end], r); err != nil {
		return nil, fmt.Errorf("decode record at %d: %w", off, err)
	}
	return r, nil
}
