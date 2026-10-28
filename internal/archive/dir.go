package archive

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Height directory (dir/NNNNNNNN.dir) layout, little-endian:
//
//	header: magic[8] | bucket u64
//	entry:  seg_first u64 | seg_last u64 | row_start u32 | row_count u32
//
// Bucket b holds DirBucket entries for heights b*DirBucket+1 ..
// (b+1)*DirBucket. An entry points at the segment covering its height and at
// that height's rows in the segment's .idx2; the zero entry means not
// archived.
const (
	DirBucket     = 10000
	DirEntrySize  = 24
	DirHeaderSize = 16
	DirMagic      = "CBADIR2\x00"
)

// DirEntry is one directory slot. The zero value means the height is not
// archived.
type DirEntry struct {
	SegFirst, SegLast  int64
	RowStart, RowCount uint32
}

// DirSlot returns the bucket of height h (h >= 1) and the byte offset of its
// entry within the bucket object.
func DirSlot(height int64) (bucket, offset int64) {
	return (height - 1) / DirBucket, DirHeaderSize + ((height-1)%DirBucket)*DirEntrySize
}

func (e DirEntry) check() error {
	if e.RowCount == 0 {
		if e != (DirEntry{}) {
			return fmt.Errorf("dir entry %+v: no rows but not empty", e)
		}
		return nil
	}
	if e.SegFirst < 1 || e.SegFirst > e.SegLast {
		return fmt.Errorf("dir entry %+v: bad segment range", e)
	}
	if uint64(e.RowStart)+uint64(e.RowCount) > math.MaxUint32 {
		return fmt.Errorf("dir entry %+v: row range overflows", e)
	}
	return nil
}

// EncodeDir returns the bytes of directory bucket b.
func EncodeDir(bucket int64, e *[DirBucket]DirEntry) []byte {
	b := make([]byte, DirHeaderSize, DirHeaderSize+DirBucket*DirEntrySize)
	copy(b, DirMagic)
	binary.LittleEndian.PutUint64(b[8:], uint64(bucket))
	for _, d := range e {
		b = binary.LittleEndian.AppendUint64(b, uint64(d.SegFirst))
		b = binary.LittleEndian.AppendUint64(b, uint64(d.SegLast))
		b = binary.LittleEndian.AppendUint32(b, d.RowStart)
		b = binary.LittleEndian.AppendUint32(b, d.RowCount)
	}
	return b
}

// DecodeDirEntry decodes one entry (e.g. from a range read at DirSlot).
func DecodeDirEntry(b []byte) (DirEntry, error) {
	if len(b) != DirEntrySize {
		return DirEntry{}, fmt.Errorf("dir entry: %d bytes, want %d", len(b), DirEntrySize)
	}
	first, last := binary.LittleEndian.Uint64(b[0:]), binary.LittleEndian.Uint64(b[8:])
	if first > math.MaxInt64 || last > math.MaxInt64 {
		return DirEntry{}, fmt.Errorf("dir entry: segment range [%d,%d] out of range", first, last)
	}
	e := DirEntry{
		SegFirst: int64(first), SegLast: int64(last),
		RowStart: binary.LittleEndian.Uint32(b[16:]), RowCount: binary.LittleEndian.Uint32(b[20:]),
	}
	if err := e.check(); err != nil {
		return DirEntry{}, err
	}
	return e, nil
}

// DecodeDir decodes a whole directory bucket. Every non-empty entry must
// point at a segment that covers its slot's height.
func DecodeDir(b []byte) (int64, *[DirBucket]DirEntry, error) {
	if len(b) != DirHeaderSize+DirBucket*DirEntrySize {
		return 0, nil, fmt.Errorf("dir: %d bytes, want %d", len(b), DirHeaderSize+DirBucket*DirEntrySize)
	}
	if string(b[:8]) != DirMagic {
		return 0, nil, fmt.Errorf("dir: bad magic %q", b[:8])
	}
	bucket := binary.LittleEndian.Uint64(b[8:])
	if bucket > (math.MaxInt64-DirBucket)/DirBucket {
		return 0, nil, fmt.Errorf("dir: bucket %d out of range", bucket)
	}
	var e [DirBucket]DirEntry
	for i := range e {
		off := DirHeaderSize + i*DirEntrySize
		d, err := DecodeDirEntry(b[off : off+DirEntrySize])
		if err != nil {
			return 0, nil, fmt.Errorf("dir bucket %d slot %d: %w", bucket, i, err)
		}
		h := int64(bucket)*DirBucket + int64(i) + 1
		if d.RowCount != 0 && (h < d.SegFirst || h > d.SegLast) {
			return 0, nil, fmt.Errorf("dir bucket %d: height %d points at segment [%d,%d]", bucket, h, d.SegFirst, d.SegLast)
		}
		e[i] = d
	}
	return int64(bucket), &e, nil
}
