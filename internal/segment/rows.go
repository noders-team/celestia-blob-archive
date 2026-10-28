package segment

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
)

// Row index (.idx2) layout, little-endian:
//
//	header: magic[8] | first u64 | last u64 | count u64
//	row:    height u64 | kind u8 | namespace[29] | commitment[32] |
//	        offset u64 | length u32 | reserved[6]=0
//
// Rows are sorted by (height, kind, namespace, offset). A height row has a
// zero namespace and commitment. A tombstone is a row of kind blob: a scrub
// replaces the record bytes in place and leaves the rows unchanged.
const (
	RowSize        = 88
	RowsHeaderSize = 32
	RowsMagic      = "CBAIDX2\x00"
)

const (
	namespaceSize  = 29
	commitmentSize = 32
)

// Row locates one record of a segment.
type Row struct {
	Height     uint64
	Kind       uint32
	Namespace  [namespaceSize]byte
	Commitment [commitmentSize]byte
	Offset     uint64
	Length     uint32
}

func compareRows(a, b Row) int {
	return cmp.Or(
		cmp.Compare(a.Height, b.Height),
		cmp.Compare(a.Kind, b.Kind),
		bytes.Compare(a.Namespace[:], b.Namespace[:]),
		cmp.Compare(a.Offset, b.Offset),
	)
}

// SortRows sorts rows by (Height, Kind, Namespace, Offset).
func SortRows(rows []Row) { slices.SortFunc(rows, compareRows) }

func checkRow(r Row) error {
	switch r.Kind {
	case KindHeight:
		if r.Namespace != ([namespaceSize]byte{}) || r.Commitment != ([commitmentSize]byte{}) {
			return fmt.Errorf("height row %d has a namespace or commitment", r.Height)
		}
	case KindBlob:
	default:
		return fmt.Errorf("row of height %d has unknown kind %d", r.Height, r.Kind)
	}
	if r.Offset+uint64(r.Length) < r.Offset {
		return fmt.Errorf("row of height %d: offset %d + length %d overflows", r.Height, r.Offset, r.Length)
	}
	return nil
}

// checkRows validates every row and requires them strictly sorted and within
// [first, last].
func checkRows(first, last uint64, rows []Row) error {
	if first > last {
		return fmt.Errorf("rows: first %d > last %d", first, last)
	}
	for i, r := range rows {
		if err := checkRow(r); err != nil {
			return fmt.Errorf("rows: row %d: %w", i, err)
		}
		if r.Height < first || r.Height > last {
			return fmt.Errorf("rows: row %d height %d outside [%d,%d]", i, r.Height, first, last)
		}
		if i > 0 && compareRows(rows[i-1], r) >= 0 {
			return fmt.Errorf("rows: row %d is not sorted after row %d", i, i-1)
		}
	}
	return nil
}

// EncodeRows returns the .idx2 bytes for the segment covering [first, last].
// The rows must be sorted (see SortRows) and lie within [first, last].
func EncodeRows(first, last uint64, rows []Row) ([]byte, error) {
	if err := checkRows(first, last, rows); err != nil {
		return nil, err
	}
	b := make([]byte, RowsHeaderSize, RowsHeaderSize+len(rows)*RowSize)
	copy(b, RowsMagic)
	binary.LittleEndian.PutUint64(b[8:], first)
	binary.LittleEndian.PutUint64(b[16:], last)
	binary.LittleEndian.PutUint64(b[24:], uint64(len(rows)))
	for _, r := range rows {
		b = appendRow(b, r)
	}
	return b, nil
}

func appendRow(b []byte, r Row) []byte {
	b = binary.LittleEndian.AppendUint64(b, r.Height)
	b = append(b, byte(r.Kind))
	b = append(b, r.Namespace[:]...)
	b = append(b, r.Commitment[:]...)
	b = binary.LittleEndian.AppendUint64(b, r.Offset)
	b = binary.LittleEndian.AppendUint32(b, r.Length)
	return append(b, make([]byte, 6)...)
}

// DecodeRowsHeader decodes the header at the start of b; b may be longer.
func DecodeRowsHeader(b []byte) (first, last, count uint64, err error) {
	if len(b) < RowsHeaderSize {
		return 0, 0, 0, fmt.Errorf("rows header: %d bytes, want %d", len(b), RowsHeaderSize)
	}
	if string(b[:8]) != RowsMagic {
		return 0, 0, 0, fmt.Errorf("rows header: bad magic %q", b[:8])
	}
	first = binary.LittleEndian.Uint64(b[8:])
	last = binary.LittleEndian.Uint64(b[16:])
	count = binary.LittleEndian.Uint64(b[24:])
	if first > last {
		return 0, 0, 0, fmt.Errorf("rows header: first %d > last %d", first, last)
	}
	return first, last, count, nil
}

// DecodeRow decodes exactly one row.
func DecodeRow(b []byte) (Row, error) {
	if len(b) != RowSize {
		return Row{}, fmt.Errorf("row: %d bytes, want %d", len(b), RowSize)
	}
	if !bytes.Equal(b[82:], make([]byte, 6)) {
		return Row{}, fmt.Errorf("row: non-zero reserved bytes")
	}
	r := Row{
		Height: binary.LittleEndian.Uint64(b[0:]),
		Kind:   uint32(b[8]),
		Offset: binary.LittleEndian.Uint64(b[70:]),
		Length: binary.LittleEndian.Uint32(b[78:]),
	}
	copy(r.Namespace[:], b[9:38])
	copy(r.Commitment[:], b[38:70])
	if err := checkRow(r); err != nil {
		return Row{}, fmt.Errorf("row: %w", err)
	}
	return r, nil
}

// DecodeRows decodes a whole .idx2 object and validates order and range.
func DecodeRows(b []byte) (first, last uint64, rows []Row, err error) {
	first, last, count, err := DecodeRowsHeader(b)
	if err != nil {
		return 0, 0, nil, err
	}
	body := b[RowsHeaderSize:]
	if len(body)%RowSize != 0 || uint64(len(body)/RowSize) != count {
		return 0, 0, nil, fmt.Errorf("rows: header count %d does not match %d body bytes", count, len(body))
	}
	rows = make([]Row, count)
	for i := range rows {
		if rows[i], err = DecodeRow(body[i*RowSize : (i+1)*RowSize]); err != nil {
			return 0, 0, nil, fmt.Errorf("rows: row %d: %w", i, err)
		}
	}
	if err := checkRows(first, last, rows); err != nil {
		return 0, 0, nil, err
	}
	return first, last, rows, nil
}

// ScanRows walks the frames of a segment and returns its rows, sorted. It is
// used where no .idx2 exists; tombstone records yield rows of kind blob.
func ScanRows(seg []byte) ([]Row, error) {
	var rows []Row
	for pos := 0; pos < len(seg); {
		n, k := binary.Uvarint(seg[pos:])
		if k <= 0 {
			return nil, fmt.Errorf("scan rows: bad frame length at %d", pos)
		}
		off := uint64(pos + k)
		if n > uint64(len(seg))-off || n > 1<<32-1 {
			return nil, fmt.Errorf("scan rows: frame at %d of %d bytes exceeds segment of %d bytes", pos, n, len(seg))
		}
		rec := &archivev1.Record{}
		if err := proto.Unmarshal(seg[off:off+n], rec); err != nil {
			return nil, fmt.Errorf("scan rows: decode record at %d: %w", off, err)
		}
		r := Row{Offset: off, Length: uint32(n)}
		var ns, cm []byte
		switch v := rec.Kind.(type) {
		case *archivev1.Record_Height:
			r.Height, r.Kind = v.Height.GetHeight(), KindHeight
		case *archivev1.Record_Blob:
			r.Height, r.Kind = v.Blob.GetHeight(), KindBlob
			ns, cm = v.Blob.GetNamespace(), v.Blob.GetCommitment()
		case *archivev1.Record_Tombstone:
			r.Height, r.Kind = v.Tombstone.GetHeight(), KindBlob
			ns, cm = v.Tombstone.GetNamespace(), v.Tombstone.GetCommitment()
		default:
			return nil, fmt.Errorf("scan rows: empty record at %d", off)
		}
		if r.Kind == KindBlob {
			if len(ns) != namespaceSize || len(cm) != commitmentSize {
				return nil, fmt.Errorf("scan rows: record at %d: namespace %d bytes, commitment %d bytes", off, len(ns), len(cm))
			}
			copy(r.Namespace[:], ns)
			copy(r.Commitment[:], cm)
		}
		rows = append(rows, r)
		pos = int(off + n)
	}
	SortRows(rows)
	return rows, nil
}
