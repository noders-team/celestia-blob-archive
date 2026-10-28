package segment_test

import (
	"reflect"
	"testing"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
)

func blobRow(h uint64, ns, c byte, off uint64) segment.Row {
	r := segment.Row{Height: h, Kind: segment.KindBlob, Offset: off, Length: 10}
	r.Namespace[28] = ns
	r.Commitment[0] = c
	return r
}

func sortedRows() []segment.Row {
	return []segment.Row{
		{Height: 5, Kind: segment.KindHeight, Offset: 1, Length: 3},
		blobRow(5, 1, 9, 40),
		blobRow(5, 1, 2, 60),
		blobRow(5, 2, 1, 20),
		{Height: 6, Kind: segment.KindHeight, Offset: 80, Length: 3},
		{Height: 7, Kind: segment.KindHeight, Offset: 90, Length: 3},
		blobRow(7, 0, 1, 100),
	}
}

func TestRowsRoundTrip(t *testing.T) {
	rows := sortedRows()
	b, err := segment.EncodeRows(5, 8, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != segment.RowsHeaderSize+len(rows)*segment.RowSize {
		t.Fatalf("encoded %d bytes", len(b))
	}
	first, last, got, err := segment.DecodeRows(b)
	if err != nil {
		t.Fatal(err)
	}
	if first != 5 || last != 8 || !reflect.DeepEqual(got, rows) {
		t.Fatalf("round trip: [%d,%d] %+v", first, last, got)
	}
	f, l, n, err := segment.DecodeRowsHeader(b[:segment.RowsHeaderSize])
	if err != nil || f != 5 || l != 8 || n != uint64(len(rows)) {
		t.Fatalf("header: %d %d %d %v", f, l, n, err)
	}
	off := segment.RowsHeaderSize + 3*segment.RowSize
	r, err := segment.DecodeRow(b[off : off+segment.RowSize])
	if err != nil || r != rows[3] {
		t.Fatalf("row 3: %+v %v", r, err)
	}
	// Empty index of a segment.
	b, err = segment.EncodeRows(1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, got, err := segment.DecodeRows(b); err != nil || len(got) != 0 {
		t.Fatalf("empty: %v %v", got, err)
	}
}

func TestRowsRejectUnsorted(t *testing.T) {
	swap := func(i, j int) []segment.Row {
		r := sortedRows()
		r[i], r[j] = r[j], r[i]
		return r
	}
	dup := sortedRows()
	dup = append(dup[:2], dup[1:]...)
	heightWithNS := sortedRows()
	heightWithNS[0].Namespace[0] = 1
	badKind := sortedRows()
	badKind[1].Kind = 3
	for name, tc := range map[string]struct {
		first, last uint64
		rows        []segment.Row
	}{
		"height order":      {5, 8, swap(3, 4)},
		"kind order":        {5, 8, swap(0, 1)},
		"namespace order":   {5, 8, swap(2, 3)},
		"offset order":      {5, 8, swap(1, 2)},
		"duplicate":         {5, 8, dup},
		"below first":       {6, 8, sortedRows()},
		"above last":        {5, 6, sortedRows()},
		"first > last":      {8, 5, nil},
		"height row has ns": {5, 8, heightWithNS},
		"unknown kind":      {5, 8, badKind},
	} {
		if _, err := segment.EncodeRows(tc.first, tc.last, tc.rows); err == nil {
			t.Errorf("%s: EncodeRows accepted", name)
		}
	}
	// The decoder applies the same checks to bytes it did not produce: write
	// a valid index, then swap two rows in place.
	b, err := segment.EncodeRows(5, 8, sortedRows())
	if err != nil {
		t.Fatal(err)
	}
	r1 := segment.RowsHeaderSize + segment.RowSize
	tmp := append([]byte(nil), b[r1:r1+segment.RowSize]...)
	copy(b[r1:], b[r1+segment.RowSize:r1+2*segment.RowSize])
	copy(b[r1+segment.RowSize:], tmp)
	if _, _, _, err := segment.DecodeRows(b); err == nil {
		t.Fatal("DecodeRows accepted unsorted rows")
	}
}

func TestRowsBadMagic(t *testing.T) {
	good, err := segment.EncodeRows(5, 8, sortedRows())
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), good...)
	bad[0] = 'X'
	if _, _, _, err := segment.DecodeRowsHeader(bad); err == nil {
		t.Fatal("header with bad magic accepted")
	}
	if _, _, _, err := segment.DecodeRows(bad); err == nil {
		t.Fatal("rows with bad magic accepted")
	}
	if _, _, _, err := segment.DecodeRowsHeader(good[:segment.RowsHeaderSize-1]); err == nil {
		t.Fatal("short header accepted")
	}
	if _, _, _, err := segment.DecodeRows(good[:len(good)-1]); err == nil {
		t.Fatal("truncated rows accepted")
	}
	if _, _, _, err := segment.DecodeRows(good[:len(good)-segment.RowSize]); err == nil {
		t.Fatal("count mismatch accepted")
	}
	reserved := append([]byte(nil), good...)
	reserved[segment.RowsHeaderSize+segment.RowSize-1] = 1
	if _, _, _, err := segment.DecodeRows(reserved); err == nil {
		t.Fatal("non-zero reserved byte accepted")
	}
}

func TestScanRowsMatchesBuilder(t *testing.T) {
	ns := func(b byte) []byte { n := make([]byte, 29); n[28] = b; return n }
	cm := func(b byte) []byte { c := make([]byte, 32); c[0] = b; return c }
	blob := func(h uint64, n, c byte) *archivev1.Record {
		return &archivev1.Record{Kind: &archivev1.Record_Blob{Blob: &archivev1.BlobRecord{
			Height: h, Namespace: ns(n), Commitment: cm(c), Data: []byte("data"),
		}}}
	}
	height := func(h uint64) *archivev1.Record {
		return &archivev1.Record{Kind: &archivev1.Record_Height{Height: &archivev1.HeightRecord{Height: h}}}
	}
	// Blobs in square order, which is not namespace order within a height.
	recs := []*archivev1.Record{
		height(10), blob(10, 3, 1), blob(10, 1, 2), blob(10, 1, 3),
		height(11),
		height(12), blob(12, 2, 4),
	}
	w := &segment.Writer{}
	var want []segment.Row
	for _, r := range recs {
		off, n, err := w.Append(r)
		if err != nil {
			t.Fatal(err)
		}
		row := segment.Row{Offset: off, Length: n}
		switch k := r.Kind.(type) {
		case *archivev1.Record_Height:
			row.Height, row.Kind = k.Height.Height, segment.KindHeight
		case *archivev1.Record_Blob:
			row.Height, row.Kind = k.Blob.Height, segment.KindBlob
			copy(row.Namespace[:], k.Blob.Namespace)
			copy(row.Commitment[:], k.Blob.Commitment)
		}
		want = append(want, row)
	}
	segment.SortRows(want)
	if _, err := segment.EncodeRows(10, 12, want); err != nil {
		t.Fatalf("sorted rows rejected: %v", err)
	}

	seg := append([]byte(nil), w.Bytes()...)
	got, err := segment.ScanRows(seg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanRows:\n got %+v\nwant %+v", got, want)
	}

	// A scrubbed blob keeps its row.
	var r segment.Row
	for _, x := range want {
		if x.Kind == segment.KindBlob && x.Commitment[0] == 2 {
			r = x
		}
	}
	tomb, err := segment.TombstoneFor(blob(10, 1, 2).GetBlob(), r.Length)
	if err != nil {
		t.Fatal(err)
	}
	copy(seg[r.Offset:], tomb)
	if got, err = segment.ScanRows(seg); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanRows after scrub:\n got %+v\nwant %+v", got, want)
	}

	if _, err := segment.ScanRows(seg[:len(seg)-1]); err == nil {
		t.Fatal("truncated segment accepted")
	}
}
