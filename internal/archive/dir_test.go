package archive

import (
	"testing"
)

func TestDirSlot(t *testing.T) {
	for _, tc := range []struct{ h, bucket, off int64 }{
		{1, 0, 16},
		{10000, 0, 16 + 9999*24},
		{10001, 1, 16},
		{25000, 2, 16 + 4999*24},
	} {
		b, off := DirSlot(tc.h)
		if b != tc.bucket || off != tc.off {
			t.Errorf("DirSlot(%d) = (%d,%d), want (%d,%d)", tc.h, b, off, tc.bucket, tc.off)
		}
	}
	if got := DirKey(3); got != "dir/00000003.dir" {
		t.Fatalf("DirKey(3) = %q", got)
	}
}

func TestDirRoundTrip(t *testing.T) {
	var e [DirBucket]DirEntry
	// bucket 1 covers heights 10001..20000; slot i is height 10001+i.
	for i := 0; i < 100; i++ {
		e[i] = DirEntry{SegFirst: 9990, SegLast: 10100, RowStart: uint32(20 + i*3), RowCount: 3}
	}
	e[DirBucket-1] = DirEntry{SegFirst: 19000, SegLast: 20000, RowStart: 7, RowCount: 1}
	b := EncodeDir(1, &e)
	if len(b) != DirHeaderSize+DirBucket*DirEntrySize {
		t.Fatalf("encoded %d bytes", len(b))
	}
	bucket, got, err := DecodeDir(b)
	if err != nil {
		t.Fatal(err)
	}
	if bucket != 1 || *got != e {
		t.Fatalf("round trip mismatch: bucket %d", bucket)
	}
	// A single entry read by range matches.
	_, off := DirSlot(10001 + 50)
	one, err := DecodeDirEntry(b[off : off+DirEntrySize])
	if err != nil {
		t.Fatal(err)
	}
	if one != e[50] {
		t.Fatalf("entry %+v, want %+v", one, e[50])
	}
	// The zero entry decodes as not archived.
	_, off = DirSlot(10001 + 200)
	if z, err := DecodeDirEntry(b[off : off+DirEntrySize]); err != nil || z != (DirEntry{}) {
		t.Fatalf("zero entry: %+v, %v", z, err)
	}
}

func TestDirBadInput(t *testing.T) {
	var e [DirBucket]DirEntry
	good := EncodeDir(0, &e)
	bad := append([]byte(nil), good...)
	bad[0] = 'X'
	if _, _, err := DecodeDir(bad); err == nil {
		t.Fatal("bad magic accepted")
	}
	if _, _, err := DecodeDir(good[:len(good)-1]); err == nil {
		t.Fatal("short dir accepted")
	}
	// An entry whose segment does not cover its slot's height.
	e[5] = DirEntry{SegFirst: 100, SegLast: 200, RowStart: 0, RowCount: 1}
	if _, _, err := DecodeDir(EncodeDir(0, &e)); err == nil {
		t.Fatal("entry for height 6 pointing at segment 100-200 accepted")
	}
	if _, err := DecodeDirEntry(make([]byte, DirEntrySize-1)); err == nil {
		t.Fatal("short entry accepted")
	}
}

func TestDirEntryRejectsInconsistent(t *testing.T) {
	for name, e := range map[string]DirEntry{
		"rowcount 0 with segment":   {SegFirst: 1, SegLast: 10, RowStart: 0, RowCount: 0},
		"rowcount 0 with seg_last":  {SegLast: 10},
		"rowcount 0 with row_start": {RowStart: 4},
		"segment without first":     {SegFirst: 0, SegLast: 10, RowCount: 1},
		"first > last":              {SegFirst: 11, SegLast: 10, RowCount: 1},
		"negative first":            {SegFirst: -1, SegLast: 10, RowCount: 1},
		"row range overflow":        {SegFirst: 1, SegLast: 1, RowStart: 1<<32 - 1, RowCount: 2},
	} {
		var all [DirBucket]DirEntry
		all[0] = e
		b := EncodeDir(0, &all)
		if _, err := DecodeDirEntry(b[DirHeaderSize : DirHeaderSize+DirEntrySize]); err == nil {
			t.Errorf("%s: DecodeDirEntry accepted %+v", name, e)
		}
		if _, _, err := DecodeDir(b); err == nil {
			t.Errorf("%s: DecodeDir accepted %+v", name, e)
		}
	}
}
