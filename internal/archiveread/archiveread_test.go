package archiveread

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// countingStore counts full Gets per key prefix (up to and including the
// first '/') and range reads with their bytes.
type countingStore struct {
	store.Store
	mu         sync.Mutex
	gets       map[string]int
	ranges     int
	rangeBytes int64
}

func newCounting(st store.Store) *countingStore {
	return &countingStore{Store: st, gets: map[string]int{}}
}

func keyPrefix(key string) string {
	if i := strings.IndexByte(key, '/'); i >= 0 {
		return key[:i+1]
	}
	return key
}

func (c *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	c.gets[keyPrefix(key)]++
	c.mu.Unlock()
	return c.Store.Get(ctx, key)
}

func (c *countingStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.mu.Lock()
	c.ranges++
	c.rangeBytes += n
	c.mu.Unlock()
	return c.Store.GetRange(ctx, key, off, n)
}

func (c *countingStore) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets, c.ranges, c.rangeBytes = map[string]int{}, 0, 0
}

// blobsOf returns the blob records of recs (tombstones are skipped).
func blobsOf(recs []*archivev1.Record) []*archivev1.BlobRecord {
	var out []*archivev1.BlobRecord
	for _, r := range recs {
		if b := r.GetBlob(); b != nil {
			out = append(out, b)
		}
	}
	return out
}

func TestReadArchive(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	if _, err := LoadManifest(ctx, st); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("empty store manifest: %v", err)
	}
	blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
	archivetest.Build(t, st, blocks, 1, 7, 3, nil)

	m, err := LoadManifest(ctx, st)
	if err != nil || m.ContiguousUntil != 7 {
		t.Fatalf("manifest %+v err %v", m, err)
	}
	refs, err := LoadCatalog(ctx, st)
	if err != nil || len(refs) != 3 {
		t.Fatalf("catalog %v err %v", refs, err)
	}
	for i := 1; i < len(refs); i++ {
		if refs[i].First <= refs[i-1].First {
			t.Fatal("catalog not sorted")
		}
	}

	for h := int64(1); h <= 7; h++ {
		e, err := Locate(ctx, st, h)
		if err != nil || e.SegFirst > h || e.SegLast < h {
			t.Fatalf("Locate(%d) = %+v, %v", h, e, err)
		}
		hr, recs, err := ReadHeight(ctx, st, h)
		if err != nil {
			t.Fatalf("ReadHeight(%d): %v", h, err)
		}
		blobs := blobsOf(recs)
		if int64(hr.Height) != h || int(hr.BlobCount) != len(blobs) || len(blobs) != len(recs) {
			t.Fatalf("height %d: record %d blobs %d/%d", h, hr.Height, hr.BlobCount, len(blobs))
		}
		if (h == 3) != (len(blobs) > 0) {
			t.Fatalf("height %d: %d blobs", h, len(blobs))
		}
		for i, b := range blobs {
			if int64(b.Height) != h || len(b.ShareProof) == 0 {
				t.Fatalf("bad blob %+v", b)
			}
			if p := blobs[max(i-1, 0)]; i > 0 && (b.TxIndex < p.TxIndex || b.TxIndex == p.TxIndex && b.BlobIndex <= p.BlobIndex) {
				t.Fatalf("height %d: blobs not in chain order", h)
			}
			gotHR, rec, err := ReadBlob(ctx, st, h, b.Namespace, b.Commitment)
			if err != nil {
				t.Fatalf("ReadBlob(%d, blob %d): %v", h, i, err)
			}
			if !proto.Equal(gotHR, hr) || !proto.Equal(rec.GetBlob(), b) {
				t.Fatalf("ReadBlob(%d, blob %d) differs from ReadHeight", h, i)
			}
		}
	}
	if len(blobsOf(mustReadHeight(t, st, 3))) < 2 {
		t.Fatal("fixture 1834013 should carry at least 2 blobs")
	}
	b3 := blobsOf(mustReadHeight(t, st, 3))[0]
	otherCommitment := bytes.Repeat([]byte{0xee}, 32)
	if _, _, err := ReadBlob(ctx, st, 3, b3.Namespace, otherCommitment); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("ReadBlob of an unknown commitment = %v, want ErrBlobNotFound", err)
	}
	if _, _, err := ReadBlob(ctx, st, 2, b3.Namespace, b3.Commitment); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("ReadBlob at an empty height = %v, want ErrBlobNotFound", err)
	}
	for _, h := range []int64{-1, 0, 8, 100} {
		if _, err := Locate(ctx, st, h); !errors.Is(err, ErrNotArchived) {
			t.Fatalf("Locate(%d) = %v, want ErrNotArchived", h, err)
		}
		if _, _, err := ReadHeight(ctx, st, h); !errors.Is(err, ErrNotArchived) {
			t.Fatalf("ReadHeight(%d) = %v, want ErrNotArchived", h, err)
		}
	}
}

func mustReadHeight(t *testing.T, st store.Store, h int64) []*archivev1.Record {
	t.Helper()
	_, recs, err := ReadHeight(context.Background(), st, h)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func TestReadBlobRangeReadsOnly(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
	archivetest.Build(t, fs, blocks, 1, 7, 3, nil)
	b := blobsOf(mustReadHeight(t, fs, 3))[1]

	st := newCounting(fs)
	hr, rec, err := ReadBlob(ctx, st, 3, b.Namespace, b.Commitment)
	if err != nil {
		t.Fatal(err)
	}
	if hr.Height != 3 || !proto.Equal(rec.GetBlob(), b) {
		t.Fatalf("ReadBlob returned height %d, blob %v", hr.Height, rec.GetBlob() != nil)
	}
	if st.gets["segments/"] != 0 || st.gets["dir/"] != 0 {
		t.Fatalf("full Gets: %v", st.gets)
	}
	if st.ranges != 4 {
		t.Fatalf("%d range reads, want 4", st.ranges)
	}
}

// synthBlob is a blob record of height h with namespace ns; i makes its
// commitment unique.
func synthBlob(h int64, ns [29]byte, i int) *archivev1.BlobRecord {
	c := sha256.Sum256([]byte(fmt.Sprintf("%d/%d", h, i)))
	return &archivev1.BlobRecord{
		Height: uint64(h), BlobIndex: uint32(i), Namespace: ns[:], Commitment: c[:],
		Data: []byte(fmt.Sprintf("blob %d of height %d", i, h)), ShareStart: uint32(i), ShareEnd: uint32(i + 1),
	}
}

func nsOf(v uint32) [29]byte {
	var ns [29]byte
	binary.BigEndian.PutUint32(ns[25:], v)
	return ns
}

// synthArchive commits one segment per element of segs; blobs maps a height
// to its blob records (in chain order). It needs no chain fixtures: the read
// side never checks proofs.
func synthArchive(t *testing.T, st store.Store, segs [][2]int64, blobs map[int64][]*archivev1.BlobRecord) {
	t.Helper()
	ctx := context.Background()
	c, err := archive.NewCommitter(ctx, st, archivetest.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range segs {
		b := archive.NewBuilder(nil)
		for h := s[0]; h <= s[1]; h++ {
			hr := &archivev1.HeightRecord{Height: uint64(h), BlobCount: uint32(len(blobs[h]))}
			if err := b.Add(hr, blobs[h]); err != nil {
				t.Fatal(err)
			}
		}
		ref, err := b.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Commit(ctx, *ref); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadBlobBinarySearch(t *testing.T) {
	ctx := context.Background()
	const n = 5000
	blobs := map[int64][]*archivev1.BlobRecord{}
	for i := 0; i < n; i++ {
		blobs[5] = append(blobs[5], synthBlob(5, nsOf(uint32(i)), i))
	}
	// Height 6: namespace runs of 3 in shuffled chain order, so row order
	// differs from offset order and the commitment needs a run scan.
	for i := 0; i < 2000; i++ {
		blobs[6] = append(blobs[6], synthBlob(6, nsOf(uint32((i*7919)%2000/3)), i))
	}
	fs := store.NewFS(t.TempDir())
	synthArchive(t, fs, [][2]int64{{4, 7}}, blobs)
	st := newCounting(fs)

	last := blobs[5][n-1]
	hr, rec, err := ReadBlob(ctx, st, 5, last.Namespace, last.Commitment)
	if err != nil {
		t.Fatal(err)
	}
	if hr.Height != 5 || !proto.Equal(rec.GetBlob(), last) {
		t.Fatal("ReadBlob returned another blob")
	}
	if limit := 3 + bits.Len(n-1) + 1; st.ranges > limit {
		t.Fatalf("%d range reads, want <= %d", st.ranges, limit)
	}
	if len(st.gets) != 0 {
		t.Fatalf("full Gets: %v", st.gets)
	}

	for _, h := range []int64{5, 6} {
		for _, i := range []int{0, 1, 2, 3, 999, 1000, 1001, 1997, 1998, 1999} {
			b := blobs[h][i]
			if _, rec, err := ReadBlob(ctx, st, h, b.Namespace, b.Commitment); err != nil || !proto.Equal(rec.GetBlob(), b) {
				t.Fatalf("ReadBlob(%d, %d): %v", h, i, err)
			}
		}
		missing := bytes.Repeat([]byte{0xee}, 32)
		for _, ns := range [][29]byte{nsOf(0), nsOf(500), nsOf(1 << 30)} {
			if _, _, err := ReadBlob(ctx, st, h, ns[:], missing); !errors.Is(err, ErrBlobNotFound) {
				t.Fatalf("ReadBlob(%d, missing) = %v, want ErrBlobNotFound", h, err)
			}
		}
		_, recs, err := ReadHeight(ctx, st, h)
		if err != nil {
			t.Fatal(err)
		}
		got := blobsOf(recs)
		if len(got) != len(blobs[h]) {
			t.Fatalf("ReadHeight(%d): %d blobs, want %d", h, len(got), len(blobs[h]))
		}
		for i := range got {
			if !proto.Equal(got[i], blobs[h][i]) {
				t.Fatalf("ReadHeight(%d): blob %d not in chain order", h, i)
			}
		}
	}
}

func TestLocateNotArchived(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	synthArchive(t, st, [][2]int64{{1, 3}, {6, 7}}, nil)
	for _, h := range []int64{0, 4, 5, 8, archive.DirBucket + 1, 1 << 40} {
		_, err := Locate(ctx, st, h)
		if !errors.Is(err, ErrNotArchived) || errors.Is(err, store.ErrNotFound) {
			t.Fatalf("Locate(%d) = %v, want ErrNotArchived and not store.ErrNotFound", h, err)
		}
	}
	for _, h := range []int64{1, 3, 6, 7} {
		if _, err := Locate(ctx, st, h); err != nil {
			t.Fatalf("Locate(%d): %v", h, err)
		}
	}
}

func TestBucketBoundaryHeights(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	blobs := map[int64][]*archivev1.BlobRecord{}
	for h := int64(9999); h <= 10002; h++ {
		blobs[h] = []*archivev1.BlobRecord{synthBlob(h, nsOf(uint32(h)), 0), synthBlob(h, nsOf(1), 1)}
	}
	synthArchive(t, st, [][2]int64{{9999, 10002}}, blobs)
	for h := int64(9999); h <= 10002; h++ {
		e, err := Locate(ctx, st, h)
		if err != nil || e.SegFirst != 9999 || e.SegLast != 10002 {
			t.Fatalf("Locate(%d) = %+v, %v", h, e, err)
		}
		hr, recs, err := ReadHeight(ctx, st, h)
		if err != nil || int64(hr.Height) != h || len(recs) != 2 {
			t.Fatalf("ReadHeight(%d): %v", h, err)
		}
		for _, b := range blobs[h] {
			if _, rec, err := ReadBlob(ctx, st, h, b.Namespace, b.Commitment); err != nil || !proto.Equal(rec.GetBlob(), b) {
				t.Fatalf("ReadBlob(%d): %v", h, err)
			}
		}
	}
}

// patch rewrites the object at key in place.
func patch(t *testing.T, st store.Store, key string, edit func([]byte)) {
	t.Helper()
	ctx := context.Background()
	b, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	edit(b)
	if err := st.Put(ctx, key, b); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptPointers(t *testing.T) {
	const h = 3 // one blob; segment [1,4]
	idxKey := archive.IndexKey(1, 4)
	dirKey := archive.DirKey(0)
	_, slot := archive.DirSlot(h)
	// Rows of the segment: heights 1,2 one row each, height 3 rows 2..3,
	// height 4 row 4. Row r starts at RowsHeaderSize + r*RowSize.
	rowAt := func(r int) int { return segment.RowsHeaderSize + r*segment.RowSize }
	const blobRow = 3
	cases := map[string]func(t *testing.T, st store.Store, segSize int64){
		"RowStart past the rows": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) { binary.LittleEndian.PutUint32(b[slot+16:], 9) })
		},
		"RowCount past the rows": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) { binary.LittleEndian.PutUint32(b[slot+20:], 4) })
		},
		"RowCount short of the rows": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) { binary.LittleEndian.PutUint32(b[slot+20:], 1) })
		},
		"row offset beyond .seg": func(t *testing.T, st store.Store, segSize int64) {
			patch(t, st, idxKey, func(b []byte) { binary.LittleEndian.PutUint64(b[rowAt(blobRow)+70:], uint64(segSize)+10) })
		},
		"row offset beyond int64": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, idxKey, func(b []byte) { binary.LittleEndian.PutUint64(b[rowAt(blobRow)+70:], math.MaxInt64) })
		},
		"row of another height": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, idxKey, func(b []byte) { binary.LittleEndian.PutUint64(b[rowAt(blobRow):], 4) })
		},
		"row points at another record": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, idxKey, func(b []byte) {
				copy(b[rowAt(blobRow)+70:rowAt(blobRow)+82], b[rowAt(blobRow-1)+70:rowAt(blobRow-1)+82])
			})
		},
		"rows of the height not led by its height row": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) {
				binary.LittleEndian.PutUint32(b[slot+16:], blobRow)
				binary.LittleEndian.PutUint32(b[slot+20:], 1)
			})
		},
		"dir entry pointing at a missing .idx2": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) { binary.LittleEndian.PutUint64(b[slot+8:], 5) })
		},
		"dir entry pointing at a segment not covering the height": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) { binary.LittleEndian.PutUint64(b[slot:], 4) })
		},
		"dir entry not decodable": func(t *testing.T, st store.Store, _ int64) {
			patch(t, st, dirKey, func(b []byte) { binary.LittleEndian.PutUint32(b[slot+20:], 0) })
		},
		"truncated dir object": func(t *testing.T, st store.Store, _ int64) {
			b, err := st.Get(context.Background(), dirKey)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Put(context.Background(), dirKey, b[:slot+10]); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := store.NewFS(t.TempDir())
			b := synthBlob(h, nsOf(7), 0)
			synthArchive(t, st, [][2]int64{{1, 4}}, map[int64][]*archivev1.BlobRecord{h: {b}})
			rows, err := LoadRows(ctx, st, mustLocate(t, st, h), h)
			if err != nil || len(rows) != 2 || rows[1].Offset != mustRowOffset(t, st, idxKey, blobRow) {
				t.Fatalf("layout assumption: rows %+v err %v", rows, err)
			}
			seg, err := st.Get(ctx, archive.SegmentKey(1, 4))
			if err != nil {
				t.Fatal(err)
			}
			corrupt(t, st, int64(len(seg)))
			if _, _, err := ReadBlob(ctx, st, h, b.Namespace, b.Commitment); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadBlob = %v, want ErrCorrupt", err)
			}
			if _, _, err := ReadHeight(ctx, st, h); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadHeight = %v, want ErrCorrupt", err)
			}
		})
	}
}

// TestReadHeightSpanCapped: rows that spread one height over more than
// maxHeightSpan bytes are corrupt and never turn into a range read that size.
func TestReadHeightSpanCapped(t *testing.T) {
	const h, blobRow = 3, 3 // one blob; segment [1,4], see TestCorruptPointers
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	b := synthBlob(h, nsOf(7), 0)
	synthArchive(t, fs, [][2]int64{{1, 4}}, map[int64][]*archivev1.BlobRecord{h: {b}})
	idxKey := archive.IndexKey(1, 4)
	rowAt := segment.RowsHeaderSize + blobRow*segment.RowSize
	patch(t, fs, idxKey, func(b []byte) { binary.LittleEndian.PutUint64(b[rowAt+70:], maxHeightSpan+1) })
	st := newCounting(fs)
	if _, _, err := ReadHeight(ctx, st, h); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadHeight = %v, want ErrCorrupt", err)
	}
	if st.rangeBytes > maxHeightSpan {
		t.Fatalf("ReadHeight requested %d bytes, cap is %d", st.rangeBytes, maxHeightSpan)
	}
}

// TestShortRowGroupBinarySearch: a directory entry that drops the last row of
// a searched (> 64 KiB) group must not turn the hidden blob into "not found".
func TestShortRowGroupBinarySearch(t *testing.T) {
	ctx := context.Background()
	const h, n = 2, 1000
	blobs := map[int64][]*archivev1.BlobRecord{}
	for i := 0; i < n; i++ {
		blobs[h] = append(blobs[h], synthBlob(h, nsOf(uint32(i)), i))
	}
	st := store.NewFS(t.TempDir())
	synthArchive(t, st, [][2]int64{{1, 3}}, blobs)
	_, slot := archive.DirSlot(h)
	patch(t, st, archive.DirKey(0), func(b []byte) {
		binary.LittleEndian.PutUint32(b[slot+20:], binary.LittleEndian.Uint32(b[slot+20:])-1)
	})
	visible, hidden := blobs[h][0], blobs[h][n-1]
	if _, rec, err := ReadBlob(ctx, st, h, visible.Namespace, visible.Commitment); err != nil || !proto.Equal(rec.GetBlob(), visible) {
		t.Fatalf("ReadBlob of a visible blob: %v", err)
	}
	if _, _, err := ReadBlob(ctx, st, h, hidden.Namespace, hidden.Commitment); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadBlob of a hidden blob = %v, want ErrCorrupt", err)
	}
	if _, _, err := ReadHeight(ctx, st, h); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadHeight = %v, want ErrCorrupt", err)
	}
}

func mustLocate(t *testing.T, st store.Store, h int64) archive.DirEntry {
	t.Helper()
	e, err := Locate(context.Background(), st, h)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func mustRowOffset(t *testing.T, st store.Store, key string, r int) uint64 {
	t.Helper()
	b, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	row, err := segment.DecodeRow(b[segment.RowsHeaderSize+r*segment.RowSize : segment.RowsHeaderSize+(r+1)*segment.RowSize])
	if err != nil {
		t.Fatal(err)
	}
	return row.Offset
}

func TestReadRecordRejectsMismatch(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	b := synthBlob(2, nsOf(7), 0)
	synthArchive(t, st, [][2]int64{{1, 2}}, map[int64][]*archivev1.BlobRecord{2: {b}})
	e := mustLocate(t, st, 2)
	rows, err := LoadRows(ctx, st, e, 2)
	if err != nil {
		t.Fatal(err)
	}
	segKey := archive.SegmentKey(e.SegFirst, e.SegLast)
	cases := map[string]segment.Row{
		"kind":       {Height: 2, Kind: segment.KindHeight, Offset: rows[1].Offset, Length: rows[1].Length},
		"height":     {Height: 1, Kind: segment.KindBlob, Namespace: rows[1].Namespace, Commitment: rows[1].Commitment, Offset: rows[1].Offset, Length: rows[1].Length},
		"commitment": {Height: 2, Kind: segment.KindBlob, Namespace: rows[1].Namespace, Offset: rows[1].Offset, Length: rows[1].Length},
		"zero":       {Height: 2, Kind: segment.KindBlob, Offset: rows[1].Offset},
	}
	for name, r := range cases {
		if _, err := ReadRecord(ctx, st, segKey, r); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: ReadRecord = %v, want ErrCorrupt", name, err)
		}
	}
	if _, err := ReadRecord(ctx, st, segKey, rows[1]); err != nil {
		t.Fatal(err)
	}
}

func ns29(b []byte) [29]byte {
	var ns [29]byte
	copy(ns[:], b)
	return ns
}

// wantBlobs requires got to be exactly want, in order.
func wantBlobs(t *testing.T, label string, got []*archivev1.Record, want []*archivev1.BlobRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d records, want %d", label, len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i].GetBlob(), want[i]) {
			t.Fatalf("%s: record %d differs", label, i)
		}
	}
}

func TestReadNamespacesOrderAndRequests(t *testing.T) {
	ctx := context.Background()
	t.Run("fixture", func(t *testing.T) {
		fs := store.NewFS(t.TempDir())
		blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
		archivetest.Build(t, fs, blocks, 1, 7, 3, nil)
		blobs := blobsOf(mustReadHeight(t, fs, 3))
		// Chain order: two blobs of x around one blob of y.
		if len(blobs) != 3 || !bytes.Equal(blobs[0].Namespace, blobs[2].Namespace) || bytes.Equal(blobs[0].Namespace, blobs[1].Namespace) {
			t.Fatalf("fixture premise: %d blobs, x y x namespaces", len(blobs))
		}
		x, y := ns29(blobs[0].Namespace), ns29(blobs[1].Namespace)
		hr, got, err := ReadNamespaces(ctx, fs, 3, [][29]byte{nsOf(7), y, x}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if hr.GetHeight() != 3 || len(got) != 3 {
			t.Fatalf("height %d, %d results", hr.GetHeight(), len(got))
		}
		wantBlobs(t, "absent namespace", got[0], nil)
		wantBlobs(t, "y", got[1], blobs[1:2])
		wantBlobs(t, "x", got[2], []*archivev1.BlobRecord{blobs[0], blobs[2]})

		st := newCounting(fs)
		if _, got, err = ReadNamespaces(ctx, st, 3, [][29]byte{x}, nil); err != nil {
			t.Fatal(err)
		}
		wantBlobs(t, "x alone", got[0], []*archivev1.BlobRecord{blobs[0], blobs[2]})
		if len(st.gets) != 0 || st.ranges > 4 {
			t.Fatalf("full Gets %v, %d range reads; want none and at most 4", st.gets, st.ranges)
		}
	})
	t.Run("interleaved namespaces", func(t *testing.T) {
		const h = 2
		a, b, c := nsOf(1), nsOf(2), nsOf(3)
		big := synthBlob(h, a, 2)
		big.Data = bytes.Repeat([]byte{1}, 8<<10) // splits b's records into two runs
		chain := []*archivev1.BlobRecord{
			synthBlob(h, b, 0), synthBlob(h, a, 1), big, synthBlob(h, b, 3),
			synthBlob(h, c, 4), synthBlob(h, b, 5), synthBlob(h, a, 6),
		}
		fs := store.NewFS(t.TempDir())
		synthArchive(t, fs, [][2]int64{{1, 3}}, map[int64][]*archivev1.BlobRecord{h: chain})
		_, got, err := ReadNamespaces(ctx, fs, h, [][29]byte{c, a, b}, nil)
		if err != nil {
			t.Fatal(err)
		}
		wantBlobs(t, "c", got[0], []*archivev1.BlobRecord{chain[4]})
		wantBlobs(t, "a", got[1], []*archivev1.BlobRecord{chain[1], chain[2], chain[6]})
		wantBlobs(t, "b", got[2], []*archivev1.BlobRecord{chain[0], chain[3], chain[5]})

		// c alone: the directory entry, the rows, the height record (8 KiB
		// before c) and c.
		st := newCounting(fs)
		if _, got, err = ReadNamespaces(ctx, st, h, [][29]byte{c}, nil); err != nil {
			t.Fatal(err)
		}
		wantBlobs(t, "c alone", got[0], []*archivev1.BlobRecord{chain[4]})
		if st.ranges != 4 {
			t.Fatalf("%d range reads, want 4", st.ranges)
		}
	})
}

func TestReadNamespacesEmpty(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
	archivetest.Build(t, st, blocks, 1, 7, 3, nil)
	nss := [][29]byte{nsOf(7), nsOf(8)}
	for _, h := range []int64{2, 3} { // no blobs at all; blobs, none in nss
		hr, got, err := ReadNamespaces(ctx, st, h, nss, nil)
		if err != nil {
			t.Fatalf("height %d: %v", h, err)
		}
		if int64(hr.GetHeight()) != h || len(got) != len(nss) {
			t.Fatalf("height %d: record of height %d, %d results", h, hr.GetHeight(), len(got))
		}
		for i, g := range got {
			if g == nil || len(g) != 0 {
				t.Fatalf("height %d namespace %d: %v, want an empty slice", h, i, g)
			}
		}
	}
	if _, _, err := ReadNamespaces(ctx, st, 8, nss, nil); !errors.Is(err, ErrNotArchived) {
		t.Fatalf("height 8: %v, want ErrNotArchived", err)
	}
}

// TestReadNamespacesShortRowGroupIsCorrupt: a directory entry that drops the
// last row must not hide that blob as "none in this namespace".
func TestReadNamespacesShortRowGroupIsCorrupt(t *testing.T) {
	ctx := context.Background()
	const h = 2
	ns := nsOf(5)
	blobs := map[int64][]*archivev1.BlobRecord{h: {synthBlob(h, ns, 0), synthBlob(h, ns, 1)}}
	st := store.NewFS(t.TempDir())
	synthArchive(t, st, [][2]int64{{1, 3}}, blobs)
	_, slot := archive.DirSlot(h)
	patch(t, st, archive.DirKey(0), func(b []byte) {
		binary.LittleEndian.PutUint32(b[slot+20:], binary.LittleEndian.Uint32(b[slot+20:])-1)
	})
	for _, nss := range [][][29]byte{{ns}, {nsOf(6)}} {
		if _, _, err := ReadNamespaces(ctx, st, h, nss, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("ReadNamespaces(%x) = %v, want ErrCorrupt", nss[0], err)
		}
	}
}

// editRows decodes the .idx2 at key, lets edit change its rows and writes it
// back re-encoded.
func editRows(t *testing.T, st store.Store, key string, edit func(rows []segment.Row)) {
	t.Helper()
	ctx := context.Background()
	b, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	first, last, rows, err := segment.DecodeRows(b)
	if err != nil {
		t.Fatal(err)
	}
	edit(rows)
	if b, err = segment.EncodeRows(first, last, rows); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, key, b); err != nil {
		t.Fatal(err)
	}
}

// blobRowsOf returns the indexes in rows of the blob rows of height h.
func blobRowsOf(rows []segment.Row, h uint64) []int {
	var out []int
	for i, r := range rows {
		if r.Height == h && r.Kind == segment.KindBlob {
			out = append(out, i)
		}
	}
	return out
}

// TestReadNamespacesSpanCapped: rows that spread the requested records over
// more than maxHeightSpan bytes are corrupt; no segment byte is read and
// nothing is reserved.
func TestReadNamespacesSpanCapped(t *testing.T) {
	const h = 3
	ns := nsOf(7)
	cases := map[string]func(rows []segment.Row, blob []int){
		"one oversized row": func(rows []segment.Row, blob []int) {
			rows[blob[0]].Length = maxHeightSpan + 1
		},
		// Each run is under the cap; together they are over it.
		"several runs": func(rows []segment.Row, blob []int) {
			a, b := &rows[blob[0]], &rows[blob[1]]
			a.Length = maxHeightSpan/2 + 1
			b.Offset, b.Length = a.Offset+maxHeightSpan, maxHeightSpan/2+1
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fs := store.NewFS(t.TempDir())
			synthArchive(t, fs, [][2]int64{{1, 4}}, map[int64][]*archivev1.BlobRecord{h: {synthBlob(h, ns, 0), synthBlob(h, ns, 1)}})
			editRows(t, fs, archive.IndexKey(1, 4), func(rows []segment.Row) {
				blob := blobRowsOf(rows, h)
				if len(blob) != 2 {
					t.Fatalf("%d blob rows, want 2", len(blob))
				}
				edit(rows, blob)
			})
			st := newCounting(fs)
			reserved := false
			reserve := func(int64) (func(), error) { reserved = true; return func() {}, nil }
			if _, _, err := ReadNamespaces(ctx, st, h, [][29]byte{ns}, reserve); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadNamespaces = %v, want ErrCorrupt", err)
			}
			if st.ranges != 2 || reserved { // directory entry and rows only
				t.Fatalf("%d range reads (%d bytes), reserved %t; want 2 and no reservation", st.ranges, st.rangeBytes, reserved)
			}
		})
	}
}

// segBytesStore counts the bytes range-read from segment objects.
type segBytesStore struct {
	store.Store
	n int64
}

func (s *segBytesStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if strings.HasSuffix(key, ".seg") {
		s.n += n
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func TestReadNamespacesReserve(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
	archivetest.Build(t, fs, blocks, 1, 7, 3, nil)
	blobs := blobsOf(mustReadHeight(t, fs, 3))
	x := ns29(blobs[0].Namespace)
	rows, err := LoadRows(ctx, fs, mustLocate(t, fs, 3), 3)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(rows[0].Length)
	for _, r := range rows[1:] {
		if r.Namespace == x {
			want += int64(r.Length)
		}
	}

	t.Run("exact bytes", func(t *testing.T) {
		var calls []int64
		released := false
		reserve := func(n int64) (func(), error) {
			calls = append(calls, n)
			return func() { released = true }, nil
		}
		st := &segBytesStore{Store: fs}
		if _, got, err := ReadNamespaces(ctx, st, 3, [][29]byte{x, nsOf(7)}, reserve); err != nil || len(got[0]) != 2 {
			t.Fatalf("ReadNamespaces: %d records, %v", len(got[0]), err)
		}
		// The reservation is exactly the segment bytes read, gaps within a
		// run included, so it is never less than the records' own lengths.
		if len(calls) != 1 || calls[0] != st.n || calls[0] < want {
			t.Fatalf("reserve calls %v, want one of %d bytes (segment bytes read, at least %d)", calls, st.n, want)
		}
		if released {
			t.Fatal("ReadNamespaces released the caller's reservation")
		}
	})
	t.Run("error stops the read", func(t *testing.T) {
		busy := errors.New("busy")
		st := newCounting(fs)
		before := -1
		reserve := func(int64) (func(), error) {
			before = st.ranges
			return nil, busy
		}
		if _, _, err := ReadNamespaces(ctx, st, 3, [][29]byte{x}, reserve); !errors.Is(err, busy) {
			t.Fatalf("ReadNamespaces = %v, want the reserve error", err)
		}
		if before < 0 || st.ranges != before {
			t.Fatalf("%d range reads after reserve failed (before %d), want none", st.ranges-before, before)
		}
	})
}

func TestReadBlobReserve(t *testing.T) {
	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	blocks := archivetest.Blocks(t, 8, map[int64]string{3: "mocha-1834013"})
	archivetest.Build(t, fs, blocks, 1, 7, 3, nil)
	b := blobsOf(mustReadHeight(t, fs, 3))[0]

	for name, cm := range map[string][]byte{"found": b.Commitment, "absent": make([]byte, 32)} {
		t.Run(name, func(t *testing.T) {
			var calls []int64
			released := false
			reserve := func(n int64) (func(), error) {
				calls = append(calls, n)
				return func() { released = true }, nil
			}
			st := &segBytesStore{Store: fs}
			_, rec, err := ReadBlobReserve(ctx, st, 3, b.Namespace, cm, reserve)
			if name == "found" && (err != nil || !proto.Equal(rec.GetBlob(), b)) {
				t.Fatalf("ReadBlobReserve: %v", err)
			}
			if name == "absent" && !errors.Is(err, ErrBlobNotFound) {
				t.Fatalf("ReadBlobReserve = %v, want ErrBlobNotFound", err)
			}
			if len(calls) != 1 || calls[0] != st.n || st.n == 0 {
				t.Fatalf("reserve calls %v, want one of the %d segment bytes read", calls, st.n)
			}
			if released {
				t.Fatal("ReadBlobReserve released the caller's reservation")
			}
		})
	}
	t.Run("error stops the read", func(t *testing.T) {
		busy := errors.New("busy")
		st := newCounting(fs)
		before := -1
		reserve := func(int64) (func(), error) {
			before = st.ranges
			return nil, busy
		}
		if _, _, err := ReadBlobReserve(ctx, st, 3, b.Namespace, b.Commitment, reserve); !errors.Is(err, busy) {
			t.Fatalf("ReadBlobReserve = %v, want the reserve error", err)
		}
		if before < 0 || st.ranges != before {
			t.Fatalf("%d range reads after reserve failed (before %d), want none", st.ranges-before, before)
		}
	})
}
