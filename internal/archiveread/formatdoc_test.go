package archiveread

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// rangeRecorder records every range read as "GET key bytes=first-last".
type rangeRecorder struct {
	store.Store
	mu   sync.Mutex
	gets []string
}

func (r *rangeRecorder) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	r.mu.Lock()
	r.gets = append(r.gets, fmt.Sprintf("GET %s bytes=%d-%d", key, off, off+n-1))
	r.mu.Unlock()
	return r.Store.GetRange(ctx, key, off, n)
}

// lookupExample returns the lines of the ```lookup block in docs/format.md,
// with "#" comments and blank lines removed.
func lookupExample(t *testing.T) []string {
	t.Helper()
	doc, err := os.ReadFile("../../docs/format.md")
	if err != nil {
		t.Fatal(err)
	}
	_, block, ok := strings.Cut(string(doc), "```lookup\n")
	if !ok {
		t.Fatal("docs/format.md has no ```lookup block")
	}
	block, _, ok = strings.Cut(block, "```")
	if !ok {
		t.Fatal("docs/format.md: unterminated ```lookup block")
	}
	var lines []string
	for _, l := range strings.Split(block, "\n") {
		l, _, _ = strings.Cut(l, "#")
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// TestFormatDocExample replays the worked lookup of docs/format.md against
// the reader: the four range GETs it lists must be exactly the ones Locate,
// FindBlobRow and ReadRecord make for the directory entry and rows it shows,
// and the rows must describe consecutive frames.
func TestFormatDocExample(t *testing.T) {
	var (
		height int64
		entry  archive.DirEntry
		rows   []segment.Row
		want   = -1
		gets   []string
	)
	for _, l := range lookupExample(t) {
		var err error
		switch f := strings.Fields(l); f[0] {
		case "height":
			_, err = fmt.Sscanf(l, "height %d", &height)
		case "entry":
			_, err = fmt.Sscanf(l, "entry seg_first=%d seg_last=%d row_start=%d row_count=%d",
				&entry.SegFirst, &entry.SegLast, &entry.RowStart, &entry.RowCount)
		case "row":
			r := segment.Row{Height: uint64(height)}
			_, err = fmt.Sscanf(l, "row kind=%d offset=%d length=%d", &r.Kind, &r.Offset, &r.Length)
			if strings.HasSuffix(l, "wanted") {
				want = len(rows)
			}
			rows = append(rows, r)
		case "GET":
			gets = append(gets, l)
		default:
			t.Fatalf("unknown line %q", l)
		}
		if err != nil {
			t.Fatalf("line %q: %v", l, err)
		}
	}
	if height == 0 || len(rows) != int(entry.RowCount) || len(gets) != 4 || want < 1 {
		t.Fatalf("example: height %d, %d rows for row_count %d, %d GETs, wanted row %d",
			height, len(rows), entry.RowCount, len(gets), want)
	}

	// One namespace for every blob (sorted by offset), distinct commitments.
	for i := range rows {
		if rows[i].Kind == segment.KindBlob {
			rows[i].Namespace[28] = 7
			rows[i].Commitment[0] = byte(i)
		}
	}
	for i := 1; i < len(rows); i++ {
		p, r := rows[i-1], rows[i]
		if next := p.Offset + uint64(p.Length) + uint64(protowire.SizeVarint(uint64(r.Length))); r.Offset != next {
			t.Errorf("row %d: offset %d, but the previous frame ends and the length prefix puts it at %d", i, r.Offset, next)
		}
	}

	ctx := context.Background()
	fs := store.NewFS(t.TempDir())
	bucket, slot := archive.DirSlot(height)
	var es [archive.DirBucket]archive.DirEntry
	es[(slot-archive.DirHeaderSize)/archive.DirEntrySize] = entry
	if err := fs.Put(ctx, archive.DirKey(bucket), archive.EncodeDir(bucket, &es)); err != nil {
		t.Fatal(err)
	}
	group, err := segment.EncodeRows(uint64(height), uint64(height), rows)
	if err != nil {
		t.Fatal(err)
	}
	// Rows before the group are never read; zero bytes stand in for them.
	idx := append(make([]byte, segment.RowsHeaderSize+int(entry.RowStart)*segment.RowSize), group[segment.RowsHeaderSize:]...)
	if err := fs.Put(ctx, archive.IndexKey(entry.SegFirst, entry.SegLast), idx); err != nil {
		t.Fatal(err)
	}

	st := &rangeRecorder{Store: fs}
	e, err := Locate(ctx, st, height)
	if err != nil {
		t.Fatal(err)
	}
	if e != entry {
		t.Fatalf("Locate = %+v, example shows %+v", e, entry)
	}
	hr, blob, err := FindBlobRow(ctx, st, e, height, rows[want].Namespace[:], rows[want].Commitment[:])
	if err != nil {
		t.Fatal(err)
	}
	if hr != rows[0] || blob != rows[want] {
		t.Fatalf("FindBlobRow = %+v, %+v; want rows 0 and %d", hr, blob, want)
	}
	// The segment is not stored, so both reads fail (ErrCorrupt wrapping
	// store.ErrNotFound) after recording their range; only the ranges are
	// checked here.
	segKey := archive.SegmentKey(e.SegFirst, e.SegLast)
	_, _ = ReadRecord(ctx, st, segKey, hr)
	_, _ = ReadRecord(ctx, st, segKey, blob)

	if strings.Join(st.gets, "\n") != strings.Join(gets, "\n") {
		t.Errorf("reader GETs:\n%s\nexample GETs:\n%s", strings.Join(st.gets, "\n"), strings.Join(gets, "\n"))
	}
}
