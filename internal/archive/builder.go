package archive

import (
	"context"
	"errors"
	"fmt"
	"time"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
)

const (
	// MaxSegmentSize closes a segment once it reaches this many bytes.
	MaxSegmentSize = 64 << 20
	// MaxSegmentAge closes a segment once it has been open this long.
	MaxSegmentAge = time.Hour
)

// Builder accumulates consecutive heights into one segment. It is not safe for
// concurrent use.
type Builder struct {
	clock    func() time.Time
	w        segment.Writer
	rows     []segment.Row
	first    int64
	last     int64
	openedAt time.Time
	empty    bool
}

// NewBuilder returns an empty Builder; clock may be nil for time.Now.
func NewBuilder(clock func() time.Time) *Builder {
	if clock == nil {
		clock = time.Now
	}
	return &Builder{clock: clock, empty: true}
}

// Add appends the height record followed by its blob records. Heights must be
// consecutive; the first Add fixes the start. Height gaps and foreign blobs are
// rejected without changing the builder; an error from record encoding is not
// recoverable and the builder must be discarded.
func (b *Builder) Add(h *archivev1.HeightRecord, blobs []*archivev1.BlobRecord) error {
	if h == nil {
		return errors.New("builder: nil height record")
	}
	height := int64(h.Height)
	if !b.empty && height != b.last+1 {
		return fmt.Errorf("builder: height %d is not %d (previous+1)", height, b.last+1)
	}
	for _, bl := range blobs {
		if bl == nil || bl.Height != h.Height {
			return fmt.Errorf("builder: blob record does not belong to height %d", height)
		}
		if len(bl.Namespace) != len(segment.Row{}.Namespace) || len(bl.Commitment) != len(segment.Row{}.Commitment) {
			return fmt.Errorf("builder: blob at height %d has a %d-byte namespace and a %d-byte commitment",
				height, len(bl.Namespace), len(bl.Commitment))
		}
	}
	// A marshal error part-way leaves earlier records of this height in the
	// writer; the builder must then be discarded.
	w := &b.w
	var added []segment.Row
	off, n, err := w.Append(&archivev1.Record{Kind: &archivev1.Record_Height{Height: h}})
	if err != nil {
		return err
	}
	added = append(added, segment.Row{Height: h.Height, Kind: segment.KindHeight, Offset: off, Length: n})
	for _, bl := range blobs {
		off, n, err := w.Append(&archivev1.Record{Kind: &archivev1.Record_Blob{Blob: bl}})
		if err != nil {
			return err
		}
		r := segment.Row{Height: h.Height, Kind: segment.KindBlob, Offset: off, Length: n}
		copy(r.Namespace[:], bl.Namespace)
		copy(r.Commitment[:], bl.Commitment)
		added = append(added, r)
	}
	b.rows = append(b.rows, added...)
	if b.empty {
		b.empty = false
		b.first = height
		b.openedAt = b.clock()
	}
	b.last = height
	return nil
}

// Size returns the current segment size in bytes.
func (b *Builder) Size() int { return b.w.Size() }

// Empty reports whether nothing has been added since the last Flush.
func (b *Builder) Empty() bool { return b.empty }

// ShouldClose reports whether the segment reached MaxSegmentSize or has been
// open longer than MaxSegmentAge (measured from the first Add).
func (b *Builder) ShouldClose(now time.Time) bool {
	if b.empty {
		return false
	}
	return b.w.Size() >= MaxSegmentSize || now.Sub(b.openedAt) > MaxSegmentAge
}

// Flush finalises the segment and its row index (.idx2), returns the ref to
// commit and resets the builder. It fails on an empty builder.
func (b *Builder) Flush(ctx context.Context) (*SegmentRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.empty {
		return nil, errors.New("builder: nothing to flush")
	}
	rows := b.rows
	segment.SortRows(rows)
	idx, err := segment.EncodeRows(uint64(b.first), uint64(b.last), rows)
	if err != nil {
		return nil, err
	}
	seg := b.w.Bytes()
	ref := &SegmentRef{
		First: b.first, Last: b.last, Key: SegmentKey(b.first, b.last),
		Size: int64(len(seg)), seg: seg, idx: idx, rows: rows,
	}
	*b = Builder{clock: b.clock, empty: true}
	return ref, nil
}
