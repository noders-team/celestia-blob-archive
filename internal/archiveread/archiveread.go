// Package archiveread holds the read side of the archive format: loading the
// manifest and catalog, locating a height through the height directory and
// reading its rows and records with range requests only.
package archiveread

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

var (
	// ErrCorrupt means archive objects exist but do not decode or contradict
	// the pointers that lead to them. An object the directory points at but
	// that is missing or shorter than promised is corrupt too; the store
	// error (store.ErrNotFound, store.ErrOutOfRange) is wrapped alongside.
	ErrCorrupt = errors.New("archiveread: corrupt archive object")
	// ErrNotArchived means the height directory has no entry for the height.
	// It never wraps store.ErrNotFound.
	ErrNotArchived = errors.New("archiveread: height not archived")
	// ErrBlobNotFound means the height is archived but holds no blob with the
	// requested namespace and commitment.
	ErrBlobNotFound = errors.New("archiveread: blob not in archived height")
)

// maxFetchAll is the largest row group of one height that is fetched whole;
// larger groups are binary-searched.
const maxFetchAll = 64 << 10

// maxHeightSpan caps the byte span of one height that ReadHeight reads with a
// single range request. The span comes from rows the archive supplies, so a
// forged .idx2 could otherwise make the client allocate gigabytes. Segments
// close at 64 MiB (archive.MaxSegmentSize) and a block is far smaller than
// this, so a larger span means corrupt rows.
const maxHeightSpan = 256 << 20

// LoadManifest reads manifest.json. A missing manifest is store.ErrNotFound.
func LoadManifest(ctx context.Context, st store.Store) (*archive.Manifest, error) {
	b, err := st.Get(ctx, archive.ManifestKey)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m archive.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if m.FormatVersion != archive.FormatVersion {
		return nil, fmt.Errorf("unsupported manifest format_version %d", m.FormatVersion)
	}
	return &m, nil
}

// LoadCatalog reads every catalog chunk (probing from 0 until the first
// missing one) and returns all segment refs sorted by First. Chunks are not
// disjoint height ranges, so all of them are needed to list the segments.
func LoadCatalog(ctx context.Context, st store.Store) ([]archive.SegmentRef, error) {
	var refs []archive.SegmentRef
	for i := 0; ; i++ {
		b, err := st.Get(ctx, archive.CatalogKey(i))
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read catalog chunk %d: %w", i, err)
		}
		var ch archive.CatalogChunk
		if err := json.Unmarshal(b, &ch); err != nil {
			return nil, fmt.Errorf("decode catalog chunk %d: %w", i, err)
		}
		refs = append(refs, ch.Segments...)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].First < refs[j].First })
	return refs, nil
}

// Range is an inclusive height range.
type Range struct{ First, Last int64 }

func (r Range) String() string {
	if r.First == r.Last {
		return fmt.Sprint(r.First)
	}
	return fmt.Sprintf("%d-%d", r.First, r.Last)
}

// Ranges merges segment refs (sorted by First) into maximal committed ranges
// and returns them with the gaps between them. Heights start at 1, so a first
// range that does not begin at 1 is preceded by a gap.
func Ranges(refs []archive.SegmentRef) (committed, gaps []Range) {
	for _, r := range refs {
		if n := len(committed); n > 0 && r.First <= committed[n-1].Last+1 {
			if r.Last > committed[n-1].Last {
				committed[n-1].Last = r.Last
			}
			continue
		}
		committed = append(committed, Range{r.First, r.Last})
	}
	next := int64(1)
	for _, c := range committed {
		if c.First > next {
			gaps = append(gaps, Range{next, c.First - 1})
		}
		next = c.Last + 1
	}
	return committed, gaps
}

// JoinRanges formats rs as "1-4, 7-8", or "none".
func JoinRanges(rs []Range) string {
	if len(rs) == 0 {
		return "none"
	}
	s := make([]string, len(rs))
	for i, r := range rs {
		s[i] = r.String()
	}
	return strings.Join(s, ", ")
}

// readPromised range-reads bytes a pointer promised. A missing object or a
// short object is ErrCorrupt; other store errors are returned as they are.
func readPromised(ctx context.Context, st store.Store, key string, off, n int64) ([]byte, error) {
	b, err := st.GetRange(ctx, key, off, n)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrOutOfRange) {
		return nil, fmt.Errorf("%w: read %s [%d,+%d): %w", ErrCorrupt, key, off, n, err)
	}
	if err != nil {
		return nil, fmt.Errorf("read %s [%d,+%d): %w", key, off, n, err)
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("%w: read %s [%d,+%d): got %d bytes", ErrCorrupt, key, off, n, len(b))
	}
	return b, nil
}

// Locate reads the directory entry of height with one range request. A
// missing directory object or a zero entry is ErrNotArchived.
func Locate(ctx context.Context, st store.Store, height int64) (archive.DirEntry, error) {
	if height < 1 {
		return archive.DirEntry{}, fmt.Errorf("%w: height %d", ErrNotArchived, height)
	}
	bucket, off := archive.DirSlot(height)
	key := archive.DirKey(bucket)
	b, err := st.GetRange(ctx, key, off, archive.DirEntrySize)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return archive.DirEntry{}, fmt.Errorf("%w: height %d", ErrNotArchived, height)
	case errors.Is(err, store.ErrOutOfRange):
		return archive.DirEntry{}, fmt.Errorf("%w: read %s at %d: %w", ErrCorrupt, key, off, err)
	case err != nil:
		return archive.DirEntry{}, fmt.Errorf("read %s at %d: %w", key, off, err)
	}
	e, err := archive.DecodeDirEntry(b)
	if err != nil {
		return archive.DirEntry{}, fmt.Errorf("%w: %s height %d: %v", ErrCorrupt, key, height, err)
	}
	if e.RowCount == 0 {
		return archive.DirEntry{}, fmt.Errorf("%w: height %d", ErrNotArchived, height)
	}
	if height < e.SegFirst || height > e.SegLast {
		return archive.DirEntry{}, fmt.Errorf("%w: %s: height %d points at segment [%d,%d]", ErrCorrupt, key, height, e.SegFirst, e.SegLast)
	}
	return e, nil
}

// checkRow rejects a row of height that cannot address a record: another
// height, an empty record, or bytes beyond int64.
func checkRow(key string, height int64, r segment.Row) error {
	switch {
	case int64(r.Height) != height || r.Height > math.MaxInt64:
		return fmt.Errorf("%w: %s: row of height %d among the rows of height %d", ErrCorrupt, key, r.Height, height)
	case r.Length == 0:
		return fmt.Errorf("%w: %s: empty row at offset %d", ErrCorrupt, key, r.Offset)
	case r.Offset+uint64(r.Length) > math.MaxInt64:
		return fmt.Errorf("%w: %s: row [%d,+%d) beyond int64", ErrCorrupt, key, r.Offset, r.Length)
	}
	return nil
}

// readRows range-reads n rows of key starting at row start and checks that
// each belongs to height. A range past the end of the .idx2 (i.e. beyond its
// row count) is ErrCorrupt.
func readRows(ctx context.Context, st store.Store, key string, height int64, start, n uint64) ([]segment.Row, error) {
	b, err := readPromised(ctx, st, key, int64(segment.RowsHeaderSize+start*segment.RowSize), int64(n*segment.RowSize))
	if err != nil {
		return nil, err
	}
	rows := make([]segment.Row, n)
	for i := range rows {
		r, err := segment.DecodeRow(b[i*segment.RowSize : (i+1)*segment.RowSize])
		if err != nil {
			return nil, fmt.Errorf("%w: %s row %d: %v", ErrCorrupt, key, start+uint64(i), err)
		}
		if err := checkRow(key, height, r); err != nil {
			return nil, err
		}
		rows[i] = r
	}
	return rows, nil
}

// LoadRows reads the rows of height that e points at with one range request.
// The group must be one height row followed by blob rows sorted by
// (namespace, offset).
func LoadRows(ctx context.Context, st store.Store, e archive.DirEntry, height int64) ([]segment.Row, error) {
	if e.RowCount == 0 {
		return nil, fmt.Errorf("%w: height %d", ErrNotArchived, height)
	}
	key := archive.IndexKey(e.SegFirst, e.SegLast)
	rows, err := readRows(ctx, st, key, height, uint64(e.RowStart), uint64(e.RowCount))
	if err != nil {
		return nil, err
	}
	if rows[0].Kind != segment.KindHeight {
		return nil, fmt.Errorf("%w: %s: rows of height %d do not start with its height row", ErrCorrupt, key, height)
	}
	for i := 1; i < len(rows); i++ {
		// DecodeRow rejects unknown kinds, so this is a height row.
		if rows[i].Kind != segment.KindBlob {
			return nil, fmt.Errorf("%w: %s: second height row for height %d", ErrCorrupt, key, height)
		}
		if i > 1 && blobRowOrder(rows[i-1], rows[i]) >= 0 {
			return nil, fmt.Errorf("%w: %s: rows of height %d not sorted at row %d", ErrCorrupt, key, height, uint64(e.RowStart)+uint64(i))
		}
	}
	return rows, nil
}

func blobRowOrder(a, b segment.Row) int {
	if c := bytes.Compare(a.Namespace[:], b.Namespace[:]); c != 0 {
		return c
	}
	switch {
	case a.Offset < b.Offset:
		return -1
	case a.Offset > b.Offset:
		return 1
	}
	return 0
}

// FindBlobRow returns the height row and the row of the blob (ns,
// commitment) of height. A row group of at most 64 KiB is fetched whole;
// a larger one is binary-searched on namespace with single-row range reads,
// then the namespace run is scanned for the commitment. A blob absent from
// the rows is ErrBlobNotFound, returned with the height row so the caller
// can check the row count against the height record (see ReadBlob).
func FindBlobRow(ctx context.Context, st store.Store, e archive.DirEntry, height int64, ns, commitment []byte) (hr, blob segment.Row, err error) {
	if len(ns) != len(segment.Row{}.Namespace) || len(commitment) != len(segment.Row{}.Commitment) {
		return hr, blob, fmt.Errorf("find blob: %d-byte namespace and %d-byte commitment", len(ns), len(commitment))
	}
	if uint64(e.RowCount)*segment.RowSize <= maxFetchAll {
		rows, err := LoadRows(ctx, st, e, height)
		if err != nil {
			return hr, blob, err
		}
		for _, r := range rows[1:] {
			if bytes.Equal(r.Namespace[:], ns) && bytes.Equal(r.Commitment[:], commitment) {
				return rows[0], r, nil
			}
		}
		return rows[0], blob, fmt.Errorf("%w: height %d", ErrBlobNotFound, height)
	}
	return searchBlobRow(ctx, st, e, height, ns, commitment)
}

func searchBlobRow(ctx context.Context, st store.Store, e archive.DirEntry, height int64, ns, commitment []byte) (hr, blob segment.Row, err error) {
	key := archive.IndexKey(e.SegFirst, e.SegLast)
	n := uint64(e.RowCount)
	cache := map[uint64]segment.Row{}
	// rowAt reads row i of the group (0 is the height row), or with fill
	// also the rows after it up to 64 KiB, caching all of them.
	rowAt := func(i uint64, fill bool) (segment.Row, error) {
		if r, ok := cache[i]; ok {
			return r, nil
		}
		cnt := uint64(1)
		if fill {
			cnt = min(n-i, maxFetchAll/segment.RowSize)
		}
		rows, err := readRows(ctx, st, key, height, uint64(e.RowStart)+i, cnt)
		if err != nil {
			return segment.Row{}, err
		}
		for j, r := range rows {
			want := segment.KindBlob
			if i+uint64(j) == 0 {
				want = segment.KindHeight
			}
			if r.Kind != want {
				return segment.Row{}, fmt.Errorf("%w: %s: row %d of height %d has kind %d, want %d", ErrCorrupt, key, uint64(e.RowStart)+i+uint64(j), height, r.Kind, want)
			}
			cache[i+uint64(j)] = r
		}
		return rows[0], nil
	}

	if hr, err = rowAt(0, false); err != nil {
		return hr, blob, err
	}
	// Lower bound of ns among blob rows 1..n-1.
	var serr error
	lo := uint64(sort.Search(int(n-1), func(j int) bool {
		if serr != nil {
			return true
		}
		r, err := rowAt(uint64(j)+1, false)
		if err != nil {
			serr = err
			return true
		}
		return bytes.Compare(r.Namespace[:], ns) >= 0
	})) + 1
	if serr != nil {
		return hr, blob, serr
	}
	var prev *segment.Row
	for i := lo; i < n; i++ {
		r, err := rowAt(i, true)
		if err != nil {
			return hr, blob, err
		}
		if c := bytes.Compare(r.Namespace[:], ns); c != 0 {
			if c < 0 {
				return hr, blob, fmt.Errorf("%w: %s: rows of height %d not sorted by namespace", ErrCorrupt, key, height)
			}
			break
		}
		if prev != nil && prev.Offset >= r.Offset {
			return hr, blob, fmt.Errorf("%w: %s: rows of height %d not sorted by offset", ErrCorrupt, key, height)
		}
		if bytes.Equal(r.Commitment[:], commitment) {
			return hr, r, nil
		}
		prev = &r
	}
	return hr, blob, fmt.Errorf("%w: height %d", ErrBlobNotFound, height)
}

// matchRow requires rec to be the record r describes: a height record for a
// height row, a blob or tombstone with the row's namespace and commitment for
// a blob row, of the row's height either way.
func matchRow(rec *archivev1.Record, r segment.Row) error {
	var (
		height uint64
		ns, cm []byte
	)
	switch k := rec.Kind.(type) {
	case *archivev1.Record_Height:
		if r.Kind != segment.KindHeight {
			return fmt.Errorf("height record for a row of kind %d", r.Kind)
		}
		height = k.Height.GetHeight()
	case *archivev1.Record_Blob:
		height, ns, cm = k.Blob.GetHeight(), k.Blob.GetNamespace(), k.Blob.GetCommitment()
	case *archivev1.Record_Tombstone:
		height, ns, cm = k.Tombstone.GetHeight(), k.Tombstone.GetNamespace(), k.Tombstone.GetCommitment()
	default:
		return errors.New("empty record")
	}
	if height != r.Height {
		return fmt.Errorf("record of height %d for a row of height %d", height, r.Height)
	}
	if _, ok := rec.Kind.(*archivev1.Record_Height); ok {
		return nil
	}
	if r.Kind != segment.KindBlob {
		return fmt.Errorf("blob or tombstone record for a row of kind %d", r.Kind)
	}
	if !bytes.Equal(ns, r.Namespace[:]) || !bytes.Equal(cm, r.Commitment[:]) {
		return errors.New("record namespace or commitment differs from its row")
	}
	return nil
}

// ReadRecord reads the record r points at in segKey with one range request.
// A record that does not match the row (kind, height, namespace, commitment)
// is ErrCorrupt.
func ReadRecord(ctx context.Context, st store.Store, segKey string, r segment.Row) (*archivev1.Record, error) {
	if err := checkRow(segKey, int64(r.Height), r); err != nil {
		return nil, err
	}
	b, err := readPromised(ctx, st, segKey, int64(r.Offset), int64(r.Length))
	if err != nil {
		return nil, err
	}
	rec, err := segment.ReadAt(b, 0, r.Length)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, segKey, err)
	}
	if err := matchRow(rec, r); err != nil {
		return nil, fmt.Errorf("%w: %s at offset %d: %v", ErrCorrupt, segKey, r.Offset, err)
	}
	return rec, nil
}

// checkBlobCount requires a row group of rowCount rows (the height row plus
// one row per blob or tombstone) to cover every blob of hr, so that a
// directory entry with too few rows cannot hide blobs.
func checkBlobCount(hr *archivev1.HeightRecord, rowCount uint32) error {
	if uint64(hr.GetBlobCount())+1 != uint64(rowCount) {
		return fmt.Errorf("%w: height %d has %d blobs but %d rows", ErrCorrupt, hr.GetHeight(), hr.GetBlobCount(), rowCount)
	}
	return nil
}

// ReadBlob returns the height record of height and the record of the blob
// (ns, commitment): a BlobRecord, or a TombstoneRecord if it was taken down.
// It makes only range requests: directory entry, rows, and the two records.
func ReadBlob(ctx context.Context, st store.Store, height int64, ns, commitment []byte) (*archivev1.HeightRecord, *archivev1.Record, error) {
	return ReadBlobReserve(ctx, st, height, ns, commitment, nil)
}

// ReadBlobReserve is ReadBlob with a reserve hook, as in ReadNamespaces: if
// not nil it is called once after the rows are found and before any record
// byte is read, with the number of segment bytes that will be read (the
// height record's, plus the blob record's unless the blob is absent). An
// error from it is returned, wrapped, with nothing read. The caller keeps
// and calls the release func.
func ReadBlobReserve(ctx context.Context, st store.Store, height int64, ns, commitment []byte,
	reserve func(bytes int64) (release func(), err error)) (*archivev1.HeightRecord, *archivev1.Record, error) {
	e, err := Locate(ctx, st, height)
	if err != nil {
		return nil, nil, err
	}
	segKey := archive.SegmentKey(e.SegFirst, e.SegLast)
	hrRow, blobRow, err := FindBlobRow(ctx, st, e, height, ns, commitment)
	absent := errors.Is(err, ErrBlobNotFound)
	if err != nil && !absent {
		return nil, nil, err
	}
	if reserve != nil {
		n := int64(hrRow.Length)
		if !absent {
			n += int64(blobRow.Length)
		}
		if _, rerr := reserve(n); rerr != nil {
			return nil, nil, fmt.Errorf("reserve %d bytes for height %d: %w", n, height, rerr)
		}
	}
	if absent {
		// A short row group would hide the blob; only the height record
		// knows the real count, so read it on this path only.
		hr, rerr := ReadRecord(ctx, st, segKey, hrRow)
		if rerr != nil {
			return nil, nil, rerr
		}
		if cerr := checkBlobCount(hr.GetHeight(), e.RowCount); cerr != nil {
			return nil, nil, cerr
		}
		return nil, nil, err
	}
	hr, err := ReadRecord(ctx, st, segKey, hrRow)
	if err != nil {
		return nil, nil, err
	}
	rec, err := ReadRecord(ctx, st, segKey, blobRow)
	if err != nil {
		return nil, nil, err
	}
	return hr.GetHeight(), rec, nil
}

// ReadHeight returns the height record of height and its blob records
// (BlobRecord or TombstoneRecord) in chain (offset) order, reading all of
// them with one range request over their span.
func ReadHeight(ctx context.Context, st store.Store, height int64) (*archivev1.HeightRecord, []*archivev1.Record, error) {
	e, err := Locate(ctx, st, height)
	if err != nil {
		return nil, nil, err
	}
	rows, err := LoadRows(ctx, st, e, height)
	if err != nil {
		return nil, nil, err
	}
	segKey := archive.SegmentKey(e.SegFirst, e.SegLast)
	blobRows := slices.Clone(rows[1:])
	slices.SortFunc(blobRows, byOffset)
	lo, hi := rows[0].Offset, rows[0].Offset+uint64(rows[0].Length)
	for _, r := range rows {
		lo, hi = min(lo, r.Offset), max(hi, r.Offset+uint64(r.Length))
	}
	if hi-lo > maxHeightSpan {
		return nil, nil, fmt.Errorf("%w: %s: height %d spans %d bytes, more than %d",
			ErrCorrupt, segKey, height, hi-lo, maxHeightSpan)
	}
	buf, err := readPromised(ctx, st, segKey, int64(lo), int64(hi-lo))
	if err != nil {
		return nil, nil, err
	}
	decode := func(r segment.Row) (*archivev1.Record, error) { return decodeAt(segKey, buf, lo, r) }
	hr, err := decode(rows[0])
	if err != nil {
		return nil, nil, err
	}
	if err := checkBlobCount(hr.GetHeight(), e.RowCount); err != nil {
		return nil, nil, err
	}
	recs := make([]*archivev1.Record, len(blobRows))
	for i, r := range blobRows {
		if recs[i], err = decode(r); err != nil {
			return nil, nil, err
		}
	}
	return hr.GetHeight(), recs, nil
}

// maxRunGap is the largest gap between two wanted records that ReadNamespaces
// reads through instead of starting another range request.
const maxRunGap = 4 << 10

// ReadNamespaces returns the height record of height and, for each namespace
// of nss, the blob records (BlobRecord or TombstoneRecord) of height in that
// namespace in chain (offset) order; the result is indexed like nss and a
// namespace without blobs has an empty slice. It reads the directory entry and
// the whole row group, then the height record and the wanted records with one
// range request per run of records no more than maxRunGap bytes apart. The
// bytes of all runs together are capped by maxHeightSpan. The height record's
// blob count must match the row group (checkBlobCount), so a short group
// cannot hide blobs.
//
// reserve, if not nil, is called once after the row group is loaded and
// before any record byte is read, with the number of segment bytes that will
// be read and held: the summed run lengths, including the gaps read through
// within a run. An error from it is returned, wrapped, with nothing read.
// ReadNamespaces never calls the release func reserve returns; the caller
// keeps it (typically captured by the hook) and releases.
func ReadNamespaces(ctx context.Context, st store.Store, height int64, nss [][29]byte,
	reserve func(bytes int64) (release func(), err error)) (*archivev1.HeightRecord, [][]*archivev1.Record, error) {
	e, err := Locate(ctx, st, height)
	if err != nil {
		return nil, nil, err
	}
	rows, err := LoadRows(ctx, st, e, height)
	if err != nil {
		return nil, nil, err
	}
	segKey := archive.SegmentKey(e.SegFirst, e.SegLast)

	want := make(map[[29]byte][]int, len(nss))
	for i, ns := range nss {
		want[ns] = append(want[ns], i)
	}
	// The height record is read with the blobs: it precedes them in the
	// segment, so it usually joins the first run.
	wanted := []segment.Row{rows[0]}
	for _, r := range rows[1:] {
		if _, ok := want[r.Namespace]; ok {
			wanted = append(wanted, r)
		}
	}
	slices.SortFunc(wanted, byOffset)

	type run struct {
		lo, hi uint64
		rows   []segment.Row
	}
	var (
		runs  []run
		total uint64
	)
	for _, r := range wanted {
		end := r.Offset + uint64(r.Length) // checkRow keeps this within int64
		if n := len(runs); n > 0 && r.Offset <= runs[n-1].hi+maxRunGap {
			last := &runs[n-1]
			total += max(end, last.hi) - last.hi
			last.hi = max(end, last.hi)
			last.rows = append(last.rows, r)
		} else {
			total += end - r.Offset
			runs = append(runs, run{r.Offset, end, []segment.Row{r}})
		}
		if total > maxHeightSpan {
			return nil, nil, fmt.Errorf("%w: %s: namespaces of height %d span more than %d bytes",
				ErrCorrupt, segKey, height, maxHeightSpan)
		}
	}
	if reserve != nil {
		// The release func is the caller's: it holds the bytes until the
		// records it gets back are no longer needed, error or not.
		if _, err := reserve(int64(total)); err != nil {
			return nil, nil, fmt.Errorf("reserve %d bytes for height %d: %w", total, height, err)
		}
	}

	var hr *archivev1.HeightRecord
	out := make([][]*archivev1.Record, len(nss))
	for i := range out {
		out[i] = []*archivev1.Record{}
	}
	for _, rn := range runs {
		buf, err := readPromised(ctx, st, segKey, int64(rn.lo), int64(rn.hi-rn.lo))
		if err != nil {
			return nil, nil, err
		}
		for _, r := range rn.rows {
			rec, err := decodeAt(segKey, buf, rn.lo, r)
			if err != nil {
				return nil, nil, err
			}
			if r.Kind == segment.KindHeight {
				hr = rec.GetHeight()
				continue
			}
			for _, i := range want[r.Namespace] {
				out[i] = append(out[i], rec)
			}
		}
	}
	if err := checkBlobCount(hr, e.RowCount); err != nil {
		return nil, nil, err
	}
	return hr, out, nil
}

// decodeAt decodes the record r points at from buf, which holds segKey from
// byte lo on, and requires it to match r.
func decodeAt(segKey string, buf []byte, lo uint64, r segment.Row) (*archivev1.Record, error) {
	rec, err := segment.ReadAt(buf, r.Offset-lo, r.Length)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, segKey, err)
	}
	if err := matchRow(rec, r); err != nil {
		return nil, fmt.Errorf("%w: %s at offset %d: %v", ErrCorrupt, segKey, r.Offset, err)
	}
	return rec, nil
}

func byOffset(a, b segment.Row) int {
	switch {
	case a.Offset < b.Offset:
		return -1
	case a.Offset > b.Offset:
		return 1
	}
	return 0
}
