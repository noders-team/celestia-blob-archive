package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// ErrOverlap means a segment intersects heights already committed under a
// different key.
var ErrOverlap = errors.New("archive: segment overlaps committed heights")

// ErrNoTombstoneSet means a stored segment differs from the one being
// committed and no tombstone set is loaded yet to tell whether only
// tombstones differ. It is retryable once ApplyTombstones has run.
var ErrNoTombstoneSet = errors.New("archive: no tombstone set loaded")

// ErrOPDelete means Commit published the segment but could not delete the OP
// objects of its withdrawn blobs. It is retryable: a retry repeats the delete.
var ErrOPDelete = errors.New("archive: delete op objects")

// Object headers by key class (see docs/format.md).
var (
	immutableBinary = store.PutOptions{CacheControl: store.CacheImmutable, ContentType: store.TypeBinary}
	segmentBinary   = store.PutOptions{CacheControl: store.CacheSegment, ContentType: store.TypeBinary}
	mutableBinary   = store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeBinary}
	immutableJSON   = store.PutOptions{CacheControl: store.CacheImmutable, ContentType: store.TypeJSON}
	mutableJSON     = store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}
)

type segRange struct {
	first, last int64
	key         string
}

// Committer publishes segments and keeps catalog and manifest consistent. It
// is safe for concurrent use.
type Committer struct {
	st      store.Store
	chainID string
	now     func() time.Time

	mu      sync.Mutex
	cur     int          // index of the open (last) catalog chunk
	curRefs []SegmentRef // refs of the open chunk, sorted by First
	keys    map[string]struct{}
	ranges  []segRange // all committed segments, sorted by first
	highest int64

	// open holds the directory buckets loaded since start that are not
	// frozen yet; frozen buckets are dropped. unfrozen lists buckets the
	// catalog covers completely whose immutable write is still due.
	open        map[int64]*[DirBucket]DirEntry
	unfrozen    map[int64]struct{}
	onFreezeErr func(key string, err error)

	// tomb is the current tombstone set (nil until ApplyTombstones);
	// onScrub reports scrubs and per-record scrub errors.
	tomb    *tombstone.Set
	onScrub func(key string, scrubbed int, err error)
	opw     *oplayer.Writer // nil if no OP namespaces are configured

	beforeCatalog func() // test hook, runs without c.mu
}

// NewCommitter loads manifest and catalog from st if present.
func NewCommitter(ctx context.Context, st store.Store, chainID string) (*Committer, error) {
	c := &Committer{
		st: st, chainID: chainID, now: time.Now, keys: map[string]struct{}{},
		open: map[int64]*[DirBucket]DirEntry{}, unfrozen: map[int64]struct{}{},
	}
	// The manifest is written last, so it may lag behind the catalog after a
	// crash. It is only used to validate format and chain; chunks are
	// discovered by probing from 0 upward.
	b, err := st.Get(ctx, ManifestKey)
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return nil, fmt.Errorf("read manifest: %w", err)
	default:
		var m Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("decode manifest: %w", err)
		}
		if m.FormatVersion != FormatVersion {
			return nil, fmt.Errorf("unsupported manifest format_version %d, want %d (run cba-ingester migrate-v2 first)",
				m.FormatVersion, FormatVersion)
		}
		if m.ChainID != chainID {
			return nil, fmt.Errorf("manifest chain_id %q does not match %q", m.ChainID, chainID)
		}
	}
	for i := 0; ; i++ {
		cb, err := st.Get(ctx, CatalogKey(i))
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read catalog chunk %d: %w", i, err)
		}
		var ch CatalogChunk
		if err := json.Unmarshal(cb, &ch); err != nil {
			return nil, fmt.Errorf("decode catalog chunk %d: %w", i, err)
		}
		for _, r := range ch.Segments {
			c.keys[r.Key] = struct{}{}
			c.ranges = append(c.ranges, segRange{r.First, r.Last, r.Key})
			if r.Last > c.highest {
				c.highest = r.Last
			}
		}
		c.cur, c.curRefs = i, ch.Segments
	}
	sort.Slice(c.ranges, func(i, j int) bool { return c.ranges[i].first < c.ranges[j].first })
	return c, nil
}

// checkOverlap rejects a ref whose heights intersect a committed segment with
// a different key. c.mu must be held.
func (c *Committer) checkOverlap(ref SegmentRef) error {
	for _, r := range c.ranges {
		if r.key != ref.Key && r.first <= ref.Last && ref.First <= r.last {
			return fmt.Errorf("%w: [%d,%d] intersects %s", ErrOverlap, ref.First, ref.Last, r.key)
		}
	}
	return nil
}

// Commit writes seg, idx2, the directory buckets the segment touches, the
// catalog chunk and the manifest, in that order. It is idempotent:
// re-committing a ref that is already published succeeds.
//
// Directory buckets are written no-cache before the catalog, so a catalogued
// height always has its entry. A bucket is frozen (rewritten with immutable
// headers) only after the catalog write that completes its coverage, so a
// crash before the catalog never leaves an immutable bucket pointing at an
// uncatalogued segment.
//
// The segment is written scrubbed with the current tombstone set, and
// re-scrubbed after the catalog write if ApplyTombstones installed a newer
// set meanwhile. A stored segment that differs from ref only by tombstones
// (an orphan of an earlier attempt) is not ErrExists; it is brought to the
// current set. A stored segment that differs before any tombstone set was
// applied is ErrNoTombstoneSet.
//
// After the catalog write, the OP keys of every blob of the current set in
// the segment are deleted (see ApplyTombstones); a failed delete fails Commit.
func (c *Committer) Commit(ctx context.Context, ref SegmentRef) error {
	if ref.seg == nil || ref.idx == nil || ref.rows == nil {
		return errors.New("commit: segment ref carries no data (use a ref from Builder.Flush)")
	}
	entries, err := dirEntries(ref)
	if err != nil {
		return err
	}
	c.mu.Lock()
	err = c.checkOverlap(ref)
	used, hook := c.tomb, c.onScrub
	c.mu.Unlock()
	if err != nil {
		return err
	}
	// Best effort: the segment is scrubbed with the current tombstone set
	// before its first write. A record that cannot be scrubbed is written as
	// it is and reported to the tombstone hook (cba_tombstone_errors_total).
	// Scrub is deterministic, so this is not retried; the operator must act.
	seg := ref.seg
	if touches(used, ref.First, ref.Last) {
		out, _, serr := tombstone.Scrub(ref.seg, used)
		if out != nil {
			seg = out
		}
		if serr != nil {
			report(hook, ref.Key, 0, serr)
		}
	}
	if err := c.st.PutIfAbsent(ctx, ref.Key, seg, segmentBinary); err != nil {
		if !errors.Is(err, store.ErrExists) {
			return fmt.Errorf("put %s: %w", ref.Key, err)
		}
		if err := c.reconcile(ctx, ref); err != nil {
			return fmt.Errorf("put %s: %w", ref.Key, err)
		}
	}
	idxKey := IndexKey(ref.First, ref.Last)
	if err := c.st.PutIfAbsent(ctx, idxKey, ref.idx, immutableBinary); err != nil {
		return fmt.Errorf("put %s: %w", idxKey, err)
	}
	if c.beforeCatalog != nil {
		c.beforeCatalog()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.keys[ref.Key]; !ok {
		// Re-check: a concurrent commit may have landed since the first check.
		if err := c.checkOverlap(ref); err != nil {
			return err
		}
		if err := c.writeDir(ctx, ref, entries); err != nil {
			return err
		}
		if err := c.addToCatalog(ctx, ref); err != nil {
			// The stored buckets now point at an uncatalogued segment;
			// reload (and clean) them on the next write.
			c.dropBuckets(ref)
			return err
		}
		for b := dirBucket(ref.First); b <= dirBucket(ref.Last); b++ {
			if c.covers(b*DirBucket+1, (b+1)*DirBucket) {
				c.unfrozen[b] = struct{}{}
			}
		}
	}
	// A takedown that ran after the scrub above may have missed this segment
	// (it was not catalogued, and maybe not listed yet). From here on the
	// segment is catalogued, so every later takedown finds it.
	if newer(used, c.tomb) && touches(c.tomb, ref.First, ref.Last) {
		if _, _, err := c.scrubKey(ctx, ref.Key, c.tomb); err != nil {
			return err
		}
	}
	// An OP object written before a takedown whose ApplyTombstones did not
	// find this segment (not stored or not listed yet, or never stored before
	// a crash) is deleted here, with the set in force. A retry repeats it.
	// A blob whose keys cannot be derived is deterministic, like a record
	// that cannot be scrubbed: reported for the operator, not retried.
	if touches(c.tomb, ref.First, ref.Last) {
		bs, werr := oplayer.Withdrawn(ref.seg, c.tomb)
		if werr != nil {
			report(c.onScrub, ref.Key, 0, fmt.Errorf("op keys of withdrawn blobs: %w", werr))
		}
		if _, err := oplayer.Delete(ctx, c.st, bs); err != nil {
			return fmt.Errorf("%w of %s: %w", ErrOPDelete, ref.Key, err)
		}
	}
	c.freeze(ctx)
	m := Manifest{
		FormatVersion: FormatVersion, ChainID: c.chainID,
		HighestHeight: c.highest, ContiguousUntil: c.contiguousUntil(),
		LastCatalogChunk: c.cur, UpdatedAt: c.now().UTC(),
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := c.st.Put(ctx, ManifestKey, b, mutableJSON); err != nil {
		return fmt.Errorf("put manifest: %w", err)
	}
	return nil
}

// reconcile handles ErrExists on the segment key. The stored segment may be
// an earlier write of the same segment (an orphan of a failed commit) that is
// scrubbed with an older or newer tombstone set than ref was. If scrubbing
// both with the current set gives the same bytes, the stored object is
// brought to those bytes; otherwise the error stands. Before ApplyTombstones
// has run the stored copy may be scrubbed by a set not loaded yet, so the
// conflict is ErrNoTombstoneSet, not ErrExists: ErrExists stops the ingester
// and invites deleting the stored copy, which would undo a takedown.
func (c *Committer) reconcile(ctx context.Context, ref SegmentRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tomb == nil {
		return fmt.Errorf("%w: %s differs from the stored copy", ErrNoTombstoneSet, ref.Key)
	}
	exists := fmt.Errorf("%w: %s", store.ErrExists, ref.Key)
	if !touches(c.tomb, ref.First, ref.Last) {
		return exists
	}
	have, err := c.st.Get(ctx, ref.Key)
	if err != nil {
		return fmt.Errorf("read %s: %w", ref.Key, err)
	}
	// Records that cannot be scrubbed stay as they are in both copies.
	want, _, werr := tombstone.Scrub(ref.seg, c.tomb)
	got, n, gerr := tombstone.Scrub(have, c.tomb)
	if want == nil || got == nil || !bytes.Equal(got, want) {
		return errors.Join(exists, werr, gerr)
	}
	if gerr != nil {
		report(c.onScrub, ref.Key, 0, gerr)
	}
	if n == 0 {
		return nil
	}
	if err := c.st.Put(ctx, ref.Key, want, segmentBinary); err != nil {
		return fmt.Errorf("write scrubbed %s: %w", ref.Key, err)
	}
	report(c.onScrub, ref.Key, n, nil)
	return nil
}

// SetTombstoneHook sets the function called after a stored segment was
// scrubbed (scrubbed > 0 records overwritten) or when a record could not be
// scrubbed (err != nil). Default: errors are logged with slog.Default.
func (c *Committer) SetTombstoneHook(f func(key string, scrubbed int, err error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onScrub = f
}

// SetOPWriter sets the OP writer that ApplyTombstones installs each set into.
// Call it before the first ApplyTombstones.
func (c *Committer) SetOPWriter(w *oplayer.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opw = w
}

// ApplyTombstones makes s the current tombstone set and scrubs every stored
// segment that may hold a blob of s: the catalogued segment of each
// tombstoned height and every segments/*.seg whose range covers one (orphans
// of failed commits are public too). Only segments that change are written,
// so a second call with the same set writes nothing. It returns the number of
// records overwritten. Later commits scrub new segments with s, and the OP
// writer, if set, stops writing the blobs of s before the takedown starts.
//
// It also deletes OP keys, whatever namespaces the OP writer has (one may
// have been removed since): the compact key (the one written) of every blob
// of s, derived from s alone, and both keys of every blob of s found in
// those segments. Deletes run on every call; a missing key is success. A
// compact key is thus deleted even if no readable segment holds its blob;
// for the full key (never written today) and for Commit: an OP write that saw
// the previous set finished before the writer took s, so its segment is
// either stored now, and found here, or not catalogued yet, and its Commit
// deletes under c.mu with s.
//
// Store errors (reads, writes, deletes) are returned joined, after every
// segment was tried; records that cannot be scrubbed or whose OP keys cannot
// be derived are reported to the tombstone hook only. c.mu is held
// throughout, so commits wait for the takedown; each delete is bounded by
// oplayer.Delete's timeout.
func (c *Committer) ApplyTombstones(ctx context.Context, s *tombstone.Set) (scrubbed int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tomb = s
	if c.opw != nil {
		// Waits for OP writes in flight under the previous set.
		c.opw.SetTombstones(s)
	}
	heights := s.Heights()
	if len(heights) == 0 {
		return 0, nil
	}
	keys := map[string]struct{}{}
	for _, r := range c.ranges {
		if inRange(heights, r.first, r.last) {
			keys[r.key] = struct{}{}
		}
	}
	var errs []error
	listed, err := c.st.List(ctx, "segments/")
	if err != nil {
		errs = append(errs, fmt.Errorf("list segments: %w", err))
	}
	for _, k := range listed {
		if first, last, ok := ParseSegmentKey(k); ok && inRange(heights, first, last) {
			keys[k] = struct{}{}
		}
	}
	// The compact key, the one the writer writes, comes straight from the
	// set, so it is deleted even if no stored segment holds the blob or the
	// segment cannot be read. The full key needs the share range and square
	// width, which only a segment has. A blob may be in a catalogued segment
	// and an orphan: each key is deleted once.
	opKeys := map[string]struct{}{}
	for _, k := range oplayer.SetKeys(s) {
		opKeys[k] = struct{}{}
	}
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		n, bs, err := c.scrubKey(ctx, k, s)
		scrubbed += n
		if err != nil {
			errs = append(errs, err)
		}
		// Withdrawn checked every blob's ID, so this cannot fail.
		ks, kerr := oplayer.BlobKeys(bs)
		if kerr != nil {
			report(c.onScrub, k, 0, fmt.Errorf("op keys of withdrawn blobs: %w", kerr))
		}
		for _, ok := range ks {
			opKeys[ok] = struct{}{}
		}
	}
	if _, err := oplayer.DeleteKeys(ctx, c.st, slices.Sorted(maps.Keys(opKeys))); err != nil {
		errs = append(errs, fmt.Errorf("delete op objects: %w", err))
	}
	return scrubbed, errors.Join(errs...)
}

// scrubKey scrubs the stored segment key with s and rewrites it if anything
// changed. It also returns the blobs of s the segment holds, read from the
// stored bytes (scrubbed or not). c.mu must be held.
func (c *Committer) scrubKey(ctx context.Context, key string, s *tombstone.Set) (int, []oplayer.Blob, error) {
	seg, err := c.st.Get(ctx, key)
	if err != nil {
		return 0, nil, fmt.Errorf("read %s for scrub: %w", key, err)
	}
	withdrawn, werr := oplayer.Withdrawn(seg, s)
	if werr != nil {
		werr = fmt.Errorf("op keys of withdrawn blobs: %w", werr)
	}
	out, n, serr := tombstone.Scrub(seg, s)
	// One report per segment: a segment that does not parse fails both.
	if serr != nil || werr != nil {
		report(c.onScrub, key, 0, errors.Join(serr, werr))
	}
	if n == 0 {
		return 0, withdrawn, nil
	}
	if err := c.st.Put(ctx, key, out, segmentBinary); err != nil {
		return 0, withdrawn, fmt.Errorf("write scrubbed %s: %w", key, err)
	}
	report(c.onScrub, key, n, nil)
	return n, withdrawn, nil
}

func report(hook func(string, int, error), key string, n int, err error) {
	switch {
	case hook != nil:
		hook(key, n, err)
	case err != nil:
		slog.Warn("tombstone scrub failed", "key", key, "err", err)
	}
}

// touches reports whether s tombstones a blob at a height in [first, last].
func touches(s *tombstone.Set, first, last int64) bool {
	return s != nil && inRange(s.Heights(), first, last)
}

// inRange reports whether the sorted heights hs hold one in [first, last].
func inRange(hs []int64, first, last int64) bool {
	i, _ := slices.BinarySearch(hs, first)
	return i < len(hs) && hs[i] <= last
}

// newer reports whether cur is a different tombstone set than used.
func newer(used, cur *tombstone.Set) bool {
	return cur != nil && (used == nil || used.Version() != cur.Version())
}

// addToCatalog appends ref to the open chunk (opening a new one if full) and
// writes it. In-memory state changes only after the write succeeds.
func (c *Committer) addToCatalog(ctx context.Context, ref SegmentRef) error {
	cur, refs := c.cur, c.curRefs
	if len(refs) >= ChunkSize {
		cur, refs = cur+1, nil
	}
	nr := make([]SegmentRef, len(refs), len(refs)+1)
	copy(nr, refs)
	ref.seg, ref.idx, ref.rows = nil, nil, nil
	nr = append(nr, ref)
	sort.SliceStable(nr, func(i, j int) bool { return nr[i].First < nr[j].First })

	b, err := json.Marshal(CatalogChunk{Segments: nr})
	if err != nil {
		return err
	}
	// The write that fills the chunk freezes it. It must still be a Put: the
	// open version of this chunk already exists, so PutIfAbsent would fail.
	// A frozen chunk is never written again (see the len(refs) check above).
	opts := mutableJSON
	if len(nr) >= ChunkSize {
		opts = immutableJSON
	}
	if err := c.st.Put(ctx, CatalogKey(cur), b, opts); err != nil {
		return fmt.Errorf("write catalog chunk %d: %w", cur, err)
	}
	c.cur, c.curRefs = cur, nr
	c.keys[ref.Key] = struct{}{}
	i := sort.Search(len(c.ranges), func(i int) bool { return c.ranges[i].first > ref.First })
	c.ranges = append(c.ranges, segRange{})
	copy(c.ranges[i+1:], c.ranges[i:])
	c.ranges[i] = segRange{ref.First, ref.Last, ref.Key}
	if ref.Last > c.highest {
		c.highest = ref.Last
	}
	return nil
}

func dirBucket(height int64) int64 { b, _ := DirSlot(height); return b }

// dirEntries returns the directory entry of every height of ref, indexed by
// height-ref.First. Each height must have a row group led by its height row.
func dirEntries(ref SegmentRef) ([]DirEntry, error) {
	if ref.First < 1 || ref.First > ref.Last {
		return nil, fmt.Errorf("commit: bad segment range [%d,%d]", ref.First, ref.Last)
	}
	if uint64(len(ref.rows)) > math.MaxUint32 {
		return nil, fmt.Errorf("commit: %s has %d rows", ref.Key, len(ref.rows))
	}
	out := make([]DirEntry, ref.Last-ref.First+1)
	for i := 0; i < len(ref.rows); {
		h := ref.rows[i].Height
		j := i + 1
		for j < len(ref.rows) && ref.rows[j].Height == h {
			j++
		}
		if h < uint64(ref.First) || h > uint64(ref.Last) || ref.rows[i].Kind != segment.KindHeight {
			return nil, fmt.Errorf("commit: %s: bad row group for height %d", ref.Key, h)
		}
		out[h-uint64(ref.First)] = DirEntry{
			SegFirst: ref.First, SegLast: ref.Last, RowStart: uint32(i), RowCount: uint32(j - i),
		}
		i = j
	}
	for i, e := range out {
		if e.RowCount == 0 {
			return nil, fmt.Errorf("commit: %s has no rows for height %d", ref.Key, ref.First+int64(i))
		}
	}
	return out, nil
}

// writeDir sets the entries of ref in every bucket it touches and writes
// those buckets no-cache. c.mu must be held.
func (c *Committer) writeDir(ctx context.Context, ref SegmentRef, entries []DirEntry) error {
	for b := dirBucket(ref.First); b <= dirBucket(ref.Last); b++ {
		es, err := c.loadBucket(ctx, b)
		if err != nil {
			c.dropBuckets(ref)
			return err
		}
		for h := max(ref.First, b*DirBucket+1); h <= min(ref.Last, (b+1)*DirBucket); h++ {
			es[(h-1)%DirBucket] = entries[h-ref.First]
		}
		if err := c.st.Put(ctx, DirKey(b), EncodeDir(b, es), mutableBinary); err != nil {
			c.dropBuckets(ref)
			return fmt.Errorf("write %s: %w", DirKey(b), err)
		}
	}
	return nil
}

// loadBucket returns open bucket b, reading it on first use (missing = all
// empty). Entries that point at an uncatalogued segment, left by a commit
// that failed before its catalog write, are cleared. c.mu must be held.
func (c *Committer) loadBucket(ctx context.Context, b int64) (*[DirBucket]DirEntry, error) {
	if es, ok := c.open[b]; ok {
		return es, nil
	}
	raw, err := c.st.Get(ctx, DirKey(b))
	var es *[DirBucket]DirEntry
	switch {
	case errors.Is(err, store.ErrNotFound):
		es = new([DirBucket]DirEntry)
	case err != nil:
		return nil, fmt.Errorf("read %s: %w", DirKey(b), err)
	default:
		got, d, err := DecodeDir(raw)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", DirKey(b), err)
		}
		if got != b {
			return nil, fmt.Errorf("decode %s: holds bucket %d", DirKey(b), got)
		}
		es = d
		for i, e := range es {
			if e != (DirEntry{}) && !c.catalogued(e.SegFirst, e.SegLast) {
				es[i] = DirEntry{}
			}
		}
	}
	c.open[b] = es
	return es, nil
}

// dropBuckets forgets the in-memory buckets ref touches. c.mu must be held.
func (c *Committer) dropBuckets(ref SegmentRef) {
	for b := dirBucket(ref.First); b <= dirBucket(ref.Last); b++ {
		delete(c.open, b)
	}
}

// SetFreezeErrorHook sets the function called when freezing a directory
// bucket fails (default: log with slog.Default). Freezing only changes
// caching headers, so a failure does not fail Commit; the bucket stays
// no-cache and is retried by the next Commit.
func (c *Committer) SetFreezeErrorHook(f func(key string, err error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onFreezeErr = f
}

// freeze writes every completely covered bucket with immutable headers and
// drops it from memory. Failures are reported and the bucket stays pending.
// c.mu must be held.
func (c *Committer) freeze(ctx context.Context) {
	buckets := make([]int64, 0, len(c.unfrozen))
	for b := range c.unfrozen {
		buckets = append(buckets, b)
	}
	slices.Sort(buckets)
	for _, b := range buckets {
		es, err := c.loadBucket(ctx, b)
		if err == nil {
			err = c.st.Put(ctx, DirKey(b), EncodeDir(b, es), immutableBinary)
		}
		if err != nil {
			if c.onFreezeErr != nil {
				c.onFreezeErr(DirKey(b), err)
			} else {
				slog.Warn("directory freeze failed; retried on next commit", "key", DirKey(b), "err", err)
			}
			continue
		}
		delete(c.open, b)
		delete(c.unfrozen, b)
	}
}

// catalogued reports whether [first, last] is a committed segment. c.mu must
// be held.
func (c *Committer) catalogued(first, last int64) bool {
	i := sort.Search(len(c.ranges), func(i int) bool { return c.ranges[i].first >= first })
	return i < len(c.ranges) && c.ranges[i].first == first && c.ranges[i].last == last
}

// covers reports whether committed segments cover every height in [lo, hi].
// c.mu must be held.
func (c *Committer) covers(lo, hi int64) bool { return coversRanges(c.ranges, lo, hi) }

// coversRanges reports whether rs, sorted by first, cover every height in
// [lo, hi].
func coversRanges(rs []segRange, lo, hi int64) bool {
	until := lo - 1
	for _, r := range rs {
		if r.last < lo {
			continue
		}
		if r.first > until+1 {
			break
		}
		until = max(until, r.last)
	}
	return until >= hi
}

// contiguousUntil is the largest H such that heights 1..H are all covered.
func (c *Committer) contiguousUntil() int64 {
	var until int64
	for _, r := range c.ranges {
		if r.first > until+1 {
			break
		}
		if r.last > until {
			until = r.last
		}
	}
	return until
}

// ContiguousUntil returns the current gap-free prefix end.
func (c *Committer) ContiguousUntil() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.contiguousUntil()
}

// CommittedRanges returns the [first, last] height ranges of all committed
// segments, sorted by first. The slice is a copy.
func (c *Committer) CommittedRanges() [][2]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][2]int64, len(c.ranges))
	for i, r := range c.ranges {
		out[i] = [2]int64{r.first, r.last}
	}
	return out
}

// HighestHeight returns the highest committed height, or 0 if none.
func (c *Committer) HighestHeight() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.highest
}
