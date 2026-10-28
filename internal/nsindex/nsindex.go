// Package nsindex maintains a per-namespace index of archived blobs, so a
// reader can list the blobs of one namespace without scanning segments.
//
// Layout (see docs/format.md, "Namespace index"):
//
//	ns/<ns_hex>/<seq:08d>.json  chunk of up to 10000 entries
//	ns/<ns_hex>/head.json       {last_seq, blobs, first_height, last_height}
//	ns/_progress.json           {last_height}, written last
package nsindex

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// ProgressKey holds the last height whose blobs are indexed.
const ProgressKey = "ns/_progress.json"

// ChunkEntries is the maximum number of entries per chunk.
const ChunkEntries = 10000

// chunkEntries is ChunkEntries; tests lower it.
var chunkEntries = ChunkEntries

// flushEntries bounds the entries buffered in memory before they are written.
const flushEntries = 50000

// Entry locates one blob record in a segment.
type Entry struct {
	Height     int64  `json:"height"`
	Commitment string `json:"commitment"`
	SegmentKey string `json:"segment_key"`
	Offset     uint64 `json:"offset"`
	Length     uint32 `json:"length"`
}

// Chunk is one ns/<ns_hex>/NNNNNNNN.json object.
type Chunk struct {
	Entries []Entry `json:"entries"`
}

// Head is ns/<ns_hex>/head.json. It is the authority on how much of the
// namespace is indexed: chunks may hold more entries after a crash.
type Head struct {
	LastSeq     int   `json:"last_seq"`
	Blobs       int   `json:"blobs"`
	FirstHeight int64 `json:"first_height"`
	LastHeight  int64 `json:"last_height"`
}

// Progress is ns/_progress.json.
type Progress struct {
	LastHeight int64 `json:"last_height"`
}

// HeadKey is the key of the head object of a namespace.
func HeadKey(nsHex string) string { return "ns/" + nsHex + "/head.json" }

// ChunkKey is the key of chunk seq of a namespace.
func ChunkKey(nsHex string, seq int) string { return fmt.Sprintf("ns/%s/%08d.json", nsHex, seq) }

func getJSON(ctx context.Context, st store.Store, key string, v any) (bool, error) {
	b, err := st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read %s: %w", key, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, fmt.Errorf("decode %s: %w", key, err)
	}
	return true, nil
}

// putJSON writes v as JSON. frozen marks an object that is never written
// again (a full chunk), which may then be cached forever.
func putJSON(ctx context.Context, st store.Store, key string, v any, frozen bool) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	opts := store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}
	if frozen {
		opts.CacheControl = store.CacheImmutable
	}
	if err := st.Put(ctx, key, b, opts); err != nil {
		return fmt.Errorf("write %s: %w", key, err)
	}
	return nil
}

// nsState is the in-memory state of one namespace during an Update.
type nsState struct {
	head   Head
	loaded int64           // head.LastHeight as loaded; entries up to it are already indexed
	seq    int             // chunk being appended to
	open   []Entry         // its entries
	closed map[int][]Entry // chunks filled during this update
	dirty  bool            // open was changed during this update
}

func loadNS(ctx context.Context, st store.Store, nsHex string) (*nsState, error) {
	s := &nsState{closed: map[int][]Entry{}}
	ok, err := getJSON(ctx, st, HeadKey(nsHex), &s.head)
	if err != nil || !ok {
		return s, err
	}
	s.seq = s.head.LastSeq
	s.loaded = s.head.LastHeight
	var c Chunk
	if ok, err := getJSON(ctx, st, ChunkKey(nsHex, s.seq), &c); err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("namespace %s: head points at missing chunk %d", nsHex, s.seq)
	}
	// A crash after the chunk write but before the head write leaves extra
	// entries; the head says how many belong to the index.
	keep := s.head.Blobs - s.seq*chunkEntries
	if keep < 0 || keep > len(c.Entries) {
		return nil, fmt.Errorf("namespace %s: head has %d blobs but chunk %d holds %d", nsHex, s.head.Blobs, s.seq, len(c.Entries))
	}
	s.open = c.Entries[:keep]
	return s, nil
}

func (s *nsState) add(e Entry) {
	if s.loaded > 0 && e.Height <= s.loaded {
		return // already indexed before a crash
	}
	if len(s.open) >= chunkEntries {
		if s.dirty {
			s.closed[s.seq] = s.open
		}
		s.seq++
		s.dirty = false
		s.open = nil
	}
	s.open = append(s.open, e)
	s.dirty = true
	if s.head.Blobs == 0 {
		s.head.FirstHeight = e.Height
	}
	s.head.Blobs++
	s.head.LastHeight = e.Height
	s.head.LastSeq = s.seq
}

// Metrics are the namespace indexer's Prometheus metrics.
type Metrics struct {
	Errors     prometheus.Counter
	LastHeight prometheus.Gauge
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Errors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_nsindex_errors_total", Help: "Namespace index updates that failed; retried on the next tick.",
		}),
		LastHeight: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_nsindex_last_height", Help: "Last height whose blobs are in the namespace index.",
		}),
	}
}

// Update indexes the blobs of all segments in (progress, ContiguousUntil].
func Update(ctx context.Context, st store.Store) error {
	_, err := update(ctx, st)
	return err
}

// update is Update that also returns the last indexed height.
func update(ctx context.Context, st store.Store) (int64, error) {
	man, err := archiveread.LoadManifest(ctx, st)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var prog Progress
	if _, err := getJSON(ctx, st, ProgressKey, &prog); err != nil {
		return 0, err
	}
	if man.ContiguousUntil <= prog.LastHeight {
		return prog.LastHeight, nil
	}
	all, err := archiveread.LoadCatalog(ctx, st)
	if err != nil {
		return 0, err
	}
	var refs []archive.SegmentRef
	for _, r := range all {
		if r.First > prog.LastHeight && r.Last <= man.ContiguousUntil {
			refs = append(refs, r)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].First < refs[j].First })

	states := map[string]*nsState{}
	buffered := 0
	flush := func(upTo int64) error {
		keys := make([]string, 0, len(states))
		for k := range states {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, ns := range keys {
			s := states[ns]
			for seq, es := range s.closed {
				if err := putJSON(ctx, st, ChunkKey(ns, seq), Chunk{Entries: es}, true); err != nil {
					return err
				}
			}
			if s.dirty {
				if err := putJSON(ctx, st, ChunkKey(ns, s.seq), Chunk{Entries: s.open}, len(s.open) >= chunkEntries); err != nil {
					return err
				}
			}
			if err := putJSON(ctx, st, HeadKey(ns), s.head, false); err != nil {
				return err
			}
		}
		if err := putJSON(ctx, st, ProgressKey, Progress{LastHeight: upTo}, false); err != nil {
			return err
		}
		states, buffered = map[string]*nsState{}, 0
		return nil
	}

	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rows, err := LoadBlobRows(ctx, st, ref)
		if err != nil {
			return 0, err
		}
		for _, e := range rows {
			ns := hex.EncodeToString(e.Namespace[:])
			s := states[ns]
			if s == nil {
				if s, err = loadNS(ctx, st, ns); err != nil {
					return 0, err
				}
				states[ns] = s
			}
			s.add(Entry{
				Height: int64(e.Height), Commitment: hex.EncodeToString(e.Commitment[:]),
				SegmentKey: ref.Key, Offset: e.Offset, Length: e.Length,
			})
			buffered++
		}
		if buffered >= flushEntries {
			if err := flush(ref.Last); err != nil {
				return 0, err
			}
		}
	}
	if len(refs) == 0 {
		return prog.LastHeight, nil
	}
	last := refs[len(refs)-1].Last
	return last, flush(last)
}

// ReadEntries returns the indexed entries of namespace nsHex in index order,
// honouring head.json as the authority on how many are valid (a chunk may
// hold more after a crash). ok is false if the namespace has no head.json.
func ReadEntries(ctx context.Context, st store.Store, nsHex string) (entries []Entry, ok bool, err error) {
	hb, err := st.Get(ctx, HeadKey(nsHex))
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read namespace head: %w", err)
	}
	var head Head
	if err := json.Unmarshal(hb, &head); err != nil {
		return nil, false, fmt.Errorf("decode namespace head: %w", err)
	}
	remaining := head.Blobs
	for seq := 0; seq <= head.LastSeq && remaining > 0; seq++ {
		cb, err := st.Get(ctx, ChunkKey(nsHex, seq))
		if err != nil {
			return nil, false, fmt.Errorf("read namespace chunk %d: %w", seq, err)
		}
		var c Chunk
		if err := json.Unmarshal(cb, &c); err != nil {
			return nil, false, fmt.Errorf("decode namespace chunk %d: %w", seq, err)
		}
		es := c.Entries[:min(len(c.Entries), remaining)]
		remaining -= len(es)
		entries = append(entries, es...)
	}
	if remaining != 0 {
		return nil, false, fmt.Errorf("namespace %s: head lists %d blobs but chunks hold %d fewer", nsHex, head.Blobs, remaining)
	}
	return entries, true, nil
}

// LoadBlobRows reads the whole row index of ref and returns its blob rows in
// segment (chain) order. The index must cover exactly ref's heights.
func LoadBlobRows(ctx context.Context, st store.Store, ref archive.SegmentRef) ([]segment.Row, error) {
	key := archive.IndexKey(ref.First, ref.Last)
	b, err := st.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read index %s: %w", key, err)
	}
	first, last, rows, err := segment.DecodeRows(b)
	if err != nil {
		return nil, fmt.Errorf("%w: decode index %s: %v", archiveread.ErrCorrupt, key, err)
	}
	if int64(first) != ref.First || int64(last) != ref.Last {
		return nil, fmt.Errorf("%w: index %s covers [%d,%d], catalog says [%d,%d]", archiveread.ErrCorrupt, key, first, last, ref.First, ref.Last)
	}
	blobs := slices.DeleteFunc(rows, func(r segment.Row) bool { return r.Kind != segment.KindBlob })
	slices.SortFunc(blobs, func(a, b segment.Row) int { return cmp.Compare(a.Offset, b.Offset) })
	return blobs, nil
}

// tick runs one Update and records its outcome on m.
func tick(ctx context.Context, st store.Store, m *Metrics) {
	last, err := update(ctx, st)
	if err != nil {
		if ctx.Err() == nil {
			m.Errors.Inc()
			slog.Error("namespace index update failed, will retry", "err", err)
		}
		return
	}
	m.LastHeight.Set(float64(last))
}

// Run calls Update every interval until ctx is done, then returns ctx.Err().
// Update errors are counted, logged and retried on the next tick.
func Run(ctx context.Context, st store.Store, every time.Duration, m *Metrics) error {
	if every <= 0 {
		return errors.New("nsindex: interval must be positive")
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		tick(ctx, st, m)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
