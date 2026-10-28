package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// MigrateBackupPrefix holds the v1 manifest and catalog chunks after
// MigrateV2; restoring them under their original keys rolls back to v1.
const MigrateBackupPrefix = "migrate-v1/"

// MigrateStats counts what MigrateV2 converted. AlreadyV2 means the archive
// was format 2 already and nothing was written.
type MigrateStats struct {
	Segments, Buckets int
	AlreadyV2         bool
}

// v1Ref is a format v1 catalog entry: it also names the protobuf .idx.
type v1Ref struct {
	First    int64  `json:"first"`
	Last     int64  `json:"last"`
	Key      string `json:"key"`
	IndexKey string `json:"index_key"`
	Size     int64  `json:"size"`
}

type v1Chunk struct {
	Segments []v1Ref `json:"segments"`
}

func v1IndexKey(first, last int64) string {
	return fmt.Sprintf("segments/%012d-%012d.idx", first, last)
}

// MigrateV2 converts a format v1 archive in st to format v2 in place:
//
//  1. a manifest that is already v2 means done (no writes);
//  2. the v1 manifest and catalog chunks are copied to migrate-v1/;
//  3. each catalogued segment's v1 .idx becomes a .idx2;
//  4. every directory bucket is written, immutable if the catalog covers it;
//  5. the catalog chunks are rewritten (still with index_key, see below);
//  6. the manifest is written with format_version 2, last.
//
// Segments and v1 .idx objects are not touched, and every catalog ref keeps
// its index_key, so an interrupted run leaves a complete v1 archive: v1
// readers and the v1 ingester work, the v2 ingester refuses to start. A
// rerun finishes to the same bytes: the v1 catalog is read back from
// migrate-v1/ once it is backed up, and every other write is deterministic.
func MigrateV2(ctx context.Context, st store.Store, chainID string) (MigrateStats, error) {
	raw, err := st.Get(ctx, ManifestKey)
	if err != nil {
		return MigrateStats{}, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return MigrateStats{}, fmt.Errorf("decode manifest: %w", err)
	}
	switch {
	case m.ChainID != chainID:
		return MigrateStats{}, fmt.Errorf("manifest chain_id %q does not match %q", m.ChainID, chainID)
	case m.FormatVersion == FormatVersion:
		return MigrateStats{AlreadyV2: true}, nil
	case m.FormatVersion != 1:
		return MigrateStats{}, fmt.Errorf("manifest format_version %d: only 1 can be migrated", m.FormatVersion)
	}
	if err := st.PutIfAbsent(ctx, MigrateBackupPrefix+ManifestKey, raw, mutableJSON); err != nil {
		return MigrateStats{}, fmt.Errorf("back up manifest: %w", err)
	}
	chunks, err := backupCatalog(ctx, st)
	if err != nil {
		return MigrateStats{}, err
	}

	var refs []v1Ref
	for _, ch := range chunks {
		refs = append(refs, ch.Segments...)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].First < refs[j].First })
	ranges := make([]segRange, len(refs))
	for i, r := range refs {
		if r.First < 1 || r.First > r.Last || r.Size < 0 || r.Key != SegmentKey(r.First, r.Last) {
			return MigrateStats{}, fmt.Errorf("catalog: bad segment ref %+v", r)
		}
		if r.IndexKey != v1IndexKey(r.First, r.Last) {
			return MigrateStats{}, fmt.Errorf("catalog: %s has index_key %q", r.Key, r.IndexKey)
		}
		if i > 0 && r.First <= refs[i-1].Last {
			return MigrateStats{}, fmt.Errorf("catalog: %s overlaps %s", r.Key, refs[i-1].Key)
		}
		ranges[i] = segRange{r.First, r.Last, r.Key}
	}

	// Segments are visited in height order, so a bucket is complete once the
	// next segment starts above it.
	stats := MigrateStats{Segments: len(refs)}
	open := map[int64]*[DirBucket]DirEntry{}
	flush := func(below int64) error {
		for _, b := range sortedKeys(open) {
			if (b+1)*DirBucket >= below {
				continue
			}
			opts := mutableBinary
			if coversRanges(ranges, b*DirBucket+1, (b+1)*DirBucket) {
				opts = immutableBinary
			}
			if err := st.Put(ctx, DirKey(b), EncodeDir(b, open[b]), opts); err != nil {
				return fmt.Errorf("write %s: %w", DirKey(b), err)
			}
			delete(open, b)
			stats.Buckets++
		}
		return nil
	}
	for _, r := range refs {
		if err := ctx.Err(); err != nil {
			return MigrateStats{}, err
		}
		if err := flush(r.First); err != nil {
			return MigrateStats{}, err
		}
		rows, err := readV1Rows(ctx, st, r)
		if err != nil {
			return MigrateStats{}, err
		}
		idx2, err := segment.EncodeRows(uint64(r.First), uint64(r.Last), rows)
		if err != nil {
			return MigrateStats{}, fmt.Errorf("%s: %w", r.IndexKey, err)
		}
		entries, err := dirEntries(SegmentRef{First: r.First, Last: r.Last, Key: r.Key, rows: rows})
		if err != nil {
			return MigrateStats{}, err
		}
		key := IndexKey(r.First, r.Last)
		if err := st.PutIfAbsent(ctx, key, idx2, immutableBinary); err != nil {
			return MigrateStats{}, fmt.Errorf("put %s: %w", key, err)
		}
		for h := r.First; h <= r.Last; h++ {
			b := dirBucket(h)
			if open[b] == nil {
				open[b] = new([DirBucket]DirEntry)
			}
			open[b][(h-1)%DirBucket] = entries[h-r.First]
		}
	}
	if err := flush(1<<63 - 1); err != nil {
		return MigrateStats{}, err
	}

	// The chunks keep index_key: v2 readers ignore it, and until the manifest
	// flips the bucket must stay readable by v1 code. The committer drops it
	// when it next rewrites the open chunk.
	for i, ch := range chunks {
		b, err := json.Marshal(ch)
		if err != nil {
			return MigrateStats{}, err
		}
		opts := mutableJSON
		if len(ch.Segments) >= ChunkSize {
			opts = immutableJSON
		}
		if err := st.Put(ctx, CatalogKey(i), b, opts); err != nil {
			return MigrateStats{}, fmt.Errorf("write catalog chunk %d: %w", i, err)
		}
	}

	// UpdatedAt is kept from v1 so a rerun writes the same bytes; the next
	// commit of the ingester sets it.
	m.FormatVersion, m.HighestHeight, m.ContiguousUntil, m.LastCatalogChunk = FormatVersion, 0, 0, len(chunks)-1
	if m.LastCatalogChunk < 0 {
		m.LastCatalogChunk = 0
	}
	for _, r := range ranges {
		m.HighestHeight = max(m.HighestHeight, r.last)
		if r.first <= m.ContiguousUntil+1 {
			m.ContiguousUntil = max(m.ContiguousUntil, r.last)
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return MigrateStats{}, err
	}
	if err := st.Put(ctx, ManifestKey, b, mutableJSON); err != nil {
		return MigrateStats{}, fmt.Errorf("put manifest: %w", err)
	}
	return stats, nil
}

// backupCatalog returns the v1 catalog chunks. A chunk already backed up is
// read from its backup (the live key may hold the v2 form after an
// interrupted run); otherwise the live chunk is backed up first.
func backupCatalog(ctx context.Context, st store.Store) ([]v1Chunk, error) {
	var chunks []v1Chunk
	for i := 0; ; i++ {
		key := CatalogKey(i)
		live, err := st.Get(ctx, key)
		if errors.Is(err, store.ErrNotFound) {
			return chunks, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", key, err)
		}
		raw, err := st.Get(ctx, MigrateBackupPrefix+key)
		switch {
		case errors.Is(err, store.ErrNotFound):
			raw = live
			if err := st.PutIfAbsent(ctx, MigrateBackupPrefix+key, raw, mutableJSON); err != nil {
				return nil, fmt.Errorf("back up %s: %w", key, err)
			}
		case err != nil:
			return nil, fmt.Errorf("read %s: %w", MigrateBackupPrefix+key, err)
		}
		var ch v1Chunk
		if err := json.Unmarshal(raw, &ch); err != nil {
			return nil, fmt.Errorf("decode v1 %s: %w", key, err)
		}
		chunks = append(chunks, ch)
	}
}

// readV1Rows reads the protobuf .idx of r and returns its entries as sorted
// rows. Format v1 has no tombstones: kind 1 is a height, kind 2 a blob.
func readV1Rows(ctx context.Context, st store.Store, r v1Ref) ([]segment.Row, error) {
	b, err := st.Get(ctx, r.IndexKey)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", r.IndexKey, err)
	}
	var idx archivev1.SegmentIndex
	if err := proto.Unmarshal(b, &idx); err != nil {
		return nil, fmt.Errorf("decode %s: %w", r.IndexKey, err)
	}
	if idx.GetFirstHeight() != uint64(r.First) || idx.GetLastHeight() != uint64(r.Last) {
		return nil, fmt.Errorf("%s covers [%d,%d], catalog says [%d,%d]",
			r.IndexKey, idx.GetFirstHeight(), idx.GetLastHeight(), r.First, r.Last)
	}
	rows := make([]segment.Row, len(idx.GetEntries()))
	for i, e := range idx.GetEntries() {
		row := segment.Row{Height: e.GetHeight(), Kind: e.GetKind(), Offset: e.GetOffset(), Length: e.GetLength()}
		switch e.GetKind() {
		case segment.KindHeight:
			if len(e.GetNamespace()) != 0 || len(e.GetCommitment()) != 0 {
				return nil, fmt.Errorf("%s entry %d: height entry with namespace or commitment", r.IndexKey, i)
			}
		case segment.KindBlob:
			if len(e.GetNamespace()) != len(row.Namespace) || len(e.GetCommitment()) != len(row.Commitment) {
				return nil, fmt.Errorf("%s entry %d: namespace %d bytes, commitment %d bytes",
					r.IndexKey, i, len(e.GetNamespace()), len(e.GetCommitment()))
			}
			copy(row.Namespace[:], e.GetNamespace())
			copy(row.Commitment[:], e.GetCommitment())
		default:
			return nil, fmt.Errorf("%s entry %d: unknown kind %d", r.IndexKey, i, e.GetKind())
		}
		if e.GetOffset() > uint64(r.Size) || uint64(e.GetLength()) > uint64(r.Size)-e.GetOffset() {
			return nil, fmt.Errorf("%s entry %d: record [%d,+%d) outside segment of %d bytes",
				r.IndexKey, i, e.GetOffset(), e.GetLength(), r.Size)
		}
		rows[i] = row
	}
	segment.SortRows(rows)
	return rows, nil
}

func sortedKeys(m map[int64]*[DirBucket]DirEntry) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
