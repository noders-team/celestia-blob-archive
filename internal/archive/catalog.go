package archive

import "github.com/noders-team/celestia-blob-archive/internal/segment"

// SegmentRef describes one committed segment; its row index key is
// IndexKey(First, Last). The unexported fields carry the object bodies and
// the sorted rows from Builder.Flush to Committer.Commit and are not
// serialised. Chunks migrated from format v1 also hold an index_key per ref
// (see MigrateV2); it is ignored on read and dropped on the next rewrite.
type SegmentRef struct {
	First int64  `json:"first"`
	Last  int64  `json:"last"`
	Key   string `json:"key"`
	Size  int64  `json:"size"`

	seg, idx []byte
	rows     []segment.Row
}

// CatalogChunk is one catalog/NNNNNNNN.json object.
//
// Catalog layout: a chunk is simply the next ChunkSize (1000) committed refs,
// kept sorted by First within the chunk. Backfill commits out of order, so
// chunks do not cover disjoint height ranges; consumers locate a height by
// scanning the chunks' ranges. The last chunk is rewritten with Put until it
// holds ChunkSize refs; the write that fills it freezes it and it is never
// written again.
type CatalogChunk struct {
	Segments []SegmentRef `json:"segments"`
}
