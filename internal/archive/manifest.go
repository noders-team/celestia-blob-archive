package archive

import "time"

// Manifest is the mutable top-level pointer of the archive.
type Manifest struct {
	FormatVersion    int       `json:"format_version"`
	ChainID          string    `json:"chain_id"`
	HighestHeight    int64     `json:"highest_height"`
	ContiguousUntil  int64     `json:"contiguous_until"`
	LastCatalogChunk int       `json:"last_catalog_chunk"`
	UpdatedAt        time.Time `json:"updated_at"`
}
