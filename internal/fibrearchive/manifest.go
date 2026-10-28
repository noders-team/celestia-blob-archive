package fibrearchive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// FibreFormat is the version of the fibre/ key space.
const FibreFormat = 1

// Manifest is fibre/manifest.json.
type Manifest struct {
	FibreFormat   int               `json:"fibre_format"`
	ChainID       string            `json:"chain_id"`
	FirstHeight   int64             `json:"first_height"`
	ScannedUntil  int64             `json:"scanned_until"`
	Pending       int               `json:"pending"`
	Counts        map[string]uint64 `json:"counts"` // records written, by status name; absent = 0
	BytesArchived uint64            `json:"bytes_archived"`
	DailyCapBytes uint64            `json:"daily_cap_bytes"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// WriteManifest writes m as fibre/manifest.json (no-cache). updated_at is
// written in UTC, truncated to the second (RFC3339). Nil counts are written
// as {}.
func WriteManifest(ctx context.Context, st store.Store, m Manifest) error {
	m.UpdatedAt = m.UpdatedAt.UTC().Truncate(time.Second)
	if m.Counts == nil {
		m.Counts = map[string]uint64{}
	}
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode %s: %w", ManifestKey, err)
	}
	if err := st.Put(ctx, ManifestKey, body, store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}); err != nil {
		return fmt.Errorf("write %s: %w", ManifestKey, err)
	}
	return nil
}

// LoadManifest reads fibre/manifest.json; a missing manifest is nil, nil.
func LoadManifest(ctx context.Context, st store.Store) (*Manifest, error) {
	raw, err := st.Get(ctx, ManifestKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ManifestKey, err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("decode %s: %w", ManifestKey, err)
	}
	return &m, nil
}
