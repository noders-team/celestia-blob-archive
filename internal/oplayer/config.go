package oplayer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// Config is the content of ConfigKey.
type Config struct {
	Namespaces []string  `json:"namespaces"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// WriteConfig overwrites ConfigKey with nss (lowercase hex, in order) and
// now. An empty list is written as such, so it clears the previous one.
func WriteConfig(ctx context.Context, st store.Store, nss [][namespaceSize]byte, now time.Time) error {
	c := Config{Namespaces: make([]string, 0, len(nss)), UpdatedAt: now.UTC()}
	for _, ns := range nss {
		c.Namespaces = append(c.Namespaces, hex.EncodeToString(ns[:]))
	}
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode %s: %w", ConfigKey, err)
	}
	opts := store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}
	if err := st.Put(ctx, ConfigKey, b, opts); err != nil {
		return fmt.Errorf("write %s: %w", ConfigKey, err)
	}
	return nil
}

// LoadConfig returns the namespaces listed in ConfigKey, validated like
// ParseNamespaces. A missing file gives an empty list.
func LoadConfig(ctx context.Context, st store.Store) ([][namespaceSize]byte, error) {
	b, err := st.Get(ctx, ConfigKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ConfigKey, err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("decode %s: %w", ConfigKey, err)
	}
	nss, err := ParseNamespaces(strings.Join(c.Namespaces, ","))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ConfigKey, err)
	}
	return nss, nil
}
