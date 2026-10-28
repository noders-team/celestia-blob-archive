package fibrearchive

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// IndexEntry is one final message in fibre/index/<b>.json.
type IndexEntry struct {
	Height    int64  `json:"height"`
	TxIndex   uint32 `json:"tx_index"`
	MsgIndex  uint32 `json:"msg_index"`
	Kind      string `json:"kind"`      // pff or timeout
	Namespace string `json:"namespace"` // hex, 29 B
	BlobID    string `json:"blob_id"`   // hex, 33 B
	BlobSize  uint32 `json:"blob_size"` // the promise's padded upload size
	Data      string `json:"data"`      // status name
	DataSize  uint64 `json:"data_size,omitempty"`
}

// IndexBucket is the object fibre/index/<b>.json.
type IndexBucket struct {
	Entries []IndexEntry `json:"entries"`
}

type entryKey struct {
	height            int64
	txIndex, msgIndex uint32
}

func (e IndexEntry) key() entryKey { return entryKey{e.Height, e.TxIndex, e.MsgIndex} }

// MergeIndex returns cur with add merged in, deduplicated by (height,
// tx_index, msg_index) and sorted by it. An identical duplicate is a no-op;
// a duplicate that differs is an error. cur may be nil and is not modified.
func MergeIndex(cur *IndexBucket, add []IndexEntry) (*IndexBucket, error) {
	var all []IndexEntry
	if cur != nil {
		all = append(all, cur.Entries...)
	}
	all = append(all, add...)
	seen := make(map[entryKey]IndexEntry, len(all))
	out := &IndexBucket{Entries: make([]IndexEntry, 0, len(all))}
	for _, e := range all {
		k := e.key()
		if prev, ok := seen[k]; ok {
			if prev != e {
				return nil, fmt.Errorf("index entry height %d tx %d msg %d: conflicting entries %+v and %+v", e.Height, e.TxIndex, e.MsgIndex, prev, e)
			}
			continue
		}
		seen[k] = e
		out.Entries = append(out.Entries, e)
	}
	slices.SortFunc(out.Entries, func(a, b IndexEntry) int {
		return cmp.Or(cmp.Compare(a.Height, b.Height), cmp.Compare(a.TxIndex, b.TxIndex), cmp.Compare(a.MsgIndex, b.MsgIndex))
	})
	return out, nil
}

// WriteIndex merges add into index bucket b and writes it: no-cache while the
// bucket is open, immutable when freeze is set. A missing bucket is empty.
func WriteIndex(ctx context.Context, st store.Store, b int64, add []IndexEntry, freeze bool) error {
	key := IndexKey(b)
	var cur IndexBucket
	raw, err := st.Get(ctx, key)
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return fmt.Errorf("read %s: %w", key, err)
	default:
		if err := json.Unmarshal(raw, &cur); err != nil {
			return fmt.Errorf("decode %s: %w", key, err)
		}
	}
	merged, err := MergeIndex(&cur, add)
	if err != nil {
		return fmt.Errorf("merge %s: %w", key, err)
	}
	body, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("encode %s: %w", key, err)
	}
	cc := store.CacheNoCache
	if freeze {
		cc = store.CacheImmutable
	}
	if err := st.Put(ctx, key, body, store.PutOptions{CacheControl: cc, ContentType: store.TypeJSON}); err != nil {
		return fmt.Errorf("write %s: %w", key, err)
	}
	return nil
}
