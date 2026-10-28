package oplayer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// ErrNoTombstoneSet means WriteHeight ran before a tombstone set was
// installed, so it cannot tell which blobs are withdrawn.
var ErrNoTombstoneSet = errors.New("oplayer: no tombstone set loaded")

// ErrBadRecord means a blob record's OP key cannot be derived. It is
// deterministic for that record, so retrying cannot fix it.
var ErrBadRecord = errors.New("oplayer: cannot derive op key")

var opBinary = store.PutOptions{CacheControl: store.CacheOP, ContentType: store.TypeBinary}

// Writer writes the OP fallback object (compact key only) of every blob in
// its namespaces that is not withdrawn. It is safe for concurrent use.
type Writer struct {
	st store.Store
	ns map[[namespaceSize]byte]struct{}

	// mu is held shared by WriteHeight and exclusively by SetTombstones, so
	// a new set is in force only once no write under the old one is running.
	mu   sync.RWMutex
	tomb *tombstone.Set
}

// NewWriter returns a Writer for namespaces. WriteHeight fails until
// SetTombstones has run.
func NewWriter(st store.Store, namespaces [][namespaceSize]byte) *Writer {
	w := &Writer{st: st, ns: make(map[[namespaceSize]byte]struct{}, len(namespaces))}
	for _, n := range namespaces {
		w.ns[n] = struct{}{}
	}
	return w
}

// Configured reports whether OP objects are written for namespace ns.
func (w *Writer) Configured(ns []byte) bool {
	if len(ns) != namespaceSize {
		return false
	}
	_, ok := w.ns[[namespaceSize]byte(ns)]
	return ok
}

// SetTombstones installs s. It returns only after every WriteHeight running
// under the previous set has finished.
func (w *Writer) SetTombstones(s *tombstone.Set) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tomb = s
}

// Skipped counts the blobs in configured namespaces WriteHeight wrote no
// object for, by reason.
type Skipped struct {
	ShareVersion int // share version is not 0
	Withdrawn    int // the tombstone set lists the blob
}

// WriteHeight writes the compact-key OP object of every blob in a configured
// namespace with share version 0 that the tombstone set does not list. Blobs
// in a configured namespace with another share version, or listed, count as
// skipped (the share version is checked first); blobs in other namespaces are
// ignored. Every key is derived before anything is written. Writes use
// PutIfAbsent, so a repeated call is a no-op. An error wrapping ErrBadRecord
// is deterministic; others are store errors.
func (w *Writer) WriteHeight(ctx context.Context, blobs []*archivev1.BlobRecord) (written int, skipped Skipped, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.tomb == nil {
		return 0, Skipped{}, ErrNoTombstoneSet
	}
	type put struct {
		key  string
		data []byte
	}
	var puts []put
	for _, r := range blobs {
		if !w.Configured(r.Namespace) {
			continue
		}
		if r.ShareVersion != 0 {
			skipped.ShareVersion++
			continue
		}
		if w.tomb.Has(int64(r.Height), r.Namespace, r.Commitment) {
			skipped.Withdrawn++
			continue
		}
		b, err := FromRecord(r)
		if err != nil {
			return 0, Skipped{}, fmt.Errorf("%w: %w", ErrBadRecord, err)
		}
		keys, err := b.Keys()
		if err != nil {
			return 0, Skipped{}, fmt.Errorf("%w: %w", ErrBadRecord, err)
		}
		puts = append(puts, put{keys[0], r.Data})
	}
	for _, p := range puts {
		if err := w.st.PutIfAbsent(ctx, p.key, p.data, opBinary); err != nil {
			return written, skipped, fmt.Errorf("write %s: %w", p.key, err)
		}
		written++
	}
	return written, skipped, nil
}
