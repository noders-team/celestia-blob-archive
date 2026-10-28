package fibrecapture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

var (
	// ErrNoTombstoneSet means the writer has not been given tombstones yet.
	ErrNoTombstoneSet = errors.New("fibrecapture: no tombstone set installed")
	// ErrRecordConflict means a record key already holds a different message.
	ErrRecordConflict = errors.New("fibrecapture: record key holds a different message")
)

// Writer writes fibre/ objects. Its read-write lock is the takedown barrier:
// PutData holds the read side for the whole write, SetTombstones takes the
// write side to install a new set, so once it returns no data write that
// started under the old set is still in flight.
type Writer struct {
	st   store.Store
	mu   sync.RWMutex
	tomb *tombstone.Set
	now  func() time.Time

	// DailyCapBytes is reported in the manifest.
	DailyCapBytes uint64
}

// NewWriter returns a writer to st that stamps records with now().
func NewWriter(st store.Store, now func() time.Time) *Writer {
	return &Writer{st: st, now: now}
}

// SetTombstones installs s after every in-flight PutData has returned, then
// deletes fibre/data/<id> for every Fibre ID it lists. A non-nil error joins
// the failed deletes; the caller must not treat s's version as applied.
func (w *Writer) SetTombstones(ctx context.Context, s *tombstone.Set) error {
	if s == nil {
		return errors.New("fibrecapture: nil tombstone set")
	}
	w.mu.Lock()
	w.tomb = s
	w.mu.Unlock()
	var errs []error
	for _, id := range s.FibreIDs() {
		key := fibrearchive.DataKey(id)
		if err := w.st.Delete(ctx, key); err != nil {
			errs = append(errs, fmt.Errorf("delete withdrawn %s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// Withdrawn reports whether id is listed in the installed tombstone set.
func (w *Writer) Withdrawn(id [33]byte) (bool, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.withdrawnLocked(id)
}

func (w *Writer) withdrawnLocked(id [33]byte) (bool, error) {
	if w.tomb == nil {
		return false, ErrNoTombstoneSet
	}
	return w.tomb.HasFibre(id[:]), nil
}

// PutData writes verified data as fibre/data/<id> unless id is withdrawn, in
// which case it writes nothing and returns true. The key holding other bytes
// is an error wrapping store.ErrExists.
func (w *Writer) PutData(ctx context.Context, id [33]byte, data []byte) (withdrawn bool, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	withdrawn, err = w.withdrawnLocked(id)
	if err != nil || withdrawn {
		return withdrawn, err
	}
	key := fibrearchive.DataKey(id)
	opts := store.PutOptions{CacheControl: store.CacheSegment, ContentType: store.TypeBinary}
	if err := w.st.PutIfAbsent(ctx, key, data, opts); err != nil {
		return false, fmt.Errorf("write %s: %w", key, err)
	}
	return false, nil
}

// ExistingData reads fibre/data/<id> and re-verifies it against the promise.
// A missing object gives ok=false; an object that fails verification is an
// error naming the key, never archived data.
func (w *Writer) ExistingData(ctx context.Context, id [33]byte, blobSize uint32) (size uint64, sum []byte, ok bool, err error) {
	key := fibrearchive.DataKey(id)
	data, err := w.st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, fmt.Errorf("read %s: %w", key, err)
	}
	if err := VerifyPayload(data, id, blobSize); err != nil {
		return 0, nil, false, fmt.Errorf("existing %s: %w", key, err)
	}
	h := sha256.Sum256(data)
	return uint64(len(data)), h[:], true, nil
}

// record builds the FibreRecord of a final entry.
func (w *Writer) record(e *Entry) *archivev1.FibreRecord {
	r := &archivev1.FibreRecord{
		Height:     uint64(e.Height),
		TxIndex:    e.TxIndex,
		MsgIndex:   e.MsgIndex,
		Kind:       uint32(e.Kind),
		Tx:         e.Tx,
		DataStatus: uint32(e.Status),
		DataSize:   e.DataSize,
		DataSha256: e.DataSHA256,
		CapturedAt: timestamppb.New(w.now()),
		BlockTime:  timestamppb.New(e.BlockTime),
		BlobId:     e.BlobID[:],
	}
	if e.Status == fibrearchive.StatusUnrecoverable {
		r.Error = e.LastErr
	}
	return r
}

// PutRecord writes the record of a final entry once. If the key already
// holds a record of the same message (a redo after a crash), the existing
// record is kept; any other content is a wrapped ErrRecordConflict.
func (w *Writer) PutRecord(ctx context.Context, e *Entry) error {
	key := e.Key()
	body, err := fibrearchive.EncodeRecord(w.record(e))
	if err != nil {
		return fmt.Errorf("record %s: %w", key, err)
	}
	// Records are write-once: validate before the write, as a reader would.
	if _, err := fibrearchive.DecodeRecord(body); err != nil {
		return fmt.Errorf("record %s: %w", key, err)
	}
	err = w.st.PutIfAbsent(ctx, key, body, store.PutOptions{CacheControl: store.CacheSegment, ContentType: store.TypeBinary})
	if err == nil {
		return nil
	}
	if !errors.Is(err, store.ErrExists) {
		return fmt.Errorf("write %s: %w", key, err)
	}
	raw, err := w.st.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read existing %s: %w", key, err)
	}
	have, err := fibrearchive.DecodeRecord(raw)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrRecordConflict, key, err)
	}
	want, _ := fibrearchive.DecodeRecord(body) // validated above
	if !fibrearchive.SameMessage(have, want) {
		return fmt.Errorf("%w: %s", ErrRecordConflict, key)
	}
	return nil
}

// FreezeBuckets rewrites each index bucket in buckets as immutable, in order,
// stopping at the first error. A bucket with no object is skipped: readers
// treat a missing bucket as holding no Fibre messages.
func (w *Writer) FreezeBuckets(ctx context.Context, buckets []int64) error {
	for _, b := range buckets {
		key := fibrearchive.IndexKey(b)
		ok, err := w.st.Exists(ctx, key)
		if err != nil {
			return fmt.Errorf("freeze %s: %w", key, err)
		}
		if !ok {
			continue
		}
		if err := fibrearchive.WriteIndex(ctx, w.st, b, nil, true); err != nil {
			return fmt.Errorf("freeze: %w", err)
		}
	}
	return nil
}

func indexEntry(e *Entry) fibrearchive.IndexEntry {
	return fibrearchive.IndexEntry{
		Height:    e.Height,
		TxIndex:   e.TxIndex,
		MsgIndex:  e.MsgIndex,
		Kind:      e.Kind.String(),
		Namespace: hex.EncodeToString(e.Namespace[:]),
		BlobID:    hex.EncodeToString(e.BlobID[:]),
		BlobSize:  e.BlobSize,
		Data:      e.Status.String(),
		DataSize:  e.DataSize,
	}
}

// Flush writes the index buckets of the final entries of s, frozen when
// freezeOK approves the bucket, then the manifest. It returns the record keys
// whose bucket was written; a failed bucket does not stop the others or the
// manifest, and its error is joined into err.
func (w *Writer) Flush(ctx context.Context, s *State, freezeOK func(b int64) bool) (indexed []string, err error) {
	buckets := map[int64][]string{}
	pending := 0
	for k, e := range s.Entries {
		if e.Phase != PhaseFinal {
			pending++
			continue
		}
		b := fibrearchive.Bucket(e.Height)
		buckets[b] = append(buckets[b], k)
	}
	order := make([]int64, 0, len(buckets))
	for b := range buckets {
		order = append(order, b)
	}
	slices.Sort(order)

	var errs []error
	for _, b := range order {
		keys := buckets[b]
		add := make([]fibrearchive.IndexEntry, 0, len(keys))
		for _, k := range keys {
			add = append(add, indexEntry(s.Entries[k]))
		}
		if err := fibrearchive.WriteIndex(ctx, w.st, b, add, freezeOK(b)); err != nil {
			errs = append(errs, err)
			continue
		}
		indexed = append(indexed, keys...)
	}

	m := fibrearchive.Manifest{
		FibreFormat:   fibrearchive.FibreFormat,
		ChainID:       s.ChainID,
		FirstHeight:   s.FirstHeight,
		ScannedUntil:  s.ScannedUntil,
		Pending:       pending,
		Counts:        s.Counts,
		BytesArchived: s.BytesArchived,
		DailyCapBytes: w.DailyCapBytes,
		UpdatedAt:     w.now(),
	}
	if err := fibrearchive.WriteManifest(ctx, w.st, m); err != nil {
		errs = append(errs, err)
	}
	return indexed, errors.Join(errs...)
}
