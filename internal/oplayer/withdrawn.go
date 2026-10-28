package oplayer

import (
	"context"
	"errors"
	"fmt"
	"time"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// Withdrawn returns the Blob of every record of seg (blob or tombstone) that s
// lists, in any namespace. The square width comes from the record of that
// height. A tombstone keeps the share range, so a scrubbed segment gives the
// same blobs as the raw one. If a listed record's blob cannot be derived, the
// others are returned with an error naming it; the error is deterministic.
func Withdrawn(seg []byte, s *tombstone.Set) ([]Blob, error) {
	rows, err := segment.ScanRows(seg)
	if err != nil {
		return nil, err
	}
	var (
		out  []Blob
		errs []error
		// Rows are sorted by height, then kind: a height's record comes first.
		height, width uint64
		haveWidth     bool
	)
	for _, r := range rows {
		if r.Kind == segment.KindHeight {
			// A bad height record skips that height's blobs only.
			rec, err := segment.ReadAt(seg, r.Offset, r.Length)
			height, haveWidth = r.Height, err == nil
			if err != nil {
				errs = append(errs, fmt.Errorf("height %d: %w", r.Height, err))
				continue
			}
			width = rec.GetHeight().GetSquareSize()
			continue
		}
		if !s.Has(int64(r.Height), r.Namespace[:], r.Commitment[:]) {
			continue
		}
		if !haveWidth || height != r.Height {
			errs = append(errs, fmt.Errorf("withdrawn blob at height %d: no height record", r.Height))
			continue
		}
		rec, err := segment.ReadAt(seg, r.Offset, r.Length)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		t := rec.GetTombstone()
		if b := rec.GetBlob(); b != nil {
			t = &archivev1.TombstoneRecord{
				Height: b.Height, Namespace: b.Namespace, Commitment: b.Commitment,
				ShareStart: b.ShareStart, ShareEnd: b.ShareEnd,
			}
		}
		ob, err := FromTombstone(t, width)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, ob)
	}
	return out, errors.Join(errs...)
}

// deleteTimeout bounds one key's delete. The committer deletes under its
// lock, so a hung request must not stall commits for longer.
const deleteTimeout = 30 * time.Second

// CompactKey returns the compact-ID OP key of the blob (height, ns,
// commitment). It needs no share range, so it can be derived from a
// tombstone entry alone.
func CompactKey(height uint64, ns [namespaceSize]byte, commitment [commitmentSize]byte) string {
	return Key(ns, Commitment(BlobID{Height: height, Commitment: commitment}.Compact()))
}

// SetKeys returns the compact OP key of every blob s lists, in any namespace.
func SetKeys(s *tombstone.Set) []string {
	var keys []string
	for _, e := range s.Entries() {
		keys = append(keys, CompactKey(uint64(e.Height), e.Namespace, e.Commitment))
	}
	return keys
}

// BlobKeys returns both OP keys (compact and full) of every blob in bs, and
// the errors of blobs whose keys cannot be derived, joined.
func BlobKeys(bs []Blob) ([]string, error) {
	var keys []string
	var errs []error
	for _, b := range bs {
		k, err := b.Keys()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		keys = append(keys, k[:]...)
	}
	return keys, errors.Join(errs...)
}

// Delete deletes both OP keys (compact and full) of every blob in bs, in any
// namespace. A missing key counts as deleted. It tries every key, each within
// deleteTimeout, and returns the number of keys deleted with the errors
// joined.
func Delete(ctx context.Context, st store.Store, bs []Blob) (deleted int, err error) {
	keys, kerr := BlobKeys(bs)
	deleted, err = DeleteKeys(ctx, st, keys)
	return deleted, errors.Join(kerr, err)
}

// DeleteKeys deletes keys like Delete does.
func DeleteKeys(ctx context.Context, st store.Store, keys []string) (deleted int, err error) {
	var errs []error
	for _, k := range keys {
		dctx, cancel := context.WithTimeout(ctx, deleteTimeout)
		err := st.Delete(dctx, k)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", k, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(errs...)
}
