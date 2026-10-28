// Package opbackfill writes the op-alt-da fallback objects of a namespace's
// archived history, for a namespace added to the OP layer after its blobs
// were archived. It reads the archive only (namespace index, then .idx2
// above it), never the node.
//
// It lives outside internal/oplayer because it reads the archive through
// archiveread and nsindex, which import internal/archive, which imports
// oplayer.
package opbackfill

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/nsindex"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// DefaultWorkers is the number of parallel workers when
// BackfillOptions.Workers is 0.
const DefaultWorkers = 8

// BackfillOptions select what Backfill writes.
type BackfillOptions struct {
	// From and To are the inclusive height range. From 0 means 1; To 0 means
	// manifest.highest_height read at start.
	From, To int64
	Workers  int
	// DryRun reads and checks everything but writes and deletes nothing.
	DryRun bool
	// Progress, if set, is called with a snapshot after every blob, one call
	// at a time. It must be fast.
	Progress func(BackfillStats)
}

// BackfillStats count what Backfill did. Blobs counts the records of the
// namespace in range (blob or tombstone); every one is Written, Present,
// Skipped (share version not 0) or Withdrawn (a tombstone record, or listed
// in tombstones.json). Written counts compact-key writes (or, in a dry run,
// writes it would make) and Bytes their data; Present counts keys that
// already held the object (an earlier run), which are not written again.
// Deleted counts written or present blobs whose keys were deleted at the end
// because a takedown listed them during the run.
type BackfillStats struct {
	Blobs, Written, Present, Skipped, Withdrawn, Deleted int
	Bytes                                                int64
	Heights                                              [2]int64
}

// cleanupTimeout bounds the final tombstone reload and deletes. They run
// even after ctx is cancelled (Ctrl-C, SIGTERM), so a takedown that landed
// during the run is honoured.
const cleanupTimeout = 2 * time.Minute

var opBinary = store.PutOptions{CacheControl: store.CacheOP, ContentType: store.TypeBinary}

// item locates one record of the namespace.
type item struct {
	segKey string
	row    segment.Row
}

// Backfill writes the compact-key OP object of every share-version-0 blob of
// ns in [From, To] that tombstones.json does not list, with PutIfAbsent; a key
// that already holds the object is counted as present and not written. ns
// must be listed in op/_namespaces.json: the ingester then writes every
// height committed after the start, and Backfill covers the ones before.
// Each blob's data is checked against its commitment before it is written.
//
// Takedowns: the keys of every withdrawn blob in range are deleted (a missing
// key is success), so a rerun removes objects an interrupted run left. After
// all writes it reloads tombstones.json and deletes the keys of every blob it
// wrote or found present that is now listed, so a takedown landing during the
// run is not undone. This last step also runs when a write failed or ctx was
// cancelled, within cleanupTimeout. None of this deletes in a dry run.
func Backfill(ctx context.Context, st store.Store, ns [29]byte, o BackfillOptions) (BackfillStats, error) {
	nsHex := hex.EncodeToString(ns[:])
	cfg, err := oplayer.LoadConfig(ctx, st)
	if err != nil {
		return BackfillStats{}, err
	}
	if !slices.Contains(cfg, ns) {
		return BackfillStats{}, fmt.Errorf("namespace %s is not in %s: add it to CBA_OP_NAMESPACES and restart the ingester first", nsHex, oplayer.ConfigKey)
	}
	skip, err := tombstone.Load(ctx, st)
	if err != nil {
		return BackfillStats{}, err
	}
	if o.From == 0 {
		o.From = 1
	}
	if o.To == 0 {
		man, err := archiveread.LoadManifest(ctx, st)
		if err != nil {
			return BackfillStats{}, err
		}
		o.To = man.HighestHeight
	}
	if o.From < 1 || o.From > o.To {
		return BackfillStats{}, fmt.Errorf("height range [%d,%d] is empty or invalid", o.From, o.To)
	}
	if o.Workers <= 0 {
		o.Workers = DefaultWorkers
	}
	items, err := collect(ctx, st, ns, o.From, o.To)
	if err != nil {
		return BackfillStats{}, err
	}

	var (
		mu    sync.Mutex
		stats = BackfillStats{Heights: [2]int64{o.From, o.To}}
		wrote []oplayer.Blob
	)
	g, gctx := errgroup.WithContext(ctx)
	work := make(chan item)
	g.Go(func() error {
		defer close(work)
		for _, it := range items {
			select {
			case work <- it:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for range o.Workers {
		g.Go(func() error {
			for it := range work {
				res, err := backfillOne(gctx, st, skip, it, o.DryRun)
				if err != nil {
					return err
				}
				mu.Lock()
				stats.Blobs++
				switch res.outcome {
				case written:
					stats.Written++
					stats.Bytes += res.bytes
					wrote = append(wrote, res.blob)
				case present:
					stats.Present++
					wrote = append(wrote, res.blob)
				case skipped:
					stats.Skipped++
				case withdrawn:
					stats.Withdrawn++
				}
				if o.Progress != nil {
					o.Progress(stats)
				}
				mu.Unlock()
			}
			return nil
		})
	}
	err = g.Wait()
	if o.DryRun || len(wrote) == 0 {
		return stats, err
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	deleted, derr := deleteWithdrawn(cctx, st, wrote)
	stats.Deleted = deleted
	return stats, errors.Join(err, derr)
}

type outcome int

const (
	written outcome = iota
	present
	skipped
	withdrawn
)

type result struct {
	outcome outcome
	bytes   int64
	blob    oplayer.Blob
}

// backfillOne reads the record of it, checks it and writes its compact key.
// A withdrawn blob's keys are deleted instead (outside a dry run).
func backfillOne(ctx context.Context, st store.Store, skip *tombstone.Set, it item, dryRun bool) (result, error) {
	rec, err := archiveread.ReadRecord(ctx, st, it.segKey, it.row)
	if err != nil {
		return result{}, fmt.Errorf("blob at height %d: %w", it.row.Height, err)
	}
	b := rec.GetBlob()
	if b == nil {
		// A scrubbed record: only the compact key can have been written.
		t := rec.GetTombstone()
		return withdrawnResult(ctx, st, dryRun, t.Height, []string{
			oplayer.CompactKey(t.Height, [29]byte(t.Namespace), [32]byte(t.Commitment)),
		})
	}
	if skip.Has(int64(b.Height), b.Namespace, b.Commitment) {
		keys := []string{oplayer.CompactKey(b.Height, [29]byte(b.Namespace), [32]byte(b.Commitment))}
		if ob, err := oplayer.FromRecord(b); err == nil {
			if k, err := ob.Keys(); err == nil {
				keys = k[:]
			}
		}
		return withdrawnResult(ctx, st, dryRun, b.Height, keys)
	}
	if b.ShareVersion != 0 {
		return result{outcome: skipped}, nil
	}
	if err := checkCommitment(b); err != nil {
		return result{}, err
	}
	ob, err := oplayer.FromRecord(b)
	if err != nil {
		return result{}, fmt.Errorf("%w: %w", oplayer.ErrBadRecord, err)
	}
	keys, err := ob.Keys()
	if err != nil {
		return result{}, fmt.Errorf("%w: %w", oplayer.ErrBadRecord, err)
	}
	ok, err := holds(ctx, st, keys[0], b.Data)
	if err != nil {
		return result{}, fmt.Errorf("blob at height %d: %w", b.Height, err)
	}
	if ok {
		return result{outcome: present, blob: ob}, nil
	}
	if !dryRun {
		if err := st.PutIfAbsent(ctx, keys[0], b.Data, opBinary); err != nil {
			return result{}, fmt.Errorf("blob at height %d: write %s: %w", b.Height, keys[0], err)
		}
	}
	return result{outcome: written, bytes: int64(len(b.Data)), blob: ob}, nil
}

// withdrawnResult deletes keys of the withdrawn blob at height (a missing key
// is success), so a rerun removes what an interrupted run left public.
func withdrawnResult(ctx context.Context, st store.Store, dryRun bool, height uint64, keys []string) (result, error) {
	if dryRun {
		return result{outcome: withdrawn}, nil
	}
	if _, err := oplayer.DeleteKeys(ctx, st, keys); err != nil {
		return result{}, fmt.Errorf("withdrawn blob at height %d: %w", height, err)
	}
	return result{outcome: withdrawn}, nil
}

// holds reports whether key already holds data. Different bytes there are
// store.ErrExists, as PutIfAbsent would report.
func holds(ctx context.Context, st store.Store, key string, data []byte) (bool, error) {
	ok, err := st.Exists(ctx, key)
	if err != nil || !ok {
		if err != nil {
			err = fmt.Errorf("check %s: %w", key, err)
		}
		return false, err
	}
	got, err := st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil // deleted meanwhile
	}
	if err != nil {
		return false, fmt.Errorf("read %s: %w", key, err)
	}
	if !bytes.Equal(got, data) {
		return false, fmt.Errorf("%s: %w", key, store.ErrExists)
	}
	return true, nil
}

// checkCommitment requires b's data to hash to its commitment, so a forged
// record is never published under a key op-alt-da trusts.
func checkCommitment(b *archivev1.BlobRecord) error {
	c, err := process.BlobCommitment(b.Namespace, b.Data, b.ShareVersion, b.Signer)
	if err != nil {
		return fmt.Errorf("blob at height %d: %w", b.Height, err)
	}
	if !bytes.Equal(c, b.Commitment) {
		return fmt.Errorf("blob at height %d: stored data does not match commitment %x", b.Height, b.Commitment)
	}
	return nil
}

// deleteWithdrawn reloads tombstones.json and deletes both OP keys of every
// blob in wrote that it lists. It returns how many blobs it deleted.
func deleteWithdrawn(ctx context.Context, st store.Store, wrote []oplayer.Blob) (int, error) {
	s, err := tombstone.Load(ctx, st)
	if err != nil {
		return 0, fmt.Errorf("reload tombstones after the backfill: a blob taken down during the run may still be public; run the backfill again over the same heights, it deletes the keys of every listed blob: %w", err)
	}
	n := 0
	var errs []error
	for _, b := range wrote {
		if !s.Has(int64(b.Height), b.Namespace[:], b.Commitment[:]) {
			continue
		}
		if _, err := oplayer.Delete(ctx, st, []oplayer.Blob{b}); err != nil {
			errs = append(errs, fmt.Errorf("withdrawn blob at height %d: %w", b.Height, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// collect lists the records of ns in [from, to]: from the namespace index up
// to its progress, and from the .idx2 of every catalogued segment above it.
// Duplicates (the same blob twice in one height) are listed once.
func collect(ctx context.Context, st store.Store, ns [29]byte, from, to int64) ([]item, error) {
	nsHex := hex.EncodeToString(ns[:])
	var prog nsindex.Progress
	if _, err := getJSON(ctx, st, nsindex.ProgressKey, &prog); err != nil {
		return nil, err
	}
	var items []item
	if from <= prog.LastHeight {
		entries, _, err := nsindex.ReadEntries(ctx, st, nsHex)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.Height < from || e.Height > min(to, prog.LastHeight) {
				continue
			}
			c, err := hex.DecodeString(e.Commitment)
			if err != nil || len(c) != 32 {
				return nil, fmt.Errorf("namespace %s index entry at height %d (%s offset %d): commitment %q is not 32 bytes of hex", nsHex, e.Height, e.SegmentKey, e.Offset, e.Commitment)
			}
			items = append(items, item{e.SegmentKey, segment.Row{
				Height: uint64(e.Height), Kind: segment.KindBlob, Namespace: ns,
				Commitment: [32]byte(c), Offset: e.Offset, Length: e.Length,
			}})
		}
	}
	refs, err := archiveread.LoadCatalog(ctx, st)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if ref.First <= prog.LastHeight || ref.First > to || ref.Last < from {
			continue
		}
		rows, err := nsindex.LoadBlobRows(ctx, st, ref)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if r.Namespace != ns || int64(r.Height) < from || int64(r.Height) > to {
				continue
			}
			items = append(items, item{ref.Key, r})
		}
	}
	slices.SortFunc(items, func(a, b item) int {
		return cmp.Or(cmp.Compare(a.row.Height, b.row.Height), bytes.Compare(a.row.Commitment[:], b.row.Commitment[:]))
	})
	return slices.CompactFunc(items, func(a, b item) bool {
		return a.row.Height == b.row.Height && a.row.Commitment == b.row.Commitment
	}), nil
}

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
