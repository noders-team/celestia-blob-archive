// Package audit samples archived heights and checks them against the chain.
package audit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// ErrMismatch means the archive disagrees with what the source derives for
// the same height (or does not cover it). Any other error from Check is a
// transport or storage problem and says nothing about the archive.
var ErrMismatch = errors.New("audit: archive differs from chain")

// Metrics are the auditor's Prometheus metrics.
type Metrics struct {
	Checks   prometheus.Counter
	Failures prometheus.Counter
	Errors   prometheus.Counter
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Checks: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_audit_checks_total", Help: "Audit samples completed (pass or fail).",
		}),
		Failures: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_audit_failures_total", Help: "Audit samples where the archive differs from the chain.",
		}),
		Errors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_audit_errors_total", Help: "Audit samples that could not run (source or store error); retried on the next tick.",
		}),
	}
}

func mismatch(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMismatch, fmt.Sprintf(format, args...))
}

// Check re-derives height from the source and compares it with the archive.
// The archived HeightRecord and BlobRecords must equal the freshly derived
// ones (same count, same order) and every archived ShareProof must validate
// against the archived data hash. A TombstoneRecord passes in place of a
// blob only if tombstones.json lists the blob and the tombstone keeps its
// height, namespace, commitment and share range; tombstones.json is loaded
// per check, only for a height holding a tombstone. Differences are reported
// as ErrMismatch.
func Check(ctx context.Context, src source.Source, st store.Store, height int64) error {
	gotHR, recs, err := archiveread.ReadHeight(ctx, st, height)
	if errors.Is(err, archiveread.ErrNotArchived) {
		return mismatch("height %d is not in the height directory", height)
	}
	if err != nil {
		// Only archive defects count: an object the directory promises is
		// missing, or it does not decode. Other store errors are transient.
		if errors.Is(err, archiveread.ErrCorrupt) || errors.Is(err, store.ErrNotFound) {
			return mismatch("height %d: %v", height, err)
		}
		return err
	}
	// tombs is loaded only if the height holds a tombstone record.
	var tombs *tombstone.Set
	for _, r := range recs {
		if r.GetTombstone() != nil {
			if tombs, err = tombstone.Load(ctx, st); err != nil {
				return fmt.Errorf("height %d: %w", height, err)
			}
			break
		}
	}
	cur, err := src.Block(ctx, height)
	if err != nil {
		return fmt.Errorf("source block %d: %w", height, err)
	}
	next, err := src.Block(ctx, height+1)
	if err != nil {
		return fmt.Errorf("source block %d: %w", height+1, err)
	}

	for i, r := range recs {
		b := r.GetBlob()
		if b == nil {
			continue // a tombstone has no proof; compared below
		}
		var sp proof.ShareProof
		if err := sp.Unmarshal(b.ShareProof); err != nil {
			return mismatch("height %d blob %d: share proof does not decode: %v", height, i, err)
		}
		if err := sp.Validate(gotHR.DataHash); err != nil {
			return mismatch("height %d blob %d: share proof does not validate against archived data hash: %v", height, i, err)
		}
	}

	wantHR, wantBlobs, err := process.Process(cur, next)
	if errors.Is(err, process.ErrCommitMismatch) {
		// The source returned blocks that do not chain; retried next tick.
		return fmt.Errorf("source blocks %d-%d: %w", height, height+1, err)
	}
	if err != nil {
		return mismatch("height %d: source block cannot be processed: %v", height, err)
	}
	if !proto.Equal(gotHR, wantHR) {
		return mismatch("height %d: height record differs", height)
	}
	if len(recs) != len(wantBlobs) {
		return mismatch("height %d: archive has %d blobs, chain has %d", height, len(recs), len(wantBlobs))
	}
	for i, want := range wantBlobs {
		if t := recs[i].GetTombstone(); t != nil {
			if err := checkTombstone(tombs, t, want); err != nil {
				return mismatch("height %d blob %d: %v", height, i, err)
			}
			continue
		}
		if !proto.Equal(recs[i].GetBlob(), want) {
			return mismatch("height %d: blob %d differs", height, i)
		}
	}
	return nil
}

// checkTombstone accepts t in place of the re-derived blob want only if the
// blob is listed in tombs and t keeps want's height, namespace, commitment
// and share range.
func checkTombstone(tombs *tombstone.Set, t *archivev1.TombstoneRecord, want *archivev1.BlobRecord) error {
	if t.Height != want.Height || !bytes.Equal(t.Namespace, want.Namespace) || !bytes.Equal(t.Commitment, want.Commitment) ||
		t.ShareStart != want.ShareStart || t.ShareEnd != want.ShareEnd {
		return fmt.Errorf("tombstone (height %d, namespace %x, commitment %x, shares [%d,%d)) differs from the chain's blob (height %d, namespace %x, commitment %x, shares [%d,%d))",
			t.Height, t.Namespace, t.Commitment, t.ShareStart, t.ShareEnd,
			want.Height, want.Namespace, want.Commitment, want.ShareStart, want.ShareEnd)
	}
	if !tombs.Has(int64(want.Height), want.Namespace, want.Commitment) {
		return errors.New("archive holds a tombstone not listed in tombstones.json")
	}
	return nil
}

// Run checks one uniformly random archived height in [1, ContiguousUntil]
// every sampleEvery until ctx is done, then returns ctx.Err(). Failures and
// errors are counted and logged, never returned.
func Run(ctx context.Context, src source.Source, st store.Store, sampleEvery time.Duration, m *Metrics) error {
	if sampleEvery <= 0 {
		return errors.New("audit: sampleEvery must be positive")
	}
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		sample(ctx, src, st, m)
	}
}

func sample(ctx context.Context, src source.Source, st store.Store, m *Metrics) {
	man, err := archiveread.LoadManifest(ctx, st)
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, store.ErrNotFound) {
			m.Errors.Inc()
			slog.Warn("audit: read manifest", "err", err)
		}
		return
	}
	if man.ContiguousUntil < 1 {
		return
	}
	height := 1 + rand.Int64N(man.ContiguousUntil)
	err = Check(ctx, src, st, height)
	switch {
	case err == nil:
		m.Checks.Inc()
		slog.Debug("audit ok", "height", height)
	case errors.Is(err, ErrMismatch):
		m.Checks.Inc()
		m.Failures.Inc()
		slog.Error("audit failure", "height", height, "reason", err)
	case ctx.Err() != nil:
	default:
		m.Errors.Inc()
		slog.Warn("audit sample failed, will retry", "height", height, "err", err)
	}
}
