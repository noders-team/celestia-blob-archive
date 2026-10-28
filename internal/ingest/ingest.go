// Package ingest runs the long-lived ingester: it backfills every missing
// height from 1 and follows the chain head, writing segments to the store.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// ErrFatalBlock wraps an error from processing a block. It is deterministic
// for that height, so retrying or restarting cannot fix it.
var ErrFatalBlock = errors.New("fatal block error")

// Config configures Run. Zero values of the optional fields mean defaults.
type Config struct {
	ChainID       string
	Workers       int           // initial limit on concurrent source calls; default 4
	MaxWorkers    int           // ceiling of that limit and number of backfill goroutines; default 8
	TailDelay     int64         // the tail stays this many blocks behind head; default 2
	RangeSize     int64         // heights per backfill range; default 10000
	LatencyTarget time.Duration // throttle p95 target; default 2s
	MetricsAddr   string        // served by cmd/ingester; unused by Run

	HeadPollInterval time.Duration // how often a caught-up tail polls head; default 2s
	BackoffBase      time.Duration // first retry delay after a source error; default 1s
	BackoffMax       time.Duration // retry delay cap; default 60s
	TombstonePoll    time.Duration // how often tombstones.json is reloaded; default 1m

	// OPNamespaces are the namespaces whose blobs get an op-alt-da fallback
	// object under op/; none if empty.
	OPNamespaces [][29]byte
}

// ProcessFunc turns block cur into archive records; next is block cur+1.
type ProcessFunc func(cur, next *source.Block) (*archivev1.HeightRecord, []*archivev1.BlobRecord, error)

// Deps are Run's collaborators. Clock, Process and Metrics may be nil for
// time.Now, process.Process and metrics on a private registry.
type Deps struct {
	Source  source.Source
	Store   store.Store
	Clock   func() time.Time
	Process ProcessFunc
	Metrics *Metrics
}

func (c Config) withDefaults() (Config, error) {
	if c.ChainID == "" {
		return c, errors.New("ingest: chain id is required")
	}
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.MaxWorkers == 0 {
		c.MaxWorkers = 8
	}
	if c.TailDelay == 0 {
		c.TailDelay = 2
	}
	if c.RangeSize == 0 {
		c.RangeSize = 10000
	}
	if c.LatencyTarget == 0 {
		c.LatencyTarget = 2 * time.Second
	}
	if c.HeadPollInterval == 0 {
		c.HeadPollInterval = 2 * time.Second
	}
	if c.BackoffBase == 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = 60 * time.Second
	}
	if c.TombstonePoll == 0 {
		c.TombstonePoll = time.Minute
	}
	switch {
	case c.Workers < 1:
		return c, fmt.Errorf("ingest: workers %d must be >= 1", c.Workers)
	case c.MaxWorkers < c.Workers:
		return c, fmt.Errorf("ingest: max workers %d must be >= workers %d", c.MaxWorkers, c.Workers)
	case c.TailDelay < 1:
		// Processing h needs block h+1, so the tail cannot reach head itself.
		return c, fmt.Errorf("ingest: tail delay %d must be >= 1", c.TailDelay)
	case c.RangeSize < 1:
		return c, fmt.Errorf("ingest: range size %d must be >= 1", c.RangeSize)
	case c.LatencyTarget < 0 || c.HeadPollInterval < 0 || c.BackoffBase < 0 || c.BackoffMax < c.BackoffBase || c.TombstonePoll < 0:
		return c, errors.New("ingest: invalid durations")
	}
	return c, nil
}

type runner struct {
	cfg     Config
	src     source.Source
	clock   func() time.Time
	process ProcessFunc
	m       *Metrics
	thr     *Throttle
	com     *archive.Committer
	st      store.Store
	log     *slog.Logger
	op      *oplayer.Writer // nil if cfg.OPNamespaces is empty

	// Version of the tombstone set last applied in full, if tombApplied.
	// Used by applyTombstones only, which never runs concurrently.
	tombVersion [32]byte
	tombApplied bool

	// tailOpen is the tail's open segment when the tail stopped on
	// cancellation, for flushTail. Set by tail, read after it returns.
	tailOpen *openSegment
}

// Run backfills all missing heights and then follows the head until ctx is
// done; on cancellation it commits the tail's open segment once before
// returning. It returns ctx.Err() on cancellation; an error wrapping
// ErrFatalBlock (and process.ErrDataRootMismatch if the block failed
// verification) if a block cannot be processed; archive.ErrOverlap; or a
// commit error after its retries are exhausted. Source errors, and errors
// loading tombstones.json before the first block, are retried forever.
// Uncommitted heights are redone on the next run.
func Run(ctx context.Context, cfg Config, deps Deps) error {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return err
	}
	if deps.Source == nil || deps.Store == nil {
		return errors.New("ingest: source and store are required")
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if deps.Process == nil {
		deps.Process = process.Process
	}
	if deps.Metrics == nil {
		deps.Metrics = NewMetrics(prometheus.NewRegistry())
	}

	com, err := archive.NewCommitter(ctx, deps.Store, cfg.ChainID)
	if err != nil {
		return fmt.Errorf("ingest: open archive: %w", err)
	}
	r := &runner{
		cfg: cfg, src: deps.Source, clock: deps.Clock, process: deps.Process, m: deps.Metrics,
		thr: NewThrottle(cfg.Workers, cfg.MaxWorkers, cfg.LatencyTarget, deps.Clock),
		com: com, st: deps.Store, log: slog.Default().With("chain_id", cfg.ChainID),
	}
	com.SetFreezeErrorHook(func(key string, err error) {
		r.m.DirFreezeErrors.Inc()
		r.log.Warn("directory freeze failed; retried on next commit", "key", key, "err", err)
	})
	com.SetTombstoneHook(r.onScrub)
	if len(cfg.OPNamespaces) > 0 {
		// The committer installs every applied tombstone set into it.
		r.op = oplayer.NewWriter(deps.Store, cfg.OPNamespaces)
		com.SetOPWriter(r.op)
	}
	r.m.OPNamespaces.Set(float64(len(cfg.OPNamespaces)))
	for _, reason := range []string{skipShareVersion, skipWithdrawn} {
		r.m.OPBlobsSkipped.WithLabelValues(reason)
	}
	// Before any worker starts, so every segment is written scrubbed with the
	// current set and a redo over a scrubbed orphan can be reconciled.
	if err := r.loadTombstones(ctx); err != nil {
		return err
	}
	if err := r.writeOPConfig(ctx); err != nil {
		return err
	}
	r.m.Workers.Set(float64(r.thr.Limit()))
	r.m.ArchivedHeight.Set(float64(com.HighestHeight()))
	r.m.ContiguousUntil.Set(float64(com.ContiguousUntil()))

	head, err := r.head(ctx)
	if err != nil {
		return err
	}
	tailStart := max(com.HighestHeight()+1, head-cfg.TailDelay)
	ranges := missingRanges(com.CommittedRanges(), tailStart-1, cfg.RangeSize)
	r.log.Info("ingester starting", "head", head, "highest", com.HighestHeight(),
		"contiguous_until", com.ContiguousUntil(), "tail_start", tailStart, "backfill_ranges", len(ranges))

	g, gctx := errgroup.WithContext(ctx)
	work := make(chan [2]int64)
	g.Go(func() error {
		defer close(work)
		for _, rg := range ranges {
			select {
			case work <- rg:
			case <-gctx.Done():
				return nil
			}
		}
		return nil
	})
	// MaxWorkers goroutines so the throttle, not the goroutine count, decides
	// how many source calls run at once.
	for range min(cfg.MaxWorkers, len(ranges)) {
		g.Go(func() error {
			for rg := range work {
				if err := r.backfill(gctx, rg[0], rg[1]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	g.Go(func() error { return r.tail(gctx, tailStart, head) })
	g.Go(func() error {
		t := time.NewTicker(cfg.TombstonePoll)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				r.applyTombstones(gctx)
			}
		}
	})

	err = g.Wait()
	switch {
	case err == nil:
		return nil // unreachable: the tail only returns on error
	case errors.Is(err, ErrFatalBlock), errors.Is(err, archive.ErrOverlap):
		r.log.Error("ingester stopped", "err", err)
		return fmt.Errorf("ingest: %w", err)
	case ctx.Err() != nil:
		r.flushTail(ctx)
		return ctx.Err()
	default:
		r.log.Error("ingester stopped", "err", err)
		return fmt.Errorf("ingest: %w", err)
	}
}

// onScrub is the committer's tombstone hook. It may run on any goroutine. A
// scrub error is deterministic (the same bytes fail the same way), so it is
// counted and logged (the error names height, namespace and commitment) for
// the operator, never retried.
func (r *runner) onScrub(key string, scrubbed int, err error) {
	if scrubbed > 0 {
		r.m.TombstoneScrubs.Add(float64(scrubbed))
		r.log.Info("tombstoned blobs scrubbed", "key", key, "records", scrubbed)
	}
	if err != nil {
		r.m.TombstoneErrors.Inc()
		r.log.Error("tombstoned blob cannot be scrubbed; needs the operator", "key", key, "err", err)
	}
}

// applyTombstones reloads tombstones.json and applies it if it changed since
// the last complete takedown. Errors are logged and counted, never fatal: on
// a load error the previous set stays in force, and a takedown cut short by a
// store error is retried on the next poll. Records that cannot be scrubbed do
// not make the takedown incomplete (see onScrub).
func (r *runner) applyTombstones(ctx context.Context) {
	s, err := tombstone.Load(ctx, r.st)
	if err != nil {
		r.m.TombstoneErrors.Inc()
		r.log.Warn("tombstone load failed; keeping the previous set", "err", err)
		return
	}
	r.apply(ctx, s)
}

// loadTombstones loads tombstones.json at start, retrying load errors with
// backoff until it succeeds or ctx is done, and applies it. Each failure is
// logged and counted. A takedown cut short by a store error is retried by the
// next poll as usual; the set is in force for commits either way.
func (r *runner) loadTombstones(ctx context.Context) error {
	delay := r.cfg.BackoffBase
	for {
		s, err := tombstone.Load(ctx, r.st)
		if err == nil {
			r.apply(ctx, s)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.m.TombstoneErrors.Inc()
		r.log.Warn("tombstone load failed at start, retrying", "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return err
		}
		delay = min(delay*2, r.cfg.BackoffMax)
	}
}

// writeOPConfig writes the OP namespace list at start (an empty list too, so
// a removed configuration is cleared), retrying store errors with backoff
// until it succeeds or ctx is done. Each failure is logged and counted.
func (r *runner) writeOPConfig(ctx context.Context) error {
	delay := r.cfg.BackoffBase
	for {
		err := oplayer.WriteConfig(ctx, r.st, r.cfg.OPNamespaces, r.clock())
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.m.OPErrors.Inc()
		r.log.Warn("op config write failed at start, retrying", "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return err
		}
		delay = min(delay*2, r.cfg.BackoffMax)
	}
}

// Reasons of cba_op_blobs_skipped_total.
const (
	skipShareVersion = "share_version"
	skipWithdrawn    = "withdrawn"
)

// writeOP writes the OP objects of one height's blobs, retrying store errors
// with jittered backoff like fetch until it succeeds or ctx is done, so a
// committed height always has its OP objects. A blob whose key cannot be
// derived is a fatal block error; store.ErrExists is returned unretried.
func (r *runner) writeOP(ctx context.Context, height int64, blobs []*archivev1.BlobRecord) error {
	if r.op == nil {
		return nil
	}
	delay := r.cfg.BackoffBase
	for {
		written, skipped, err := r.op.WriteHeight(ctx, blobs)
		if err == nil {
			r.m.OPObjectsWritten.Add(float64(written))
			r.m.OPBlobsSkipped.WithLabelValues(skipShareVersion).Add(float64(skipped.ShareVersion))
			r.m.OPBlobsSkipped.WithLabelValues(skipWithdrawn).Add(float64(skipped.Withdrawn))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, oplayer.ErrBadRecord) {
			return fmt.Errorf("%w: height %d: %w", ErrFatalBlock, height, err)
		}
		if errors.Is(err, store.ErrExists) {
			// Like commit: a key holding other bytes stays that way.
			return fmt.Errorf("height %d: %w", height, err)
		}
		// ErrNoTombstoneSet cannot happen (Run applies a set before any
		// worker starts); retrying it, like a store error, keeps the height
		// from being committed without its OP objects.
		r.m.OPErrors.Inc()
		r.log.Warn("op write failed, retrying", "height", height, "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return err
		}
		delay = min(delay*2, r.cfg.BackoffMax)
	}
}

// apply applies the loaded set s unless it was applied in full already.
func (r *runner) apply(ctx context.Context, s *tombstone.Set) {
	r.m.Tombstones.Set(float64(s.Len()))
	if r.tombApplied && s.Version() == r.tombVersion {
		return
	}
	n, err := r.com.ApplyTombstones(ctx, s)
	if err != nil {
		// Not marked applied: the next poll runs the whole takedown again.
		r.m.TombstoneErrors.Inc()
		r.log.Warn("takedown incomplete; retried on next poll", "scrubbed", n, "err", err)
		return
	}
	r.tombVersion, r.tombApplied = s.Version(), true
	r.log.Info("tombstones applied", "tombstones", s.Len(), "scrubbed", n)
}

// missingRanges returns the heights in [1, upTo] not covered by committed
// (sorted by first), split into ranges of at most size heights.
func missingRanges(committed [][2]int64, upTo, size int64) [][2]int64 {
	var out [][2]int64
	emit := func(a, b int64) {
		for a <= b {
			e := min(a+size-1, b)
			out = append(out, [2]int64{a, e})
			a = e + 1
		}
	}
	next := int64(1)
	for _, c := range committed {
		if next > upTo {
			break
		}
		if c[0] > next {
			emit(next, min(c[0]-1, upTo))
		}
		next = max(next, c[1]+1)
	}
	if next <= upTo {
		emit(next, upTo)
	}
	return out
}

// openSegment is a worker's builder plus the blob count it holds.
type openSegment struct {
	b     *archive.Builder
	blobs int
}

func (r *runner) newSegment() *openSegment {
	return &openSegment{b: archive.NewBuilder(r.clock)}
}

// backfill archives heights [first, last] and commits them.
func (r *runner) backfill(ctx context.Context, first, last int64) error {
	r.log.Debug("backfill range", "first", first, "last", last)
	seg := r.newSegment()
	cur, err := r.fetch(ctx, first, false)
	if err != nil {
		return err
	}
	for h := first; h <= last; h++ {
		next, err := r.fetch(ctx, h+1, false)
		if err != nil {
			return err
		}
		if err := r.add(ctx, seg, cur, next); err != nil {
			return err
		}
		cur = next
	}
	return r.flush(ctx, seg)
}

// tail archives heights from start onward, staying cfg.TailDelay blocks
// behind head: height h is processed once h <= head-TailDelay. When it stops
// on cancellation it leaves its open segment in r.tailOpen.
func (r *runner) tail(ctx context.Context, start, head int64) (err error) {
	seg := r.newSegment()
	defer func() {
		// Only a cancellation leaves the builder whole: other errors may come
		// from a failed Add, after which the builder must be discarded.
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			r.tailOpen = seg
		}
	}()
	var cur *source.Block
	for h := start; ; h++ {
		for h > head-r.cfg.TailDelay {
			// Idle: the open segment still closes on age.
			if seg.b.ShouldClose(r.clock()) {
				if err := r.flush(ctx, seg); err != nil {
					return err
				}
			}
			if err := sleep(ctx, r.cfg.HeadPollInterval); err != nil {
				return err
			}
			var err error
			if head, err = r.head(ctx); err != nil {
				return err
			}
		}
		if cur == nil {
			var err error
			if cur, err = r.fetch(ctx, h, true); err != nil {
				return err
			}
		}
		next, err := r.fetch(ctx, h+1, true)
		if err != nil {
			return err
		}
		if err := r.add(ctx, seg, cur, next); err != nil {
			return err
		}
		cur = next
	}
}

// add processes cur and appends it to seg, flushing seg if it should close.
func (r *runner) add(ctx context.Context, seg *openSegment, cur, next *source.Block) error {
	hr, blobs, err := r.process(cur, next)
	if err != nil {
		if errors.Is(err, process.ErrDataRootMismatch) {
			r.m.DataRootMismatch.Inc()
		}
		if errors.Is(err, process.ErrCommitMismatch) {
			// The source served blocks that do not chain, like a header that
			// does not match its block id: not fatal, a restart re-fetches them.
			return fmt.Errorf("height %d: %w", cur.Height, err)
		}
		return fmt.Errorf("%w: height %d: %w", ErrFatalBlock, cur.Height, err)
	}
	if err := r.writeOP(ctx, cur.Height, blobs); err != nil {
		return err
	}
	if err := seg.b.Add(hr, blobs); err != nil {
		return fmt.Errorf("%w: height %d: add to segment: %w", ErrFatalBlock, cur.Height, err)
	}
	seg.blobs += len(blobs)
	r.m.BlocksProcessed.Inc()
	if seg.b.ShouldClose(r.clock()) {
		return r.flush(ctx, seg)
	}
	return nil
}

// flush commits seg if it holds anything. A cancelled ctx stops it before the
// commit starts; a commit already started runs to completion so the catalog
// and manifest are not left half-written.
func (r *runner) flush(ctx context.Context, seg *openSegment) error {
	if seg.b.Empty() {
		return nil
	}
	ref, err := seg.b.Flush(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	if err := r.commit(ctx, *ref); err != nil {
		return fmt.Errorf("commit %s: %w", ref.Key, err)
	}
	r.committed(seg, ref, start)
	return nil
}

// flushTail commits the tail's open segment left by a cancelled tail, in one
// attempt bounded by commitTimeout so shutdown stays within the unit's stop
// timeout. A failure is logged and dropped: the next start redoes the heights.
// A segment already flushed and waiting between commit retries when the
// cancel arrives is not retried here; it is redone on the next start too.
func (r *runner) flushTail(ctx context.Context) {
	seg := r.tailOpen
	if seg == nil || seg.b.Empty() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	ref, err := seg.b.Flush(ctx)
	if err != nil {
		r.log.Warn("tail segment not flushed on shutdown", "err", err)
		return
	}
	start := time.Now()
	if err := r.com.Commit(ctx, *ref); err != nil {
		r.log.Warn("tail segment not committed on shutdown; redone on next start",
			"first", ref.First, "last", ref.Last, "err", err)
		return
	}
	r.committed(seg, ref, start)
}

// committed records a successful commit of seg as ref, started at start.
func (r *runner) committed(seg *openSegment, ref *archive.SegmentRef, start time.Time) {
	r.m.CommitSeconds.Observe(time.Since(start).Seconds())
	r.m.BlobsArchived.Add(float64(seg.blobs))
	seg.blobs = 0
	r.m.ArchivedHeight.Set(float64(r.com.HighestHeight()))
	r.m.ContiguousUntil.Set(float64(r.com.ContiguousUntil()))
	r.log.Info("segment committed", "first", ref.First, "last", ref.Last, "bytes", ref.Size)
}

const (
	commitAttempts = 8
	commitTimeout  = 90 * time.Second
)

// commit publishes ref, retrying transient store errors up to commitAttempts
// times with backoff. ErrOverlap and ErrExists are bugs and are not retried.
// Each attempt runs to completion even if ctx is cancelled, so the catalog and
// manifest are not left half-written; cancellation only stops further retries.
// Commit is idempotent, so a retry after a partial write is safe.
func (r *runner) commit(ctx context.Context, ref archive.SegmentRef) error {
	delay := r.cfg.BackoffBase
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		err := r.com.Commit(actx, ref)
		cancel()
		opDelete := errors.Is(err, archive.ErrOPDelete)
		if opDelete {
			// The segment is published; deleting withdrawn OP objects failed.
			r.m.OPErrors.Inc()
		}
		if err == nil || errors.Is(err, archive.ErrOverlap) || errors.Is(err, store.ErrExists) || attempt == commitAttempts {
			return err
		}
		r.m.CommitRetries.Inc()
		msg := "segment commit failed, retrying"
		if opDelete {
			msg = "op object delete failed in commit, retrying"
		}
		r.log.Warn(msg, "key", ref.Key, "attempt", attempt, "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return err
		}
		delay = min(delay*2, r.cfg.BackoffMax)
	}
}

// fetch returns block h, retrying source errors with jittered exponential
// backoff until it succeeds or ctx is done. It never gives up on a height.
// tail calls take throttle slots ahead of queued backfill calls.
func (r *runner) fetch(ctx context.Context, h int64, tail bool) (*source.Block, error) {
	delay := r.cfg.BackoffBase
	acquire := r.thr.Acquire
	if tail {
		acquire = r.thr.AcquireTail
	}
	for {
		if err := acquire(ctx); err != nil {
			return nil, err
		}
		start := time.Now()
		b, err := r.src.Block(ctx, h)
		if err == nil && (b == nil || b.Height != h) {
			err = fmt.Errorf("source returned wrong block for height %d", h)
		}
		failed := err != nil && ctx.Err() == nil
		r.thr.Release(time.Since(start), failed)
		r.m.Workers.Set(float64(r.thr.Limit()))
		if err == nil {
			return b, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		r.m.SourceErrors.Inc()
		r.log.Warn("source block failed, retrying", "height", h, "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return nil, err
		}
		delay = min(delay*2, r.cfg.BackoffMax)
	}
}

// head returns the chain head, retrying source errors like fetch.
func (r *runner) head(ctx context.Context) (int64, error) {
	delay := r.cfg.BackoffBase
	for {
		h, err := r.src.Head(ctx)
		if err == nil {
			r.m.ChainHead.Set(float64(h))
			return h, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		r.m.SourceErrors.Inc()
		r.log.Warn("source head failed, retrying", "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return 0, err
		}
		delay = min(delay*2, r.cfg.BackoffMax)
	}
}

// jitter returns a random duration in [d/2, d].
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	return d/2 + rand.N(d/2+1)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
