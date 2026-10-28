package fibrecapture

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/celestiaorg/celestia-app/v10/fibre"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// Downloader fetches the original bytes of a Fibre blob.
type Downloader interface {
	Download(ctx context.Context, id [33]byte, promiseHeight uint64) ([]byte, error)
}

// Params reads the chain's Fibre parameters.
type Params interface {
	Fibre(ctx context.Context) (retention, promiseTimeout time.Duration, err error)
}

// Config tunes Run. Zero values take the defaults noted.
type Config struct {
	ChainID         string
	DailyCapBytes   uint64        // default 50e9
	DownloadWorkers int           // default 2
	HeadPoll        time.Duration // default 2s
	ParamsPoll      time.Duration // default 10m
	TombstonePoll   time.Duration // default 1m
	FlushEvery      time.Duration // default 30s
	Now             func() time.Time
}

func (c *Config) setDefaults() {
	if c.DailyCapBytes == 0 {
		c.DailyCapBytes = 50_000_000_000
	}
	if c.DownloadWorkers <= 0 {
		c.DownloadWorkers = 2
	}
	if c.HeadPoll <= 0 {
		c.HeadPoll = 2 * time.Second
	}
	if c.ParamsPoll <= 0 {
		c.ParamsPoll = 10 * time.Minute
	}
	if c.TombstonePoll <= 0 {
		c.TombstonePoll = time.Minute
	}
	if c.FlushEvery <= 0 {
		c.FlushEvery = 30 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Deps are Run's collaborators. Metrics and Log may be nil.
type Deps struct {
	Source     source.Source
	Downloader Downloader
	Params     Params
	Store      store.Store
	Dir        *Dir
	Metrics    *Metrics
	Log        *slog.Logger
}

// ErrChainMismatch means state.json belongs to another chain.
var ErrChainMismatch = errors.New("fibrecapture: state chain id mismatch")

const (
	flushFinalCount = 100              // flush at once when this many entries are final
	saveEvery       = 30 * time.Second // upper bound between saves of scan progress
	scanBatch       = 1000             // heights scanned per loop step
	shutdownFlush   = 20 * time.Second // bound on the flush at shutdown
	backoffBase     = time.Second
	backoffMax      = 60 * time.Second
	spoolRetry      = 5 * time.Second // delay before re-trying an entry after a spool write error
	// startSlack moves the first start (no state) further inside the
	// retention window than the deadline margin: the first blocks found would
	// otherwise reach their deadline at the moment of the search and expire
	// while the scan of the first batch (up to scanBatch heights) runs.
	startSlack = 5 * time.Minute
)

// Run captures Fibre messages from the state's scanned height to the chain
// head and keeps following it until ctx is cancelled. It returns nil on
// cancellation, ErrChainMismatch for a state of another chain, and an error
// wrapping store.ErrExists or ErrRecordConflict when stored objects disagree
// with what capture verified.
func Run(ctx context.Context, cfg Config, d Deps) error {
	cfg.setDefaults()
	if d.Metrics == nil {
		d.Metrics = NewMetrics(prometheus.NewRegistry())
	}
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	w := NewWriter(d.Store, cfg.Now)
	w.DailyCapBytes = cfg.DailyCapBytes
	r := &runner{
		cfg: cfg, d: d, m: d.Metrics, log: d.Log, w: w,
		busy:    map[[33]byte]bool{},
		writes:  map[string]bool{},
		retry:   map[string]retryState{},
		results: make(chan result, cfg.DownloadWorkers),
	}
	r.m.DailyCap.Set(float64(cfg.DailyCapBytes))
	if err := r.start(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return r.loop(ctx)
}

type runner struct {
	cfg Config
	d   Deps
	m   *Metrics
	log *slog.Logger
	w   *Writer
	cap *Cap
	st  *State
	q   Queue

	retention   time.Duration
	head        int64
	tombVersion [32]byte
	tombApplied bool

	// busy holds blob IDs being downloaded or spooled awaiting PutData, so
	// a PFF and its timeout never download the same blob twice.
	busy     map[[33]byte]bool
	writes   map[string]bool // keys of entries whose record (and data) is still to be written
	retry    map[string]retryState
	inflight int
	wg       sync.WaitGroup
	results  chan result

	dirty     bool      // a status change not yet saved
	stale     bool      // scan progress not yet saved
	lastSave  time.Time // wall clock
	finals    int       // final entries not yet indexed
	srcFailed bool      // a source error stopped the scan; it resumes at the next head tick
	shutdown  bool      // saves are tried once
	fatal     error
}

type retryState struct {
	at    time.Time // wall clock
	delay time.Duration
}

func (r *runner) start(ctx context.Context) error {
	st, err := r.d.Dir.Load()
	if err != nil {
		return err
	}
	if st != nil && st.ChainID != r.cfg.ChainID {
		return fmt.Errorf("%w: state.json has %q, want %q", ErrChainMismatch, st.ChainID, r.cfg.ChainID)
	}
	if err := r.retryLoop(ctx, "fibre params", r.m.SourceErrors, func() error { return r.readParams(ctx) }); err != nil {
		return err
	}
	if err := r.retryLoop(ctx, "source head", r.m.SourceErrors, func() error { return r.readHead(ctx) }); err != nil {
		return err
	}
	if st == nil {
		// Begin the deadline margin plus startSlack inside the retention
		// window, so even the earliest block scanned still has startSlack
		// before its deadline.
		var first int64
		err := r.retryLoop(ctx, "first height search", r.m.SourceErrors, func() error {
			var err error
			from := r.cfg.Now().Add(-r.retention + min(maxDeadlineMargin, r.retention/4) + startSlack)
			first, err = FirstHeightAfter(ctx, r.d.Source, from, r.head)
			return err
		})
		if err != nil {
			return err
		}
		st = &State{Version: stateVersion, ChainID: r.cfg.ChainID, FirstHeight: first, ScannedUntil: first - 1, FrozenThrough: -1}
		r.log.Info("fibre capture starts", "first_height", first, "head", r.head)
	}
	if st.Counts == nil {
		st.Counts = map[string]uint64{}
	}
	if st.Entries == nil {
		st.Entries = map[string]*Entry{}
	}
	r.st = st
	r.cap = NewCap(r.cfg.DailyCapBytes, st.Day, st.DayBytes, r.cfg.Now)
	r.rebuild()

	var s *tombstone.Set
	err = r.retryLoop(ctx, "tombstone load", r.m.TombstoneErrors, func() error {
		var err error
		s, err = tombstone.Load(ctx, r.d.Store)
		return err
	})
	if err != nil {
		return err
	}
	if err := r.installTombstones(ctx, s); err != nil {
		return err
	}
	return r.save(ctx)
}

// rebuild restores the queue and write path from the loaded entries and
// sweeps the spool.
func (r *runner) rebuild() {
	for _, k := range slices.Sorted(maps.Keys(r.st.Entries)) {
		e := r.st.Entries[k]
		switch {
		case e.Phase == PhaseFinal:
			r.finals++
			continue
		case e.Phase == PhaseSpooled:
			r.busy[e.BlobID] = true
			r.writes[k] = true
		case e.Status != 0:
			r.writes[k] = true
		case !r.recoverSpool(k, e):
			r.q.Push(e)
		}
	}
	r.rescheduleAll()
	keep := map[[33]byte]bool{}
	for _, e := range r.st.Entries {
		if e.Phase == PhaseSpooled {
			keep[e.BlobID] = true
		}
	}
	if err := r.d.Dir.SpoolSweep(keep); err != nil {
		r.m.StateErrors.Inc()
		r.log.Warn("spool sweep failed", "err", err)
	}
}

// recoverSpool turns a pending entry into a spooled one when a crash came
// between the spool write and its save: the verified payload is on disk, so
// it is not downloaded again. The download is charged to the cap now.
func (r *runner) recoverSpool(k string, e *Entry) bool {
	if r.busy[e.BlobID] {
		return false
	}
	data, err := r.d.Dir.SpoolRead(e.BlobID)
	if err != nil {
		return false
	}
	if err := VerifyPayload(data, e.BlobID, e.BlobSize); err != nil {
		r.log.Warn("spooled payload fails verification; downloading again", "key", k, "err", err)
		if err := r.d.Dir.SpoolRemove(e.BlobID); err != nil {
			r.m.StateErrors.Inc()
		}
		return false
	}
	sum := sha256.Sum256(data)
	e.Phase, e.DataSize, e.DataSHA256 = PhaseSpooled, uint64(len(data)), sum[:]
	r.cap.Commit(0, uint64(len(data)))
	r.busy[e.BlobID] = true
	r.writes[k] = true
	r.dirty = true
	return true
}

func (r *runner) readParams(ctx context.Context) error {
	ret, _, err := r.d.Params.Fibre(ctx)
	if err != nil {
		return err
	}
	if ret <= 0 {
		return fmt.Errorf("fibre params: shard_retention %v", ret)
	}
	r.m.ShardRetention.Set(ret.Seconds())
	if ret != r.retention {
		if r.retention != 0 {
			r.log.Info("shard_retention changed; rescheduling", "from", r.retention, "to", ret)
		}
		r.retention = ret
		if r.st != nil {
			r.rescheduleAll()
			r.dirty = true
		}
	}
	return nil
}

// rescheduleAll recomputes the deadline of every entry still to download.
func (r *runner) rescheduleAll() {
	for _, e := range r.st.Entries {
		if e.Phase == PhasePending && e.Status == 0 {
			e.Deadline = Deadline(e.BlockTime, r.retention)
		}
	}
	r.q.Reschedule(r.retention)
}

func (r *runner) readHead(ctx context.Context) error {
	h, err := r.d.Source.Head(ctx)
	if err != nil {
		return err
	}
	r.head = h
	r.m.ChainHead.Set(float64(h))
	return nil
}

// installTombstones installs s in the writer (deleting listed data) and
// withdraws every listed entry. s's version counts as applied only if the
// deletes succeeded.
func (r *runner) installTombstones(ctx context.Context, s *tombstone.Set) error {
	err := r.w.SetTombstones(ctx, s)
	if err != nil {
		r.m.TombstoneErrors.Inc()
		r.log.Warn("takedown incomplete; retrying on the next poll", "err", err)
		r.tombApplied = false
	} else {
		r.tombVersion, r.tombApplied = s.Version(), true
	}
	return r.withdrawListed(ctx, s)
}

// withdrawListed marks every not-yet-final entry whose blob ID s lists as
// withdrawn. Spooled payloads are removed after the state says so.
func (r *runner) withdrawListed(ctx context.Context, s *tombstone.Set) error {
	var unspool [][33]byte
	for _, k := range slices.Sorted(maps.Keys(r.st.Entries)) {
		e := r.st.Entries[k]
		if e.Phase == PhaseFinal || e.Err != "" || e.Status == fibrearchive.StatusWithdrawn || !s.HasFibre(e.BlobID[:]) {
			continue
		}
		switch {
		case e.Phase == PhaseSpooled:
			unspool = append(unspool, e.BlobID)
			delete(r.busy, e.BlobID)
		case e.Status == 0:
			r.q.Remove(e) // an in-flight download is discarded by handle
		case e.Status != fibrearchive.StatusArchived:
			continue // expired, skipped_cap: nothing to withdraw
		}
		e.Status, e.DataSize, e.DataSHA256 = fibrearchive.StatusWithdrawn, 0, nil
		r.writes[k] = true
		r.dirty = true
		r.log.Info("fibre blob withdrawn before capture", "key", k)
	}
	if len(unspool) == 0 {
		return nil
	}
	if err := r.save(ctx); err != nil {
		return err
	}
	for _, id := range unspool {
		if err := r.d.Dir.SpoolRemove(id); err != nil {
			r.m.StateErrors.Inc()
			r.log.Warn("withdrawn spool not removed", "err", err)
		}
	}
	return nil
}

func (r *runner) pollTombstones(ctx context.Context) {
	s, err := tombstone.Load(ctx, r.d.Store)
	if err != nil {
		r.m.TombstoneErrors.Inc()
		r.log.Warn("tombstone load failed; keeping the previous set", "err", err)
		return
	}
	if r.tombApplied && s.Version() == r.tombVersion {
		return
	}
	if err := r.installTombstones(ctx, s); err != nil && ctx.Err() == nil {
		r.log.Warn("tombstone apply", "err", err)
	}
}

// save writes state.json, retrying with backoff until it succeeds or ctx is
// done (once at shutdown).
func (r *runner) save(ctx context.Context) error {
	r.st.Day, r.st.DayBytes = r.cap.Snapshot()
	do := func() error { return r.d.Dir.Save(r.st) }
	var err error
	if r.shutdown {
		if err = do(); err != nil {
			r.m.StateErrors.Inc()
		}
	} else {
		err = r.retryLoop(ctx, "state save", r.m.StateErrors, do)
	}
	if err != nil {
		return err
	}
	r.dirty, r.stale = false, false
	r.lastSave = time.Now()
	return nil
}

func (r *runner) loop(ctx context.Context) error {
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	headT := time.NewTicker(r.cfg.HeadPoll)
	paramsT := time.NewTicker(r.cfg.ParamsPoll)
	tombT := time.NewTicker(r.cfg.TombstonePoll)
	flushT := time.NewTicker(r.cfg.FlushEvery)
	defer headT.Stop()
	defer paramsT.Stop()
	defer tombT.Stop()
	defer flushT.Stop()

	flushDue := false
	onHead := func() {
		r.srcFailed = false // the scan may retry a failed block now
		if err := r.readHead(ctx); err != nil && ctx.Err() == nil {
			r.m.SourceErrors.Inc()
			r.log.Warn("source head failed", "err", err)
		}
	}
	onParams := func() {
		if err := r.readParams(ctx); err != nil && ctx.Err() == nil {
			r.m.SourceErrors.Inc()
			r.log.Warn("fibre params read failed; keeping the previous values", "err", err)
		}
	}
	for r.fatal == nil && ctx.Err() == nil {
		more := r.step(ctx, wctx, &flushDue)
		if r.fatal != nil || ctx.Err() != nil {
			break
		}
		if more {
			// Catching up: do not wait, but serve every ticker that is due,
			// so polls and flushes keep running during a long catch-up.
			for _, c := range []struct {
				ch <-chan time.Time
				fn func()
			}{
				{headT.C, onHead},
				{paramsT.C, onParams},
				{tombT.C, func() { r.pollTombstones(ctx) }},
				{flushT.C, func() { flushDue = true }},
			} {
				select {
				case <-c.ch:
					c.fn()
				default:
				}
			}
			continue
		}
		select {
		case <-ctx.Done():
		case res := <-r.results:
			r.handle(res)
		case <-headT.C:
			onHead()
		case <-paramsT.C:
			onParams()
		case <-tombT.C:
			r.pollTombstones(ctx)
		case <-flushT.C:
			flushDue = true
		}
	}
	r.stop(wcancel)
	if r.fatal != nil {
		r.log.Error("fibre capture stops", "err", r.fatal)
	}
	return r.fatal
}

// step does one round of work and reports whether scanning is behind head.
func (r *runner) step(ctx, wctx context.Context, flushDue *bool) bool {
	for drained := false; !drained; {
		select {
		case res := <-r.results:
			r.handle(res)
		default:
			drained = true
		}
	}
	if err := r.scan(ctx); err != nil {
		return false
	}
	now := r.cfg.Now()
	r.expire(now)
	r.dispatch(wctx, now)
	if r.dirty || (r.stale && time.Since(r.lastSave) >= saveEvery) {
		if err := r.save(ctx); err != nil {
			return false
		}
	}
	r.writeAll(ctx)
	if r.fatal != nil || ctx.Err() != nil {
		return false
	}
	if *flushDue || r.finals >= flushFinalCount {
		*flushDue = false
		r.flush(ctx)
	}
	r.updateGauges(now)
	// After a source error the loop waits for the next head tick.
	return !r.srcFailed && r.st.ScannedUntil < r.head
}

// scan reads up to scanBatch heights past ScannedUntil. A block with Fibre
// messages is saved with its entries and its height in one write before any
// of them is downloaded.
func (r *runner) scan(ctx context.Context) error {
	if r.srcFailed {
		return nil // wait for the next head tick
	}
	end := min(r.head, r.st.ScannedUntil+scanBatch)
	for h := r.st.ScannedUntil + 1; h <= end; h++ {
		b, err := r.d.Source.Block(ctx, h)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.m.SourceErrors.Inc()
			r.log.Warn("source block failed; retrying on the next head poll", "height", h, "err", err)
			r.srcFailed = true
			return nil
		}
		items, err := Extract(b)
		if err != nil {
			r.fatal = fmt.Errorf("extract height %d: %w", h, err)
			return r.fatal
		}
		now := r.cfg.Now()
		var queued []*Entry
		for _, it := range items {
			k := it.Key()
			if _, ok := r.st.Entries[k]; ok {
				continue
			}
			e := r.newEntry(it, now)
			r.st.Entries[k] = e
			r.m.Messages.WithLabelValues(it.Kind.String()).Inc()
			if e.Status == 0 {
				queued = append(queued, e)
			} else {
				r.writes[k] = true
			}
		}
		r.st.ScannedUntil = h
		r.m.ScannedHeight.Set(float64(h))
		if len(items) == 0 {
			r.stale = true
			continue
		}
		if err := r.save(ctx); err != nil {
			return err
		}
		for _, e := range queued {
			r.q.Push(e)
		}
	}
	return nil
}

func (r *runner) newEntry(it Item, now time.Time) *Entry {
	e := &Entry{Item: it, Phase: PhasePending, ScannedAt: now}
	switch {
	case it.Err != "":
		// The blob ID may be all zero: never look it up.
		e.Status, e.LastErr = fibrearchive.StatusUnrecoverable, it.Err
	case r.withdrawn(it.BlobID):
		e.Status = fibrearchive.StatusWithdrawn
	default:
		e.Deadline = Deadline(it.BlockTime, r.retention)
		e.NextAttempt = NextAttempt(now, 0)
	}
	return e
}

func (r *runner) withdrawn(id [33]byte) bool {
	wd, err := r.w.Withdrawn(id)
	return err == nil && wd
}

func (r *runner) expire(now time.Time) {
	for _, e := range r.q.Expired(now) {
		e.Status = fibrearchive.StatusExpired
		r.writes[e.Key()] = true
		r.dirty = true
	}
}

// job is what a download worker needs of an entry.
type job struct {
	key           string
	id            [33]byte
	blobSize      uint32
	promiseHeight uint64
}

type result struct {
	key       string
	id        [33]byte
	reserved  uint64 // cap bytes reserved for the attempt
	existing  bool   // fibre/data already held verified bytes
	skipped   bool   // the cap refused the attempt
	spooled   bool   // downloaded, verified and spooled
	cancelled bool   // shutdown cut the attempt short
	size      uint64
	sum       []byte
	err       error // download or verification error
	mismatch  bool  // err is a verification failure
	spoolErr  error // the verified payload could not be spooled
}

func (r *runner) dispatch(wctx context.Context, now time.Time) {
	var deferred []*Entry
	defer func() {
		for _, e := range deferred {
			r.q.Push(e)
		}
	}()
	for r.inflight < r.cfg.DownloadWorkers && wctx.Err() == nil {
		e := r.q.Due(now)
		if e == nil {
			return
		}
		if r.withdrawn(e.BlobID) {
			e.Status = fibrearchive.StatusWithdrawn
			r.writes[e.Key()] = true
			r.dirty = true
			continue
		}
		if r.busy[e.BlobID] {
			deferred = append(deferred, e)
			continue
		}
		r.busy[e.BlobID] = true
		r.inflight++
		r.wg.Add(1)
		j := job{key: e.Key(), id: e.BlobID, blobSize: e.BlobSize, promiseHeight: e.PromiseHeight}
		go func() {
			defer r.wg.Done()
			r.results <- r.attempt(wctx, j)
		}()
	}
}

// attempt runs on a worker goroutine: existing data, cap, download, verify,
// spool.
func (r *runner) attempt(ctx context.Context, j job) result {
	res := result{key: j.key, id: j.id}
	size, sum, ok, err := r.w.ExistingData(ctx, j.id, j.blobSize)
	switch {
	case err != nil && errors.Is(err, ErrMismatch):
		// PutData will refuse to overwrite it and stop the capture.
		r.log.Error("stored fibre data fails verification", "key", fibrearchive.DataKey(j.id), "err", err)
	case err != nil && ctx.Err() != nil:
		res.cancelled = true
		return res
	case err != nil:
		// The store is unreachable, but the shards may not outlive the
		// outage: download into the spool anyway. PutData's PutIfAbsent
		// later accepts identical bytes already stored, at the cost of a
		// redundant download.
		r.m.StoreErrors.Inc()
		r.log.Warn("existing-data check failed; downloading anyway", "key", j.key, "err", err)
	case ok:
		res.existing, res.size, res.sum = true, size, sum
		return res
	}
	if !r.cap.Reserve(uint64(j.blobSize)) {
		res.skipped = true
		return res
	}
	res.reserved = uint64(j.blobSize)

	actx, cancel := context.WithTimeout(ctx, AttemptTimeout(j.blobSize))
	began := time.Now()
	data, err := r.d.Downloader.Download(actx, j.id, j.promiseHeight)
	timedOut := actx.Err() == context.DeadlineExceeded
	cancel()
	if err != nil && ctx.Err() != nil {
		res.cancelled = true
		return res
	}
	r.m.DownloadSeconds.Observe(time.Since(began).Seconds())
	if err != nil {
		r.m.DownloadAttempts.WithLabelValues(downloadOutcome(err, timedOut)).Inc()
		res.err = err
		return res
	}
	if err := VerifyPayload(data, j.id, j.blobSize); err != nil {
		r.m.DownloadAttempts.WithLabelValues("mismatch").Inc()
		res.err, res.mismatch = err, true
		return res
	}
	r.m.DownloadAttempts.WithLabelValues("ok").Inc()
	if err := r.d.Dir.SpoolWrite(j.id, data); err != nil {
		res.spoolErr = err
		return res
	}
	h := sha256.Sum256(data)
	res.spooled, res.size, res.sum = true, uint64(len(data)), h[:]
	return res
}

func downloadOutcome(err error, timedOut bool) string {
	switch {
	case errors.Is(err, fibre.ErrNotFound):
		return "not_found"
	case errors.Is(err, fibre.ErrNotEnoughShards):
		return "not_enough_shards"
	case timedOut || errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "error"
}

// handle applies a worker result on the main goroutine. Every cap
// reservation is committed (spooled) or released here.
func (r *runner) handle(res result) {
	r.inflight--
	if res.reserved > 0 {
		if res.spooled {
			r.cap.Commit(res.reserved, res.size) // reserved blob_size, charge len(data)
		} else {
			r.cap.Release(res.reserved)
		}
	}
	if !res.spooled {
		delete(r.busy, res.id)
	}
	e := r.st.Entries[res.key]
	if e == nil || e.Status != 0 {
		// Withdrawn while in flight (and maybe already indexed): discard
		// the payload.
		if res.spooled {
			delete(r.busy, res.id)
			if err := r.d.Dir.SpoolRemove(res.id); err != nil {
				r.m.StateErrors.Inc()
			}
		}
		return
	}
	now := r.cfg.Now()
	switch {
	case res.cancelled:
		r.q.Push(e)
	case res.spoolErr != nil:
		r.m.StateErrors.Inc()
		r.log.Warn("spool write failed; retrying", "key", res.key, "err", res.spoolErr)
		e.NextAttempt = later(e.NextAttempt, now.Add(spoolRetry))
		r.q.Push(e)
	case res.existing:
		e.Status, e.DataSize, e.DataSHA256 = fibrearchive.StatusArchived, res.size, res.sum
		r.writes[res.key] = true
		r.dirty = true
	case res.skipped:
		e.Status = fibrearchive.StatusSkippedCap
		r.writes[res.key] = true
		r.dirty = true
	case res.mismatch:
		r.log.Warn("downloaded fibre blob fails verification", "key", res.key, "err", res.err)
		e.Status, e.LastErr = fibrearchive.StatusUnrecoverable, res.err.Error()
		r.writes[res.key] = true
		r.dirty = true
	case res.err != nil:
		e.Attempts++
		e.LastErr = res.err.Error()
		e.NextAttempt = NextAttempt(e.ScannedAt, e.Attempts)
		r.q.Push(e)
		r.dirty = true
	case res.spooled:
		e.Phase, e.DataSize, e.DataSHA256 = PhaseSpooled, res.size, res.sum
		r.writes[res.key] = true
		r.dirty = true
	}
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// writeAll advances every entry on the write path whose backoff has passed.
func (r *runner) writeAll(ctx context.Context) {
	now := time.Now()
	for _, k := range slices.Sorted(maps.Keys(r.writes)) {
		if ctx.Err() != nil || r.fatal != nil {
			return
		}
		if rs, ok := r.retry[k]; ok && now.Before(rs.at) {
			continue
		}
		if err := r.writeOne(ctx, k); err != nil {
			r.fatal = err
			return
		}
	}
}

// backoff delays the next write attempt of k (1s doubling to 60s, jittered).
func (r *runner) backoff(k string) {
	rs := r.retry[k]
	rs.delay = min(max(rs.delay*2, backoffBase), backoffMax)
	rs.at = time.Now().Add(jitter(rs.delay))
	r.retry[k] = rs
}

// writeOne runs the write order for entry k: data (if spooled), record,
// state final, spool removal. It returns only fatal errors; store errors are
// retried later.
func (r *runner) writeOne(ctx context.Context, k string) error {
	e := r.st.Entries[k]
	spooled := e.Phase == PhaseSpooled
	if spooled && e.Status == 0 {
		data, err := r.d.Dir.SpoolRead(e.BlobID)
		if errors.Is(err, fs.ErrNotExist) {
			return r.lostSpool(ctx, k, e)
		}
		if err != nil {
			r.m.StateErrors.Inc()
			r.log.Warn("spool read failed; retrying", "key", k, "err", err)
			r.backoff(k)
			return nil
		}
		wd, err := r.w.PutData(ctx, e.BlobID, data)
		if errors.Is(err, store.ErrExists) {
			return err
		}
		if err != nil {
			r.storeError(ctx, k, err)
			return nil
		}
		if wd {
			e.Status, e.DataSize, e.DataSHA256 = fibrearchive.StatusWithdrawn, 0, nil
		} else {
			e.Status = fibrearchive.StatusArchived
		}
	}
	if err := r.w.PutRecord(ctx, e); err != nil {
		if errors.Is(err, ErrRecordConflict) {
			return err
		}
		r.storeError(ctx, k, err)
		return nil
	}
	e.Phase = PhaseFinal
	r.st.Counts[e.Status.String()]++
	if spooled && e.Status == fibrearchive.StatusArchived {
		r.st.BytesArchived += e.DataSize
		r.m.BytesArchived.Add(float64(e.DataSize))
	}
	r.m.Records.WithLabelValues(e.Kind.String(), e.Status.String()).Inc()
	delete(r.writes, k)
	delete(r.retry, k)
	r.finals++
	if err := r.save(ctx); err != nil {
		return nil // ctx done: the restart redoes the idempotent writes
	}
	if spooled {
		delete(r.busy, e.BlobID)
		if err := r.d.Dir.SpoolRemove(e.BlobID); err != nil {
			r.m.StateErrors.Inc()
			r.log.Warn("spool not removed; swept at the next start", "key", k, "err", err)
		}
	}
	return nil
}

func (r *runner) storeError(ctx context.Context, k string, err error) {
	if ctx.Err() != nil {
		return
	}
	r.m.StoreErrors.Inc()
	r.log.Warn("fibre write failed; retrying", "key", k, "err", err)
	r.backoff(k)
}

// lostSpool handles a spooled entry whose payload file is gone: data already
// in the store completes it, otherwise it is downloaded again.
func (r *runner) lostSpool(ctx context.Context, k string, e *Entry) error {
	size, sum, ok, err := r.w.ExistingData(ctx, e.BlobID, e.BlobSize)
	if err != nil {
		r.storeError(ctx, k, err)
		return nil
	}
	if ok {
		// The payload came from the store, not from this run's spool: the
		// entry finishes like an existing-data hit, without counting bytes.
		e.Phase, e.Status, e.DataSize, e.DataSHA256 = PhasePending, fibrearchive.StatusArchived, size, sum
		delete(r.busy, e.BlobID)
		r.dirty = true
		return r.writeOne(ctx, k)
	}
	r.log.Warn("spooled payload missing; downloading again", "key", k)
	e.Phase, e.DataSize, e.DataSHA256 = PhasePending, 0, nil
	delete(r.busy, e.BlobID)
	delete(r.writes, k)
	delete(r.retry, k)
	r.q.Push(e)
	r.dirty = true
	return nil
}

// flush writes the index and manifest, drops the indexed entries, freezes
// every bucket the scan has passed, and saves.
func (r *runner) flush(ctx context.Context) {
	approved := map[int64]bool{}
	freezeOK := func(b int64) bool {
		ok := r.freezeOK(b)
		approved[b] = ok
		return ok
	}
	indexed, err := r.w.Flush(ctx, r.st, freezeOK)
	if err != nil {
		r.m.StoreErrors.Inc()
		r.log.Warn("fibre flush incomplete; retrying at the next flush", "err", err)
	}
	frozen := map[int64]bool{} // every frozen bucket above FrozenThrough
	for _, b := range r.st.Frozen {
		frozen[b] = true
	}
	changed := len(indexed) > 0
	for _, k := range indexed {
		b := fibrearchive.Bucket(r.st.Entries[k].Height)
		if approved[b] && !frozen[b] {
			frozen[b] = true
		}
		delete(r.st.Entries, k)
	}
	r.finals -= len(indexed)

	open := map[int64]bool{} // buckets still holding entries
	for _, e := range r.st.Entries {
		open[fibrearchive.Bucket(e.Height)] = true
	}
	// Freeze every eligible bucket the scan has passed, past open ones too;
	// each is frozen once, then recorded. Buckets below first_height stay
	// open for the history stage.
	last := r.st.ScannedUntil/fibrearchive.BucketSize - 1
	for b := r.st.FrozenThrough + 1; b <= last; b++ {
		if b*fibrearchive.BucketSize+1 < r.st.FirstHeight || open[b] || frozen[b] {
			continue
		}
		if err := r.w.FreezeBuckets(ctx, []int64{b}); err != nil {
			r.m.StoreErrors.Inc()
			r.log.Warn("index freeze failed; retrying at the next flush", "bucket", b, "err", err)
			break
		}
		frozen[b] = true
	}
	// Advance the watermark over the leading frozen (or pre-first_height)
	// buckets and keep the rest in Frozen.
	for b := r.st.FrozenThrough + 1; b <= last && (frozen[b] || b*fibrearchive.BucketSize+1 < r.st.FirstHeight); b++ {
		r.st.FrozenThrough = b
		delete(frozen, b)
		changed = true
	}
	above := slices.Sorted(maps.Keys(frozen))
	if !slices.Equal(above, r.st.Frozen) {
		changed = true
	}
	r.st.Frozen = above
	if len(r.st.Frozen) == 0 {
		r.st.Frozen = nil
	}
	if changed {
		_ = r.save(ctx)
	}
}

// freezeOK approves freezing bucket b in a flush: the scan passed its end,
// it lies wholly inside the captured range, and every entry in it is final
// (being flushed now).
func (r *runner) freezeOK(b int64) bool {
	if r.st.ScannedUntil < (b+1)*fibrearchive.BucketSize || b*fibrearchive.BucketSize+1 < r.st.FirstHeight {
		return false
	}
	for _, e := range r.st.Entries {
		if e.Phase != PhaseFinal && fibrearchive.Bucket(e.Height) == b {
			return false
		}
	}
	return true
}

func (r *runner) updateGauges(now time.Time) {
	pending := 0
	var nearest time.Time
	for _, e := range r.st.Entries {
		if e.Phase == PhaseFinal {
			continue
		}
		pending++
		// An un-uploaded promise timeout waits to its deadline by design:
		// only PFF deadlines are a warning sign.
		if e.Kind == fibrearchive.KindPayForFibre && e.Phase == PhasePending && e.Status == 0 &&
			(nearest.IsZero() || e.Deadline.Before(nearest)) {
			nearest = e.Deadline
		}
	}
	r.m.Pending.Set(float64(pending))
	if nearest.IsZero() {
		r.m.NearestDeadline.Set(math.NaN())
	} else {
		r.m.NearestDeadline.Set(nearest.Sub(now).Seconds())
	}
	_, today := r.cap.Snapshot()
	r.m.BytesToday.Set(float64(today))
	if n, err := r.d.Dir.SpoolBytes(); err == nil {
		r.m.SpoolBytes.Set(float64(n))
	}
}

// stop cancels in-flight downloads (they stay pending), applies the results,
// saves once and attempts one bounded flush.
func (r *runner) stop(wcancel context.CancelFunc) {
	wcancel()
	r.wg.Wait()
	for r.inflight > 0 {
		r.handle(<-r.results)
	}
	r.shutdown = true
	sctx, cancel := context.WithTimeout(context.Background(), shutdownFlush)
	defer cancel()
	if err := r.save(sctx); err != nil {
		r.log.Error("final state save failed", "err", err)
		return
	}
	r.flush(sctx)
}

// retryLoop calls f until it succeeds or ctx is done, with jittered backoff
// from 1s to 60s; each failure is counted in errs.
func (r *runner) retryLoop(ctx context.Context, what string, errs prometheus.Counter, f func() error) error {
	delay := backoffBase
	for {
		err := f()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		errs.Inc()
		r.log.Warn(what+" failed, retrying", "err", err, "retry_in", delay)
		if err := sleep(ctx, jitter(delay)); err != nil {
			return err
		}
		delay = min(delay*2, backoffMax)
	}
}

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

// FirstHeightAfter returns the lowest height whose block time is not before
// t, or head if every block is older. It binary-searches with at most
// ceil(log2(head))+1 Block calls.
func FirstHeightAfter(ctx context.Context, src source.Source, t time.Time, head int64) (int64, error) {
	if head < 1 {
		return 0, fmt.Errorf("fibrecapture: head %d", head)
	}
	older := func(h int64) (bool, error) {
		b, err := src.Block(ctx, h)
		if err != nil {
			return false, fmt.Errorf("first height search: %w", err)
		}
		return b.Time.Before(t), nil
	}
	old, err := older(head)
	if err != nil || old {
		return head, err
	}
	lo, hi := int64(1), head // the answer is in [lo, hi]; hi is not older
	for lo < hi {
		mid := lo + (hi-lo)/2
		old, err := older(mid)
		if err != nil {
			return 0, err
		}
		if old {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}
