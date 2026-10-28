package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// reloadEvery limits the immediate tombstone reloads a stale set triggers.
const reloadEvery = 10 * time.Second

// healthyAge is how recent the manifest must be for /health to answer 200.
const healthyAge = 60 * time.Second

// maxStateObject bounds manifest.json and tombstones.json: a larger one
// fails the load, and the previous state is kept.
const maxStateObject = 16 << 20

// stateStore is the store the state loads read through: Get is capped at
// maxStateObject.
type stateStore struct{ store.Store }

func (s stateStore) Get(ctx context.Context, key string) ([]byte, error) {
	return store.GetLimit(ctx, s.Store, key, maxStateObject)
}

// clock is the time source of the background reloads.
type clock interface {
	Now() time.Time
	// Tick returns a channel that fires every d and the func that stops it.
	Tick(d time.Duration) (<-chan time.Time, func())
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Tick(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// liveState is the archiveState Start loads and reloads in the background.
type liveState struct {
	s *Server

	mu    sync.RWMutex
	man   archive.Manifest
	manAt time.Time // last successful manifest load
	set   *tombstone.Set
	setAt time.Time // last successful tombstones load
	immAt time.Time // last immediate tombstone reload

	// tombSem (capacity 1) serialises tombstone loads, so a slow older load
	// cannot replace the set a newer one installed. A channel rather than a
	// mutex, so that waiting for it can give up when the context is done.
	tombSem chan struct{}
}

// Start loads manifest.json and tombstones.json and keeps reloading them
// every ManifestPoll and TombstonePoll until ctx is done. The first load
// fails on any error, a chain_id other than Config.ChainID, or a
// format_version other than 2; a later failed reload keeps the previous
// state and is counted in cba_api_state_errors_total. Start runs once; a
// Start that failed may be retried.
func (s *Server) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("api: already started")
	}
	ls := &liveState{s: s, tombSem: make(chan struct{}, 1)}
	if err := ls.loadManifest(ctx); err != nil {
		s.started.Store(false)
		return fmt.Errorf("api: %w", err)
	}
	if err := ls.loadTombstones(ctx); err != nil {
		s.started.Store(false)
		return fmt.Errorf("api: %w", err)
	}
	s.setState(ls)
	manTick, stopMan := s.clock.Tick(s.cfg.ManifestPoll)
	tombTick, stopTomb := s.clock.Tick(s.cfg.TombstonePoll)
	go func() {
		defer stopMan()
		defer stopTomb()
		for {
			select {
			case <-ctx.Done():
				return
			case <-manTick:
				if err := ls.loadManifest(ctx); err != nil && ctx.Err() == nil {
					s.m.stateError()
					s.log.Warn("manifest reload failed; keeping the previous manifest", "err", err)
				}
				s.m.ages(ls.manifestAge(), ls.tombstonesAge())
			case <-tombTick:
				if err := ls.loadTombstones(ctx); err != nil && ctx.Err() == nil {
					s.m.stateError()
					s.log.Warn("tombstones reload failed; keeping the previous set", "err", err)
				}
			}
		}
	}()
	return nil
}

// ContiguousUntil returns the loaded manifest's contiguous_until, or 0
// before Start. It is the cachestore's contiguousUntil func.
func (s *Server) ContiguousUntil() int64 {
	st := s.state()
	if st == nil {
		return 0
	}
	_, c := st.heights()
	return c
}

// loadManifest reads and checks manifest.json and installs it.
func (ls *liveState) loadManifest(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ls.s.cfg.RequestTimeout)
	defer cancel()
	// LoadManifest refuses a format_version other than archive.FormatVersion (2).
	m, err := archiveread.LoadManifest(ctx, stateStore{ls.s.st})
	if err != nil {
		return err
	}
	if m.ChainID != ls.s.cfg.ChainID {
		return fmt.Errorf("manifest chain_id %q does not match %q", m.ChainID, ls.s.cfg.ChainID)
	}
	if m.HighestHeight < 0 || m.ContiguousUntil < 0 || m.ContiguousUntil > m.HighestHeight {
		return fmt.Errorf("manifest heights out of order: contiguous_until %d, highest_height %d",
			m.ContiguousUntil, m.HighestHeight)
	}
	ls.mu.Lock()
	ls.man, ls.manAt = *m, ls.s.clock.Now()
	ls.mu.Unlock()
	ls.s.m.loaded(m.HighestHeight, -1)
	ls.s.m.ages(0, ls.tombstonesAge())
	return nil
}

// loadTombstones reads tombstones.json and installs the set. Waiting for
// another load and the load itself are bounded by ctx and RequestTimeout.
func (ls *liveState) loadTombstones(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ls.s.cfg.RequestTimeout)
	defer cancel()
	select {
	case ls.tombSem <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("wait for another tombstones load: %w", ctx.Err())
	}
	defer func() { <-ls.tombSem }()
	set, err := tombstone.Load(ctx, stateStore{ls.s.st})
	if err != nil {
		return err
	}
	ls.mu.Lock()
	ls.set, ls.setAt = set, ls.s.clock.Now()
	ls.mu.Unlock()
	ls.s.m.loaded(-1, set.Len())
	ls.s.m.ages(ls.manifestAge(), 0)
	return nil
}

func (ls *liveState) heights() (int64, int64) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.man.HighestHeight, ls.man.ContiguousUntil
}

func (ls *liveState) tombstones() *tombstone.Set {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.set
}

func (ls *liveState) manifestAge() time.Duration {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.s.clock.Now().Sub(ls.manAt)
}

func (ls *liveState) tombstonesAge() time.Duration {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.s.clock.Now().Sub(ls.setAt)
}

// reloadTombstones reloads tombstones.json at most once per reloadEvery.
// It runs detached from the caller's cancellation, so a client giving up
// cannot abort it, but within the caller's deadline (or RequestTimeout if
// it has none), so it never holds the call past its own timeout, including
// the wait for a scheduled load in progress.
func (ls *liveState) reloadTombstones(ctx context.Context) {
	now := ls.s.clock.Now()
	ls.mu.Lock()
	if !ls.immAt.IsZero() && now.Sub(ls.immAt) < reloadEvery {
		ls.mu.Unlock()
		return
	}
	ls.immAt = now
	ls.mu.Unlock()
	rctx := context.WithoutCancel(ctx)
	if d, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		rctx, cancel = context.WithDeadline(rctx, d)
		defer cancel()
	}
	if err := ls.loadTombstones(rctx); err != nil {
		ls.s.m.stateError()
		ls.s.log.Warn("immediate tombstones reload failed; keeping the previous set", "err", err)
	}
}

// health is the body of GET /health.
type health struct {
	ChainID            string `json:"chain_id"`
	HighestHeight      int64  `json:"highest_height"`
	ContiguousUntil    int64  `json:"contiguous_until"`
	ManifestAgeSeconds int64  `json:"manifest_age_seconds"`
	Tombstones         int    `json:"tombstones"`
	// TombstonesAgeSeconds is informational: a stale set does not make the
	// API unhealthy, but listed blobs not yet scrubbed stay served.
	TombstonesAgeSeconds int64 `json:"tombstones_age_seconds"`
}

// serveHealth answers 200 if the manifest was loaded less than healthyAge
// ago and 503 otherwise (or before Start, with both ages -1).
func (s *Server) serveHealth(w http.ResponseWriter) {
	h := health{ChainID: s.cfg.ChainID, ManifestAgeSeconds: -1, TombstonesAgeSeconds: -1}
	healthy := false
	if st := s.state(); st != nil {
		age := st.manifestAge()
		h.HighestHeight, h.ContiguousUntil = st.heights()
		h.ManifestAgeSeconds = int64(age / time.Second)
		h.Tombstones = st.tombstones().Len()
		h.TombstonesAgeSeconds = int64(st.tombstonesAge() / time.Second)
		healthy = age < healthyAge
	}
	if !healthy {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	writeJSON(w, h)
}
