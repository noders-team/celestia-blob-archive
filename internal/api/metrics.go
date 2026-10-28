package api

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/noders-team/celestia-blob-archive/internal/cachestore"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// Method labels of requests that run no served method: methodOther for
// JSON-RPC calls of no served method (unsupported, malformed) and
// requests refused at JSON-RPC level, methodHTTP for requests refused
// before JSON-RPC (body too large or unreadable, wrong HTTP method or path,
// WebSocket).
const (
	methodOther = "other"
	methodHTTP  = "http"
)

// Metrics are the API's Prometheus metrics. Every method is safe on a nil
// *Metrics and on one with nil fields.
type Metrics struct {
	// Requests counts calls by method and outcome (the outcome* labels).
	Requests *prometheus.CounterVec
	// RequestSeconds is the duration of calls of served methods.
	RequestSeconds *prometheus.HistogramVec
	// Upstream counts reads of the archive upstream by outcome (ok,
	// not_found, error, canceled); see CountUpstream.
	Upstream *prometheus.CounterVec
	// HighestHeight is the loaded manifest's highest_height.
	HighestHeight prometheus.Gauge
	// ManifestAge is the time since the manifest was last loaded.
	ManifestAge prometheus.Gauge
	// Tombstones is the number of blobs in the loaded tombstone set.
	Tombstones prometheus.Gauge
	// TombstonesAge is the time since tombstones.json was last loaded.
	TombstonesAge prometheus.Gauge
	// StateErrors counts failed reloads of manifest.json or tombstones.json.
	StateErrors prometheus.Counter
	// VerifyFailures counts stored records that were refused because they
	// failed verification or did not decode.
	VerifyFailures prometheus.Counter
	// Panics counts calls that panicked and were answered as upstream
	// unavailable.
	Panics prometheus.Counter

	cache atomic.Pointer[func() cachestore.Stats]
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		Requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cba_api_requests_total",
			Help: "JSON-RPC calls by method and outcome (ok, not_found, not_archived, withdrawn, bad_request, upstream, verify_failed).",
		}, []string{"method", "outcome"}),
		RequestSeconds: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cba_api_request_seconds",
			Help:    "Duration of JSON-RPC calls of served methods.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"method"}),
		Upstream: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cba_api_upstream_requests_total",
			Help: "Reads of the upstream archive (below the cache) by outcome.",
		}, []string{"outcome"}),
		HighestHeight: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_api_archive_highest_height",
			Help: "highest_height of the loaded manifest.",
		}),
		ManifestAge: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_api_manifest_age_seconds",
			Help: "Seconds since manifest.json was last loaded successfully; updated every manifest poll.",
		}),
		Tombstones: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_api_tombstones",
			Help: "Blobs in the loaded tombstone set.",
		}),
		TombstonesAge: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_api_tombstones_age_seconds",
			Help: "Seconds since tombstones.json was last loaded successfully; updated every manifest poll.",
		}),
		StateErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_api_state_errors_total",
			Help: "Failed reloads of manifest.json or tombstones.json; the previous state was kept.",
		}),
		VerifyFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_api_verify_failures_total",
			Help: "Stored records refused because they failed verification or were corrupt.",
		}),
		Panics: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_api_panics_total",
			Help: "JSON-RPC calls that panicked; each was answered as upstream unavailable.",
		}),
	}
	reg.MustRegister(cacheCollector{m})
	return m
}

// WatchCache exports stats (a cachestore's Stats) as the cba_api_cache_*
// metrics.
func (m *Metrics) WatchCache(stats func() cachestore.Stats) {
	if m != nil && stats != nil {
		m.cache.Store(&stats)
	}
}

func inc(c prometheus.Counter) {
	if c != nil {
		c.Inc()
	}
}

func set(g prometheus.Gauge, v float64) {
	if g != nil {
		g.Set(v)
	}
}

func (m *Metrics) panicked() {
	if m != nil {
		inc(m.Panics)
	}
}

func (m *Metrics) verifyFailed() {
	if m != nil {
		inc(m.VerifyFailures)
	}
}

func (m *Metrics) stateError() {
	if m != nil {
		inc(m.StateErrors)
	}
}

// request records a call of method with outcome; d is its duration, or 0
// for a call that ran no method.
func (m *Metrics) request(method, outcome string, d time.Duration) {
	if m == nil {
		return
	}
	if m.Requests != nil {
		m.Requests.WithLabelValues(method, outcome).Inc()
	}
	if d > 0 && m.RequestSeconds != nil {
		m.RequestSeconds.WithLabelValues(method).Observe(d.Seconds())
	}
}

func (m *Metrics) loaded(highest int64, tombstones int) {
	if m == nil {
		return
	}
	if highest >= 0 {
		set(m.HighestHeight, float64(highest))
	}
	if tombstones >= 0 {
		set(m.Tombstones, float64(tombstones))
	}
}

// ages sets the manifest and tombstones age gauges.
func (m *Metrics) ages(manifest, tombstones time.Duration) {
	if m != nil {
		set(m.ManifestAge, manifest.Seconds())
		set(m.TombstonesAge, tombstones.Seconds())
	}
}

var cacheClasses = []string{cachestore.ClassDirFrozen, cachestore.ClassDirOpen, cachestore.ClassIdx2}

var (
	descCacheBytes = prometheus.NewDesc("cba_api_cache_bytes",
		"Bytes held by the read cache.", nil, nil)
	descCacheHits = prometheus.NewDesc("cba_api_cache_hits_total",
		"Read cache hits by class (dir_frozen, dir_open, idx2).", []string{"class"}, nil)
	descCacheMisses = prometheus.NewDesc("cba_api_cache_misses_total",
		"Read cache misses by class. Every waiter of a shared (singleflight) fetch counts as a miss, "+
			"so misses can exceed the upstream reads they caused.", []string{"class"}, nil)
)

// cacheCollector reads the watched cache's stats at scrape time; it exports
// nothing until WatchCache is called.
type cacheCollector struct{ m *Metrics }

func (c cacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descCacheBytes
	ch <- descCacheHits
	ch <- descCacheMisses
}

func (c cacheCollector) Collect(ch chan<- prometheus.Metric) {
	f := c.m.cache.Load()
	if f == nil {
		return
	}
	st := (*f)()
	ch <- prometheus.MustNewConstMetric(descCacheBytes, prometheus.GaugeValue, float64(st.Bytes))
	for _, class := range cacheClasses {
		ch <- prometheus.MustNewConstMetric(descCacheHits, prometheus.CounterValue, float64(st.Hits[class]), class)
		ch <- prometheus.MustNewConstMetric(descCacheMisses, prometheus.CounterValue, float64(st.Misses[class]), class)
	}
}

// CountUpstream returns st with its reads counted in
// cba_api_upstream_requests_total. Wrap the upstream client below the
// cache, so that cache hits are not counted.
func (m *Metrics) CountUpstream(st store.Store) store.Store {
	return upstreamStore{Store: st, m: m}
}

type upstreamStore struct {
	store.Store
	m *Metrics
}

func (u upstreamStore) count(err error) {
	if u.m == nil || u.m.Upstream == nil {
		return
	}
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		outcome = "not_found"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		outcome = "canceled"
	default:
		outcome = "error"
	}
	u.m.Upstream.WithLabelValues(outcome).Inc()
}

func (u upstreamStore) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := u.Store.Get(ctx, key)
	u.count(err)
	return b, err
}

func (u upstreamStore) GetLimit(ctx context.Context, key string, max int64) ([]byte, error) {
	b, err := store.GetLimit(ctx, u.Store, key, max)
	u.count(err)
	return b, err
}

func (u upstreamStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b, err := u.Store.GetRange(ctx, key, off, n)
	u.count(err)
	return b, err
}

func (u upstreamStore) Exists(ctx context.Context, key string) (bool, error) {
	ok, err := u.Store.Exists(ctx, key)
	u.count(err)
	return ok, err
}
