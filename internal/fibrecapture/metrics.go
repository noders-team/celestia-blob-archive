package fibrecapture

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
)

// Metrics are the Fibre capture's Prometheus metrics.
type Metrics struct {
	ScannedHeight    prometheus.Gauge
	ChainHead        prometheus.Gauge
	Messages         *prometheus.CounterVec
	Records          *prometheus.CounterVec
	Pending          prometheus.Gauge
	NearestDeadline  prometheus.Gauge
	BytesToday       prometheus.Gauge
	DailyCap         prometheus.Gauge
	BytesArchived    prometheus.Counter
	DownloadAttempts *prometheus.CounterVec
	DownloadSeconds  prometheus.Histogram
	StoreErrors      prometheus.Counter
	SourceErrors     prometheus.Counter
	StateErrors      prometheus.Counter
	TombstoneErrors  prometheus.Counter
	SpoolBytes       prometheus.Gauge
	ShardRetention   prometheus.Gauge
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		ScannedHeight: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_scanned_height", Help: "Highest height scanned for Fibre messages.",
		}),
		ChainHead: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_chain_head_height", Help: "Latest chain height reported by the source.",
		}),
		Messages: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cba_fibre_messages_total", Help: "Fibre messages seen by the scan, by kind (pff, timeout).",
		}, []string{"kind"}),
		Records: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cba_fibre_records_total", Help: "Final Fibre records written, by kind and data status.",
		}, []string{"kind", "status"}),
		Pending: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_pending", Help: "Fibre messages without a written record.",
		}),
		NearestDeadline: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_nearest_deadline_seconds", Help: "Seconds until the nearest download deadline of a pending PFF (timeouts excluded); NaN when no PFF is pending.",
		}),
		BytesToday: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_bytes_today", Help: "Bytes charged to today's (UTC) download cap.",
		}),
		DailyCap: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_daily_cap_bytes", Help: "Daily download cap in bytes.",
		}),
		BytesArchived: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_fibre_bytes_archived_total", Help: "Payload bytes written to fibre/data/.",
		}),
		DownloadAttempts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cba_fibre_download_attempts_total", Help: "Download attempts by outcome (ok, not_found, not_enough_shards, timeout, mismatch, error).",
		}, []string{"outcome"}),
		DownloadSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name: "cba_fibre_download_seconds", Help: "Duration of one download attempt.",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600},
		}),
		StoreErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_fibre_store_errors_total", Help: "Failed object store calls (each is retried).",
		}),
		SourceErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_fibre_source_errors_total", Help: "Failed block source calls (each is retried).",
		}),
		StateErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_fibre_state_errors_total", Help: "Failed state.json saves and spool operations.",
		}),
		TombstoneErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_fibre_tombstone_errors_total", Help: "Failed tombstone loads and takedown deletes (retried on the next poll).",
		}),
		SpoolBytes: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_spool_bytes", Help: "Bytes of verified payloads waiting in the spool.",
		}),
		ShardRetention: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_fibre_shard_retention_seconds", Help: "Current Fibre shard_retention parameter.",
		}),
	}
	m.NearestDeadline.Set(math.NaN())
	// Export every labelled series at 0, so increase() sees the first event
	// after a start.
	for _, k := range []fibrearchive.Kind{fibrearchive.KindPayForFibre, fibrearchive.KindTimeout} {
		for s := fibrearchive.StatusArchived; s <= fibrearchive.StatusSkippedCap; s++ {
			m.Records.WithLabelValues(k.String(), s.String())
		}
	}
	for _, o := range []string{"ok", "not_found", "not_enough_shards", "timeout", "mismatch", "error"} {
		m.DownloadAttempts.WithLabelValues(o)
	}
	return m
}
