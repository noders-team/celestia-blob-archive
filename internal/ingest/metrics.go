package ingest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are the ingester's Prometheus metrics.
type Metrics struct {
	ArchivedHeight   prometheus.Gauge
	ChainHead        prometheus.Gauge
	ContiguousUntil  prometheus.Gauge
	BlocksProcessed  prometheus.Counter
	BlobsArchived    prometheus.Counter
	SourceErrors     prometheus.Counter
	DataRootMismatch prometheus.Counter
	Workers          prometheus.Gauge
	CommitSeconds    prometheus.Histogram
	CommitRetries    prometheus.Counter
	DirFreezeErrors  prometheus.Counter
	Tombstones       prometheus.Gauge
	TombstoneScrubs  prometheus.Counter
	TombstoneErrors  prometheus.Counter
	OPObjectsWritten prometheus.Counter
	OPBlobsSkipped   *prometheus.CounterVec
	OPErrors         prometheus.Counter
	OPNamespaces     prometheus.Gauge
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		ArchivedHeight: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_archived_height", Help: "Highest committed height.",
		}),
		ChainHead: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_chain_head_height", Help: "Latest chain height reported by the source.",
		}),
		ContiguousUntil: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_contiguous_until", Help: "Largest H such that heights 1..H are all committed.",
		}),
		BlocksProcessed: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_blocks_processed_total", Help: "Blocks processed into segments (uncommitted work redone after a restart counts again).",
		}),
		BlobsArchived: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_blobs_archived_total", Help: "Blobs in committed segments.",
		}),
		SourceErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_source_errors_total", Help: "Failed calls to the block source (each is retried).",
		}),
		DataRootMismatch: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_data_root_mismatch_total", Help: "Blocks whose rebuilt data root differs from the header; the ingester stops.",
		}),
		Workers: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_workers", Help: "Current limit on concurrent source calls.",
		}),
		CommitSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name: "cba_segment_commit_seconds", Help: "Time to commit one segment to the store.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
		}),
		CommitRetries: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_commit_retries_total", Help: "Segment commit attempts that failed and were retried.",
		}),
		DirFreezeErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_dir_freeze_errors_total", Help: "Failed immutable rewrites of completed directory buckets (retried on the next commit).",
		}),
		Tombstones: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_tombstones", Help: "Blobs listed in the last successfully loaded tombstones.json.",
		}),
		TombstoneScrubs: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_tombstone_scrubs_total", Help: "Blob records overwritten by tombstones in stored segments.",
		}),
		TombstoneErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_tombstone_errors_total", Help: "Failed tombstone loads and takedowns (retried on the next poll), and tombstoned records that cannot be scrubbed (not retried; need the operator).",
		}),
		OPObjectsWritten: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_op_objects_written_total", Help: "OP fallback objects written, one per key (a redone height counts again).",
		}),
		OPBlobsSkipped: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cba_op_blobs_skipped_total", Help: "Blobs in OP namespaces without an OP object, by reason (share_version: not version 0; withdrawn: tombstoned).",
		}, []string{"reason"}),
		OPErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "cba_op_errors_total", Help: "Failed writes of OP objects and of the OP config, and failed OP deletes in segment commits (each is retried).",
		}),
		OPNamespaces: f.NewGauge(prometheus.GaugeOpts{
			Name: "cba_op_namespaces", Help: "Namespaces the ingester writes OP fallback objects for.",
		}),
	}
}
