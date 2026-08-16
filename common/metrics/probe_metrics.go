package metrics

import "github.com/prometheus/client_golang/prometheus"

// Temporary diagnostic probes for the WAL write-path investigation.
//
// The goroutine dumps taken under load showed all sync workers parked in
// syscall.Fsync inside processFlushTask, while the block device reported
// ~0.98ms write latency and a queue depth of only ~2.5. These probes split the
// flush path into its parts so the gap between "device is idle" and "workers
// are all in fsync" can be attributed rather than inferred.
//
// Every series here is unlabelled beyond node_id on purpose: they are meant to
// be cheap enough to leave on during a full-rate run, and to be deleted once
// the question is answered.
var (
	// WpProbeFsyncLatency measures file.Sync() alone.
	WpProbeFsyncLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "fsync_latency",
		Help:    "Duration of a single file.Sync() call in the flush path (ms)",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 16), // 0.05ms ~ 1.6s
	}, []string{"node_id"})

	// WpProbeWriteLatency measures file.Write() alone, for contrast with fsync.
	WpProbeWriteLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "write_latency",
		Help:    "Duration of a single file.Write() call in the flush path (ms)",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 16),
	}, []string{"node_id"})

	// WpProbeFsyncInflight counts fsyncs running concurrently on this node. If
	// the filesystem journal serialises them, this climbs while the device
	// queue depth stays flat.
	WpProbeFsyncInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "fsync_inflight",
		Help: "Number of file.Sync() calls currently in flight on this node",
	}, []string{"node_id"})

	// WpProbeFlushLockWait measures contention on the per-writer flushMu.
	// Expected to be ~0; a non-zero tail would falsify "no cross-writer lock".
	WpProbeFlushLockWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "flush_lock_wait",
		Help:    "Time spent waiting to acquire the per-writer flush mutex (ms)",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 16),
	}, []string{"node_id"})

	// WpProbeQueueWait measures enqueue -> worker pickup, isolating scheduler
	// queueing from everything downstream of it.
	WpProbeQueueWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "queue_wait",
		Help:    "Time a sync job waits in the scheduler queue before a worker picks it up (ms)",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 16),
	}, []string{"node_id"})

	// WpProbeFlushBlockBytes records how full each flushed block actually is.
	// Measured at 30.6 KiB against a 4 MiB syncMaxBytes budget, so the fixed
	// per-flush cost is being paid on very little payload.
	WpProbeFlushBlockBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "flush_block_bytes",
		Help:    "Payload size of one flushed block (bytes)",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 14), // 1KiB ~ 8MiB
	}, []string{"node_id"})

	// WpProbeFlushEntries records entries coalesced into one flushed block.
	WpProbeFlushEntries = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "flush_entries",
		Help:    "Number of entries coalesced into one flushed block",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"node_id"})

	// WpProbeFsyncSkipped counts flushes that skipped fsync under
	// WP_FSYNC_MODE. Diagnostic only — see fsyncMode in writer_impl.go.
	WpProbeFsyncSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace, Subsystem: "probe", Name: "fsync_skipped_total",
		Help: "Flushes that skipped fsync because of the WP_FSYNC_MODE diagnostic override",
	}, []string{"node_id"})
)

func registerProbeMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(WpProbeFsyncLatency)
	registerer.MustRegister(WpProbeWriteLatency)
	registerer.MustRegister(WpProbeFsyncInflight)
	registerer.MustRegister(WpProbeFlushLockWait)
	registerer.MustRegister(WpProbeQueueWait)
	registerer.MustRegister(WpProbeFlushBlockBytes)
	registerer.MustRegister(WpProbeFlushEntries)
	registerer.MustRegister(WpProbeFsyncSkipped)
}
