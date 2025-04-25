package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	wp_namespace     = "woodpecker"
	server_namespace = "server"
	client_namespace = "client"
)

var (
	WpRegisterOnce sync.Once

	WpAppendOpRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "append_requests",
			Help:      "The number of append requests",
		},
		[]string{"log_name"},
	)
	WpAppendOpEntriesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "append_entries",
			Help:      "The number of append entries",
		},
		[]string{"log_name"},
	)
	WpAppendBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_bytes",
			Help:      "bytes of append data",
		},
		[]string{"log_name"},
	)
	WpAppendReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_req_latency",
			Help:      "The latency of append requests",
		},
		[]string{"log_name"},
	)
	WpReadRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_requests",
			Help:      "The number of read requests",
		},
		[]string{"log_name"},
	)
	WpReadEntriesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_entries",
			Help:      "The number of read entries",
		},
		[]string{"log_name"},
	)
	WpReadBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_bytes",
			Help:      "bytes of read data",
		},
		[]string{"log_name"},
	)
	WpReadReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_req_latency",
			Help:      "The latency of read requests",
		},
		[]string{"log_name"},
	)
	WpCompactBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "compact_bytes",
			Help:      "bytes of compact data",
		},
		[]string{"log_name"},
	)
	WpCompactReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "compact_req_latency",
			Help:      "The latency of compact requests",
		},
		[]string{"log_name"},
	)
	WpFragmentBufferBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_buffer_bytes",
			Help:      "The Size of fragment buffer bytes",
		},
		[]string{"log_name"},
	)
	WpFragmentLoadedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_loaded",
			Help:      "The number of loaded fragments",
		},
		[]string{"log_name"},
	)
	WpFragmentFlushBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_flush_bytes",
			Help:      "bytes of fragment flush data",
		},
		[]string{"log_name"},
	)
	WpFragmentFlushLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_flush_latency",
			Help:      "The latency of fragment flush",
		},
		[]string{"log_name"},
	)
	WpFragmentCacheBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_cache_bytes",
			Help:      "bytes of fragment cache data",
		},
		[]string{"log_name"},
	)
	WpWriteBufferSlots = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "write_buffer_slots",
			Help:      "The Size of write buffer slots",
		},
		[]string{"log_name"},
	)
	WpSegmentRollingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segment_rolling_latency",
			Help:      "The latency of segment rolling",
		},
		[]string{"log_name"},
	)

	// New metrics for fragment cache performance
	WpFragmentCacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_cache_hits",
			Help:      "Number of successful fragment cache lookups",
		},
		[]string{"log_name"},
	)
	WpFragmentCacheMisses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_cache_misses",
			Help:      "Number of failed fragment cache lookups",
		},
		[]string{"log_name"},
	)
	WpFragmentCacheEvictions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_cache_evictions",
			Help:      "Number of fragment evictions from cache",
		},
		[]string{"log_name"},
	)

	// Metrics for segments
	WpSegmentsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segments_total",
			Help:      "Total number of segments per log",
		},
		[]string{"log_name", "state"},
	)
	WpSegmentCleanupOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segment_cleanup_operations",
			Help:      "Number of segment cleanup operations",
		},
		[]string{"log_name", "status"},
	)
	WpSegmentCleanupLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segment_cleanup_latency",
			Help:      "Latency of segment cleanup operations",
		},
		[]string{"log_name"},
	)

	// Metrics for truncation operations
	WpTruncationOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "truncation_operations",
			Help:      "Number of log truncation operations",
		},
		[]string{"log_name", "status"},
	)
	WpTruncationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "truncation_latency",
			Help:      "Latency of log truncation operations",
		},
		[]string{"log_name"},
	)
	WpTruncatedSegments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "truncated_segments",
			Help:      "Number of segments truncated",
		},
		[]string{"log_name"},
	)

	// Metrics for readers and logfiles
	WpActiveReadersGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "active_readers",
			Help:      "Number of active readers per log",
		},
		[]string{"log_name"},
	)
	WpLogfileLoadLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "logfile_load_latency",
			Help:      "Latency of loading logfiles",
		},
		[]string{"log_name", "storage_type"},
	)
	WpLogfileCreateLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "logfile_create_latency",
			Help:      "Latency of creating new logfiles",
		},
		[]string{"log_name", "storage_type"},
	)

	// System resource metrics
	WpDiskUsageBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "disk_usage_bytes",
			Help:      "Disk usage by woodpecker logs",
		},
		[]string{"log_name", "storage_type"},
	)
	WpStorageOperationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "storage_operation_errors",
			Help:      "Number of storage operation errors",
		},
		[]string{"log_name", "operation", "storage_type"},
	)
)

func RegisterWoodpeckerWithRegisterer(registerer prometheus.Registerer) {
	WpRegisterOnce.Do(func() {
		// for append
		registerer.MustRegister(WpAppendOpRequestsGauge)
		registerer.MustRegister(WpAppendOpEntriesGauge)
		registerer.MustRegister(WpAppendBytes)
		registerer.MustRegister(WpAppendReqLatency)

		// for read
		registerer.MustRegister(WpReadRequestsGauge)
		registerer.MustRegister(WpReadEntriesGauge)
		registerer.MustRegister(WpReadBytes)
		registerer.MustRegister(WpReadReqLatency)

		// for fragment
		registerer.MustRegister(WpFragmentBufferBytes)
		registerer.MustRegister(WpFragmentLoadedGauge)
		registerer.MustRegister(WpFragmentFlushBytes)
		registerer.MustRegister(WpFragmentFlushLatency)
		registerer.MustRegister(WpFragmentCacheBytesGauge)
		registerer.MustRegister(WpFragmentCacheHits)
		registerer.MustRegister(WpFragmentCacheMisses)
		registerer.MustRegister(WpFragmentCacheEvictions)

		// for write buffer
		registerer.MustRegister(WpWriteBufferSlots)

		// for segment
		registerer.MustRegister(WpSegmentRollingLatency)
		registerer.MustRegister(WpCompactBytes)
		registerer.MustRegister(WpCompactReqLatency)
		registerer.MustRegister(WpSegmentsTotal)
		registerer.MustRegister(WpSegmentCleanupOperations)
		registerer.MustRegister(WpSegmentCleanupLatency)

		// for truncation
		registerer.MustRegister(WpTruncationOperations)
		registerer.MustRegister(WpTruncationLatency)
		registerer.MustRegister(WpTruncatedSegments)

		// for readers and logfiles
		registerer.MustRegister(WpActiveReadersGauge)
		registerer.MustRegister(WpLogfileLoadLatency)
		registerer.MustRegister(WpLogfileCreateLatency)

		// for system resources
		registerer.MustRegister(WpDiskUsageBytes)
		registerer.MustRegister(WpStorageOperationErrors)
	})
}

// RegisterWoodpecker register wp metrics
func RegisterWoodpecker(registry *prometheus.Registry) {
	RegisterWoodpeckerWithRegisterer(registry)
}
