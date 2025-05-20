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

	// === Client-side metrics ===

	// Append operations metrics
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

	// Read operations metrics
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

	// Compact operations metrics
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

	// Fragment metrics
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

	// Write buffer metrics
	WpWriteBufferSlots = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "write_buffer_slots",
			Help:      "The Size of write buffer slots",
		},
		[]string{"log_name"},
	)

	// Segment rolling metrics
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
		[]string{"log_name", "storage_type", "operation"},
	)

	// === New metrics based on architecture ===

	// Client layer metrics
	WpClientOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "operations_total",
			Help:      "Total number of client operations",
		},
		[]string{"log_name", "operation", "status"},
	)
	WpClientOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "operation_latency",
			Help:      "Latency of client operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_name", "operation"},
	)
	WpClientActiveConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "active_connections",
			Help:      "Number of active client connections",
		},
		[]string{"log_name"},
	)

	// LogHandle metrics
	WpLogHandleOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "log_handle_operations_total",
			Help:      "Total number of log handle operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpLogHandleOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "log_handle_operation_latency",
			Help:      "Latency of log handle operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation"},
	)

	// LogReader/LogWriter metrics
	WpLogReaderBytesRead = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "reader_bytes_read",
			Help:      "Total bytes read by log readers",
		},
		[]string{"log_id", "reader_name"},
	)
	WpLogReaderOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "reader_operation_latency",
			Help:      "Latency of log reader operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation"},
	)
	WpLogWriterBytesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "writer_bytes_written",
			Help:      "Total bytes written by log writers",
		},
		[]string{"log_id"},
	)
	WpLogWriterOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "writer_operation_latency",
			Help:      "Latency of log writer operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation"},
	)

	// SegmentHandle metrics
	WpSegmentHandleOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "segment_handle_operations_total",
			Help:      "Total number of segment handle operations",
		},
		[]string{"log_id", "segment_id", "operation", "status"},
	)
	WpSegmentHandleOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "segment_handle_operation_latency",
			Help:      "Latency of segment handle operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "segment_id", "operation"},
	)
	WpSegmentHandlePendingAppendOps = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "segment_handle_pending_append_ops",
			Help:      "Number of pending append operations in segment handles",
		},
		[]string{"log_id", "segment_id"},
	)

	// LogStore metrics
	WpLogStoreOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "logstore_operations_total",
			Help:      "Total number of log store operations",
		},
		[]string{"log_id", "segment_id", "operation", "status"},
	)
	WpLogStoreOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "logstore_operation_latency",
			Help:      "Latency of log store operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "segment_id", "operation"},
	)
	WpLogStoreActiveSegmentProcessors = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "logstore_active_segment_processors",
			Help:      "Number of active segment processors in log store",
		},
		[]string{"log_id"},
	)

	// SegmentProcessor metrics
	WpSegmentProcessorOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segment_processor_operations_total",
			Help:      "Total number of segment processor operations",
		},
		[]string{"log_id", "segment_id", "operation", "status"},
	)
	WpSegmentProcessorOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segment_processor_operation_latency",
			Help:      "Latency of segment processor operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "segment_id", "operation"},
	)

	// File metrics
	WpFileOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "file_operations_total",
			Help:      "Total number of file operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpFileOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "file_operation_latency",
			Help:      "Latency of file operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation"},
	)
	WpFileBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "file_bytes",
			Help:      "Size of files in bytes",
		},
		[]string{"log_id", "file_type"},
	)

	// Fragment metrics
	WpFragmentOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_operations_total",
			Help:      "Total number of fragment operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpFragmentOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_operation_latency",
			Help:      "Latency of fragment operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation"},
	)
	WpFragmentCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_count",
			Help:      "Number of fragments",
		},
		[]string{"log_id", "segment_id"},
	)

	// Object storage metrics
	WpObjectStorageOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "object_storage_operations_total",
			Help:      "Total number of object storage operations",
		},
		[]string{"operation", "status", "storage_type"},
	)
	WpObjectStorageOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "object_storage_operation_latency",
			Help:      "Latency of object storage operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"operation", "storage_type"},
	)
	WpObjectStorageBytesTransferred = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "object_storage_bytes_transferred",
			Help:      "Total bytes transferred to/from object storage",
		},
		[]string{"operation", "storage_type"},
	)
)

func RegisterWoodpeckerWithRegisterer(registerer prometheus.Registerer) {
	WpRegisterOnce.Do(func() {
		// --------- User related requests ---------
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

		// --------- Server related intents ---------
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

		// --------- New metrics ---------
		// Client layer metrics
		registerer.MustRegister(WpClientOperationsTotal)
		registerer.MustRegister(WpClientOperationLatency)
		registerer.MustRegister(WpClientActiveConnections)

		// LogHandle metrics
		registerer.MustRegister(WpLogHandleOperationsTotal)
		registerer.MustRegister(WpLogHandleOperationLatency)

		// LogReader/LogWriter metrics
		registerer.MustRegister(WpLogReaderBytesRead)
		registerer.MustRegister(WpLogReaderOperationLatency)
		registerer.MustRegister(WpLogWriterBytesWritten)
		registerer.MustRegister(WpLogWriterOperationLatency)

		// SegmentHandle metrics
		registerer.MustRegister(WpSegmentHandleOperationsTotal)
		registerer.MustRegister(WpSegmentHandleOperationLatency)
		registerer.MustRegister(WpSegmentHandlePendingAppendOps)

		// LogStore metrics
		registerer.MustRegister(WpLogStoreOperationsTotal)
		registerer.MustRegister(WpLogStoreOperationLatency)
		registerer.MustRegister(WpLogStoreActiveSegmentProcessors)

		// SegmentProcessor metrics
		registerer.MustRegister(WpSegmentProcessorOperationsTotal)
		registerer.MustRegister(WpSegmentProcessorOperationLatency)

		// File metrics
		registerer.MustRegister(WpFileOperationsTotal)
		registerer.MustRegister(WpFileOperationLatency)
		registerer.MustRegister(WpFileBytes)

		// Fragment metrics
		registerer.MustRegister(WpFragmentOperationsTotal)
		registerer.MustRegister(WpFragmentOperationLatency)
		registerer.MustRegister(WpFragmentCount)

		// Object storage metrics
		registerer.MustRegister(WpObjectStorageOperationsTotal)
		registerer.MustRegister(WpObjectStorageOperationLatency)
		registerer.MustRegister(WpObjectStorageBytesTransferred)
	})
}

// RegisterWoodpecker register wp metrics
func RegisterWoodpecker(registry *prometheus.Registry) {
	RegisterWoodpeckerWithRegisterer(registry)
}
