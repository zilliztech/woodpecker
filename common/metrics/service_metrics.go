// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	woodpeckerNamespace = "woodpecker"
	serverRole          = "server"
	clientRole          = "client"
)

// NodeID identifies the server node.
// Initialized from the NODE_NAME environment variable; defaults to "embed".
// Can be overridden before metrics are scraped (e.g. from a CLI flag).
var NodeID = func() string {
	if v := os.Getenv("NODE_NAME"); v != "" {
		return v
	}
	return "embed"
}()

// BuildLogNs returns the metrics log_ns string from bucket name and root path.
func BuildLogNs(bucketName, rootPath string) string {
	return bucketName + "/" + rootPath
}

// Server metrics are initialized at package level so they are always safe to use.
// node_id is a regular label whose value comes from the NodeID package variable.
// Calling RegisterServerMetricsWithRegisterer makes them actually scraped by a registry;
// without registration they silently collect data that goes nowhere.
var (
	WpServerRegisterOnce sync.Once

	// LogStore resource gauges
	WpLogStoreActiveLogs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_active_logs",
		Help:      "Number of active logs in the log store",
	}, []string{"node_id", "log_ns"})
	WpLogStoreActiveSegments = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_active_segments",
		Help:      "Number of active segments in the log store",
	}, []string{"node_id", "log_ns", "log_id"})
	WpFileReaders = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_reader",
		Help:      "Number of active segment file readers for log",
	}, []string{"node_id", "log_ns", "log_id"})

	// LogStore metrics
	WpLogStoreRunningTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_instances_total",
		Help:      "Total number of log store instances",
	}, []string{"node_id"})
	WpLogStoreOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_operations_total",
		Help:      "Total number of log store operations",
	}, []string{"node_id", "log_ns", "log_id", "operation", "status"})
	WpLogStoreOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_operation_latency",
		Help:      "Latency of log store operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"node_id", "log_ns", "log_id", "operation", "status"})
	WpLogStoreActiveSegmentProcessors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_active_segment_processors",
		Help:      "Number of active segment processors in log store",
	}, []string{"node_id", "log_ns", "log_id"})

	// Disk watermark backpressure metrics (issue #215)
	WpLogStoreDiskUsageRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_disk_usage_ratio",
		Help:      "Local WAL disk usage ratio, used/(used+free) (df semantics)",
	}, []string{"node_id", "path"})
	WpLogStoreDiskFreeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_disk_free_bytes",
		Help:      "Local WAL disk free bytes available to the process (statfs Bavail)",
	}, []string{"node_id", "path"})
	WpLogStoreWriteBackpressureState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_write_backpressure_state",
		Help:      "Disk watermark backpressure state: 0=normal, 1=warn, 2=blocked",
	}, []string{"node_id"})
	WpLogStoreWriteRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_write_rejected_total",
		Help:      "Total writes rejected by node-level admission gates",
	}, []string{"node_id", "reason"})
	WpLogStoreWriteRejectProbability = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "logstore_write_reject_probability",
		Help:      "Current probability that a new append is rejected by the disk watermark policy (0..1)",
	}, []string{"node_id"})

	// Buffer wait latency
	WpServerBufferWaitLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "buffer_wait_latency",
		Help:      "Time entries spend waiting in the buffer before being notified (ms)",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 15), // 0.1ms ~ ~1638ms
	}, []string{"node_id", "log_ns", "log_id"})

	// Segment File Impl metrics
	WpFileOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_operations_total",
		Help:      "Total number of file operations",
	}, []string{"node_id", "log_ns", "log_id", "operation", "status"})
	WpFileOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_operation_latency",
		Help:      "Latency of file operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"node_id", "log_ns", "log_id", "operation", "status"})
	WpFileWriters = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_writer",
		Help:      "Number of segment file writer for log",
	}, []string{"node_id", "log_ns", "log_id"})

	WpFileReadBatchLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "read_batch_latency",
		Help:      "Latency of read batch operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
	}, []string{"node_id", "log_ns", "log_id"})
	WpFileReadBatchBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "read_batch_bytes_total",
		Help:      "Total bytes read in batch operations",
	}, []string{"node_id", "log_ns", "log_id"})

	WpFileFlushLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_flush_latency",
		Help:      "Latency of flush blocks operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
	}, []string{"node_id", "log_ns", "log_id"})
	WpFileFlushBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "flush_bytes_written",
		Help:      "Total block bytes flushed",
	}, []string{"node_id", "log_ns", "log_id"})

	WpFileCompactLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_compaction_latency",
		Help:      "Latency of compaction blocks operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
	}, []string{"node_id", "log_ns", "log_id"})
	WpFileCompactBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "compact_bytes_written",
		Help:      "Total block bytes written by compaction",
	}, []string{"node_id", "log_ns", "log_id"})

	// Object storage metrics
	WpObjectStorageOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "object_storage_operations_total",
		Help:      "Total number of object storage operations",
	}, []string{"node_id", "log_ns", "log_id", "operation", "status"})
	WpObjectStorageOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "object_storage_operation_latency",
		Help:      "Latency of object storage operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to ~16s
	}, []string{"node_id", "log_ns", "log_id", "operation", "status"})
	WpObjectStorageRequestBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "object_storage_request_bytes",
		Help:      "Size of individual object storage requests in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 4, 8), // 1KB ~ 16MB
	}, []string{"node_id", "log_ns", "log_id", "operation"})
	WpObjectStorageBytesTransferred = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "object_storage_bytes_transferred",
		Help:      "Total bytes transferred to/from object storage",
	}, []string{"node_id", "log_ns", "log_id", "operation"})

	// Stored data size gauges
	WpObjectStorageStoredBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "object_storage_stored_bytes",
		Help:      "Current total bytes stored in object storage",
	}, []string{"node_id", "log_ns", "log_id"})
	WpObjectStorageStoredObjects = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "object_storage_stored_objects",
		Help:      "Current number of objects stored in object storage",
	}, []string{"node_id", "log_ns", "log_id"})
	WpFileStoredBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_stored_bytes",
		Help:      "Current total bytes stored in local files",
	}, []string{"node_id", "log_ns", "log_id"})
	WpFileStoredCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "file_stored_count",
		Help:      "Current number of local segment files",
	}, []string{"node_id", "log_ns", "log_id"})

	WpSyncSchedulerScheduled = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "sync_scheduler_scheduled",
		Help:      "Number of delayed staged-writer sync checks scheduled",
	}, []string{"node_id"})
	WpSyncSchedulerRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "sync_scheduler_running",
		Help:      "Number of running staged-writer sync scheduler workers",
	}, []string{"node_id"})
	WpSyncSchedulerWaiting = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "sync_scheduler_waiting",
		Help:      "Number of staged-writer sync scheduler jobs waiting",
	}, []string{"node_id"})
	WpSyncSchedulerCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "sync_scheduler_capacity",
		Help:      "Worker capacity of the staged-writer sync scheduler",
	}, []string{"node_id"})

	// WpQuorumSelectionSkew counts node-selection outcomes by mode, to observe
	// how often load actually changed the selection (issue #114).
	// mode: "weighted" (load-ranked), "random_no_load" (no fresh load data,
	// fell back to random).
	WpQuorumSelectionSkew = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: serverRole,
		Name:      "quorum_selection_skew_total",
		Help:      "Count of quorum node selections by mode (weighted/random_no_load)",
	}, []string{"mode"})
)

// RegisterServerMetricsWithRegisterer registers all server-side metrics and system metrics.
// Without calling this, metrics still work but are not scraped by any registry.
func RegisterServerMetricsWithRegisterer(registerer prometheus.Registerer) {
	WpServerRegisterOnce.Do(func() {
		// LogStore resource gauges
		registerer.MustRegister(WpLogStoreActiveLogs)
		registerer.MustRegister(WpLogStoreActiveSegments)
		registerer.MustRegister(WpFileReaders)
		// LogStore metrics
		registerer.MustRegister(WpLogStoreRunningTotal)
		registerer.MustRegister(WpLogStoreOperationsTotal)
		registerer.MustRegister(WpLogStoreOperationLatency)
		registerer.MustRegister(WpLogStoreActiveSegmentProcessors)
		// Disk watermark backpressure metrics (issue #215)
		registerer.MustRegister(WpLogStoreDiskUsageRatio)
		registerer.MustRegister(WpLogStoreDiskFreeBytes)
		registerer.MustRegister(WpLogStoreWriteBackpressureState)
		registerer.MustRegister(WpLogStoreWriteRejectedTotal)
		registerer.MustRegister(WpLogStoreWriteRejectProbability)
		// Buffer wait latency
		registerer.MustRegister(WpServerBufferWaitLatency)
		// Segment File Impl metrics
		registerer.MustRegister(WpFileOperationsTotal)
		registerer.MustRegister(WpFileOperationLatency)
		registerer.MustRegister(WpFileWriters)
		registerer.MustRegister(WpFileReadBatchLatency)
		registerer.MustRegister(WpFileReadBatchBytes)
		registerer.MustRegister(WpFileFlushLatency)
		registerer.MustRegister(WpFileFlushBytesWritten)
		registerer.MustRegister(WpFileCompactLatency)
		registerer.MustRegister(WpFileCompactBytesWritten)
		// Object storage metrics
		registerer.MustRegister(WpObjectStorageOperationsTotal)
		registerer.MustRegister(WpObjectStorageOperationLatency)
		registerer.MustRegister(WpObjectStorageRequestBytes)
		registerer.MustRegister(WpObjectStorageBytesTransferred)
		// Stored data size gauges
		registerer.MustRegister(WpObjectStorageStoredBytes)
		registerer.MustRegister(WpObjectStorageStoredObjects)
		registerer.MustRegister(WpFileStoredBytes)
		registerer.MustRegister(WpFileStoredCount)
		registerer.MustRegister(WpSyncSchedulerScheduled)
		registerer.MustRegister(WpSyncSchedulerRunning)
		registerer.MustRegister(WpSyncSchedulerWaiting)
		registerer.MustRegister(WpSyncSchedulerCapacity)
		// Quorum selection skew (load-aware node selection, issue #114)
		registerer.MustRegister(WpQuorumSelectionSkew)
	})
}
