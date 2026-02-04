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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	woodpeckerNamespace = "woodpecker"
	serverRole          = "server"
	clientRole          = "client"
)

var (
	WpServerRegisterOnce sync.Once

	// NodeID identifies the server node (e.g. "woodpecker-node1").
	// Must be set before RegisterWoodpecker* is called.
	NodeID string

	// MetricsNamespace identifies the logical namespace (bucketName/rootPath).
	// Must be set before RegisterWoodpecker* is called.
	MetricsNamespace string

	// --- Server metrics (initialized by initServerMetrics with node_id ConstLabel + namespace regular label) ---

	// LogStore resource gauges
	WpLogStoreActiveLogs              *prometheus.GaugeVec
	WpLogStoreActiveSegments          *prometheus.GaugeVec
	WpFileReaders                     *prometheus.GaugeVec
	WpLogStoreRunningTotal            *prometheus.GaugeVec // per-node logstore instance count
	WpLogStoreOperationsTotal         *prometheus.CounterVec
	WpLogStoreOperationLatency        *prometheus.HistogramVec
	WpLogStoreActiveSegmentProcessors *prometheus.GaugeVec

	// Buffer wait latency
	WpServerBufferWaitLatency *prometheus.HistogramVec

	// Segment File Impl metrics
	WpFileOperationsTotal     *prometheus.CounterVec
	WpFileOperationLatency    *prometheus.HistogramVec
	WpFileWriters             *prometheus.GaugeVec
	WpFileReadBatchLatency    *prometheus.HistogramVec
	WpFileReadBatchBytes      *prometheus.CounterVec
	WpFileFlushLatency        *prometheus.HistogramVec
	WpFileFlushBytesWritten   *prometheus.CounterVec
	WpFileCompactLatency      *prometheus.HistogramVec
	WpFileCompactBytesWritten *prometheus.CounterVec

	// Object storage metrics
	WpObjectStorageOperationsTotal  *prometheus.CounterVec
	WpObjectStorageOperationLatency *prometheus.HistogramVec
	WpObjectStorageRequestBytes     *prometheus.HistogramVec
	WpObjectStorageBytesTransferred *prometheus.CounterVec

	// Stored data size gauges
	WpObjectStorageStoredBytes   *prometheus.GaugeVec
	WpObjectStorageStoredObjects *prometheus.GaugeVec
	WpFileStoredBytes            *prometheus.GaugeVec
	WpFileStoredCount            *prometheus.GaugeVec
)

// initServerMetrics creates all server-side metrics with a node_id ConstLabel
// and namespace as a regular label so each time series can be filtered per tenant.
func initServerMetrics() {
	constLabels := prometheus.Labels{"node_id": NodeID}

	// LogStore resource gauges
	WpLogStoreActiveLogs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "logstore_active_logs",
		Help:        "Number of active logs in the log store",
		ConstLabels: constLabels,
	}, []string{"namespace"})
	WpLogStoreActiveSegments = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "logstore_active_segments",
		Help:        "Number of active segments in the log store",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpFileReaders = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_reader",
		Help:        "Number of active segment file readers for log",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	// LogStore metrics
	WpLogStoreRunningTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "logstore_instances_total",
		Help:        "Total number of log store instances",
		ConstLabels: constLabels,
	}, []string{"namespace"})
	WpLogStoreOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "logstore_operations_total",
		Help:        "Total number of log store operations",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation", "status"})
	WpLogStoreOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "logstore_operation_latency",
		Help:        "Latency of log store operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation", "status"})
	WpLogStoreActiveSegmentProcessors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "logstore_active_segment_processors",
		Help:        "Number of active segment processors in log store",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	// Buffer wait latency
	WpServerBufferWaitLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "buffer_wait_latency",
		Help:        "Time entries spend waiting in the buffer before being notified (ms)",
		Buckets:     prometheus.ExponentialBuckets(0.1, 2, 15), // 0.1ms ~ ~1638ms
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	// Segment File Impl metrics
	WpFileOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_operations_total",
		Help:        "Total number of file operations",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation", "status"})
	WpFileOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_operation_latency",
		Help:        "Latency of file operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation", "status"})
	WpFileWriters = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_writer",
		Help:        "Number of segment file writer for log",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	WpFileReadBatchLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "read_batch_latency",
		Help:        "Latency of read batch operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpFileReadBatchBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "read_batch_bytes_total",
		Help:        "Total bytes read in batch operations",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	WpFileFlushLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_flush_latency",
		Help:        "Latency of flush blocks operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpFileFlushBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "flush_bytes_written",
		Help:        "Total block bytes flushed",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	WpFileCompactLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_compaction_latency",
		Help:        "Latency of compaction blocks operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpFileCompactBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "compact_bytes_written",
		Help:        "Total block bytes written by compaction",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})

	// Object storage metrics
	WpObjectStorageOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "object_storage_operations_total",
		Help:        "Total number of object storage operations",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation", "status"})
	WpObjectStorageOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "object_storage_operation_latency",
		Help:        "Latency of object storage operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 15), // 1ms to ~16s
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation", "status"})
	WpObjectStorageRequestBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "object_storage_request_bytes",
		Help:        "Size of individual object storage requests in bytes",
		Buckets:     prometheus.ExponentialBuckets(1024, 4, 8), // 1KB ~ 16MB
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation"})
	WpObjectStorageBytesTransferred = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "object_storage_bytes_transferred",
		Help:        "Total bytes transferred to/from object storage",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id", "operation"})

	// Stored data size gauges
	WpObjectStorageStoredBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "object_storage_stored_bytes",
		Help:        "Current total bytes stored in object storage",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpObjectStorageStoredObjects = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "object_storage_stored_objects",
		Help:        "Current number of objects stored in object storage",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpFileStoredBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_stored_bytes",
		Help:        "Current total bytes stored in local files",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
	WpFileStoredCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   serverRole,
		Name:        "file_stored_count",
		Help:        "Current number of local segment files",
		ConstLabels: constLabels,
	}, []string{"namespace", "log_id"})
}

// RegisterServerMetricsWithRegisterer initializes and registers all server-side metrics and system metrics.
func RegisterServerMetricsWithRegisterer(registerer prometheus.Registerer) {
	WpServerRegisterOnce.Do(func() {
		initServerMetrics()

		// LogStore resource gauges
		registerer.MustRegister(WpLogStoreActiveLogs)
		registerer.MustRegister(WpLogStoreActiveSegments)
		registerer.MustRegister(WpFileReaders)
		// LogStore metrics
		registerer.MustRegister(WpLogStoreRunningTotal)
		registerer.MustRegister(WpLogStoreOperationsTotal)
		registerer.MustRegister(WpLogStoreOperationLatency)
		registerer.MustRegister(WpLogStoreActiveSegmentProcessors)
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

		// System metrics
		RegisterSystemMetrics(registerer)
	})
}

// RegisterWoodpeckerWithRegisterer registers both client and server metrics.
// This is the backward-compatible entry point used by cmd/main.go.
func RegisterWoodpeckerWithRegisterer(registerer prometheus.Registerer) {
	RegisterClientMetricsWithRegisterer(registerer)
	RegisterServerMetricsWithRegisterer(registerer)
}

// RegisterWoodpecker register wp metrics
func RegisterWoodpecker(registry *prometheus.Registry) {
	RegisterWoodpeckerWithRegisterer(registry)
}
