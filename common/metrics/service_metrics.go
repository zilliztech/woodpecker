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
	WpRegisterOnce sync.Once

	// Log name-id mapping
	WpLogNameIdMapping = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "log_name_id_mapping",
			Help:      "Mapping between log name and id",
		},
		[]string{"log_name"},
	)

	// client metrics
	WpClientOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "operations_total",
			Help:      "Total number of client operations",
		},
		[]string{"operation", "status"},
	)
	WpClientOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "operation_latency",
			Help:      "Latency of client operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"operation", "status"},
	)
	WpClientActiveConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "active_connections",
			Help:      "Number of active client connections",
		},
		[]string{"node"},
	)

	// client append data to log
	WpClientAppendRequestsTotal = prometheus.NewCounterVec( //used in segment Append request method
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "append_requests_total",
			Help:      "Total number of append requests",
		},
		[]string{"log_id"},
	)
	WpClientAppendEntriesTotal = prometheus.NewCounterVec( //used in segment Append request method
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "append_entries_total",
			Help:      "Total number of entries appended",
		},
		[]string{"log_id"},
	)
	WpClientAppendBytes = prometheus.NewHistogramVec( // used in appendOp
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "append_bytes",
			Help:      "Size of append operations in bytes",
		},
		[]string{"log_id"},
	)
	WpClientAppendLatency = prometheus.NewHistogramVec( // used in appendOp
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "append_latency",
			Help:      "Latency of append operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id"},
	)

	// LogHandle metrics
	WpLogHandleOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "log_handle_operations_total",
			Help:      "Total number of log handle operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpLogHandleOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "log_handle_operation_latency",
			Help:      "Latency of log handle operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation", "status"},
	)

	// LogReader metrics
	WpLogReaderBytesRead = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "reader_bytes_read",
			Help:      "Total bytes read by log readers",
		},
		[]string{"log_id", "reader_name"},
	)
	WpLogReaderOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "reader_operation_latency",
			Help:      "Latency of log reader operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation", "status"},
	)

	// LogWriter metrics, including writer/auditor/cleanup related metrics
	WpLogWriterBytesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "writer_bytes_written",
			Help:      "Total bytes written by log writers",
		},
		[]string{"log_id"},
	)
	WpLogWriterOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "writer_operation_latency",
			Help:      "Latency of log writer operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation", "status"},
	)

	// SegmentHandle metrics
	WpSegmentHandleOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "segment_handle_operations_total",
			Help:      "Total number of segment handle operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpSegmentHandleOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "segment_handle_operation_latency",
			Help:      "Latency of segment handle operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation", "status"},
	)
	WpSegmentHandlePendingAppendOps = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "segment_handle_pending_append_ops",
			Help:      "Number of pending append operations in segment handles",
		},
		[]string{"log_id"},
	)

	// Etcd Meta metrics
	WpEtcdMetaOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "etcd_meta_operations_total",
			Help:      "Total number of etcd meta related operations",
		},
		[]string{"operation", "status"},
	)
	WpEtcdMetaOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: clientRole,
			Name:      "etcd_meta_operation_latency",
			Help:      "Latency of etcd meta related operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"operation", "status"},
	)

	// LogStore metrics
	WpLogStoreRunningTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "logstore_instances_total",
			Help:      "Total number of log store instances",
		},
		[]string{"node"},
	)
	WpLogStoreOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "logstore_operations_total",
			Help:      "Total number of log store operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpLogStoreOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "logstore_operation_latency",
			Help:      "Latency of log store operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation", "status"},
	)
	WpLogStoreActiveSegmentProcessors = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "logstore_active_segment_processors",
			Help:      "Number of active segment processors in log store",
		},
		[]string{"log_id"},
	)

	// Segment File Impl metrics
	WpFileOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "file_operations_total",
			Help:      "Total number of file operations",
		},
		[]string{"log_id", "operation", "status"},
	)
	WpFileOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "file_operation_latency",
			Help:      "Latency of file operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"log_id", "operation", "status"},
	)
	WpFileWriters = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "file_writer",
			Help:      "Number of segment file writer for log",
		},
		[]string{"log_id"},
	)

	WpFileReadBatchLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "read_flush_latency",
			Help:      "Latency of read batch operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
		},
		[]string{"log_id"},
	)
	WpFileReadBatchBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "read_bytes_written",
			Help:      "Total read bytes",
		},
		[]string{"log_id"},
	)

	WpFileFlushLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "file_flush_latency",
			Help:      "Latency of flush blocks operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
		},
		[]string{"log_id"},
	)
	WpFileFlushBytesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "flush_bytes_written",
			Help:      "Total block bytes flushed",
		},
		[]string{"log_id"},
	)

	WpFileCompactLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "file_compaction_latency",
			Help:      "Latency of compaction blocks operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1ms to 1024ms
		},
		[]string{"log_id"},
	)
	WpFileCompactBytesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "compact_bytes_written",
			Help:      "Total block bytes written by compaction",
		},
		[]string{"log_id"},
	)

	// Object storage metrics
	WpObjectStorageOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "object_storage_operations_total",
			Help:      "Total number of object storage operations",
		},
		[]string{"operation", "status"},
	)
	WpObjectStorageOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "object_storage_operation_latency",
			Help:      "Latency of object storage operations",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		},
		[]string{"operation", "status"},
	)
	WpObjectStorageBytesTransferred = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: woodpeckerNamespace,
			Subsystem: serverRole,
			Name:      "object_storage_bytes_transferred",
			Help:      "Total bytes transferred to/from object storage",
		},
		[]string{"operation"},
	)
)

func RegisterWoodpeckerWithRegisterer(registerer prometheus.Registerer) {
	WpRegisterOnce.Do(func() {
		// -------------- log name-id mapping metrics -----------------
		registerer.MustRegister(WpLogNameIdMapping)

		// -------------- Client metrics -----------------
		// Client metrics
		registerer.MustRegister(WpClientOperationsTotal)
		registerer.MustRegister(WpClientOperationLatency)
		registerer.MustRegister(WpClientActiveConnections)
		registerer.MustRegister(WpClientAppendRequestsTotal)
		registerer.MustRegister(WpClientAppendEntriesTotal)
		registerer.MustRegister(WpClientAppendBytes)
		registerer.MustRegister(WpClientAppendLatency)
		// LogHandle metrics
		registerer.MustRegister(WpLogHandleOperationsTotal)
		registerer.MustRegister(WpLogHandleOperationLatency)
		// LogReader metrics
		registerer.MustRegister(WpLogReaderBytesRead)
		registerer.MustRegister(WpLogReaderOperationLatency)
		// LogWriter metrics
		registerer.MustRegister(WpLogWriterBytesWritten)
		registerer.MustRegister(WpLogWriterOperationLatency)
		// SegmentHandle metrics
		registerer.MustRegister(WpSegmentHandleOperationsTotal)
		registerer.MustRegister(WpSegmentHandleOperationLatency)
		registerer.MustRegister(WpSegmentHandlePendingAppendOps)
		// etcd meta metrics
		registerer.MustRegister(WpEtcdMetaOperationsTotal)
		registerer.MustRegister(WpEtcdMetaOperationLatency)

		// -------------- Server metrics -----------------
		// LogStore metrics
		registerer.MustRegister(WpLogStoreRunningTotal)
		registerer.MustRegister(WpLogStoreOperationsTotal)
		registerer.MustRegister(WpLogStoreOperationLatency)
		registerer.MustRegister(WpLogStoreActiveSegmentProcessors)
		// Segment File Impl metrics
		registerer.MustRegister(WpFileOperationsTotal)
		registerer.MustRegister(WpFileOperationLatency)
		registerer.MustRegister(WpFileWriters)
		registerer.MustRegister(WpFileCompactLatency)
		registerer.MustRegister(WpFileCompactBytesWritten)
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
