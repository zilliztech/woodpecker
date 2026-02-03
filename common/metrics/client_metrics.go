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

var (
	WpClientRegisterOnce sync.Once

	// --- Client metrics (initialized by initClientMetrics with namespace ConstLabel) ---

	WpLogNameIdMapping              *prometheus.GaugeVec
	WpClientAppendRequestsTotal     *prometheus.CounterVec
	WpClientAppendEntriesTotal      *prometheus.CounterVec
	WpClientAppendBytes             *prometheus.HistogramVec
	WpClientAppendLatency           *prometheus.HistogramVec
	WpLogHandleOperationsTotal      *prometheus.CounterVec
	WpLogHandleOperationLatency     *prometheus.HistogramVec
	WpClientReadRequestsTotal       *prometheus.CounterVec
	WpClientReadEntriesTotal        *prometheus.CounterVec
	WpClientReadLatency             *prometheus.HistogramVec
	WpLogReaderBytesRead            *prometheus.CounterVec
	WpLogReaderOperationLatency     *prometheus.HistogramVec
	WpLogWriterBytesWritten         *prometheus.CounterVec
	WpLogWriterOperationLatency     *prometheus.HistogramVec
	WpSegmentHandleOperationsTotal  *prometheus.CounterVec
	WpSegmentHandleOperationLatency *prometheus.HistogramVec
	WpSegmentHandlePendingAppendOps *prometheus.GaugeVec
	WpEtcdMetaOperationsTotal       *prometheus.CounterVec
	WpEtcdMetaOperationLatency      *prometheus.HistogramVec
)

// initClientMetrics creates all client-side metrics with a namespace ConstLabel.
func initClientMetrics() {
	constLabels := prometheus.Labels{"namespace": MetricsNamespace}

	// Log name-id mapping
	// WARNING: In large-scale deployments with many logs, the "log_name" label
	// may cause high cardinality issues. Consider removing or sampling if the
	// number of distinct log names exceeds a few thousand.
	WpLogNameIdMapping = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "log_name_id_mapping",
		Help:        "Mapping between log name and id",
		ConstLabels: constLabels,
	}, []string{"log_name"})

	// client append data to log
	WpClientAppendRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "append_requests_total",
		Help:        "Total number of append requests",
		ConstLabels: constLabels,
	}, []string{"log_id"})
	WpClientAppendEntriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "append_entries_total",
		Help:        "Total number of entries appended",
		ConstLabels: constLabels,
	}, []string{"log_id"})
	WpClientAppendBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "append_bytes",
		Help:        "Size of append operations in bytes",
		Buckets:     prometheus.ExponentialBuckets(256, 4, 8), // 256B ~ 4MB
		ConstLabels: constLabels,
	}, []string{"log_id"})
	WpClientAppendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "append_latency",
		Help:        "Latency of append operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"log_id"})

	// LogHandle metrics
	WpLogHandleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "log_handle_operations_total",
		Help:        "Total number of log handle operations",
		ConstLabels: constLabels,
	}, []string{"log_id", "operation", "status"})
	WpLogHandleOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "log_handle_operation_latency",
		Help:        "Latency of log handle operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"log_id", "operation", "status"})

	// Client read metrics
	WpClientReadRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "read_requests_total",
		Help:        "Total number of read requests",
		ConstLabels: constLabels,
	}, []string{"log_id"})
	WpClientReadEntriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "read_entries_total",
		Help:        "Total number of entries read",
		ConstLabels: constLabels,
	}, []string{"log_id"})
	WpClientReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "read_latency",
		Help:        "Latency of read operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"log_id"})

	// LogReader metrics
	WpLogReaderBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "reader_bytes_read",
		Help:        "Total bytes read by log readers",
		ConstLabels: constLabels,
	}, []string{"log_id", "reader_name"})
	WpLogReaderOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "reader_operation_latency",
		Help:        "Latency of log reader operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"log_id", "operation", "status"})

	// LogWriter metrics
	WpLogWriterBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "writer_bytes_written",
		Help:        "Total bytes written by log writers",
		ConstLabels: constLabels,
	}, []string{"log_id"})
	WpLogWriterOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "writer_operation_latency",
		Help:        "Latency of log writer operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"log_id", "operation", "status"})

	// SegmentHandle metrics
	WpSegmentHandleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "segment_handle_operations_total",
		Help:        "Total number of segment handle operations",
		ConstLabels: constLabels,
	}, []string{"log_id", "operation", "status"})
	WpSegmentHandleOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "segment_handle_operation_latency",
		Help:        "Latency of segment handle operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"log_id", "operation", "status"})
	WpSegmentHandlePendingAppendOps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "segment_handle_pending_append_ops",
		Help:        "Number of pending append operations in segment handles",
		ConstLabels: constLabels,
	}, []string{"log_id"})

	// Etcd Meta metrics
	WpEtcdMetaOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "etcd_meta_operations_total",
		Help:        "Total number of etcd meta related operations",
		ConstLabels: constLabels,
	}, []string{"operation", "status"})
	WpEtcdMetaOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   woodpeckerNamespace,
		Subsystem:   clientRole,
		Name:        "etcd_meta_operation_latency",
		Help:        "Latency of etcd meta related operations",
		Buckets:     prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
		ConstLabels: constLabels,
	}, []string{"operation", "status"})
}

// RegisterClientMetricsWithRegisterer initializes and registers all client-side metrics.
func RegisterClientMetricsWithRegisterer(registerer prometheus.Registerer) {
	WpClientRegisterOnce.Do(func() {
		initClientMetrics()

		// log name-id mapping
		registerer.MustRegister(WpLogNameIdMapping)

		// Client append metrics
		registerer.MustRegister(WpClientAppendRequestsTotal)
		registerer.MustRegister(WpClientAppendEntriesTotal)
		registerer.MustRegister(WpClientAppendBytes)
		registerer.MustRegister(WpClientAppendLatency)
		// LogHandle metrics
		registerer.MustRegister(WpLogHandleOperationsTotal)
		registerer.MustRegister(WpLogHandleOperationLatency)
		// Client read metrics
		registerer.MustRegister(WpClientReadRequestsTotal)
		registerer.MustRegister(WpClientReadEntriesTotal)
		registerer.MustRegister(WpClientReadLatency)
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
	})
}

// RegisterClientMetrics is a convenience wrapper that registers client metrics with a specific registry.
func RegisterClientMetrics(registry *prometheus.Registry) {
	RegisterClientMetricsWithRegisterer(registry)
}
