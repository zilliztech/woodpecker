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

// Client metrics are initialized at package level so they are always safe to use.
// Calling RegisterClientMetricsWithRegisterer makes them actually scraped by a registry;
// without registration they silently collect data that goes nowhere.
var (
	WpClientRegisterOnce sync.Once

	// Log name-id mapping
	// WARNING: In large-scale deployments with many logs, the "log_name" label
	// may cause high cardinality issues. Consider removing or sampling if the
	// number of distinct log names exceeds a few thousand.
	WpLogNameIdMapping = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "log_name_id_mapping",
		Help:      "Mapping between log name and id",
	}, []string{"namespace", "log_name"})

	// Segment state tracking: gauge value = number of segments in that state
	WpClientSegmentState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_state",
		Help:      "Number of segments in each state per log",
	}, []string{"namespace", "log_id", "state"})

	// client append data to log
	WpClientAppendRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_requests_total",
		Help:      "Total number of append requests",
	}, []string{"namespace", "log_id"})
	WpClientAppendEntriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_entries_total",
		Help:      "Total number of entries appended",
	}, []string{"namespace", "log_id"})
	WpClientAppendBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_bytes",
		Help:      "Size of append operations in bytes",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 8), // 256B ~ 4MB
	}, []string{"namespace", "log_id"})
	WpClientAppendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_latency",
		Help:      "Latency of append operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id"})

	// LogHandle metrics
	WpLogHandleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "log_handle_operations_total",
		Help:      "Total number of log handle operations",
	}, []string{"namespace", "log_id", "operation", "status"})
	WpLogHandleOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "log_handle_operation_latency",
		Help:      "Latency of log handle operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id", "operation", "status"})

	// Client read metrics
	WpClientReadRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_requests_total",
		Help:      "Total number of read requests",
	}, []string{"namespace", "log_id"})
	WpClientReadEntriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_entries_total",
		Help:      "Total number of entries read",
	}, []string{"namespace", "log_id"})
	WpClientReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_latency",
		Help:      "Latency of read operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id"})

	// LogReader metrics
	WpLogReaderBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "reader_bytes_read",
		Help:      "Total bytes read by log readers",
	}, []string{"namespace", "log_id", "reader_name"})
	WpLogReaderOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "reader_operation_latency",
		Help:      "Latency of log reader operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id", "operation", "status"})

	// LogWriter metrics
	WpLogWriterBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "writer_bytes_written",
		Help:      "Total bytes written by log writers",
	}, []string{"namespace", "log_id"})
	WpLogWriterOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "writer_operation_latency",
		Help:      "Latency of log writer operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id", "operation", "status"})

	// SegmentHandle metrics
	WpSegmentHandleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_handle_operations_total",
		Help:      "Total number of segment handle operations",
	}, []string{"namespace", "log_id", "operation", "status"})
	WpSegmentHandleOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_handle_operation_latency",
		Help:      "Latency of segment handle operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id", "operation", "status"})
	WpSegmentHandlePendingAppendOps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_handle_pending_append_ops",
		Help:      "Number of pending append operations in segment handles",
	}, []string{"namespace", "log_id"})

	// Direct read metrics (client reads sealed segments directly from object storage)
	WpClientDirectReadRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "direct_read_requests_total",
		Help:      "Total number of direct read requests from object storage for sealed segments",
	}, []string{"namespace", "log_id", "status"})
	WpClientDirectReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "direct_read_latency",
		Help:      "Latency of direct read operations from object storage for sealed segments",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "log_id"})

	// Etcd Meta metrics
	WpEtcdMetaOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "etcd_meta_operations_total",
		Help:      "Total number of etcd meta related operations",
	}, []string{"namespace", "operation", "status"})
	WpEtcdMetaOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "etcd_meta_operation_latency",
		Help:      "Latency of etcd meta related operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"namespace", "operation", "status"})
)

// RegisterClientMetricsWithRegisterer registers all client-side metrics with the given registerer.
// Without calling this, metrics still work but are not scraped by any registry.
func RegisterClientMetricsWithRegisterer(registerer prometheus.Registerer) {
	WpClientRegisterOnce.Do(func() {
		// log name-id mapping
		registerer.MustRegister(WpLogNameIdMapping)
		// segment state tracking
		registerer.MustRegister(WpClientSegmentState)

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
		// Direct read metrics
		registerer.MustRegister(WpClientDirectReadRequestsTotal)
		registerer.MustRegister(WpClientDirectReadLatency)
		// etcd meta metrics
		registerer.MustRegister(WpEtcdMetaOperationsTotal)
		registerer.MustRegister(WpEtcdMetaOperationLatency)
	})
}

// UpdateSegmentState transitions the segment state gauge: decrements the old state count
// and increments the new state count for the given namespace and log.
func UpdateSegmentState(namespace, logId, oldState, newState string) {
	WpClientSegmentState.WithLabelValues(namespace, logId, oldState).Dec()
	WpClientSegmentState.WithLabelValues(namespace, logId, newState).Inc()
}
