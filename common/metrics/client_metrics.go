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
	frontierMu           sync.Mutex
	writeFrontiers       = make(map[string]progressFrontier)
	compactionFrontiers  = make(map[string]progressFrontier)
	truncationFrontiers  = make(map[string]progressFrontier)
	// Keyed by reader as well as by log: several readers may follow one log at
	// different positions, and one must not clamp another.
	readFrontiers = make(map[string]progressFrontier)

	// Log name-id mapping
	// WARNING: In large-scale deployments with many logs, the "log_name" label
	// may cause high cardinality issues. Consider removing or sampling if the
	// number of distinct log names exceeds a few thousand.
	WpLogNameIdMapping = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "log_name_id_mapping",
		Help:      "Mapping between log name and id",
	}, []string{"log_ns", "log_name"})

	// Segment state tracking: gauge value = number of segments in that state
	WpClientSegmentState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_state",
		Help:      "Number of segments in each state per log",
	}, []string{"log_ns", "log_id", "state"})
	WpClientWriteFrontierSegment = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "write_frontier_segment",
		Help:      "Highest segment id acknowledged for writes per log",
	}, []string{"log_ns", "log_id"})
	WpClientWriteFrontierEntry = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "write_frontier_entry",
		Help:      "Highest entry id acknowledged in the write frontier segment per log",
	}, []string{"log_ns", "log_id"})
	WpClientCompactionFrontierSegment = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "compaction_frontier_segment",
		Help:      "Highest segment id successfully sealed by compaction per log",
	}, []string{"log_ns", "log_id"})
	WpClientCompactionFrontierEntry = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "compaction_frontier_entry",
		Help:      "Highest entry id in the highest segment successfully sealed by compaction per log",
	}, []string{"log_ns", "log_id"})
	WpClientTruncationFrontierSegment = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "truncation_frontier_segment",
		Help:      "Current truncated segment id per log",
	}, []string{"log_ns", "log_id"})
	WpClientTruncationFrontierEntry = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "truncation_frontier_entry",
		Help:      "Current truncated entry id per log",
	}, []string{"log_ns", "log_id"})
	WpClientReadFrontierSegment = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_frontier_segment",
		Help:      "Highest segment id delivered to the caller per reader",
	}, []string{"log_ns", "log_id", "reader_name"})
	WpClientReadFrontierEntry = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_frontier_entry",
		Help:      "Highest entry id delivered to the caller in the read frontier segment per reader",
	}, []string{"log_ns", "log_id", "reader_name"})

	// client append data to log
	WpClientAppendRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_requests_total",
		Help:      "Total number of append requests",
	}, []string{"log_ns", "log_id"})
	WpClientAppendBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_bytes",
		Help:      "Size of append operations in bytes",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 8), // 256B ~ 4MB
	}, []string{"log_ns", "log_id"})
	WpClientAppendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "append_latency",
		Help:      "Latency of append operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id"})

	// LogHandle metrics
	WpLogHandleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "log_handle_operations_total",
		Help:      "Total number of log handle operations",
	}, []string{"log_ns", "log_id", "operation", "status"})
	WpLogHandleOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "log_handle_operation_latency",
		Help:      "Latency of log handle operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id", "operation", "status"})

	// Client read metrics
	WpClientReadRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_requests_total",
		Help:      "Total number of read requests",
	}, []string{"log_ns", "log_id"})
	WpClientReadEntriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_entries_total",
		Help:      "Total number of entries read",
	}, []string{"log_ns", "log_id"})
	WpClientReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_latency",
		Help:      "Latency of read operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id"})

	// LogReader metrics
	WpLogReaderBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "reader_bytes_read",
		Help:      "Total bytes read by log readers",
	}, []string{"log_ns", "log_id", "reader_name"})
	WpLogReaderOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "reader_operation_latency",
		Help:      "Latency of log reader operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id", "operation", "status"})
	// Failures to maintain a reader's temp info. A sustained non-zero rate means
	// the reader's read position is going stale in metadata; if its session lease
	// is also lost, the writer stops protecting the segments it still needs.
	WpLogReaderTempInfoErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "reader_temp_info_errors_total",
		Help:      "Total reader temp info maintenance failures, by operation",
	}, []string{"log_ns", "log_id", "operation"})
	// Forced read-position moves over physically cleaned (GC'd) segments. This is
	// an expected outcome of truncation, but it is silent to the application, so
	// it is counted here: entries in the skipped range are never delivered.
	WpLogReaderGCSkipsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "reader_gc_skips_total",
		Help:      "Total times a reader skipped forward over a cleaned(GC'd) segment range",
	}, []string{"log_ns", "log_id"})

	// LogWriter metrics
	WpLogWriterBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "writer_bytes_written",
		Help:      "Total bytes written by log writers",
	}, []string{"log_ns", "log_id"})
	WpLogWriterOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "writer_operation_latency",
		Help:      "Latency of log writer operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id", "operation", "status"})

	// SegmentHandle metrics
	WpSegmentHandleOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_handle_operations_total",
		Help:      "Total number of segment handle operations",
	}, []string{"log_ns", "log_id", "operation", "status"})
	WpSegmentHandleOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_handle_operation_latency",
		Help:      "Latency of segment handle operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id", "operation", "status"})
	WpSegmentHandlePendingAppendOps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_handle_pending_append_ops",
		Help:      "Number of pending append operations in segment handles",
	}, []string{"log_ns", "log_id"})
	WpSegmentCompactionFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "segment_compaction_failures_total",
		Help:      "Total number of failed segment compaction operations split by stable reason",
	}, []string{"log_ns", "log_id", "reason"})

	// Active (writable) segment -> quorum node membership. One series per node of
	// the current active segment; value is always 1. Series are deleted when the
	// segment leaves the writable state, so a query reflects only the currently
	// active segment of each log (used to verify node switching/failover).
	WpActiveSegmentNode = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "active_segment_node",
		Help:      "Quorum node membership of each log's current active (writable) segment (value always 1, one series per node)",
	}, []string{"log_ns", "log_id", "segment_id", "node", "az"})

	// Direct read metrics (client reads sealed segments directly from object storage)
	WpClientDirectReadRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "direct_read_requests_total",
		Help:      "Total number of direct read requests from object storage for sealed segments",
	}, []string{"log_ns", "log_id", "status"})
	WpClientDirectReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "direct_read_latency",
		Help:      "Latency of direct read operations from object storage for sealed segments",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "log_id"})

	// Replica placement metrics. az_scope is where the peer sits relative to
	// this client — local / cross_az / cross_region, or unknown when either
	// side's topology is unset. The node endpoint is deliberately NOT a label:
	// it would multiply by log_id and explode cardinality, while az_scope stays
	// at four values and answers the question that costs money.
	WpClientReplicaAppendTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "replica_append_total",
		Help:      "Total per-replica append outcomes, by how far the replica sits from this client",
	}, []string{"log_ns", "log_id", "az_scope", "status"})
	WpClientReplicaAppendBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "replica_append_bytes_total",
		Help:      "Total bytes acknowledged per replica, by how far the replica sits from this client",
	}, []string{"log_ns", "log_id", "az_scope"})
	// Quorum reads of an active segment. A caught-up tail reader polls
	// continuously and gets "no entry yet" every time, so that outcome is not
	// counted here at all — only reads that moved data or genuinely failed.
	WpClientQuorumReadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "quorum_read_total",
		Help:      "Total quorum read outcomes, by how far the serving replica sits from this client",
	}, []string{"log_ns", "log_id", "az_scope", "status"})
	WpClientQuorumReadBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "quorum_read_bytes_total",
		Help:      "Total bytes read from quorum replicas, by how far the serving replica sits from this client",
	}, []string{"log_ns", "log_id", "az_scope"})
	WpClientReadFailoverTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "read_failover_total",
		Help:      "Total quorum reads served by a replica in a different scope than the preferred one",
	}, []string{"log_ns", "log_id", "from_scope", "to_scope"})

	// Etcd Meta metrics
	WpEtcdMetaOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "etcd_meta_operations_total",
		Help:      "Total number of etcd meta related operations",
	}, []string{"log_ns", "operation", "status"})
	WpEtcdMetaOperationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: woodpeckerNamespace,
		Subsystem: clientRole,
		Name:      "etcd_meta_operation_latency",
		Help:      "Latency of etcd meta related operations",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 1024ms
	}, []string{"log_ns", "operation", "status"})
)

type progressFrontier struct {
	segmentID   int64
	entryID     int64
	initialized bool
}

// RegisterClientMetricsWithRegisterer registers all client-side metrics with the given registerer.
// Without calling this, metrics still work but are not scraped by any registry.
func RegisterClientMetricsWithRegisterer(registerer prometheus.Registerer) {
	WpClientRegisterOnce.Do(func() {
		// log name-id mapping
		registerer.MustRegister(WpLogNameIdMapping)
		// segment state tracking
		registerer.MustRegister(WpClientSegmentState)
		registerer.MustRegister(WpClientWriteFrontierSegment)
		registerer.MustRegister(WpClientWriteFrontierEntry)
		registerer.MustRegister(WpClientCompactionFrontierSegment)
		registerer.MustRegister(WpClientCompactionFrontierEntry)
		registerer.MustRegister(WpClientTruncationFrontierSegment)
		registerer.MustRegister(WpClientTruncationFrontierEntry)
		registerer.MustRegister(WpClientReadFrontierSegment)
		registerer.MustRegister(WpClientReadFrontierEntry)

		// Client append metrics
		registerer.MustRegister(WpClientAppendRequestsTotal)
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
		registerer.MustRegister(WpLogReaderTempInfoErrorsTotal)
		registerer.MustRegister(WpLogReaderGCSkipsTotal)
		// LogWriter metrics
		registerer.MustRegister(WpLogWriterBytesWritten)
		registerer.MustRegister(WpLogWriterOperationLatency)
		// SegmentHandle metrics
		registerer.MustRegister(WpSegmentHandleOperationsTotal)
		registerer.MustRegister(WpSegmentHandleOperationLatency)
		registerer.MustRegister(WpSegmentHandlePendingAppendOps)
		registerer.MustRegister(WpSegmentCompactionFailuresTotal)
		// Active segment node membership
		registerer.MustRegister(WpActiveSegmentNode)
		// Direct read metrics
		registerer.MustRegister(WpClientDirectReadRequestsTotal)
		registerer.MustRegister(WpClientDirectReadLatency)
		// Replica placement metrics
		registerer.MustRegister(WpClientReplicaAppendTotal)
		registerer.MustRegister(WpClientReplicaAppendBytesTotal)
		registerer.MustRegister(WpClientQuorumReadTotal)
		registerer.MustRegister(WpClientQuorumReadBytesTotal)
		registerer.MustRegister(WpClientReadFailoverTotal)
		// etcd meta metrics
		registerer.MustRegister(WpEtcdMetaOperationsTotal)
		registerer.MustRegister(WpEtcdMetaOperationLatency)
	})
}

// UpdateSegmentState transitions the segment state gauge: decrements the old state count
// and increments the new state count for the given logNs and log.
func UpdateSegmentState(logNs, logId, oldState, newState string) {
	WpClientSegmentState.WithLabelValues(logNs, logId, oldState).Dec()
	WpClientSegmentState.WithLabelValues(logNs, logId, newState).Inc()
}

func SetWriteFrontier(logNs, logId string, segmentId, entryId int64) {
	setMonotonicFrontier(writeFrontiers, WpClientWriteFrontierSegment, WpClientWriteFrontierEntry, logNs, logId, segmentId, entryId)
}

func SetCompactionFrontier(logNs, logId string, segmentId, entryId int64) {
	setMonotonicFrontier(compactionFrontiers, WpClientCompactionFrontierSegment, WpClientCompactionFrontierEntry, logNs, logId, segmentId, entryId)
}

func SetTruncationFrontier(logNs, logId string, segmentId, entryId int64) {
	setMonotonicFrontier(truncationFrontiers, WpClientTruncationFrontierSegment, WpClientTruncationFrontierEntry, logNs, logId, segmentId, entryId)
}

// SetReadFrontier records how far a reader has delivered to its caller. Unlike
// the write-side frontiers this is keyed by reader as well as by log: a log can
// have several readers at different positions, and sharing one monotonic guard
// would let them clamp each other. Keying by reader also gives the right
// behaviour for free when a reader is reopened at an earlier position -- it is a
// different reader_name, so it is a different series and legitimately starts
// lower.
func SetReadFrontier(logNs, logId, readerName string, segmentId, entryId int64) {
	setMonotonicFrontierKeyed(readFrontiers, WpClientReadFrontierSegment, WpClientReadFrontierEntry,
		readFrontierKey(logNs, logId, readerName), []string{logNs, logId, readerName}, segmentId, entryId)
}

// ClearReadFrontier drops a reader's series when it closes. reader_name carries a
// unique id, so without this every reader ever opened would leave a series behind
// with a frozen value -- readers are rebuilt on every scanner restart. Already
// scraped samples stay in Prometheus, so nothing historical is lost; the series
// simply stops appearing in instant queries.
func ClearReadFrontier(logNs, logId, readerName string) {
	labels := prometheus.Labels{"log_ns": logNs, "log_id": logId, "reader_name": readerName}
	WpClientReadFrontierSegment.Delete(labels)
	WpClientReadFrontierEntry.Delete(labels)
	frontierMu.Lock()
	delete(readFrontiers, readFrontierKey(logNs, logId, readerName))
	frontierMu.Unlock()
}

func readFrontierKey(logNs, logId, readerName string) string {
	return logNs + "\x00" + logId + "\x00" + readerName
}

func setMonotonicFrontier(frontiers map[string]progressFrontier, segmentGauge, entryGauge *prometheus.GaugeVec, logNs, logId string, segmentId, entryId int64) {
	setMonotonicFrontierKeyed(frontiers, segmentGauge, entryGauge, logNs+"\x00"+logId, []string{logNs, logId}, segmentId, entryId)
}

func setMonotonicFrontierKeyed(frontiers map[string]progressFrontier, segmentGauge, entryGauge *prometheus.GaugeVec, key string, labels []string, segmentId, entryId int64) {
	frontierMu.Lock()
	current := frontiers[key]
	if current.initialized && (segmentId < current.segmentID || (segmentId == current.segmentID && entryId < current.entryID)) {
		frontierMu.Unlock()
		return
	}
	frontiers[key] = progressFrontier{segmentID: segmentId, entryID: entryId, initialized: true}
	segmentGauge.WithLabelValues(labels...).Set(float64(segmentId))
	entryGauge.WithLabelValues(labels...).Set(float64(entryId))
	frontierMu.Unlock()
}

// ActiveSegmentNode is one replica of a log's current writable segment: the
// node's endpoint and the availability zone it sits in (empty when the quorum
// metadata carries no placement for it).
type ActiveSegmentNode struct {
	Node string
	AZ   string
}

// SetActiveSegmentNodes marks each node of an active (writable) segment's quorum
// with a value of 1 (one series per node). Call when a segment becomes writable.
func SetActiveSegmentNodes(logNs, logId, segmentId string, nodes []ActiveSegmentNode) {
	for _, node := range nodes {
		WpActiveSegmentNode.WithLabelValues(logNs, logId, segmentId, node.Node, node.AZ).Set(1)
	}
}

// ClearActiveSegmentNodes deletes the per-node series for an active segment.
// Call when the segment leaves the writable state (completed / rolling / closed).
// It matches on the segment alone, so a quorum whose membership or placement
// changed since it was published is still removed in full.
func ClearActiveSegmentNodes(logNs, logId, segmentId string) {
	WpActiveSegmentNode.DeletePartialMatch(prometheus.Labels{
		"log_ns":     logNs,
		"log_id":     logId,
		"segment_id": segmentId,
	})
}
