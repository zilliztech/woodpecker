package prometheus

import (
	"context"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// ConsumeModule owns metric collection/quality. Business hypotheses live in
// diagnostics/milvusconsume; native reader/quorum rules remain wpcli-owned.
type ConsumeModule struct{}

func (ConsumeModule) Descriptor() d.Descriptor {
	desc := d.Descriptor{Source: "prometheus", Issue: 464, RequiredSettings: []string{"endpoint", "deployment", "match_labels"}}
	for _, scope := range []string{"milvus", "e2e"} {
		desc.Profiles = append(desc.Profiles, d.Profile{ID: scope + ".consume.steps.metrics", Version: 1, Scope: scope, Flow: "read", Stage: "consume", Modes: []string{"live", "historical"}, TargetFields: []string{"deployment", "instance"}, Description: "Scanner, subscription and visibility source facts; WP retrieval stays unassociated context, not per-reader progress"})
	}
	return desc
}

func (ConsumeModule) ValidateConnection(c d.Connection) error {
	_, err := settingsFor(c, consumePlan)
	return err
}

func (ConsumeModule) Run(ctx context.Context, r d.Request, c d.Connection) d.Result {
	return runPlan(ctx, r, c, consumePlan)
}

type ConsumeSignal struct {
	ID          string            `json:"id"`
	Metric      string            `json:"metric"`
	Component   string            `json:"component"`
	Kind        string            `json:"kind"`
	ThresholdMS float64           `json:"default_threshold_ms,omitempty"`
	Factor      float64           `json:"metric_to_milliseconds,omitempty"`
	Match       map[string]string `json:"match_labels,omitempty"`
	Regex       map[string]string `json:"regex_labels,omitempty"`
}

// Catalog v1 is pinned to the Helm example's Milvus 27bd28d20688ab00cdd74232a20e988fb2162aed
// and WP metric contracts. Blocking read-next time is not storage I/O latency.
func ConsumeCatalog() []ConsumeSignal {
	return []ConsumeSignal{
		{"milvus.consume.mode", "milvus_wal_scanner_total", "streamingnode", "scanners", 0, 0, nil, nil},
		{"milvus.consume.scanned", "milvus_wal_scan_message_total", "streamingnode", "counter_messages", 0, 0, nil, nil},
		{"milvus.consume.passed", "milvus_wal_scan_pass_message_total", "streamingnode", "counter_messages", 0, 0, nil, nil},
		{"milvus.consume.pending", "milvus_wal_scanner_pending_queue_bytes", "streamingnode", "bytes", 0, 0, nil, nil},
		{"milvus.consume.paused", "milvus_wal_scanner_pause_consumption", "streamingnode", "state_indicator", 0, 0, nil, nil},
		{"milvus.consume.txn", "milvus_wal_scanner_txn_buf_bytes", "streamingnode", "bytes", 0, 0, nil, nil},
		{"milvus.consume.ordering", "milvus_wal_scanner_time_tick_buf_bytes", "streamingnode", "bytes", 0, 0, nil, nil},
		{"milvus.consume.subscription", "milvus_streaming_consumer_total", "consumer", "consumers", 0, 0, nil, map[string]string{"component": "streamingnode|querynode"}},
		{"milvus.consume.unavailable", "milvus_streaming_resuming_consumer_total", "consumer", "consumers", 0, 0, map[string]string{"status": "unavailable"}, map[string]string{"component": "streamingnode|querynode"}},
		{"milvus.consume.delivered", "milvus_streaming_consume_bytes_sum", "consumer", "counter_bytes", 0, 0, nil, map[string]string{"component": "streamingnode|querynode"}},
		{"milvus.consume.remote", "milvus_streamingnode_consume_inflight_total", "streamingnode", "requests", 0, 0, nil, nil},
		{"milvus.consume.waiting", "milvus_querynode_wait_processing_msg_count", "consumer", "messages", 0, 0, nil, map[string]string{"component": "streamingnode|querynode"}},
		{"milvus.consume.processing", "milvus_querynode_process_insert_or_delete_latency", "consumer", "histogram", 250, 1, nil, map[string]string{"component": "streamingnode|querynode"}},
		{"milvus.consume.lag", "milvus_querynode_consume_tt_lag_ms", "consumer", "milliseconds", 0, 0, map[string]string{"msg_type": "timetick"}, map[string]string{"component": "streamingnode|querynode"}},
		{"milvus.consume.packs", "milvus_querynode_consume_msg_count", "consumer", "counter_packs", 0, 0, map[string]string{"msg_type": "all"}, map[string]string{"component": "streamingnode|querynode"}},
		{"wp.read_next", "woodpecker_client_reader_operation_latency", "streamingnode", "histogram", 1000, 1, map[string]string{"operation": "read_next", "status": "success"}, nil},
		{"wp.read_batch", "woodpecker_server_read_batch_latency", "woodpecker", "histogram", 250, 1, nil, nil},
		{"wp.direct_read", "woodpecker_client_direct_read_latency", "streamingnode", "histogram", 250, 1, nil, nil},
		{"wp.quorum_bytes", "woodpecker_client_quorum_read_bytes_total", "streamingnode", "counter_bytes", 0, 0, nil, nil},
		{"wp.eof", "woodpecker_server_logstore_operations_total", "woodpecker", "counter_events", 0, 0, map[string]string{"operation": "get_batch_entries", "status": "end_of_file"}, nil},
	}
}

func consumeThresholds() map[string]float64 {
	out := map[string]float64{}
	for _, s := range ConsumeCatalog() {
		if s.Kind == "histogram" {
			out[s.ID] = s.ThresholdMS
		}
	}
	return out
}

func consumeSignals(scope string) []signal {
	out := []signal{}
	for _, s := range ConsumeCatalog() {
		if scope == "milvus" && strings.HasPrefix(s.ID, "wp.") {
			continue
		}
		component := s.Component
		if component == "consumer" {
			component = ""
		}
		out = append(out, signal{ID: s.ID, Metric: s.Metric, Component: component, Stage: "consume", Factor: s.Factor, Match: s.Match, Regex: s.Regex, Gauge: s.Kind != "histogram", GaugeUnit: s.Kind})
	}
	return out
}

// The producer's msg_type=all counts MsgPacks while msg_type=timetick reports
// their lag. Project only this documented dimension, keeping collection,
// exporter and every unknown label; never use the broader query exporter key.
func annotateConsume(data *Dataset) {
	if data.Signal != "milvus.consume.lag" && data.Signal != "milvus.consume.packs" {
		return
	}
	keys := map[string]string{}
	for _, s := range data.Series {
		pop, key := map[string]string{}, map[string]string{}
		for k, v := range s.AllLabels {
			if k == "__name__" {
				continue
			}
			pop[k] = v
			if k != "msg_type" {
				key[k] = v
			}
		}
		keys[fingerprint(pop)] = fingerprint(key)
	}
	for i := range data.Facts {
		data.Facts[i].CollectionKey = keys[data.Facts[i].Population]
	}
}
