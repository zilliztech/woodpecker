package prometheus

import (
	"context"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// AppendModule reuses the source transport, raw-scrape quality checks and
// histogram policy. Stage interpretation belongs to diagnostics/milvusappend.
type AppendModule struct{}

func (AppendModule) Descriptor() d.Descriptor {
	desc := d.Descriptor{Source: "prometheus", Issue: 463, RequiredSettings: []string{"endpoint", "deployment", "match_labels"}}
	for _, scope := range []string{"milvus", "e2e"} {
		desc.Profiles = append(desc.Profiles, d.Profile{ID: scope + ".append.steps.metrics", Version: 1, Scope: scope, Flow: "write", Stage: "append", Modes: []string{"live", "historical"}, TargetFields: []string{"deployment", "instance"}, Description: "Append-step source facts, independent exporter populations; no inferred collection/channel/WP tenant joins"})
	}
	return desc
}

func (AppendModule) ValidateConnection(c d.Connection) error {
	_, e := settingsFor(c, appendPlan)
	return e
}

func (AppendModule) Run(ctx context.Context, r d.Request, c d.Connection) d.Result {
	return runPlan(ctx, r, c, appendPlan)
}

type AppendSignal struct {
	ID          string            `json:"id"`
	Metric      string            `json:"metric"`
	Component   string            `json:"component"`
	Kind        string            `json:"kind"`
	ThresholdMS float64           `json:"default_threshold_ms,omitempty"`
	Factor      float64           `json:"metric_to_milliseconds,omitempty"`
	Match       map[string]string `json:"match_labels,omitempty"`
	Regex       map[string]string `json:"regex_labels,omitempty"`
}

// AppendCatalog is a source-owned v1 contract pinned to the Helm example's
// Milvus 27bd28d20688ab00cdd74232a20e988fb2162aed and WP metric definitions.
// Defaults are example policies, not product SLOs or copied native WP rules.
func AppendCatalog() []AppendSignal {
	return []AppendSignal{
		{"milvus.append", "milvus_proxy_mutation_latency", "proxy", "histogram", 1000, 1, nil, map[string]string{"msg_type": "insert|upsert|delete"}},
		{"milvus.append.pk", "milvus_proxy_apply_pk_latency", "proxy", "histogram", 100, 1, nil, nil},
		{"milvus.append.timestamp", "milvus_proxy_apply_timestamp_latency", "proxy", "histogram", 100, 1, nil, nil},
		{"milvus.append.produce", "milvus_streaming_produce_success_duration_seconds", "proxy", "histogram", 500, 1000, nil, nil},
		{"milvus.append.quota", "milvus_streaming_produce_rate_limit_delay_seconds", "proxy", "histogram", 250, 1000, nil, nil},
		{"milvus.append.unavailable", "milvus_streaming_resuming_producer_total", "proxy", "producers", 0, 0, map[string]string{"status": "unavailable"}, nil},
		{"milvus.append.inflight", "milvus_streamingnode_produce_inflight_total", "streamingnode", "requests", 0, 0, nil, nil},
		{"milvus.wal_append", "milvus_wal_append_message_duration_seconds", "streamingnode", "histogram", 500, 1000, map[string]string{"status": "ok"}, nil},
		{"milvus.append.backend", "milvus_wal_impls_append_message_duration_seconds", "streamingnode", "histogram", 500, 1000, map[string]string{"status": "ok"}, nil},
		{"milvus.append.before", "milvus_wal_interceptor_before_append_duration_seconds", "streamingnode", "histogram", 250, 1000, nil, nil},
		{"milvus.append.after", "milvus_wal_interceptor_after_append_duration_seconds", "streamingnode", "histogram", 250, 1000, nil, nil},
		{"wp.client_append", "woodpecker_client_append_latency", "streamingnode", "histogram", 250, 1, nil, nil},
		{"wp.pending", "woodpecker_client_segment_handle_pending_append_ops", "streamingnode", "operations", 0, 0, nil, nil},
		{"wp.append", "woodpecker_server_logstore_operation_latency", "woodpecker", "histogram", 250, 1, map[string]string{"status": "success"}, map[string]string{"operation": "add_entry|add_entry_batch"}},
		{"wp.buffer_wait", "woodpecker_server_buffer_wait_latency", "woodpecker", "histogram", 100, 1, nil, nil},
		{"wp.flush", "woodpecker_server_file_flush_latency", "woodpecker", "histogram", 100, 1, nil, nil},
		{"wp.disk_state", "woodpecker_server_logstore_write_backpressure_state", "woodpecker", "state_0_normal_1_warn_2_blocked", 0, 0, nil, nil},
	}
}

func appendThresholds() map[string]float64 {
	out := map[string]float64{}
	for _, sig := range AppendCatalog() {
		if sig.Kind == "histogram" {
			out[sig.ID] = sig.ThresholdMS
		}
	}
	return out
}

func appendSignals(scope string) []signal {
	out := []signal{}
	for _, s := range AppendCatalog() {
		if scope == "milvus" && s.ID[:3] == "wp." {
			continue
		}
		out = append(out, signal{ID: s.ID, Metric: s.Metric, Component: s.Component, Stage: "append", Factor: s.Factor, Match: s.Match, Regex: s.Regex, Gauge: s.Kind != "histogram", GaugeUnit: s.Kind})
	}
	return out
}
