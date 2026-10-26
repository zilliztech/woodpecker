package prometheus

import (
	"context"
	"strings"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// PersistenceModule collects source facts; business semantics live in
// diagnostics/milvuspersistence. Existing overview/query profiles keep their plans.
type PersistenceModule struct{}

func (PersistenceModule) Descriptor() d.Descriptor {
	desc := d.Descriptor{Source: "prometheus", Issue: 465, RequiredSettings: []string{"endpoint", "deployment", "match_labels"}}
	for _, scope := range []string{"milvus", "e2e"} {
		desc.Profiles = append(desc.Profiles, d.Profile{ID: scope + ".persistence.steps.metrics", Version: 1, Scope: scope, Flow: "write", Stage: "persistence", Modes: []string{"live", "historical"}, TargetFields: []string{"deployment", "instance"}, Description: "Persistence/flush/checkpoint source facts; WP maintenance remains parallel unassociated context"})
	}
	return desc
}

func (PersistenceModule) ValidateConnection(c d.Connection) error {
	_, err := settingsFor(c, persistencePlan)
	return err
}

func (PersistenceModule) Run(ctx context.Context, r d.Request, c d.Connection) d.Result {
	return runPlan(ctx, r, c, persistencePlan)
}

type PersistenceSignal struct {
	ID          string            `json:"id"`
	Metric      string            `json:"metric"`
	Component   string            `json:"component"`
	Kind        string            `json:"kind"`
	ThresholdMS float64           `json:"default_threshold_ms,omitempty"`
	Factor      float64           `json:"metric_to_milliseconds,omitempty"`
	Match       map[string]string `json:"match_labels,omitempty"`
	Regex       map[string]string `json:"regex_labels,omitempty"`
}

// Catalog v1 uses the Helm example's Milvus 27bd28d20688ab00cdd74232a20e988fb2162aed
// and WP definitions. Gauge names ending in bytes/total are not assumed counters.
func PersistenceCatalog() []PersistenceSignal {
	return []PersistenceSignal{
		{"milvus.persistence.growing", "milvus_wal_growing_segment_bytes", "streamingnode", "bytes", 0, 0, nil, nil},
		{"milvus.persistence.pressure", "milvus_wal_growing_segment_flush_pressure_bytes", "streamingnode", "bytes", 0, 0, nil, nil},
		{"milvus.persistence.hwm", "milvus_wal_growing_segment_hwm_bytes", "streamingnode", "bytes", 0, 0, nil, nil},
		{"milvus.persistence.flusher", "milvus_wal_flusher_info", "streamingnode", "state_indicator", 0, 0, nil, nil},
		{"milvus.persistence.pending", "milvus_wal_flusher_sync_dispatcher_pending_tasks", "streamingnode", "tasks", 0, 0, nil, nil},
		{"milvus.persistence.queue", "milvus_wal_flusher_sync_dispatcher_queue_duration_seconds", "streamingnode", "histogram", 250, 1000, nil, nil},
		{"milvus.persistence.execute", "milvus_wal_flusher_sync_dispatcher_execute_duration_seconds", "streamingnode", "histogram", 1000, 1000, nil, nil},
		{"milvus.persistence.save", "milvus_datanode_save_latency", "persistor", "histogram", 1000, 1, nil, map[string]string{"component": "streamingnode|datanode"}},
		{"milvus.persistence.put", "milvus_storage_request_latency", "streamingnode", "histogram", 1000, 1, map[string]string{"persistent_data_op_type": "put"}, nil},
		{"milvus.persistence.seen", "milvus_wal_flusher_time_tick", "streamingnode", "unix_seconds", 0, 0, nil, nil},
		{"milvus.persistence.memory", "milvus_wal_recovery_in_mem_time_tick", "streamingnode", "unix_seconds", 0, 0, nil, nil},
		{"milvus.persistence.persisted", "milvus_wal_recovery_persisted_time_tick", "streamingnode", "unix_seconds", 0, 0, nil, nil},
		{"milvus.persistence.busy", "milvus_wal_recovery_is_on_persisting", "streamingnode", "state_indicator", 0, 0, nil, nil},
		{"milvus.persistence.checkpoint", "milvus_datacoord_channel_checkpoint_unix_seconds", "datacoord", "unix_seconds", 0, 0, nil, nil},
		{"wp.flush", "woodpecker_server_file_flush_latency", "woodpecker", "histogram", 100, 1, nil, nil},
		{"wp.compaction", "woodpecker_server_file_compaction_latency", "woodpecker", "histogram", 1000, 1, nil, nil},
		{"wp.storage", "woodpecker_server_object_storage_operation_latency", "woodpecker", "histogram", 1000, 1, map[string]string{"status": "success"}, nil},
		{"wp.disk_state", "woodpecker_server_logstore_write_backpressure_state", "woodpecker", "state_0_normal_1_warn_2_blocked", 0, 0, nil, nil},
	}
}

func persistenceThresholds() map[string]float64 {
	out := map[string]float64{}
	for _, s := range PersistenceCatalog() {
		if s.Kind == "histogram" {
			out[s.ID] = s.ThresholdMS
		}
	}
	return out
}

func persistenceSignals(scope string) []signal {
	out := []signal{}
	for _, s := range PersistenceCatalog() {
		if scope == "milvus" && strings.HasPrefix(s.ID, "wp.") {
			continue
		}
		component := s.Component
		if component == "persistor" {
			component = ""
		}
		out = append(out, signal{ID: s.ID, Metric: s.Metric, Component: component, Stage: "persistence", Factor: s.Factor, Match: s.Match, Regex: s.Regex, Gauge: s.Kind != "histogram", GaugeUnit: s.Kind})
	}
	return out
}
