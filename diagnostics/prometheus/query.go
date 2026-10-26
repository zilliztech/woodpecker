package prometheus

import (
	"context"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// QueryModule collects/evaluates query-step source facts using the same client,
// histogram rules and policies as Module. Business hypotheses live in milvusquery.
// It has a separate descriptor so existing source defaults remain unambiguous.
type QueryModule struct{}

func (QueryModule) Descriptor() d.Descriptor {
	desc := d.Descriptor{Source: "prometheus", Issue: 466, RequiredSettings: []string{"endpoint", "deployment", "match_labels"}}
	for _, scope := range []string{"milvus", "e2e"} {
		desc.Profiles = append(desc.Profiles, d.Profile{ID: scope + ".query.steps.metrics", Version: 1, Scope: scope, Flow: "read", Stage: "query", Modes: []string{"live", "historical"}, TargetFields: []string{"deployment", "instance"}, Description: "Query-step metric facts; incomplete/missing populations stay explicit, without WP or cross-request joins"})
	}
	return desc
}

func (QueryModule) ValidateConnection(c d.Connection) error {
	_, err := settingsFor(c, true)
	return err
}

func (QueryModule) Run(ctx context.Context, r d.Request, c d.Connection) d.Result {
	return runPlan(ctx, r, c, true)
}

// QuerySignal is the source-owned, versioned metric contract exposed to stage
// consumers and deterministic fixtures. Thresholds are owned by source policy.
type QuerySignal struct {
	ID          string  `json:"id"`
	Metric      string  `json:"metric"`
	Component   string  `json:"component"`
	Kind        string  `json:"kind"`
	ThresholdMS float64 `json:"default_threshold_ms,omitempty"`
}

func QueryCatalog() []QuerySignal {
	return []QuerySignal{
		{"milvus.query", "milvus_proxy_sq_latency", "proxy", "histogram", 1000},
		{"milvus.query.tsafe", "milvus_querynode_sq_wait_tsafe_latency", "consumer", "histogram", 250},
		{"milvus.query.handler", "milvus_querynode_sq_req_latency", "consumer", "histogram", 1000},
		{"milvus.query.queue", "milvus_querynode_sq_queue_latency", "consumer", "histogram", 100},
		{"milvus.query.ready", "milvus_querynode_read_task_ready_len", "consumer", "tasks", 0},
		{"milvus.query.concurrency", "milvus_querynode_read_task_concurrency", "consumer", "tasks", 0},
		{"milvus.query.core", "milvus_querynode_sq_core_latency", "consumer", "histogram", 500},
		{"milvus.query.segment", "milvus_querynode_sq_segment_latency", "consumer", "histogram", 500},
		{"milvus.query.loading", "milvus_querynode_load_segment_latency", "consumer", "histogram", 1000},
		{"milvus.query.wait_result", "milvus_proxy_sq_wait_result_latency", "proxy", "histogram", 1000},
		{"milvus.query.proxy_reduce", "milvus_proxy_sq_reduce_result_latency", "proxy", "histogram", 250},
		{"milvus.query.decode", "milvus_proxy_sq_decode_result_latency", "proxy", "histogram", 250},
		{"milvus.query.node_reduce", "milvus_querynode_sq_reduce_latency", "consumer", "histogram", 250},
		{"milvus.query.tt_lag", "milvus_querynode_consume_tt_lag_ms", "consumer", "milliseconds", 0},
	}
}

func queryThresholds() map[string]float64 {
	values := map[string]float64{}
	for _, sig := range QueryCatalog() {
		if sig.Kind == "histogram" {
			values[sig.ID] = sig.ThresholdMS
		}
	}
	return values
}

func querySignals() []signal {
	plan := []signal{}
	for _, entry := range QueryCatalog() {
		sig := signal{ID: entry.ID, Metric: entry.Metric, Component: entry.Component, Stage: "query", Factor: 1, Gauge: entry.Kind != "histogram", GaugeUnit: entry.Kind}
		if entry.Component == "consumer" {
			sig.Component = ""
			sig.Regex = map[string]string{"component": "querynode|streamingnode"}
		}
		plan = append(plan, sig)
	}
	return plan
}

// Preserve unknown dimensions in the hash. Only documented request dimensions
// are projected away for exporter-level association, never a request/tenant join.
func annotateExporters(data *Dataset) {
	identities := map[string]string{}
	for _, series := range data.Series {
		population, exporter := map[string]string{}, map[string]string{}
		for k, v := range series.AllLabels {
			if k == "__name__" || k == "le" {
				continue
			}
			population[k] = v
			if !d.Has([]string{"query_type", "segment_state", "reduce_level", "reduce_type", "collection_id", "collection_name", "database", "resource_group", "db_name", "rg", "msg_type", "outcome"}, k) {
				exporter[k] = v
			}
		}
		identities[fingerprint(population)] = fingerprint(exporter)
	}
	for i := range data.Facts {
		data.Facts[i].ExporterKey = identities[data.Facts[i].Population]
	}
}
