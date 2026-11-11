package milvusquery_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	q "github.com/zilliztech/woodpecker/diagnostics/milvusquery"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

type scenario struct {
	slow, missing map[string]bool
	ready         []float64
	otherReady    bool
	deny          bool
	reset         bool
}

func setup(t *testing.T, s scenario) (d.Config, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if s.deny {
			http.Error(w, "sentinel-secret", 401)
			return
		}
		expr := r.URL.Query().Get("query")
		var spec prometheus.QuerySignal
		for _, entry := range prometheus.QueryCatalog() {
			if strings.Contains(expr, entry.Metric) {
				spec = entry
				break
			}
		}
		require.NotEmpty(t, spec.ID)
		rows := []map[string]any{}
		if !s.missing[spec.ID] {
			component := spec.Component
			if component == "consumer" {
				component = "querynode"
			}
			labels := map[string]string{"cluster": "test", "component": component, "instance": component + ":9091", "node_id": component, "job": component}
			if spec.Kind == "histogram" && spec.ID != "milvus.query.loading" {
				labels["query_type"] = "Search"
			}
			if spec.ID == "milvus.query.queue" {
				labels["db_name"] = "default"
				labels["rg"] = "default"
			}
			if spec.ID == "milvus.query.segment" {
				labels["segment_state"] = "sealed"
			}
			if spec.ID == "milvus.query.tt_lag" {
				labels["collection_id"] = "7"
			}
			if s.otherReady && spec.ID == "milvus.query.ready" {
				labels["instance"] = "other:9091"
			}
			makeRow := func(name, le string, values []float64) map[string]any {
				m := map[string]string{}
				for k, v := range labels {
					m[k] = v
				}
				m["__name__"] = name
				if le != "" {
					m["le"] = le
				}
				points := [][]any{}
				for i, v := range values {
					points = append(points, []any{1005 + i*10, strconv.FormatFloat(v, 'f', -1, 64)})
				}
				return map[string]any{"metric": m, "values": points}
			}
			if spec.Kind != "histogram" {
				values := []float64{0, 0, 0, 0, 0, 0}
				if spec.ID == "milvus.query.ready" && s.ready != nil {
					values = s.ready
				}
				if spec.ID == "milvus.query.concurrency" {
					values = []float64{2, 2, 2, 2, 2, 2}
				}
				rows = append(rows, makeRow(spec.Metric, "", values))
			} else {
				count := []float64{0, 20, 40, 60, 80, 100}
				if s.reset {
					count = []float64{0, 20, 40, 0, 20, 40}
				}
				latency := 10.0
				if s.slow[spec.ID] {
					latency = 5000
				}
				sum := []float64{}
				for _, v := range count {
					sum = append(sum, v*latency)
				}
				rows = append(rows, makeRow(spec.Metric+"_count", "", count), makeRow(spec.Metric+"_sum", "", sum))
				for _, upper := range []float64{10, 100, 1000, 10000} {
					values := make([]float64, 6)
					if upper >= latency {
						copy(values, count)
					}
					rows = append(rows, makeRow(spec.Metric+"_bucket", strconv.FormatFloat(upper, 'f', -1, 64), values))
				}
				rows = append(rows, makeRow(spec.Metric+"_bucket", "+Inf", count))
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": rows}})
	}))
	t.Cleanup(server.Close)
	cfg, err := d.ParseConfig([]byte(fmt.Sprintf("schema_version: wpdiag.config/v1\nsources:\n  prometheus:\n    connections:\n      test:\n        settings:\n          endpoint: %s\n          deployment: fixture\n          match_labels: {cluster: test}\n", server.URL)), t.TempDir())
	require.NoError(t, err)
	return cfg, calls
}

func request(scope string) d.Request {
	from, to := time.Unix(1000, 0).UTC(), time.Unix(1060, 0).UTC()
	return d.Request{Selection: d.Selection{Sources: []string{"prometheus"}, Scope: scope, Flow: "read", Stage: "query", Window: d.Window{Mode: "historical", From: &from, To: &to}}, Connection: "test", Limits: d.DefaultLimits()}
}

func outcome(t *testing.T, report d.Report, id string) d.Finding {
	t.Helper()
	require.Len(t, report.Assessments, 1)
	for _, f := range report.Assessments[0].Findings {
		if f.RuleID == id {
			return f
		}
	}
	t.Fatalf("missing stage rule %s", id)
	return d.Finding{}
}

func TestQueryStepsLocalizeWithoutReplacingSourceRules(t *testing.T) {
	for _, tc := range []struct{ id, want string }{
		{"milvus.query", "observed_slow_proxy_completions"},
		{"milvus.query.tsafe", "suspected_visibility_wait"},
		{"milvus.query.core", "suspected_execution_work"},
		{"milvus.query.loading", "observed_slow_segment_loading"},
		{"milvus.query.wait_result", "suspected_downstream_result_wait"},
		{"milvus.query.decode", "suspected_proxy_result_processing"},
		{"milvus.query.segment", "observed_slow_segment_work"},
		{"milvus.query.node_reduce", "observed_slow_downstream_reduction"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			cfg, calls := setup(t, scenario{slow: map[string]bool{tc.id: true}})
			report := q.Run(context.Background(), cfg, request("milvus"))
			require.Equal(t, "completed", report.Status)
			require.Equal(t, int64(len(prometheus.QueryCatalog())), calls.Load())
			rule := tc.id
			if tc.id == "milvus.query" {
				rule = "milvus.query.proxy_completion"
			}
			f := outcome(t, report, rule)
			require.Equal(t, tc.want, f.Outcome)
			require.Len(t, f.DerivedFrom, 1)
			source := map[string]d.Finding{}
			obs := map[string]bool{}
			for _, s := range report.Results[0].Findings {
				source[s.ID] = s
			}
			for _, o := range report.Results[0].Observations {
				obs[o.ID] = true
			}
			require.Equal(t, "suspected_high_latency", source[f.DerivedFrom[0]].Outcome)
			for _, id := range f.Supporting {
				require.True(t, obs[id])
			}
			require.Contains(t, f.Unknown, "independent_metric_populations_not_one_request")
			body, _, err := d.EncodeReport(report)
			require.NoError(t, err)
			require.NotContains(t, string(body), "sentinel-secret")
		})
	}
}

func TestSchedulingPressureRecoveryAndContradiction(t *testing.T) {
	for _, tc := range []struct {
		name          string
		ready         []float64
		other         bool
		want          string
		contradiction bool
	}{
		{"pressure", []float64{2, 2, 3, 3, 4, 5}, false, "suspected_scheduling_pressure", false},
		{"recovery", []float64{3, 3, 2, 1, 0, 0}, false, "slow_queue_completions_with_backlog_recovery", true},
		{"empty", []float64{0, 0, 0, 0, 0, 0}, false, "slow_queue_completions_without_observed_backlog", true},
		{"wrong exporter", []float64{2, 2, 3, 3, 4, 5}, true, "suspected_scheduling_delay", false},
		{"intermittent", []float64{0, 0, 0, 2, 1, 3}, false, "suspected_scheduling_delay", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.query.queue": true}, ready: tc.ready, otherReady: tc.other})
			report := q.Run(context.Background(), cfg, request("milvus"))
			f := outcome(t, report, "milvus.query.queue")
			require.Equal(t, tc.want, f.Outcome)
			require.Equal(t, tc.contradiction, len(f.Contradicting) > 0)
			if tc.contradiction {
				require.Equal(t, "inconclusive", f.Strength)
				require.Equal(t, "unknown", f.Severity)
			}
			if tc.other {
				require.Contains(t, f.Unknown, "missing_ready_association")
			} else {
				require.Len(t, f.DerivedFrom, 2)
			}
		})
	}
}

func TestHealthyIdleMissingResetAndE2ELimits(t *testing.T) {
	cfg, _ := setup(t, scenario{})
	report := q.Run(context.Background(), cfg, request("e2e"))
	require.Equal(t, "within_configured_latency_for_observed_completions", outcome(t, report, "milvus.query.core").Outcome)
	concurrency := outcome(t, report, "milvus.query.concurrency")
	require.Contains(t, concurrency.Unknown, "executing_concurrency_is_not_configured_capacity")
	gapCodes := []string{}
	for _, g := range report.Assessments[0].Gaps {
		gapCodes = append(gapCodes, g.Code)
	}
	require.Contains(t, gapCodes, "wp_visibility_mapping_unavailable")
	cfg, _ = setup(t, scenario{missing: map[string]bool{"milvus.query.loading": true}})
	report = q.Run(context.Background(), cfg, request("milvus"))
	require.Equal(t, "inconclusive", outcome(t, report, "milvus.query.loading").Outcome)
	cfg, _ = setup(t, scenario{reset: true})
	report = q.Run(context.Background(), cfg, request("milvus"))
	require.Contains(t, outcome(t, report, "milvus.query.core").Unknown, "counter_reset_or_restart")
	cfg, _ = setup(t, scenario{deny: true})
	report = q.Run(context.Background(), cfg, request("milvus"))
	require.Equal(t, "failed", report.Status)
	require.Equal(t, "failed", report.Assessments[0].Status)
}

func TestSelectionRejectsBeforeCollection(t *testing.T) {
	cfg, calls := setup(t, scenario{})
	for _, edit := range []func(*d.Request){
		func(r *d.Request) { r.Selection.Scope = "wp" }, func(r *d.Request) { r.Selection.Flow = "write" },
		func(r *d.Request) { r.Selection.Stage = "append" }, func(r *d.Request) { r.Selection.Target.Collection = "7" },
		func(r *d.Request) { r.Selection.Target.Channel = "channel" }, func(r *d.Request) { r.Selection.Sources = []string{"trace"} },
		func(r *d.Request) { r.Selection.Window = d.Window{Mode: "snapshot"} },
	} {
		r := request("milvus")
		edit(&r)
		report := q.Run(context.Background(), cfg, r)
		require.Contains(t, []string{"invalid", "unsupported"}, report.Status)
		require.Empty(t, report.Assessments)
	}
	require.Zero(t, calls.Load())
}

func TestAssessmentIsReadOnlyRepeatableAndFailsClosed(t *testing.T) {
	cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.query.queue": true}, ready: []float64{2, 2, 2, 2, 2, 2}})
	engine, err := d.NewEngine(prometheus.QueryModule{})
	require.NoError(t, err)
	source := engine.Run(context.Background(), cfg, request("milvus"))
	before, err := json.Marshal(source)
	require.NoError(t, err)
	a := q.Assess(context.Background(), source)
	b := q.Assess(context.Background(), source)
	require.Equal(t, a, b)
	after, err := json.Marshal(source)
	require.NoError(t, err)
	require.Equal(t, before, after)
	for _, tc := range []struct {
		code string
		edit func(*d.Report)
	}{
		{"unsupported_stage_input", func(r *d.Report) { r.SchemaVersion = "wpdiag.report/v2" }},
		{"unsupported_source_profile", func(r *d.Report) { r.Results[0].Profile = "milvus.read.metrics" }},
		{"unsupported_source_schema_or_window", func(r *d.Report) { r.Results[0].Metadata = json.RawMessage(`{}`) }},
		{"source_selection_window_or_deployment_mismatch", func(r *d.Report) { from := time.Unix(900, 0).UTC(); r.Selection.Window.From = &from }},
		{"conflicting_observation_identity", func(r *d.Report) {
			o := r.Results[0].Observations[0]
			o.Code = "different"
			r.Results[0].Observations = append(r.Results[0].Observations, o)
		}},
		{"unsupported_source_finding_contract", func(r *d.Report) { r.Results[0].Findings[0].RuleVersion = 2 }},
		{"dangling_or_wrong_source_reference", func(r *d.Report) { r.Results[0].Findings[0].Supporting = []string{"missing"} }},
		{"unsupported_source_rule_or_outcome", func(r *d.Report) { r.Results[0].Findings[0].Outcome = "confirmed_root_cause" }},
		{"invalid_fact_identity_or_time", func(r *d.Report) { r.Results[0].Findings[0].Target.Instance = "wrong" }},
		{"invalid_observation_contract", func(r *d.Report) {
			var data prometheus.Dataset
			require.NoError(t, json.Unmarshal(r.Results[0].Observations[0].Data, &data))
			data.From = data.From.Add(time.Second)
			body, err := json.Marshal(data)
			require.NoError(t, err)
			r.Results[0].Observations[0].Data = body
		}},
	} {
		t.Run(tc.code, func(t *testing.T) {
			var modified d.Report
			require.NoError(t, json.Unmarshal(before, &modified))
			tc.edit(&modified)
			a := q.Assess(context.Background(), modified)
			require.Equal(t, "failed", a.Status)
			require.Equal(t, tc.code, a.Gaps[0].Code)
			require.Empty(t, a.Findings)
		})
	}
	source.Results[0].Findings = append(source.Results[0].Findings, source.Results[0].Findings[0])
	source.Results[0].Observations = append(source.Results[0].Observations, source.Results[0].Observations[0])
	dedup := q.Assess(context.Background(), source)
	require.Len(t, dedup.Findings, len(a.Findings))
	require.GreaterOrEqual(t, len(dedup.Gaps), 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, "cancelled", q.Assess(ctx, source).Status)
}

var update = flag.Bool("update-query-fixture", false, "Update the query-stage JSON fixture")

func TestStageFixture(t *testing.T) {
	cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.query.queue": true}, ready: []float64{3, 3, 2, 1, 0, 0}})
	report := q.Run(context.Background(), cfg, request("e2e"))
	var meta struct {
		RunID string `json:"source_run_id"`
	}
	require.NoError(t, json.Unmarshal(report.Results[0].Metadata, &meta))
	body, err := json.MarshalIndent(report.Assessments[0], "", "  ")
	require.NoError(t, err)
	body = []byte(strings.ReplaceAll(string(body), meta.RunID, "fixture-query-source") + "\n")
	path := filepath.Join("testdata", "recovery-v1.json")
	if *update {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, body, 0o644))
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(body))
}
