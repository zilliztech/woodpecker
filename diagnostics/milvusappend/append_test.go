package milvusappend_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	a "github.com/zilliztech/woodpecker/diagnostics/milvusappend"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

type scenario struct {
	slow, missing                                                    map[string]bool
	gauges                                                           map[string][]float64
	idle, reset, deny, wrongInstance, wrongTime, duplicate, conflict bool
}

func setup(t *testing.T, s scenario) (d.Config, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "GET", r.Method)
		if s.deny {
			http.Error(w, "sentinel-secret", 401)
			return
		}
		expr := r.URL.Query().Get("query")
		var spec prometheus.AppendSignal
		for _, entry := range prometheus.AppendCatalog() {
			if strings.Contains(expr, entry.Metric) {
				spec = entry
				break
			}
		}
		require.NotEmpty(t, spec.ID)
		rows := []map[string]any{}
		if !s.missing[spec.ID] {
			labels := map[string]string{"cluster": "test", "component": spec.Component, "instance": spec.Component + ":9091", "node_id": spec.Component, "job": spec.Component, "secret": "sentinel-secret"}
			for k, v := range spec.Match {
				labels[k] = v
			}
			for k, v := range spec.Regex {
				labels[k] = strings.Split(v, "|")[0]
			}
			if strings.HasPrefix(spec.ID, "wp.") {
				labels["log_ns"] = "wp-client-tenant"
				labels["log_id"] = "9007199254740993"
			}
			if strings.Contains(spec.Metric, "wal_") || strings.Contains(spec.Metric, "streaming_") {
				labels["channel_name"] = "channel-a"
			}
			if strings.Contains(spec.ID, "before") || strings.Contains(spec.ID, "after") {
				labels["interceptor_name"] = "timetick"
			}
			if s.wrongInstance {
				labels["instance"] = "wrong:9091"
			}
			row := func(name, le string, values []float64) map[string]any {
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
					ts := 1005 + i*10
					if s.wrongTime {
						ts -= 100
					}
					points = append(points, []any{ts, strconv.FormatFloat(v, 'f', -1, 64)})
				}
				return map[string]any{"metric": m, "values": points}
			}
			if spec.Kind != "histogram" {
				values := []float64{0, 0, 0, 0, 0, 0}
				if custom := s.gauges[spec.ID]; custom != nil {
					values = custom
				}
				rows = append(rows, row(spec.Metric, "", values))
			} else {
				count := []float64{0, 20, 40, 60, 80, 100}
				if s.idle {
					count = []float64{0, 0, 0, 0, 0, 0}
				}
				if s.reset {
					count = []float64{0, 20, 40, 0, 20, 40}
				}
				latency := 10.0
				if s.slow[spec.ID] {
					latency = 5000
				}
				sum := []float64{}
				for _, c := range count {
					sum = append(sum, c*latency/spec.Factor)
				}
				rows = append(rows, row(spec.Metric+"_count", "", count), row(spec.Metric+"_sum", "", sum))
				for _, bound := range []float64{10, 100, 1000, 10000} {
					values := make([]float64, 6)
					if bound >= latency {
						copy(values, count)
					}
					rows = append(rows, row(spec.Metric+"_bucket", strconv.FormatFloat(bound/spec.Factor, 'f', -1, 64), values))
				}
				rows = append(rows, row(spec.Metric+"_bucket", "+Inf", count))
			}
		}
		if s.duplicate && len(rows) > 0 {
			rows = append(rows, rows[0])
		}
		if s.conflict && len(rows) > 0 {
			copyRow := map[string]any{"metric": rows[0]["metric"], "values": [][]any{{1005, "999999"}, {1015, "999999"}}}
			rows = append(rows, copyRow)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": rows}})
	}))
	t.Cleanup(server.Close)
	cfg, e := d.ParseConfig([]byte(fmt.Sprintf("schema_version: wpdiag.config/v1\nsources:\n  prometheus:\n    connections:\n      test:\n        settings:\n          endpoint: %q\n          deployment: fixture\n          match_labels: {cluster: test}\n", server.URL)), t.TempDir())
	require.NoError(t, e)
	return cfg, calls
}

func request(scope string) d.Request {
	from, to := time.Unix(1000, 0).UTC(), time.Unix(1060, 0).UTC()
	return d.Request{Selection: d.Selection{Sources: []string{"prometheus"}, Scope: scope, Flow: "write", Stage: "append", Window: d.Window{Mode: "historical", From: &from, To: &to}}, Connection: "test", Limits: d.DefaultLimits()}
}

func finding(t *testing.T, report d.Report, id string) d.Finding {
	t.Helper()
	require.Len(t, report.Assessments, 1)
	for _, f := range report.Assessments[0].Findings {
		if f.RuleID == "milvus.append.step."+id {
			return f
		}
	}
	t.Fatalf("missing %s: %+v", id, report)
	return d.Finding{}
}

func TestWriteStepsReuseSourceRulesAndUnits(t *testing.T) {
	outcomes := map[string]string{"milvus.append": "observed_slow_proxy_completions", "milvus.append.pk": "suspected_proxy_preparation_work", "milvus.append.timestamp": "suspected_proxy_preparation_work", "milvus.append.produce": "observed_slow_producer_completions", "milvus.append.quota": "suspected_quota_admission_wait", "milvus.wal_append": "observed_slow_wal_completions", "milvus.append.backend": "observed_slow_backend_completions", "milvus.append.before": "suspected_before_append_interceptor_work", "milvus.append.after": "suspected_after_append_interceptor_work", "wp.client_append": "observed_slow_wp_client_completions", "wp.append": "observed_slow_wp_server_completions", "wp.buffer_wait": "observed_slow_wp_buffer_notifications", "wp.flush": "observed_slow_wp_flush_completions"}
	for id, want := range outcomes {
		t.Run(id, func(t *testing.T) {
			cfg, _ := setup(t, scenario{slow: map[string]bool{id: true}})
			report := a.Run(context.Background(), cfg, request("e2e"))
			require.Equal(t, "completed", report.Status)
			f := finding(t, report, id)
			require.Equal(t, want, f.Outcome)
			require.Len(t, f.DerivedFrom, 1)
			found := false
			for _, original := range report.Results[0].Findings {
				if original.ID != f.DerivedFrom[0] {
					continue
				}
				found = true
				require.Equal(t, "suspected_high_latency", original.Outcome)
				require.Equal(t, original.Supporting, f.Supporting)
				require.Equal(t, original.Severity, f.Severity)
			}
			require.True(t, found)
			require.NotEmpty(t, f.NextChecks)
			require.Contains(t, f.Unknown, "overlapping_timers_do_not_add_or_subtract_quantiles")
			require.Contains(t, f.Unknown, "client_observed_latency_not_measured")
			if strings.HasPrefix(id, "wp.") {
				require.Contains(t, f.Unknown, "wp_metric_context_unassociated")
				require.Equal(t, "9007199254740993", f.Target.LogID)
			}
			b, _, e := d.EncodeReport(report)
			require.NoError(t, e)
			require.NotContains(t, string(b), "sentinel-secret")
		})
	}
}

func TestGaugesRecoveryAndNoFalseStalls(t *testing.T) {
	for _, tc := range []struct {
		id     string
		values []float64
		want   string
	}{
		{"milvus.append.unavailable", []float64{1, 1, 1, 1, 1, 1}, "persistent_unavailable_producers_observed"},
		{"milvus.append.unavailable", []float64{2, 2, 1, 1, 0, 0}, "producer_availability_recovery_observed"},
		{"milvus.append.unavailable", []float64{0, 1, 0, 1, 0, 1}, "intermittent_producer_unavailability_observed"},
		{"milvus.append.unavailable", nil, "no_unavailable_producers_observed"},
		{"milvus.append.inflight", []float64{2, 3, 4, 4, 4, 4}, "inflight_requests_observed"},
		{"wp.pending", []float64{1, 1, 1, 1, 1, 1}, "persistent_pending_appends_observed"},
		{"wp.pending", []float64{2, 2, 1, 1, 0, 0}, "pending_append_recovery_observed"},
		{"wp.pending", []float64{0, 1, 0, 1, 0, 1}, "intermittent_pending_appends_observed"},
		{"wp.pending", nil, "no_pending_appends_observed"},
		{"wp.disk_state", []float64{2, 2, 2, 2, 2, 2}, "exported_disk_admission_pressure_observed"},
		{"wp.disk_state", []float64{2, 1, 0, 0, 0, 0}, "disk_admission_recovery_observed"},
		{"wp.disk_state", nil, "no_disk_admission_pressure_observed"},
		{"wp.disk_state", []float64{0, 2, 1.5, 1, 0, 0}, "inconclusive"},
	} {
		t.Run(tc.id+"/"+tc.want, func(t *testing.T) {
			cfg, _ := setup(t, scenario{gauges: map[string][]float64{tc.id: tc.values}})
			report := a.Run(context.Background(), cfg, request("e2e"))
			require.Equal(t, tc.want, finding(t, report, tc.id).Outcome)
			require.NotContains(t, finding(t, report, tc.id).Outcome, "stalled")
		})
	}
}

func TestControlsScopeTimeAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		s                        scenario
		status, outcome, unknown string
	}{
		{"normal", scenario{}, "completed", "within_configured_latency_for_observed_completions", ""},
		{"idle", scenario{idle: true}, "completed", "idle_or_no_completions", ""},
		{"missing", scenario{missing: map[string]bool{"milvus.append": true}}, "completed", "inconclusive", "no_matching_series"},
		{"reset", scenario{reset: true}, "completed", "inconclusive", "counter_reset_or_restart"},
		{"denied", scenario{deny: true}, "failed", "", ""},
		{"wrong time", scenario{wrongTime: true}, "failed", "", ""},
		{"wrong instance", scenario{wrongInstance: true}, "failed", "", ""},
		{"duplicate", scenario{duplicate: true}, "completed", "within_configured_latency_for_observed_completions", ""},
		{"conflicting duplicate", scenario{conflict: true}, "failed", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, tc.s)
			r := request("milvus")
			if tc.s.wrongInstance {
				r.Selection.Target.Instance = "proxy:9091"
			}
			report := a.Run(context.Background(), cfg, r)
			require.Equal(t, tc.status, report.Status)
			if tc.outcome != "" {
				f := finding(t, report, "milvus.append")
				require.Equal(t, tc.outcome, f.Outcome)
				if tc.unknown != "" {
					require.Contains(t, f.Unknown, tc.unknown)
				}
			}
			for _, f := range report.Assessments[0].Findings {
				require.NotContains(t, f.RuleID, ".wp.")
			}
		})
	}
	cfg, _ := setup(t, scenario{})
	report := a.Run(context.Background(), cfg, request("e2e"))
	require.Contains(t, report.Assessments[0].Gaps, d.Gap{Code: "wp_metric_context_unassociated", Detail: finding(t, report, "wp.append").ID})
}

func TestSelectionsRejectBeforeAnyCollection(t *testing.T) {
	cfg, calls := setup(t, scenario{})
	for _, edit := range []func(*d.Request){func(r *d.Request) { r.Selection.Scope = "wp" }, func(r *d.Request) { r.Selection.Flow = "read" }, func(r *d.Request) { r.Selection.Stage = "query" }, func(r *d.Request) { r.Selection.Target.Collection = "7" }, func(r *d.Request) { r.Selection.Target.Channel = "channel-a" }, func(r *d.Request) { r.Selection.Sources = []string{"wpcli"} }, func(r *d.Request) { r.Selection.Sources = []string{"trace", "prometheus"} }, func(r *d.Request) { r.Selection.Window = d.Window{Mode: "snapshot"} }} {
		r := request("milvus")
		edit(&r)
		report := a.Run(context.Background(), cfg, r)
		require.Contains(t, []string{"invalid", "unsupported"}, report.Status)
		require.Empty(t, report.Assessments)
	}
	require.Zero(t, calls.Load())
}

func TestAssessIsPureRepeatableAndRejectsBadLineage(t *testing.T) {
	cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.append.quota": true}})
	engine, e := d.NewEngine(prometheus.AppendModule{})
	require.NoError(t, e)
	source := engine.Run(context.Background(), cfg, request("milvus"))
	before, e := json.Marshal(source)
	require.NoError(t, e)
	first := a.Assess(context.Background(), source)
	require.Equal(t, first, a.Assess(context.Background(), source))
	after, e := json.Marshal(source)
	require.NoError(t, e)
	require.Equal(t, before, after)
	for _, tc := range []struct {
		code string
		edit func(*d.Report)
	}{
		{"unsupported_stage_input", func(r *d.Report) { r.SchemaVersion = "wpdiag.report/v2" }},
		{"unsupported_source_profile", func(r *d.Report) { r.Results[0].Profile = "e2e.write.metrics" }},
		{"unsupported_source_schema_or_window", func(r *d.Report) { r.Results[0].Metadata = json.RawMessage(`{}`) }},
		{"source_selection_window_or_deployment_mismatch", func(r *d.Report) { r.Selection.Target.Deployment = "other" }},
		{"missing_observation_identity", func(r *d.Report) { r.Results[0].Observations[0].ID = "" }},
		{"invalid_observation_contract", func(r *d.Report) { r.Results[0].Observations[0].Kind = "unknown" }},
		{"conflicting_observation_identity", func(r *d.Report) {
			o := r.Results[0].Observations[0]
			o.Code = "different"
			r.Results[0].Observations = append(r.Results[0].Observations, o)
		}},
		{"conflicting_source_finding", func(r *d.Report) {
			f := r.Results[0].Findings[0]
			f.Outcome = "different"
			r.Results[0].Findings = append(r.Results[0].Findings, f)
		}},
		{"unsupported_source_finding_contract", func(r *d.Report) { r.Results[0].Findings[0].RuleVersion = 2 }},
		{"dangling_or_wrong_source_reference", func(r *d.Report) { r.Results[0].Findings[0].Supporting = []string{"missing"} }},
		{"unsupported_source_rule_or_outcome", func(r *d.Report) { r.Results[0].Findings[0].Outcome = "confirmed_cause" }},
		{"invalid_fact_identity_or_time", func(r *d.Report) { r.Results[0].Findings[0].Target.Instance = "other" }},
		{"invalid_fact_identity_or_time", func(r *d.Report) { r.Selection.Target.Instance = "other" }},
	} {
		t.Run(tc.code, func(t *testing.T) {
			var modified d.Report
			require.NoError(t, json.Unmarshal(before, &modified))
			tc.edit(&modified)
			assessment := a.Assess(context.Background(), modified)
			require.Equal(t, "failed", assessment.Status)
			require.Equal(t, tc.code, assessment.Gaps[0].Code)
			require.Empty(t, assessment.Findings)
		})
	}
	source.Results[0].Observations = append(source.Results[0].Observations, source.Results[0].Observations[0])
	source.Results[0].Findings = append(source.Results[0].Findings, source.Results[0].Findings[0])
	require.Len(t, a.Assess(context.Background(), source).Findings, len(first.Findings))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, "cancelled", a.Assess(ctx, source).Status)
	source.Results[0].Observations = nil
	source.Results[0].Findings = nil
	require.NotEmpty(t, a.Assess(context.Background(), source).Gaps)
}
