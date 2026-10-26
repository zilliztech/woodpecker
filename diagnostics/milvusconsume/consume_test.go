package milvusconsume_test

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
	a "github.com/zilliztech/woodpecker/diagnostics/milvusconsume"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

type scenario struct {
	labels   map[string]map[string]string
	offsets  map[string]int
	counters map[string][]float64

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
		var spec prometheus.ConsumeSignal
		for _, entry := range prometheus.ConsumeCatalog() {
			if strings.Contains(expr, entry.Metric) {
				spec = entry
				break
			}
		}
		require.NotEmpty(t, spec.ID)
		rows := []map[string]any{}
		if !s.missing[spec.ID] {
			labels := map[string]string{"cluster": "test", "component": spec.Component, "instance": spec.Component + ":9091", "node_id": spec.Component, "job": spec.Component, "secret": "sentinel-secret"}
			if spec.Component == "consumer" {
				labels["component"] = "streamingnode"
				labels["instance"] = "streamingnode:9091"
				labels["node_id"] = "streamingnode"
				labels["job"] = "streamingnode"
			}
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
			if spec.ID == "milvus.consume.mode" || spec.ID == "milvus.consume.scanned" || spec.ID == "milvus.consume.passed" {
				labels["scanner_model"] = "catchup"
			}
			if spec.ID == "milvus.consume.scanned" || spec.ID == "milvus.consume.passed" {
				labels["message_type"] = "Insert"
			}
			if strings.HasPrefix(spec.ID, "milvus.consume.") && !strings.Contains(spec.Metric, "querynode_") {
				labels["channel_name"] = "channel-a"
			}
			if spec.ID == "milvus.consume.subscription" {
				labels["access_model"] = "local"
			}
			if spec.ID == "milvus.consume.waiting" || spec.ID == "milvus.consume.processing" {
				labels["msg_type"] = "Insert"
			}
			if spec.ID == "milvus.consume.lag" || spec.ID == "milvus.consume.packs" {
				labels["collection_id"] = "7"
			}
			if spec.ID == "wp.quorum_bytes" {
				labels["az_scope"] = "local"
			}
			for k, v := range s.labels[spec.ID] {
				labels[k] = v
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
					ts := 1005 + i*10 + s.offsets[spec.ID]
					if s.wrongTime {
						ts -= 100
					}
					points = append(points, []any{ts, strconv.FormatFloat(v, 'f', -1, 64)})
				}
				return map[string]any{"metric": m, "values": points}
			}
			if strings.HasPrefix(spec.Kind, "counter_") {
				values := []float64{0, 20, 40, 60, 80, 100}
				if s.idle {
					values = []float64{0, 0, 0, 0, 0, 0}
				}
				if s.reset {
					values = []float64{0, 20, 40, 0, 20, 40}
				}
				if custom := s.counters[spec.ID]; custom != nil {
					values = custom
				}
				rows = append(rows, row(spec.Metric, "", values))
			} else if spec.Kind != "histogram" {
				values := []float64{0, 0, 0, 0, 0, 0}
				if spec.ID == "milvus.consume.mode" || spec.ID == "milvus.consume.subscription" {
					values = []float64{1, 1, 1, 1, 1, 1}
				}
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
	return d.Request{Selection: d.Selection{Sources: []string{"prometheus"}, Scope: scope, Flow: "read", Stage: "consume", Window: d.Window{Mode: "historical", From: &from, To: &to}}, Connection: "test", Limits: d.DefaultLimits()}
}

func finding(t *testing.T, report d.Report, id string) d.Finding {
	t.Helper()
	require.Len(t, report.Assessments, 1)
	for _, f := range report.Assessments[0].Findings {
		if f.RuleID == "milvus.consume.step."+id {
			return f
		}
	}
	t.Fatalf("missing %s: %+v", id, report)
	return d.Finding{}
}

func TestQualifiedTimersAndBlockingReadExclusion(t *testing.T) {
	for id, want := range map[string]string{"milvus.consume.processing": "observed_slow_consumer_processing", "wp.read_batch": "observed_slow_wp_batch_reads", "wp.direct_read": "observed_slow_wp_direct_reads", "wp.read_next": "blocking_read_next_completion_time_observed"} {
		t.Run(id, func(t *testing.T) {
			cfg, _ := setup(t, scenario{slow: map[string]bool{id: true}, missing: map[string]bool{"milvus.consume.waiting": true}})
			report := a.Run(context.Background(), cfg, request("e2e"))
			require.Equal(t, "completed", report.Status)
			f := finding(t, report, id)
			require.Equal(t, want, f.Outcome)
			require.Len(t, f.DerivedFrom, 1)
			for _, original := range report.Results[0].Findings {
				if original.ID == f.DerivedFrom[0] {
					require.Equal(t, "suspected_high_latency", original.Outcome)
					require.Equal(t, original.Supporting, f.Supporting)
				}
			}
			if id == "wp.read_next" {
				require.Equal(t, "unknown", f.Severity)
				require.Contains(t, f.Unknown, "read_next_can_wait_normally_for_new_data_not_io_latency")
			}
			if strings.HasPrefix(id, "wp.") {
				require.Contains(t, f.Unknown, "wp_reader_mapping_unavailable")
				require.Equal(t, "9007199254740993", f.Target.LogID)
			}
			b, _, e := d.EncodeReport(report)
			require.NoError(t, e)
			require.NotContains(t, string(b), "sentinel-secret")
		})
	}
}

func TestScannerBackpressureRecoveryAndNoFabricatedJoins(t *testing.T) {
	for _, tc := range []struct {
		name           string
		pause, pending []float64
		labels         map[string]string
		offset         int
		want, gap      string
	}{
		{"pressure", []float64{1, 1, 1, 1, 1, 1}, []float64{10, 10, 20, 20, 30, 30}, nil, 0, "suspected_scanner_downstream_backpressure", ""},
		{"idle", []float64{0, 0, 0, 0, 0, 0}, []float64{0, 0, 0, 0, 0, 0}, nil, 0, "scanner_pause_state_observed", ""},
		{"recovered", []float64{1, 1, 1, 1, 0, 0}, []float64{20, 20, 10, 10, 0, 0}, nil, 0, "scanner_pause_and_pending_recovery_observed", ""},
		{"wrong channel", []float64{1, 1, 1, 1, 1, 1}, []float64{10, 10, 10, 10, 10, 10}, map[string]string{"channel_name": "other"}, 0, "scanner_pause_state_observed", "related_population_missing"},
		{"hidden dimension", []float64{1, 1, 1, 1, 1, 1}, []float64{10, 10, 10, 10, 10, 10}, map[string]string{"private_dimension": "other"}, 0, "scanner_pause_state_observed", "related_population_missing"},
		{"wrong time", []float64{1, 1, 1, 1, 1, 1}, []float64{10, 10, 10, 10, 10, 10}, nil, 1, "scanner_pause_state_observed", "actual_samples_not_aligned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, scenario{gauges: map[string][]float64{"milvus.consume.paused": tc.pause, "milvus.consume.pending": tc.pending}, labels: map[string]map[string]string{"milvus.consume.pending": tc.labels}, offsets: map[string]int{"milvus.consume.pending": tc.offset}})
			f := finding(t, a.Run(context.Background(), cfg, request("milvus")), "milvus.consume.paused")
			require.Equal(t, tc.want, f.Outcome)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
		})
	}
}

func TestConsumerProcessingPressureAndContradictoryRecovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		waiting   []float64
		labels    map[string]string
		want, gap string
	}{
		{"pressure", []float64{1, 2, 3, 3, 4, 4}, nil, "suspected_consumer_processing_pressure", ""},
		{"recovery", []float64{2, 2, 1, 1, 0, 0}, nil, "slow_processing_with_backlog_recovery", ""},
		{"idle", []float64{0, 0, 0, 0, 0, 0}, nil, "slow_processing_without_persistent_waiting_work", ""},
		{"intermittent", []float64{0, 1, 0, 1, 1, 1}, nil, "observed_slow_consumer_processing", "intermittent_waiting_not_continuous_pressure"},
		{"other exporter", []float64{1, 1, 1, 1, 1, 1}, map[string]string{"instance": "other:9091"}, "observed_slow_consumer_processing", "related_population_missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.consume.processing": true}, gauges: map[string][]float64{"milvus.consume.waiting": tc.waiting}, labels: map[string]map[string]string{"milvus.consume.waiting": tc.labels}})
			f := finding(t, a.Run(context.Background(), cfg, request("milvus")), "milvus.consume.processing")
			require.Equal(t, tc.want, f.Outcome)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if tc.name == "recovery" || tc.name == "idle" {
				require.NotEmpty(t, f.Contradicting)
				require.Equal(t, "inconclusive", f.Strength)
			}
		})
	}
}

func TestLagFreshnessRequiresExactCollectionAndActualSampleTimes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		counts    []float64
		labels    map[string]string
		offset    int
		want, gap string
	}{
		{"fresh packs", []float64{0, 20, 40, 60, 80, 100}, nil, 0, "lag_increased_during_observed_msgpack_consumption", ""},
		{"idle", []float64{0, 0, 0, 0, 0, 0}, nil, 0, "consumer_time_tick_lag_increased", "no_new_packs_no_stall_verdict"},
		{"intermittent", []float64{0, 0, 20, 20, 40, 40}, nil, 0, "consumer_time_tick_lag_increased", "fresh_pack_activity_not_continuous"},
		{"reset", []float64{0, 20, 40, 0, 20, 40}, nil, 0, "consumer_time_tick_lag_increased", "related_population_missing"},
		{"different collection", []float64{0, 20, 40, 60, 80, 100}, map[string]string{"collection_id": "8"}, 0, "consumer_time_tick_lag_increased", "related_population_missing"},
		{"hidden dimension", []float64{0, 20, 40, 60, 80, 100}, map[string]string{"private": "other"}, 0, "consumer_time_tick_lag_increased", "related_population_missing"},
		{"time mismatch", []float64{0, 20, 40, 60, 80, 100}, nil, 1, "consumer_time_tick_lag_increased", "actual_samples_not_aligned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, scenario{gauges: map[string][]float64{"milvus.consume.lag": {100, 200, 300, 400, 500, 600}}, counters: map[string][]float64{"milvus.consume.packs": tc.counts}, labels: map[string]map[string]string{"milvus.consume.packs": tc.labels}, offsets: map[string]int{"milvus.consume.packs": tc.offset}})
			report := a.Run(context.Background(), cfg, request("milvus"))
			f := finding(t, report, "milvus.consume.lag")
			require.Equal(t, tc.want, f.Outcome)
			require.Equal(t, "unknown", f.Severity)
			require.Contains(t, f.Unknown, "time_tick_lag_not_per_row_visibility_or_payload_demand")
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if tc.name == "idle" {
				require.NotEmpty(t, f.Contradicting)
			}
		})
	}
}

func TestModesLocalSubscriptionsEOFAndCounterUnits(t *testing.T) {
	cfg, _ := setup(t, scenario{})
	report := a.Run(context.Background(), cfg, request("e2e"))
	require.Equal(t, "completed", report.Status)
	require.Contains(t, finding(t, report, "milvus.consume.mode").Unknown, "tailing_can_use_memory_without_wp_rpc")
	require.Contains(t, finding(t, report, "milvus.consume.subscription").Unknown, "local_subscription_bypasses_remote_grpc_handler")
	require.Equal(t, "server_eof_observed", finding(t, report, "wp.eof").Outcome)
	require.Contains(t, finding(t, report, "milvus.consume.scanned").Unknown, "scan_pass_counts_not_a_loss_ratio")
	for _, o := range report.Results[0].Observations {
		var data prometheus.Dataset
		require.NoError(t, json.Unmarshal(o.Data, &data))
		if data.Signal == "milvus.consume.delivered" {
			require.Equal(t, 100.0, data.Facts[0].Counter.Increase)
			require.Equal(t, 2.0, data.Facts[0].Counter.PerSecond)
			require.Equal(t, "counter_bytes", data.Unit)
		}
	}
	cfg, _ = setup(t, scenario{labels: map[string]map[string]string{"milvus.consume.mode": {"scanner_model": "future-mode"}, "milvus.consume.subscription": {"access_model": "future-mode"}}, gauges: map[string][]float64{"milvus.consume.paused": {0, 0, 2, 0, 0, 0}}})
	report = a.Run(context.Background(), cfg, request("milvus"))
	require.Equal(t, "inconclusive", finding(t, report, "milvus.consume.mode").Outcome)
	require.Equal(t, "inconclusive", finding(t, report, "milvus.consume.subscription").Outcome)
	require.Contains(t, finding(t, report, "milvus.consume.paused").Unknown, "unknown_state_indicator")
	cfg, _ = setup(t, scenario{missing: map[string]bool{"milvus.consume.remote": true}, labels: map[string]map[string]string{"milvus.consume.mode": {"scanner_model": "tailing"}}})
	report = a.Run(context.Background(), cfg, request("milvus"))
	require.Equal(t, "inconclusive", finding(t, report, "milvus.consume.remote").Outcome)
	require.Equal(t, "scanner_mode_population_observed", finding(t, report, "milvus.consume.mode").Outcome)
}

func TestControlsSourceFailuresAndUnsupportedSelections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		s      scenario
		status string
	}{
		{"normal", scenario{}, "completed"}, {"idle", scenario{idle: true}, "completed"}, {"reset", scenario{reset: true}, "completed"}, {"denied", scenario{deny: true}, "failed"}, {"wrong time", scenario{wrongTime: true}, "failed"}, {"wrong instance", scenario{wrongInstance: true}, "failed"}, {"duplicate", scenario{duplicate: true}, "completed"}, {"conflict", scenario{conflict: true}, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, tc.s)
			r := request("milvus")
			if tc.s.wrongInstance {
				r.Selection.Target.Instance = "streamingnode:9091"
			}
			report := a.Run(context.Background(), cfg, r)
			require.Equal(t, tc.status, report.Status)
			if tc.name == "idle" {
				require.Equal(t, "idle_or_no_completions", finding(t, report, "milvus.consume.processing").Outcome)
				require.Equal(t, "no_scan_events_observed", finding(t, report, "milvus.consume.scanned").Outcome)
			}
			if tc.name == "reset" {
				require.Contains(t, finding(t, report, "milvus.consume.scanned").Unknown, "counter_reset_or_restart")
			}
			for _, f := range report.Assessments[0].Findings {
				require.NotContains(t, f.RuleID, ".wp.")
			}
		})
	}
	cfg, calls := setup(t, scenario{})
	for _, edit := range []func(*d.Request){func(r *d.Request) { r.Selection.Scope = "wp" }, func(r *d.Request) { r.Selection.Flow = "write" }, func(r *d.Request) { r.Selection.Stage = "append" }, func(r *d.Request) { r.Selection.Target.Collection = "7" }, func(r *d.Request) { r.Selection.Target.Channel = "channel-a" }, func(r *d.Request) { r.Selection.Sources = []string{"wpcli"} }, func(r *d.Request) { r.Selection.Sources = []string{"trace", "prometheus"} }, func(r *d.Request) { r.Selection.Window = d.Window{Mode: "snapshot"} }} {
		r := request("milvus")
		edit(&r)
		report := a.Run(context.Background(), cfg, r)
		require.Contains(t, []string{"invalid", "unsupported"}, report.Status)
		require.Empty(t, report.Assessments)
	}
	require.Zero(t, calls.Load())
}

func TestPureAssessmentVersionedEvidenceAndIdentityValidation(t *testing.T) {
	cfg, _ := setup(t, scenario{})
	engine, e := d.NewEngine(prometheus.ConsumeModule{})
	require.NoError(t, e)
	source := engine.Run(context.Background(), cfg, request("e2e"))
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
		{"unsupported_source_profile", func(r *d.Report) { r.Results[0].Profile = "e2e.read.metrics" }},
		{"unsupported_source_schema_or_window", func(r *d.Report) { r.Results[0].Metadata = json.RawMessage(`{}`) }},
		{"source_selection_window_or_deployment_mismatch", func(r *d.Report) { r.Selection.Target.Deployment = "other" }},
		{"missing_observation_identity", func(r *d.Report) { r.Results[0].Observations[0].ID = "" }},
		{"invalid_observation_contract", func(r *d.Report) { r.Results[0].Observations[0].Kind = "unknown" }},
		{"observation_run_or_deployment_mismatch", func(r *d.Report) { r.Results[0].Observations[0].Target.Deployment = "other" }},
		{"observation_source_or_query_mismatch", func(r *d.Report) { r.Results[0].Observations[0].Query.Executed = false }},
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
		{"invalid_fact_sample_contract", func(r *d.Report) {
			var data prometheus.Dataset
			require.NoError(t, json.Unmarshal(r.Results[0].Observations[0].Data, &data))
			data.Facts[0].Gauge.First = 12345
			r.Results[0].Observations[0].Data, _ = json.Marshal(data)
		}},
		{"invalid_fact_sample_contract", func(r *d.Report) {
			var data prometheus.Dataset
			require.NoError(t, json.Unmarshal(r.Results[0].Observations[1].Data, &data))
			data.Facts[0].Counter.Increase = 12345
			r.Results[0].Observations[1].Data, _ = json.Marshal(data)
		}},
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

// This is a sanitized projection with complete lineage for one finding, not a
// substitute for a full wpdiag.report/v1 execution report.
var update = flag.Bool("update-consume-fixture", false, "Update the consume-stage lineage example")

func TestVersionedExample(t *testing.T) {
	cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.consume.processing": true}, gauges: map[string][]float64{"milvus.consume.waiting": {3, 3, 2, 1, 0, 0}}})
	report := a.Run(context.Background(), cfg, request("e2e"))
	f := finding(t, report, "milvus.consume.processing")
	var meta struct {
		RunID string `json:"source_run_id"`
	}
	require.NoError(t, json.Unmarshal(report.Results[0].Metadata, &meta))
	source := []d.Finding{}
	observations := []d.Observation{}
	for _, sf := range report.Results[0].Findings {
		if d.Has(f.DerivedFrom, sf.ID) {
			source = append(source, sf)
		}
	}
	for _, o := range report.Results[0].Observations {
		if d.Has(f.Supporting, o.ID) || d.Has(f.Contradicting, o.ID) {
			o.StartedAt = time.Unix(1061, 0).UTC()
			o.FinishedAt = o.StartedAt.Add(time.Millisecond)
			observations = append(observations, o)
		}
	}
	example := map[string]any{"schema_version": "wpdiag.consume.example/v1", "profile": "milvus.consume", "version": 1, "finding": f, "source_findings": source, "observations": observations}
	body, err := json.MarshalIndent(example, "", "  ")
	require.NoError(t, err)
	body = []byte(strings.ReplaceAll(string(body), meta.RunID, "fixture-consume-source") + "\n")
	path := filepath.Join("testdata", "recovery-v1.json")
	if *update {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, body, 0o644))
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(body))
	require.NotContains(t, string(body), "sentinel-secret")
	require.NotContains(t, string(body), "127.0.0.1")
	for i := 1; i < len(report.Assessments[0].Findings); i++ {
		require.LessOrEqual(t, report.Assessments[0].Findings[i-1].Summary[:1], report.Assessments[0].Findings[i].Summary[:1])
	}
}
