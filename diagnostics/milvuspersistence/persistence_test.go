package milvuspersistence_test

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
	a "github.com/zilliztech/woodpecker/diagnostics/milvuspersistence"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

type scenario struct {
	labels  map[string]map[string]string
	offsets map[string]int

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
		var spec prometheus.PersistenceSignal
		for _, entry := range prometheus.PersistenceCatalog() {
			if strings.Contains(expr, entry.Metric) {
				spec = entry
				break
			}
		}
		require.NotEmpty(t, spec.ID)
		rows := []map[string]any{}
		if !s.missing[spec.ID] {
			labels := map[string]string{"cluster": "test", "component": spec.Component, "instance": spec.Component + ":9091", "node_id": spec.Component, "job": spec.Component, "secret": "sentinel-secret"}
			if spec.Component == "persistor" {
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
			if spec.Kind == "unix_seconds" || spec.ID == "milvus.persistence.busy" || spec.ID == "milvus.persistence.flusher" {
				labels["channel_name"] = "channel-a"
				if spec.ID != "milvus.persistence.checkpoint" {
					labels["term"] = "1"
				}
			}
			if spec.ID == "milvus.persistence.flusher" {
				labels["state"] = "working"
			}
			for k, v := range s.labels[spec.ID] {
				labels[k] = v
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
					ts := 1005 + i*10 + s.offsets[spec.ID]
					if s.wrongTime {
						ts -= 100
					}
					points = append(points, []any{ts, strconv.FormatFloat(v, 'f', -1, 64)})
				}
				return map[string]any{"metric": m, "values": points}
			}
			if spec.Kind != "histogram" {
				values := []float64{0, 0, 0, 0, 0, 0}
				if spec.Kind == "unix_seconds" {
					values = []float64{990, 992, 994, 996, 998, 1000}
				}
				if spec.ID == "milvus.persistence.hwm" {
					values = []float64{100, 100, 100, 100, 100, 100}
				}
				if spec.ID == "milvus.persistence.flusher" {
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
	return d.Request{Selection: d.Selection{Sources: []string{"prometheus"}, Scope: scope, Flow: "write", Stage: "persistence", Window: d.Window{Mode: "historical", From: &from, To: &to}}, Connection: "test", Limits: d.DefaultLimits()}
}

func finding(t *testing.T, report d.Report, id string) d.Finding {
	t.Helper()
	require.Len(t, report.Assessments, 1)
	for _, f := range report.Assessments[0].Findings {
		if f.RuleID == "milvus.persistence.step."+id {
			return f
		}
	}
	t.Fatalf("missing %s: %+v", id, report)
	return d.Finding{}
}

func TestLatencyStepsReuseSourceFindingsAndUnits(t *testing.T) {
	for id, want := range map[string]string{
		"milvus.persistence.queue":   "observed_slow_sync_queue_completions",
		"milvus.persistence.execute": "observed_slow_sync_execution",
		"milvus.persistence.save":    "observed_slow_save_completions",
		"milvus.persistence.put":     "observed_slow_storage_put_completions",
		"wp.flush":                   "observed_slow_wp_local_flush", "wp.compaction": "observed_slow_wp_compaction", "wp.storage": "observed_slow_wp_storage_completions",
	} {
		t.Run(id, func(t *testing.T) {
			s := scenario{slow: map[string]bool{id: true}}
			if id == "milvus.persistence.queue" {
				s.missing = map[string]bool{"milvus.persistence.pending": true}
			}
			cfg, _ := setup(t, s)
			report := a.Run(context.Background(), cfg, request("e2e"))
			require.Equal(t, "completed", report.Status)
			f := finding(t, report, id)
			require.Equal(t, want, f.Outcome)
			require.Len(t, f.DerivedFrom, 1)
			for _, original := range report.Results[0].Findings {
				if original.ID == f.DerivedFrom[0] {
					require.Equal(t, "suspected_high_latency", original.Outcome)
					require.Equal(t, original.Supporting, f.Supporting)
					require.Equal(t, original.Severity, f.Severity)
				}
			}
			require.Contains(t, f.Unknown, "overlapping_timers_do_not_add_or_subtract_quantiles")
			require.NotEmpty(t, f.NextChecks)
			b, _, e := d.EncodeReport(report)
			require.NoError(t, e)
			require.NotContains(t, string(b), "sentinel-secret")
			if strings.HasPrefix(id, "wp.") {
				require.Contains(t, f.Unknown, "wp_maintenance_mapping_unavailable")
				require.Equal(t, "9007199254740993", f.Target.LogID)
			}
		})
	}
}

func TestQueuePressureRecoveryAndAssociations(t *testing.T) {
	for _, tc := range []struct {
		name      string
		values    []float64
		labels    map[string]string
		offset    int
		want, gap string
	}{
		{"persistent", []float64{2, 3, 4, 4, 5, 5}, nil, 0, "suspected_sync_scheduling_pressure", ""},
		{"recovered", []float64{3, 2, 1, 1, 0, 0}, nil, 0, "slow_queue_completions_with_pending_recovery", ""},
		{"idle", []float64{0, 0, 0, 0, 0, 0}, nil, 0, "slow_queue_completions_without_persistent_pending_work", ""},
		{"intermittent", []float64{0, 1, 0, 1, 1, 1}, nil, 0, "observed_slow_sync_queue_completions", "intermittent_pending_work_not_continuous_pressure"},
		{"other exporter", []float64{1, 1, 1, 1, 1, 1}, map[string]string{"instance": "other:9091"}, 0, "observed_slow_sync_queue_completions", "related_population_missing"},
		{"unknown dimension", []float64{1, 1, 1, 1, 1, 1}, map[string]string{"private_dimension": "other"}, 0, "observed_slow_sync_queue_completions", "related_population_missing"},
		{"different actual time", []float64{1, 1, 1, 1, 1, 1}, nil, 1, "observed_slow_sync_queue_completions", "actual_samples_not_aligned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.persistence.queue": true}, gauges: map[string][]float64{"milvus.persistence.pending": tc.values}, labels: map[string]map[string]string{"milvus.persistence.pending": tc.labels}, offsets: map[string]int{"milvus.persistence.pending": tc.offset}})
			report := a.Run(context.Background(), cfg, request("milvus"))
			f := finding(t, report, "milvus.persistence.queue")
			require.Equal(t, tc.want, f.Outcome)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if tc.name == "recovered" || tc.name == "idle" {
				require.NotEmpty(t, f.Contradicting)
				require.Equal(t, "inconclusive", f.Strength)
			}
		})
	}
}

func TestConfiguredGrowingPressureAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		values, hwm []float64
		want, gap   string
	}{
		{"pressure", []float64{120, 120, 130, 140, 150, 150}, nil, "suspected_growing_flush_pressure", ""},
		{"below", []float64{10, 20, 30, 40, 50, 50}, nil, "below_configured_growing_hwm_observed", ""},
		{"recovery", []float64{120, 120, 110, 80, 20, 10}, nil, "growing_pressure_below_hwm_after_breach", "below_hwm_is_not_full_hysteresis_recovery"},
		{"intermittent", []float64{120, 80, 120, 80, 120, 80}, nil, "intermittent_growing_hwm_pressure_observed", ""},
		{"disabled", []float64{120, 120, 130, 140, 150, 150}, []float64{0, 0, 0, 0, 0, 0}, "growing_flush_pressure_bytes_observed", "hwm_uninitialized_or_disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gauges := map[string][]float64{"milvus.persistence.pressure": tc.values}
			if tc.hwm != nil {
				gauges["milvus.persistence.hwm"] = tc.hwm
			}
			cfg, _ := setup(t, scenario{gauges: gauges})
			f := finding(t, a.Run(context.Background(), cfg, request("milvus")), "milvus.persistence.pressure")
			require.Equal(t, tc.want, f.Outcome)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
		})
	}
}

func TestRecoverySnapshotRequiresAlignedChannelTermAndPersistingEvidence(t *testing.T) {
	constant := []float64{980, 980, 980, 980, 980, 980}
	moving := []float64{990, 992, 994, 996, 998, 1000}
	busy := []float64{1, 1, 1, 1, 1, 1}
	for _, tc := range []struct {
		name      string
		edit      func(*scenario)
		want, gap string
	}{
		{"suspected", nil, "suspected_recovery_snapshot_persistence_delay", "not_a_stall_or_storage_failure_verdict"},
		{"idle", func(s *scenario) { s.gauges["milvus.persistence.memory"] = constant }, "recovery_frontiers_equal_at_final_scrapes", ""},
		{"no snapshot work", func(s *scenario) { s.gauges["milvus.persistence.busy"] = []float64{0, 0, 0, 0, 0, 0} }, "persisted_recovery_frontier_unchanged", "persisting_not_continuous_or_recovered"},
		{"persisting recovered", func(s *scenario) { s.gauges["milvus.persistence.busy"] = []float64{1, 1, 1, 1, 0, 0} }, "persisted_recovery_frontier_unchanged", "persisting_not_continuous_or_recovered"},
		{"persisted advanced", func(s *scenario) { s.gauges["milvus.persistence.persisted"] = []float64{970, 971, 972, 973, 974, 975} }, "persisted_recovery_frontier_advanced", ""},
		{"gap closed", func(s *scenario) { s.gauges["milvus.persistence.persisted"] = []float64{980, 982, 984, 986, 998, 1000} }, "recovery_frontiers_equal_at_final_scrapes", "physical_seconds_equality_not_complete_log_durability"},
		{"reset", func(s *scenario) { s.gauges["milvus.persistence.persisted"] = []float64{980, 980, 970, 970, 970, 970} }, "inconclusive", "frontier_reset_or_restart"},
		{"zero", func(s *scenario) { s.gauges["milvus.persistence.persisted"] = []float64{0, 0, 0, 0, 0, 0} }, "inconclusive", "timestamp_uninitialized_or_clock_incompatible"},
		{"future", func(s *scenario) {
			s.gauges["milvus.persistence.persisted"] = []float64{2000, 2000, 2000, 2000, 2000, 2000}
		}, "inconclusive", "timestamp_uninitialized_or_clock_incompatible"},
		{"term changed", func(s *scenario) { s.labels = map[string]map[string]string{"milvus.persistence.memory": {"term": "2"}} }, "frontier_unchanged_observed", "related_population_missing"},
		{"channel changed", func(s *scenario) {
			s.labels = map[string]map[string]string{"milvus.persistence.memory": {"channel_name": "unrelated"}}
		}, "frontier_unchanged_observed", "related_population_missing"},
		{"term missing", func(s *scenario) {
			s.labels = map[string]map[string]string{"milvus.persistence.persisted": {"term": ""}}
		}, "frontier_unchanged_observed", "channel_term_identity_missing"},
		{"time mismatch", func(s *scenario) { s.offsets = map[string]int{"milvus.persistence.memory": 1} }, "frontier_unchanged_observed", "actual_samples_not_aligned"},
		{"persisted exceeds memory", func(s *scenario) { s.gauges["milvus.persistence.memory"] = []float64{970, 972, 974, 976, 978, 980} }, "inconclusive", "persisted_frontier_exceeds_in_memory"},
		{"busy missing", func(s *scenario) { s.missing = map[string]bool{"milvus.persistence.busy": true} }, "persisted_recovery_frontier_unchanged", "related_population_missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := scenario{gauges: map[string][]float64{"milvus.persistence.persisted": constant, "milvus.persistence.memory": moving, "milvus.persistence.busy": busy}}
			if tc.edit != nil {
				tc.edit(&s)
			}
			cfg, _ := setup(t, s)
			report := a.Run(context.Background(), cfg, request("milvus"))
			f := finding(t, report, "milvus.persistence.persisted")
			require.Equal(t, tc.want, f.Outcome)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if tc.name == "no snapshot work" || tc.name == "persisting recovered" || tc.name == "persisted exceeds memory" {
				require.NotEmpty(t, f.Contradicting)
			}
		})
	}
}

func TestCheckpointAgeIdleAndFlusherSeenAreNotDurability(t *testing.T) {
	cfg, _ := setup(t, scenario{gauges: map[string][]float64{"milvus.persistence.checkpoint": {900, 900, 900, 900, 900, 900}}})
	report := a.Run(context.Background(), cfg, request("milvus"))
	f := finding(t, report, "milvus.persistence.checkpoint")
	require.Equal(t, "frontier_unchanged_observed", f.Outcome)
	require.Contains(t, f.Summary, "155.000s")
	require.Contains(t, f.Unknown, "idle_checkpoint_age_not_stall")
	require.Contains(t, finding(t, report, "milvus.persistence.seen").Unknown, "flusher_seen_timetick_is_not_durable_checkpoint")
}

func TestNormalIdleMissingResetAndSourceFailures(t *testing.T) {
	for _, tc := range []struct {
		name            string
		s               scenario
		status, outcome string
	}{
		{"normal", scenario{}, "completed", "within_configured_latency_for_observed_completions"},
		{"idle", scenario{idle: true}, "completed", "idle_or_no_completions"},
		{"missing", scenario{missing: map[string]bool{"milvus.persistence.execute": true}}, "completed", "inconclusive"},
		{"reset", scenario{reset: true}, "completed", "inconclusive"},
		{"denied", scenario{deny: true}, "failed", ""},
		{"wrong time", scenario{wrongTime: true}, "failed", ""},
		{"wrong instance", scenario{wrongInstance: true}, "failed", ""},
		{"duplicate", scenario{duplicate: true}, "completed", "within_configured_latency_for_observed_completions"},
		{"conflict", scenario{conflict: true}, "failed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := setup(t, tc.s)
			r := request("milvus")
			if tc.s.wrongInstance {
				r.Selection.Target.Instance = "streamingnode:9091"
			}
			report := a.Run(context.Background(), cfg, r)
			require.Equal(t, tc.status, report.Status)
			if tc.outcome != "" {
				require.Equal(t, tc.outcome, finding(t, report, "milvus.persistence.execute").Outcome)
			}
			for _, f := range report.Assessments[0].Findings {
				require.NotContains(t, f.RuleID, ".wp.")
			}
		})
	}
	for _, id := range []string{"milvus.persistence.busy", "milvus.persistence.flusher", "wp.disk_state"} {
		cfg, _ := setup(t, scenario{gauges: map[string][]float64{id: {0, 0, 4, 0, 0, 0}}})
		f := finding(t, a.Run(context.Background(), cfg, request("e2e")), id)
		require.Equal(t, "inconclusive", f.Outcome)
		require.Contains(t, f.Unknown, "unknown_state_indicator")
	}
}

func TestSelectionRejectsWithoutCollection(t *testing.T) {
	cfg, calls := setup(t, scenario{})
	for _, edit := range []func(*d.Request){func(r *d.Request) { r.Selection.Scope = "wp" }, func(r *d.Request) { r.Selection.Flow = "read" }, func(r *d.Request) { r.Selection.Stage = "append" }, func(r *d.Request) { r.Selection.Target.Collection = "7" }, func(r *d.Request) { r.Selection.Target.Channel = "channel-a" }, func(r *d.Request) { r.Selection.Sources = []string{"wpcli"} }, func(r *d.Request) { r.Selection.Sources = []string{"trace", "prometheus"} }, func(r *d.Request) { r.Selection.Window = d.Window{Mode: "snapshot"} }} {
		r := request("milvus")
		edit(&r)
		report := a.Run(context.Background(), cfg, r)
		require.Contains(t, []string{"invalid", "unsupported"}, report.Status)
		require.Empty(t, report.Assessments)
	}
	require.Zero(t, calls.Load())
}

func TestPureAssessmentAndVersionedLineage(t *testing.T) {
	cfg, _ := setup(t, scenario{slow: map[string]bool{"milvus.persistence.execute": true}})
	engine, e := d.NewEngine(prometheus.PersistenceModule{})
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
		{"unsupported_source_profile", func(r *d.Report) { r.Results[0].Profile = "e2e.write.metrics" }},
		{"unsupported_source_schema_or_window", func(r *d.Report) { r.Results[0].Metadata = json.RawMessage(`{}`) }},
		{"source_selection_window_or_deployment_mismatch", func(r *d.Report) { r.Selection.Target.Deployment = "other" }},
		{"missing_observation_identity", func(r *d.Report) { r.Results[0].Observations[0].ID = "" }},
		{"invalid_observation_contract", func(r *d.Report) { r.Results[0].Observations[0].Kind = "unknown" }},
		{"observation_run_or_deployment_mismatch", func(r *d.Report) { r.Results[0].Observations[0].Target.Deployment = "unrelated" }},
		{"observation_run_or_deployment_mismatch", func(r *d.Report) { r.Results[0].Observations[0].ID = "unrelated:q0" }},
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
		{"invalid_gauge_sample_contract", func(r *d.Report) {
			var data prometheus.Dataset
			require.NoError(t, json.Unmarshal(r.Results[0].Observations[0].Data, &data))
			data.Facts[0].Gauge.First = 12345
			r.Results[0].Observations[0].Data, _ = json.Marshal(data)
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

var updateFixture = flag.Bool("update-persistence-fixture", false, "Update the sanitized v1 persistence example")

func TestVersionedExample(t *testing.T) {
	cfg, _ := setup(t, scenario{gauges: map[string][]float64{
		"milvus.persistence.persisted": {980, 980, 980, 980, 980, 980},
		"milvus.persistence.memory":    {990, 992, 994, 996, 998, 1000},
		"milvus.persistence.busy":      {1, 1, 1, 1, 1, 1},
	}})
	report := a.Run(context.Background(), cfg, request("milvus"))
	selected := finding(t, report, "milvus.persistence.persisted")
	var meta struct {
		RunID string `json:"source_run_id"`
	}
	require.NoError(t, json.Unmarshal(report.Results[0].Metadata, &meta))
	originals := []d.Finding{}
	observations := []d.Observation{}
	for _, f := range report.Results[0].Findings {
		if d.Has(selected.DerivedFrom, f.ID) {
			originals = append(originals, f)
		}
	}
	for _, o := range report.Results[0].Observations {
		if d.Has(selected.Supporting, o.ID) {
			o.StartedAt = time.Unix(2000, 0).UTC()
			o.FinishedAt = time.Unix(2001, 0).UTC()
			observations = append(observations, o)
		}
	}
	body, e := json.MarshalIndent(map[string]any{"schema_version": "wpdiag.persistence.example/v1", "description": "One selected finding with its original source findings and executed observations, projected from a complete fixture report", "selection": report.Selection, "assessment_profile": report.Assessments[0].Profile, "assessment_version": report.Assessments[0].Version, "finding": selected, "source_findings": originals, "observations": observations}, "", "  ")
	require.NoError(t, e)
	body = []byte(strings.ReplaceAll(string(body), meta.RunID, "fixture-persistence-source") + "\n")
	path := filepath.Join("testdata", "snapshot-delay-v1.json")
	if *updateFixture {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, body, 0o644))
	}
	expected, e := os.ReadFile(path)
	require.NoError(t, e)
	require.JSONEq(t, string(expected), string(body))
}
