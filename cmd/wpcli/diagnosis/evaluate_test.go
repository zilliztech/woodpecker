package diagnosis

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ptr(v int64) *int64 { return &v }

func fixtureReport(scenario string) Report {
	start := time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC)
	r := Report{
		SchemaVersion: SchemaVersion, RunID: "fixture-451", Source: "wp-runtime", Scope: "wp",
		Mode: "live_window", WindowMS: 2000, IntervalMS: 1000, StartedAt: start, FinishedAt: start.Add(3 * time.Second),
		Status: "completed", Limits: Limits{MaxNodes: 1, MaxSamples: 3, MaxOps: 10, MaxSeries: 100, ResponseBytes: 4096, BudgetMS: 5000},
		Targets:      []Target{{NodeID: "node-1", Cluster: "fixture-service", Identity: "discovered"}},
		Observations: []Observation{}, Findings: []Finding{}, Gaps: []Gap{},
	}
	for _, rule := range Rules() {
		if rule.Scenario == scenario {
			r.Rules = []Rule{rule}
		}
	}
	for round := 0; round < 3; round++ {
		for _, kind := range []string{"metrics", "ops", "registry"} {
			now := start.Add(time.Duration(round) * time.Second)
			if round > 0 {
				now = now.Add(time.Millisecond)
			}
			obs := Observation{
				ID: kind + string(rune('0'+round)), SourceID: kind, NodeID: "node-1", Kind: kind, Round: round,
				StartedAt: now, FinishedAt: now.Add(time.Millisecond), Status: "observed",
			}
			switch kind {
			case "metrics":
				processStart := 100.0
				obs.ProcessStart = &processStart
				for _, operation := range []string{"add_entry", "add_entry_batch", "get_batch_entries"} {
					obs.Counters = append(obs.Counters, Counter{Labels: map[string]string{"node_id": "node-1", "log_id": "2", "log_ns": "tenant-a", "operation": operation, "status": "success"}, Value: 5})
				}
				obs.Counters = append(obs.Counters, Counter{Labels: map[string]string{"node_id": "node-1", "log_id": "2", "log_ns": "tenant-a", "operation": "get_batch_entries", "status": "end_of_file"}, Value: 0})
			case "ops":
				typ := "logstore.add_entry"
				if scenario == "stuck-read" {
					typ = "logstore.get_batch_entries"
				}
				obs.Ops = []Operation{{ID: "op-slow", Type: typ, LogID: ptr(2), SegmentID: ptr(7), StartedAt: start.Add(-time.Minute), TraceID: "1234567890abcdef1234567890abcdef", SpanID: "1234567890abcdef"}}
			case "registry":
				obs.Registry = &Registry{Capacity: 1024, InUse: 1, Evicted: 3}
			}
			r.Observations = append(r.Observations, obs)
		}
	}
	r.Observations = append(r.Observations, Observation{ID: "build", SourceID: "build", NodeID: "node-1", Kind: "build", Round: 0, StartedAt: start, FinishedAt: start, Status: "observed", Version: "v0.1.46"})
	return r
}

func change(r *Report, kind string, round int, fn func(*Observation)) {
	for i := range r.Observations {
		obs := &r.Observations[i]
		if obs.Kind == kind && (round < 0 || obs.Round == round) {
			fn(obs)
		}
	}
}

func TestNativeStallSemantics(t *testing.T) {
	cases := []struct {
		name, scenario, outcome, strength, gap string
		modify                                 func(*Report)
	}{
		{name: "persistent write", outcome: "suspected_stall", strength: "suspected"},
		{name: "persistent batch", outcome: "suspected_stall", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops[0].Type = "logstore.add_entry_batch" })
		}},
		{name: "persistent read", scenario: "stuck-read", outcome: "suspected_stall"},
		{name: "idle", outcome: "no_pending_work_observed", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = []Operation{} })
			change(r, "registry", -1, func(o *Observation) { o.Registry.InUse = 0 })
		}},
		{name: "tail read is not historical read", scenario: "stuck-read", outcome: "no_pending_work_observed", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops[0].Type = "reader.read_next" })
		}},
		{name: "EOF is progress not stalled", scenario: "stuck-read", outcome: "conflicting", strength: "conflicting", modify: func(r *Report) {
			change(r, "metrics", 2, func(o *Observation) { o.Counters[3].Value++ })
		}},
		{name: "same log progress cannot prove this op completed", outcome: "conflicting", modify: func(r *Report) {
			change(r, "metrics", 2, func(o *Observation) { o.Counters[0].Value++ })
		}},
		{name: "another log cannot mask pending write", outcome: "suspected_stall", modify: func(r *Report) {
			change(r, "metrics", 2, func(o *Observation) {
				o.Counters = append(o.Counters, Counter{Labels: map[string]string{"node_id": "node-1", "log_id": "3", "operation": "add_entry", "status": "success"}, Value: 1000})
			})
		}},
		{name: "write progress cannot mask read", scenario: "stuck-read", outcome: "suspected_stall", modify: func(r *Report) {
			change(r, "metrics", 2, func(o *Observation) { o.Counters[0].Value++ })
		}},
		{name: "intermediate reset not hidden by recovered total", outcome: "inconclusive", gap: "completion_counter_reset", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) { o.Counters[0].Value = 0 })
		}},
		{name: "reset cannot be cancelled by other series growth", outcome: "inconclusive", gap: "completion_counter_reset", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) { o.Counters[0].Value = 0; o.Counters[1].Value = 10 })
		}},
		{name: "process restart", outcome: "inconclusive", gap: "process_restart_in_window", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) { v := 200.0; o.ProcessStart = &v })
		}},
		{name: "missing lazy counter not zero", outcome: "inconclusive", gap: "matching_completion_series_absent", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) { o.Counters = nil })
		}},
		{name: "disappearing series", outcome: "inconclusive", gap: "completion_series_changed", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) { o.Counters = o.Counters[1:] })
		}},
		{name: "wrong node labels", outcome: "inconclusive", gap: "matching_completion_series_absent", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) {
				for i := range o.Counters {
					o.Counters[i].Labels["node_id"] = "other-node"
				}
			})
		}},
		{name: "new op not sustained", outcome: "no_persistent_operation_observed", modify: func(r *Report) {
			change(r, "ops", 0, func(o *Observation) { o.Ops = nil })
			change(r, "registry", 0, func(o *Observation) { o.Registry.InUse = 0 })
		}},
		{name: "op absent in middle cannot prove continuity", outcome: "no_persistent_operation_observed", modify: func(r *Report) {
			change(r, "ops", 1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", 1, func(o *Observation) { o.Registry.InUse = 0 })
		}},
		{name: "reused op ID with new start", outcome: "no_persistent_operation_observed", modify: func(r *Report) {
			change(r, "ops", 2, func(o *Observation) { o.Ops[0].StartedAt = o.Ops[0].StartedAt.Add(time.Second) })
		}},
		{name: "unscoped operation", outcome: "inconclusive", gap: "log_identity_unknown", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops[0].LogID = nil })
		}},
		{name: "snapshot", outcome: "inconclusive", gap: "insufficient_live_samples", modify: func(r *Report) { r.Mode = "snapshot" }},
		{name: "source failure", outcome: "inconclusive", gap: "required_source_incomplete", modify: func(r *Report) {
			change(r, "ops", 1, func(o *Observation) { o.Status = "unavailable" })
		}},
		{name: "unsupported version", outcome: "inconclusive", gap: "unsupported_version_or_conflicting_observations", modify: func(r *Report) {
			change(r, "build", 0, func(o *Observation) { o.Status = "unsupported"; o.Code = "unsupported_version"; o.Version = "v2.0.0" })
		}},
		{name: "bounded registry cannot exclude hidden work", outcome: "inconclusive", gap: "registry_listing_may_be_truncated", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", -1, func(o *Observation) { o.Registry.InUse = 100 })
		}},
		{name: "evicted registry cannot prove completed", outcome: "inconclusive", gap: "registry_eviction_or_reset_in_window", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", 1, func(o *Observation) { o.Registry.Evicted++ })
		}},
		{name: "unknown registry", outcome: "inconclusive", gap: "registry_coverage_unknown", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", -1, func(o *Observation) { o.Status = "unsupported"; o.Registry = nil })
		}},
		{name: "server-side limit below requested cap", outcome: "inconclusive", gap: "registry_listing_incomplete_or_skewed", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", -1, func(o *Observation) { o.Registry.InUse = 2 })
		}},
		{name: "conflicting duplicate counter", outcome: "inconclusive", gap: "conflicting_duplicate_counter_series", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) { dup := o.Counters[0]; dup.Value++; o.Counters = append(o.Counters, dup) })
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scenario := tc.scenario
			if scenario == "" {
				scenario = "stuck-write"
			}
			r := fixtureReport(scenario)
			if tc.modify != nil {
				tc.modify(&r)
			}
			Evaluate(&r)
			require.Len(t, r.Findings, 1)
			f := r.Findings[0]
			require.Equal(t, tc.outcome, f.Outcome)
			if f.Outcome == "inconclusive" || f.Outcome == "conflicting" {
				require.Equal(t, "unknown", f.Severity)
			}
			if tc.strength != "" {
				require.Equal(t, tc.strength, f.Strength)
			}
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if f.Scope.Operation != "" {
				require.Contains(t, f.Scope.Tenant, "unresolved")
				require.NotContains(t, f.Scope.Tenant, "tenant-a")
			}
			for _, id := range append(f.Supporting, f.Contradicting...) {
				found := false
				for _, obs := range r.Observations {
					found = found || obs.ID == id
				}
				require.True(t, found, "dangling evidence reference %s", id)
			}
		})
	}
}

func TestDuplicateEvidenceDoesNotBecomeCorroboration(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		r := fixtureReport("stuck-write")
		duplicate := r.Observations[0]
		duplicate.ID = "another-wrapper-of-same-observation"
		if conflict {
			duplicate.Status = "invalid"
		}
		r.Observations = append(r.Observations, duplicate)
		Evaluate(&r)
		require.Len(t, r.Observations, 10)
		if conflict {
			require.Equal(t, "inconclusive", r.Findings[0].Outcome)
		} else {
			require.Equal(t, "suspected", r.Findings[0].Strength)
			require.Len(t, r.Findings[0].Supporting, 6)
		}
	}
}

func TestReportFixtures(t *testing.T) {
	for _, name := range []string{"scoped-write-v1", "ambiguous-read-v1"} {
		scenario := "stuck-write"
		if name == "ambiguous-read-v1" {
			scenario = "stuck-read"
		}
		r := fixtureReport(scenario)
		if scenario == "stuck-read" {
			change(&r, "metrics", 2, func(o *Observation) { o.Counters[2].Value++ })
		}
		Evaluate(&r)
		body, err := json.MarshalIndent(r, "", "  ")
		require.NoError(t, err)
		body = append(body, '\n')
		path := filepath.Join("testdata", name+".json")
		if os.Getenv("UPDATE_DIAG_FIXTURES") == "1" {
			require.NoError(t, os.MkdirAll("testdata", 0o755))
			require.NoError(t, os.WriteFile(path, body, 0o600))
		}
		expected, err := os.ReadFile(path)
		require.NoError(t, err)
		require.JSONEq(t, string(expected), string(body))
		var parsed Report
		require.NoError(t, json.Unmarshal(expected, &parsed))
		require.Equal(t, SchemaVersion, parsed.SchemaVersion)
		require.Equal(t, "node-1", parsed.Findings[0].Scope.NodeID)
		require.Equal(t, int64(2), *parsed.Findings[0].Scope.LogID)
		require.Equal(t, int64(7), *parsed.Findings[0].Scope.SegmentID)
		require.Contains(t, parsed.Findings[0].Unknown, "tenant_association_unknown")
	}
}

func TestCancelledEvaluationRetainsEvidenceWithoutStallAssertion(t *testing.T) {
	r := fixtureReport("stuck-write")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	EvaluateContext(ctx, &r)
	require.Equal(t, "cancelled", r.Status)
	require.Len(t, r.Observations, 10)
	require.Len(t, r.Findings, 1)
	require.Equal(t, "inconclusive", r.Findings[0].Outcome)
	require.Contains(t, r.Findings[0].Unknown, "evaluation_interrupted")
}
