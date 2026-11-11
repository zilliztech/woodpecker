package diagnosis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFixture() Report {
	r := fixtureReport("stuck-write")
	r.Rules = writeRules()
	for round := 0; round < 3; round++ {
		var source Observation
		for _, obs := range r.Observations {
			if obs.Kind == "ops" && obs.Round == round {
				source = obs
				break
			}
		}
		source.ID, source.SourceID, source.Kind, source.Ops = fmt.Sprintf("writers%d", round), "writers", "writers", nil
		source.Writers = []Writer{{LogID: 2, SegmentID: 7, Backend: "stagedstorage", Writable: true, BufferBytes: 1024, BufferCount: 8, QueueDepth: 2, QueueCap: 8, Durable: 5}}
		r.Observations = append(r.Observations, source)
	}
	change(&r, "ops", -1, func(obs *Observation) { obs.Ops[0].Type = "file.flush" })
	change(&r, "metrics", -1, func(obs *Observation) {
		for _, v := range []struct {
			name  string
			value float64
		}{{"disk_usage_ratio", .6}, {"disk_watermark_soft", .8}, {"disk_watermark_hard", .9}, {"write_backpressure_state", 0}} {
			obs.Signals = append(obs.Signals, Signal{Name: v.name, Value: v.value, Labels: map[string]string{"node_id": "node-1"}})
		}
	})
	return r
}

func findingByRule(t *testing.T, r Report, id string) Finding {
	t.Helper()
	for _, f := range r.Findings {
		if f.RuleID == id {
			return f
		}
	}
	t.Fatalf("missing %s in %+v", id, r.Findings)
	return Finding{}
}

func TestWriteBuildupSemantics(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, gap string
		modify             func(*Report)
	}{
		{name: "no durable progress", outcome: "no_durable_progress_observed"},
		{name: "progress despite long flush", outcome: "no_buildup_observed", modify: func(r *Report) {
			change(r, "writers", -1, func(o *Observation) { o.Writers[0].Durable += int64(o.Round) })
		}},
		{name: "incoming pressure with progress", outcome: "observed_buildup", modify: func(r *Report) {
			change(r, "writers", -1, func(o *Observation) {
				o.Writers[0].BufferBytes += int64(o.Round) * 100
				o.Writers[0].Durable += int64(o.Round)
			})
		}},
		{name: "queue saturation despite progress", outcome: "queue_saturated", modify: func(r *Report) {
			change(r, "writers", -1, func(o *Observation) { o.Writers[0].QueueDepth = 8; o.Writers[0].Durable += int64(o.Round) })
		}},
		{name: "idle buffer", outcome: "no_buildup_observed", modify: func(r *Report) { change(r, "writers", -1, func(o *Observation) { o.Writers[0].QueueDepth = 0 }) }},
		{name: "normal batching not pressure", outcome: "buffer_growth_observed", gap: "buffer_growth_may_be_normal_batching", modify: func(r *Report) {
			change(r, "writers", -1, func(o *Observation) { o.Writers[0].QueueDepth = 0; o.Writers[0].BufferBytes += int64(o.Round) })
		}},
		{name: "transient recovered", outcome: "transient_buildup_observed", modify: func(r *Report) {
			change(r, "writers", -1, func(o *Observation) {
				o.Writers[0].Durable += int64(o.Round)
				if o.Round == 1 {
					o.Writers[0].QueueDepth = 8
				}
			})
		}},
		{name: "draining", outcome: "no_buildup_observed", modify: func(r *Report) {
			change(r, "writers", -1, func(o *Observation) {
				o.Writers[0].QueueDepth = 2 - o.Round
				o.Writers[0].BufferBytes = int64(2-o.Round) * 512
				o.Writers[0].Durable += int64(o.Round)
			})
		}},
		{name: "snapshot", outcome: "inconclusive", gap: "insufficient_live_samples", modify: func(r *Report) { r.Mode = "snapshot" }},
		{name: "partial", outcome: "inconclusive", gap: "required_source_incomplete", modify: func(r *Report) { change(r, "writers", 1, func(o *Observation) { o.Status = "unavailable" }) }},
		{name: "restart", outcome: "inconclusive", gap: "process_restart_in_window", modify: func(r *Report) { change(r, "metrics", 1, func(o *Observation) { n := 101.0; o.ProcessStart = &n }) }},
		{name: "missing process identity", outcome: "inconclusive", gap: "process_identity_unavailable", modify: func(r *Report) { change(r, "metrics", 1, func(o *Observation) { o.ProcessStart = nil }) }},
		{name: "writer disappears", outcome: "inconclusive", gap: "writer_changed_or_reset", modify: func(r *Report) { change(r, "writers", 1, func(o *Observation) { o.Writers = nil }) }},
		{name: "durable reset", outcome: "inconclusive", gap: "writer_changed_or_reset", modify: func(r *Report) { change(r, "writers", 1, func(o *Observation) { o.Writers[0].Durable = 0 }) }},
		{name: "closed in middle", outcome: "inconclusive", gap: "writer_changed_or_reset", modify: func(r *Report) { change(r, "writers", 1, func(o *Observation) { o.Writers[0].Closed = true }) }},
		{name: "registry eviction qualifies positive evidence", outcome: "no_durable_progress_observed", gap: "registry_eviction_or_reset_in_window", modify: func(r *Report) { change(r, "registry", 1, func(o *Observation) { o.Registry.Evicted++ }) }},
		{name: "unsupported", outcome: "inconclusive", gap: "evaluation_interrupted_or_incompatible", modify: func(r *Report) { change(r, "build", 0, func(o *Observation) { o.Code = "unsupported_version" }) }},
		{name: "unverified node", outcome: "inconclusive", gap: "node_identity_unverified", modify: func(r *Report) { r.Targets[0].Identity = "explicit_unverified" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := writeFixture()
			if tc.modify != nil {
				tc.modify(&r)
			}
			Evaluate(&r)
			f := findingByRule(t, r, "flush_buildup")
			require.Equal(t, tc.outcome, f.Outcome, f.Summary)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if f.Severity == "warning" {
				require.Equal(t, "suspected", f.Strength)
				require.EqualValues(t, 2, *f.Scope.LogID)
				require.EqualValues(t, 7, *f.Scope.SegmentID)
			}
		})
	}
}

func TestDiskAdmissionUsesDeployedPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, gap       string
		usage, soft, hard, state float64
		modify                   func(*Observation)
	}{
		{name: "normal", outcome: "below_configured_watermarks", usage: .7, soft: .8, hard: .9},
		{name: "custom soft", outcome: "observed_admission_pressure", usage: .6, soft: .5, hard: .7, state: 1},
		{name: "hard", outcome: "observed_admission_pressure", usage: .9, soft: .8, hard: .9, state: 2},
		{name: "disabled", outcome: "policy_disabled", usage: .99},
		{name: "disabled with lazy gauges absent", outcome: "policy_disabled", modify: func(o *Observation) { o.Signals = o.Signals[1:3] }},
		{name: "free space floor may block below hard", outcome: "observed_admission_pressure", gap: "absolute_free_floor_or_sampling_skew_unverified", usage: .6, soft: .8, hard: .9, state: 2},
		{name: "empty soft hard band", outcome: "observed_admission_pressure", usage: .8, soft: .8, hard: .8, state: 2},
		{name: "skew", outcome: "conflicting", usage: .95, soft: .8, hard: .9},
		{name: "missing", outcome: "inconclusive", gap: "disk_signals_missing", modify: func(o *Observation) { o.Signals = nil }},
		{name: "invalid policy", outcome: "inconclusive", gap: "invalid_disk_policy_or_state", usage: .5, soft: .9, hard: .8},
		{name: "timestamp", outcome: "inconclusive", gap: "explicit_metric_timestamp_unverified", modify: func(o *Observation) { o.Signals[0].TimestampMS = ptr(123) }},
		{name: "duplicate", outcome: "inconclusive", gap: "ambiguous_disk_series", modify: func(o *Observation) { o.Signals = append(o.Signals, o.Signals[0]) }},
		{name: "several paths cannot be bound to one node policy", outcome: "inconclusive", gap: "disk_policy_path_ambiguous", modify: func(o *Observation) {
			o.Signals = append(o.Signals, Signal{Name: "disk_usage_ratio", Value: .95, Labels: map[string]string{"node_id": "node-1", "path": "/old-wal"}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := writeFixture()
			r.Mode = "snapshot"
			change(&r, "metrics", -1, func(o *Observation) {
				for i, v := range []float64{tc.usage, tc.soft, tc.hard, tc.state} {
					o.Signals[i].Value = v
				}
				if tc.modify != nil {
					tc.modify(o)
				}
			})
			Evaluate(&r)
			f := findingByRule(t, r, "disk_admission")
			require.Equal(t, tc.outcome, f.Outcome)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
		})
	}
}

func replicaFixture() Report {
	r := writeFixture()
	baseline := append([]Observation(nil), r.Observations...)
	for n := 2; n <= 3; n++ {
		id := fmt.Sprintf("node-%d", n)
		r.Targets = append(r.Targets, Target{NodeID: id, Identity: "discovered"})
		for _, obs := range baseline {
			obs.ID, obs.SourceID, obs.NodeID = id+":"+obs.ID, id+":"+obs.SourceID, id
			if obs.Kind == "writers" {
				obs.Writers = append([]Writer(nil), obs.Writers...)
				obs.Writers[0].Durable += int64(obs.Round)
			}
			r.Observations = append(r.Observations, obs)
		}
	}
	r.Observations = append(r.Observations, Observation{ID: "metadata", SourceID: "metadata", Kind: "quorum", Status: "observed", Quorum: &Quorum{LogID: 2, SegmentID: 7, State: "Active", ES: 3, WQ: 3, AQ: 2, NodeIDs: []string{"node-1", "node-2", "node-3"}, Revision: 42}})
	return r
}

func TestReplicaLagIsNotQuorumOutage(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, gap string
		modify             func(*Report)
	}{
		{name: "one lagging", outcome: "suspected_replica_lag"},
		{name: "all idle", outcome: "no_replica_lag_observed", modify: func(r *Report) { change(r, "writers", -1, func(o *Observation) { o.Writers[0].Durable = 5 }) }},
		{name: "no namespace selected", outcome: "inconclusive", gap: "explicit_quorum_unavailable", modify: func(r *Report) { r.Observations = r.Observations[:len(r.Observations)-1] }},
		{name: "partial fanout", outcome: "inconclusive", gap: "partial_replica_fanout_not_quorum_outage", modify: func(r *Report) { r.Targets = r.Targets[:2] }},
		{name: "unresolved member", outcome: "inconclusive", gap: "quorum_identity_unresolved_or_ambiguous", modify: func(r *Report) { r.Observations[len(r.Observations)-1].Quorum.NodeIDs[2] = "" }},
		{name: "non active", outcome: "inconclusive", gap: "inactive_or_invalid_quorum", modify: func(r *Report) { r.Observations[len(r.Observations)-1].Quorum.State = "Sealed" }},
		{name: "replica restarted", outcome: "inconclusive", gap: "partial_replica_fanout_not_quorum_outage", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) {
				if o.NodeID == "node-3" {
					n := 101.0
					o.ProcessStart = &n
				}
			})
		}},
		{name: "missing writer", outcome: "inconclusive", gap: "partial_replica_fanout_not_quorum_outage", modify: func(r *Report) {
			change(r, "writers", 1, func(o *Observation) {
				if o.NodeID == "node-3" {
					o.Writers = nil
				}
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := replicaFixture()
			if tc.modify != nil {
				tc.modify(&r)
			}
			Evaluate(&r)
			f := findingByRule(t, r, "replica_participation")
			require.Equal(t, tc.outcome, f.Outcome, f.Summary)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if tc.outcome == "suspected_replica_lag" {
				require.Contains(t, f.Summary, "wq=3, aq=2")
				require.Contains(t, f.Summary, "node-1")
				require.Equal(t, "suspected", f.Strength)
			}
		})
	}
}

func TestWriteDuplicateViewsDoNotPromoteConfidence(t *testing.T) {
	r := writeFixture()
	Evaluate(&r)
	want := findingByRule(t, r, "flush_buildup")
	r.Observations = append(r.Observations, r.Observations...)
	Evaluate(&r)
	got := findingByRule(t, r, "flush_buildup")
	require.Equal(t, want, got)
	change(&r, "writers", 1, func(o *Observation) { o.Writers[0].Durable++ })
	// A separate contradictory observation of one node/round is not a new source.
	conflict := r.Observations[10]
	conflict.Writers = []Writer{{LogID: 2}}
	r.Observations = append(r.Observations, conflict)
	Evaluate(&r)
	require.Equal(t, "inconclusive", findingByRule(t, r, "flush_buildup").Outcome)
}

func TestBoundedWriterCollection(t *testing.T) {
	for _, tc := range []struct {
		name, list, detail, code string
		limit                    int
	}{
		{name: "valid"},
		{name: "empty idle", list: `{"segments":null}`},
		{name: "missing list", list: `{}`, code: "invalid_writer_list"},
		{name: "missing fields", detail: `{"log_id":2,"segment_id":7}`, code: "invalid_writer_detail"},
		{name: "wrong identity", detail: `{"log_id":99}`, code: "invalid_writer_detail"},
		{name: "ambiguous identity", list: `{"segments":[{"log_id":2,"segment_id":7,"backend":"disk"},{"log_id":2,"segment_id":7,"backend":"disk"}]}`, code: "ambiguous_writer_identity"},
		{name: "limit", list: `{"segments":[{"log_id":2,"segment_id":7,"backend":"disk"},{"log_id":2,"segment_id":8,"backend":"disk"}]}`, limit: 1, code: "writer_limit_exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, http.MethodGet, req.Method)
				if strings.HasSuffix(req.URL.Path, "/detail") {
					requests++
					if tc.detail != "" {
						fmt.Fprint(w, tc.detail)
						return
					}
					json.NewEncoder(w).Encode(Writer{LogID: 2, SegmentID: 7, Backend: "disk", Writable: true, QueueCap: 8, Durable: 5})
					return
				}
				if tc.list != "" {
					fmt.Fprint(w, tc.list)
				} else {
					fmt.Fprint(w, `{"segments":[{"log_id":2,"segment_id":7,"backend":"disk"}]}`)
				}
			}))
			defer server.Close()
			o := testOptions()
			o.Scenario = "write"
			o.Limits.MaxWriters = tc.limit
			obs := collectOne(context.Background(), server.Client(), Node{Target: Target{NodeID: "node-1"}, URL: server.URL}, "writers", 0, o)
			require.Equal(t, tc.code, obs.Code)
			require.LessOrEqual(t, requests, o.writerLimit())
		})
	}
	o := testOptions()
	o.Scenario = "write"
	o.Limits.MaxWriters = 65
	require.Error(t, o.Validate())
	o.Limits.MaxWriters = 16
	o.Limits.MaxNodes = 32
	o.Limits.MaxSamples = 121
	require.Error(t, o.Validate())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := writeFixture()
	EvaluateContext(ctx, &r)
	require.Equal(t, "cancelled", r.Status)
	require.Equal(t, "inconclusive", findingByRule(t, r, "flush_buildup").Outcome)
}

func TestWriteAdmissionCollectionCoverage(t *testing.T) {
	for _, tc := range []struct {
		name, usage, status, outcome, gap string
		limit                             int
	}{
		{name: "deployed soft policy", usage: "0.85", status: "completed", outcome: "observed_admission_pressure"},
		{name: "cached timestamp is unverified", usage: "0.85 123", status: "completed", outcome: "inconclusive", gap: "explicit_metric_timestamp_unverified"},
		{name: "nonfinite gauge is incomplete", usage: "NaN", status: "partial", outcome: "inconclusive", gap: "invalid_write_signal"},
		{name: "negative gauge is incomplete", usage: "-1", status: "partial", outcome: "inconclusive", gap: "invalid_write_signal"},
		{name: "signal limit is not a healthy zero", usage: "0.85", limit: 1, status: "partial", outcome: "inconclusive", gap: "metric_series_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := testMetrics + "# TYPE woodpecker_server_logstore_disk_usage_ratio gauge\nwoodpecker_server_logstore_disk_usage_ratio{node_id=\"node-1\",path=\"/wal\"} " + tc.usage + "\n"
			for _, v := range []struct {
				name  string
				value float64
			}{{"disk_watermark_soft", .8}, {"disk_watermark_hard", .9}, {"write_backpressure_state", 1}} {
				metrics += fmt.Sprintf("# TYPE %s%s gauge\n%s%s{node_id=\"node-1\"} %g\n", diskPrefix, v.name, diskPrefix, v.name, v.value)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, http.MethodGet, req.Method)
				switch req.URL.Path {
				case "/metrics":
					fmt.Fprint(w, metrics)
				case "/admin/logstore/segments":
					fmt.Fprint(w, `{"segments":[]}`)
				case "/admin/runtime/ops":
					fmt.Fprint(w, `[]`)
				case "/admin/runtime/ops/stats":
					fmt.Fprint(w, `{"capacity":1024,"in_use":0,"evicted_total":0}`)
				case "/admin/env":
					fmt.Fprint(w, `{"build":{"version":"dev"},"env":{"TOKEN":"sentinel-secret"}}`)
				default:
					t.Errorf("unexpected request: %s", req.URL.Path)
				}
			}))
			defer server.Close()
			o := testOptions()
			o.Scenario, o.Mode = "write", "snapshot"
			if tc.limit != 0 {
				o.Limits.MaxSeries = tc.limit
			}
			r := NewReport(o, "fixture")
			Collect(context.Background(), &r, []Node{{Target: Target{NodeID: "node-1", Identity: "discovered"}, URL: server.URL}}, o)
			require.Equal(t, tc.status, r.Status)
			require.Equal(t, tc.outcome, findingByRule(t, r, "disk_admission").Outcome)
			if tc.gap != "" {
				found := false
				for _, gap := range r.Gaps {
					found = found || gap.Code == tc.gap
				}
				require.True(t, found, "missing gap %s", tc.gap)
			}
			raw, err := json.Marshal(r)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "sentinel-secret")
			require.NotContains(t, string(raw), server.URL)
		})
	}
}
