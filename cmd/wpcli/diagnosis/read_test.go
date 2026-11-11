package diagnosis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func readFixture() Report {
	r := fixtureReport("stuck-read")
	r.Rules = readRules()
	r.ReadMode = "catch-up"
	r.WindowMS = 60000
	r.IntervalMS = 30000
	start := r.StartedAt
	for i := range r.Observations {
		o := &r.Observations[i]
		o.StartedAt = start.Add(time.Duration(o.Round) * 30 * time.Second)
		if o.Round > 0 {
			o.StartedAt = o.StartedAt.Add(time.Millisecond)
		}
		o.FinishedAt = o.StartedAt.Add(time.Millisecond)
	}
	for round := 0; round < 3; round++ {
		now := start.Add(time.Duration(round)*30*time.Second + 3*time.Millisecond)
		s := &ReadState{
			LogID: 2, SegmentID: 7, SegmentState: "Active", LastEntry: -1, TruncatedSegment: -1, TruncatedEntry: -1, ES: 1, WQ: 1, AQ: 1, NodeIDs: []string{"node-1"}, Revision: int64(round + 100), ReportIntervalMS: 30000,
			Readers: []ReaderPosition{{Name: "reader-1", OpenedMS: start.Add(-time.Minute).UnixMilli(), ReportedMS: now.Add(-time.Second).UnixMilli(), SegmentID: 7, NextEntry: 5, CreateRevision: 50, LeaseTTL: 120}},
		}
		r.Observations = append(r.Observations, Observation{ID: fmt.Sprintf("metadata%d", round), SourceID: "metadata", Kind: "read_metadata", Round: round, StartedAt: now, FinishedAt: now, Status: "observed", ReadState: s},
			Observation{ID: fmt.Sprintf("position%d", round), SourceID: "position", NodeID: "node-1", Kind: "read_position", Round: round, StartedAt: now, FinishedAt: now, Status: "observed", ReadPosition: &ReadPosition{LogID: 2, SegmentID: 7, Present: true, Durable: 10, Backend: "stagedstorage"}})
	}
	r.FinishedAt = start.Add(61 * time.Second)
	return r
}

func readResult(t *testing.T, r Report, id string) Finding {
	t.Helper()
	for _, f := range r.Findings {
		if f.RuleID == id {
			return f
		}
	}
	t.Fatalf("missing rule %s", id)
	return Finding{}
}

func TestReaderExpectedProgressSemantics(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, gap string
		modify             func(*Report)
	}{
		{name: "renewed catchup attempts with backlog", outcome: "suspected_catchup_no_progress"},
		{name: "normal tail with backlog is not a stall", outcome: "read_expectation_unverified", modify: func(r *Report) { r.ReadMode = "tail" }},
		{name: "unknown mode", outcome: "read_expectation_unverified", modify: func(r *Report) { r.ReadMode = "unknown" }},
		{name: "caught up selected segment", outcome: "caught_up_or_awaiting_data", modify: func(r *Report) { change(r, "read_position", -1, func(o *Observation) { o.ReadPosition.Durable = 4 }) }},
		{name: "data arrived late", outcome: "inconclusive", gap: "expected_data_not_continuously_observed", modify: func(r *Report) { change(r, "read_position", 0, func(o *Observation) { o.ReadPosition.Durable = 4 }) }},
		{name: "position moved", outcome: "reader_forward_progress_observed", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) { o.ReadState.Readers[0].NextEntry += int64(o.Round) })
		}},
		{name: "reader movement remains visible with partial replica evidence", outcome: "reader_forward_progress_observed", gap: "insufficient_quorum_positions", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) { o.ReadState.Readers[0].NextEntry += int64(o.Round) })
			change(r, "read_position", 1, func(o *Observation) { o.ReadPosition.Present = false })
		}},
		{name: "durable replica position reset", outcome: "inconclusive", gap: "durable_position_reset_or_backend_changed", modify: func(r *Report) { change(r, "read_position", 1, func(o *Observation) { o.ReadPosition.Durable = 8 }) }},
		{name: "short window", outcome: "inconclusive", gap: "reader_window_shorter_than_report_cadence", modify: func(r *Report) { r.WindowMS = 2000 }},
		{name: "snapshot", outcome: "inconclusive", gap: "insufficient_reader_window", modify: func(r *Report) { r.Mode = "snapshot" }},
		{name: "attempt timestamp not renewed", outcome: "inconclusive", gap: "reader_attempt_renewal_unverified", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) { o.ReadState.Readers[0].ReportedMS = r.StartedAt.UnixMilli() + 3 })
		}},
		{name: "stale report", outcome: "inconclusive", gap: "reader_report_stale", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) {
				o.ReadState.Readers[0].OpenedMS = r.StartedAt.Add(-3 * time.Minute).UnixMilli()
				o.ReadState.Readers[0].ReportedMS = o.StartedAt.Add(-2 * time.Minute).UnixMilli()
			})
		}},
		{name: "expired lease", outcome: "inconclusive", gap: "reader_lease_expired_or_unverified", modify: func(r *Report) {
			change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.Readers[0].LeaseTTL = -1 })
		}},
		{name: "future clock", outcome: "inconclusive", gap: "reader_clock_or_timestamp_unverified", modify: func(r *Report) {
			change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.Readers[0].ReportedMS = o.StartedAt.Add(time.Minute).UnixMilli() })
		}},
		{name: "reader reopened", outcome: "inconclusive", gap: "reader_generation_changed", modify: func(r *Report) {
			change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.Readers[0].CreateRevision++ })
		}},
		{name: "position reset", outcome: "inconclusive", gap: "reader_position_or_report_reset", modify: func(r *Report) {
			change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.Readers[0].NextEntry = 3 })
		}},
		{name: "reader disappeared", outcome: "inconclusive", gap: "reader_disappeared_or_lease_expired", modify: func(r *Report) { change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.Readers = nil }) }},
		{name: "wrong segment", outcome: "inconclusive", gap: "reader_segment_outside_selection", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) { o.ReadState.Readers[0].SegmentID = 8 })
		}},
		{name: "Latest sentinel is not a numeric position", outcome: "inconclusive", gap: "reader_start_position_unresolved", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) {
				o.ReadState.Readers[0].SegmentID = math.MaxInt64
				o.ReadState.Readers[0].NextEntry = math.MaxInt64
			})
		}},
		{name: "declared truncation", outcome: "inconclusive", gap: "reader_position_truncated", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) { o.ReadState.TruncatedSegment = 7; o.ReadState.TruncatedEntry = 5 })
		}},
		{name: "partial selection", outcome: "inconclusive", gap: "insufficient_quorum_positions", modify: func(r *Report) { r.Targets[0].Identity = "explicit_unverified" }},
		{name: "missing writer is not corruption", outcome: "inconclusive", gap: "insufficient_quorum_positions", modify: func(r *Report) {
			change(r, "read_position", 1, func(o *Observation) { o.ReadPosition.Present = false })
		}},
		{name: "quorum changed", outcome: "inconclusive", gap: "reader_metadata_scope_changed_or_incomplete", modify: func(r *Report) {
			change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.NodeIDs = []string{"other"} })
		}},
		{name: "restart", outcome: "inconclusive", gap: "process_restart_in_window", modify: func(r *Report) { change(r, "metrics", 1, func(o *Observation) { v := 101.0; o.ProcessStart = &v }) }},
		{name: "node mapping reaches another exporter", outcome: "inconclusive", gap: "quorum_node_identity_unverified", modify: func(r *Report) {
			change(r, "metrics", 1, func(o *Observation) {
				for i := range o.Counters {
					o.Counters[i].Labels["node_id"] = "other-node"
				}
			})
		}},
		{name: "duplicate reader identity", outcome: "inconclusive", gap: "ambiguous_reader_identity", modify: func(r *Report) {
			change(r, "read_metadata", 1, func(o *Observation) { o.ReadState.Readers = append(o.ReadState.Readers, o.ReadState.Readers[0]) })
		}},
		{name: "finalized metadata boundary", outcome: "suspected_catchup_no_progress", modify: func(r *Report) {
			change(r, "read_metadata", -1, func(o *Observation) { o.ReadState.SegmentState = "Sealed"; o.ReadState.LastEntry = 10 })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := readFixture()
			if tc.modify != nil {
				tc.modify(&r)
			}
			Evaluate(&r)
			f := readResult(t, r, "reader_progress")
			require.Equal(t, tc.outcome, f.Outcome, f.Summary)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			if f.Severity == "warning" {
				require.Equal(t, "suspected", f.Strength)
				require.Equal(t, "reader-1", f.Scope.Reader)
			}
		})
	}
}

func TestReadActivityDoesNotInventExpectedWork(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, gap string
		modify             func(*Report)
	}{
		{name: "pending with unknown expectation", outcome: "pending_read_expectation_unknown"},
		{name: "EOF", outcome: "eof_observed", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) {
				for i := range o.Counters {
					if o.Counters[i].Labels["status"] == "end_of_file" {
						o.Counters[i].Value += float64(o.Round)
					}
				}
			})
		}},
		{name: "success", outcome: "forward_progress_observed", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) { o.Counters[2].Value += float64(o.Round) })
		}},
		{name: "unrelated write progress", outcome: "pending_read_expectation_unknown", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) { o.Counters[0].Value += float64(o.Round) })
		}},
		{name: "unrelated log progress", outcome: "pending_read_expectation_unknown", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) {
				o.Counters = append(o.Counters, Counter{Labels: map[string]string{"node_id": "node-1", "log_id": "99", "log_ns": "tenant-a", "operation": "get_batch_entries", "status": "success"}, Value: float64(o.Round)})
			})
		}},
		{name: "reset", outcome: "inconclusive", gap: "completion_counter_reset", modify: func(r *Report) { change(r, "metrics", 1, func(o *Observation) { o.Counters[2].Value = 1 }) }},
		{name: "same numeric log in several tenants", outcome: "inconclusive", gap: "ambiguous_log_namespace", modify: func(r *Report) {
			change(r, "metrics", -1, func(o *Observation) {
				o.Counters = append(o.Counters, Counter{Labels: map[string]string{"node_id": "node-1", "log_id": "2", "log_ns": "tenant-b", "operation": "get_batch_entries", "status": "success"}, Value: 5})
			})
		}},
		{name: "idle without claiming health", outcome: "no_pending_read_observed", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", -1, func(o *Observation) { o.Registry.InUse = 0 })
		}},
		{name: "eviction prevents idle claim", outcome: "inconclusive", gap: "registry_eviction_or_reset_in_window", modify: func(r *Report) {
			change(r, "ops", -1, func(o *Observation) { o.Ops = nil })
			change(r, "registry", -1, func(o *Observation) { o.Registry.InUse = 0; o.Registry.Evicted += int64(o.Round) })
		}},
		{name: "unsupported build", outcome: "inconclusive", gap: "read_node_evidence_unverified", modify: func(r *Report) { change(r, "build", 0, func(o *Observation) { o.Code = "unsupported_version" }) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := readFixture()
			if tc.modify != nil {
				tc.modify(&r)
			}
			Evaluate(&r)
			f := readResult(t, r, "read_activity")
			require.Equal(t, tc.outcome, f.Outcome, f.Summary)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			}
			require.NotEqual(t, "warning", f.Severity)
		})
	}
}

func TestReadEvidenceIsNotDuplicated(t *testing.T) {
	r := readFixture()
	Evaluate(&r)
	want := readResult(t, r, "reader_progress")
	r.Observations = append(r.Observations, r.Observations...)
	Evaluate(&r)
	require.Equal(t, want, readResult(t, r, "reader_progress"))
	conflict := r.Observations[10]
	state := *conflict.ReadState
	state.AQ = 9
	conflict.ReadState = &state
	r.Observations = append(r.Observations, conflict)
	Evaluate(&r)
	require.Equal(t, "inconclusive", readResult(t, r, "reader_progress").Outcome)
}

func TestReadPositionCollectionIsBoundedAndTyped(t *testing.T) {
	for _, tc := range []struct{ name, body, code string }{
		{name: "valid large IDs", body: `{"segments":[{"log_id":469986968496707423,"segment_id":7,"last_entry_id":10,"backend":"stagedstorage"}]}`},
		{name: "missing identity", body: `{"segments":[{"segment_id":7,"last_entry_id":10}]}`, code: "ambiguous_read_position_identity"},
		{name: "duplicate segment", body: `{"segments":[{"log_id":469986968496707423,"segment_id":7,"last_entry_id":10,"backend":"disk"},{"log_id":469986968496707423,"segment_id":7,"last_entry_id":11,"backend":"disk"}]}`, code: "ambiguous_read_position_identity"},
		{name: "empty loaded writers", body: `{"segments":null}`},
		{name: "missing list", body: `{}`, code: "invalid_read_position_schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "469986968496707423", r.URL.Query().Get("log_id"))
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			o := testOptions()
			o.Scenario = "read"
			o.ReadScope = &ReadState{LogID: 469986968496707423, SegmentID: 7}
			obs := collectOne(context.Background(), server.Client(), Node{URL: server.URL}, "read_position", 0, o)
			require.Equal(t, tc.code, obs.Code)
			if tc.name == "valid large IDs" {
				raw, err := json.Marshal(obs)
				require.NoError(t, err)
				require.Contains(t, string(raw), "469986968496707423")
				require.True(t, obs.ReadPosition.Present)
			}
		})
	}
}

func TestReadQuorumVisibilityUsesAQAndPreservesPartialCoverage(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, gap string
		partial            int
		duplicate          bool
	}{
		{name: "aq-th position is eight, not max or min", outcome: "readable_boundary_observed"},
		{name: "aq answers are not whole-quorum coverage", partial: 2, outcome: "partial_reachability", gap: "partial_quorum_observation"},
		{name: "less than aq", partial: 1, outcome: "partial_reachability", gap: "insufficient_quorum_positions"},
		{name: "same node is not two replicas", duplicate: true, outcome: "inconclusive", gap: "quorum_identity_ambiguous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := readFixture()
			baseline := append([]Observation(nil), r.Observations...)
			for n := 2; n <= 3; n++ {
				id := fmt.Sprintf("node-%d", n)
				r.Targets = append(r.Targets, Target{NodeID: id, Identity: "discovered"})
				for _, o := range baseline {
					if o.NodeID != "node-1" {
						continue
					}
					o.ID, o.SourceID, o.NodeID = id+o.ID, id+o.SourceID, id
					o.Counters = append([]Counter(nil), o.Counters...)
					for i := range o.Counters {
						labels := map[string]string{}
						for key, value := range o.Counters[i].Labels {
							labels[key] = value
						}
						labels["node_id"] = id
						o.Counters[i].Labels = labels
					}
					if o.ReadPosition != nil {
						p := *o.ReadPosition
						p.Durable = int64(10 - n)
						o.ReadPosition = &p
					}
					r.Observations = append(r.Observations, o)
				}
			}
			change(&r, "read_metadata", -1, func(o *Observation) {
				o.ReadState.ES, o.ReadState.WQ, o.ReadState.AQ = 3, 3, 2
				o.ReadState.NodeIDs = []string{"node-1", "node-2", "node-3"}
				if tc.duplicate {
					o.ReadState.NodeIDs[2] = "node-1"
				}
			})
			if tc.partial != 0 {
				r.Targets = r.Targets[:tc.partial]
			}
			Evaluate(&r)
			f := readResult(t, r, "read_quorum_visibility")
			require.Equal(t, tc.outcome, f.Outcome, f.Summary)
			if tc.gap != "" {
				require.Contains(t, f.Unknown, tc.gap)
			} else {
				require.Contains(t, f.Summary, "entry=8")
				require.Contains(t, f.Summary, "aq=2")
			}
		})
	}
}

func TestReadCollectionRetainsRequestedSourceCoverage(t *testing.T) {
	for _, tc := range []struct{ name, metadataStatus, position, status, outcome string }{
		{name: "snapshot boundary", metadataStatus: "observed", status: "completed", outcome: "readable_boundary_observed"},
		{name: "requested metadata unavailable", metadataStatus: "unavailable", status: "partial", outcome: "inconclusive"},
		{name: "reader bound exceeded", metadataStatus: "truncated", status: "partial", outcome: "inconclusive"},
		{name: "invalid node position", metadataStatus: "observed", position: `{}`, status: "partial", outcome: "partial_reachability"},
		{name: "no loaded writer is not transport failure", metadataStatus: "observed", position: `{"segments":null}`, status: "completed", outcome: "partial_reachability"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			positionRequests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				switch r.URL.Path {
				case "/metrics":
					fmt.Fprint(w, testMetrics)
				case "/admin/runtime/ops":
					fmt.Fprint(w, `[]`)
				case "/admin/runtime/ops/stats":
					fmt.Fprint(w, `{"capacity":1024,"in_use":0,"evicted_total":0}`)
				case "/admin/env":
					fmt.Fprint(w, `{"build":{"version":"v0.1.46"},"env":{"TOKEN":"sentinel-secret"}}`)
				case "/admin/logstore/segments":
					positionRequests++
					require.Equal(t, "2", r.URL.Query().Get("log_id"))
					if tc.position != "" {
						fmt.Fprint(w, tc.position)
					} else {
						fmt.Fprint(w, `{"segments":[{"log_id":2,"segment_id":7,"backend":"stagedstorage","last_entry_id":10}]}`)
					}
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer server.Close()
			o := testOptions()
			o.Scenario, o.Mode = "read", "snapshot"
			o.ReadMetadata = func(context.Context) Observation {
				now := time.Now().UTC()
				return Observation{Status: tc.metadataStatus, Code: map[bool]string{true: "", false: "fixture_metadata_incomplete"}[tc.metadataStatus == "observed"], StartedAt: now, FinishedAt: now, ReadState: &ReadState{LogID: 2, SegmentID: 7, SegmentState: "Active", ES: 1, WQ: 1, AQ: 1, NodeIDs: []string{"node-1"}, Revision: 100, ReportIntervalMS: 30000}}
			}
			r := NewReport(o, "fixture")
			Collect(context.Background(), &r, []Node{{Target: Target{NodeID: "node-1", Identity: "discovered"}, URL: server.URL}}, o)
			require.Equal(t, tc.status, r.Status)
			require.Equal(t, tc.outcome, readResult(t, r, "read_quorum_visibility").Outcome)
			if tc.metadataStatus != "observed" {
				require.Zero(t, positionRequests, "failed metadata must not broaden node/log inspection")
			}
			raw, err := json.Marshal(r)
			require.NoError(t, err)
			require.NotContains(t, string(raw), server.URL)
			require.NotContains(t, string(raw), "sentinel-secret")
		})
	}
}
