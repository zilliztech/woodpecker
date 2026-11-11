package diagnosis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
)

func readFinding(rule Rule, node string) Finding {
	return Finding{
		RuleID: rule.ID, RuleVersion: rule.Version, Flow: rule.Flow, Stage: rule.Stage,
		Scope:   Scope{NodeID: node, Tenant: "unresolved: server operations have no reader or tenant identity"},
		Outcome: "inconclusive", Severity: "unknown", Strength: "inconclusive", Supporting: []string{}, Contradicting: []string{}, Unknown: []string{},
		NextChecks: []string{"Reuse the original WP context and node mappings", "wp ops list NODE --type logstore.get_batch_entries", "wp log readers NAME --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX", "wp logstore lac NAME SEGMENT --etcd ENDPOINTS --meta-prefix WP_CLIENT_PREFIX"},
	}
}

func readProcessWindow(report *Report, metrics, ops []Observation) string {
	if report.Mode != "live_window" || len(metrics) < 2 || len(ops) != len(metrics) {
		return "insufficient_live_samples"
	}
	if ops[len(ops)-1].StartedAt.Sub(ops[0].FinishedAt).Milliseconds() < report.WindowMS || !ops[len(ops)-1].StartedAt.After(ops[0].FinishedAt) {
		return "observed_window_shorter_than_requested"
	}
	var start float64
	for i, obs := range metrics {
		if obs.Status != "observed" || ops[i].Status != "observed" {
			return "required_source_incomplete"
		}
		if obs.ProcessStart == nil {
			return "process_identity_unavailable"
		}
		if i > 0 && *obs.ProcessStart != start {
			return "process_restart_in_window"
		}
		start = *obs.ProcessStart
	}
	return ""
}

func evaluateReadActivity(ctx context.Context, report *Report, target Target, rule Rule, byRound map[int]map[string]Observation, rounds []int, conflict, unsupported bool) {
	f := readFinding(rule, target.NodeID)
	if ctx.Err() != nil || conflict || unsupported || target.Identity != "discovered" {
		f.Summary = "Read activity cannot be assessed with interrupted, incompatible or unverified node evidence"
		f.Unknown = append(f.Unknown, "read_node_evidence_unverified")
		appendFinding(report, f)
		return
	}
	var metrics, ops, registry []Observation
	for _, round := range rounds {
		metrics = append(metrics, byRound[round]["metrics"])
		ops = append(ops, byRound[round]["ops"])
		registry = append(registry, byRound[round]["registry"])
	}
	if gap := readProcessWindow(report, metrics, ops); gap != "" {
		f.Summary = "A complete stable live window is required for read activity"
		f.Unknown = append(f.Unknown, gap)
		appendFinding(report, f)
		return
	}
	for i, obs := range metrics {
		f.Supporting = appendIDs(f.Supporting, obs.ID)
		f.Supporting = appendIDs(f.Supporting, ops[i].ID)
	}
	registryUnknown := registryGaps(registry, ops, report.Limits.MaxOps)
	logs := map[int64]bool{}
	var samples [][]Operation
	for _, obs := range ops {
		samples = append(samples, obs.Ops)
		for _, op := range obs.Ops {
			if MatchesOperation("read_stall", op.Type) && op.LogID != nil {
				logs[*op.LogID] = true
			}
		}
	}
	for _, obs := range metrics {
		for _, c := range obs.Counters {
			if c.Labels["node_id"] == target.NodeID && c.Labels["operation"] == "get_batch_entries" {
				if id, err := strconv.ParseInt(c.Labels["log_id"], 10, 64); err == nil && id >= 0 {
					logs[id] = true
				}
			}
		}
	}
	if len(logs) == 0 {
		f.Outcome, f.Severity = "no_pending_read_observed", "none"
		f.Summary = "No scoped pending read or read completion series was observed; this is not proof that every reader is idle or healthy"
		f.Unknown = append(f.Unknown, registryUnknown...)
		if len(registryUnknown) > 0 {
			f.Outcome, f.Severity = "inconclusive", "unknown"
		}
		appendFinding(report, f)
		return
	}
	ids := make([]int64, 0, len(logs))
	for id := range logs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	persistent := persistentOperations(ctx, samples, "read_stall")
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		current := f
		current.Scope.LogID = &id
		current.Unknown = append([]string{}, registryUnknown...)
		current.Unknown = append(current.Unknown, "sampled_not_continuous", "metrics_and_ops_not_atomic", "server_op_reader_binding_unavailable", "tenant_association_unknown")
		var pending *Operation
		for i := range persistent {
			if persistent[i].LogID != nil && *persistent[i].LogID == id {
				pending = &persistent[i]
				break
			}
		}
		if pending != nil {
			current.Scope.Operation, current.Scope.SegmentID, current.Scope.TraceID = pending.ID, pending.SegmentID, pending.TraceID
		}
		progress, gap := compareProgress(ctx, metrics, "read_stall", target.NodeID, &id)
		namespaces := map[string]bool{}
		eof := false
		previous := map[string]float64{}
		for _, obs := range metrics {
			for _, c := range obs.Counters {
				l := c.Labels
				if l["node_id"] != target.NodeID || l["log_id"] != strconv.FormatInt(id, 10) || l["operation"] != "get_batch_entries" {
					continue
				}
				namespaces[l["log_ns"]] = true
				key := l["log_ns"] + ":" + l["status"]
				if before, ok := previous[key]; ok && l["status"] == "end_of_file" && c.Value > before {
					eof = true
				}
				previous[key] = c.Value
			}
		}
		if len(namespaces) > 1 {
			gap = "ambiguous_log_namespace"
		}
		switch {
		case gap != "":
			current.Summary = "Read completion progress is unverified; absence is not zero"
			current.Unknown = append(current.Unknown, gap)
		case progress && eof:
			current.Outcome, current.Severity = "eof_observed", "none"
			current.Summary = "Matching-log EOF completions advanced; normal boundary/tailing is possible, and this does not prove a particular pending operation completed"
		case progress:
			current.Outcome, current.Severity = "forward_progress_observed", "none"
			current.Summary = "Matching-log read completions advanced; do not attribute those completions to a particular pending operation or reader"
		case pending != nil:
			current.Outcome = "pending_read_expectation_unknown"
			current.Summary = "A server read operation persisted without matching completion progress; expected data and reader association are unverified, so no catch-up stall is asserted"
		default:
			current.Outcome, current.Severity = "no_pending_read_observed", "none"
			current.Summary = "No persistent read operation or completion increase was observed for this log; idle is possible but global read health is unproven"
		}
		if pending != nil && progress {
			current.Unknown = append(current.Unknown, "same_log_progress_may_mask_pending_operation")
		}
		if len(registryUnknown) > 0 && current.Outcome == "no_pending_read_observed" {
			current.Outcome, current.Severity = "inconclusive", "unknown"
		}
		current.Summary = fmt.Sprintf("Log %d: %s", id, current.Summary)
		appendFinding(report, current)
	}
}
