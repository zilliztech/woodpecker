package diagnosis

import (
	"context"
	"fmt"
)

func writeRules() []Rule {
	return []Rule{
		{
			ID: "flush_buildup", Version: 1, Scenario: "write", Flow: "write", Stage: "flush",
			Description: "Writer-local queue/buffer buildup versus durable progress; persistent file.flush is additional operation evidence, never proof of a disk cause",
			Required:    []string{"bounded writer details in every round", "process start identity"}, Optional: []string{"ops and registry"}, MinimumSamples: 2,
			Applicability: "v0.1.x/dev writer snapshot API; disk/stagedstorage/objectstorage backends; current live samples only",
			Comparison:    "same node/log/segment/backend; buffer bytes and entries, queue tasks/capacity, durable entry ID across consecutive samples; no universal latency threshold",
			Exclusions:    []string{"idle/draining/completed writers", "restart/reset", "missing/changing writer", "snapshot", "unknown queue capacity", "partial/truncated collection"},
		},
		{
			ID: "disk_admission", Version: 1, Scenario: "write", Flow: "write", Stage: "admission",
			Description: "Current disk usage relative to this node's exported policy watermarks and admission state; does not establish workload impact",
			Required:    []string{"node-labelled disk usage and soft/hard watermark/state gauges"}, Optional: []string{}, MinimumSamples: 1,
			Applicability: "v0.1.x/dev exporters with disk watermark metrics; disabled policy is explicit",
			Comparison:    "ratios 0..1; state 0=normal, 1=warn, 2=blocked; latest current sample, not a duration or latency assertion",
			Exclusions:    []string{"policy disabled", "missing/ambiguous series", "explicit unverified timestamps", "contradictory policy/state"},
		},
		{
			ID: "replica_participation", Version: 1, Scenario: "write", Flow: "write", Stage: "replication",
			Description: "Compare current durable positions of the explicitly selected segment's configured replicas; lag or incomplete observation is not a quorum outage",
			Required:    []string{"explicit log/segment metadata quorum", "discovered replica identities", "writer details and process identity across the live window"}, Optional: []string{}, MinimumSamples: 2,
			Applicability: "Active segment in an explicitly selected WP-client namespace; configured es/wq/aq; current native evidence only",
			Comparison:    "durable entry IDs per node for the same log/segment; report observed responders against wq/aq without inferring successful acknowledgements",
			Exclusions:    []string{"no explicit metadata binding", "finished/unloaded segment", "partial fan-out", "restart/reset", "no observed peer progress", "unknown tenant association"},
		},
	}
}

func writeBase(rule Rule, node string) Finding {
	return Finding{
		RuleID: rule.ID, RuleVersion: rule.Version, Flow: rule.Flow, Stage: rule.Stage,
		Scope:   Scope{NodeID: node, Tenant: "unresolved: runtime APIs have no tenant namespace"},
		Outcome: "inconclusive", Severity: "unknown", Strength: "inconclusive",
		Supporting: []string{}, Contradicting: []string{}, Unknown: []string{},
		NextChecks: []string{fmt.Sprintf("wp logstore flush-queue %s", node), fmt.Sprintf("wp logstore buffer %s", node), fmt.Sprintf("wp ops list %s --type file.flush", node)},
	}
}

func evaluateWriteRule(ctx context.Context, report *Report, target Target, rule Rule, rounds map[int]map[string]Observation, order []int, conflict, unsupported bool) {
	f := writeBase(rule, target.NodeID)
	if target.Identity != "discovered" {
		f.Summary = "Native write pressure requires a discovered node identity"
		f.Unknown = append(f.Unknown, "node_identity_unverified")
		appendFinding(report, f)
		return
	}
	if ctx.Err() != nil || conflict || unsupported {
		f.Summary = "Interrupted evaluation, conflicting observations or unsupported build prevents this assessment"
		f.Unknown = append(f.Unknown, "evaluation_interrupted_or_incompatible")
		appendFinding(report, f)
		return
	}
	if rule.ID == "replica_participation" {
		if report.Targets[0].NodeID == target.NodeID {
			evaluateReplicas(ctx, report, rule)
		}
		return
	}
	if rule.ID == "disk_admission" {
		if len(order) == 0 {
			f.Summary = "No disk observation is available"
			f.Unknown = append(f.Unknown, "disk_signals_missing")
		} else {
			evaluateDisk(&f, rounds[order[len(order)-1]]["metrics"])
		}
		appendFinding(report, f)
		return
	}
	var writers, metrics, ops, registry []Observation
	for _, round := range order {
		writers = append(writers, rounds[round]["writers"])
		metrics = append(metrics, rounds[round]["metrics"])
		ops = append(ops, rounds[round]["ops"])
		registry = append(registry, rounds[round]["registry"])
	}
	if problem := writeWindow(report, writers, metrics); problem != "" {
		f.Summary = "Writer buildup cannot be assessed for a complete, stable live window"
		f.Unknown = append(f.Unknown, problem)
		appendFinding(report, f)
		return
	}
	for _, obs := range writers {
		f.Supporting = appendIDs(f.Supporting, obs.ID)
	}
	for _, obs := range metrics {
		f.Supporting = appendIDs(f.Supporting, obs.ID)
	}
	if len(writers[0].Writers) == 0 {
		f.Outcome, f.Severity = "no_loaded_writer_observed", "none"
		f.Summary = "No loaded writer was observed in the baseline; this is not proof of a healthy write path"
		for _, obs := range writers[1:] {
			if len(obs.Writers) > 0 {
				f.Outcome, f.Severity = "inconclusive", "unknown"
				f.Unknown = append(f.Unknown, "writer_set_changed")
			}
		}
		appendFinding(report, f)
		return
	}
	for _, first := range writers[0].Writers {
		if ctx.Err() != nil {
			return
		}
		current := f
		current.Unknown = []string{"sampled_not_continuous", "metrics_and_ops_not_atomic", "tenant_association_unknown", "maintenance_object_store_cause_unobserved"}
		current.Scope.LogID, current.Scope.SegmentID = &first.LogID, &first.SegmentID
		current.NextChecks = append([]string{}, f.NextChecks...)
		current.NextChecks = append(current.NextChecks, fmt.Sprintf("wp logstore segment-show %s --log %d --seg %d", target.NodeID, first.LogID, first.SegmentID), "Inspect file.compact operations and backend-specific storage evidence before attributing a maintenance, object-store, disk or fsync cause")
		pending, full, progress, invalid, transientGrowth := true, true, false, false, false
		last := first
		for _, obs := range writers {
			w, ok := findWriter(obs, first.LogID, first.SegmentID)
			if !ok || w.Backend != first.Backend || w.Durable < last.Durable || w.QueueCap != first.QueueCap || !w.Writable || w.Fenced || w.Finalized || w.Closed {
				invalid = true
				break
			}
			pending = pending && w.QueueDepth > 0
			full = full && w.QueueCap > 0 && w.QueueDepth == w.QueueCap
			progress = progress || w.Durable > last.Durable
			transientGrowth = transientGrowth || w.QueueDepth > last.QueueDepth || w.BufferBytes > last.BufferBytes
			last = w
		}
		switch {
		case invalid:
			current.Summary = "Writer identity, capacity or durable position changed/reset; no buildup conclusion"
			current.Unknown = append(current.Unknown, "writer_changed_or_reset")
		case !first.Writable || first.Fenced || first.Finalized || first.Closed || !last.Writable || last.Fenced || last.Finalized || last.Closed:
			current.Summary = "Writer is not continuously accepting writes; rolling/fencing/draining requires separate context"
			current.Unknown = append(current.Unknown, "writer_lifecycle_transition")
		case full || pending && !progress || last.QueueDepth > first.QueueDepth || pending && last.BufferBytes > first.BufferBytes:
			current.Outcome, current.Severity, current.Strength = "observed_buildup", "warning", "suspected"
			current.Summary = fmt.Sprintf("Queue %d→%d/%d tasks; buffer %d→%d bytes; durable entry %d→%d. Sampled buildup does not establish its cause or client latency", first.QueueDepth, last.QueueDepth, last.QueueCap, first.BufferBytes, last.BufferBytes, first.Durable, last.Durable)
			if pending && !progress {
				current.Outcome = "no_durable_progress_observed"
			}
			if full {
				current.Outcome = "queue_saturated"
			}
		case last.BufferBytes > first.BufferBytes:
			current.Outcome = "buffer_growth_observed"
			current.Summary = "Buffer grew without a continuously pending flush queue; normal batching cannot be excluded without deployed flush policy and workload context"
			current.Unknown = append(current.Unknown, "buffer_growth_may_be_normal_batching")
		case transientGrowth:
			current.Outcome, current.Severity = "transient_buildup_observed", "none"
			current.Summary = "Buffer/queue grew within the sampled window then returned to or below baseline; no sustained buildup is established"
		default:
			current.Outcome, current.Severity = "no_buildup_observed", "none"
			current.Summary = fmt.Sprintf("Queue/buffer did not grow or stay full in the sampled window; durable progress=%t. This is not a latency/SLO verdict", progress)
		}
		for _, op := range persistentOperations(ctx, operationSamples(ops), "flush_buildup") {
			if op.LogID != nil && op.SegmentID != nil && *op.LogID == first.LogID && *op.SegmentID == first.SegmentID {
				current.Scope.Operation, current.Scope.TraceID = op.ID, op.TraceID
				current.Unknown = append(current.Unknown, "pending_flush_is_not_proof_of_disk_cause")
				for _, obs := range ops {
					current.Supporting = appendIDs(current.Supporting, obs.ID)
				}
				if progress {
					current.Summary += "; a flush operation persisted while this writer made durable progress, so the writer is not established as fully stalled"
				}
				break
			}
		}
		current.Unknown = append(current.Unknown, registryGaps(registry, ops, report.Limits.MaxOps)...)
		appendFinding(report, current)
	}
	// New writers cannot be silently omitted from a baseline-based comparison.
	for _, obs := range writers[1:] {
		for _, w := range obs.Writers {
			if _, ok := findWriter(writers[0], w.LogID, w.SegmentID); !ok {
				f.Summary = "Additional writers appeared after the baseline; only stable baseline writers were compared"
				f.Unknown = append(f.Unknown, "writer_set_changed")
				appendFinding(report, f)
				return
			}
		}
	}
}

func operationSamples(observations []Observation) [][]Operation {
	var samples [][]Operation
	for _, obs := range observations {
		if obs.Status != "observed" {
			return nil
		}
		samples = append(samples, obs.Ops)
	}
	return samples
}

func findWriter(obs Observation, logID, segmentID int64) (Writer, bool) {
	for _, w := range obs.Writers {
		if w.LogID == logID && w.SegmentID == segmentID {
			return w, true
		}
	}
	return Writer{}, false
}

func writeWindow(report *Report, writers, metrics []Observation) string {
	if report.Mode != "live_window" || len(writers) < 2 || len(writers) != len(metrics) {
		return "insufficient_live_samples"
	}
	if writers[len(writers)-1].StartedAt.Sub(writers[0].FinishedAt).Milliseconds() < report.WindowMS || writers[len(writers)-1].StartedAt.Sub(writers[0].FinishedAt) <= 0 {
		return "observed_window_shorter_than_requested"
	}
	var start float64
	for i, obs := range writers {
		if obs.Status != "observed" || metrics[i].Status != "observed" {
			return "required_source_incomplete"
		}
		if metrics[i].ProcessStart == nil {
			return "process_identity_unavailable"
		}
		if i > 0 && start != *metrics[i].ProcessStart {
			return "process_restart_in_window"
		}
		start = *metrics[i].ProcessStart
	}
	return ""
}

func evaluateDisk(f *Finding, obs Observation) {
	f.NextChecks = []string{fmt.Sprintf("wp metrics snapshot %s", f.Scope.NodeID), fmt.Sprintf("wp config show %s", f.Scope.NodeID), "Check rejected-write evidence and client errors before inferring workload impact"}
	f.Summary = "Disk admission evidence is unavailable or ambiguous"
	if obs.Status != "observed" {
		f.Unknown = append(f.Unknown, "required_source_incomplete")
		return
	}
	values := map[string]float64{}
	usage := -1.0
	paths := map[string]bool{}
	for _, signal := range obs.Signals {
		if signal.Labels["node_id"] != f.Scope.NodeID {
			continue
		}
		if signal.TimestampMS != nil {
			f.Unknown = append(f.Unknown, "explicit_metric_timestamp_unverified")
			return
		}
		if signal.Name == "disk_usage_ratio" {
			path := signal.Labels["path"]
			if paths[path] {
				f.Unknown = append(f.Unknown, "ambiguous_disk_series")
				return
			}
			paths[path] = true
			if signal.Value > 1 {
				f.Unknown = append(f.Unknown, "invalid_disk_ratio")
				return
			}
			if signal.Value > usage {
				usage = signal.Value
			}
		} else {
			if _, ok := values[signal.Name]; ok {
				f.Unknown = append(f.Unknown, "ambiguous_disk_series")
				return
			}
			values[signal.Name] = signal.Value
		}
	}
	soft, s := values["disk_watermark_soft"]
	hard, h := values["disk_watermark_hard"]
	state, a := values["write_backpressure_state"]
	if len(paths) > 1 {
		f.Unknown = append(f.Unknown, "disk_policy_path_ambiguous")
		return
	}
	if s && h && soft == 0 && hard == 0 && (!a || state == 0) {
		f.Outcome, f.Severity = "policy_disabled", "none"
		f.Summary = "Exported soft/hard watermarks are both zero: disk watermark admission is disabled; this does not establish disk health"
		f.Supporting = appendIDs(f.Supporting, obs.ID)
		f.Unknown = append(f.Unknown, "client_impact_unobserved", "policy_gauge_refresh_time_unavailable")
		return
	}
	if !s || !h || !a || usage < 0 {
		f.Unknown = append(f.Unknown, "disk_signals_missing")
		return
	}
	f.Supporting = appendIDs(f.Supporting, obs.ID)
	if state != 0 && state != 1 && state != 2 || soft > 1 || hard > 1 || (soft != 0 || hard != 0) && (soft <= 0 || hard < soft) {
		f.Unknown = append(f.Unknown, "invalid_disk_policy_or_state")
		return
	}
	f.Summary = fmt.Sprintf("Current disk usage=%.4f, deployed soft=%.4f, hard=%.4f, admission state=%.0f; no duration or client impact is inferred", usage, soft, hard, state)
	expected := 0.0
	if hard > 0 && usage >= hard {
		expected = 2
	} else if soft > 0 && usage >= soft {
		expected = 1
	}
	if expected != state {
		if state == 2 && hard > 0 {
			f.Outcome, f.Severity, f.Strength = "observed_admission_pressure", "critical", "suspected"
			f.Unknown = append(f.Unknown, "absolute_free_floor_or_sampling_skew_unverified", "client_impact_unobserved", "policy_gauge_refresh_time_unavailable")
			return
		}
		f.Outcome, f.Strength = "conflicting", "conflicting"
		f.Unknown = append(f.Unknown, "disk_state_policy_disagree_or_sampling_skew")
		f.Contradicting = appendIDs(f.Contradicting, obs.ID)
		return
	}
	f.Severity = "none"
	if hard == 0 {
		f.Outcome = "policy_disabled"
	} else if state == 0 {
		f.Outcome = "below_configured_watermarks"
	} else {
		f.Outcome, f.Severity, f.Strength = "observed_admission_pressure", "warning", "suspected"
		if state == 2 {
			f.Severity = "critical"
		}
	}
	f.Unknown = append(f.Unknown, "current_sample_not_historical", "client_impact_unobserved", "policy_gauge_refresh_time_unavailable")
}
