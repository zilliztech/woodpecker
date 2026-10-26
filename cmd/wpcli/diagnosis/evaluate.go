package diagnosis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

func opKey(op Operation) string {
	body, _ := json.Marshal(op)
	return string(body)
}

// PersistentOperations requires the same identity in every supplied sample.
// Duplicate records never count as independent observations.
func PersistentOperations(samples [][]Operation, ruleID string) []Operation {
	return persistentOperations(context.Background(), samples, ruleID)
}

func persistentOperations(ctx context.Context, samples [][]Operation, ruleID string) []Operation {
	if len(samples) < 2 {
		return nil
	}
	first := map[string]Operation{}
	for i, op := range samples[0] {
		if i%128 == 0 && ctx.Err() != nil {
			return nil
		}
		if op.ID != "" && MatchesOperation(ruleID, op.Type) {
			first[opKey(op)] = op
		}
	}
	for _, ops := range samples[1:] {
		seen := map[string]bool{}
		for i, op := range ops {
			if i%128 == 0 && ctx.Err() != nil {
				return nil
			}
			seen[opKey(op)] = true
		}
		for key := range first {
			if !seen[key] {
				delete(first, key)
			}
		}
	}
	result := make([]Operation, 0, len(first))
	seen := map[string]bool{}
	// The registry's final ordering prioritizes its oldest operations. Preserve
	// that order for the legacy report while deduplicating repeated records.
	for _, op := range samples[len(samples)-1] {
		key := opKey(op)
		if _, ok := first[key]; ok && !seen[key] {
			result = append(result, op)
			seen[key] = true
		}
	}
	return result
}

// Evaluate applies the selected typed native rules. A completed report
// with an inconclusive outcome is never an assertion that the target is healthy.
func Evaluate(report *Report) {
	EvaluateContext(context.Background(), report)
}

// EvaluateContext includes post-collection rule work in the caller's budget.
func EvaluateContext(ctx context.Context, report *Report) {
	defer func() {
		if ctx.Err() != nil {
			report.Status = "cancelled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				report.Status = "budget_exceeded"
			}
		}
	}()
	report.Findings = []Finding{}
	conflicts := deduplicate(ctx, report)
	for _, target := range report.Targets {
		byRound := map[int]map[string]Observation{}
		for _, obs := range report.Observations {
			if obs.NodeID != target.NodeID {
				continue
			}
			if byRound[obs.Round] == nil {
				byRound[obs.Round] = map[string]Observation{}
			}
			byRound[obs.Round][obs.Kind] = obs
		}
		rounds := make([]int, 0, len(byRound))
		for round := range byRound {
			rounds = append(rounds, round)
		}
		sort.Ints(rounds)
		var metrics, ops, stats []Observation
		versionUnsupported := false
		for _, round := range rounds {
			metrics = append(metrics, byRound[round]["metrics"])
			ops = append(ops, byRound[round]["ops"])
			stats = append(stats, byRound[round]["registry"])
			build := byRound[round]["build"]
			if build.Code == "unsupported_version" {
				versionUnsupported = true
			}
		}
		for _, rule := range report.Rules {
			if rule.Scenario == "write" {
				evaluateWriteRule(ctx, report, target, rule, byRound, rounds, conflicts[target.NodeID], versionUnsupported)
				continue
			}
			if rule.Scenario == "read" {
				if rule.ID == "read_activity" {
					evaluateReadActivity(ctx, report, target, rule, byRound, rounds, conflicts[target.NodeID], versionUnsupported)
				} else if report.Targets[0].NodeID == target.NodeID {
					evaluateReaderProgress(ctx, report, rule, conflicts)
				}
				continue
			}
			base := Finding{
				RuleID: rule.ID, RuleVersion: rule.Version, Flow: rule.Flow, Stage: rule.Stage,
				Scope:   Scope{NodeID: target.NodeID, Tenant: "unresolved: ops has no tenant namespace"},
				Outcome: "inconclusive", Severity: "unknown", Strength: "inconclusive",
				Supporting: []string{}, Contradicting: []string{}, Unknown: []string{},
				NextChecks: []string{
					fmt.Sprintf("wp ops list %s", target.NodeID), fmt.Sprintf("wp ops stats %s", target.NodeID),
					"Select the authoritative WP-client metadata namespace before joining tenant/log state",
				},
			}
			if ctx.Err() != nil {
				base.Summary = "Evaluation interrupted; collected evidence remains available"
				base.Unknown = append(base.Unknown, "evaluation_interrupted")
				appendFinding(report, base)
				continue
			}
			if report.Mode == "snapshot" || len(rounds) < rule.MinimumSamples {
				base.Summary = "A snapshot or incomplete window cannot establish sustained no-progress"
				base.Unknown = append(base.Unknown, "insufficient_live_samples")
				appendFinding(report, base)
				continue
			}
			if versionUnsupported || conflicts[target.NodeID] {
				base.Summary = "Unsupported build or contradictory observations prevent a reliable evaluation"
				base.Unknown = append(base.Unknown, "unsupported_version_or_conflicting_observations")
				appendFinding(report, base)
				continue
			}
			valid := true
			var opSamples [][]Operation
			for i := range rounds {
				if metrics[i].Status != "observed" || ops[i].Status != "observed" {
					valid = false
				}
				opSamples = append(opSamples, ops[i].Ops)
				base.Supporting = appendIDs(base.Supporting, ops[i].ID)
			}
			if !valid {
				base.Summary = "Required metrics/ops collection is incomplete; absence is not zero progress"
				base.Unknown = append(base.Unknown, "required_source_incomplete")
				appendFinding(report, base)
				continue
			}
			base.Unknown = append(base.Unknown, registryGaps(stats, ops, report.Limits.MaxOps)...)
			startMissing, restarted := false, false
			var processStart *float64
			for _, obs := range metrics {
				startMissing = startMissing || obs.ProcessStart == nil
				if processStart != nil && obs.ProcessStart != nil && *processStart != *obs.ProcessStart {
					restarted = true
				}
				if obs.ProcessStart != nil {
					processStart = obs.ProcessStart
				}
			}
			if restarted {
				base.Summary = "Process start identity changed during the window; progress comparison is inconclusive"
				base.Unknown = append(base.Unknown, "process_restart_in_window")
				appendFinding(report, base)
				continue
			}
			if startMissing {
				base.Unknown = append(base.Unknown, "process_identity_unavailable")
			}
			if ops[len(ops)-1].StartedAt.Sub(ops[0].FinishedAt) <= 0 {
				base.Summary = "Distinct ops sampling times are required for a live window"
				base.Unknown = append(base.Unknown, "non_increasing_observation_time")
				appendFinding(report, base)
				continue
			}
			if ops[len(ops)-1].StartedAt.Sub(ops[0].FinishedAt).Milliseconds() < report.WindowMS {
				base.Summary = "Collected ops evidence does not span the requested live interval"
				base.Unknown = append(base.Unknown, "observed_window_shorter_than_requested")
				appendFinding(report, base)
				continue
			}
			persistent := persistentOperations(ctx, opSamples, rule.ID)
			if ctx.Err() != nil {
				base.Summary = "Evaluation interrupted; collected evidence remains available"
				base.Unknown = append(base.Unknown, "evaluation_interrupted")
				appendFinding(report, base)
				continue
			}
			if len(persistent) == 0 {
				base.Severity = "none"
				base.Outcome = "no_persistent_operation_observed"
				base.Summary = "No matching operation persisted through all samples; this does not establish health or completion"
				matching := false
				for _, sample := range opSamples {
					for _, op := range sample {
						matching = matching || MatchesOperation(rule.ID, op.Type)
					}
				}
				if !matching {
					base.Outcome = "no_pending_work_observed"
					base.Summary = "No pending work for this operation family was observed; idle/tailing is not a stall"
				}
				if len(base.Unknown) > 0 {
					base.Severity = "unknown"
					base.Outcome = "inconclusive"
					base.Summary = "Registry bounds/eviction or missing coverage prevent excluding unobserved work"
				}
				appendFinding(report, base)
				continue
			}
			type progressResult struct {
				progressed bool
				problem    string
			}
			progressByLog := map[string]progressResult{}
			for _, op := range persistent {
				if ctx.Err() != nil {
					base.Summary = "Evaluation interrupted; collected evidence remains available"
					base.Unknown = append(base.Unknown, "evaluation_interrupted")
					appendFinding(report, base)
					break
				}
				f := base
				f.Unknown = append([]string{}, base.Unknown...)
				f.Scope.LogID, f.Scope.SegmentID, f.Scope.Operation = op.LogID, op.SegmentID, op.ID
				f.Scope.TraceID = op.TraceID
				f.Unknown = append(f.Unknown, "tenant_association_unknown", "sampled_not_continuous", "metrics_and_ops_not_atomic")
				key := "unknown"
				if op.LogID != nil {
					key = strconv.FormatInt(*op.LogID, 10)
				}
				result, cached := progressByLog[key]
				if !cached {
					result.progressed, result.problem = compareProgress(ctx, metrics, rule.ID, target.NodeID, op.LogID)
					progressByLog[key] = result
				}
				progress, problem := result.progressed, result.problem
				for _, obs := range metrics {
					if progress && problem == "" {
						f.Contradicting = appendIDs(f.Contradicting, obs.ID)
					} else {
						f.Supporting = appendIDs(f.Supporting, obs.ID)
					}
				}
				switch {
				case problem != "":
					f.Summary = "The operation remained pending, but completion progress cannot be assessed: " + problem
					f.Unknown = append(f.Unknown, problem)
				case progress:
					f.Outcome, f.Strength = "conflicting", "conflicting"
					f.Summary = "Operation remained pending while matching-log completions (including read EOF) advanced; progress cannot be attributed to this operation or tenant"
				case !progress:
					f.Outcome, f.Strength, f.Severity = "suspected_stall", "suspected", "warning"
					f.Summary = "Same operation persisted with no observed matching-log completion increase; investigate, not a confirmed root cause"
				}
				appendFinding(report, f)
			}
		}
	}
}

func appendFinding(report *Report, f Finding) {
	f.ID = fmt.Sprintf("%s:findings:%d", report.RunID, len(report.Findings))
	report.Findings = append(report.Findings, f)
	for _, code := range f.Unknown {
		report.Gaps = append(report.Gaps, Gap{Code: code, NodeID: f.Scope.NodeID, Detail: "Applies to finding " + f.ID})
	}
}

func appendIDs(ids []string, id string) []string {
	if id == "" {
		return ids
	}
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}

func registryGaps(stats, ops []Observation, maxOps int) []string {
	gaps := map[string]bool{}
	var previous *Registry
	for i, obs := range stats {
		if obs.Status != "observed" || obs.Registry == nil {
			gaps["registry_coverage_unknown"] = true
			continue
		}
		r := obs.Registry
		if r.InUse >= maxOps || len(ops[i].Ops) >= maxOps {
			gaps["registry_listing_may_be_truncated"] = true
		}
		if r.InUse > len(ops[i].Ops) {
			gaps["registry_listing_incomplete_or_skewed"] = true
		}
		if previous != nil && r.Evicted != previous.Evicted {
			gaps["registry_eviction_or_reset_in_window"] = true
		}
		previous = r
	}
	result := make([]string, 0, len(gaps))
	for gap := range gaps {
		result = append(result, gap)
	}
	sort.Strings(result)
	return result
}

func compareProgress(ctx context.Context, metrics []Observation, ruleID, node string, logID *int64) (bool, string) {
	if logID == nil {
		return false, "log_identity_unknown"
	}
	progress := false
	var previous map[string]float64
	var start *float64
	for _, obs := range metrics {
		if start != nil && obs.ProcessStart != nil && *start != *obs.ProcessStart {
			return false, "process_restart_in_window"
		}
		if obs.ProcessStart != nil {
			start = obs.ProcessStart
		}
		current := map[string]float64{}
		for i, counter := range obs.Counters {
			if i%128 == 0 && ctx.Err() != nil {
				return false, "evaluation_interrupted"
			}
			l := counter.Labels
			if l["node_id"] != node || l["log_id"] != strconv.FormatInt(*logID, 10) || !MatchesOperation(ruleID, "logstore."+l["operation"]) {
				continue
			}
			if l["status"] != "success" && !(ruleID == "read_stall" && l["status"] == "end_of_file") {
				continue
			}
			if counter.TimestampMS != nil {
				// Explicit exporter timestamps may describe cached/past data or a
				// different clock. Do not relabel it as a current native snapshot.
				return false, "explicit_metric_timestamp_unverified"
			}
			keyBytes, _ := json.Marshal(l)
			key := string(keyBytes)
			if value, ok := current[key]; ok && value != counter.Value {
				return false, "conflicting_duplicate_counter_series"
			}
			current[key] = counter.Value
		}
		if len(current) == 0 {
			return false, "matching_completion_series_absent"
		}
		if previous != nil {
			if len(current) != len(previous) {
				return false, "completion_series_changed"
			}
			for key, value := range current {
				before, ok := previous[key]
				if !ok {
					return false, "completion_series_changed"
				}
				if value < before {
					return false, "completion_counter_reset"
				}
				progress = progress || value > before
			}
		}
		previous = current
	}
	return progress, ""
}

func deduplicate(ctx context.Context, report *Report) map[string]bool {
	conflicts := map[string]bool{}
	seen := map[string]Observation{}
	retained := []Observation{}
	for i, obs := range report.Observations {
		if ctx.Err() != nil {
			retained = append(retained, report.Observations[i:]...)
			break
		}
		key := fmt.Sprintf("%s:%d:%s", obs.NodeID, obs.Round, obs.Kind)
		canonical := obs
		canonical.ID = ""
		canonical.SourceID = ""
		if previous, ok := seen[key]; ok {
			if !reflect.DeepEqual(previous, canonical) {
				conflicts[obs.NodeID] = true
				report.Gaps = append(report.Gaps, Gap{
					Code: "conflicting_duplicate_observation", NodeID: obs.NodeID,
					ObservationID: obs.ID, Detail: "Same node/round/source supplied contradictory observations",
				})
			}
			continue
		}
		seen[key] = canonical
		retained = append(retained, obs)
	}
	report.Observations = retained
	return conflicts
}
