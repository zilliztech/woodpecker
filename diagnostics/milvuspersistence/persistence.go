// Package milvuspersistence assigns persistence-step business meaning to existing source
// findings. It never re-evaluates histogram thresholds or fetches WP metadata.
package milvuspersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func Descriptor() d.StageDescriptor {
	return d.StageDescriptor{Command: "milvus persistence", Issue: 465, Sources: []string{"prometheus"}, Profiles: (prometheus.PersistenceModule{}).Descriptor().Profiles}
}

// Run uses the same config/selection/execution contracts as source commands.
// Assess can also be used on an already collected report, with no re-query.
func Run(ctx context.Context, config d.Config, r d.Request) d.Report {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.Limits.BudgetMS)*time.Millisecond)
	defer cancel()
	engine, _ := d.NewEngine(prometheus.PersistenceModule{})
	report := engine.Run(ctx, config, r)
	if report.Status == "unsupported" && len(report.Results) == 1 && len(report.Results[0].Problems) == 1 && report.Results[0].Problems[0].Code == "module_not_implemented" {
		report.Results[0].Problems[0] = d.Problem{Code: "stage_source_not_supported", Message: "Milvus persistence currently supports Prometheus only; other independent source commands are not persistence-stage inputs"}
	}
	if !d.Has([]string{"invalid", "unsupported", "disabled"}, report.Status) {
		a := Assess(ctx, report)
		report.Assessments = []d.Assessment{a}
		if a.Status == "failed" && report.Status == "completed" {
			report.Status = "failed"
		}
		if ctx.Err() != nil {
			report.Status = "cancelled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				report.Status = "budget_exceeded"
			}
		}
		report.FinishedAt = time.Now().UTC()
	}
	return report
}

type evidence struct {
	finding d.Finding
	fact    *prometheus.Fact
	data    prometheus.Dataset
}

func newAssessment(status string) d.Assessment {
	return d.Assessment{Profile: "milvus.persistence", Version: 1, Status: status, Findings: []d.Finding{}, Gaps: []d.Gap{}}
}

// Assess consumes the versioned source contract, preserving all source IDs.
// It deliberately has no connector/config argument and performs no I/O.
func Assess(ctx context.Context, report d.Report) d.Assessment {
	a := newAssessment(report.Status)
	if ctx.Err() != nil {
		a.Status = cancelStatus(ctx)
		return a
	}
	fail := func(code string) d.Assessment {
		failed := newAssessment("failed")
		failed.Gaps = append(failed.Gaps, d.Gap{Code: code, Detail: "Cannot assess the supported Prometheus persistence-step contract; source evidence remains unchanged"})
		return failed
	}
	if report.SchemaVersion != d.ReportSchema || report.Selection.Flow != "write" || report.Selection.Stage != "persistence" || !d.Has([]string{"milvus", "e2e"}, report.Selection.Scope) || len(report.Results) != 1 || len(report.Selection.Sources) != 1 || report.Selection.Sources[0] != "prometheus" {
		return fail("unsupported_stage_input")
	}
	source := report.Results[0]
	if source.Source != "prometheus" || source.Profile != report.Selection.Scope+".persistence.steps.metrics" {
		return fail("unsupported_source_profile")
	}
	var meta struct {
		Schema     string `json:"source_schema_version"`
		RunID      string `json:"source_run_id"`
		From, To   time.Time
		Deployment string `json:"bound_deployment"`
	}
	if json.Unmarshal(source.Metadata, &meta) != nil || meta.Schema != "wpdiag.prometheus/v1" || meta.RunID == "" || meta.Deployment == "" || meta.From.IsZero() || !meta.To.After(meta.From) {
		return fail("unsupported_source_schema_or_window")
	}
	w := report.Selection.Window
	if (w.Mode == "historical" && (w.From == nil || w.To == nil || !w.From.Equal(meta.From) || !w.To.Equal(meta.To))) || (w.Mode == "live" && w.DurationMS != meta.To.Sub(meta.From).Milliseconds()) || !d.Has([]string{"historical", "live"}, w.Mode) || (report.Selection.Target.Deployment != "" && report.Selection.Target.Deployment != meta.Deployment) {
		return fail("source_selection_window_or_deployment_mismatch")
	}
	observations := map[string]d.Observation{}
	datasets := map[string]prometheus.Dataset{}
	for _, o := range source.Observations {
		if ctx.Err() != nil {
			a.Status = cancelStatus(ctx)
			return a
		}
		if o.ID == "" {
			return fail("missing_observation_identity")
		}
		if old, ok := observations[o.ID]; ok {
			if !reflect.DeepEqual(old, o) {
				return fail("conflicting_observation_identity")
			}
			a.Gaps = append(a.Gaps, d.Gap{Code: "duplicate_observation_deduplicated", ObservationID: o.ID})
			continue
		}
		if !strings.HasPrefix(o.ID, meta.RunID+":q") || o.Target.Deployment != meta.Deployment {
			return fail("observation_run_or_deployment_mismatch")
		}
		observations[o.ID] = o
		if o.Status != "observed" {
			continue
		}
		var data prometheus.Dataset
		if o.Kind != "prometheus_matrix" || json.Unmarshal(o.Data, &data) != nil || !data.From.Equal(meta.From) || !data.To.Equal(meta.To) {
			return fail("invalid_observation_contract")
		}
		if o.SourceID != "prometheus:"+data.Signal || o.Query == nil || !o.Query.Executed || o.Query.Language != "promql" {
			return fail("observation_source_or_query_mismatch")
		}
		datasets[o.ID] = data
	}
	all := []evidence{}
	seen := map[string]d.Finding{}
	for _, f := range source.Findings {
		if ctx.Err() != nil {
			a.Status = cancelStatus(ctx)
			return a
		}
		if old, ok := seen[f.ID]; ok {
			if !reflect.DeepEqual(old, f) {
				return fail("conflicting_source_finding")
			}
			a.Gaps = append(a.Gaps, d.Gap{Code: "duplicate_source_finding_deduplicated", Detail: f.ID})
			continue
		}
		seen[f.ID] = f
		if f.ID == "" || f.RuleVersion != 1 || f.Flow != "write" || f.Stage != "persistence" || len(f.Supporting) != 1 || len(f.Contradicting) != 0 {
			return fail("unsupported_source_finding_contract")
		}
		o := observations[f.Supporting[0]]
		data, ok := datasets[o.ID]
		if !ok || o.Status != "observed" || !strings.HasPrefix(f.RuleID, data.Signal+".") {
			return fail("dangling_or_wrong_source_reference")
		}
		known := false
		for _, sig := range prometheus.PersistenceCatalog() {
			known = known || sig.ID == data.Signal && (report.Selection.Scope == "e2e" || !strings.HasPrefix(sig.ID, "wp."))
		}
		if !known || !d.Has([]string{data.Signal + ".latency", data.Signal + ".gauge", data.Signal + ".throughput_change"}, f.RuleID) || !d.Has([]string{"inconclusive", "suspected_high_latency", "within_configured_latency_for_observed_completions", "idle_or_no_completions", "observed_throughput_drop", "gauge_observations_only"}, f.Outcome) {
			return fail("unsupported_source_rule_or_outcome")
		}
		item := evidence{finding: f, data: data}
		for i := range data.Facts {
			fact := data.Facts[i]
			if f.ID == o.ID+":"+fact.Population {
				copyFact := fact
				item.fact = &copyFact
				break
			}
		}
		if d.Has([]string{"suspected_high_latency", "within_configured_latency_for_observed_completions", "gauge_observations_only"}, f.Outcome) {
			if item.fact == nil || item.fact.ObservedFromMS <= meta.From.UnixMilli() || item.fact.ObservedToMS > meta.To.UnixMilli() || item.fact.ObservedFromMS >= item.fact.ObservedToMS || item.fact.Labels["instance"] != f.Target.Instance || item.fact.Labels["node_id"] != f.Target.NodeID || f.Target.Instance == "" || f.Target.Deployment != meta.Deployment || (report.Selection.Target.Instance != "" && f.Target.Instance != report.Selection.Target.Instance) {
				return fail("invalid_fact_identity_or_time")
			}
		}
		if item.fact != nil && item.fact.Gauge != nil && !validSamples(*item.fact) {
			return fail("invalid_gauge_sample_contract")
		}
		all = append(all, item)
	}
	for _, entry := range prometheus.PersistenceCatalog() {
		if report.Selection.Scope == "milvus" && strings.HasPrefix(entry.ID, "wp.") {
			continue
		}
		found := false
		for _, item := range all {
			if ctx.Err() != nil {
				a.Status = cancelStatus(ctx)
				return a
			}
			if item.data.Signal != entry.ID || strings.HasSuffix(item.finding.RuleID, ".throughput_change") {
				continue
			}
			found = true
			derived := interpret(item, all)
			a.Findings = append(a.Findings, derived)
			for _, code := range derived.Unknown {
				if d.Has([]string{"related_population_missing", "related_population_ambiguous", "actual_samples_not_aligned", "frontier_reset_or_restart", "timestamp_uninitialized_or_clock_incompatible", "wp_maintenance_mapping_unavailable"}, code) {
					a.Gaps = append(a.Gaps, d.Gap{Code: code, Detail: derived.ID})
				}
			}
		}
		if !found {
			a.Gaps = append(a.Gaps, d.Gap{Code: "step_evidence_unavailable", Detail: entry.ID + " has no usable source finding; inspect source errors/coverage"})
		}
	}
	if report.Selection.Scope == "e2e" {
		a.Gaps = append(a.Gaps, d.Gap{Code: "wp_maintenance_mapping_unavailable", Detail: "No authoritative, time-valid Milvus instance/collection/channel to WP-client metadata namespace/log/segment/node mapping was collected. WP flush/offload observations are parallel unassociated context, not a serial Milvus persistence step"})
	}
	a.Gaps = append(a.Gaps, d.Gap{Code: "optional_sources_not_selected", Detail: "Only Prometheus was queried. Native write/flush/disk rules remain owned by wp diagnose --scenario write (#452); native/trace/Loki enrichment was not collected and unsupported requested sources fail explicitly"})
	return a
}

func cancelStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "budget_exceeded"
	}
	return "cancelled"
}

func validSamples(f prometheus.Fact) bool {
	samples := f.Gauge.Samples
	if len(samples) < 2 || samples[0].TimeMS != f.ObservedFromMS || samples[len(samples)-1].TimeMS != f.ObservedToMS {
		return false
	}
	for i, p := range samples {
		if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) || p.Value < 0 || i > 0 && p.TimeMS <= samples[i-1].TimeMS {
			return false
		}
	}
	g := f.Gauge
	minimum, peak := samples[0].Value, samples[0].Value
	for _, p := range samples {
		minimum = min(minimum, p.Value)
		peak = max(peak, p.Value)
	}
	if g.First != samples[0].Value || g.Last != samples[len(samples)-1].Value || g.Minimum != minimum || g.Peak != peak || g.LastTwoZero != (samples[len(samples)-1].Value == 0 && samples[len(samples)-2].Value == 0) {
		return false
	}
	return true
}

func interpret(item evidence, all []evidence) d.Finding {
	f := item.finding
	f.ID = item.finding.ID + ":milvus-persistence"
	f.RuleID = "milvus.persistence.step." + item.data.Signal
	f.DerivedFrom = []string{item.finding.ID}
	f.Supporting = append([]string{}, f.Supporting...)
	f.Unknown = append([]string{}, f.Unknown...)
	label, next := stepMeaning(item.data.Signal)
	f.Summary = label + ": " + f.Summary
	f.NextChecks = []string{next}
	f.Unknown = append(f.Unknown, "independent_metric_populations_not_one_request", "overlapping_timers_do_not_add_or_subtract_quantiles", "client_latency_and_crash_durability_not_measured")
	if item.fact != nil {
		labels := []string{}
		for _, key := range []string{"component", "channel_name", "term", "state", "lv", "msg_type", "persistent_data_op_type", "log_ns", "log_id", "operation", "status"} {
			if value := item.fact.Labels[key]; value != "" {
				labels = append(labels, fmt.Sprintf("%s=%q", key, value))
			}
		}
		if len(labels) > 0 {
			f.Summary += " [" + strings.Join(labels, ", ") + "]"
		}
	}
	if f.Outcome == "suspected_high_latency" {
		f.Outcome = map[string]string{"milvus.persistence.queue": "observed_slow_sync_queue_completions", "milvus.persistence.execute": "observed_slow_sync_execution", "milvus.persistence.save": "observed_slow_save_completions", "milvus.persistence.put": "observed_slow_storage_put_completions", "wp.flush": "observed_slow_wp_local_flush", "wp.compaction": "observed_slow_wp_compaction", "wp.storage": "observed_slow_wp_storage_completions"}[item.data.Signal]
		if item.data.Signal == "milvus.persistence.queue" {
			queuePressure(&f, item, all)
		}
	}
	if item.fact != nil && item.fact.Gauge != nil {
		gaugeMeaning(&f, item, all)
	}
	if strings.HasPrefix(item.data.Signal, "wp.") {
		f.Unknown = append(f.Unknown, "wp_maintenance_mapping_unavailable")
		f.Summary += "; WP parallel context, not mapped to a Milvus request or persistence channel"
	}
	return f
}

func related(f *d.Finding, item evidence, all []evidence, id string, align bool) *evidence {
	matches := []evidence{}
	for _, other := range all {
		if other.data.Signal == id && other.fact != nil && other.fact.Gauge != nil && other.fact.Population == item.fact.Population {
			matches = append(matches, other)
		}
	}
	if len(matches) != 1 {
		code := "related_population_missing"
		if len(matches) > 1 {
			code = "related_population_ambiguous"
		}
		f.Unknown = append(f.Unknown, code)
		return nil
	}
	other := matches[0]
	if item.fact.ObservedFromMS != other.fact.ObservedFromMS || item.fact.ObservedToMS != other.fact.ObservedToMS {
		f.Unknown = append(f.Unknown, "actual_samples_not_aligned")
		return nil
	}
	if align {
		left, right := item.fact.Gauge.Samples, other.fact.Gauge.Samples
		if len(left) != len(right) {
			f.Unknown = append(f.Unknown, "actual_samples_not_aligned")
			return nil
		}
		for i := range left {
			if left[i].TimeMS != right[i].TimeMS {
				f.Unknown = append(f.Unknown, "actual_samples_not_aligned")
				return nil
			}
		}
	}
	return &other
}

func include(f *d.Finding, other evidence, contradiction bool) {
	f.DerivedFrom = appendUnique(f.DerivedFrom, other.finding.ID)
	if contradiction {
		f.Contradicting = appendUnique(f.Contradicting, other.finding.Supporting...)
	} else {
		f.Supporting = appendUnique(f.Supporting, other.finding.Supporting...)
	}
}

func queuePressure(f *d.Finding, item evidence, all []evidence) {
	pending := related(f, item, all, "milvus.persistence.pending", false)
	if pending == nil {
		return
	}
	g := pending.fact.Gauge
	f.Unknown = append(f.Unknown, "pending_tasks_include_executing_work_not_just_queue", "pending_is_not_configured_capacity")
	if g.Minimum > 0 {
		f.Outcome = "suspected_sync_scheduling_pressure"
		include(f, *pending, false)
	} else if g.Peak == 0 || g.LastTwoZero {
		f.Outcome = "slow_queue_completions_without_persistent_pending_work"
		if g.Peak > 0 {
			f.Outcome = "slow_queue_completions_with_pending_recovery"
		}
		include(f, *pending, true)
		f.Strength = "inconclusive"
		f.Severity = "unknown"
		f.Summary += "; pending observations contradict sustained current pressure, without disproving earlier delay"
	} else {
		include(f, *pending, false)
		f.Unknown = append(f.Unknown, "intermittent_pending_work_not_continuous_pressure")
	}
}

func gaugeMeaning(f *d.Finding, item evidence, all []evidence) {
	g := item.fact.Gauge
	f.Strength = "observed_exporter_state"
	f.Severity = "unknown"
	switch item.data.Signal {
	case "milvus.persistence.growing":
		f.Outcome = "growing_bytes_observed"
		f.Unknown = append(f.Unknown, "growing_bytes_are_not_flush_pressure_or_stall")
	case "milvus.persistence.pressure":
		pressureMeaning(f, item, all)
	case "milvus.persistence.hwm":
		f.Outcome = "configured_growing_hwm_observed"
	case "milvus.persistence.flusher":
		f.Outcome = "flusher_state_observed"
		f.Unknown = append(f.Unknown, "state_indicator_not_completion_or_durable_checkpoint")
	case "milvus.persistence.pending":
		f.Outcome = "intermittent_pending_sync_work_observed"
		if g.Peak == 0 {
			f.Outcome = "no_pending_sync_work_observed"
		} else if g.LastTwoZero {
			f.Outcome = "pending_sync_recovery_observed"
		} else if g.Minimum > 0 {
			f.Outcome = "persistent_pending_sync_work_observed"
		}
		f.Unknown = append(f.Unknown, "pending_tasks_include_executing_work_not_just_queue", "expected_traffic_and_capacity_not_established")
	case "milvus.persistence.busy":
		f.Outcome = "recovery_persisting_state_observed"
		f.Unknown = append(f.Unknown, "persisting_state_not_snapshot_completion")
	case "milvus.persistence.seen", "milvus.persistence.memory", "milvus.persistence.persisted", "milvus.persistence.checkpoint":
		frontierMeaning(f, item, all)
	case "wp.disk_state":
		f.Outcome = "no_wp_disk_admission_pressure_observed"
		if g.Peak > 0 {
			f.Outcome = "exported_wp_disk_admission_pressure_observed"
			f.Severity = "warning"
		}
		if g.Peak > 0 && g.LastTwoZero {
			f.Outcome = "wp_disk_admission_recovery_observed"
			f.Severity = "unknown"
		}
		f.Unknown = append(f.Unknown, "exported_0_normal_1_warn_2_blocked_not_latency_root_cause")
	}
}

func pressureMeaning(f *d.Finding, item evidence, all []evidence) {
	f.Outcome = "growing_flush_pressure_bytes_observed"
	hwm := related(f, item, all, "milvus.persistence.hwm", true)
	if hwm == nil {
		return
	}
	left, right := item.fact.Gauge.Samples, hwm.fact.Gauge.Samples
	above := 0
	for i := range left {
		if right[i].Value <= 0 {
			f.Unknown = append(f.Unknown, "hwm_uninitialized_or_disabled")
			return
		}
		if left[i].Value >= right[i].Value {
			above++
		}
	}
	include(f, *hwm, false)
	f.Outcome = "below_configured_growing_hwm_observed"
	if above == len(left) {
		f.Outcome = "suspected_growing_flush_pressure"
		f.Severity = "warning"
		f.Strength = "suspected"
	} else if above > 0 {
		f.Outcome = "intermittent_growing_hwm_pressure_observed"
		n := len(left)
		if n >= 2 && left[n-1].Value < right[n-1].Value && left[n-2].Value < right[n-2].Value {
			f.Outcome = "growing_pressure_below_hwm_after_breach"
			f.Unknown = append(f.Unknown, "below_hwm_is_not_full_hysteresis_recovery")
		}
	}
	f.Unknown = append(f.Unknown, "sampled_configured_policy_comparison_not_disk_cause")
}

func frontierValid(g *prometheus.GaugeFact) string {
	for i, p := range g.Samples {
		if p.Value == 0 || p.Value > float64(p.TimeMS)/1000 {
			return "timestamp_uninitialized_or_clock_incompatible"
		}
		if i > 0 && p.Value < g.Samples[i-1].Value {
			return "frontier_reset_or_restart"
		}
	}
	return ""
}

func frontierMeaning(f *d.Finding, item evidence, all []evidence) {
	g := item.fact.Gauge
	if code := frontierValid(g); code != "" {
		f.Outcome = "inconclusive"
		f.Strength = "inconclusive"
		f.Unknown = append(f.Unknown, code)
		return
	}
	f.Outcome = "frontier_unchanged_observed"
	if g.Last > g.First {
		f.Outcome = "frontier_advanced_observed"
	}
	switch item.data.Signal {
	case "milvus.persistence.seen":
		f.Unknown = append(f.Unknown, "flusher_seen_timetick_is_not_durable_checkpoint")
	case "milvus.persistence.memory":
		f.Unknown = append(f.Unknown, "in_memory_timetick_is_not_persisted_checkpoint_or_payload_demand")
	case "milvus.persistence.checkpoint":
		f.Summary += fmt.Sprintf("; checkpoint age at final retained scrape %.3fs", float64(g.Samples[len(g.Samples)-1].TimeMS)/1000-g.Last)
		f.Unknown = append(f.Unknown, "idle_checkpoint_age_not_stall", "datacoord_virtual_channel_not_joined_to_physical_wal_channel")
	case "milvus.persistence.persisted":
		f.Unknown = append(f.Unknown, "snapshot_frontier_not_user_acknowledgement_or_payload_verification")
		if item.fact.Labels["channel_name"] == "" || item.fact.Labels["term"] == "" {
			f.Unknown = append(f.Unknown, "channel_term_identity_missing")
			return
		}
		memory := related(f, item, all, "milvus.persistence.memory", true)
		if memory == nil {
			return
		}
		if code := frontierValid(memory.fact.Gauge); code != "" {
			f.Unknown = append(f.Unknown, code)
			return
		}
		left, right := g.Samples, memory.fact.Gauge.Samples
		lagAll := true
		for i := range left {
			if left[i].Value > right[i].Value {
				include(f, *memory, true)
				f.Outcome = "inconclusive"
				f.Unknown = append(f.Unknown, "persisted_frontier_exceeds_in_memory")
				return
			}
			lagAll = lagAll && left[i].Value < right[i].Value
		}
		include(f, *memory, false)
		n := len(left)
		if left[n-1].Value == right[n-1].Value && left[n-2].Value == right[n-2].Value {
			f.Outcome = "recovery_frontiers_equal_at_final_scrapes"
			f.Unknown = append(f.Unknown, "physical_seconds_equality_not_complete_log_durability")
			return
		}
		if g.Last > g.First {
			f.Outcome = "persisted_recovery_frontier_advanced"
			return
		}
		f.Outcome = "persisted_recovery_frontier_unchanged"
		f.Unknown = append(f.Unknown, "idle_or_snapshot_cadence_can_explain_unchanged_frontier")
		if !lagAll || memory.fact.Gauge.Last <= memory.fact.Gauge.First {
			return
		}
		busy := related(f, item, all, "milvus.persistence.busy", true)
		if busy == nil {
			return
		}
		if busy.fact.Gauge.Minimum == 1 && busy.fact.Gauge.Peak == 1 {
			include(f, *busy, false)
			f.Outcome = "suspected_recovery_snapshot_persistence_delay"
			f.Strength = "suspected"
			f.Severity = "warning"
			f.Unknown = append(f.Unknown, "snapshot_duration_and_configured_cadence_not_established", "not_a_stall_or_storage_failure_verdict")
		} else {
			include(f, *busy, true)
			f.Unknown = append(f.Unknown, "persisting_not_continuous_or_recovered")
		}
	}
}

func appendUnique(values []string, extra ...string) []string {
	for _, v := range extra {
		if !d.Has(values, v) {
			values = append(values, v)
		}
	}
	return values
}

func stepMeaning(id string) (string, string) {
	switch id {
	case "milvus.persistence.growing", "milvus.persistence.pressure", "milvus.persistence.hwm":
		return "1 Growing data / configured pressure", "Compare flush-pressure bytes with the same node's actual HWM; growing bytes alone can reflect ordinary batching. Check configured LWM/hysteresis and input traffic separately"
	case "milvus.persistence.flusher", "milvus.persistence.pending", "milvus.persistence.queue":
		return "2 Flusher state / sync scheduling", "Inspect flusher recovery/working state and same-exporter pending work; pending includes queued and executing tasks, not configured capacity"
	case "milvus.persistence.execute", "milvus.persistence.save", "milvus.persistence.put":
		return "3 Sync execution / upload / callbacks", "Compare independent sync, save and successful storage-put populations; execution includes callbacks, while put timing does not isolate network/server time. Use bounded logs/trace to identify a concrete failing step"
	case "milvus.persistence.seen", "milvus.persistence.memory", "milvus.persistence.persisted", "milvus.persistence.busy", "milvus.persistence.checkpoint":
		return "4 Persisted frontier / checkpoint", "Check expected data, snapshot cadence and channel/term ownership. Flusher-seen and in-memory markers are not durable checkpoints; idle age and missing lazy metrics are not stalls"
	default:
		return "5 Parallel WP maintenance (unassociated context)", "Use wp diagnose --scenario write for native flush/disk/replica evidence. Resolve the authoritative client metadata namespace/log/node mapping before associating WP with Milvus; inspect bounded compaction/offload ops and storage status"
	}
}
