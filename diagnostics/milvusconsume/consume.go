// Package milvusconsume assigns consume-step business meaning to existing source
// findings. It never re-evaluates histogram thresholds or fetches WP metadata.
package milvusconsume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func Descriptor() d.StageDescriptor {
	return d.StageDescriptor{Command: "milvus consume", Issue: 464, Sources: []string{"prometheus"}, Profiles: (prometheus.ConsumeModule{}).Descriptor().Profiles}
}

// Run uses the same config/selection/execution contracts as source commands.
// Assess can also be used on an already collected report, with no re-query.
func Run(ctx context.Context, config d.Config, r d.Request) d.Report {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.Limits.BudgetMS)*time.Millisecond)
	defer cancel()
	engine, _ := d.NewEngine(prometheus.ConsumeModule{})
	report := engine.Run(ctx, config, r)
	if report.Status == "unsupported" && len(report.Results) == 1 && len(report.Results[0].Problems) == 1 && report.Results[0].Problems[0].Code == "module_not_implemented" {
		report.Results[0].Problems[0] = d.Problem{Code: "stage_source_not_supported", Message: "Milvus consume currently supports Prometheus only; other independent source commands are not consume-stage inputs"}
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
	return d.Assessment{Profile: "milvus.consume", Version: 1, Status: status, Findings: []d.Finding{}, Gaps: []d.Gap{}}
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
		failed.Gaps = append(failed.Gaps, d.Gap{Code: code, Detail: "Cannot assess the supported Prometheus consume-step contract; source evidence remains unchanged"})
		return failed
	}
	if report.SchemaVersion != d.ReportSchema || report.Selection.Flow != "read" || report.Selection.Stage != "consume" || !d.Has([]string{"milvus", "e2e"}, report.Selection.Scope) || len(report.Results) != 1 || len(report.Selection.Sources) != 1 || report.Selection.Sources[0] != "prometheus" {
		return fail("unsupported_stage_input")
	}
	source := report.Results[0]
	if source.Source != "prometheus" || source.Profile != report.Selection.Scope+".consume.steps.metrics" {
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
		if f.ID == "" || f.RuleVersion != 1 || f.Flow != "read" || f.Stage != "consume" || len(f.Supporting) != 1 || len(f.Contradicting) != 0 {
			return fail("unsupported_source_finding_contract")
		}
		o := observations[f.Supporting[0]]
		data, ok := datasets[o.ID]
		if !ok || o.Status != "observed" || !strings.HasPrefix(f.RuleID, data.Signal+".") {
			return fail("dangling_or_wrong_source_reference")
		}
		known := false
		for _, sig := range prometheus.ConsumeCatalog() {
			known = known || sig.ID == data.Signal && (report.Selection.Scope == "e2e" || !strings.HasPrefix(sig.ID, "wp."))
		}
		if !known || !d.Has([]string{data.Signal + ".latency", data.Signal + ".gauge", data.Signal + ".counter", data.Signal + ".throughput_change"}, f.RuleID) || !d.Has([]string{"inconclusive", "suspected_high_latency", "within_configured_latency_for_observed_completions", "idle_or_no_completions", "observed_throughput_drop", "gauge_observations_only", "counter_increase_observed", "idle_or_no_events"}, f.Outcome) {
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
		if d.Has([]string{"suspected_high_latency", "within_configured_latency_for_observed_completions", "gauge_observations_only", "counter_increase_observed", "idle_or_no_events"}, f.Outcome) {
			if item.fact == nil || item.fact.ObservedFromMS <= meta.From.UnixMilli() || item.fact.ObservedToMS > meta.To.UnixMilli() || item.fact.ObservedFromMS >= item.fact.ObservedToMS || item.fact.Labels["instance"] != f.Target.Instance || item.fact.Labels["node_id"] != f.Target.NodeID || f.Target.Instance == "" || f.Target.Deployment != meta.Deployment || (report.Selection.Target.Instance != "" && f.Target.Instance != report.Selection.Target.Instance) {
				return fail("invalid_fact_identity_or_time")
			}
		}
		if item.fact != nil && !validSamples(*item.fact) {
			return fail("invalid_fact_sample_contract")
		}
		all = append(all, item)
	}
	for _, entry := range prometheus.ConsumeCatalog() {
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
				if d.Has([]string{"related_population_missing", "related_population_ambiguous", "actual_samples_not_aligned", "wp_reader_mapping_unavailable", "unrecognized_scanner_model", "unrecognized_access_model", "fresh_pack_activity_not_continuous"}, code) {
					a.Gaps = append(a.Gaps, d.Gap{Code: code, Detail: derived.ID})
				}
			}
		}
		if !found {
			a.Gaps = append(a.Gaps, d.Gap{Code: "step_evidence_unavailable", Detail: entry.ID + " has no usable source finding; inspect source errors/coverage"})
		}
	}
	sort.SliceStable(a.Findings, func(i, j int) bool { return a.Findings[i].Summary[:1] < a.Findings[j].Summary[:1] })
	if report.Selection.Scope == "e2e" {
		a.Gaps = append(a.Gaps, d.Gap{Code: "wp_reader_mapping_unavailable", Detail: "No authoritative, time-valid Milvus instance/collection/channel to WP-client metadata namespace/log/segment/node mapping was collected. WP retrieval observations are parallel unassociated context, not a serial Milvus consume step"})
	}
	a.Gaps = append(a.Gaps, d.Gap{Code: "optional_sources_not_selected", Detail: "Only Prometheus was queried. Native reader progress/quorum rules remain owned by wp diagnose --scenario read (#453); native/trace/Loki enrichment was not collected and unsupported requested sources fail explicitly"})
	return a
}

func cancelStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "budget_exceeded"
	}
	return "cancelled"
}

func factPoints(f *prometheus.Fact) []prometheus.Point {
	if f.Gauge != nil {
		return f.Gauge.Samples
	}
	if f.Counter != nil {
		return f.Counter.Samples
	}
	return nil
}

func validSamples(f prometheus.Fact) bool {
	if f.Gauge == nil && f.Counter == nil {
		return true
	}
	if f.Gauge != nil && f.Counter != nil {
		return false
	}
	points := factPoints(&f)
	if len(points) < 2 || points[0].TimeMS != f.ObservedFromMS || points[len(points)-1].TimeMS != f.ObservedToMS {
		return false
	}
	minimum, peak := points[0].Value, points[0].Value
	for i, p := range points {
		if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) || p.Value < 0 || i > 0 && (p.TimeMS <= points[i-1].TimeMS || f.Counter != nil && p.Value < points[i-1].Value) {
			return false
		}
		minimum = min(minimum, p.Value)
		peak = max(peak, p.Value)
	}
	if g := f.Gauge; g != nil {
		return g.First == points[0].Value && g.Last == points[len(points)-1].Value && g.Minimum == minimum && g.Peak == peak && g.LastTwoZero == (points[len(points)-1].Value == 0 && points[len(points)-2].Value == 0)
	}
	c := f.Counter
	increase := points[len(points)-1].Value - points[0].Value
	return c.Increase == increase && c.PerSecond == increase/(float64(f.ObservedToMS-f.ObservedFromMS)/1000)
}

func interpret(item evidence, all []evidence) d.Finding {
	f := item.finding
	f.ID = item.finding.ID + ":milvus-consume"
	f.RuleID = "milvus.consume.step." + item.data.Signal
	f.DerivedFrom = []string{item.finding.ID}
	f.Supporting = append([]string{}, f.Supporting...)
	f.Unknown = append([]string{}, f.Unknown...)
	label, next := stepMeaning(item.data.Signal)
	f.Summary = label + ": " + f.Summary
	f.NextChecks = []string{next}
	f.Unknown = append(f.Unknown, "independent_populations_not_one_reader_or_request", "overlapping_timers_do_not_add_or_subtract_quantiles", "per_row_visibility_and_client_latency_not_measured")
	if item.fact != nil {
		parts := []string{}
		for _, key := range []string{"component", "channel_name", "scanner_model", "message_type", "access_model", "collection_id", "msg_type", "log_ns", "log_id", "az_scope", "operation", "status"} {
			if value := item.fact.Labels[key]; value != "" {
				parts = append(parts, fmt.Sprintf("%s=%q", key, value))
			}
		}
		if len(parts) > 0 {
			f.Summary += " [" + strings.Join(parts, ", ") + "]"
		}
	}
	if item.finding.Outcome == "suspected_high_latency" {
		switch item.data.Signal {
		case "milvus.consume.processing":
			f.Outcome = "observed_slow_consumer_processing"
			processingPressure(&f, item, all)
		case "wp.read_batch":
			f.Outcome = "observed_slow_wp_batch_reads"
		case "wp.direct_read":
			f.Outcome = "observed_slow_wp_direct_reads"
		}
	}
	if item.data.Signal == "wp.read_next" && d.Has([]string{"suspected_high_latency", "within_configured_latency_for_observed_completions"}, item.finding.Outcome) {
		f.Outcome = "blocking_read_next_completion_time_observed"
		f.Severity = "unknown"
		f.Strength = "observed_completion_timer"
		f.Unknown = append(f.Unknown, "read_next_can_wait_normally_for_new_data_not_io_latency", "expected_data_for_this_reader_not_established")
	}
	if item.fact != nil && item.fact.Gauge != nil {
		gaugeMeaning(&f, item, all)
	}
	if item.fact != nil && item.fact.Counter != nil {
		counterMeaning(&f, item)
	}
	if strings.HasPrefix(item.data.Signal, "wp.") {
		f.Unknown = append(f.Unknown, "wp_reader_mapping_unavailable")
		f.Summary += "; separately observed WP population, not an identified Milvus reader"
	}
	return f
}

func related(f *d.Finding, item evidence, all []evidence, id string, align, collection bool) *evidence {
	matches := []evidence{}
	for _, other := range all {
		if other.data.Signal != id || other.fact == nil {
			continue
		}
		same := other.fact.Population == item.fact.Population
		if collection {
			same = item.fact.CollectionKey != "" && item.fact.Labels["collection_id"] != "" && other.fact.CollectionKey == item.fact.CollectionKey
		}
		if same {
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
		left, right := factPoints(item.fact), factPoints(other.fact)
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

func processingPressure(f *d.Finding, item evidence, all []evidence) {
	other := related(f, item, all, "milvus.consume.waiting", false, false)
	if other == nil || other.fact.Gauge == nil {
		return
	}
	g := other.fact.Gauge
	if g.Minimum > 0 {
		f.Outcome = "suspected_consumer_processing_pressure"
		include(f, *other, false)
	} else if g.Peak == 0 || g.LastTwoZero {
		f.Outcome = "slow_processing_without_persistent_waiting_work"
		if g.Peak > 0 {
			f.Outcome = "slow_processing_with_backlog_recovery"
		}
		include(f, *other, true)
		f.Strength = "inconclusive"
		f.Severity = "unknown"
	} else {
		include(f, *other, false)
		f.Unknown = append(f.Unknown, "intermittent_waiting_not_continuous_pressure")
	}
	f.Unknown = append(f.Unknown, "wait_processing_is_aggregate_not_reader_identity")
}

func gaugeMeaning(f *d.Finding, item evidence, all []evidence) {
	g := item.fact.Gauge
	f.Strength = "observed_exporter_state"
	f.Severity = "unknown"
	switch item.data.Signal {
	case "milvus.consume.mode":
		f.Outcome = "scanner_mode_population_observed"
		if !d.Has([]string{"catchup", "tailing"}, item.fact.Labels["scanner_model"]) {
			f.Outcome = "inconclusive"
			f.Unknown = append(f.Unknown, "unrecognized_scanner_model")
		}
		f.Unknown = append(f.Unknown, "tailing_can_use_memory_without_wp_rpc", "mode_population_not_an_authoritative_wp_reader_mapping")
	case "milvus.consume.pending":
		f.Outcome = bufferOutcome(g, "scanner_pending")
		f.Unknown = append(f.Unknown, "pending_bytes_not_committed_loss_or_disk_fault")
	case "milvus.consume.paused":
		f.Outcome = "scanner_pause_state_observed"
		other := related(f, item, all, "milvus.consume.pending", true, false)
		if other == nil || other.fact.Gauge == nil {
			return
		}
		if g.Minimum == 1 && g.Peak == 1 && other.fact.Gauge.Minimum > 0 {
			f.Outcome = "suspected_scanner_downstream_backpressure"
			f.Strength = "suspected"
			f.Severity = "warning"
			include(f, *other, false)
		} else if g.Peak == 1 && g.LastTwoZero && other.fact.Gauge.LastTwoZero {
			f.Outcome = "scanner_pause_and_pending_recovery_observed"
			include(f, *other, false)
		}
		f.Unknown = append(f.Unknown, "pause_is_backpressure_not_proof_of_wp_io_failure")
	case "milvus.consume.txn":
		f.Outcome = bufferOutcome(g, "transaction_buffer")
		f.Unknown = append(f.Unknown, "transaction_buffer_not_missing_commit_or_corruption_proof")
	case "milvus.consume.ordering":
		f.Outcome = bufferOutcome(g, "ordering_buffer")
		f.Unknown = append(f.Unknown, "ordering_wait_not_a_scan_pass_loss_ratio")
	case "milvus.consume.subscription":
		f.Outcome = "subscription_access_population_observed"
		if !d.Has([]string{"local", "remote"}, item.fact.Labels["access_model"]) {
			f.Outcome = "inconclusive"
			f.Unknown = append(f.Unknown, "unrecognized_access_model")
		}
		f.Unknown = append(f.Unknown, "local_subscription_bypasses_remote_grpc_handler", "subscription_population_not_channel_to_client_metadata_mapping")
	case "milvus.consume.unavailable":
		f.Outcome = bufferOutcome(g, "unavailable_consumer")
		f.Unknown = append(f.Unknown, "startup_retry_or_routing_cause_unknown")
	case "milvus.consume.remote":
		f.Outcome = "remote_inflight_consume_observed"
		f.Unknown = append(f.Unknown, "remote_only_series_can_be_absent_for_local_subscriptions", "inflight_is_not_configured_capacity")
	case "milvus.consume.waiting":
		f.Outcome = bufferOutcome(g, "waiting_processing")
		f.Unknown = append(f.Unknown, "waiting_work_alone_not_a_stall")
	case "milvus.consume.lag":
		f.Outcome = "consumer_time_tick_lag_observed"
		f.Unknown = append(f.Unknown, "time_tick_lag_not_per_row_visibility_or_payload_demand", "time_tick_lag_can_age_without_new_packs")
		if g.Last <= g.First {
			return
		}
		f.Outcome = "consumer_time_tick_lag_increased"
		other := related(f, item, all, "milvus.consume.packs", true, true)
		if other == nil || other.fact.Counter == nil {
			return
		}
		c := other.fact.Counter
		if c.Increase == 0 {
			include(f, *other, true)
			f.Unknown = append(f.Unknown, "no_new_packs_no_stall_verdict")
			return
		}
		continuous := true
		for i := 1; i < len(c.Samples); i++ {
			continuous = continuous && c.Samples[i].Value > c.Samples[i-1].Value
		}
		include(f, *other, false)
		if continuous {
			f.Outcome = "lag_increased_during_observed_msgpack_consumption"
		} else {
			f.Unknown = append(f.Unknown, "fresh_pack_activity_not_continuous")
		}
	}
}

func bufferOutcome(g *prometheus.GaugeFact, name string) string {
	if g.Peak == 0 {
		return "no_" + name + "_observed"
	}
	if g.LastTwoZero {
		return name + "_recovery_observed"
	}
	if g.Minimum > 0 {
		return "persistent_" + name + "_observed"
	}
	return "intermittent_" + name + "_observed"
}

func counterMeaning(f *d.Finding, item evidence) {
	c := item.fact.Counter
	f.Severity = "unknown"
	f.Strength = "observed_counter_delta"
	switch item.data.Signal {
	case "milvus.consume.scanned", "milvus.consume.passed":
		f.Outcome = "scan_activity_observed"
		if c.Increase == 0 {
			f.Outcome = "no_scan_events_observed"
		}
		if item.data.Signal == "milvus.consume.passed" && c.Increase > 0 {
			f.Outcome = "passed_message_activity_observed"
		}
		if !d.Has([]string{"catchup", "tailing"}, item.fact.Labels["scanner_model"]) {
			f.Outcome = "inconclusive"
			f.Unknown = append(f.Unknown, "unrecognized_scanner_model")
		}
		f.Unknown = append(f.Unknown, "scan_pass_counts_not_a_loss_ratio", "control_message_filtering_expected", "aggregate_mode_counts_not_per_reader_frontier")
	case "milvus.consume.delivered":
		f.Outcome = "delivered_bytes_observed"
		if c.Increase == 0 {
			f.Outcome = "no_delivery_bytes_observed"
		}
		f.Unknown = append(f.Unknown, "delivered_bytes_not_user_visibility_or_row_count")
	case "milvus.consume.packs":
		f.Outcome = "msgpack_consumption_observed"
		if c.Increase == 0 {
			f.Outcome = "no_new_msgpacks_observed"
		}
		f.Unknown = append(f.Unknown, "msgpacks_include_control_traffic_not_fresh_payload_demand")
	case "wp.quorum_bytes":
		f.Outcome = "quorum_read_bytes_observed"
		if c.Increase == 0 {
			f.Outcome = "no_quorum_read_bytes_observed"
		}
		f.Unknown = append(f.Unknown, "zero_rpc_traffic_can_mean_memory_tailing_or_direct_read")
	case "wp.eof":
		f.Outcome = "server_eof_observed"
		if c.Increase == 0 {
			f.Outcome = "no_server_eof_events_observed"
		}
		f.Unknown = append(f.Unknown, "eof_is_normal_at_tail_not_proof_all_readers_caught_up")
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
	case "milvus.consume.mode":
		return "1 Read mode / source", "Inspect catchup versus tailing mode and the actual client namespace/log mapping. Memory tailing can legitimately produce no WP service reads"
	case "wp.read_next", "wp.read_batch", "wp.direct_read", "wp.quorum_bytes", "wp.eof":
		return "2 WP retrieval (unassociated context)", "Use wp diagnose --scenario read for native reader reporting cadence, expected catch-up progress and explicit quorum evidence. Blocking read-next/EOF and idle service traffic alone do not prove I/O trouble"
	case "milvus.consume.scanned", "milvus.consume.passed", "milvus.consume.pending", "milvus.consume.paused", "milvus.consume.txn", "milvus.consume.ordering":
		return "3 Scanner / filtering / buffers", "Inspect this exporter's channel, scanner mode, pause state, ordering and transaction completion. Scanned and passed counts are not a loss ratio; check downstream handling before assigning a storage cause"
	case "milvus.consume.subscription", "milvus.consume.unavailable", "milvus.consume.delivered", "milvus.consume.remote":
		return "4 Subscription / dispatch", "Inspect local versus remote access, availability and reconnect state. Local consumption bypasses the remote handler; missing remote-only series is not an error"
	default:
		return "5 Consumer processing / visibility", "Compare same-exporter pending processing and completion latency; inspect actual consistency/TSafe using wpdiag milvus query. Sampled lag/MsgPacks do not measure per-row insert-to-visible time or identify a WP reader stall"
	}
}
