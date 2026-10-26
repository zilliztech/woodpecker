// Package milvusappend assigns append-step business meaning to existing source
// findings. It never re-evaluates histogram thresholds or fetches WP metadata.
package milvusappend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func Descriptor() d.StageDescriptor {
	return d.StageDescriptor{Command: "milvus append", Issue: 463, Sources: []string{"prometheus"}, Profiles: (prometheus.AppendModule{}).Descriptor().Profiles}
}

// Run uses the same config/selection/execution contracts as source commands.
// Assess can also be used on an already collected report, with no re-query.
func Run(ctx context.Context, config d.Config, r d.Request) d.Report {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.Limits.BudgetMS)*time.Millisecond)
	defer cancel()
	engine, _ := d.NewEngine(prometheus.AppendModule{})
	report := engine.Run(ctx, config, r)
	if report.Status == "unsupported" && len(report.Results) == 1 && len(report.Results[0].Problems) == 1 && report.Results[0].Problems[0].Code == "module_not_implemented" {
		report.Results[0].Problems[0] = d.Problem{Code: "stage_source_not_supported", Message: "Milvus append currently supports Prometheus only; other independent source commands are not append-stage inputs"}
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
	return d.Assessment{Profile: "milvus.append", Version: 1, Status: status, Findings: []d.Finding{}, Gaps: []d.Gap{}}
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
		failed.Gaps = append(failed.Gaps, d.Gap{Code: code, Detail: "Cannot assess the supported Prometheus append-step contract; source evidence remains unchanged"})
		return failed
	}
	if report.SchemaVersion != d.ReportSchema || report.Selection.Flow != "write" || report.Selection.Stage != "append" || !d.Has([]string{"milvus", "e2e"}, report.Selection.Scope) || len(report.Results) != 1 || len(report.Selection.Sources) != 1 || report.Selection.Sources[0] != "prometheus" {
		return fail("unsupported_stage_input")
	}
	source := report.Results[0]
	if source.Source != "prometheus" || source.Profile != report.Selection.Scope+".append.steps.metrics" {
		return fail("unsupported_source_profile")
	}
	var meta struct {
		Schema     string `json:"source_schema_version"`
		From, To   time.Time
		Deployment string `json:"bound_deployment"`
	}
	if json.Unmarshal(source.Metadata, &meta) != nil || meta.Schema != "wpdiag.prometheus/v1" || meta.Deployment == "" || meta.From.IsZero() || !meta.To.After(meta.From) {
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
		observations[o.ID] = o
		if o.Status != "observed" {
			continue
		}
		var data prometheus.Dataset
		if o.Kind != "prometheus_matrix" || json.Unmarshal(o.Data, &data) != nil || !data.From.Equal(meta.From) || !data.To.Equal(meta.To) {
			return fail("invalid_observation_contract")
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
		if f.ID == "" || f.RuleVersion != 1 || f.Flow != "write" || f.Stage != "append" || len(f.Supporting) != 1 || len(f.Contradicting) != 0 {
			return fail("unsupported_source_finding_contract")
		}
		o := observations[f.Supporting[0]]
		data, ok := datasets[o.ID]
		if !ok || o.Status != "observed" || !strings.HasPrefix(f.RuleID, data.Signal+".") {
			return fail("dangling_or_wrong_source_reference")
		}
		known := false
		for _, sig := range prometheus.AppendCatalog() {
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
		all = append(all, item)
	}
	for _, entry := range prometheus.AppendCatalog() {
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
			derived := interpret(item)
			a.Findings = append(a.Findings, derived)
			for _, code := range derived.Unknown {
				if d.Has([]string{"wp_metric_context_unassociated"}, code) {
					a.Gaps = append(a.Gaps, d.Gap{Code: code, Detail: derived.ID})
				}
			}
		}
		if !found {
			a.Gaps = append(a.Gaps, d.Gap{Code: "step_evidence_unavailable", Detail: entry.ID + " has no usable source finding; inspect source errors/coverage"})
		}
	}
	if report.Selection.Scope == "e2e" {
		a.Gaps = append(a.Gaps, d.Gap{Code: "wp_append_mapping_unavailable", Detail: "No authoritative, time-valid instance/collection/channel to WP-client namespace/log/segment/node mapping was collected. WP metric populations remain separately observed context; the WP service context does not identify client metadata authority"})
	}
	a.Gaps = append(a.Gaps, d.Gap{Code: "optional_sources_not_selected", Detail: "Only Prometheus was queried. Native write findings remain owned by wp diagnose --scenario write (#452); trace/Loki and native enrichment require separate collection and authoritative mappings. Unsupported requested sources fail explicitly"})
	return a
}

func cancelStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "budget_exceeded"
	}
	return "cancelled"
}

func interpret(item evidence) d.Finding {
	f := item.finding
	f.ID = item.finding.ID + ":milvus-append"
	f.RuleID = "milvus.append.step." + item.data.Signal
	f.DerivedFrom = []string{item.finding.ID}
	f.Supporting = append([]string{}, item.finding.Supporting...)
	f.Unknown = append([]string{}, item.finding.Unknown...)
	label, next := stepMeaning(item.data.Signal)
	f.Summary = label + ": " + item.finding.Summary
	f.NextChecks = []string{next}
	f.Unknown = append(f.Unknown, "independent_metric_populations_not_one_request", "overlapping_timers_do_not_add_or_subtract_quantiles", "client_observed_latency_not_measured")
	if item.fact != nil {
		labels := []string{}
		for _, key := range []string{"component", "msg_type", "channel_name", "interceptor_name", "log_ns", "log_id", "status", "operation"} {
			if v := item.fact.Labels[key]; v != "" {
				labels = append(labels, fmt.Sprintf("%s=%q", key, v))
			}
		}
		if len(labels) > 0 {
			f.Summary += " [" + strings.Join(labels, ", ") + "]"
		}
	}
	if item.finding.Outcome == "suspected_high_latency" {
		outcomes := map[string]string{"milvus.append": "observed_slow_proxy_completions", "milvus.append.pk": "suspected_proxy_preparation_work", "milvus.append.timestamp": "suspected_proxy_preparation_work", "milvus.append.produce": "observed_slow_producer_completions", "milvus.append.quota": "suspected_quota_admission_wait", "milvus.wal_append": "observed_slow_wal_completions", "milvus.append.backend": "observed_slow_backend_completions", "milvus.append.before": "suspected_before_append_interceptor_work", "milvus.append.after": "suspected_after_append_interceptor_work", "wp.client_append": "observed_slow_wp_client_completions", "wp.append": "observed_slow_wp_server_completions", "wp.buffer_wait": "observed_slow_wp_buffer_notifications", "wp.flush": "observed_slow_wp_flush_completions"}
		f.Outcome = outcomes[item.data.Signal]
	}
	if item.fact != nil && item.fact.Gauge != nil {
		interpretGauge(&f, item)
	}
	if strings.HasPrefix(item.data.Signal, "wp.") {
		f.Unknown = append(f.Unknown, "wp_metric_context_unassociated")
		f.Summary += "; separately observed WP population, not mapped to this Milvus request/channel"
	}
	return f
}

func interpretGauge(f *d.Finding, item evidence) {
	g := item.fact.Gauge
	f.Strength = "observed_exporter_state"
	f.Severity = "unknown"
	f.Summary += fmt.Sprintf("; retained range %.0f..%.0f, latest %.0f", g.Minimum, g.Peak, g.Last)
	switch item.data.Signal {
	case "milvus.append.unavailable":
		f.Outcome = "intermittent_producer_unavailability_observed"
		if g.Peak == 0 {
			f.Outcome = "no_unavailable_producers_observed"
		} else if g.LastTwoZero {
			f.Outcome = "producer_availability_recovery_observed"
		} else if g.Minimum > 0 {
			f.Outcome = "persistent_unavailable_producers_observed"
		}
		f.Unknown = append(f.Unknown, "producer_startup_retry_or_routing_cause_unknown", "expected_traffic_not_established")
	case "milvus.append.inflight":
		f.Outcome = "inflight_requests_observed"
		f.Unknown = append(f.Unknown, "inflight_is_not_configured_capacity_or_proof_of_saturation")
	case "wp.pending":
		f.Outcome = "intermittent_pending_appends_observed"
		if g.Peak == 0 {
			f.Outcome = "no_pending_appends_observed"
		} else if g.LastTwoZero {
			f.Outcome = "pending_append_recovery_observed"
		} else if g.Minimum > 0 {
			f.Outcome = "persistent_pending_appends_observed"
		}
		f.Unknown = append(f.Unknown, "pending_work_alone_does_not_establish_stall_or_fresh_demand")
	case "wp.disk_state":
		f.Outcome = "no_disk_admission_pressure_observed"
		if g.Peak > 0 {
			f.Outcome = "exported_disk_admission_pressure_observed"
			f.Severity = "warning"
		}
		if g.Peak > 0 && g.LastTwoZero {
			f.Outcome = "disk_admission_recovery_observed"
			f.Severity = "unknown"
		}
		f.Unknown = append(f.Unknown, "exported_0_normal_1_warn_2_blocked_state_not_latency_cause")
	}
}

func stepMeaning(id string) (string, string) {
	switch id {
	case "milvus.append", "milvus.append.pk", "milvus.append.timestamp":
		return "1 Proxy preparation / completion", "Compare successful mutation completions with SDK latency, errors and traffic; PK/TSO timers can have broader populations. No user latency or preparation residual is calculated"
	case "milvus.append.produce", "milvus.append.quota", "milvus.append.unavailable":
		return "2 Producer routing / quota admission", "Check the named channel's producer availability and configured quota/resource protection; quota waiting differs from SN/backend append. A resumed producer may be starting or retrying"
	case "milvus.wal_append", "milvus.append.backend", "milvus.append.before", "milvus.append.after", "milvus.append.inflight":
		return "3 SN WAL / interceptors", "Inspect the named interceptor/channel and overlapping timer scopes, retries and SN resources; obtain an actual matching trace before attributing a residual delay"
	case "wp.client_append", "wp.pending":
		return "4 WP client inside SN (unassociated context)", "Resolve the authoritative Milvus-client metadata namespace/log/channel association and its validity interval; the WP service context or a shared log number is insufficient"
	default:
		return "5 WP server / buffer / local flush (unassociated context)", "Use wp diagnose --scenario write for native write-stall/flush/disk/replica rules; inspect bounded wp ops and logs for the actual mapped nodes. Local flush latency alone does not identify disk, sync, batching or scheduling cause"
	}
}
