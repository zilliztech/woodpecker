// Package milvusquery assigns query-step business meaning to existing source
// findings. It never re-evaluates histogram thresholds or fetches WP metadata.
package milvusquery

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
	return d.StageDescriptor{Command: "milvus query", Issue: 466, Sources: []string{"prometheus"}, Profiles: (prometheus.QueryModule{}).Descriptor().Profiles}
}

// Run uses the same config/selection/execution contracts as source commands.
// Assess can also be used on an already collected report, with no re-query.
func Run(ctx context.Context, config d.Config, r d.Request) d.Report {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.Limits.BudgetMS)*time.Millisecond)
	defer cancel()
	engine, _ := d.NewEngine(prometheus.QueryModule{})
	report := engine.Run(ctx, config, r)
	if report.Status == "unsupported" && len(report.Results) == 1 && len(report.Results[0].Problems) == 1 && report.Results[0].Problems[0].Code == "module_not_implemented" {
		report.Results[0].Problems[0] = d.Problem{Code: "stage_source_not_supported", Message: "Milvus query currently supports Prometheus only; other independent source commands are not query-stage inputs"}
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
	return d.Assessment{Profile: "milvus.query", Version: 1, Status: status, Findings: []d.Finding{}, Gaps: []d.Gap{}}
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
		failed.Gaps = append(failed.Gaps, d.Gap{Code: code, Detail: "Cannot assess the supported Prometheus query-step contract; source evidence remains unchanged"})
		return failed
	}
	if report.SchemaVersion != d.ReportSchema || report.Selection.Flow != "read" || report.Selection.Stage != "query" || !d.Has([]string{"milvus", "e2e"}, report.Selection.Scope) || len(report.Results) != 1 || len(report.Selection.Sources) != 1 || report.Selection.Sources[0] != "prometheus" {
		return fail("unsupported_stage_input")
	}
	source := report.Results[0]
	if source.Source != "prometheus" || source.Profile != report.Selection.Scope+".query.steps.metrics" {
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
		if f.ID == "" || f.RuleVersion != 1 || f.Flow != "read" || f.Stage != "query" || len(f.Supporting) != 1 || len(f.Contradicting) != 0 {
			return fail("unsupported_source_finding_contract")
		}
		o := observations[f.Supporting[0]]
		data, ok := datasets[o.ID]
		if !ok || o.Status != "observed" || !strings.HasPrefix(f.RuleID, data.Signal+".") {
			return fail("dangling_or_wrong_source_reference")
		}
		known := false
		for _, sig := range prometheus.QueryCatalog() {
			known = known || sig.ID == data.Signal
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
			if item.fact == nil || item.fact.ObservedFromMS <= meta.From.UnixMilli() || item.fact.ObservedToMS > meta.To.UnixMilli() || item.fact.ObservedFromMS >= item.fact.ObservedToMS || item.fact.Labels["instance"] != f.Target.Instance || item.fact.Labels["node_id"] != f.Target.NodeID || f.Target.Instance == "" || f.Target.Deployment != meta.Deployment {
				return fail("invalid_fact_identity_or_time")
			}
		}
		all = append(all, item)
	}
	for _, entry := range prometheus.QueryCatalog() {
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
				if d.Has([]string{"missing_ready_association", "ambiguous_ready_population", "non_overlapping_actual_coverage"}, code) {
					a.Gaps = append(a.Gaps, d.Gap{Code: code, Detail: derived.ID})
				}
			}
		}
		if !found {
			a.Gaps = append(a.Gaps, d.Gap{Code: "step_evidence_unavailable", Detail: entry.ID + " has no usable source finding; inspect source errors/coverage"})
		}
	}
	if report.Selection.Scope == "e2e" {
		a.Gaps = append(a.Gaps, d.Gap{Code: "wp_visibility_mapping_unavailable", Detail: "No authoritative, time-valid collection/channel/WP-client namespace/log association was collected. Query-only findings remain useful; WP is not implicated"})
	}
	a.Gaps = append(a.Gaps, d.Gap{Code: "optional_sources_not_selected", Detail: "wpcli, trace and Loki were not queried; unsupported requested sources fail explicitly"})
	return a
}

func cancelStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "budget_exceeded"
	}
	return "cancelled"
}

func interpret(item evidence, all []evidence) d.Finding {
	f := item.finding
	f.ID = item.finding.ID + ":milvus-query"
	f.RuleID = "milvus.query." + strings.TrimPrefix(item.data.Signal, "milvus.query")
	f.RuleID = strings.ReplaceAll(f.RuleID, "..", ".")
	f.DerivedFrom = []string{item.finding.ID}
	if item.data.Signal == "milvus.query" {
		f.RuleID = "milvus.query.proxy_completion"
	}
	f.Supporting = append([]string{}, item.finding.Supporting...)
	f.Unknown = append([]string{}, item.finding.Unknown...)
	f.NextChecks = []string{}
	label, next := stepMeaning(item.data.Signal)
	f.Summary = label + ": " + item.finding.Summary
	if item.fact != nil {
		labels := []string{}
		for _, key := range []string{"component", "query_type", "db_name", "collection_name", "collection_id", "segment_state"} {
			if value := item.fact.Labels[key]; value != "" {
				labels = append(labels, fmt.Sprintf("%s=%q", key, value))
			}
		}
		if len(labels) > 0 {
			f.Summary += " [" + strings.Join(labels, ", ") + "]"
		}
	}
	f.NextChecks = append(f.NextChecks, next)
	f.Unknown = append(f.Unknown, "independent_metric_populations_not_one_request", "timers_may_overlap_do_not_add_quantiles")
	if item.finding.Outcome == "suspected_high_latency" {
		outcomes := map[string]string{"milvus.query": "observed_slow_proxy_completions", "milvus.query.tsafe": "suspected_visibility_wait", "milvus.query.handler": "observed_slow_handler_completions", "milvus.query.queue": "suspected_scheduling_delay", "milvus.query.core": "suspected_execution_work", "milvus.query.segment": "observed_slow_segment_work", "milvus.query.loading": "observed_slow_segment_loading", "milvus.query.wait_result": "suspected_downstream_result_wait", "milvus.query.proxy_reduce": "suspected_proxy_result_processing", "milvus.query.decode": "suspected_proxy_result_processing", "milvus.query.node_reduce": "observed_slow_downstream_reduction"}
		f.Outcome = outcomes[item.data.Signal]
		if item.data.Signal == "milvus.query.queue" {
			associateReady(&f, item, all)
		}
	}
	if item.fact != nil && item.fact.Gauge != nil {
		interpretGauge(&f, item)
	}
	return f
}

func associateReady(f *d.Finding, item evidence, all []evidence) {
	if item.fact == nil || item.fact.ExporterKey == "" {
		f.Unknown = append(f.Unknown, "missing_ready_association")
		return
	}
	candidates := []evidence{}
	for _, other := range all {
		if other.data.Signal == "milvus.query.ready" && other.fact != nil && other.fact.Gauge != nil && other.fact.ExporterKey == item.fact.ExporterKey {
			candidates = append(candidates, other)
		}
	}
	if len(candidates) != 1 {
		code := "missing_ready_association"
		if len(candidates) > 1 {
			code = "ambiguous_ready_population"
		}
		f.Unknown = append(f.Unknown, code)
		return
	}
	ready := candidates[0]
	g := ready.fact.Gauge
	if max(item.fact.ObservedFromMS, ready.fact.ObservedFromMS) >= min(item.fact.ObservedToMS, ready.fact.ObservedToMS) {
		f.Unknown = append(f.Unknown, "non_overlapping_actual_coverage")
		return
	}
	f.DerivedFrom = append(f.DerivedFrom, ready.finding.ID)
	f.Unknown = append(f.Unknown, "ready_queue_is_exporter_aggregate_not_request_specific")
	if g.Minimum > 0 && g.Last > 0 {
		f.Outcome = "suspected_scheduling_pressure"
		f.Supporting = appendUnique(f.Supporting, ready.finding.Supporting...)
		f.Summary += "; same exporter has a positive ready backlog throughout retained scrapes"
	} else if g.Peak == 0 || g.LastTwoZero {
		f.Outcome = "slow_queue_completions_without_observed_backlog"
		if g.Peak > 0 {
			f.Outcome = "slow_queue_completions_with_backlog_recovery"
		}
		f.Strength = "inconclusive"
		f.Severity = "unknown"
		f.Contradicting = appendUnique(f.Contradicting, ready.finding.Supporting...)
		f.Summary += "; ready-queue observations contradict sustained current pressure, without disproving earlier queue delay"
	} else {
		f.Supporting = appendUnique(f.Supporting, ready.finding.Supporting...)
		f.Unknown = append(f.Unknown, "intermittent_backlog_not_continuous_pressure")
	}
}

func interpretGauge(f *d.Finding, item evidence) {
	g := item.fact.Gauge
	f.Severity = "unknown"
	f.Strength = "inconclusive"
	switch item.data.Signal {
	case "milvus.query.ready":
		f.Outcome = "intermittent_ready_backlog_observed"
		if g.Peak == 0 {
			f.Outcome = "no_ready_backlog_observed"
			f.Severity = "none"
		} else if g.LastTwoZero {
			f.Outcome = "ready_backlog_recovery_observed"
			f.Severity = "none"
		} else if g.Minimum > 0 {
			f.Outcome = "persistent_ready_backlog_observed"
		}
	case "milvus.query.concurrency":
		f.Outcome = "executing_concurrency_observed"
		f.Unknown = append(f.Unknown, "executing_concurrency_is_not_configured_capacity")
	case "milvus.query.tt_lag":
		f.Outcome = "consumer_time_tick_lag_observed"
		f.Unknown = append(f.Unknown, "collection_time_tick_is_not_query_tsafe_or_wp_binding")
	}
	f.Summary += fmt.Sprintf("; retained range %.0f..%.0f, latest %.0f", g.Minimum, g.Peak, g.Last)
}

func appendUnique(values []string, extra ...string) []string {
	for _, value := range extra {
		if !d.Has(values, value) {
			values = append(values, value)
		}
	}
	return values
}

func stepMeaning(id string) (string, string) {
	switch id {
	case "milvus.query":
		return "1 Proxy completion", "Check client latency/error counts and query_type. Proxy success timers exclude SDK/network time and failed/pending requests"
	case "milvus.query.tsafe", "milvus.query.handler", "milvus.query.tt_lag":
		return "2 Visibility / handler", "Check the requested consistency level and authoritative collection/channel consumption. TSafe waiting does not establish a WP read or disk fault"
	case "milvus.query.queue", "milvus.query.ready", "milvus.query.concurrency":
		return "3 Scheduling", "Compare ready backlog and actual execution concurrency on the same exporter; inspect configured scheduling capacity separately"
	case "milvus.query.core", "milvus.query.segment", "milvus.query.loading":
		return "4 Execution / loading", "Inspect query plan, NQ/topK, growing/sealed segments and loading; these timers have different populations and cannot be added"
	default:
		return "5 Result return", "Inspect fan-out, reduction/result size and decoding. Proxy result wait includes downstream execution and transport, not pure network latency"
	}
}
