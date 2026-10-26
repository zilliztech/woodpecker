package jaeger

import (
	"context"
	"fmt"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func finding(r d.Request, t Trace, obsID, rule string) d.Finding {
	target := r.Selection.Target
	target.TraceID = t.ID
	target.Identity = "retained_trace"
	target.TenantAssociation = "unverified: trace IDs do not establish WP-client metadata ownership"
	return d.Finding{
		ID: obsID + ":" + rule, RuleID: rule, RuleVersion: 1, Flow: r.Selection.Flow, Stage: r.Profile.Stage, Target: target,
		Outcome: "inconclusive", Severity: "unknown", Strength: "inconclusive", Supporting: []string{obsID}, Contradicting: []string{}, Unknown: []string{"sampling_export_and_retention_completeness_unverified", "application_profile_version_unverified"},
		NextChecks: []string{"Open the returned Jaeger UI path using the selected query connection", "Check sampling/export/retention and missing parents before treating absent spans as absent work", "Use the relevant WP-native or Milvus stage diagnostic to test a runtime hypothesis; a slow span does not prove a disk/fsync or object-store cause"},
	}
}

func evaluate(ctx context.Context, r d.Request, s Settings, t *Trace, obsID string) []d.Finding {
	coverage := finding(r, *t, obsID, "trace.relationship_coverage")
	coverage.Unknown = append(coverage.Unknown, t.Notes...)
	coverage.Unknown = append(coverage.Unknown, "durations_are_inclusive_not_additive", "critical_path_not_established")
	spans := map[string]*Span{}
	parents := map[string]string{}
	for i := range t.Spans {
		spans[t.Spans[i].ID] = &t.Spans[i]
	}
	for i := range t.Spans {
		span := &t.Spans[i]
		for _, ref := range span.References {
			if ref.Type == "FOLLOWS_FROM" {
				coverage.Unknown = appendUnique(coverage.Unknown, "async_links_not_serial_children")
				continue
			}
			if ref.TraceID != t.ID {
				coverage.Unknown = appendUnique(coverage.Unknown, "external_trace_parent_not_fetched")
				continue
			}
			parent := spans[ref.SpanID]
			if parent == nil {
				coverage.Unknown = appendUnique(coverage.Unknown, "parent_not_retained")
				continue
			}
			if before, ok := parents[span.ID]; ok && before != parent.ID {
				coverage.Unknown = appendUnique(coverage.Unknown, "multiple_parents_unverified")
				span.Notes = appendUnique(span.Notes, "multiple_parents_unverified")
				continue
			}
			parents[span.ID] = parent.ID
			allow := s.Policy.ClockSkewMS * 1000
			if parent.StartUS > span.StartUS && parent.StartUS-span.StartUS > allow || span.StartUS+span.DurationUS > parent.StartUS+parent.DurationUS && (span.StartUS+span.DurationUS)-(parent.StartUS+parent.DurationUS) > allow {
				coverage.Unknown = appendUnique(coverage.Unknown, "clock_or_async_lifetime_unverified")
			}
		}
	}
	// Resolve ancestor role masks once per span. Repeated API views or shared
	// roots do not create additional confirmations or a serial critical path.
	colors := map[string]int{}
	masks := map[string]uint8{}
	selectedAncestors := map[string]bool{}
	cycle, interrupted := false, false
	var visit func(string) uint8
	visit = func(id string) uint8 {
		if ctx.Err() != nil {
			interrupted = true
			return 0
		}
		if colors[id] == 2 {
			return masks[id]
		}
		if colors[id] == 1 {
			cycle = true
			return 0
		}
		colors[id] = 1
		mask := uint8(0)
		if parent := parents[id]; parent != "" {
			mask = visit(parent)
			selectedAncestors[id] = selectedAncestors[parent] || spans[parent].Selected
		}
		span := spans[id]
		if span.Selected {
			switch span.Role {
			case "proxy":
				mask |= 1
			case "streamingnode":
				if span.Scope != "LogWriter" && span.Scope != "LogReader" {
					mask |= 2
				}
			case "wp_server":
				mask |= 4
			case "querynode", "segcore":
				mask |= 8
			case "wp_client":
				mask |= 16
			}
			if span.Scope == "LogWriter" || span.Scope == "LogReader" {
				mask |= 16
			}
		}
		colors[id], masks[id] = 2, mask
		return mask
	}
	for _, span := range t.Spans {
		visit(span.ID)
	}
	if cycle {
		coverage.Unknown = appendUnique(coverage.Unknown, "cyclic_span_references")
	}
	if interrupted {
		coverage.Unknown = appendUnique(coverage.Unknown, "evaluation_interrupted")
	}
	results := []d.Finding{}
	selected, roots := 0, 0
	path := false
	longWait := false
	for _, span := range t.Spans {
		if !span.Selected {
			continue
		}
		for _, note := range span.Notes {
			coverage.Unknown = appendUnique(coverage.Unknown, note)
		}
		selected++
		if !selectedAncestors[span.ID] {
			roots++
		}
		mask := masks[span.ID]
		switch r.Selection.Scope {
		case "wp":
			path = path || span.Role == "wp_server" && mask&16 != 0
		case "milvus":
			if r.Selection.Flow == "write" {
				path = path || span.Role == "streamingnode" && mask&1 != 0
			} else {
				path = path || (span.Role == "querynode" || span.Role == "streamingnode" || span.Role == "segcore") && mask&1 != 0
			}
		case "e2e":
			if r.Selection.Flow == "write" {
				path = path || span.Role == "wp_server" && mask&3 == 3
			} else {
				path = path || (span.Role == "querynode" || span.Role == "streamingnode" || span.Role == "segcore") && mask&1 != 0
			}
		}
		ms := float64(span.DurationUS) / 1000
		if span.Phase == "tail_or_catchup_wait" && ms > s.Policy.SlowSpanMS {
			longWait = true
		}
		if !span.Error && (ms <= s.Policy.SlowSpanMS || span.Phase == "tail_or_catchup_wait" || span.Phase == "async") {
			continue
		}
		rule := "trace.slow_span"
		if span.Error {
			rule = "trace.span_error"
		}
		f := finding(r, *t, obsID, rule+":"+span.ID)
		f.RuleID = rule
		f.Unknown = append(f.Unknown, span.Notes...)
		f.Unknown = append(f.Unknown, t.Notes...)
		for _, gap := range coverage.Unknown {
			f.Unknown = appendUnique(f.Unknown, gap)
		}
		f.Unknown = append(f.Unknown, "inclusive_span_is_not_client_latency_or_root_cause")
		f.Outcome, f.Severity, f.Strength = "observed_slow_span", "warning", "suspected"
		if span.Error {
			f.Outcome = "observed_span_error"
		}
		f.Summary = fmt.Sprintf("Trace %s span %s: %s/%s (%s), duration %.3fms; threshold %.3fms, error=%t. Span timing/flags identify suspicious observed work, not the cause", t.ID, span.ID, span.Service, span.Operation, span.Phase, ms, s.Policy.SlowSpanMS, span.Error)
		results = append(results, f)
	}
	if r.Selection.Flow == "read" && r.Selection.Scope != "wp" {
		coverage.Unknown = appendUnique(coverage.Unknown, "query_trace_does_not_establish_wal_consumption")
	}
	if r.Selection.Flow == "write" && r.Selection.Scope != "wp" {
		coverage.Unknown = appendUnique(coverage.Unknown, "insert_propagation_can_be_partial")
	}
	switch {
	case selected == 0:
		coverage.Summary = "No supported in-window operations matched the selected scope/flow and target; absence is not healthy"
		coverage.Unknown = appendUnique(coverage.Unknown, "supported_span_evidence_absent")
	case cycle || interrupted || d.Has(coverage.Unknown, "multiple_parents_unverified"):
		coverage.Summary = "Observed span relationships are ambiguous or evaluation was interrupted; no path association is asserted"
	case !path || roots != 1:
		coverage.Outcome = "partial_observed_path"
		coverage.Summary = fmt.Sprintf("Observed %d matching spans in %d selected roots; expected connected component stages are missing or split. Same-time/service presence is not an E2E path", selected, roots)
		coverage.Unknown = appendUnique(coverage.Unknown, "expected_connected_stages_not_established")
	default:
		coverage.Outcome, coverage.Severity = "observed_stages_connected", "none"
		coverage.Summary = "Expected component stages share an observed CHILD_OF ancestry. This is minimum retained-span association, not complete instrumentation, a critical path or a health verdict"
	}
	if longWait {
		coverage.Unknown = appendUnique(coverage.Unknown, "long_readnext_may_be_normal_tailing")
	}
	if d.Has(coverage.Unknown, "clock_or_async_lifetime_unverified") {
		coverage.Unknown = appendUnique(coverage.Unknown, "cross_process_timing_not_subtracted")
	}
	if selected > 0 && len(results) == 0 {
		f := finding(r, *t, obsID, "trace.observed_span_summary")
		f.Outcome, f.Severity = "no_supported_suspicion_in_observed_spans", "none"
		f.Summary = "No supported error or synchronous slow-span policy matched the selected retained spans; this does not clear unobserved work or establish population health"
		f.Unknown = append(f.Unknown, "observed_sample_not_population_health")
		if d.Has(coverage.Unknown, "error_tag_semantics_unverified") {
			f.Outcome, f.Severity = "inconclusive", "unknown"
			f.Unknown = append(f.Unknown, "error_tag_semantics_unverified")
			f.Summary = "Selected spans contain unsupported/conflicting error flags; absence of an interpreted error is not a healthy result"
		}
		results = append(results, f)
	}
	return append(results, coverage)
}
