package checks

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func connection(c d.Config, source, ref string) (d.Connection, bool) {
	v, ok := c.Sources[source].Connections[ref]
	return v, ok && v.IsEnabled()
}

func metricReference(t evidence.Target, d Definition) string {
	return t.Prometheus
}

func baseResult(def Definition) Result {
	return Result{Assessment: def.Assessment, Evidence: evidence.Unknown(def.ID, "not_started", "Check was not collected"), Name: def.Name, Title: def.Title, Source: def.Source, Step: def.Step, RuleVersion: def.RuleVersion, Execution: "pending", Rule: def.Description.Criterion, IdentityScope: "configured_metric_populations", Queries: []d.SourceQuery{}, Coverage: []string{def.Limits}}
}

func emit(ch chan<- Event, def Definition, phase string) {
	if ch != nil {
		select {
		case ch <- Event{ID: def.ID, Source: def.Source, Phase: phase, At: time.Now().UTC()}:
		default:
		}
	}
}

func finishResult(r *Result) {
	r.DataState = evidence.DataState(r.Evidence)
	r.StatusLabel = evidence.DisplayState(r.Evidence)
}

func ErrorReport(q Request, code, message string) Report {
	return Report{SchemaVersion: ReportSchema, RunID: uuid.NewString(), Request: q, Status: "invalid", Checks: []Result{}, Observations: []d.Observation{}, Problems: []d.Problem{{Code: code, Message: message}}, Coverage: []string{}, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Summary: Summary{Metrics: SourceSummary{Status: "not_configured"}, Trace: SourceSummary{Status: "not_configured"}, Steps: []StepSummary{}}}
}

func Run(ctx context.Context, c d.Config, q Request, events chan<- Event) Report {
	if q.IncludeMilvus {
		return ErrorReport(q, "scope_not_supported", "Milvus diagnosis has been retired; select Woodpecker checks")
	}
	if q.Bindings != nil {
		for _, scope := range []*Scope{q.Bindings.Client, q.Bindings.Server} {
			if scope != nil && scope.Milvus != nil {
				return ErrorReport(q, "scope_not_supported", "Milvus diagnostic associations are no longer supported")
			}
		}
	}

	if q.Details == "" {
		q.Details = "abnormal"
	}
	if q.BudgetMS == 0 {
		q.BudgetMS = 60000
	}
	s, err := LoadSettings(c)
	if err != nil {
		return ErrorReport(q, "configuration_invalid", err.Error())
	}
	if q.From.IsZero() || !q.From.Before(q.To) || q.To.After(time.Now()) {
		return ErrorReport(q, "invalid_window", "Choose a past interval with from < to, using explicit RFC3339 timestamps or --window")
	}
	if q.Details != "none" && q.Details != "abnormal" && q.Details != "all" {
		return ErrorReport(q, "invalid_details", "details must be none, abnormal or all")
	}
	if q.BudgetMS < 1000 || q.BudgetMS > 600000 {
		return ErrorReport(q, "invalid_budget", "Budget must be between 1s and 10m")
	}
	defs, gaps, err := Selection(q, s.Policy)
	if err != nil {
		return ErrorReport(q, "invalid_selection", err.Error())
	}
	q.Bindings = BindingsForChecks(q.Bindings, defs)
	target, ok := s.Targets[q.Target]
	componentTargets := map[string]evidence.Target{}
	if q.Bindings != nil {
		var resolveErr error
		c, componentTargets, resolveErr = ResolveBindings(c, q.Bindings)
		if resolveErr != nil {
			return ErrorReport(q, "invalid_bindings", resolveErr.Error())
		}
		labels := []string{}
		for _, role := range []string{"client", "server"} {
			if t, yes := componentTargets[role]; yes {
				labels = append(labels, strings.Title(role)+": "+t.Label)
			}
		}
		target = evidence.Target{ID: "selection", Label: strings.Join(labels, " + ")}
		ok = true
	}
	if !ok {
		return ErrorReport(q, "target_not_configured", "Choose Client/Server observability sources, or a legacy --target")
	}
	targetFor := func(def Definition) evidence.Target {
		if q.Bindings != nil {
			return componentTargets[CheckComponent(def)]
		}
		return target
	}
	if q.Bindings != nil {
		selected := []Definition{}
		for _, def := range defs {
			if _, ok := componentTargets[CheckComponent(def)]; ok {
				selected = append(selected, def)
			}
		}
		defs = selected
		if len(defs) == 0 {
			return ErrorReport(q, "component_not_selected", "Provide the Client or Server connection fields required by this check")
		}
	}
	policy := s.Policy
	if q.Threshold != nil {
		if q.Scenario != "" || len(defs) != 1 {
			return ErrorReport(q, "invalid_threshold", "Override a single numeric check only")
		}
		def := defs[0]
		if !validRuleThreshold(def, *q.Threshold) {
			return ErrorReport(q, "unsupported_threshold", "Invalid threshold or this check has no configurable numeric threshold")
		}
		policy.Thresholds = copyThresholds(policy.Thresholds)
		policy.Thresholds[def.ID] = *q.Threshold
		origins := map[string]string{}
		for k, v := range policy.RuleSources {
			origins[k] = v
		}
		origins[def.ID] = "command-line --threshold"
		policy.RuleSources = origins
		defs, _, _ = Selection(q, policy)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(q.BudgetMS)*time.Millisecond)
	defer cancel()
	r := Report{SchemaVersion: ReportSchema, RunID: uuid.NewString(), Status: "completed", Request: q, Target: target, Policy: policy, StartedAt: time.Now().UTC(), Checks: []Result{}, Observations: []d.Observation{}, Problems: []d.Problem{}, Coverage: append([]string{}, gaps...)}
	r.Target.Metadata = evidence.MetadataBinding{}
	r.Target.Milvus = nil
	r.Target.Loki = ""
	r.Target.WPCLI = ""
	r.Summary = Summary{Metrics: SourceSummary{Status: "not_configured"}, Trace: SourceSummary{Status: "not_configured"}, Steps: []StepSummary{}}
	r.Summary.Metrics.Selected = q.Scenario != "" || q.Source == "metrics"
	r.Summary.Trace.Selected = q.Scenario != "" || q.Source == "trace"
	// For a scenario, an explicitly bound enabled source is enough to start. For
	// an atomic check, only its own source can satisfy the configuration preflight.
	traceOK, metricsOK := false, false
	metricRefs, traceRefs := map[string]bool{}, map[string]bool{}
	for _, def := range defs {
		t := targetFor(def)
		ref, source := t.Trace, "trace"
		if def.Source == "metrics" {
			ref, source = metricReference(t, def), "prometheus"
		}
		_, yes := connection(c, source, ref)
		if source == "trace" {
			traceOK = traceOK || yes
			if ref != "" {
				traceRefs[ref] = true
			}
		} else {
			metricsOK = metricsOK || yes
			if ref != "" {
				metricRefs[ref] = true
			}
		}
	}
	refs := func(m map[string]bool) string {
		keys := []string{}
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}
	r.Summary.Metrics.Configured = metricsOK
	r.Summary.Metrics.Connection = refs(metricRefs)
	r.Summary.Trace.Configured = traceOK
	r.Summary.Trace.Connection = refs(traceRefs)
	if metricsOK {
		r.Summary.Metrics.Status = "configured"
	}
	if traceOK {
		r.Summary.Trace.Status = "configured"
	}
	if !r.Summary.Metrics.Selected {
		r.Summary.Metrics.Status = "not_selected"
	}
	if !r.Summary.Trace.Selected {
		r.Summary.Trace.Status = "not_selected"
	}
	missing := q.Scenario != "" && !metricsOK && !traceOK || q.Scenario == "" && ((q.Source == "metrics" && !metricsOK) || (q.Source == "trace" && !traceOK))
	if missing {
		r.Status = "invalid"
		message := "No enabled Metrics or Trace connection is configured for this target. Configure at least one source and select it for Client or Server."
		if q.Source != "" {
			message = "No enabled " + q.Source + " connection is configured for this target. Configure the connection and select it for this component."
		}
		r.Problems = append(r.Problems, d.Problem{Code: "connection_not_configured", Message: message})
		r.FinishedAt = time.Now().UTC()
		return r
	}
	for _, def := range defs {
		t := targetFor(def)
		source, ref := "trace", t.Trace
		if def.Source == "metrics" {
			source, ref = "prometheus", metricReference(t, def)
		}
		if _, configured := connection(c, source, ref); !configured {
			continue
		}
		for _, field := range def.Requires.Parameters {
			if field == "node_id" && t.NodeID == "" {
				return ErrorReport(q, "required_parameter_missing", fmt.Sprintf("%s Metrics node_id is required by %s (%s)", strings.Title(CheckComponent(def)), def.ID, def.Name))
			}
		}
	}
	results := map[string]Result{}
	metricGroups := map[string][]Definition{}
	traceGroups := map[string][]Definition{}
	for _, def := range defs {
		item := baseResult(def)
		target := targetFor(def)
		ref := target.Trace
		source := "trace"
		if def.Source == "metrics" {
			ref = metricReference(target, def)
			source = "prometheus"
		}
		_, enabled := connection(c, source, ref)
		switch {
		case !enabled:
			item.Evidence = evidence.Unknown(def.ID, "source_not_configured", "No enabled "+def.Source+" connection is bound to this target")
			item.Execution = "skipped"
			item.Execution = "skipped"
		default:
			if def.Source == "metrics" {
				key := ref
				if q.Bindings != nil {
					key += "\x00" + CheckComponent(def)
				}
				metricGroups[key] = append(metricGroups[key], def)
			} else {
				key := ref
				if q.Bindings != nil {
					key += "\x00" + CheckComponent(def)
				}
				if q.TraceID == "" {
					key += "\x00" + def.Name
				}
				traceGroups[key] = append(traceGroups[key], def)
			}
		}
		results[def.ID] = item
	}
	keys := []string{}
	for key := range metricGroups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var work sync.WaitGroup
	var resultMu sync.Mutex
	for _, key := range keys {
		key := key
		work.Add(1)
		go func() {
			defer work.Done()
			group := metricGroups[key]
			target := targetFor(group[0])
			ids := []string{}
			for _, def := range group {
				ids = append(ids, def.ID)
				emit(events, def, "collecting")
			}
			ref := strings.Split(key, "\x00")[0]
			conn, _ := connection(c, "prometheus", ref)
			pl := evidence.PlanMetrics(target, ids, false)
			// Server failures also include processor-acquisition failures; routine EOF
			// remains excluded. This matcher is catalog-owned and response-validated.
			for i, query := range pl.Queries {
				if strings.HasPrefix(query.ID, "S01.") || strings.HasPrefix(query.ID, "S08.") {
					delete(query.Match, "status")
					query.Regex = map[string]string{"status": "error|error_get_processor"}
					pl.Queries[i] = query
				}
			}
			var collection prometheus.WALCollection
			if len(pl.Queries) > 0 && ctx.Err() == nil {
				collection = prometheus.CollectChecks(ctx, conn, d.Target{}, q.From, q.To, pl.Queries, d.DefaultLimits(), nil)
			}
			resultMu.Lock()
			r.Observations = append(r.Observations, collection.Observations...)
			r.Problems = append(r.Problems, collection.Problems...)
			resultMu.Unlock()
			observed := map[string]d.Observation{}
			for _, o := range collection.Observations {
				observed[strings.TrimPrefix(o.SourceID, "prometheus:")] = o
			}
			for _, def := range group {
				item := baseResult(def)
				item.Execution = "completed"
				ownQueriesReady := len(pl.Keys[def.ID]) > 0
				for _, key := range pl.Keys[def.ID] {
					o, present := observed[key]
					ownQueriesReady = ownQueriesReady && present && o.Status == "observed"
				}
				if ctx.Err() != nil && !ownQueriesReady {
					item.Execution = "cancelled"
					item.Evidence = evidence.Unknown(def.ID, "cancelled", "Collection cancelled or its budget expired")
				} else if reason := pl.Skip[def.ID]; reason != "" {
					item.Execution = "skipped"
					item.Evidence = evidence.Unknown(def.ID, "scope_missing", reason)
				} else if collection.Status != "completed" {
					// Successful individual queries may still provide results after a partial
					// source failure; a check requires every one of its own query results.
					item.Execution = "completed"
				}
				if item.Execution == "completed" {
					item.Evidence = evidence.Evaluate(rawDefinition(def.ID), pl.Keys[def.ID], collection.Datasets, policy, q.From, q.To)
					for _, query := range pl.Keys[def.ID] {
						o, ok := observed[query]
						if !ok || o.Status != "observed" {
							item.Execution = "failed"
							code := "query_unavailable"
							if o.Code != "" {
								code = o.Code
							}
							item.Evidence = evidence.Unknown(def.ID, code, "A required Metrics query failed or exceeded its source budget")
							break
						}
					}
					if len(pl.Keys[def.ID]) == 0 {
						item.Execution = "skipped"
						item.Evidence = evidence.Unknown(def.ID, "scope_missing", "No verified metric query can be scoped to this target")
					}
				}
				for _, query := range pl.Keys[def.ID] {
					if o, ok := observed[query]; ok {
						item.ObservationIDs = append(item.ObservationIDs, o.ID)
						if o.Query != nil {
							item.Queries = append(item.Queries, *o.Query)
						}
					}
				}
				if def.ID == "W05" && item.Execution == "completed" {
					pendingGrowth(&item, policy)
				}
				if def.ID == "R10" && len(item.Facts) > 0 && item.Execution == "completed" {
					qualified, positive := true, false
					for _, f := range item.Facts {
						qualified = qualified && f.Code == ""
						positive = positive || f.Delta > 0
					}
					if qualified && !positive {
						item.Status = "context"
						item.Code = ""
						item.Value = "0 recorded outcomes"
						item.Note = "Qualified branch counters stayed unchanged; no branch usage or read health is inferred"
					}
				}
				if def.ID == "R10" && len(item.Facts) == 0 && item.Execution == "completed" {
					item.Evidence = evidence.Unknown(def.ID, "no_matching_series", "No matching branch activity series")
				}
				// A no-data/quality replacement must not lose its executed-query provenance.
				item.ObservationIDs = nil
				item.Queries = nil
				for _, key := range pl.Keys[def.ID] {
					if o, ok := observed[key]; ok {
						item.ObservationIDs = append(item.ObservationIDs, o.ID)
						if o.Query != nil {
							item.Queries = append(item.Queries, *o.Query)
						}
					}
				}
				r0 := evidence.Report{SchemaVersion: "wpdiag.wal/v1", Policy: policy, Evidence: []evidence.Evidence{item.Evidence}, Observations: collection.Observations}
				detail, _ := evidence.Inspect(r0, def.ID)
				detail.Description = metricDescription(rawDefinition(def.ID), policy)
				if def.ID == "W05" {
					if item.Status != "unknown" {
						detail.Conclusion = "Pending operations measured; workload demand and write stalls are assessed separately."
					} else {
						detail.Conclusion = item.Note
					}
					if item.Status == "abnormal" {
						detail.Conclusion = "Pending operations increased over a qualified nondecreasing interval."
					}
				}
				if def.ID == "R10" {
					detail.Conclusion = item.Note
				}
				item.Inspect = &Inspection{Inspection: detail}
				finishResult(&item)
				resultMu.Lock()
				results[def.ID] = item
				resultMu.Unlock()
				emit(events, def, "complete")
			}
		}()
	}
	traceSlots := make(chan struct{}, 4)
	for _, traceDefs := range traceGroups {
		traceDefs := traceDefs
		target := targetFor(traceDefs[0])
		work.Add(1)
		go func() {
			defer work.Done()
			select {
			case traceSlots <- struct{}{}:
				defer func() { <-traceSlots }()
			case <-ctx.Done():
			}
			operations := []string{}
			seen := map[string]bool{}
			for _, def := range traceDefs {
				for _, op := range def.Operations {
					if !seen[op] {
						operations = append(operations, op)
						seen[op] = true
					}
				}
				emit(events, def, "collecting")
			}
			conn, _ := connection(c, "trace", target.Trace)
			col := jaeger.Collect(ctx, conn, jaeger.Selection{CandidateKind: traceDefs[0].Kind, MinDurationMS: threshold(traceDefs[0], policy), From: q.From, To: q.To, Flow: traceDefs[0].Flow, Service: target.TraceService, TraceID: q.TraceID, RequestID: q.RequestID, Operations: operations, Identity: map[string]string{"log_ns": target.LogNamespace, "log_id": target.LogID, "node_id": target.NodeID, "reader_name": target.ReaderName}}, d.DefaultLimits())
			resultMu.Lock()
			r.Observations = append(r.Observations, col.Observations...)
			if col.Code != "" {
				duplicate := false
				for _, problem := range r.Problems {
					duplicate = duplicate || problem.Code == col.Code && problem.Message == col.Message
				}
				if !duplicate {
					r.Problems = append(r.Problems, d.Problem{Code: col.Code, Message: col.Message})
				}
			}
			resultMu.Unlock()
			for _, def := range traceDefs {
				item := evaluateTrace(def, col, policy)
				if ctx.Err() != nil {
					item.Execution = "cancelled"
					item.Evidence = evidence.Unknown(def.ID, "cancelled", "Trace collection cancelled or budget expired")
				}
				finishResult(&item)
				resultMu.Lock()
				results[def.ID] = item
				resultMu.Unlock()
				emit(events, def, "complete")
			}
		}()
	}
	work.Wait()
	for _, def := range defs {
		item := results[def.ID]
		finishResult(&item)
		if item.Inspect != nil {
			item.Inspect.Status = item.Status
			item.Inspect.StatusLabel = item.StatusLabel
			item.Inspect.DataState = item.DataState
			item.Inspect.Value = item.Value
		}
		r.Checks = append(r.Checks, item)
	}
	summarize(&r)
	r.Complete = ctx.Err() == nil && r.Status == "completed"
	r.FinishedAt = time.Now().UTC()
	if ctx.Err() != nil {
		r.Status = "cancelled"
		if ctx.Err() == context.DeadlineExceeded {
			r.Status = "budget_exceeded"
		}
		r.Complete = false
	}
	return r
}

func copyThresholds(in map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func pendingGrowth(item *Result, p evidence.Policy) {
	for i, f := range item.Facts {
		if f.Code != "" {
			continue
		}
		best := int64(0)
		start := 0
		for j := 1; j < len(f.Samples); j++ {
			if f.Samples[j].Value < f.Samples[j-1].Value {
				start = j
			}
			if f.Samples[j].Value > f.Samples[start].Value {
				best = max(best, f.Samples[j].TimeMS-f.Samples[start].TimeMS)
			}
		}
		if best >= p.SustainMS {
			item.Status = "abnormal"
			item.Code = ""
			item.Value = fmt.Sprintf("%.0f → %.0f", f.First, f.Last)
			item.Note = "Pending append operations increased over a nondecreasing interval; a write stall is not inferred"
			item.Facts[i].Assessment = "abnormal"
			item.Facts[i].Reading = item.Value
		}
	}
}

func summarize(r *Report) {
	steps := map[string]int{}
	traceIDs, badTraceIDs := map[string]bool{}, map[string]bool{}
	failed, completed := 0, 0
	for _, item := range r.Checks {
		s := &r.Summary.Metrics
		if item.Source == "trace" {
			s = &r.Summary.Trace
		}
		s.Checks++
		if item.Status == "abnormal" {
			s.Abnormal++
		}
		if item.Status == "unknown" {
			s.Inconclusive++
		}
		if item.DataState == "no_data" || item.DataState == "no_completed_samples" {
			s.NoData++
		}
		switch item.Execution {
		case "failed", "cancelled":
			s.Failed++
			failed++
		case "skipped":
			s.Skipped++
		default:
			completed++
		}
		for _, id := range item.TraceIDs {
			traceIDs[id] = true
		}
		for _, id := range item.AbnormalTraceIDs {
			badTraceIDs[id] = true
		}
		index, ok := steps[item.Step]
		if !ok {
			index = len(r.Summary.Steps)
			steps[item.Step] = index
			r.Summary.Steps = append(r.Summary.Steps, StepSummary{ID: item.Step, Checks: []string{}, Abnormal: []string{}, Gaps: []string{}})
		}
		step := &r.Summary.Steps[index]
		step.Checks = append(step.Checks, item.ID)
		if item.Status == "abnormal" {
			step.Abnormal = append(step.Abnormal, item.ID)
		}
		if item.Status == "unknown" || item.Execution != "completed" {
			step.Gaps = append(step.Gaps, item.ID)
		}
	}
	r.Summary.Trace.MatchedTraces = len(traceIDs)
	r.Summary.Trace.AbnormalTraces = len(badTraceIDs)
	for _, s := range []*SourceSummary{&r.Summary.Metrics, &r.Summary.Trace} {
		if s.Configured && s.Failed > 0 {
			s.Status = "partial"
			if s.Failed+s.Skipped == s.Checks {
				s.Status = "failed"
			}
		}
	}
	if failed > 0 {
		r.Status = "partial"
		if completed == 0 {
			r.Status = "failed"
		}
	}
}

func threshold(def Definition, p evidence.Policy) float64 {
	if v, ok := p.Thresholds[def.ID]; ok {
		return v
	}
	if v, ok := p.Thresholds[def.Name]; ok {
		return v
	}
	return *RuleFor(def, p).Threshold
}

func evaluateTrace(def Definition, col jaeger.Collection, p evidence.Policy) Result {
	item := baseResult(def)
	if col.CandidateKind != "" {
		item.SelectionMode = col.CandidateKind + "_candidates"
	}
	item.Execution = "completed"
	item.IdentityScope = col.IdentityScope
	item.Coverage = append(item.Coverage, col.Coverage...)
	item.Evidence = evidence.Evidence{ID: def.ID, Status: "clear", Value: "0 errors", Note: "No verified error flags observed in matching retained spans", ObservationIDs: []string{}}
	ins := &Inspection{Inspection: evidence.Inspection{Description: def.Description, Measurements: []evidence.Measurement{}, Anomalies: []evidence.Measurement{}, Curves: []evidence.Curve{}, ObservationIDs: []string{}}, Spans: []SpanSample{}, Coverage: item.Coverage}
	bound := threshold(def, p)
	if def.Kind == "duration" {
		item.Rule = fmt.Sprintf("Recorded span duration > %g ms", bound)
		ins.Criterion = item.Rule
	}
	var peak float64
	unknown := false
	matchingOperationObserved := false
	matched, bad := map[string]bool{}, map[string]bool{}
	for _, o := range col.Observations {
		if o.Query != nil {
			item.Queries = append(item.Queries, *o.Query)
			item.ObservationIDs = append(item.ObservationIDs, o.ID)
		}
	}
	for _, t := range col.Traces {
		selected := false
		for _, span := range t.Spans {
			if !span.Selected || !has(def.Operations, span.Operation) || col.CandidateKind != "" && span.Service != col.CandidateService {
				continue
			}
			if len(def.Scopes) > 0 && !has(def.Scopes, span.Scope) {
				continue
			}
			if len(def.Scopes) == 0 && def.Role != "" && span.Role != def.Role {
				continue
			}
			sample := SpanSample{TraceID: t.ID, SpanID: span.ID, Service: span.Service, Operation: span.Operation, From: time.UnixMicro(span.StartUS).UTC(), DurationMS: float64(span.DurationUS) / 1000, Error: span.Error, Path: t.Path}
			unverified := has(span.Notes, "error_tag_semantics_unverified") || has(span.Notes, "append_rpc_cancellation_outcome_unverified")
			matchingOperationObserved = true
			if col.CandidateKind == "duration" && sample.DurationMS <= bound {
				continue
			}
			if col.CandidateKind == "errors" && !span.Error && !unverified {
				continue
			}
			selected = true
			matched[t.ID] = true
			if unverified {
				unknown = true
			}
			sample.Abnormal = def.Kind == "duration" && sample.DurationMS > bound || def.Kind == "errors" && span.Error && !unverified
			if sample.Abnormal {
				bad[t.ID] = true
				item.Status = "abnormal"
			}
			peak = math.Max(peak, sample.DurationMS)
			ins.Spans = append(ins.Spans, sample)
			row := evidence.Measurement{Labels: map[string]string{"service": sample.Service, "operation": sample.Operation, "trace_id": t.ID, "span_id": span.ID}, Value: fmt.Sprintf("%g ms", sample.DurationMS), FromMS: sample.From.UnixMilli(), ToMS: sample.From.Add(time.Duration(span.DurationUS) * time.Microsecond).UnixMilli(), Status: "context", Detail: "Recorded sampled span; not a population percentile"}
			if sample.Abnormal {
				row.Status = "abnormal"
				ins.Anomalies = append(ins.Anomalies, row)
			}
			ins.Measurements = append(ins.Measurements, row)
		}
		if selected {
			for _, o := range col.Observations {
				if o.Kind == "jaeger_trace" && o.Target.TraceID == t.ID {
					item.ObservationIDs = append(item.ObservationIDs, o.ID)
				}
			}
		}
	}
	for id := range matched {
		item.TraceIDs = append(item.TraceIDs, id)
	}
	for id := range bad {
		item.AbnormalTraceIDs = append(item.AbnormalTraceIDs, id)
	}
	sort.Strings(item.TraceIDs)
	sort.Strings(item.AbnormalTraceIDs)
	if def.Kind == "duration" {
		item.Value = fmt.Sprintf("Peak %g ms", peak)
		item.Note = "Retained operation durations compared with the configured reference; cause unconfirmed"
		if col.CandidateKind != "" {
			item.Note = "Matching retained slow-span candidates exceed the configured reference; this is not a population latency or a confirmed cause"
		}
	} else {
		count := 0
		for _, s := range ins.Spans {
			if s.Abnormal {
				count++
			}
		}
		item.Value = fmt.Sprintf("%d error spans", count)
		item.Status = "clear"
		if float64(count) > bound {
			item.Status = "abnormal"
		} else {
			item.AbnormalTraceIDs = nil
			ins.Anomalies = nil
			for i := range ins.Spans {
				ins.Spans[i].Abnormal = false
			}
			for i := range ins.Measurements {
				ins.Measurements[i].Status = "context"
			}
		}
		item.Note = fmt.Sprintf("%d verified error spans; configured reference %g; sampled spans do not establish the application outcome", count, bound)
		if col.CandidateKind == "errors" && item.Status != "abnormal" {
			item.Status = "context"
			item.Note += ". Counts cover error=true / otel.status_code=ERROR candidates, not every status convention"
		}
	}
	if len(ins.Spans) == 0 {
		item.Evidence = evidence.Unknown(def.ID, "no_matching_series", "No matching selected spans; sampling, retention, selectors or an idle window may explain absence")
		if col.CandidateKind != "" && col.Status == "completed" && (len(col.Traces) == 0 || matchingOperationObserved) {
			kind := "slow"
			if col.CandidateKind == "errors" {
				kind = "error"
			}
			item.Evidence = evidence.Evidence{ID: def.ID, Status: "context", Code: "no_matching_candidates", Value: "0 matching " + kind + " spans", Note: "Filtered search completed without matching " + kind + " candidates in retained data. No population latency, application activity or overall health was established", ObservationIDs: []string{}}
		}
	}
	if unknown && item.Status != "abnormal" {
		item.Status = "unknown"
		item.Code = "span_error_semantics_unverified"
		item.Note = "Some span error/cancellation semantics are unverified"
	}
	if col.Status != "completed" {
		item.Execution = "failed"
		if item.Status != "abnormal" {
			item.Evidence = evidence.Unknown(def.ID, col.Code, "Trace source is incomplete or failed; no healthy verdict")
		}
	}
	item.ObservationIDs = nil
	for _, o := range col.Observations {
		if o.Query != nil || o.Kind == "jaeger_trace" && matched[o.Target.TraceID] {
			item.ObservationIDs = append(item.ObservationIDs, o.ID)
		}
	}
	ins.ObservationIDs = append([]string{}, item.ObservationIDs...)
	ins.ValueLabel = "Matching retained spans"
	if col.CandidateKind == "duration" {
		ins.ValueLabel = "Peak retained slow-span duration"
	} else if col.CandidateKind == "errors" {
		ins.ValueLabel = "Verified error candidates"
	}
	ins.Conclusion = item.Note
	item.Inspect = ins
	return item
}

func has(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func CloneReport(r Report) Report {
	body, _ := json.Marshal(r)
	var out Report
	_ = json.Unmarshal(body, &out)
	return out
}
