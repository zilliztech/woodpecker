package jaeger

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type candidateSearch struct {
	operation   string
	minDuration time.Duration
	tags        map[string]string
}
type candidateUsage struct {
	returnedTraces    int
	ref               string
	started, finished time.Time
}

func candidateSearches(s Settings, q Selection) ([]candidateSearch, string) {
	if q.CandidateKind == "" || q.TraceID != "" {
		return []candidateSearch{{operation: s.Search.Operation}}, ""
	}
	if q.CandidateKind != "duration" && q.CandidateKind != "errors" {
		return nil, "invalid_candidate_search"
	}
	if q.CandidateKind == "duration" && (q.MinDurationMS <= 0 || q.MinDurationMS > 86400000 || math.IsNaN(q.MinDurationMS) || math.IsInf(q.MinDurationMS, 0)) {
		return nil, "invalid_candidate_threshold"
	}
	operations := q.Operations
	if s.Search.Operation != "" {
		operations = []string{s.Search.Operation}
	}
	seen := map[string]bool{}
	plans := []candidateSearch{}
	for _, op := range operations {
		if op == "" || seen[op] {
			continue
		}
		seen[op] = true
		if q.CandidateKind == "duration" {
			// Round down to nanoseconds so the server filter cannot exclude a span
			// that exceeds a fractional-millisecond local threshold.
			plans = append(plans, candidateSearch{operation: op, minDuration: time.Duration(q.MinDurationMS * float64(time.Millisecond))})
		} else {
			plans = append(plans, candidateSearch{operation: op, tags: map[string]string{"error": "true"}}, candidateSearch{operation: op, tags: map[string]string{"otel.status_code": "ERROR"}})
		}
	}
	if len(plans) == 0 {
		return nil, "candidate_operation_required"
	}
	return plans, ""
}

func (c client) searchCandidates(ctx context.Context, r d.Request, from, to time.Time, plans []candidateSearch) ([]Trace, []d.Observation, string, candidateUsage) {
	usage := candidateUsage{ref: uuid.NewString(), started: time.Now().UTC()}
	observations := []d.Observation{}
	merged := []Trace{}
	indices := map[string]int{}
	shared := budget{targets: map[string]bool{}}
	spanCount := 0
	for i, plan := range plans {
		one := c
		one.settings.Search.Operation = plan.operation
		one.settings.candidateMinDuration = plan.minDuration
		one.settings.candidateTags = plan.tags
		// Bytes and queries account for actual input, including repeated full traces.
		// Trace/span limits below account for the union, not duplicate OR/alias matches.
		b := budget{bytes: shared.bytes, queries: shared.queries, targets: shared.targets}
		started := time.Now().UTC()
		traces, code, expr := one.query(ctx, r, from, to, &b)
		finished := time.Now().UTC()
		shared.bytes, shared.queries = b.bytes, b.queries
		usage.returnedTraces = b.returnedTraces
		usage.finished = finished
		o := d.Observation{ID: fmt.Sprintf("%s:query:%d", usage.ref, i), SourceID: "trace:query", Kind: "jaeger_query", StartedAt: started, FinishedAt: finished, Status: "observed", Query: &d.SourceQuery{Language: "jaeger-query-v1", Expression: expr, Executed: b.queries > 0}}
		if code != "" {
			o.Status = "unavailable"
			o.Code = code
			observations = append(observations, o)
			return nil, observations, code, usage
		}
		observations = append(observations, o)
		for _, trace := range traces {
			if at, ok := indices[trace.ID]; ok {
				existing := &merged[at]
				bySpan := map[string]int{}
				for j, span := range existing.Spans {
					bySpan[span.ID] = j
				}
				for _, span := range trace.Spans {
					if j, ok := bySpan[span.ID]; ok {
						old := existing.Spans[j]
						left, right := old, span
						left.Selected, right.Selected = false, false
						left.RequestAnchor, right.RequestAnchor = false, false
						left.Notes, right.Notes = nil, nil
						if !reflect.DeepEqual(left, right) {
							return nil, observations, "conflicting_duplicate_span", usage
						}
						old.Selected = old.Selected || span.Selected
						old.RequestAnchor = old.RequestAnchor || span.RequestAnchor
						for _, note := range span.Notes {
							old.Notes = appendUnique(old.Notes, note)
						}
						existing.Spans[j] = old
						continue
					}
					bySpan[span.ID] = len(existing.Spans)
					existing.Spans = append(existing.Spans, span)
					spanCount++
				}
				for _, note := range trace.Notes {
					existing.Notes = appendUnique(existing.Notes, note)
				}
			} else {
				indices[trace.ID] = len(merged)
				merged = append(merged, trace)
				spanCount += len(trace.Spans)
			}
			code = ""
			if len(merged) > c.settings.Limits.MaxTraces {
				code = "trace_budget_exceeded"
				usage.returnedTraces = len(merged)
			} else if spanCount > c.settings.Limits.MaxSpans {
				code = "span_budget_exceeded"
			}
			if code != "" {
				observations[len(observations)-1].Status = "unavailable"
				observations[len(observations)-1].Code = code
				return nil, observations, code, usage
			}
		}
	}
	return merged, observations, "", usage
}
