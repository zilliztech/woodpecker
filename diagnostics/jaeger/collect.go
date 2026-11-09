package jaeger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// Selection narrows a source query. Operations are supplied by the verified
// check catalog, never arbitrary browser input. No legacy composite evaluator
// is called by Collect.
type Selection struct {
	From, To                          time.Time
	Flow, Service, TraceID, RequestID string
	CandidateKind                     string // duration or errors: a source-side candidate search.
	MinDurationMS                     float64
	Operations                        []string
	Identity                          map[string]string
}
type Collection struct {
	CandidateService      string
	Traces                []Trace
	Observations          []d.Observation
	Status, Code, Message string
	CandidateKind         string
	IdentityScope         string
	Coverage              []string
}

func Collect(ctx context.Context, connection d.Connection, q Selection, limits d.Limits) Collection {
	out := Collection{Status: "completed", Traces: []Trace{}, Observations: []d.Observation{}, IdentityScope: "service", Coverage: []string{}}
	fail := func(code, message string) Collection {
		out.Status = "failed"
		out.Code = code
		out.Message = message
		return out
	}
	s, err := settings(connection)
	if err != nil {
		return fail("invalid_connection_settings", "Check source-owned Jaeger connection settings")
	}
	if q.From.IsZero() || !q.From.Before(q.To) || q.To.After(time.Now()) || q.To.Sub(q.From).Milliseconds() > s.Limits.MaxWindowMS {
		return fail("window_budget_invalid", "Incident window exceeds the configured Jaeger maximum or is invalid; it was not shortened")
	}
	if q.Service != "" {
		s.Search.Service = q.Service
	}
	if q.TraceID == "" && s.Services[s.Search.Service] == "" {
		return fail("trace_service_unbound", "Configure an exact trace search service bound in services, or target.trace_service")
	}
	s.atomicOperations = map[string]bool{}
	for _, op := range q.Operations {
		s.atomicOperations[op] = true
	}
	if len(q.Operations) == 1 && s.Search.Operation == "" {
		s.Search.Operation = q.Operations[0]
	}
	if q.TraceID == "" && s.Search.Operation != "" && !s.atomicOperations[s.Search.Operation] {
		return fail("operation_selector_mismatch", "Configured search operation does not belong to the selected checks")
	}
	if q.TraceID != "" {
		id, ok := canonicalID(q.TraceID, 32)
		if !ok {
			return fail("invalid_trace_id", "Trace ID must be nonzero 16/32-digit hexadecimal")
		}
		q.TraceID = id
		out.IdentityScope = "exact_trace"
	}
	if len(q.RequestID) > 512 || strings.ContainsAny(q.RequestID, "\x00\r\n") {
		return fail("invalid_request_id", "Request ID must be a bounded literal value")
	}
	if q.RequestID != "" && s.RequestIDTag == "" {
		return fail("request_id_tag_unconfigured", "Configure request_id_tag before selecting a request ID")
	}
	if s.MatchTags == nil {
		s.MatchTags = map[string]string{}
	}
	bound, total := 0, 0
	for logical, value := range q.Identity {
		if value == "" {
			continue
		}
		total++
		if key := s.IdentityTags[logical]; key != "" {
			if old, ok := s.MatchTags[key]; ok && old != value {
				return fail("identity_binding_conflict", "Target identity conflicts with configured trace match tags")
			}
			s.MatchTags[key] = value
			bound++
		}
	}
	if total > 0 && bound == total {
		out.IdentityScope = "mapped_target_tags"
	} else {
		out.Coverage = append(out.Coverage, "Trace evidence is scoped to configured service/tags; the selected WP log/node identity is not fully mapped. It is not automatically correlated with Metrics.")
	}
	if s.Search.Operation != "" {
		out.Coverage = append(out.Coverage, "Trace search is restricted to configured operation "+s.Search.Operation)
	}
	token, user, password, err := auth(s)
	if err != nil {
		return fail("authentication_secret_unavailable", "Configured authentication environment reference is unavailable")
	}
	transport, err := d.HTTPTransport(s.TLS, connection.BaseDir)
	if err != nil {
		return fail("tls_configuration_unavailable", err.Error())
	}
	defer transport.CloseIdleConnections()
	c := client{http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, settings: s, token: token, user: user, password: password}
	r := d.Request{Selection: d.Selection{Scope: "e2e", Flow: q.Flow, Window: d.Window{Mode: "historical", From: &q.From, To: &q.To}, Target: d.Target{TraceID: q.TraceID, OperationID: q.RequestID}}, Limits: limits}
	plans, code := candidateSearches(s, q)
	if code != "" {
		return fail(code, "The selected checks and source search filters cannot form a bounded candidate query")
	}
	if q.CandidateKind != "" && q.TraceID == "" {
		out.CandidateKind = q.CandidateKind
		out.CandidateService = s.Search.Service
		if q.CandidateKind == "duration" {
			out.Coverage = append(out.Coverage, fmt.Sprintf("Server-side candidate search: exact service/operation and minDuration=%g ms. Returned full traces remain context; only matching spans above the reference are assessed.", q.MinDurationMS))
		} else {
			out.Coverage = append(out.Coverage, "Server-side error candidate search: exact service/operation and error=true OR otel.status_code=ERROR. This does not exhaust other status-tag conventions or uninstrumented failures.")
		}
	}
	if len(plans) > s.Limits.MaxQueries {
		return fail("query_budget_exceeded", fmt.Sprintf("This check needs %d filtered Jaeger queries; the configured max_queries is %d. Use an exact operation or Trace ID, or review the query budget.", len(plans), s.Limits.MaxQueries))
	}
	traces, observations, code, usage := c.searchCandidates(ctx, r, q.From, q.To, plans)
	out.Observations = observations
	if code != "" {
		out.Status = "failed"
		out.Code = code
		out.Message = "Jaeger query is incomplete or unusable; no healthy result is inferred"
		if budgetCode(code) {
			out.Status = "budget_exceeded"
			out.Message = fmt.Sprintf("Jaeger candidate search exceeded its configured budget (%s). Narrow the incident interval or use an exact operation, request ID or Trace ID; no truncated result was assessed.", code)
			if code == "trace_budget_exceeded" {
				out.Message = fmt.Sprintf("Jaeger candidate search returned %d trace candidates; max_traces=%d. This is a result-count limit, not a one-hour window limit. Narrow the incident interval or refine the check threshold/operation, request ID or Trace ID; no truncated result was assessed.", usage.returnedTraces, s.Limits.MaxTraces)
			}
		}
		return out
	}
	for _, t := range traces {
		if len(t.Notes) > 0 {
			out.Coverage = append(out.Coverage, t.Notes...)
		}
		if d.Has(t.Notes, "backend_errors_withheld") || d.Has(t.Notes, "backend_trace_warnings_withheld") {
			out.Status = "partial"
		}
		data, _ := json.Marshal(t)
		out.Observations = append(out.Observations, d.Observation{ID: usage.ref + ":trace:" + t.ID, SourceID: "trace:retained", Kind: "jaeger_trace", Target: d.Target{TraceID: t.ID}, StartedAt: usage.started, FinishedAt: usage.finished, Status: "observed", Data: data})
	}
	out.Traces = traces
	out.Coverage = append(out.Coverage, fmt.Sprintf("Selected span start in [%s,%s]; spans outside the window are context. Sampling and retention limit coverage.", q.From.Format(time.RFC3339Nano), q.To.Format(time.RFC3339Nano)))
	return out
}
