package jaeger

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func (Module) Run(ctx context.Context, r d.Request, connection d.Connection) d.Result {
	s, err := settings(connection)
	if err != nil {
		return d.Failure("trace", "invalid", "invalid_connection_settings", "Check documented Jaeger settings")
	}
	if r.Selection.Target.Deployment != "" && r.Selection.Target.Deployment != s.Deployment {
		return d.Failure("trace", "invalid", "deployment_binding_mismatch", "Selected deployment differs from this configured source")
	}
	if r.Selection.Target.OperationID != "" && s.RequestIDTag == "" {
		return d.Failure("trace", "unsupported", "request_id_tag_unconfigured", "Configure the exact request-ID tag before selecting --request-id/--operation-id")
	}
	if r.Selection.Target.TraceID == "" && r.Selection.Window.Mode == "snapshot" {
		return d.Failure("trace", "unsupported", "snapshot_requires_trace_id", "Snapshot retrieves an exact retained trace; search requires a live/historical window")
	}
	if r.Selection.Target.TraceID == "" && s.Search.Service == "" {
		return d.Failure("trace", "invalid", "search_service_required", "Configure a bounded service/operation search or select an exact trace ID")
	}
	if r.Selection.Window.Mode != "snapshot" && r.Selection.Window.DurationMS > s.Limits.MaxWindowMS {
		return d.Failure("trace", "invalid", "window_budget_invalid", "Requested live window exceeds this source budget")
	}
	token, user, password, err := auth(s)
	if err != nil {
		return d.Failure("trace", "invalid", "authentication_secret_unavailable", "Authentication environment reference is unavailable")
	}
	to := time.Now().UTC()
	from := to
	if r.Selection.Window.Mode == "historical" {
		from, to = *r.Selection.Window.From, *r.Selection.Window.To
		if to.After(time.Now()) || to.Sub(from).Milliseconds() > s.Limits.MaxWindowMS {
			return d.Failure("trace", "invalid", "historical_window_invalid", "Use a retained non-future interval within the configured maximum")
		}
	}
	if r.Selection.Window.Mode == "live" {
		from = to
		to = from.Add(time.Duration(r.Selection.Window.DurationMS) * time.Millisecond)
		wait := time.Duration(r.Selection.Window.DurationMS+s.Limits.ExportDelayMS) * time.Millisecond
		if wait >= time.Duration(r.Limits.BudgetMS)*time.Millisecond {
			return d.Failure("trace", "invalid", "live_budget_too_short", "Budget must cover the live window, export delay and collection")
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return d.Failure("trace", "cancelled", "cancelled", "Live trace collection interrupted before the window completed")
		case <-timer.C:
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	c := client{http: &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, settings: s, token: token, user: user, password: password}
	b := &budget{targets: map[string]bool{}}
	started := time.Now().UTC()
	traces, code, expression := c.query(ctx, r, from, to, b)
	finished := time.Now().UTC()
	result := d.NewResult("trace")
	result.Status = "completed"
	runID := uuid.NewString()
	queryID := runID + ":query"
	query := d.Observation{ID: queryID, SourceID: runID + ":jaeger", Kind: "jaeger_query", StartedAt: started, FinishedAt: finished, Status: "observed", Query: &d.SourceQuery{Language: "jaeger-query-v1", Expression: expression, Executed: b.queries > 0}}
	if code != "" {
		query.Status, query.Code = "unavailable", code
		result.Status = "failed"
		if budgetCode(code) {
			result.Status = "budget_exceeded"
		}
		result.Gaps = append(result.Gaps, d.Gap{Code: code, ObservationID: queryID, Detail: "Jaeger query was incomplete or unusable; endpoint, credentials, error bodies and arbitrary span attributes withheld"})
	}
	result.Observations = append(result.Observations, query)
	for _, trace := range traces {
		if ctx.Err() != nil {
			break
		}
		id := runID + ":trace:" + trace.ID
		findings := evaluate(ctx, r, s, &trace, id)
		data, _ := json.Marshal(trace)
		result.Observations = append(result.Observations, d.Observation{ID: id, SourceID: runID + ":jaeger", Kind: "jaeger_trace", Target: d.Target{TraceID: trace.ID, Identity: "retained_trace"}, StartedAt: started, FinishedAt: finished, Status: "observed", Data: data})
		result.Findings = append(result.Findings, findings...)
		for _, f := range findings {
			for _, gap := range f.Unknown {
				result.Gaps = append(result.Gaps, d.Gap{Code: gap, ObservationID: id, Detail: "Applies to " + f.ID})
			}
		}
		if d.Has(trace.Notes, "backend_errors_withheld") {
			result.Status = "partial"
		}
	}
	if code == "" && len(traces) == 0 {
		f := finding(r, Trace{}, queryID, "trace.no_retained_evidence")
		f.Summary = "No retained trace matched; sampling, export delay, retention, selectors or an idle window can explain absence"
		f.Unknown = append(f.Unknown, "no_match_is_not_healthy")
		result.Findings = append(result.Findings, f)
		result.Gaps = append(result.Gaps, d.Gap{Code: "no_retained_trace_evidence", ObservationID: queryID, Detail: f.Summary})
	}
	if ctx.Err() != nil {
		result.Status = "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Status = "budget_exceeded"
		}
	}
	result.Metadata, _ = json.Marshal(struct {
		Schema         string     `json:"source_schema_version"`
		RunID          string     `json:"source_run_id"`
		Deployment     string     `json:"configured_deployment"`
		Backend        string     `json:"backend_profile"`
		Mode           string     `json:"window_mode"`
		From           *time.Time `json:"from,omitempty"`
		To             *time.Time `json:"to,omitempty"`
		Semantics      string     `json:"window_semantics"`
		InputBytes     int64      `json:"input_bytes"`
		Queries        int        `json:"executed_queries"`
		ReturnedTraces int        `json:"returned_traces_before_deduplication"`
		ReturnedSpans  int        `json:"returned_spans_before_deduplication"`
		Policy         Policy     `json:"policy"`
	}{
		"wpdiag.jaeger/v1", runID, s.Deployment, "Jaeger Query v1 JSON (1.53 compatibility profile)", r.Selection.Window.Mode,
		windowEndpoint(from, r.Selection.Window.Mode), windowEndpoint(to, r.Selection.Window.Mode), "Snapshot: exact retained trace without event-time filtering. Live/historical: selected span start in [from,to], with other spans retained as context; live fetch follows the window/export delay, without polling.", b.bytes, b.queries, b.traces, b.spans, s.Policy,
	})
	return result
}

func windowEndpoint(t time.Time, mode string) *time.Time {
	if mode == "snapshot" {
		return nil
	}
	return &t
}

func budgetCode(code string) bool {
	return d.Has([]string{"query_budget_exceeded", "trace_budget_exceeded", "span_budget_exceeded", "reference_budget_exceeded", "target_budget_exceeded", "input_budget_exceeded"}, code)
}
