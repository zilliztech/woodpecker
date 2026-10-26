package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func (Module) Run(ctx context.Context, r d.Request, connection d.Connection) d.Result {
	return runPlan(ctx, r, connection, defaultPlan)
}

type queryPlanKind int

const (
	defaultPlan queryPlanKind = iota
	queryPlan
	appendPlan
	persistencePlan
	consumePlan
)

func runPlan(ctx context.Context, r d.Request, connection d.Connection, planKind queryPlanKind) d.Result {
	s, err := settingsFor(connection, planKind)
	if err != nil {
		return d.Failure("prometheus", "invalid", "invalid_connection_settings", "Check the source-owned Prometheus settings")
	}
	if r.Selection.Target.Deployment != "" && r.Selection.Target.Deployment != s.Deployment {
		return d.Failure("prometheus", "invalid", "deployment_binding_mismatch", "Selected deployment does not match the explicit connection binding")
	}
	r.Selection.Target.Deployment = s.Deployment
	var from, to time.Time
	if r.Selection.Window.Mode == "historical" && r.Selection.Window.From != nil && r.Selection.Window.To != nil {
		from, to = *r.Selection.Window.From, *r.Selection.Window.To
	} else if r.Selection.Window.Mode == "live" {
		from = time.Now().UTC().Truncate(time.Millisecond)
		to = from.Add(time.Duration(r.Selection.Window.DurationMS) * time.Millisecond)
	} else {
		return d.Failure("prometheus", "unsupported", "window_mode_not_supported", "Use an explicit historical range or bounded live window; snapshot cannot establish interval progress")
	}
	if to.Sub(from) < time.Millisecond || to.Sub(from).Milliseconds() > s.Limits.MaxWindowMS {
		return d.Failure("prometheus", "invalid", "window_budget_invalid", "Interval must be positive and fit max_window_ms")
	}
	if r.Selection.Window.Mode == "historical" && to.After(time.Now()) {
		return d.Failure("prometheus", "invalid", "future_historical_window", "Historical queries require a retained past interval")
	}
	if r.Selection.Window.IntervalMS > 0 {
		s.Policy.MaxGapMS = min(s.Policy.MaxGapMS, r.Selection.Window.IntervalMS)
	}
	plan := signals(r.Selection.Scope, r.Selection.Flow)
	if planKind == queryPlan {
		plan = querySignals()
	} else if planKind == appendPlan {
		plan = appendSignals(r.Selection.Scope)
	} else if planKind == persistencePlan {
		plan = persistenceSignals(r.Selection.Scope)
	} else if planKind == consumePlan {
		plan = consumeSignals(r.Selection.Scope)
	}
	if len(plan) > s.Limits.MaxQueries {
		return d.Failure("prometheus", "invalid", "query_budget_invalid", "Profile does not fit max_queries; no requests were sent")
	}
	token, user, password, err := auth(s)
	if err != nil {
		return d.Failure("prometheus", "failed", "authentication_unavailable", "Configured authentication environment reference is unavailable; values withheld")
	}
	if r.Selection.Window.Mode == "live" {
		if deadline, ok := ctx.Deadline(); ok && !deadline.After(to) {
			return d.Failure("prometheus", "invalid", "live_budget_too_short", "Execution budget must cover the full live window plus queries")
		}
		timer := time.NewTimer(time.Until(to))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return d.Failure("prometheus", "cancelled", "cancelled", "Live collection cancelled before its bounded window completed")
		case <-timer.C:
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	c := client{http: &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, settings: s, token: token, user: user, password: password}
	result := d.NewResult("prometheus")
	result.Status = "completed"
	budget := queryBudget{targets: map[string]bool{}}
	runID := uuid.NewString()
	okQueries := 0
	for index, sig := range plan {
		if ctx.Err() != nil {
			break
		}
		if budget.bytes >= r.Limits.MaxInputBytes {
			result.Status = "budget_exceeded"
			result.Gaps = append(result.Gaps, d.Gap{Code: "input_budget_exceeded", Detail: "Total HTTP input budget exhausted; remaining queries were not executed"})
			break
		}
		expr := expression(s, sig, r.Selection.Target, from, to)
		id := runID + ":q" + strconv.Itoa(index)
		started := time.Now().UTC()
		data, code := c.query(ctx, expr, sig, r.Selection.Target, from, to, &budget, r.Limits)
		finished := time.Now().UTC()
		observation := d.Observation{ID: id, SourceID: "prometheus:" + sig.ID, Kind: "prometheus_matrix", Target: d.Target{Deployment: s.Deployment}, StartedAt: started, FinishedAt: finished, Status: "observed", Query: &d.SourceQuery{Language: "promql", Expression: expr, Executed: true}}
		if code != "" {
			observation.Status = "unavailable"
			observation.Code = code
			result.Gaps = append(result.Gaps, d.Gap{Code: code, ObservationID: id, Detail: "Source response could not be used; error bodies and endpoint credentials withheld"})
		} else {
			okQueries++
			findings := evaluate(&data, sig, r, s.Policy, id)
			if planKind == queryPlan || planKind == appendPlan || planKind == persistencePlan {
				annotateExporters(&data)
			}
			if planKind == consumePlan {
				annotateConsume(&data)
			}
			result.Findings = append(result.Findings, findings...)
			for _, f := range findings {
				for _, gap := range f.Unknown {
					result.Gaps = append(result.Gaps, d.Gap{Code: gap, ObservationID: id, Detail: "Applies to " + f.ID})
				}
			}
		}
		observation.Data, _ = json.Marshal(data)
		result.Observations = append(result.Observations, observation)
		if code != "" || d.Has(data.Notes, "backend_annotations_withheld") {
			result.Status = "partial"
		}
		if isBudget(code) {
			result.Status = "budget_exceeded"
			break
		}
	}
	if result.Status == "partial" && okQueries == 0 {
		result.Status = "failed"
	}
	if ctx.Err() != nil {
		result.Status = "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Status = "budget_exceeded"
		}
	}
	if r.Selection.Scope == "e2e" && planKind == defaultPlan {
		result.Gaps = append(result.Gaps, d.Gap{Code: "source_only_partial_stage_coverage", Detail: "Profiles compare independent exporter populations. They do not establish request/channel/log joins, SN consumption/visibility, query execution, or cross-source causality; stage rules remain #463–#466"})
	}
	result.Metadata, _ = json.Marshal(struct {
		Schema     string    `json:"source_schema_version"`
		RunID      string    `json:"source_run_id"`
		Deployment string    `json:"bound_deployment"`
		From       time.Time `json:"from"`
		To         time.Time `json:"to"`
		Semantics  string    `json:"window_semantics"`
		Policy     Policy    `json:"policy"`
		InputBytes int64     `json:"input_bytes"`
		Series     int       `json:"returned_series_before_deduplication"`
		Samples    int       `json:"returned_samples_before_deduplication"`
		Queries    int       `json:"executed_queries"`
	}{"wpdiag.prometheus/v1", runID, s.Deployment, from, to, "Raw retained scrapes in (from,to]; deltas cover actual first/last samples, without extrapolation or resampling", s.Policy, budget.bytes, budget.series, budget.samples, len(result.Observations)})
	return result
}
