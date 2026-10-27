package integrated

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func (r *Runner) Run(ctx context.Context, c d.Config, q Request, events chan<- Event) Report {
	started := time.Now().UTC()
	result := Report{SchemaVersion: Schema, RunID: uuid.NewString(), StartedAt: started, Runs: []Run{}}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(q.Limits.BudgetMS)*time.Millisecond)
	defer cancel()
	emit := func(id, status string) {
		if events != nil {
			select {
			case events <- Event{RunID: result.RunID, ModuleID: id, Status: status, At: time.Now().UTC()}:
			default:
			}
		}
	}
	result.Plan = r.Plan(c, q)
	emit("", "planned")
	if len(result.Plan.Problems) > 0 {
		result.Status = "invalid"
		result.Summary = Summarize(ctx, nil)
		result.FinishedAt = time.Now().UTC()
		emit("", result.Status)
		return result
	}
	runs := make([]Run, len(result.Plan.Entries))
	var wg sync.WaitGroup
	for i, entry := range result.Plan.Entries {
		wg.Add(1)
		go func(i int, e Entry) {
			defer wg.Done()
			emit(e.ID, "started")
			var report d.Report
			if e.Status != "ready" {
				failure := d.NewResult(e.Source)
				failure.Status = e.Status
				failure.Connection = e.Request.Connection
				failure.Profile = e.Profile.ID
				failure.Problems = e.Problems
				report = d.Report{SchemaVersion: d.ReportSchema, RunID: uuid.NewString(), Selection: e.Request.Selection, Limits: e.Request.Limits, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Status: e.Status, Results: []d.Result{failure}}
			} else {
				def := r.definition(e.ID)
				engine, _ := d.NewEngine(def.Module)
				report = engine.Run(ctx, c, e.Request)
				if def.Assess != nil && !d.Has([]string{"invalid", "unsupported", "disabled"}, report.Status) {
					a := def.Assess(ctx, report)
					report.Assessments = []d.Assessment{a}
					if a.Status == "failed" && report.Status == "completed" {
						report.Status = "failed"
					}
				}
			}
			// Use the same standalone encoder/output bound before nesting evidence.
			body, _, err := d.EncodeReport(report)
			if err == nil {
				err = decodeChild(body, &report)
			}
			if err != nil {
				report = d.Report{SchemaVersion: d.ReportSchema, RunID: uuid.NewString(), Selection: e.Request.Selection, Status: "failed", Results: []d.Result{d.Failure(e.Source, "failed", "invalid_module_report", "Module report could not be encoded")}}
			}
			runs[i] = Run{ModuleID: e.ID, Role: e.Role, Report: report}
			emit(e.ID, report.Status)
		}(i, entry)
	}
	wg.Wait()
	result.Runs = runs
	statuses := []string{}
	for _, run := range runs {
		statuses = append(statuses, run.Report.Status)
	}
	result.Status = aggregate(statuses)
	result.Summary = Summarize(ctx, runs)
	for _, gap := range result.Summary.Gaps {
		if gap.Code == "invalid_report_lineage" || gap.Code == "conflicting_module_identity" {
			if result.Status == "completed" {
				result.Status = "failed"
			}
		}
	}
	if ctx.Err() != nil {
		result.Status = "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Status = "budget_exceeded"
		}
	}
	result.FinishedAt = time.Now().UTC()
	emit("", result.Status)
	return result
}
