package checks

import (
	"encoding/json"

	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

// Encode applies presentation detail selection after all checks finish. It never
// changes a verdict or re-queries a source. The full in-memory report can be
// reused by a WebUI inspector without invoking a collector again.
func Encode(r Report) []byte {
	r.Checks = append([]Result{}, r.Checks...)
	r.Observations = append(r.Observations[:0:0], r.Observations...)
	for i := range r.Checks {
		r.Checks[i].Facts = nil
		if r.Request.Details == "none" || r.Request.Details == "abnormal" && r.Checks[i].Status != "abnormal" {
			r.Checks[i].Inspect = nil
		}
	}
	if r.Request.Details != "all" {
		for i := range r.Observations {
			r.Observations[i].Data = nil
		}
	}
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil || len(body) > 16<<20 {
		r.Status = "budget_exceeded"
		r.Complete = false
		r.Observations = nil
		r.Coverage = append(r.Coverage, "Output exceeded 16 MiB; evidence withheld. Request a smaller window or less detail.")
		for i, item := range r.Checks {
			r.Checks[i].Evidence = evidence.Unknown(item.ID, "report_output_limit", "Evidence withheld because the report exceeded its output budget")
			r.Checks[i].Execution = "failed"
			r.Checks[i].Inspect = nil
			r.Checks[i].Queries = nil
			r.Checks[i].TraceIDs = nil
			r.Checks[i].AbnormalTraceIDs = nil
			finishResult(&r.Checks[i])
		}
		metrics, trace := r.Summary.Metrics, r.Summary.Trace
		r.Summary = Summary{Metrics: SourceSummary{Selected: metrics.Selected, Configured: metrics.Configured, Connection: metrics.Connection, Status: metrics.Status}, Trace: SourceSummary{Selected: trace.Selected, Configured: trace.Configured, Connection: trace.Connection, Status: trace.Status}, Steps: []StepSummary{}}
		summarize(&r)
		r.Status = "budget_exceeded"
		body, _ = json.MarshalIndent(r, "", "  ")
	}
	return append(body, '\n')
}

func ExitCode(r Report, failOnFindings bool) int {
	switch r.Status {
	case "invalid":
		for _, p := range r.Problems {
			if p.Code == "configuration_invalid" || p.Code == "connection_not_configured" || p.Code == "target_not_configured" {
				return 12
			}
		}
		return 2
	case "budget_exceeded":
		return 5
	case "partial":
		return 6
	case "cancelled":
		return 7
	case "failed":
		return 1
	}
	if failOnFindings {
		for _, item := range r.Checks {
			if item.Status == "abnormal" {
				return 8
			}
		}
	}
	return 0
}
