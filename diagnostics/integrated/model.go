// Package integrated orchestrates source collectors and stage assessors without
// owning their diagnostic rules. Reports retain complete standalone lineage.
package integrated

import (
	"context"
	"encoding/json"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

const Schema = "wpdiag.run/v1"

type Request struct {
	Selection     d.Selection         `json:"selection"`
	Modules       []string            `json:"modules,omitempty"`
	Stages        []string            `json:"stages,omitempty"`
	Connection    string              `json:"connection"`
	Connections   map[string]string   `json:"connections,omitempty"`
	Targets       map[string]d.Target `json:"source_targets,omitempty"`
	CurrentNative bool                `json:"current_native_context,omitempty"`
	Limits        d.Limits            `json:"limits"`
}

// Definition is also the extension point for independently owned modules. Run
// must honor ctx and the declared input budget, as all standard modules do.
type Definition struct {
	ID       string                                       `json:"id"`
	Source   string                                       `json:"source"`
	Stage    string                                       `json:"stage,omitempty"`
	Profiles []d.Profile                                  `json:"profiles"`
	Module   d.Module                                     `json:"-"`
	Assess   func(context.Context, d.Report) d.Assessment `json:"-"`
}

type Entry struct {
	ID       string      `json:"id"`
	Source   string      `json:"source"`
	Role     string      `json:"role"`
	Request  d.Request   `json:"request"`
	Profile  d.Profile   `json:"profile"`
	Status   string      `json:"status"`
	Problems []d.Problem `json:"problems"`
}

type Plan struct {
	Schema    string      `json:"schema_version"`
	Request   Request     `json:"request"`
	Status    string      `json:"status"`
	Entries   []Entry     `json:"entries"`
	Omitted   []d.Problem `json:"auto_omissions"`
	Problems  []d.Problem `json:"problems"`
	Semantics []string    `json:"semantics"`
}

type Run struct {
	ModuleID string   `json:"module_id"`
	Role     string   `json:"role"`
	Report   d.Report `json:"report"`
}

type Reference struct {
	ModuleID string `json:"module_id"`
	ID       string `json:"id"`
	EventID  string `json:"event_id,omitempty"`
	SpanID   string `json:"span_id,omitempty"`
}

// View points to original findings. It does not assign a new confidence, copy a
// source rule, add quantiles or turn other populations into contradictions.
type View struct {
	Reference
	Stage   string `json:"stage"`
	Kind    string `json:"kind"`
	Context string `json:"context"`
}

type Association struct {
	ID           string      `json:"id"`
	Kind         string      `json:"kind"`
	Identity     string      `json:"identity"`
	Findings     []Reference `json:"findings"`
	Observations []Reference `json:"observations"`
	Unknown      []string    `json:"unknown"`
}

type Summary struct {
	Schema       string        `json:"schema_version"`
	Findings     []View        `json:"findings"`
	Associations []Association `json:"associations"`
	Gaps         []d.Gap       `json:"coverage_gaps"`
}

type Report struct {
	SchemaVersion string    `json:"schema_version"`
	RunID         string    `json:"run_id"`
	Plan          Plan      `json:"plan"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	Status        string    `json:"status"`
	Runs          []Run     `json:"runs"`
	Summary       Summary   `json:"summary"`
}

// Event delivery is advisory and nonblocking. The caller owns the channel and
// reads final statuses from Report even if progress events were dropped.
type Event struct {
	RunID    string    `json:"run_id"`
	ModuleID string    `json:"module_id,omitempty"`
	Status   string    `json:"status"`
	At       time.Time `json:"at"`
}

func Encode(report Report) ([]byte, int, error) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, 1, err
	}
	limit := report.Plan.Request.Limits.MaxOutputBytes
	if limit < 4096 || limit > 64<<20 {
		limit = d.DefaultLimits().MaxOutputBytes
	}
	if int64(len(data))+1 > limit {
		report.Status = "budget_exceeded"
		report.Plan = Plan{Schema: "wpdiag.plan/v1", Status: "budget_exceeded", Problems: []d.Problem{{Code: "report_output_limit", Message: "Integrated report exceeded its byte budget; oversized evidence was withheld"}}}
		report.Runs = []Run{}
		report.Summary = Summary{Schema: "wpdiag.summary/v1", Findings: []View{}, Associations: []Association{}, Gaps: []d.Gap{}}
		data, err = json.MarshalIndent(report, "", "  ")
	}
	return append(data, '\n'), d.ExitCode(report.Status), err
}
