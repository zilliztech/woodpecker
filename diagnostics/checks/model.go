// Package checks executes source-local checks and aggregates their unchanged
// results into WAL scenarios. It has no CLI, HTTP, Agent or native-WP dependency.
package checks

import (
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

const ReportSchema = "wp.diag/v1"

type Definition struct {
	ID          string               `json:"id"`
	Name        string               `json:"name"`
	Source      string               `json:"source"`
	Step        string               `json:"step"`
	Title       string               `json:"title"`
	Flow        string               `json:"flow"`
	Kind        string               `json:"kind"`
	RuleVersion int                  `json:"rule_version"`
	Metrics     []string             `json:"metrics,omitempty"`
	Operations  []string             `json:"operations,omitempty"`
	Scopes      []string             `json:"scopes,omitempty"`
	Requires    CheckRequirements    `json:"requires"`
	Component   string               `json:"component"`
	Role        string               `json:"role,omitempty"`
	References  []string             `json:"references"`
	Description evidence.Description `json:"description"`
	Assessment  AssessmentRule       `json:"assessment"`
	Limits      string               `json:"limits"`
}
type Scenario struct {
	ID     string   `json:"id"`
	Title  string   `json:"title"`
	Checks []string `json:"checks"`
	Gaps   []string `json:"coverage_gaps,omitempty"`
}
type Catalog struct {
	SchemaVersion string       `json:"schema_version"`
	Checks        []Definition `json:"checks"`
	Scenarios     []Scenario   `json:"scenarios"`
}
type Request struct {
	Source   string    `json:"source,omitempty"`
	Check    string    `json:"check,omitempty"`
	Scenario string    `json:"scenario,omitempty"`
	Target   string    `json:"target,omitempty"`
	Bindings *Bindings `json:"bindings,omitempty"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	// IncludeMilvus is retained only to reject retired API requests explicitly.
	IncludeMilvus bool     `json:"include_milvus,omitempty"`
	Details       string   `json:"details"`
	TraceID       string   `json:"trace_id,omitempty"`
	RequestID     string   `json:"request_id,omitempty"`
	Threshold     *float64 `json:"threshold,omitempty"`
	BudgetMS      int64    `json:"budget_ms,omitempty"`
}
type Inspection struct {
	evidence.Inspection
	Spans    []SpanSample `json:"spans,omitempty"`
	Coverage []string     `json:"coverage_notes,omitempty"`
}
type SpanSample struct {
	TraceID    string    `json:"trace_id"`
	SpanID     string    `json:"span_id"`
	Service    string    `json:"service"`
	Operation  string    `json:"operation"`
	From       time.Time `json:"from"`
	DurationMS float64   `json:"duration_ms"`
	Error      bool      `json:"error"`
	Abnormal   bool      `json:"abnormal"`
	Path       string    `json:"trace_path"`
}
type Result struct {
	SelectionMode string `json:"selection_mode,omitempty"`
	evidence.Evidence
	Name             string          `json:"name"`
	Source           string          `json:"source"`
	Step             string          `json:"step"`
	Title            string          `json:"title"`
	RuleVersion      int             `json:"rule_version"`
	Execution        string          `json:"execution"`
	Rule             string          `json:"rule"`
	IdentityScope    string          `json:"identity_scope"`
	Queries          []d.SourceQuery `json:"queries"`
	Inspect          *Inspection     `json:"inspect,omitempty"`
	TraceIDs         []string        `json:"trace_ids,omitempty"`
	AbnormalTraceIDs []string        `json:"abnormal_trace_ids,omitempty"`
	Coverage         []string        `json:"coverage_notes,omitempty"`
	Assessment       AssessmentRule  `json:"assessment"`
}
type SourceSummary struct {
	Selected       bool   `json:"selected"`
	Configured     bool   `json:"configured"`
	Connection     string `json:"connection,omitempty"`
	Status         string `json:"status"`
	Checks         int    `json:"checks"`
	Abnormal       int    `json:"abnormal"`
	Inconclusive   int    `json:"inconclusive"`
	NoData         int    `json:"no_data"`
	Failed         int    `json:"failed"`
	Skipped        int    `json:"skipped"`
	MatchedTraces  int    `json:"matched_traces,omitempty"`
	AbnormalTraces int    `json:"traces_with_abnormal_spans,omitempty"`
}
type StepSummary struct {
	ID       string   `json:"id"`
	Checks   []string `json:"checks"`
	Abnormal []string `json:"abnormal"`
	Gaps     []string `json:"gaps"`
}
type Summary struct {
	Metrics SourceSummary `json:"metrics"`
	Trace   SourceSummary `json:"trace"`
	Steps   []StepSummary `json:"steps"`
}
type Report struct {
	SchemaVersion string          `json:"schema_version"`
	RunID         string          `json:"run_id"`
	Status        string          `json:"status"`
	Complete      bool            `json:"complete"`
	Request       Request         `json:"request"`
	Target        evidence.Target `json:"target"`
	Policy        evidence.Policy `json:"policy"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    time.Time       `json:"finished_at"`
	Summary       Summary         `json:"summary"`
	Checks        []Result        `json:"checks"`
	Observations  []d.Observation `json:"observations"`
	Problems      []d.Problem     `json:"problems"`
	Coverage      []string        `json:"coverage_gaps"`
}
type Event struct {
	ID     string    `json:"check_id"`
	Source string    `json:"source"`
	Phase  string    `json:"phase"`
	At     time.Time `json:"at"`
}
