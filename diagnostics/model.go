// Package diagnostics defines the independent diagnostic tool's public module,
// selection and result contracts. It contains no WP or Milvus rule engine.
package diagnostics

import (
	"context"
	"encoding/json"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ReportSchema  = "wpdiag.report/v1"
	ConfigSchema  = "wpdiag.config/v1"
	CatalogSchema = "wpdiag.capabilities/v1"
)

type Target struct {
	Deployment        string `json:"deployment,omitempty"`
	Instance          string `json:"instance,omitempty"`
	NodeID            string `json:"node_id,omitempty"`
	LogID             string `json:"log_id,omitempty"`
	SegmentID         string `json:"segment_id,omitempty"`
	ReaderID          string `json:"reader_id,omitempty"`
	Collection        string `json:"collection,omitempty"`
	Channel           string `json:"channel,omitempty"`
	OperationID       string `json:"operation_id,omitempty"`
	TraceID           string `json:"trace_id,omitempty"`
	TenantAssociation string `json:"tenant_association,omitempty"`
	ServiceCluster    string `json:"observed_service_cluster,omitempty"`
	Identity          string `json:"identity,omitempty"`
}

type Window struct {
	Mode       string     `json:"mode"`
	DurationMS int64      `json:"duration_ms,omitempty"`
	IntervalMS int64      `json:"interval_ms,omitempty"`
	From       *time.Time `json:"from,omitempty"`
	To         *time.Time `json:"to,omitempty"`
}

type Selection struct {
	Sources []string `json:"sources"`
	Scope   string   `json:"scope"`
	Flow    string   `json:"flow"`
	Stage   string   `json:"stage,omitempty"`
	Profile string   `json:"profile,omitempty"`
	Target  Target   `json:"target"`
	Window  Window   `json:"window"`
}

type Limits struct {
	BudgetMS       int64 `json:"budget_ms" yaml:"budget_ms"`
	MaxInputBytes  int64 `json:"max_input_bytes" yaml:"max_input_bytes"`
	MaxOutputBytes int64 `json:"max_output_bytes" yaml:"max_output_bytes"`
	MaxTargets     int   `json:"max_targets" yaml:"max_targets"`
}

func DefaultLimits() Limits {
	return Limits{BudgetMS: 60000, MaxInputBytes: 8 << 20, MaxOutputBytes: 16 << 20, MaxTargets: 8}
}

type Request struct {
	Selection  Selection `json:"selection"`
	Connection string    `json:"connection"`
	Limits     Limits    `json:"limits"`
	Profile    Profile   `json:"-"`
}

type Profile struct {
	ID           string   `json:"id"`
	Version      int      `json:"version"`
	Scope        string   `json:"scope"`
	Flow         string   `json:"flow"`
	Stage        string   `json:"stage"`
	Modes        []string `json:"modes"`
	TargetFields []string `json:"target_fields"`
	Description  string   `json:"description"`
}

type Descriptor struct {
	Source           string    `json:"source"`
	Status           string    `json:"status"`
	Issue            int       `json:"issue"`
	Profiles         []Profile `json:"profiles"`
	RequiredSettings []string  `json:"required_connection_settings,omitempty"`
}

type Catalog struct {
	SchemaVersion string            `json:"schema_version"`
	Sources       []Descriptor      `json:"sources"`
	Stages        []StageDescriptor `json:"stages,omitempty"`
}

// StageDescriptor describes an independently invoked business-stage assessor.
type StageDescriptor struct {
	Command  string    `json:"command"`
	Issue    int       `json:"issue"`
	Sources  []string  `json:"sources"`
	Profiles []Profile `json:"profiles"`
}

// Connection contains private settings, never serialized into a report. The
// module owns their schema; the core only identifies/enables named connections.
type Connection struct {
	Enabled  *bool     `yaml:"enabled" json:"-"`
	Settings yaml.Node `yaml:"settings" json:"-"`
	BaseDir  string    `yaml:"-" json:"-"`
}

func (c Connection) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

type Problem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Observation struct {
	ID         string          `json:"id"`
	SourceID   string          `json:"source_id"`
	Kind       string          `json:"kind"`
	Target     Target          `json:"target"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
	Status     string          `json:"status"`
	Code       string          `json:"code,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
	Query      *SourceQuery    `json:"query,omitempty"`
}

// SourceQuery is optional reproducibility metadata supplied by the collector.
// Executed distinguishes an attempted query from a suggested next check. It
// must exclude credentials, authenticated URLs and sensitive query parameters.
type SourceQuery struct {
	Language   string `json:"language"`
	Expression string `json:"expression"`
	Executed   bool   `json:"executed"`
}

type Finding struct {
	ID            string   `json:"id"`
	RuleID        string   `json:"rule_id"`
	RuleVersion   int      `json:"rule_version"`
	Flow          string   `json:"flow"`
	Stage         string   `json:"stage"`
	Target        Target   `json:"target"`
	Outcome       string   `json:"outcome"`
	Severity      string   `json:"severity"`
	Strength      string   `json:"evidence_strength"`
	Summary       string   `json:"summary"`
	Supporting    []string `json:"supporting_observations"`
	Contradicting []string `json:"contradicting_observations"`
	Unknown       []string `json:"unknown"`
	NextChecks    []string `json:"next_read_only_checks"`
	DerivedFrom   []string `json:"derived_from_findings,omitempty"`
}

type Gap struct {
	Code          string `json:"code"`
	NodeID        string `json:"node_id,omitempty"`
	ObservationID string `json:"observation_id,omitempty"`
	Detail        string `json:"detail"`
}

type Result struct {
	Source       string          `json:"source"`
	Connection   string          `json:"connection"`
	Profile      string          `json:"profile,omitempty"`
	Status       string          `json:"status"`
	Problems     []Problem       `json:"problems"`
	Observations []Observation   `json:"observations"`
	Findings     []Finding       `json:"findings"`
	Gaps         []Gap           `json:"coverage_gaps"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

type Report struct {
	SchemaVersion string       `json:"schema_version"`
	RunID         string       `json:"run_id"`
	Selection     Selection    `json:"selection"`
	Limits        Limits       `json:"limits"`
	StartedAt     time.Time    `json:"started_at"`
	FinishedAt    time.Time    `json:"finished_at"`
	Status        string       `json:"status"`
	Results       []Result     `json:"results"`
	Assessments   []Assessment `json:"assessments,omitempty"`
}

// Assessment adds stage semantics to source evidence without copying or
// replacing source results. References resolve against Report.Results.
type Assessment struct {
	Profile  string    `json:"profile"`
	Version  int       `json:"version"`
	Status   string    `json:"status"`
	Findings []Finding `json:"findings"`
	Gaps     []Gap     `json:"coverage_gaps"`
}

// Module is a compile-time interface, not a dynamic plugin/DSL. Public results
// are shared by individual commands and future orchestration/presentation.
type Module interface {
	Descriptor() Descriptor
	ValidateConnection(Connection) error
	Run(context.Context, Request, Connection) Result
}

func NewResult(source string) Result {
	return Result{Source: source, Status: "failed", Problems: []Problem{}, Observations: []Observation{}, Findings: []Finding{}, Gaps: []Gap{}}
}

func Failure(source, status, code, message string) Result {
	r := NewResult(source)
	r.Status = status
	r.Problems = append(r.Problems, Problem{Code: code, Message: message})
	return r
}
