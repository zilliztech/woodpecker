// Package diagnosis implements bounded WP-native live diagnosis. Its JSON
// contract is consumed externally; it does not connect to monitoring backends.
package diagnosis

import "time"

const SchemaVersion = "wp.diagnosis/v1"

type Rule struct {
	ID             string   `json:"id"`
	Version        int      `json:"version"`
	Scenario       string   `json:"scenario"`
	Flow           string   `json:"flow"`
	Stage          string   `json:"stage"`
	Description    string   `json:"description"`
	Required       []string `json:"required_evidence"`
	Optional       []string `json:"optional_evidence"`
	Applicability  string   `json:"applicability"`
	Comparison     string   `json:"comparison"`
	MinimumSamples int      `json:"minimum_samples"`
	Exclusions     []string `json:"exclusions"`
}

func Rules() []Rule {
	var rules []Rule
	for _, item := range []struct{ id, scenario, flow, stage string }{
		{"write_stall", "stuck-write", "write", "append"},
		{"read_stall", "stuck-read", "read", "wal_read"},
	} {
		rules = append(rules, Rule{
			ID: item.id, Version: 1, Scenario: item.scenario, Flow: item.flow, Stage: item.stage,
			Description:    "Persistent matching operation with no observed matching-log completion progress; suspicion only, not a root cause",
			Required:       []string{"ops identity in successive samples", "labelled completion counter coverage"},
			Optional:       []string{"registry statistics", "process start time", "build version"},
			Applicability:  "WP service mode; v0.1.x/dev API shape; other or unknown builds are explicitly qualified",
			Comparison:     "same operation across all samples; compare each counter series between consecutive samples (counts); never equate absence to zero",
			MinimumSamples: 2,
			Exclusions:     []string{"snapshot/insufficient window", "idle or new/completed operation", "counter/process reset", "EOF/read progress", "missing/changed series", "unsupported API/build"},
		})
	}
	return rules
}

type Target struct {
	NodeID   string `json:"node_id"`
	Cluster  string `json:"service_cluster,omitempty"`
	Identity string `json:"identity"`
}

type Scope struct {
	NodeID    string `json:"node_id"`
	LogID     *int64 `json:"log_id,omitempty"`
	SegmentID *int64 `json:"segment_id,omitempty"`
	Operation string `json:"operation_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
	Tenant    string `json:"tenant_association"`
}

type Operation struct {
	ID        string    `json:"op_id"`
	Type      string    `json:"op_type"`
	LogID     *int64    `json:"log_id,omitempty"`
	SegmentID *int64    `json:"segment_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
	TraceID   string    `json:"trace_id,omitempty"`
	SpanID    string    `json:"span_id,omitempty"`
}

type Counter struct {
	Labels      map[string]string `json:"labels"`
	Value       float64           `json:"value"`
	TimestampMS *int64            `json:"exported_timestamp_ms,omitempty"`
}

type Registry struct {
	Capacity int   `json:"capacity"`
	InUse    int   `json:"in_use"`
	Evicted  int64 `json:"evicted_total"`
}

type Observation struct {
	ID           string      `json:"id"`
	SourceID     string      `json:"source_id"`
	NodeID       string      `json:"node_id"`
	Kind         string      `json:"kind"`
	Round        int         `json:"round"`
	StartedAt    time.Time   `json:"started_at"`
	FinishedAt   time.Time   `json:"finished_at"`
	Status       string      `json:"status"`
	Code         string      `json:"code,omitempty"`
	Counters     []Counter   `json:"counters,omitempty"`
	ProcessStart *float64    `json:"process_start_time_seconds,omitempty"`
	Ops          []Operation `json:"ops,omitempty"`
	Registry     *Registry   `json:"registry,omitempty"`
	Version      string      `json:"build_version,omitempty"`
}

type Gap struct {
	Code          string `json:"code"`
	NodeID        string `json:"node_id,omitempty"`
	ObservationID string `json:"observation_id,omitempty"`
	Detail        string `json:"detail"`
}

type Finding struct {
	ID            string   `json:"id"`
	RuleID        string   `json:"rule_id"`
	RuleVersion   int      `json:"rule_version"`
	Flow          string   `json:"flow"`
	Stage         string   `json:"stage"`
	Scope         Scope    `json:"target"`
	Outcome       string   `json:"outcome"`
	Severity      string   `json:"severity"`
	Strength      string   `json:"evidence_strength"`
	Summary       string   `json:"summary"`
	Supporting    []string `json:"supporting_observations"`
	Contradicting []string `json:"contradicting_observations"`
	Unknown       []string `json:"unknown"`
	NextChecks    []string `json:"next_read_only_checks"`
}

type Limits struct {
	MaxNodes      int   `json:"max_nodes"`
	MaxSamples    int   `json:"max_samples_per_node"`
	MaxOps        int   `json:"max_ops_per_sample"`
	MaxSeries     int   `json:"max_counter_series_per_sample"`
	ResponseBytes int64 `json:"max_response_bytes"`
	BudgetMS      int64 `json:"wall_clock_budget_ms"`
}

type Report struct {
	SchemaVersion string        `json:"schema_version"`
	RunID         string        `json:"run_id"`
	Source        string        `json:"source"`
	Scope         string        `json:"scope"`
	Context       string        `json:"service_context,omitempty"`
	Mode          string        `json:"observation_mode"`
	WindowMS      int64         `json:"requested_window_ms"`
	IntervalMS    int64         `json:"requested_interval_ms"`
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	Status        string        `json:"collection_status"`
	Limits        Limits        `json:"limits"`
	Targets       []Target      `json:"targets"`
	Rules         []Rule        `json:"rules"`
	Observations  []Observation `json:"observations"`
	Findings      []Finding     `json:"findings"`
	Gaps          []Gap         `json:"coverage_gaps"`
}

// MatchesOperation is shared with the legacy metrics report's stall evaluator.
func MatchesOperation(ruleID, opType string) bool {
	if ruleID == "read_stall" {
		return opType == "logstore.get_batch_entries"
	}
	return ruleID == "write_stall" && (opType == "logstore.add_entry" || opType == "logstore.add_entry_batch")
}
