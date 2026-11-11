package wpcli

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func protocolError() error {
	return fmt.Errorf("Native report violates the supported wp.diagnosis/v1 contract; output withheld")
}

func decimal(n *int64) string {
	if n == nil {
		return ""
	}
	return strconv.FormatInt(*n, 10)
}

func importReport(data []byte, exit int, request d.Request, settings Settings) (d.Result, error) {
	var native Report
	if json.Unmarshal(data, &native) != nil || native.SchemaVersion != "wp.diagnosis/v1" {
		return d.Result{}, protocolError()
	}
	if err := validate(native, exit, request, settings); err != nil {
		return d.Result{}, err
	}
	result := d.NewResult("wpcli")
	result.Status = native.Status
	targets := map[string]d.Target{}
	for _, t := range native.Targets {
		targets[t.NodeID] = d.Target{NodeID: t.NodeID, ServiceCluster: t.Cluster, Identity: t.Identity}
	}
	seen := map[string]bool{}
	for _, o := range native.Observations {
		if seen[o.ID] {
			result.Gaps = append(result.Gaps, d.Gap{Code: "duplicate_observation_deduplicated", ObservationID: o.ID, NodeID: o.NodeID, Detail: "Repeated native observation ID counted once"})
			continue
		}
		seen[o.ID] = true
		// Only known native fields are retained. In particular, arbitrary labels
		// and new unknown top-level/env fields cannot become report credentials.
		for i, counter := range o.Counters {
			labels := map[string]string{}
			for _, key := range []string{"node_id", "log_id", "log_ns", "operation", "status"} {
				if value, ok := counter.Labels[key]; ok {
					labels[key] = value
				}
			}
			o.Counters[i].Labels = labels
		}
		type operation struct {
			ID        string    `json:"op_id"`
			Type      string    `json:"op_type"`
			LogID     string    `json:"log_id,omitempty"`
			SegmentID string    `json:"segment_id,omitempty"`
			StartedAt time.Time `json:"started_at"`
			TraceID   string    `json:"trace_id,omitempty"`
			SpanID    string    `json:"span_id,omitempty"`
		}
		ops := []operation{}
		for _, op := range o.Ops {
			ops = append(ops, operation{ID: op.ID, Type: op.Type, LogID: decimal(op.LogID), SegmentID: decimal(op.SegmentID), StartedAt: op.StartedAt, TraceID: op.TraceID, SpanID: op.SpanID})
		}
		body, _ := json.Marshal(struct {
			Round        int         `json:"round"`
			Counters     []Counter   `json:"counters,omitempty"`
			ProcessStart *float64    `json:"process_start_time_seconds,omitempty"`
			Ops          []operation `json:"ops,omitempty"`
			Registry     *Registry   `json:"registry,omitempty"`
			Version      string      `json:"build_version,omitempty"`
		}{o.Round, o.Counters, o.ProcessStart, ops, o.Registry, o.Version})
		result.Observations = append(result.Observations, d.Observation{ID: o.ID, SourceID: o.SourceID, Kind: o.Kind, Target: targets[o.NodeID], StartedAt: o.StartedAt, FinishedAt: o.FinishedAt, Status: o.Status, Code: o.Code, Data: body})
	}
	for _, f := range native.Findings {
		target := targets[f.Scope.NodeID]
		target.LogID = decimal(f.Scope.LogID)
		target.SegmentID = decimal(f.Scope.SegmentID)
		target.OperationID = f.Scope.Operation
		target.TraceID = f.Scope.TraceID
		target.TenantAssociation = f.Scope.Tenant
		result.Findings = append(result.Findings, d.Finding{ID: f.ID, RuleID: f.RuleID, RuleVersion: f.RuleVersion, Flow: f.Flow, Stage: f.Stage, Target: target, Outcome: f.Outcome, Severity: f.Severity, Strength: f.Strength, Summary: f.Summary, Supporting: unique(f.Supporting), Contradicting: unique(f.Contradicting), Unknown: unique(f.Unknown), NextChecks: unique(f.NextChecks)})
	}
	for _, g := range native.Gaps {
		result.Gaps = append(result.Gaps, d.Gap{Code: g.Code, NodeID: g.NodeID, ObservationID: g.ObservationID, Detail: g.Detail})
	}
	result.Metadata, _ = json.Marshal(struct {
		Schema     string    `json:"native_schema_version"`
		RunID      string    `json:"native_run_id"`
		Source     string    `json:"native_source"`
		Context    string    `json:"service_context"`
		Exit       int       `json:"native_exit_code"`
		StartedAt  time.Time `json:"started_at"`
		FinishedAt time.Time `json:"finished_at"`
		Mode       string    `json:"observation_mode"`
		WindowMS   int64     `json:"requested_window_ms"`
		IntervalMS int64     `json:"requested_interval_ms"`
		Limits     Limits    `json:"native_limits"`
		Targets    []Target  `json:"native_targets"`
		Rules      []Rule    `json:"native_rules"`
	}{native.SchemaVersion, native.RunID, native.Source, native.Context, exit, native.StartedAt, native.FinishedAt, native.Mode, native.WindowMS, native.IntervalMS, native.Limits, native.Targets, native.Rules})
	return result, nil
}

func unique(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func validTime(start, end time.Time) bool {
	return !start.IsZero() && !end.IsZero() && !end.Before(start)
}

func validate(n Report, exit int, r d.Request, s Settings) error {
	w := r.Selection.Window
	expectedMode := "snapshot"
	if w.Mode == "live" {
		expectedMode = "live_window"
	}
	if n.RunID == "" || n.Source != "wp-runtime" || n.Scope != "wp" || n.Context != s.Context || n.Mode != expectedMode || n.WindowMS != w.DurationMS || n.IntervalMS != w.IntervalMS || !validTime(n.StartedAt, n.FinishedAt) {
		return protocolError()
	}
	if d.ExitCode(n.Status) != exit || !d.Has([]string{"completed", "failed", "partial", "budget_exceeded", "cancelled"}, n.Status) {
		return protocolError()
	}
	maxNodes := r.Limits.MaxTargets
	if r.Selection.Target.NodeID != "" {
		maxNodes = 1
	}
	maxSamples := 2
	if w.Mode == "live" {
		maxSamples = int((w.DurationMS+w.IntervalMS-1)/w.IntervalMS) + 1
	}
	if n.Limits.MaxSamples > maxSamples {
		return protocolError()
	}
	if len(n.Targets) > maxNodes || n.Limits.MaxNodes > maxNodes || n.Limits.MaxNodes < 1 || n.Limits.MaxSamples < 2 || n.Limits.MaxSamples > s.NativeLimits.MaxSamples || n.Limits.MaxOps < 1 || n.Limits.MaxOps > s.NativeLimits.MaxOps || n.Limits.MaxSeries < 1 || n.Limits.MaxSeries > s.NativeLimits.MaxSeries || n.Limits.ResponseBytes < 1024 || n.Limits.ResponseBytes > s.NativeLimits.ResponseBytes || n.Limits.BudgetMS < 1 || n.Limits.BudgetMS > r.Limits.BudgetMS {
		return protocolError()
	}
	if n.Status == "completed" && len(n.Targets) == 0 {
		return protocolError()
	}
	targets := map[string]Target{}
	for _, t := range n.Targets {
		if t.NodeID == "" || targets[t.NodeID].NodeID != "" || !d.Has([]string{"discovered", "explicit_unverified"}, t.Identity) {
			return protocolError()
		}
		targets[t.NodeID] = t
	}
	if r.Selection.Target.NodeID != "" && len(n.Targets) > 0 && targets[r.Selection.Target.NodeID].NodeID == "" {
		return protocolError()
	}
	ruleID := "write_stall"
	scenario := "stuck-write"
	if r.Selection.Flow == "read" {
		ruleID = "read_stall"
		scenario = "stuck-read"
	}
	if len(n.Rules) != 1 {
		return protocolError()
	}
	rule := n.Rules[0]
	if rule.ID != ruleID || rule.Version != 1 || rule.Flow != r.Selection.Flow || rule.Stage != r.Profile.Stage || rule.Scenario != scenario || rule.MinimumSamples != 2 || len(rule.Required) == 0 || rule.Comparison == "" || rule.Applicability == "" {
		return protocolError()
	}
	seen := map[string]Observation{}
	lineage := map[string]string{}
	for _, o := range n.Observations {
		if o.ID == "" || o.SourceID == "" || targets[o.NodeID].NodeID == "" || o.Round < 0 || o.Round >= n.Limits.MaxSamples || !validTime(o.StartedAt, o.FinishedAt) || o.StartedAt.Before(n.StartedAt) || o.FinishedAt.After(n.FinishedAt) || !d.Has([]string{"metrics", "ops", "registry", "build"}, o.Kind) || !d.Has([]string{"observed", "unavailable", "unsupported", "invalid", "truncated"}, o.Status) || len(o.Ops) > n.Limits.MaxOps || len(o.Counters) > n.Limits.MaxSeries {
			return protocolError()
		}
		if w.Mode == "snapshot" && o.Round != 0 {
			return protocolError()
		}
		if previous, ok := seen[o.ID]; ok && !reflect.DeepEqual(previous, o) {
			return protocolError()
		}
		seen[o.ID] = o
		key := fmt.Sprintf("%s:%d:%s", o.NodeID, o.Round, o.Kind)
		if previous := lineage[key]; previous != "" && previous != o.ID {
			return protocolError()
		}
		lineage[key] = o.ID
		for _, op := range o.Ops {
			if (op.LogID != nil && *op.LogID < 0) || (op.SegmentID != nil && *op.SegmentID < 0) {
				return protocolError()
			}
		}
	}
	findings := map[string]bool{}
	coveredNodes := map[string]bool{}
	for _, f := range n.Findings {
		if f.ID == "" || findings[f.ID] || seen[f.ID].ID != "" || f.RuleID != ruleID || f.RuleVersion != 1 || f.Flow != rule.Flow || f.Stage != rule.Stage || targets[f.Scope.NodeID].NodeID == "" || (f.Scope.LogID != nil && *f.Scope.LogID < 0) || (f.Scope.SegmentID != nil && *f.Scope.SegmentID < 0) {
			return protocolError()
		}
		findings[f.ID] = true
		coveredNodes[f.Scope.NodeID] = true
		if !d.Has([]string{"suspected_stall", "conflicting", "no_pending_work_observed", "no_persistent_operation_observed", "inconclusive"}, f.Outcome) || !d.Has([]string{"warning", "none", "unknown"}, f.Severity) || !d.Has([]string{"inconclusive", "suspected", "conflicting"}, f.Strength) {
			return protocolError()
		}
		for _, id := range append(append([]string{}, f.Supporting...), f.Contradicting...) {
			if seen[id].ID == "" || seen[id].NodeID != f.Scope.NodeID {
				return protocolError()
			}
		}
		// An observation cannot be counted as independent support and contradiction.
		for _, id := range f.Supporting {
			if d.Has(f.Contradicting, id) {
				return protocolError()
			}
		}
	}
	if n.Status == "completed" {
		for node := range targets {
			if !coveredNodes[node] {
				return protocolError()
			}
		}
	}
	for _, g := range n.Gaps {
		if (g.NodeID != "" && targets[g.NodeID].NodeID == "") || (g.ObservationID != "" && seen[g.ObservationID].ID == "") {
			return protocolError()
		}
	}
	return nil
}
