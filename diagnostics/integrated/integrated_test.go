package integrated

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/jaeger"
	"github.com/zilliztech/woodpecker/diagnostics/loki"
)

type stubModule struct {
	source, stage, status string
	calls                 atomic.Int64
	requests              chan d.Request
	gate                  <-chan struct{}
}

func (m *stubModule) Descriptor() d.Descriptor {
	profiles := []d.Profile{}
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"read", "write"} {
			if m.source == "wpcli" && scope != "wp" {
				continue
			}
			stage := m.stage
			if stage == "" {
				stage = "append"
				if flow == "read" {
					stage = "wal_read"
				}
			}
			modes := []string{"snapshot", "live", "historical"}
			if m.source == "wpcli" {
				modes = []string{"snapshot", "live"}
			}
			profiles = append(profiles, d.Profile{ID: scope + "." + flow + "." + m.source + "." + stage, Version: 1, Scope: scope, Flow: flow, Stage: stage, Modes: modes, TargetFields: []string{"deployment", "instance", "node_id"}})
		}
	}
	return d.Descriptor{Source: m.source, Profiles: profiles}
}
func (m *stubModule) ValidateConnection(d.Connection) error { return nil }
func (m *stubModule) Run(ctx context.Context, r d.Request, _ d.Connection) d.Result {
	m.calls.Add(1)
	if m.requests != nil {
		m.requests <- r
	}
	if m.gate != nil {
		select {
		case <-m.gate:
		case <-ctx.Done():
		}
	}
	result := d.NewResult(m.source)
	result.Status = m.status
	if result.Status == "" {
		result.Status = "completed"
	}
	id := m.source + ":" + r.Selection.Stage
	result.Observations = []d.Observation{{ID: id + ":o", SourceID: id, Kind: "fixture", Status: "observed", StartedAt: time.Unix(1000, 0), FinishedAt: time.Unix(1060, 0), Data: json.RawMessage(`{"retained":true}`)}}
	result.Findings = []d.Finding{{ID: id + ":f", RuleID: "fixture.rule", RuleVersion: 1, Flow: r.Selection.Flow, Stage: r.Selection.Stage, Target: d.Target{NodeID: "node-1"}, Outcome: "fixture_suspicion", Severity: "warning", Strength: "observed_fixture", Supporting: []string{id + ":o"}, Contradicting: []string{}, Unknown: []string{"not_a_health_verdict"}, NextChecks: []string{"Inspect the exact source evidence"}}}
	return result
}

func testConfig(t *testing.T, disabled ...string) d.Config {
	t.Helper()
	text := `schema_version: wpdiag.config/v1
sources:
  wpcli:
    connections:
      local:
        settings: {executable: wp, context: fixture}
  prometheus:
    connections:
      local:
        settings: {endpoint: 'http://127.0.0.1:1', deployment: fixture, match_labels: {cluster: fixture}}
  trace:
    connections:
      local:
        settings: {endpoint: 'http://127.0.0.1:1', deployment: fixture, services: {proxy: proxy, sn: streamingnode, wp: wp_server}, search: {service: proxy}}
  loki:
    connections:
      local:
        settings: {endpoint: 'http://127.0.0.1:1', deployment: fixture, match_labels: {namespace: fixture}}
`
	c, err := d.ParseConfig([]byte(text), t.TempDir())
	require.NoError(t, err)
	for _, source := range disabled {
		v := false
		x := c.Sources[source].Connections["local"]
		x.Enabled = &v
		c.Sources[source].Connections["local"] = x
	}
	return c
}

func request(scope, flow, mode string) Request {
	w := d.Window{Mode: mode}
	if mode == "historical" {
		a, b := time.Unix(1000, 0).UTC(), time.Unix(1060, 0).UTC()
		w.From = &a
		w.To = &b
	}
	if mode == "live" {
		w.DurationMS = 30000
	}
	return Request{Selection: d.Selection{Scope: scope, Flow: flow, Window: w}, Limits: d.DefaultLimits()}
}

func TestStandardPlans(t *testing.T) {
	r := Standard()
	c := testConfig(t)
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			q := request(scope, flow, "live")
			p := r.Plan(c, q)
			require.Equal(t, "completed", p.Status, "%s %s: %+v", scope, flow, p)
			want := 4
			if scope == "e2e" {
				want = 5
			}
			require.Len(t, p.Entries, want)
			var input, output int64
			for _, e := range p.Entries {
				require.Equal(t, "ready", e.Status)
				input += e.Request.Limits.MaxInputBytes
				output += e.Request.Limits.MaxOutputBytes
				require.Equal(t, 1, e.Profile.Version)
				if e.Source == "wpcli" {
					require.Equal(t, "wp", e.Request.Selection.Scope)
					require.EqualValues(t, 1000, e.Request.Selection.Window.IntervalMS)
				} else {
					require.EqualValues(t, 30000, e.Request.Selection.Window.IntervalMS)
				}
			}
			require.LessOrEqual(t, input, q.Limits.MaxInputBytes)
			require.LessOrEqual(t, output, q.Limits.MaxOutputBytes)
		}
	}
	q := request("e2e", "write", "historical")
	p := r.Plan(c, q)
	require.Len(t, p.Entries, 4)
	q.CurrentNative = true
	p = r.Plan(c, q)
	require.Equal(t, "completed", p.Status)
	require.Len(t, p.Entries, 5)
	for _, e := range p.Entries {
		if e.Source == "wpcli" {
			require.Equal(t, "current_context", e.Role)
			require.Equal(t, "snapshot", e.Request.Selection.Window.Mode)
		}
	}
	p = r.Plan(testConfig(t, "trace", "loki"), request("wp", "read", "snapshot"))
	require.Len(t, p.Entries, 1)
	require.Equal(t, "wpcli", p.Entries[0].Source)
}

func TestPlanRejectsWithoutIO(t *testing.T) {
	r := Standard()
	c := testConfig(t)
	for _, edit := range []func(*Request){
		func(q *Request) { q.Selection.Flow = "invalid" }, func(q *Request) { q.Stages = []string{"query"} },
		func(q *Request) { q.Modules = []string{"milvus.append"}; q.Stages = []string{"append"} }, func(q *Request) { q.Modules = []string{"unknown"} },
		func(q *Request) { q.Selection.Profile = "x" }, func(q *Request) { q.Limits.MaxInputBytes = 1024 },
		func(q *Request) { q.CurrentNative = true }, func(q *Request) { q.Targets = map[string]d.Target{"unknown": {NodeID: "1"}} },
		func(q *Request) { q.Selection.Sources = []string{"loki"}; q.Stages = []string{"append"} },
		func(q *Request) { q.Modules = []string{"wpcli"}; q.Selection.Sources = []string{"prometheus"} },
	} {
		q := request("e2e", "write", "historical")
		edit(&q)
		if q.CurrentNative {
			q.Selection.Window = d.Window{Mode: "snapshot"}
		}
		p := r.Plan(c, q)
		require.Equal(t, "invalid", p.Status)
		require.NotEmpty(t, p.Problems)
	}
	q := request("e2e", "write", "historical")
	q.Selection.Sources = []string{"prometheus", "wpcli"}
	p := r.Plan(c, q)
	require.Equal(t, "partial", p.Status)
	q.Targets = map[string]d.Target{"prometheus": {Instance: "sn:9091"}, "wpcli": {NodeID: "node-1"}}
	q.CurrentNative = true
	p = r.Plan(c, q)
	require.Equal(t, "completed", p.Status)
	for _, e := range p.Entries {
		if e.Source == "prometheus" {
			require.Equal(t, "sn:9091", e.Request.Selection.Target.Instance)
		} else {
			require.Equal(t, "node-1", e.Request.Selection.Target.NodeID)
		}
	}
	q = request("wp", "write", "snapshot")
	q.Selection.Sources = []string{"wpcli"}
	q.Targets = map[string]d.Target{"wpcli": {TenantAssociation: "claimed"}}
	require.Equal(t, "invalid", r.Plan(c, q).Status)
}

func TestParallelExecutionLineageAndStatuses(t *testing.T) {
	gate := make(chan struct{})
	requests := make(chan d.Request, 8)
	prom := &stubModule{source: "prometheus", requests: requests, gate: gate}
	native := &stubModule{source: "wpcli", requests: requests, gate: gate}
	r, err := New(Definition{ID: "prometheus", Source: "prometheus", Module: prom}, Definition{ID: "wpcli", Source: "wpcli", Module: native})
	require.NoError(t, err)
	q := request("e2e", "write", "live")
	q.Modules = []string{"prometheus", "wpcli"}
	q.Limits.BudgetMS = 1000
	finished := make(chan Report, 1)
	events := make(chan Event) // an unread channel cannot block collection
	go func() { finished <- r.Run(context.Background(), testConfig(t), q, events) }()
	for i := 0; i < 2; i++ {
		select {
		case req := <-requests:
			require.Greater(t, req.Limits.MaxInputBytes, int64(0))
		case <-time.After(time.Second):
			t.Fatal("modules were not started concurrently")
		}
	}
	close(gate)
	report := <-finished
	require.Equal(t, "completed", report.Status)
	require.Len(t, report.Runs, 2)
	require.Len(t, report.Summary.Findings, 2)
	for _, view := range report.Summary.Findings {
		f, ok := Resolve(report, view.Reference)
		require.True(t, ok)
		require.Equal(t, "fixture_suspicion", f.Outcome)
		require.Equal(t, "observed_fixture", f.Strength)
	}
	require.Empty(t, report.Summary.Associations, "same-node/same-window facts are not an authoritative join")
	before, _ := json.Marshal(report.Runs)
	again := Summarize(context.Background(), report.Runs)
	after, _ := json.Marshal(report.Runs)
	require.Equal(t, before, after)
	require.Equal(t, report.Summary, again)
	prom.gate = nil
	native.gate = nil
	native.status = "failed"
	report = r.Run(context.Background(), testConfig(t), q, nil)
	require.Equal(t, "partial", report.Status)
	require.Equal(t, 6, d.ExitCode(report.Status))
	require.Len(t, report.Summary.Findings, 2)
	native.status = "completed"
	c := testConfig(t, "wpcli")
	report = r.Run(context.Background(), c, q, nil)
	require.Equal(t, "partial", report.Status)
	require.Equal(t, "disabled", report.Runs[1].Report.Status)
}

func TestHistoricalCurrentContextAndStageReuse(t *testing.T) {
	m := &stubModule{source: "prometheus", stage: "append"}
	native := &stubModule{source: "wpcli"}
	assess := func(_ context.Context, report d.Report) d.Assessment {
		f := report.Results[0].Findings[0]
		f.DerivedFrom = []string{f.ID}
		f.ID += "-stage"
		return d.Assessment{Profile: "fixture.stage", Version: 1, Status: report.Status, Findings: []d.Finding{f}}
	}
	r, err := New(Definition{ID: "milvus.append", Source: "prometheus", Stage: "append", Module: m, Assess: assess}, Definition{ID: "wpcli", Source: "wpcli", Module: native})
	require.NoError(t, err)
	q := request("e2e", "write", "historical")
	q.Modules = []string{"milvus.append", "wpcli"}
	q.CurrentNative = true
	report := r.Run(context.Background(), testConfig(t), q, nil)
	require.Equal(t, "completed", report.Status)
	require.Len(t, report.Summary.Findings, 2)
	require.Equal(t, assess(context.Background(), report.Runs[0].Report), report.Runs[0].Report.Assessments[0])
	require.EqualValues(t, 1, m.calls.Load())
	require.Equal(t, "current_context", report.Summary.Findings[1].Context)
	require.Equal(t, "fixture_suspicion", report.Runs[0].Report.Assessments[0].Findings[0].Outcome)
	require.Contains(t, fmt.Sprint(report.Summary.Gaps), "current_runtime_not_historical_evidence")
}

func TestCancellationDeadlineAndOutputBound(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		gate := make(chan struct{})
		m := &stubModule{source: "prometheus", gate: gate}
		r, err := New(Definition{ID: "prometheus", Source: "prometheus", Module: m})
		require.NoError(t, err)
		q := request("wp", "write", "historical")
		q.Modules = []string{"prometheus"}
		q.Limits.BudgetMS = 15
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		report := r.Run(ctx, testConfig(t), q, nil)
		cancel()
		want := "budget_exceeded"
		if cancelled {
			want = "cancelled"
		}
		require.Equal(t, want, report.Status)
	}
	r := Report{SchemaVersion: Schema, Status: "completed", Plan: Plan{Request: Request{Limits: d.DefaultLimits()}}}
	r.Plan.Request.Limits.MaxOutputBytes = 4096
	for i := 0; i < 1000; i++ {
		r.Summary.Gaps = append(r.Summary.Gaps, d.Gap{Code: "large", Detail: "large source evidence must not become truncated JSON"})
	}
	data, exit, err := Encode(r)
	require.NoError(t, err)
	require.Equal(t, 5, exit)
	require.True(t, json.Valid(data))
	require.LessOrEqual(t, len(data), 4096)
	require.NotContains(t, string(data), "large source evidence")
}

func correlatedReports(t *testing.T) []Run {
	t.Helper()
	id := "11111111111111111111111111111111"
	at := time.Unix(1000, 0)
	tr := jaeger.Trace{ID: id, Spans: []jaeger.Span{{ID: "0000000000000001", TraceID: id, Selected: true, StartUS: at.UnixMicro(), DurationUS: 2000000}}}
	body, err := json.Marshal(tr)
	require.NoError(t, err)
	traceResult := d.NewResult("trace")
	traceResult.Status = "completed"
	traceResult.Metadata = json.RawMessage(`{"source_schema_version":"wpdiag.jaeger/v1","configured_deployment":"fixture"}`)
	traceResult.Observations = []d.Observation{{ID: "tr-o", Status: "observed", Kind: "jaeger_trace", Data: body, StartedAt: time.Now(), FinishedAt: time.Now()}}
	traceResult.Findings = []d.Finding{{ID: "tr-f", RuleID: "trace.test", RuleVersion: 1, Flow: "write", Stage: "append", Supporting: []string{"tr-o"}, Target: d.Target{TraceID: id}}}
	e := loki.Event{ID: "event-1", StreamID: "stream", Pattern: "wp.append_failure", PatternVersion: 1, TraceID: id, TimestampNS: fmt.Sprint(at.Add(time.Second).UnixNano())}
	body, err = json.Marshal(map[string]any{"events": []loki.Event{e}})
	require.NoError(t, err)
	logs := d.NewResult("loki")
	logs.Status = "completed"
	logs.Metadata = json.RawMessage(`{"source_schema_version":"wpdiag.loki/v1","bound_deployment":"fixture"}`)
	logs.Observations = []d.Observation{{ID: "log-o", Kind: "loki_log_page", Status: "observed", Data: body, StartedAt: time.Now(), FinishedAt: time.Now()}}
	logs.Findings = []d.Finding{{ID: "loki:finding:wp.append_failure:stream", RuleID: "loki.wp.append_failure", RuleVersion: 1, Flow: "write", Stage: "append", Supporting: []string{"log-o"}}}
	report := func(result d.Result) d.Report {
		return d.Report{SchemaVersion: d.ReportSchema, RunID: result.Source + "-run", Selection: d.Selection{Flow: "write", Window: d.Window{Mode: "historical"}}, Status: "completed", Results: []d.Result{result}}
	}
	return []Run{{ModuleID: "trace", Role: "source_context", Report: report(traceResult)}, {ModuleID: "loki", Role: "source_context", Report: report(logs)}}
}

func TestActualEventIdentityTimeAndDeduplication(t *testing.T) {
	runs := correlatedReports(t)
	s := Summarize(context.Background(), runs)
	require.Len(t, s.Associations, 1)
	require.Len(t, s.Associations[0].Observations, 2)
	require.Len(t, s.Associations[0].Findings, 1)
	for _, ref := range s.Associations[0].Observations {
		if ref.ModuleID == "loki" {
			require.Equal(t, "event-1", ref.EventID)
		} else {
			require.Equal(t, "0000000000000001", ref.SpanID)
		}
	}
	duplicate := append(append([]Run{}, runs...), runs[0])
	again := Summarize(context.Background(), duplicate)
	require.Equal(t, s.Associations, again.Associations)
	require.Len(t, again.Findings, 2)
	for _, modify := range []func([]Run){
		func(r []Run) {
			r[1].Report.Results[0].Metadata = json.RawMessage(`{"source_schema_version":"wpdiag.loki/v1","bound_deployment":"other"}`)
		},
		func(r []Run) {
			var p map[string][]loki.Event
			require.NoError(t, json.Unmarshal(r[1].Report.Results[0].Observations[0].Data, &p))
			p["events"][0].TraceID = "22222222222222222222222222222222"
			r[1].Report.Results[0].Observations[0].Data, _ = json.Marshal(p)
		},
		func(r []Run) {
			var p map[string][]loki.Event
			require.NoError(t, json.Unmarshal(r[1].Report.Results[0].Observations[0].Data, &p))
			p["events"][0].TimestampNS = fmt.Sprint(time.Unix(2000, 0).UnixNano())
			r[1].Report.Results[0].Observations[0].Data, _ = json.Marshal(p)
		},
		func(r []Run) { r[0].Role = "current_context" },
	} {
		r := correlatedReports(t)
		modify(r)
		require.Empty(t, Summarize(context.Background(), r).Associations)
	}
	conflict := correlatedReports(t)
	other := conflict[0]
	other.Role = "other"
	conflict = append(conflict, other)
	summary := Summarize(context.Background(), conflict)
	require.Empty(t, summary.Associations)
	require.Contains(t, fmt.Sprint(summary.Gaps), "conflicting_module_identity")
	broken := correlatedReports(t)
	broken[0].Report.Results[0].Findings[0].Supporting = []string{"missing"}
	require.Empty(t, Summarize(context.Background(), broken).Associations)
}

func TestSourceExtensionsShareRunAndProgressContract(t *testing.T) {
	defs := []Definition{}
	modules := map[string]*stubModule{}
	for _, source := range []string{"prometheus", "wpcli", "trace", "loki"} {
		m := &stubModule{source: source}
		modules[source] = m
		defs = append(defs, Definition{ID: source, Source: source, Module: m})
	}
	r, err := New(defs...)
	require.NoError(t, err)
	c := testConfig(t)
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			q := request(scope, flow, "live")
			q.Modules = []string{"prometheus", "trace", "loki"}
			if scope != "milvus" {
				q.Modules = append(q.Modules, "wpcli")
			}
			// No consumer: advisory progress must never block execution.
			result := r.Run(context.Background(), c, q, make(chan Event))
			require.Equal(t, "completed", result.Status)
			require.Len(t, result.Runs, len(q.Modules))
			require.Len(t, result.Summary.Findings, len(q.Modules))
			for _, view := range result.Summary.Findings {
				f, exists := Resolve(result, view.Reference)
				require.True(t, exists)
				require.Equal(t, "observed_fixture", f.Strength)
				require.Equal(t, flow, f.Flow)
			}
		}
	}
	require.EqualValues(t, 6, modules["trace"].calls.Load())
	require.EqualValues(t, 6, modules["loki"].calls.Load())
	_, err = New(defs[0], defs[0])
	require.Error(t, err, "duplicate module identity must be rejected")
	for _, sources := range [][]string{{"prometheus"}, {"wpcli"}} {
		q := request("e2e", "write", "historical")
		q.CurrentNative = true
		q.Selection.Sources = sources
		require.Equal(t, "invalid", r.Plan(c, q).Status)
	}
	q := request("e2e", "write", "live")
	q.Connection = "injected\nconnection"
	require.Equal(t, "invalid", r.Plan(c, q).Status)
}

func TestCatalogCannotMutateRunnerProfiles(t *testing.T) {
	r := Standard()
	before := r.Catalog()
	catalog := r.Catalog()
	catalog[0].Profiles[0].Modes[0] = "invalid"
	catalog[0].Profiles[0].TargetFields[0] = "invalid"
	catalog[0].Profiles[0].ID = "invalid"
	require.Equal(t, before[0].Profiles, r.Catalog()[0].Profiles)
}
