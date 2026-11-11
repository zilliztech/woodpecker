package checks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

type backend struct {
	mu                          sync.Mutex
	metrics                     []string
	trace                       int
	metricFailure, traceFailure bool
	empty                       bool
	traceLog                    string
}

func (b *backend) handler(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/traces") {
		b.mu.Lock()
		b.trace++
		failed := b.traceFailure
		log := b.traceLog
		b.mu.Unlock()
		if failed {
			http.Error(w, "private trace backend error", 503)
			return
		}
		if operation := r.URL.Query().Get("operation"); operation != "" && operation != "AddEntry" {
			fmt.Fprint(w, `{"data":[]}`)
			return
		}
		if value := r.URL.Query().Get("minDuration"); value != "" {
			duration, err := time.ParseDuration(value)
			if err == nil && duration > 2*time.Second {
				fmt.Fprint(w, `{"data":[]}`)
				return
			}
		}
		var filters map[string]string
		_ = json.Unmarshal([]byte(r.URL.Query().Get("tags")), &filters)
		if filters["otel.status_code"] != "" {
			fmt.Fprint(w, `{"data":[]}`)
			return
		}
		start, _ := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
		if start == 0 {
			start = time.Now().Add(-time.Minute).UnixMicro()
		}
		tags := []any{map[string]any{"key": "otel.scope.name", "type": "string", "value": "LogStore"}, map[string]any{"key": "service.instance.id", "type": "string", "value": "n"}, map[string]any{"key": "log_id", "type": "string", "value": log}}
		span := map[string]any{"traceID": "11111111111111111111111111111111", "spanID": "2222222222222222", "operationName": "AddEntry", "processID": "p", "startTime": start + 1000000, "duration": 2000000, "tags": []any{map[string]any{"key": "error", "type": "bool", "value": true}}}
		trace := map[string]any{"traceID": "11111111111111111111111111111111", "spans": []any{span}, "processes": map[string]any{"p": map[string]any{"serviceName": "wp", "tags": tags}}}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{trace}})
		return
	}
	query := r.URL.Query().Get("query")
	b.mu.Lock()
	b.metrics = append(b.metrics, query)
	failed, empty := b.metricFailure, b.empty
	b.mu.Unlock()
	if failed {
		http.Error(w, "private metric backend error", 503)
		return
	}
	if empty {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		return
	}
	interval := regexp.MustCompile(`\[(\d+)ms\] @ ([0-9.]+)$`).FindStringSubmatch(query)
	if len(interval) != 3 {
		http.Error(w, "invalid query", 400)
		return
	}
	length, _ := strconv.ParseFloat(interval[1], 64)
	end, _ := strconv.ParseFloat(interval[2], 64)
	start := end - length/1000
	labels := map[string]string{"cluster": "fixture", "instance": "wp:9091", "node_id": "n", "log_ns": "tenant", "log_id": "1"}
	for _, m := range regexp.MustCompile(`(\w+)="((?:[^"\\]|\\.)*)"`).FindAllStringSubmatch(query, -1) {
		if m[1] != "__name__" {
			var v string
			_ = json.Unmarshal([]byte(`"`+m[2]+`"`), &v)
			labels[m[1]] = v
		}
	}
	name := ""
	hist := false
	if m := regexp.MustCompile(`__name__=~"([^"]+)"`).FindStringSubmatch(query); len(m) > 0 {
		name = strings.TrimSuffix(strings.Split(m[1], "(")[0], "_")
		hist = true
	} else {
		m := regexp.MustCompile(`__name__="([^"]+)"`).FindStringSubmatch(query)
		if len(m) > 0 {
			name = m[1]
		}
	}
	if strings.Contains(query, `status=~"error|error_get_processor"`) {
		labels["status"] = "error_get_processor"
	}
	rows := []any{}
	add := func(metric string, extra map[string]string, values func(int) float64) {
		lab := map[string]string{}
		for k, v := range labels {
			lab[k] = v
		}
		for k, v := range extra {
			lab[k] = v
		}
		lab["__name__"] = metric
		points := [][2]any{}
		for i, at := 0, start+1; at < end; i, at = i+1, at+10 {
			points = append(points, [2]any{at, strconv.FormatFloat(values(i), 'f', -1, 64)})
		}
		rows = append(rows, map[string]any{"metric": lab, "values": points})
	}
	if hist {
		add(name+"_count", nil, func(i int) float64 { return float64(i * 200) })
		add(name+"_sum", nil, func(i int) float64 { return float64(i * 200 * 600) })
		for _, bound := range []string{"100", "500", "1000", "+Inf"} {
			bound := bound
			add(name+"_bucket", map[string]string{"le": bound}, func(i int) float64 {
				if bound == "100" || bound == "500" {
					return 0
				}
				return float64(i * 200)
			})
		}
	} else {
		add(name, nil, func(i int) float64 {
			switch {
			case strings.Contains(name, "disk_usage_ratio"):
				return .92
			case strings.Contains(name, "watermark_soft"):
				return .8
			case strings.Contains(name, "watermark_hard"):
				return .9
			case strings.Contains(name, "pending_append"):
				return float64(12 + i*2)
			case strings.HasSuffix(name, "_segment") || strings.HasSuffix(name, "_entry"):
				return 1
			case strings.Contains(name, "system_"):
				return .1
			default:
				return float64(i * 10)
			}
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": rows}})
}

func configuration(t *testing.T, endpoint string, metrics, trace bool) d.Config {
	t.Helper()
	sources := map[string]any{}
	if metrics {
		sources["metrics"] = map[string]any{"connections": map[string]any{"local": map[string]any{"settings": map[string]any{"endpoint": endpoint, "deployment": "fixture", "match_labels": map[string]string{"cluster": "fixture"}}}}}
	}
	if trace {
		sources["trace"] = map[string]any{"connections": map[string]any{"local": map[string]any{"settings": map[string]any{"endpoint": endpoint, "deployment": "fixture", "services": map[string]string{"wp": "wp_server"}, "search": map[string]string{"service": "wp"}}}}}
	}
	root := map[string]any{"schema_version": d.ConfigSchema, "sources": sources, "targets": map[string]any{"log": map[string]any{"label": "Fixture log", "metrics": "local", "trace": "local", "log_namespace": "tenant", "log_id": "1", "node_id": "n"}}}
	b, err := yaml.Marshal(root)
	require.NoError(t, err)
	c, err := d.ParseConfig(b, t.TempDir())
	require.NoError(t, err)
	return c
}

func request() Request {
	to := time.Now().UTC().Add(-time.Second).Truncate(time.Second)
	return Request{Scenario: "slow-writes", Target: "log", From: to.Add(-5 * time.Minute), To: to, Details: "all"}
}

func fixture(t *testing.T) (*backend, *httptest.Server) {
	t.Helper()
	b := &backend{traceLog: "1"}
	s := httptest.NewServer(http.HandlerFunc(b.handler))
	t.Cleanup(s.Close)
	return b, s
}

func TestScenarioCombinesUnchangedAtomsAndDeduplicatesTraces(t *testing.T) {
	b, server := fixture(t)
	c := configuration(t, server.URL, true, true)
	q := request()
	r := Run(context.Background(), c, q, nil)
	require.Equal(t, "completed", r.Status, r.Problems)
	require.True(t, r.Complete)
	require.True(t, r.Summary.Metrics.Configured)
	require.True(t, r.Summary.Trace.Configured)
	require.Equal(t, 1, r.Summary.Trace.MatchedTraces)
	require.Equal(t, 1, r.Summary.Trace.AbnormalTraces)
	require.Greater(t, b.trace, 1, "scenario uses source-side filtered operation/threshold queries")
	for _, row := range r.Checks {
		atomic := q
		atomic.Scenario = ""
		atomic.Source = row.Source
		atomic.Check = row.Name
		one := Run(context.Background(), c, atomic, nil)
		require.Len(t, one.Checks, 1)
		require.Equal(t, row.Status, one.Checks[0].Status, row.Name)
		require.Equal(t, row.Value, one.Checks[0].Value, row.Name)
		require.Equal(t, row.Rule, one.Checks[0].Rule, row.Name)
	}
	for _, step := range r.Summary.Steps {
		for _, id := range step.Abnormal {
			found := false
			for _, row := range r.Checks {
				if row.ID == id {
					require.Equal(t, "abnormal", row.Status)
					found = true
				}
			}
			require.True(t, found)
		}
	}
	require.NotContains(t, string(Encode(r)), "private backend")
}

func TestAtomicPendingDoesNotRequireSubmissionEvidence(t *testing.T) {
	b, server := fixture(t)
	c := configuration(t, server.URL, true, false)
	q := request()
	q.Scenario = ""
	q.Source = "metrics"
	q.Check = "append-pending"
	r := Run(context.Background(), c, q, nil)
	require.Len(t, r.Checks, 1)
	require.Equal(t, "abnormal", r.Checks[0].Status)
	require.Len(t, b.metrics, 1)
	require.NotContains(t, b.metrics[0], "append_requests_total")
}

func TestNoSourcesIsAnErrorButOneSourceIsEnough(t *testing.T) {
	_, server := fixture(t)
	q := request()
	r := Run(context.Background(), configuration(t, server.URL, false, false), q, nil)
	require.Equal(t, 12, ExitCode(r, false))
	require.Equal(t, "connection_not_configured", r.Problems[0].Code)
	for _, trace := range []bool{false, true} {
		r = Run(context.Background(), configuration(t, server.URL, !trace, trace), q, nil)
		require.Equal(t, "completed", r.Status, r.Problems)
		require.True(t, r.Complete)
		require.Equal(t, trace, r.Summary.Trace.Configured)
		require.Equal(t, !trace, r.Summary.Metrics.Configured)
	}
}

func TestFailedSourceIsNotMissingAndDoesNotHideOtherResults(t *testing.T) {
	b, server := fixture(t)
	b.traceFailure = true
	r := Run(context.Background(), configuration(t, server.URL, true, true), request(), nil)
	require.Equal(t, "partial", r.Status)
	require.True(t, r.Summary.Trace.Configured)
	require.Positive(t, r.Summary.Trace.Failed)
	require.Positive(t, r.Summary.Metrics.Abnormal)
	require.Equal(t, 6, ExitCode(r, false))
	require.NotContains(t, string(Encode(r)), "private trace backend error")
}

func TestResponseIdentityIsValidatedEvenWhenBackendIgnoresSelectors(t *testing.T) {
	_, server := fixture(t)
	c := configuration(t, server.URL, false, true)
	group := c.Sources["trace"]
	conn := group.Connections["local"]
	var settings map[string]any
	require.NoError(t, conn.Settings.Decode(&settings))
	settings["identity_tags"] = map[string]string{"log_id": "log_id"}
	require.NoError(t, conn.Settings.Encode(settings))
	group.Connections["local"] = conn
	c.Sources["trace"] = group
	settings2, err := LoadSettings(c)
	require.NoError(t, err)
	target := settings2.Targets["log"]
	target.LogID = "2"
	require.NoError(t, c.Targets.Encode(map[string]any{"log": target}))
	q := request()
	q.Scenario = ""
	q.Source = "trace"
	q.Check = "server-append-errors"
	r := Run(context.Background(), c, q, nil)
	require.Equal(t, "completed", r.Status)
	require.Equal(t, 0, r.Summary.Trace.MatchedTraces)
	require.Equal(t, "unknown", r.Checks[0].Status)
	require.Equal(t, "no_data", r.Checks[0].DataState)
	require.NotEmpty(t, r.Checks[0].ObservationIDs)
}

func TestDetailsArePresentationOnlyAndEncodingDoesNotMutate(t *testing.T) {
	_, server := fixture(t)
	r := Run(context.Background(), configuration(t, server.URL, true, true), request(), nil)
	original, _ := json.Marshal(r)
	for _, level := range []string{"none", "abnormal", "all"} {
		r.Request.Details = level
		data := Encode(r)
		var out Report
		require.NoError(t, json.Unmarshal(data, &out))
		for i, row := range out.Checks {
			require.Equal(t, r.Checks[i].Status, row.Status)
			require.Equal(t, level == "all" || level == "abnormal" && row.Status == "abnormal", row.Inspect != nil)
		}
	}
	r.Request.Details = "all"
	after, _ := json.Marshal(r)
	require.Equal(t, original, after)
}

func TestAllIsBoundedCatalogUnionWithNoLokiOrMetadata(t *testing.T) {
	_, server := fixture(t)
	q := request()
	q.Scenario = "all"
	r := Run(context.Background(), configuration(t, server.URL, true, true), q, nil)
	require.Equal(t, "completed", r.Status, r.Problems)
	require.Len(t, r.Checks, 55)
	seen := map[string]bool{}
	for _, row := range r.Checks {
		require.False(t, seen[row.ID])
		seen[row.ID] = true
		require.Contains(t, []string{"metrics", "trace"}, row.Source)
		require.False(t, strings.HasPrefix(row.ID, "MV"))
	}
}

func TestInvalidSelectionDoesNotQuerySources(t *testing.T) {
	b, server := fixture(t)
	c := configuration(t, server.URL, true, true)
	q := request()
	q.From = q.To.Add(time.Minute)
	r := Run(context.Background(), c, q, nil)
	require.Equal(t, "invalid", r.Status)
	require.Empty(t, b.metrics)
	require.Zero(t, b.trace)
}

func TestSlowMetricsCannotConsumeTraceCollectionOpportunity(t *testing.T) {
	b := &backend{traceLog: "1"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/traces") {
			b.handler(w, r)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	c := configuration(t, server.URL, true, true)
	q := request()
	q.BudgetMS = 1000
	report := Run(context.Background(), c, q, nil)
	require.Equal(t, "budget_exceeded", report.Status)
	require.Equal(t, 1, report.Summary.Trace.MatchedTraces)
	found := false
	for _, item := range report.Checks {
		if item.ID == "TS01" {
			require.Equal(t, "completed", item.Execution)
			require.Equal(t, "abnormal", item.Status)
			found = true
		}
	}
	require.True(t, found)
}

func TestTraceBudgetFailurePreservesMetricsAndExplainsNextSelection(t *testing.T) {
	b := &backend{traceLog: "1"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/traces") {
			fmt.Fprint(w, `{"data":[{},{}]}`)
			return
		}
		b.handler(w, r)
	}))
	defer server.Close()
	c := configuration(t, server.URL, true, true)
	group := c.Sources["trace"]
	conn := group.Connections["local"]
	var settings map[string]any
	require.NoError(t, conn.Settings.Decode(&settings))
	settings["limits"] = map[string]any{"max_traces": 1}
	require.NoError(t, conn.Settings.Encode(settings))
	group.Connections["local"] = conn
	c.Sources["trace"] = group
	report := Run(context.Background(), c, request(), nil)
	require.Equal(t, "partial", report.Status)
	require.Zero(t, report.Summary.Metrics.Failed)
	require.Positive(t, report.Summary.Metrics.Abnormal)
	require.Positive(t, report.Summary.Trace.Failed)
	require.Zero(t, report.Summary.Trace.MatchedTraces)
	require.Len(t, report.Problems, 1)
	require.Equal(t, "trace_budget_exceeded", report.Problems[0].Code)
	require.Contains(t, report.Problems[0].Message, "Narrow the incident interval")
}

func TestExplicitClientServerBindingsKeepSourcesAndScopesSeparate(t *testing.T) {
	client, cs := fixture(t)
	server, ss := fixture(t)
	cfg := configuration(t, cs.URL, true, true)
	group := cfg.Sources["prometheus"]
	other := configuration(t, ss.URL, true, false)
	group.Connections["server"] = other.Sources["prometheus"].Connections["local"]
	cfg.Sources["prometheus"] = group
	before, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	q := request()
	q.Target = ""
	q.Bindings = &Bindings{Client: &Scope{Name: "client-a", Metrics: "local", LogNamespace: "client-namespace", LogID: "7", Metadata: ClientMetadata{Etcd: "http://never-dial-etcd.invalid:2379", Prefix: "unrelated-meta-prefix", Bucket: "bucket", RootPath: "root"}}, Server: &Scope{Name: "cluster-b", Metrics: "server", NodeID: "server-node", Context: "never-dial-admin"}}
	report := Run(context.Background(), cfg, q, nil)
	require.Equal(t, "completed", report.Status)
	require.Contains(t, report.Target.Label, "client-a")
	require.Contains(t, report.Target.Label, "cluster-b")
	client.mu.Lock()
	cq := append([]string{}, client.metrics...)
	client.mu.Unlock()
	server.mu.Lock()
	sq := append([]string{}, server.metrics...)
	server.mu.Unlock()
	require.NotEmpty(t, cq)
	require.NotEmpty(t, sq)
	for _, query := range cq {
		require.NotContains(t, query, "server-node")
		require.NotContains(t, query, "unrelated-meta-prefix")
	}
	require.True(t, strings.Contains(strings.Join(cq, "\n"), `log_ns="client-namespace"`))
	for _, query := range sq {
		require.NotContains(t, query, "client-namespace")
		require.NotContains(t, query, `log_id="7"`)
	}
	require.Contains(t, strings.Join(sq, "\n"), `node_id="server-node"`)
	after, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestExplicitComponentSelectionManualSourcesAndValidation(t *testing.T) {
	b, srv := fixture(t)
	cfg := configuration(t, srv.URL, false, false)
	q := request()
	q.Target = ""
	q.Bindings = &Bindings{Server: &Scope{Name: "standalone server", Context: "my-context", MetricsEndpoint: srv.URL, MetricLabels: map[string]string{"cluster": "fixture"}, NodeID: "n"}}
	report := Run(context.Background(), cfg, q, nil)
	require.Equal(t, "completed", report.Status)
	require.NotEmpty(t, report.Checks)
	for _, observation := range report.Observations {
		require.Equal(t, "my-context", observation.Target.Deployment)
		require.NotContains(t, observation.Query.Expression, "my-context", "context names are not metric selectors")
	}
	for _, item := range report.Checks {
		for _, def := range Definitions(report.Policy) {
			if def.ID == item.ID {
				require.Equal(t, "server", CheckComponent(def))
			}
		}
	}
	b.mu.Lock()
	count := len(b.metrics)
	b.mu.Unlock()
	require.Positive(t, count)
	q.Bindings.Server.MetricLabels = map[string]string{"status": "error"}
	report = Run(context.Background(), cfg, q, nil)
	require.Equal(t, "invalid", report.Status)
	b.mu.Lock()
	require.Len(t, b.metrics, count)
	b.mu.Unlock()
}

func TestCheckRequirementsRejectMissingNodeBeforeQueries(t *testing.T) {
	b, srv := fixture(t)
	cfg := configuration(t, srv.URL, true, false)
	q := request()
	q.Scenario = ""
	q.Source = "metrics"
	q.Check = "disk-pressure"
	q.Bindings = &Bindings{Server: &Scope{Metrics: "local"}}
	var selected Definition
	for _, def := range Definitions(evidence.Policy{}) {
		if def.Source == "metrics" && CheckComponent(def) == "server" {
			for _, field := range def.Requires.Parameters {
				if field == "node_id" {
					selected = def
					break
				}
			}
		}
		if selected.ID != "" {
			break
		}
	}
	require.NotEmpty(t, selected.ID)
	q.Check = selected.Name
	r := Run(context.Background(), cfg, q, nil)
	require.Equal(t, "invalid", r.Status)
	require.Equal(t, "required_parameter_missing", r.Problems[0].Code)
	b.mu.Lock()
	require.Empty(t, b.metrics)
	b.mu.Unlock()
}

func TestAtomicCheckUsesOnlyItsRequiredConnectionDraft(t *testing.T) {
	b, srv := fixture(t)
	cfg := configuration(t, srv.URL, true, true)
	q := request()
	q.Scenario = ""
	q.Source = "metrics"
	q.Check = "writer-throughput"
	q.Bindings = &Bindings{Client: &Scope{Metrics: "local", Trace: "missing-unused-trace", TraceEndpoint: "invalid-unused-endpoint", LogID: "1", Metadata: ClientMetadata{Prefix: strings.Repeat("unused", 200)}}, Server: &Scope{Metrics: "missing-unused-server"}}
	r := Run(context.Background(), cfg, q, nil)
	require.Equal(t, "completed", r.Status)
	require.Len(t, r.Checks, 1)
	require.Nil(t, r.Request.Bindings.Server)
	require.Empty(t, r.Request.Bindings.Client.Trace)
	b.mu.Lock()
	require.Zero(t, b.trace)
	require.NotEmpty(t, b.metrics)
	b.mu.Unlock()
}

func TestTraceRuleThresholdReachesJaegerForOneHourWindow(t *testing.T) {
	b := &backend{traceLog: "1"}
	var queries []string
	var queryMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryMu.Lock()
		queries = append(queries, r.URL.RawQuery)
		queryMu.Unlock()
		b.handler(w, r)
	}))
	defer server.Close()
	c := configuration(t, server.URL, false, true)
	dir := t.TempDir()
	writeRules(t, dir, "trace", "evidence:\n  server-append-duration: {threshold: 2500, unit: ms}\n")
	c, err := WithRuleFiles(c, dir, nil)
	require.NoError(t, err)
	q := request()
	q.Scenario = ""
	q.Source = "trace"
	q.Check = "server-append-duration"
	q.From = q.To.Add(-time.Hour)
	report := Run(context.Background(), c, q, nil)
	require.Equal(t, "completed", report.Status, report.Problems)
	require.Equal(t, "0 matching slow spans", report.Checks[0].Value)
	require.Equal(t, "no_matching_candidates", report.Checks[0].DataState)
	require.Equal(t, "context", report.Checks[0].Status)
	queryMu.Lock()
	snapshot := append([]string{}, queries...)
	queryMu.Unlock()
	for _, raw := range snapshot {
		params, err := url.ParseQuery(raw)
		require.NoError(t, err)
		duration, err := time.ParseDuration(params.Get("minDuration"))
		require.NoError(t, err)
		require.Equal(t, 2500*time.Millisecond, duration)
		require.NotEmpty(t, params.Get("operation"))
	}
	queryMu.Lock()
	queries = nil
	queryMu.Unlock()
	threshold := 1000.0
	q.Threshold = &threshold
	report = Run(context.Background(), c, q, nil)
	require.Equal(t, "abnormal", report.Checks[0].Status)
	require.Len(t, report.Checks[0].Inspect.Spans, 1)
	queryMu.Lock()
	snapshot = append([]string{}, queries...)
	queryMu.Unlock()
	for _, raw := range snapshot {
		params, _ := url.ParseQuery(raw)
		duration, err := time.ParseDuration(params.Get("minDuration"))
		require.NoError(t, err)
		require.Equal(t, time.Second, duration)
	}
}
