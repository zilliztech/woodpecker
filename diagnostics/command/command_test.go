package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
)

func writeConfig(t *testing.T, endpoint string) string {
	t.Helper()
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	f := map[string]any{"current-context": "demo", "contexts": map[string]any{"demo": map[string]any{"endpoint": "http://wp.invalid:9091", "diagnostic_target": "log"}, "other": map[string]any{"endpoint": "http://other.invalid:9091", "diagnostic_target": "log"}}, "diagnostics": map[string]any{"sources": map[string]any{"metrics": map[string]any{"connections": map[string]any{"local": map[string]any{"settings": map[string]any{"endpoint": endpoint, "deployment": "fixture", "match_labels": map[string]string{"cluster": "fixture"}}}}}}, "targets": map[string]any{"log": map[string]any{"label": "Fixture log", "metrics": "local", "log_namespace": "tenant", "log_id": "1", "node_id": "node-1", "log_name": "log-name", "metadata": map[string]string{"etcd": "http://client-etcd.invalid:2379", "prefix": "milvus/client/woodpecker"}}}}}
	for _, value := range f["contexts"].(map[string]any) {
		value.(map[string]any)["observability"] = map[string]string{"prometheus": "local"}
	}
	b, err := yaml.Marshal(f)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return path
}

func execute(t *testing.T, args ...string) ([]byte, []byte, error) {
	t.Helper()
	root := &cobra.Command{Use: "wp", SilenceErrors: true, SilenceUsage: true}
	var output, ctx string
	root.PersistentFlags().StringVarP(&output, "output", "o", "table", "")
	root.PersistentFlags().StringVar(&ctx, "context", "", "")
	root.AddCommand(NewCommand(Options{Context: func() string { return ctx }, Output: func() string { return output }}))
	root.AddCommand(NewReportCommand(Options{Context: func() string { return ctx }, Output: func() string { return output }}))
	var out, stderr bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&stderr)
	root.SetArgs(args)
	err := root.ExecuteContext(context.Background())
	return out.Bytes(), stderr.Bytes(), err
}

func TestConfigContextAndDiagnosticsAreIndependentOfAgent(t *testing.T) {
	path := writeConfig(t, "http://metrics.invalid:9090")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, name := range []string{"demo", "other"} {
		c, target, err := Load(path, name)
		require.NoError(t, err)
		s, err := checks.LoadSettings(c)
		require.NoError(t, err)
		require.Equal(t, "context-"+name, target)
		require.Equal(t, name, s.Targets[target].Context)
		require.False(t, s.Targets[target].MetadataReady)
		require.Empty(t, s.Targets[target].Metadata.Prefix)
	}
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	f, err := config.Load(path)
	require.NoError(t, err)
	f.CurrentContext = "other"
	b, err := yaml.Marshal(f)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, b, 0o600))
	_, _, err = Load(path, "")
	require.NoError(t, err)
	// A bad optional model provider must not disable deterministic diagnosis.
	var raw map[string]any
	require.NoError(t, yaml.Unmarshal(before, &raw))
	raw["diagnostics"].(map[string]any)["workbench"] = map[string]any{"agent": map[string]any{"provider": "not-installed"}}
	b, err = yaml.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, b, 0o600))
	_, _, err = Load(path, "")
	require.NoError(t, err)
}

func TestExplicitMissingConfigDoesNotFallThrough(t *testing.T) {
	t.Setenv("WOODPECKER_CLI_CONFIG", filepath.Join(t.TempDir(), "absent.yaml"))
	_, _, err := Load("", "")
	require.ErrorContains(t, err, "WOODPECKER_CLI_CONFIG")
}

func TestAtomicChecksDoNotExecuteOtherChecks(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query().Get("query"))
		mu.Unlock()
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer source.Close()
	path := writeConfig(t, source.URL)
	for _, tc := range []struct {
		name, id string
		queries  int
	}{{"writer-throughput", "W03", 1}, {"append-pending", "W05", 1}, {"writer-progress", "W07", 2}} {
		mu.Lock()
		queries = nil
		mu.Unlock()
		out, progress, err := execute(t, "diag", "--config", path, "metrics", "check", tc.name, "--window", "5m", "--output", "json", "--progress")
		require.NoError(t, err, string(out))
		var r checks.Report
		require.NoError(t, json.Unmarshal(out, &r))
		require.Equal(t, checks.ReportSchema, r.SchemaVersion)
		require.Len(t, r.Checks, 1)
		require.Equal(t, tc.id, r.Checks[0].ID)
		require.Equal(t, "no_data", r.Checks[0].DataState)
		require.NotEmpty(t, progress)
		mu.Lock()
		n := len(queries)
		mu.Unlock()
		require.Equal(t, tc.queries, n)
	}
}

func TestScenarioDefaultTextJSONAndArbitraryHistoricalWindow(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer source.Close()
	path := writeConfig(t, source.URL)
	end := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	start := end.Add(-23*time.Minute - 17*time.Second)
	out, _, err := execute(t, "diag", "--config", path, "scenario", "slow-writes", "--node-id", "node-1", "--from", start.Format(time.RFC3339), "--to", end.Format(time.RFC3339), "--output", "json")
	require.NoError(t, err, string(out))
	var r checks.Report
	require.NoError(t, json.Unmarshal(out, &r))
	require.Equal(t, start, r.Request.From)
	require.Equal(t, end, r.Request.To)
	require.True(t, r.Summary.Metrics.Configured)
	require.False(t, r.Summary.Trace.Configured)
	require.Positive(t, r.Summary.Trace.Skipped)
	require.NotEmpty(t, r.Checks)
	for _, c := range r.Checks {
		require.NotEqual(t, "clear", c.Status)
	}
	out, _, err = execute(t, "diag", "--config", path, "metrics", "check", "writer-throughput", "--to", end.Format(time.RFC3339), "--window", "23m17s", "--output", "json")
	require.NoError(t, err, string(out))
	require.NoError(t, json.Unmarshal(out, &r))
	require.Equal(t, start, r.Request.From)
	out, _, err = execute(t, "diag", "--config", path, "metrics", "check", "writer-throughput")
	require.NoError(t, err)
	require.Contains(t, string(out), "Metrics: configured")
	require.NotContains(t, string(out), "schema_version")
}

func TestMissingConnectionsAndInvalidArgumentsAreJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(path, []byte("contexts: {}\n"), 0o600))
	for _, args := range [][]string{{"scenario", "all"}, {"metrics", "check", "writer-latency"}, {"trace", "check", "server-append-duration"}} {
		out, _, err := execute(t, append([]string{"diag", "--config", path, "--output", "json"}, args...)...)
		require.Error(t, err)
		var r checks.Report
		require.NoError(t, json.Unmarshal(out, &r))
		require.Equal(t, "connection_not_configured", r.Problems[0].Code)
		var code *Error
		require.ErrorAs(t, err, &code)
		require.Equal(t, 12, code.Code)
	}
	out, _, err := execute(t, "diag", "--config", path, "metrics", "check", "--output", "json")
	require.Error(t, err)
	require.True(t, json.Valid(out))
	out, _, err = execute(t, "diag", "--config", filepath.Join(t.TempDir(), "absent"), "scenario", "all", "--output", "json")
	require.Error(t, err)
	require.True(t, json.Valid(out))
}

func TestEmptyDiagnosticsMappingReportsMissingConnections(t *testing.T) {
	for _, content := range []string{"diagnostics: {}\n", "diagnostics:\n  sources: {}\n", "diagnostics:\n  sources: null\n"} {
		path := filepath.Join(t.TempDir(), "cli.yaml")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		out, _, err := execute(t, "diag", "--config", path, "scenario", "all", "--output", "json")
		var code *Error
		require.ErrorAs(t, err, &code)
		require.Equal(t, 12, code.Code)
		var report checks.Report
		require.NoError(t, json.Unmarshal(out, &report))
		require.Equal(t, "connection_not_configured", report.Problems[0].Code)
	}
}

func TestDefaultCatalogNeedsNoConfigurationOrSource(t *testing.T) {
	t.Setenv("WOODPECKER_CLI_CONFIG", "")
	out, _, err := execute(t, "diag", "catalog", "--output", "json")
	require.NoError(t, err)
	var c checks.Catalog
	require.NoError(t, json.Unmarshal(out, &c))
	require.Len(t, c.Checks, 55)
	require.Len(t, c.Scenarios, 7)
	for _, def := range c.Checks {
		require.Contains(t, []string{"metrics", "trace"}, def.Source)
		require.NotEmpty(t, def.Name)
	}
}

func TestRemovedEntrypointsAreNotRegistered(t *testing.T) {
	c := NewCommand(Options{})
	names := []string{}
	for _, child := range c.Commands() {
		names = append(names, child.Name())
	}
	require.ElementsMatch(t, []string{"catalog", "metrics", "trace", "scenario"}, names)
}

func TestBindingsFileDoesNotNeedCurrentContextOrLegacyTarget(t *testing.T) {
	path := writeConfig(t, "http://metrics.invalid:9090")
	f, err := config.Load(path)
	require.NoError(t, err)
	f.CurrentContext = "missing"
	body, err := yaml.Marshal(f)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o600))
	bindings := filepath.Join(t.TempDir(), "bindings.json")
	require.NoError(t, os.WriteFile(bindings, []byte(`{"server":{"name":"test-server","context":"missing"}}`), 0o600))
	out, _, err := execute(t, "diag", "scenario", "slow-writes", "--config", path, "--bindings-file", bindings, "-o", "json")
	require.Error(t, err)
	var report checks.Report
	require.NoError(t, json.Unmarshal(out, &report))
	require.Equal(t, "connection_not_configured", report.Problems[0].Code)
	require.NotNil(t, report.Request.Bindings.Server)
}

func TestServerContextOwnsObservabilityReferences(t *testing.T) {
	path := writeConfig(t, "http://legacy-metrics.invalid:9090")
	f, err := config.Load(path)
	require.NoError(t, err)
	ctx := f.Contexts["demo"]
	ctx.Observability = config.ObservabilityConfig{Prometheus: "server-prom", Trace: "server-trace", Grafana: "http://grafana:3000"}
	f.Contexts["demo"] = ctx
	body, err := yaml.Marshal(f)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o600))
	cfg, target, err := Load(path, "demo")
	require.NoError(t, err)
	settings, err := checks.LoadSettings(cfg)
	require.NoError(t, err)
	require.Equal(t, "server-prom", settings.Targets[target].Prometheus)
	require.Equal(t, "server-trace", settings.Targets[target].Trace)
}

func TestCatalogLoadsAdjacentRuleFilesWithoutRewritingCLI(t *testing.T) {
	path := writeConfig(t, "http://metrics.invalid:9090")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	rules := filepath.Join(filepath.Dir(path), "diag-metrics.yaml")
	require.NoError(t, os.WriteFile(rules, []byte("schema_version: wp.diag.rules/v1\nsource: metrics\nevidence:\n  W01: {threshold: 432, unit: ms}\n"), 0o600))
	out, _, err := execute(t, "diag", "--config", path, "catalog", "--check", "W01", "-o", "json")
	require.NoError(t, err)
	var result checks.Catalog
	require.NoError(t, json.Unmarshal(out, &result))
	require.Len(t, result.Checks, 1)
	require.Equal(t, 432.0, *result.Checks[0].Assessment.Threshold)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
