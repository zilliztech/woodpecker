package diagnostics_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/wpcli"
)

func request() d.Request {
	return d.Request{Selection: d.Selection{Sources: []string{"wpcli"}, Scope: "wp", Flow: "write", Window: d.Window{Mode: "snapshot"}}, Limits: d.DefaultLimits(), Connection: "local"}
}

func config(t *testing.T, extra string) d.Config {
	t.Helper()
	c, err := d.ParseConfig([]byte("schema_version: wpdiag.config/v1\nsources:\n  wpcli:\n    connections:\n      local:\n"+extra), "/tmp")
	require.NoError(t, err)
	return c
}

func TestCapabilityAndSelectionMatrix(t *testing.T) {
	e, err := d.NewEngine(wpcli.Module{})
	require.NoError(t, err)
	catalog := e.Catalog()
	expected, err := os.ReadFile("testdata/capabilities-v1.json")
	require.NoError(t, err)
	actual, err := json.Marshal(catalog)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))
	require.Equal(t, d.CatalogSchema, catalog.SchemaVersion)
	require.Len(t, catalog.Sources, 4)
	require.Len(t, catalog.Sources[0].Profiles, 2)
	for _, s := range catalog.Sources[1:] {
		require.Equal(t, "not_implemented", s.Status)
		require.Empty(t, s.Profiles)
	}
	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			for _, mode := range []string{"snapshot", "live", "historical"} {
				t.Run(scope+"/"+flow+"/"+mode, func(t *testing.T) {
					r := request()
					r.Selection.Scope = scope
					r.Selection.Flow = flow
					r.Selection.Window.Mode = mode
					if mode == "live" {
						r.Selection.Window.DurationMS = 100
						r.Selection.Window.IntervalMS = 50
					}
					if mode == "historical" {
						r.Selection.Window.From = &from
						r.Selection.Window.To = &to
					}
					_, err := d.Normalize(r.Selection)
					require.NoError(t, err)
					report := e.Run(context.Background(), d.Config{}, r)
					if scope == "wp" && mode != "historical" {
						require.Equal(t, "invalid", report.Status)
						require.Equal(t, "connection_not_found", report.Results[0].Problems[0].Code)
					} else {
						require.Equal(t, "unsupported", report.Status)
					}
				})
			}
		}
	}
	for _, stage := range []string{"consume", "query"} {
		r := request()
		r.Selection.Flow = "read"
		r.Selection.Stage = stage
		_, err := d.Normalize(r.Selection)
		require.Error(t, err)
	}
	r := request()
	r.Selection.Stage = "wal_read"
	_, err = d.Normalize(r.Selection)
	require.Error(t, err)
	r = request()
	r.Selection.Target.LogID = "9223372036854775807"
	_, err = d.Normalize(r.Selection)
	require.NoError(t, err)
	r.Selection.Target.LogID = "9223372036854775808"
	_, err = d.Normalize(r.Selection)
	require.Error(t, err)
	r = request()
	r.Selection.Target.TenantAssociation = "tenant-from-label"
	_, err = d.Normalize(r.Selection)
	require.Error(t, err)
	r = request()
	r.Selection.Window.DurationMS = 1
	_, err = d.Normalize(r.Selection)
	require.Error(t, err)
}

func TestSourceAndConnectionFailures(t *testing.T) {
	e, err := d.NewEngine(wpcli.Module{})
	require.NoError(t, err)
	for _, tc := range []struct{ name, settings, status, code string }{
		{"disabled", "        enabled: false\n        settings: {}\n", "disabled", "connection_disabled"},
		{"invalid", "        settings: {executable: wp, context: local, password: sentinel-secret}\n", "invalid", "invalid_connection_settings"},
		{"missing executable", "        settings: {executable: /does/not/exist, context: local}\n", "failed", "native_start_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := e.Run(context.Background(), config(t, tc.settings), request())
			require.Equal(t, tc.status, report.Status)
			require.Equal(t, tc.code, report.Results[0].Problems[0].Code)
			body, _, err := d.EncodeReport(report)
			require.NoError(t, err)
			require.NotContains(t, string(body), "sentinel-secret")
			require.NotContains(t, string(body), "/does/not/exist")
		})
	}
	r := request()
	r.Selection.Sources = []string{"prometheus"}
	require.Equal(t, "unsupported", e.Run(context.Background(), d.Config{}, r).Status)
	r.Selection.Sources = []string{"wpcli", "loki"}
	require.Equal(t, "multi_source_not_implemented", e.Run(context.Background(), d.Config{}, r).Results[0].Problems[0].Code)
	r = request()
	r.Selection.Target.LogID = "2"
	require.Equal(t, "target_filter_not_supported", e.Run(context.Background(), d.Config{}, r).Results[0].Problems[0].Code)
}

type testModule struct {
	profiles []d.Profile
	calls    *int
}

func (m testModule) Descriptor() d.Descriptor {
	return d.Descriptor{Source: "prometheus", Profiles: m.profiles}
}
func (testModule) ValidateConnection(d.Connection) error { return nil }
func (m testModule) Run(ctx context.Context, _ d.Request, _ d.Connection) d.Result {
	*m.calls++
	<-ctx.Done()
	return d.NewResult("prometheus")
}

func TestModuleBoundaryCancellationAndAmbiguity(t *testing.T) {
	calls := 0
	p := d.Profile{ID: "custom", Version: 1, Scope: "e2e", Flow: "read", Stage: "custom_stage", Modes: []string{"snapshot"}}
	e, err := d.NewEngine(testModule{[]d.Profile{p}, &calls})
	require.NoError(t, err)
	cfg := d.Config{SchemaVersion: d.ConfigSchema, Sources: map[string]d.SourceConfig{"prometheus": {Connections: map[string]d.Connection{"local": {}}}}}
	r := request()
	r.Selection.Sources = []string{"prometheus"}
	r.Selection.Scope = "e2e"
	r.Selection.Flow = "read"
	r.Selection.Stage = "custom_stage"
	r.Limits.BudgetMS = 10
	report := e.Run(context.Background(), cfg, r)
	require.Equal(t, "budget_exceeded", report.Status)
	require.Equal(t, 1, calls)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report = e.Run(ctx, cfg, r)
	require.Equal(t, "cancelled", report.Status)
	require.Equal(t, 1, calls)
	p2 := p
	p2.ID = "other"
	e, err = d.NewEngine(testModule{[]d.Profile{p, p2}, &calls})
	require.NoError(t, err)
	require.Equal(t, "profile_not_supported", e.Run(context.Background(), cfg, r).Results[0].Problems[0].Code)
	_, err = d.NewEngine(testModule{[]d.Profile{p, p}, &calls})
	require.Error(t, err)
}

func TestReportBudgetAndPrivateConfig(t *testing.T) {
	e, err := d.NewEngine()
	require.NoError(t, err)
	r := request()
	r.Limits.MaxOutputBytes = 4096
	report := e.Run(context.Background(), d.Config{}, r)
	report.Results[0].Metadata = json.RawMessage(`"` + strings.Repeat("a", 5000) + `"`)
	body, code, err := d.EncodeReport(report)
	require.NoError(t, err)
	require.Equal(t, 5, code)
	require.LessOrEqual(t, len(body), 4096)
	require.True(t, json.Valid(body))
	require.Contains(t, string(body), "report_output_limit")
	r.Limits.BudgetMS = 0
	r.Limits.MaxOutputBytes = 0
	report = e.Run(context.Background(), d.Config{}, r)
	_, code, err = d.EncodeReport(report)
	require.NoError(t, err)
	require.Equal(t, 2, code)
	c := config(t, "        settings: {token: sentinel-secret}\n")
	body, err = json.Marshal(c)
	require.NoError(t, err)
	require.Equal(t, "{}", string(body))
	for _, yaml := range []string{"schema_version: wpdiag.config/v2", "schema_version: wpdiag.config/v1\npassword: sentinel-secret", "schema_version: wpdiag.config/v1\n---\nsentinel-secret"} {
		_, err := d.ParseConfig([]byte(yaml), "/tmp")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "sentinel-secret")
	}
}
