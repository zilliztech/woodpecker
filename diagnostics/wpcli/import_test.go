package wpcli

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

var updateFixture = flag.Bool("update", false, "Update the versioned adapter report fixture")

func TestImportedReportFixture(t *testing.T) {
	n := fixture(t, "scoped-write")
	result, err := importFixture(t, n, 0)
	require.NoError(t, err)
	r, _ := fixtureRequest("write")
	r.Selection.Profile = r.Profile.ID
	result.Profile = r.Profile.ID
	result.Connection = "local"
	report := d.Report{SchemaVersion: d.ReportSchema, RunID: "fixture-454", Selection: r.Selection, Limits: r.Limits, StartedAt: n.StartedAt, FinishedAt: n.FinishedAt, Status: result.Status, Results: []d.Result{result}}
	actual, _, err := d.EncodeReport(report)
	require.NoError(t, err)
	path := filepath.Join("testdata", "scoped-write-import-v1.json")
	if *updateFixture {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, actual, 0o644))
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))
}

func fixture(t *testing.T, name string) Report {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "cmd", "wpcli", "diagnosis", "testdata", name+"-v1.json"))
	require.NoError(t, err)
	var native Report
	require.NoError(t, json.Unmarshal(body, &native))
	native.Context = "fixture"
	return native
}

func fixtureRequest(flow string) (d.Request, Settings) {
	m := Module{}
	p := m.Descriptor().Profiles[0]
	if flow == "read" {
		p = m.Descriptor().Profiles[1]
	}
	r := d.Request{Selection: d.Selection{Sources: []string{"wpcli"}, Scope: "wp", Flow: flow, Stage: p.Stage, Window: d.Window{Mode: "live", DurationMS: 2000, IntervalMS: 1000}}, Profile: p, Limits: d.DefaultLimits()}
	s := Settings{Context: "fixture", NativeLimits: NativeLimits{MaxSamples: 121, MaxOps: 100, MaxSeries: 500, ResponseBytes: 2 << 20, Concurrency: 8, TimeoutMS: 30000}}
	return r, s
}

func importFixture(t *testing.T, n Report, exit int) (d.Result, error) {
	t.Helper()
	r, s := fixtureRequest(n.Rules[0].Flow)
	body, err := json.Marshal(n)
	require.NoError(t, err)
	return importReport(body, exit, r, s)
}

func TestNativeImportPreservesMeaningAndIDs(t *testing.T) {
	for _, name := range []string{"scoped-write", "ambiguous-read"} {
		t.Run(name, func(t *testing.T) {
			n := fixture(t, name)
			result, err := importFixture(t, n, 0)
			require.NoError(t, err)
			require.Equal(t, "completed", result.Status)
			require.Len(t, result.Observations, len(n.Observations))
			require.Len(t, result.Findings, len(n.Findings))
			for i, f := range result.Findings {
				original := n.Findings[i]
				require.Equal(t, original.ID, f.ID)
				require.Equal(t, original.Outcome, f.Outcome)
				require.Equal(t, original.Strength, f.Strength)
				require.Equal(t, original.Supporting, f.Supporting)
				require.Equal(t, original.Unknown, f.Unknown)
				require.Equal(t, original.Scope.Tenant, f.Target.TenantAssociation)
			}
			for i, o := range result.Observations {
				require.Equal(t, n.Observations[i].ID, o.ID)
				require.Equal(t, n.Observations[i].SourceID, o.SourceID)
				require.Equal(t, n.Observations[i].StartedAt, o.StartedAt)
			}
			require.Contains(t, string(result.Metadata), `"native_run_id":"fixture-451"`)
		})
	}
	n := fixture(t, "scoped-write")
	large := int64(9223372036854775807)
	n.Findings[0].Scope.LogID = &large
	n.Observations[1].Ops[0].LogID = &large
	result, err := importFixture(t, n, 0)
	require.NoError(t, err)
	require.Equal(t, "9223372036854775807", result.Findings[0].Target.LogID)
	require.Contains(t, string(result.Observations[1].Data), `"log_id":"9223372036854775807"`)
	n.Observations = append(n.Observations, n.Observations[0])
	result, err = importFixture(t, n, 0)
	require.NoError(t, err)
	require.Len(t, result.Observations, len(n.Observations)-1)
	require.Equal(t, "duplicate_observation_deduplicated", result.Gaps[0].Code)
}

func TestInvalidNativeContractsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Report)
		exit int
	}{
		{"schema", func(n *Report) { n.SchemaVersion = "wp.diagnosis/v2" }, 0},
		{"unknown rule", func(n *Report) { n.Rules[0].Version = 2 }, 0},
		{"rule mismatch", func(n *Report) { n.Findings[0].RuleVersion = 2 }, 0},
		{"exit mismatch", func(_ *Report) {}, 6},
		{"unexpected finding exit", func(_ *Report) {}, 8},
		{"wrong context", func(n *Report) { n.Context = "other" }, 0},
		{"historical masquerade", func(n *Report) { n.Mode = "historical" }, 0},
		{"window mismatch", func(n *Report) { n.WindowMS = 500 }, 0},
		{"node ambiguity", func(n *Report) { n.Targets = append(n.Targets, n.Targets[0]) }, 0},
		{"missing evidence", func(n *Report) { n.Findings[0].Supporting = []string{"missing"} }, 0},
		{"conflicting duplicate", func(n *Report) {
			o := n.Observations[0]
			o.Code = "different"
			n.Observations = append(n.Observations, o)
		}, 0},
		{"duplicate lineage", func(n *Report) { o := n.Observations[0]; o.ID = "new-id"; n.Observations = append(n.Observations, o) }, 0},
		{"duplicate finding", func(n *Report) { n.Findings = append(n.Findings, n.Findings[0]) }, 0},
		{"double counted proof", func(n *Report) { n.Findings[0].Contradicting = n.Findings[0].Supporting[:1] }, 0},
		{"outside actual window", func(n *Report) { n.Observations[0].StartedAt = n.StartedAt.Add(-time.Second) }, 0},
		{"source limits", func(n *Report) { n.Limits.MaxOps = 1001 }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := fixture(t, "scoped-write")
			tc.edit(&n)
			_, err := importFixture(t, n, tc.exit)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "fixture-service")
		})
	}
	for status, exit := range map[string]int{"failed": 1, "partial": 6, "cancelled": 7, "budget_exceeded": 5} {
		n := fixture(t, "scoped-write")
		n.Status = status
		result, err := importFixture(t, n, exit)
		require.NoError(t, err)
		require.Equal(t, status, result.Status)
		require.NotEmpty(t, result.Observations)
	}
}

func TestAllowlistUnknownFieldsAndLiteralArguments(t *testing.T) {
	n := fixture(t, "scoped-write")
	n.Observations[0].Counters[0].Labels["password"] = "sentinel-secret"
	body, err := json.Marshal(n)
	require.NoError(t, err)
	body = append([]byte(`{"env":{"token":"sentinel-secret"},`), body[1:]...)
	r, s := fixtureRequest("write")
	result, err := importReport(body, 0, r, s)
	require.NoError(t, err)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "sentinel-secret")
	r.Selection.Target.NodeID = "--endpoint=malicious"
	s.Context = "--help"
	args, err := arguments(context.Background(), r, s)
	require.NoError(t, err)
	require.Equal(t, []string{"--", "--endpoint=malicious"}, args[len(args)-2:])
	require.Equal(t, "--context=--help", args[0])
	r.Selection.Window.DurationMS = 300001
	_, err = arguments(context.Background(), r, s)
	require.Error(t, err)
	r.Selection.Window.DurationMS = 2000
	s.NativeLimits.MaxSamples = 2
	_, err = arguments(context.Background(), r, s)
	require.Error(t, err)
}
