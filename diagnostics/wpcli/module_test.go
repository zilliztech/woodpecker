package wpcli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func nativeConnection(t *testing.T, executable string) d.Connection {
	t.Helper()
	var c d.Connection
	body, err := yaml.Marshal(map[string]any{"executable": executable, "context": "fixture"})
	require.NoError(t, err)
	var node yaml.Node
	require.NoError(t, yaml.Unmarshal(body, &node))
	c.Settings = *node.Content[0]
	return c
}

func TestModuleSubprocessReportAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name, flow, status, exit, want, problem string
		inputLimit                              int64
	}{
		{name: "write", flow: "write", status: "completed", exit: "0", want: "completed"},
		{name: "read", flow: "read", status: "completed", exit: "0", want: "completed"},
		{name: "partial retains evidence", flow: "write", status: "partial", exit: "6", want: "partial"},
		{name: "exit mismatch", flow: "write", status: "completed", exit: "6", want: "failed", problem: "native_protocol_invalid"},
		{name: "incompatible schema", flow: "write", status: "incompatible", exit: "0", want: "failed", problem: "native_protocol_invalid"},
		{name: "stdout bound", flow: "write", status: "completed", exit: "0", want: "budget_exceeded", problem: "native_output_limit", inputLimit: 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := fixture(t, "scoped-write")
			if tc.flow == "read" {
				n = fixture(t, "ambiguous-read")
			}
			n.Status = tc.status
			if tc.status == "incompatible" {
				n.SchemaVersion = "wp.diagnosis/v2"
			}
			body, err := json.Marshal(n)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "native report.json")
			require.NoError(t, os.WriteFile(path, body, 0o600))
			t.Setenv("WPDIAG_TEST_NATIVE_REPORT", path)
			t.Setenv("WPDIAG_TEST_NATIVE_EXIT", tc.exit)
			r, _ := fixtureRequest(tc.flow)
			if tc.inputLimit != 0 {
				r.Limits.MaxInputBytes = tc.inputLimit
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := (Module{}).Run(ctx, r, nativeConnection(t, os.Args[0]))
			require.Equal(t, tc.want, result.Status)
			if tc.problem != "" {
				require.Equal(t, tc.problem, result.Problems[0].Code)
				require.Empty(t, result.Observations)
			} else {
				require.NotEmpty(t, result.Observations)
				require.Equal(t, n.Findings[0].ID, result.Findings[0].ID)
				require.Equal(t, n.Findings[0].Outcome, result.Findings[0].Outcome)
			}
		})
	}
	r, _ := fixtureRequest("write")
	result := (Module{}).Run(context.Background(), r, nativeConnection(t, ""))
	require.Equal(t, "invalid_connection_settings", result.Problems[0].Code)
	r.Selection.Window.DurationMS = 300001
	result = (Module{}).Run(context.Background(), r, nativeConnection(t, os.Args[0]))
	require.Equal(t, "native_budget_invalid", result.Problems[0].Code)
}

func TestConnectionPathsAndNativeRequestBudgets(t *testing.T) {
	c := nativeConnection(t, "./bin/wp")
	c.BaseDir = t.TempDir()
	var node yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte("executable: ./bin/wp\ncontext: fixture\ncli_config: configs/cli.yaml\n"), &node))
	c.Settings = *node.Content[0]
	s, err := settings(c)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(c.BaseDir, "bin/wp"), s.Executable)
	require.Equal(t, filepath.Join(c.BaseDir, "configs/cli.yaml"), s.CLIConfig)
	r, _ := fixtureRequest("write")
	r.Selection.Window = d.Window{Mode: "snapshot"}
	args, err := arguments(context.Background(), r, s)
	require.NoError(t, err)
	require.Contains(t, args, "--max-samples=2")
	require.Contains(t, args, "--mode=snapshot")
	r.Selection.Window = d.Window{Mode: "live", DurationMS: 2000, IntervalMS: 1000}
	r.Limits.MaxTargets = 32
	s.NativeLimits.MaxSeries = 5000
	_, err = arguments(context.Background(), r, s)
	require.Error(t, err)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	r.Limits.MaxTargets = 1
	s.NativeLimits.MaxSeries = 500
	_, err = arguments(ctx, r, s)
	require.Error(t, err)
}
