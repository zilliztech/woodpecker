package command

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
)

func TestReportConsumersNeedNoConfigurationOrSources(t *testing.T) {
	report := checks.Report{SchemaVersion: checks.ReportSchema, Request: checks.Request{Details: "all"}, Checks: []checks.Result{{Evidence: evidence.Evidence{ID: "W01", Status: "abnormal", Value: "500–1000 ms"}, Name: "writer-latency", Inspect: &checks.Inspection{Inspection: evidence.Inspection{Value: "500–1000 ms", Description: evidence.Description{Meaning: "Recorded Writer timer"}}}}}}
	body := checks.Encode(report)
	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	out, stderr, err := execute(t, "report", "inspect", path, "writer-latency", "--output", "json")
	require.NoError(t, err, string(stderr))
	require.True(t, json.Valid(out))
	require.Contains(t, string(out), "Recorded Writer timer")
	out, stderr, err = execute(t, "report", "export", path)
	require.NoError(t, err, string(stderr))
	require.Equal(t, body, out)
	out, stderr, err = execute(t, "report", "export", path, "--packet")
	require.NoError(t, err, string(stderr))
	require.Contains(t, string(out), "report_sha256")
}

func TestRemovedMilvusFlagIsRejected(t *testing.T) {
	_, _, err := execute(t, "diag", "scenario", "all", "--include-milvus")
	require.Error(t, err)
}

func TestReportAgentConfigIgnoresEvidenceRulesAndConnections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cli.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`current-context: local
contexts:
  local:
    endpoint: not-an-admin-origin
    observability:
      prometheus:
        endpoint: not-a-prometheus-origin
    agent:
      enabled: false
      provider: codex
      auth: cli
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "diag-metrics.yaml"), []byte("invalid: ["), 0o600))
	agent, err := reportAgentConfig(path, "")
	require.NoError(t, err)
	require.Equal(t, "codex", agent.Provider)
	require.False(t, agent.Enabled)
	_, err = reportAgentConfig(path, "missing")
	require.Error(t, err)
}
