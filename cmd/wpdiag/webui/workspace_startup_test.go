package webui

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

func TestWorkspaceDoesNotRunAgentLoginDuringPageSetup(t *testing.T) {
	app, server, _, calls := workspaceFixture(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "login-started")
	executable := filepath.Join(dir, "fake-codex")
	require.NoError(t, os.WriteFile(executable, []byte(fmt.Sprintf("#!/bin/sh\nprintf checked > %q\nexit 0\n", marker)), 0o700))
	ctx := app.workspace.file.Contexts["a"]
	require.NoError(t, ctx.Agent.Encode(map[string]any{"enabled": true, "provider": "codex", "auth": "cli", "executable": executable}))
	app.workspace.file.Contexts["a"] = ctx
	client := browser(t, server.URL)
	code, body := wsCall(t, client, server.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code)
	var info struct {
		Agent wal.AgentState `json:"agent"`
	}
	require.NoError(t, json.Unmarshal(body, &info))
	require.Equal(t, "agent_status_pending", info.Agent.Code)
	require.False(t, info.Agent.Ready)
	_, err := os.Stat(marker)
	require.True(t, os.IsNotExist(err), "page setup must not spawn the Agent CLI")
	code, body = wsCall(t, client, server.URL, app.token, "GET", "/api/workspace/agent", nil)
	require.Equal(t, 200, code)
	var status wal.AgentState
	require.NoError(t, json.Unmarshal(body, &status))
	require.True(t, status.Ready, status)
	_, err = os.Stat(marker)
	require.NoError(t, err)
	require.Zero(t, calls.Load(), "setup and local login checks must not query backend sources")
}

func TestStaticAssetsDoNotReadDiagnosticRuleFilesForEachNewSession(t *testing.T) {
	app, server, path, _ := workspaceFixture(t)
	// New browser sessions and static assets must not validate the diagnostic files.
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(path), "diag-metrics.yaml"), []byte("invalid: true\n"), 0o600))
	client := browser(t, server.URL)
	code, body := wsCall(t, client, server.URL, app.token, "GET", "/", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), "overview-modules")
	code, _ = wsCall(t, client, server.URL, app.token, "GET", "/api/diag/catalog", nil)
	require.Equal(t, 422, code, "real diagnostic requests still validate current rule files")
}
