package webui

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigPreviewPreservesOriginalFileBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cli.yaml")
	body := []byte("# Keep this comment and ordering\r\ncontexts:\r\n  dev:\r\n    endpoint: 'http://localhost:9091' # inline comment\r\n    password: &credential local-test-value\r\n    other: *credential\r\n" + strings.Repeat("# long source line\r\n", 500) + "# END OF FILE\r\n")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	preview, err := configPreview(path)
	require.NoError(t, err)
	require.Equal(t, string(body), preview)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, body, after)
}

func TestWorkspaceConfigPreviewReadsCurrentWholeFile(t *testing.T) {
	app, server, path, calls := workspaceFixture(t)
	client := browser(t, server.URL)
	before := calls.Load()
	code, body := wsCall(t, client, server.URL, app.token, "GET", "/api/workspace/config", nil)
	require.Equal(t, 200, code)
	var preview struct {
		YAML     string `json:"yaml"`
		Snapshot string `json:"snapshot"`
	}
	require.NoError(t, json.Unmarshal(body, &preview))
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(original), preview.YAML)
	updated := "# Changed on disk, without changing active contexts\n" + strings.Repeat("# content\n", 20000) + "# END OF FILE\n"
	require.NoError(t, os.WriteFile(path, []byte(updated), 0o600))
	code, body = wsCall(t, client, server.URL, app.token, "GET", "/api/workspace/config", nil)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &preview))
	require.Equal(t, updated, preview.YAML)
	require.Equal(t, "Current file on disk", preview.Snapshot)
	require.Equal(t, before, calls.Load())
	require.NotEmpty(t, app.workspace.file.Contexts)
}

func TestRulePreviewAndCatalogReload(t *testing.T) {
	app, server, path, calls := workspaceFixture(t)
	client := browser(t, server.URL)
	before := calls.Load()
	code, body := wsCall(t, client, server.URL, app.token, "GET", "/api/workspace/rules/metrics", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), `"exists":false`)
	file := filepath.Join(filepath.Dir(path), "diag-metrics.yaml")
	rules := "schema_version: wp.diag.rules/v1\nsource: metrics\nevidence:\n  W01: {threshold: 987, unit: ms}\n"
	require.NoError(t, os.WriteFile(file, []byte(rules), 0o600))
	code, body = wsCall(t, client, server.URL, app.token, "GET", "/api/workspace/rules/metrics", nil)
	require.Equal(t, 200, code)
	var preview struct {
		YAML   string `json:"yaml"`
		Exists bool   `json:"exists"`
	}
	require.NoError(t, json.Unmarshal(body, &preview))
	require.True(t, preview.Exists)
	require.Equal(t, rules, preview.YAML)
	code, body = wsCall(t, client, server.URL, app.token, "GET", "/api/diag/catalog", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), `"threshold":987`)
	require.NoError(t, os.WriteFile(file, []byte(strings.ReplaceAll(rules, "987", "654")), 0o600))
	code, body = wsCall(t, client, server.URL, app.token, "GET", "/api/diag/catalog", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), `"threshold":654`)
	require.NoError(t, os.WriteFile(file, []byte("invalid: true"), 0o600))
	code, _ = wsCall(t, client, server.URL, app.token, "GET", "/api/diag/catalog", nil)
	require.Equal(t, 422, code)
	require.Equal(t, before, calls.Load())
}
