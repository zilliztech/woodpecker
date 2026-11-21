package webui

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The read-only config preview is reachable before a browser session exists,
// so credentials in cli.yaml must be masked in the response body.
func TestConfigPreviewRedactsSecretsForAnonymousReaders(t *testing.T) {
	_, srv, path, _ := workspaceFixture(t)
	const secret = "CONFIG_PREVIEW_SECRET_DO_NOT_LEAK_9f3a"
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	body = append(body, []byte("    secretAccessKey: "+secret+"\n    password: "+secret+"\n")...)
	require.NoError(t, os.WriteFile(path, body, 0o600))

	res, err := http.Get(srv.URL + "/api/workspace/config")
	require.NoError(t, err)
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode, string(raw))
	require.NotContains(t, string(raw), secret, "anonymous preview leaked a credential")
	require.Contains(t, string(raw), "secretAccessKey")
	require.Contains(t, string(raw), "[redacted]")

	var preview struct {
		YAML     string `json:"yaml"`
		Redacted bool   `json:"redacted"`
	}
	require.NoError(t, json.Unmarshal(raw, &preview))
	require.True(t, preview.Redacted)
	require.NotContains(t, preview.YAML, secret)
	require.Contains(t, preview.YAML, "current-context: a")
}

// An established browser session still sees the current file with only the
// credential values masked, so the read-only viewer stays useful.
func TestConfigPreviewKeepsAuthorizedPreviewUsable(t *testing.T) {
	app, srv, path, _ := workspaceFixture(t)
	client := browser(t, srv.URL)
	const secret = "CONFIG_PREVIEW_AUTHORIZED_SECRET_7b2c"
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	body = append(body, []byte("    secretAccessKey: "+secret+"\n")...)
	require.NoError(t, os.WriteFile(path, body, 0o600))

	code, raw := wsCall(t, client, srv.URL, app.token, "GET", "/api/workspace/config", nil)
	require.Equal(t, 200, code, string(raw))
	require.NotContains(t, string(raw), secret)
	require.Contains(t, string(raw), "current-context: a")
	require.Contains(t, string(raw), "diagnostics:")
}
