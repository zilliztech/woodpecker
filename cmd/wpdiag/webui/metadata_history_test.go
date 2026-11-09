package webui

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
)

func TestInputCandidatesAreSeparateFromConfigAndNotAttachments(t *testing.T) {
	app, srv, path, calls := workspaceFixture(t)
	c := browser(t, srv.URL)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	code, data := wsCall(t, c, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "connect-meta", "etcd": "http://etcd:2379", "prefix": "tenant/wp"})
	require.Equal(t, 200, code, string(data))
	values, err := candidates.List()
	require.NoError(t, err)
	require.Empty(t, values, "selecting inputs is not a successful source query")
	// Simulate inputs from an executed CLI metadata operation. The WebUI sees
	// the same per-field store, without inferring endpoint/prefix relationships.
	require.NoError(t, candidates.Save(candidates.Values{"metadata.etcd": {"http://other-etcd:2379"}, "metadata.prefix": {"other/wp"}}))
	code, data = wsCall(t, c, srv.URL, app.token, "GET", "/api/workspace/candidates", nil)
	require.Equal(t, 200, code, string(data))
	require.Contains(t, string(data), "other/wp")
	beforePreviewCalls := calls.Load() // The preview reads only the local candidate cache.
	code, preview := wsCall(t, c, srv.URL, app.token, "GET", "/api/workspace/candidates/config", nil)
	require.Equal(t, 200, code, string(preview))
	var configPreview struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	require.NoError(t, json.Unmarshal(preview, &configPreview))
	require.Equal(t, filepath.Join(candidates.Directory(), "recent-inputs.json"), configPreview.Path)
	contents, err := os.ReadFile(configPreview.Path)
	require.NoError(t, err)
	require.Equal(t, string(contents), configPreview.Content)
	require.Contains(t, configPreview.Content, "other/wp")
	require.NotContains(t, configPreview.Content, "contexts")
	require.Equal(t, beforePreviewCalls, calls.Load())
	fresh := browser(t, srv.URL)
	code, data = wsCall(t, fresh, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code)
	var response struct {
		Session browserSession   `json:"session"`
		History []metadataRecent `json:"metadata_history"`
	}
	require.NoError(t, json.Unmarshal(data, &response))
	require.Nil(t, response.Session.Metadata)
	require.Empty(t, response.History)
	code, data = wsCall(t, c, srv.URL, app.token, "POST", "/api/workspace/candidates", map[string]string{"key": "metadata.etcd", "value": "http://other-etcd:2379"})
	require.Equal(t, 200, code, string(data))
	values, err = candidates.List()
	require.NoError(t, err)
	require.Empty(t, values["metadata.etcd"])
	require.Equal(t, "other/wp", values["metadata.prefix"][0].Value)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = os.Stat(path + ".webui-history.json")
	require.True(t, os.IsNotExist(err))
}
