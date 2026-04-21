package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// spinTestServer mounts handlers for /admin/memberlist and /admin/node/status
// on a single httptest server. Used by multiple command tests in this package.
func spinTestServer(t *testing.T, memberlist string, nodeStatus map[string]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(memberlist))
	})
	for id, body := range nodeStatus {
		mux.HandleFunc("/admin/node/status-for/"+id, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		})
	}
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node_id":"node-default","state":"active","member_count":1}`))
	})
	return httptest.NewServer(mux)
}

// withCliYAML writes a cli.yaml pointing current-context at `prod` with the given
// endpoint and sets WOODPECKER_CLI_CONFIG to that file for this test.
func withCliYAML(t *testing.T, endpoint string) {
	t.Helper()
	dir := t.TempDir()
	cfg := `current-context: prod
contexts:
  prod:
    endpoint: ` + endpoint + `
`
	p := filepath.Join(dir, "cli.yaml")
	require.NoError(t, os.WriteFile(p, []byte(cfg), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", p)
}

// extractPort parses an httptest.Server.URL and returns the port as a string.
func extractPort(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	return u.Port()
}

func TestNodeList_HappyPath(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080","az":"us-east-1a","rg":"default","state":0,"incarnation":1,"last_seen_ms":1}
	]}`
	srv := spinTestServer(t, ml, nil)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	adminPort := extractPort(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "list", "--admin-port", adminPort, "-o", "json"})

	require.NoError(t, root.Execute())

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "node-1", rows[0]["name"])
}
