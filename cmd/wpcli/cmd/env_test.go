package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvShow_Build(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	env := `{
		"env":{"PATH":"/usr/bin"},
		"runtime":{"go_version":"go1.24.2"},
		"host":{"hostname":"node-1","os":"linux","arch":"amd64"},
		"build":{"version":"v0.1.26","commit":"abc1234","build_time":"2026-04-09T00:00:00Z","go_version":"go1.24.2"}
	}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/env", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(env)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"env", "show", "node-1", "--section", "build", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "v0.1.26")
}
