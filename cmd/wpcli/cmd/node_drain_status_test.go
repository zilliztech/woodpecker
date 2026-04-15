package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeDrainStatus_OneShot(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	progress := `{"state":"decommissioning","remaining_processors":3,"has_local_data":true,"safe_to_terminate":false}`

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(progress))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "drain-status", "node-1", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "decommissioning")
	require.Contains(t, buf.String(), "remaining_processors")
}
