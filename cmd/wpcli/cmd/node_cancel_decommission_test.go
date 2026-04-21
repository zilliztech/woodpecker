package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCancelDecommission_HappyPath(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	var called int32
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission/cancel", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&called, 1)
		require.Equal(t, http.MethodPost, r.Method)
		_, _ = w.Write([]byte(`{"status":"decommission cancelled"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "cancel-decommission", "node-1", "-y", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Equal(t, int32(1), atomic.LoadInt32(&called))
	require.Contains(t, buf.String(), "cancelled")
}

func TestNodeCancelDecommission_Conflict(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission/cancel", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"already decommissioned"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "cancel-decommission", "node-1", "-y", "--admin-port", extractPort(t, srv.URL)})
	err := root.Execute()
	require.Error(t, err)
}
