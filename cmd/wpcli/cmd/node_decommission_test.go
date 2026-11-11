package cmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeDecommission_AsyncMode(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`
	var decommissionCalled int32

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&decommissionCalled, 1)
		_, _ = w.Write([]byte(`{"status":"decommission started"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{
		"node", "decommission", "node-1",
		"--async", "-y",
		"--admin-port", extractPort(t, srv.URL),
	})
	require.NoError(t, root.Execute())
	require.Equal(t, int32(1), atomic.LoadInt32(&decommissionCalled))
	require.Contains(t, buf.String(), "decommission started")
}

func TestNodeDecommission_WaitUntilSafe(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`
	var progressCalls int32

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/decommission", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"started"}`))
	})
	mux.HandleFunc("/admin/node/decommission/progress", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&progressCalls, 1)
		safe := "false"
		if n >= 3 {
			safe = "true"
		}
		_, _ = fmt.Fprintf(w, `{"state":"decommissioning","remaining_processors":%d,"has_local_data":false,"safe_to_terminate":%s}`, 4-n, safe)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{
		"node", "decommission", "node-1", "-y",
		"--interval", "10ms",
		"--heartbeat-interval", "100ms",
		"--timeout", "5s",
		"--admin-port", extractPort(t, srv.URL),
	})
	require.NoError(t, root.Execute())
	require.GreaterOrEqual(t, atomic.LoadInt32(&progressCalls), int32(3))
	require.Contains(t, buf.String(), "Safe to terminate")
}
