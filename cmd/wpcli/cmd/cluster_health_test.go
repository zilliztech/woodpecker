package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestClusterHealth_Green(t *testing.T) {
	// 3 nodes, all reachable, all active — exceeds default N=3 in active count.
	// With 3 active and N=3, that's zero redundancy → YELLOW. Use N=2 for GREEN.
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","az":"us-east-1a"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","az":"us-east-1b"},
		{"id":"node-3","gossip_addr":"127.0.0.1:17946","az":"us-east-1c"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","version":"v0.1.26"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "health", "--expected-replicas", "2", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	require.Contains(t, buf.String(), "GREEN")
}

func TestClusterHealth_Yellow_ZeroRedundancy(t *testing.T) {
	// 3 active, N=3 → zero redundancy → YELLOW.
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","az":"us-east-1a"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","az":"us-east-1b"},
		{"id":"node-3","gossip_addr":"127.0.0.1:17946","az":"us-east-1c"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","version":"v0.1.26"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "health", "--admin-port", extractPort(t, srv.URL)})
	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 8, wperrors.ExitCodeFor(err))
	require.Contains(t, buf.String(), "YELLOW")
}

func TestClusterHealth_Red_OneUnreachable(t *testing.T) {
	// 3 members but one is at an unreachable IP → unreachableCount > 0 → RED.
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","az":"us-east-1a"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","az":"us-east-1b"},
		{"id":"node-3","gossip_addr":"10.255.255.255:17946","az":"us-east-1c"}
	]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","version":"v0.1.26"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "health", "--timeout", "1s", "--admin-port", extractPort(t, srv.URL)})

	err := root.Execute()
	require.Error(t, err)
	require.Equal(t, 9, wperrors.ExitCodeFor(err))
}
