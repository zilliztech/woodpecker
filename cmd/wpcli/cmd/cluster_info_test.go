package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterInfo_HappyPath(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","cluster_name":"cluster-a","region":"us-east-1","az":"us-east-1a","rg":"default"},
		{"id":"node-2","gossip_addr":"127.0.0.1:17946","cluster_name":"cluster-a","region":"us-east-1","az":"us-east-1a","rg":"default"},
		{"id":"node-3","gossip_addr":"127.0.0.1:17946","cluster_name":"cluster-a","region":"us-east-1","az":"us-east-1b","rg":"default"}
	]}`

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id":"x","state":"active","member_count":3}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"cluster", "info", "--admin-port", extractPort(t, srv.URL)})
	require.NoError(t, root.Execute())
	out := buf.String()
	require.Contains(t, out, "Total Nodes")
	require.Contains(t, out, "cluster-a")
	require.Contains(t, out, "us-east-1")
	require.Contains(t, out, "us-east-1a")
	require.Contains(t, out, "node-1")
}
