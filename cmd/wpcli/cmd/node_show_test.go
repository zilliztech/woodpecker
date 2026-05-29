package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeShow_HappyPath(t *testing.T) {
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080","cluster_name":"cluster-a","region":"us-east-1","az":"us-east-1a","rg":"default","state":0}
	]}`
	status := `{
		"node_id":"node-1","state":"active","is_decommissioning":false,
		"member_count":1,"address":"127.0.0.1:18080","cluster_name":"cluster-a","region":"us-east-1","resource_group":"default","az":"us-east-1a",
		"tags":{"role":"logstore"},
		"started_at_ms":1712500000000,"version":"v0.1.26-test","last_health_check_ms":1712553600000
	}`

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mux.HandleFunc("/admin/node/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(status))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "show", "node-1", "--admin-port", extractPort(t, srv.URL)})

	require.NoError(t, root.Execute())
	out := buf.String()
	require.Contains(t, out, "node-1")
	require.Contains(t, out, "Identity")
	require.Contains(t, out, "Lifecycle")
	require.Contains(t, out, "cluster-a")
	require.Contains(t, out, "us-east-1")
	require.Contains(t, out, "v0.1.26-test")
}

func TestNodeShow_NotInMemberlist(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "show", "node-99", "--admin-port", extractPort(t, srv.URL)})

	err := root.Execute()
	require.Error(t, err)
}
