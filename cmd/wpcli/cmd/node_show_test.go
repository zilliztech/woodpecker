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

// TestNodeShow_ExplicitAddressIsDialedDirectly covers the one way into a cluster from outside it.
// Nodes advertise the address their own cluster knows them by — under Kubernetes a headless-service
// FQDN that resolves only inside that network — so from outside, a port-forward to a single pod is
// what an operator actually has. Resolution against the memberlist cannot name that address, which
// leaves the reachable endpoint unusable.
func TestNodeShow_ExplicitAddressIsDialedDirectly(t *testing.T) {
	// The memberlist knows nothing about 127.0.0.1: only the unreachable in-cluster names.
	ml := `{"members":[
		{"id":"node-1","gossip_addr":"10.244.1.5:17946","service_addr":"10.244.1.5:18080"}
	]}`
	srv := spinTestServer(t, ml, nil)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	// The stub answers /admin/node/status on its own address, which is the one that works.
	root.SetArgs([]string{"node", "show", "127.0.0.1:" + extractPort(t, srv.URL), "--timeout", "3s"})

	err := root.Execute()

	require.NoError(t, err, "an explicit address should be dialed as given, not looked up in the memberlist")
	require.Contains(t, buf.String(), "node-default")
}

// TestNodeLogHealth_ReportsPerLogHealth and TestNodeHealthz_ReportsProbeResult cover the two
// admin endpoints that had no command. /healthz in particular is the actual readiness probe:
// `wp cluster health` sounds like it consults it but fans out /admin/node/status instead, so
// what Kubernetes acts on was not reachable from the CLI at all.
func TestNodeLogHealth_ReportsPerLogHealth(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`))
	})
	mux.HandleFunc("/admin/log-health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":"Healthy","reason":"","node_id":"node-1","tracked_logs":3,"healthy_logs":2,"stalled_logs":1,"failed_logs":0,"idle_logs":0,"logs":[]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "log-health", "127.0.0.1:" + extractPort(t, srv.URL)})

	require.NoError(t, root.Execute())
	out := buf.String()
	require.Contains(t, out, "Healthy")
	require.Contains(t, out, "3")
	require.Contains(t, out, "1", "a stalled log is the reason to run this at all")
}

func TestNodeHealthz_ReportsProbeResult(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"state":"Not all components are healthy, 1/2","detail":[{"name":"LogStore","code":"Healthy"},{"name":"Membership","code":"Abnormal"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "healthz", "127.0.0.1:" + extractPort(t, srv.URL)})

	err := root.Execute()

	require.Error(t, err, "an unhealthy probe must not report success")
	require.Contains(t, buf.String(), "Abnormal", "the failing component is the whole point of the output")
}

// TestResolvedConfigSourceIsAnnounced is the condition attached to auto-discovery. Picking up a
// cli.yaml from beside the binary makes it possible to be pointed somewhere unintended: an
// operator with a staging config in their home directory who runs a toolkit copy carrying a
// production one gets no signal at all. Saying what resolved is what keeps that from being silent.
func TestResolvedConfigSourceIsAnnounced(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946","service_addr":"127.0.0.1:18080"}]}`
	srv := spinTestServer(t, ml, nil)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "list", "--admin-port", extractPort(t, srv.URL), "-v"})

	require.NoError(t, root.Execute())

	out := buf.String()
	require.Contains(t, out, "cli.yaml", "the resolved config file should be named")
	require.Contains(t, out, srv.URL, "the endpoint being acted on should be named")
}
