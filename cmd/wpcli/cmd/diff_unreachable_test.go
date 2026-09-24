package cmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// diffTestCluster spins a server that answers the memberlist for `n` nodes and serves
// /admin/config and /admin/env for the ones named in `reachable`. A node is made unreachable
// by pointing its admin_port at a port nothing listens on.
func diffTestCluster(t *testing.T, ids []string, reachable map[string]bool, body string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for _, path := range []string{"/admin/config", "/admin/env"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	live := extractPort(t, srv.URL)

	members := ""
	for i, id := range ids {
		port := "1" // nothing listens here
		if reachable[id] {
			port = live
		}
		if i > 0 {
			members += ","
		}
		members += fmt.Sprintf(`{"id":%q,"gossip_addr":"127.0.0.1:1794%d","service_addr":"127.0.0.1:1808%d","tags":{"admin_port":%q}}`, id, i, i, port)
	}
	ml := `{"members":[` + members + `]}`

	mlMux := http.NewServeMux()
	mlMux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(ml))
	})
	mlSrv := httptest.NewServer(mlMux)
	t.Cleanup(mlSrv.Close)
	return mlSrv
}

func runDiff(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(append(args, "--timeout", "2s"))
	err := root.Execute()
	return buf.String(), err
}

// TestConfigDiff_AllUnreachableIsNotAgreement pins the worst outcome: a failed fetch used to be
// stored as a value, so with every node down each entry held the same sentinel, every comparison
// was equal, and the command reported full agreement at exit 0. "Nothing answered" and "every
// node agrees" are opposite findings.
func TestConfigDiff_AllUnreachableIsNotAgreement(t *testing.T) {
	srv := diffTestCluster(t, []string{"node-1", "node-2"}, map[string]bool{}, `{"a":1}`)
	withCliYAML(t, srv.URL)

	out, err := runDiff(t, "config", "diff", "--all")

	require.Error(t, err, "a run that read nothing cannot report agreement")
	require.NotContains(t, out, "identical", "an unreachable node is not identical to anything")
}

// TestEnvDiff_AllUnreachableIsNotAgreement is the same defect in the sibling command.
func TestEnvDiff_AllUnreachableIsNotAgreement(t *testing.T) {
	srv := diffTestCluster(t, []string{"node-1", "node-2"}, map[string]bool{}, `{"a":1}`)
	withCliYAML(t, srv.URL)

	out, err := runDiff(t, "env", "diff")

	require.Error(t, err, "a run that read nothing cannot report agreement")
	require.NotContains(t, out, "identical", "an unreachable node is not identical to anything")
}

// TestConfigDiff_PartialMarksUnreachableAndWarns covers the case an error cannot cover: some
// nodes answered, so the command succeeds and prints a report. The unreachable node must not be
// counted as agreeing, and the report must say it is incomplete.
func TestConfigDiff_PartialMarksUnreachableAndWarns(t *testing.T) {
	srv := diffTestCluster(t, []string{"node-1", "node-2"}, map[string]bool{"node-1": true}, `{"a":1}`)
	withCliYAML(t, srv.URL)

	out, err := runDiff(t, "config", "diff", "--all")

	require.NoError(t, err, "one node answered, so there is a report to show")
	require.Contains(t, out, "UNREACHABLE", "an unreachable node must be named as such")
	require.NotContains(t, out, "node-2: identical", "an unreachable node was never compared")
	require.Contains(t, out, "this view is incomplete", "a partial comparison must say so")
}

// TestConfigDiff_ReferenceFallsBackToAReachableNode matches the behaviour --all already
// advertises ("against the first reachable node"). With the first node down, comparing against
// its absent config makes every other unreachable node look identical to it.
func TestConfigDiff_ReferenceFallsBackToAReachableNode(t *testing.T) {
	srv := diffTestCluster(t, []string{"node-1", "node-2"}, map[string]bool{"node-2": true}, `{"a":1}`)
	withCliYAML(t, srv.URL)

	out, err := runDiff(t, "config", "diff", "--all")

	require.NoError(t, err)
	require.Contains(t, out, "node-2: (reference)", "the reference must be a node that answered")
}
