package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/proto"
)

func TestMappedQuorumRequests(t *testing.T) {
	old := Globals
	defer func() { Globals = old }()
	Globals.Timeout = time.Second
	addresses := []string{"pod-0.cluster.invalid:18080", "pod-1.cluster.invalid:18080"}
	mappings := map[string]string{}
	var mu sync.Mutex
	calls := make([]map[string]int, 2)
	for i, addr := range addresses {
		calls[i] = map[string]int{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls[i][r.URL.Path]++
			mu.Unlock()
			switch r.URL.Path {
			case "/admin/logstore/segments":
				require.Equal(t, "7", r.URL.Query().Get("log_id"))
				fmt.Fprint(w, `{"segments":[{"segment_id":3,"last_entry_id":9}]}`)
			case "/admin/logstore/segment/probe":
				fmt.Fprint(w, `{"first_entry":0,"last_entry":9,"entries_read":10,"outcome":"ok"}`)
			case "/admin/logstore/segment/inspect":
				fmt.Fprint(w, `{"survey":{"blocks":[],"lac":9}}`)
			case "/admin/logstore/fence":
				require.Equal(t, http.MethodPost, r.Method)
				fmt.Fprint(w, `{}`)
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(srv.Close)
		mappings[addr] = srv.URL
	}
	ac := client.New("http://seed.invalid", client.ClientOpts{NodeAdminURLs: mappings})
	// Second node represents a historical quorum member missing from discovery.
	members := &client.Memberlist{Members: []client.Member{{ID: "node-0", ServiceAddr: addresses[0]}}}
	quorum := &proto.QuorumInfo{Nodes: append(append([]string{}, addresses...), "unknown.invalid:18080")}
	positions := collectQuorumPositions(ac, members, quorum, 7, 3)
	probes := probeEachNode(ac, members, quorum, 7, 3, 0, 10)
	inspections := inspectEachNode(ac, members, quorum, 7, 3, 0, 10)
	fences := fenceEachNode(ac, members, quorum.Nodes, 7, 3, "mapping regression")
	scans := scanSegmentNodes(ac, members, segmentMeta{id: 3, meta: &proto.SegmentMetadata{Quorum: quorum}}, 7, scanModeQuick)
	mu.Lock()
	defer mu.Unlock()
	for i, addr := range addresses {
		require.Equal(t, posOK, positions[i].State)
		require.Equal(t, int64(9), positions[i].Durable)
		require.Equal(t, addr, positions[i].Node)
		require.Equal(t, posOK, probes[i].State)
		require.Equal(t, posOK, inspections[i].State)
		require.Equal(t, fenceDone, fences[i].State)
		require.True(t, scans[i].answered)
		require.Equal(t, map[string]int{"/admin/logstore/segments": 1, "/admin/logstore/segment/probe": 1, "/admin/logstore/segment/inspect": 2, "/admin/logstore/fence": 1}, calls[i])
	}
	require.Equal(t, addresses[1], scans[1].label)
	require.Equal(t, posUnknownNode, positions[2].State)
	require.Equal(t, posUnknownNode, probes[2].State)
	require.Equal(t, posUnknownNode, inspections[2].State)
	require.Equal(t, posUnknownNode, fences[2].State)
	require.Equal(t, posUnknownNode, scans[2].state)
	_, err := resolveFenceTargets(quorum, members, []string{mappings[addresses[0]]})
	require.ErrorContains(t, err, "not a member")
}

func TestResolveNodeAdminMappingOverrides(t *testing.T) {
	srv := spinTestServer(t, `{"members":[{"id":"node-0","service_addr":"pod-0:18080"}]}`, nil)
	defer srv.Close()
	withCliYAML(t, srv.URL)
	path := os.Getenv("WOODPECKER_CLI_CONFIG")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString("    node_admin_urls:\n      pod-0:18080: http://localhost:19091\n      pod-1:18080: http://localhost:29091\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	old := Globals
	defer func() { Globals = old }()
	Globals = GlobalFlags{Timeout: time.Second, NodeAdminURLs: []string{"pod-0:18080=http://localhost:39091/"}}
	res, err := resolveAndDiscover()
	require.NoError(t, err)
	require.Equal(t, "http://localhost:39091", res.Client.PeerAdminURL(res.Members.Members[0]))
	require.Equal(t, "http://localhost:29091", res.Context.NodeAdminURLs["pod-1:18080"])
	for _, bad := range []string{"missing-equals", "pod-0=ftp://localhost:9091", "pod-0=http://localhost/path"} {
		Globals.NodeAdminURLs = []string{bad}
		Globals.Endpoint = "http://unreachable.invalid"
		_, err = resolveAndDiscover()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "fetch memberlist")
	}
}

func TestNodeListMappedFanout(t *testing.T) {
	oldGlobals, oldWriter := Globals, sourceWriter
	defer func() { Globals, sourceWriter = oldGlobals, oldWriter }()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/admin/node/status", r.URL.Path)
		fmt.Fprint(w, `{"state":"decommissioned"}`)
	}))
	defer peer.Close()
	seed := spinTestServer(t, `{"members":[{"id":"node-0","service_addr":"pod-0.invalid:18080"},{"id":"node-1","service_addr":"pod-1.invalid:18080"}]}`, map[string]string{"/admin/node/status": `{"state":"active"}`})
	defer seed.Close()
	withCliYAML(t, seed.URL)
	for _, strict := range []bool{false, true} {
		root := NewRootCommand()
		var out, diagnostic bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&diagnostic)
		args := []string{"node", "list", "-o", "json", "--node-admin-url", "pod-0.invalid:18080=" + seed.URL, "--node-admin-url", "pod-1.invalid:18080=" + peer.URL}
		if strict {
			args = append(args, "--strict")
		}
		root.SetArgs(args)
		require.NoError(t, root.Execute())
		var rows []nodeListRow
		require.NoError(t, json.Unmarshal(out.Bytes(), &rows))
		require.Len(t, rows, 2)
		require.Equal(t, "pod-0.invalid:18080", rows[0].Addr)
		require.Equal(t, "active", rows[0].State)
		require.Equal(t, "decommissioned", rows[1].State)
	}
	peer.Close()
	for _, strict := range []bool{false, true} {
		root := NewRootCommand()
		var out, diagnostic bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&diagnostic)
		args := []string{"node", "list", "-o", "json", "--node-admin-url", "pod-0.invalid:18080=" + seed.URL, "--node-admin-url", "pod-1.invalid:18080=" + peer.URL}
		if strict {
			args = append(args, "--strict")
		}
		root.SetArgs(args)
		err := root.Execute()
		if strict {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			var rows []nodeListRow
			require.NoError(t, json.Unmarshal(out.Bytes(), &rows))
			require.Equal(t, "UNREACHABLE", rows[1].State)
			require.Contains(t, diagnostic.String(), "1")
		}
	}
}
