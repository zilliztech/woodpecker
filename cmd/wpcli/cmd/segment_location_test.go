package cmd

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/proto"
)

func TestSegmentExplicitLocation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "bucket with space", r.URL.Query().Get("bucket_name"))
		require.Equal(t, "tenant/root", r.URL.Query().Get("root_path"))
		if r.URL.Path == "/admin/logstore/segment/probe" {
			fmt.Fprint(w, `{"source":"none","outcome":"no_local_data","first_entry":-1,"last_entry":-1}`)
		} else {
			fmt.Fprint(w, `{"survey":{"blocks":[],"stop_reason":"no_local_blocks"}}`)
		}
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()
	members := &client.Memberlist{Members: []client.Member{{ID: "node", ServiceAddr: addr, Tags: map[string]string{"admin_port": extractPort(t, srv.URL)}}}}
	ac := client.New(srv.URL, client.ClientOpts{Timeout: 2 * time.Second})
	quorum := &proto.QuorumInfo{Nodes: []string{addr}}
	location := scanLocation{"bucket with space", "tenant/root"}
	inspected := inspectEachNode(ac, members, quorum, 1, 1, 0, 64, location)
	require.Len(t, inspected, 1)
	require.Equal(t, posOK, inspected[0].State)
	probed := probeEachNode(ac, members, quorum, 1, 1, 0, 64, location)
	require.Len(t, probed, 1)
	require.Equal(t, posOK, probed[0].State)
	for _, command := range [][]string{{"log", "scan", "log"}, {"segment", "inspect", "log", "1"}, {"segment", "probe", "log", "1"}} {
		for _, flag := range []string{"--bucket", "--root-path"} {
			root := NewRootCommand()
			args := append([]string{}, command...)
			root.SetArgs(append(args, flag, "value"))
			require.ErrorContains(t, root.Execute(), "must be provided together")
		}
	}
}

func TestSegmentProbeEmptyActiveIsNotPermanentStall(t *testing.T) {
	results := []probeNode{{Node: "one", State: "ok", Outcome: probeOutcomeNoLocalData, FirstEntry: -1, LastEntry: -1}, {Node: "two", State: "ok", Outcome: probeOutcomeNoLocalData, FirstEntry: -1, LastEntry: -1}}
	findings, _, err := readProbeFindings(results, &proto.SegmentMetadata{State: proto.SegmentState_Active, LastEntryId: -1})
	require.NoError(t, err)
	require.Contains(t, strings.Join(findings, " "), "final entry boundary is unknown")
	require.NotContains(t, strings.Join(findings, " "), "waits forever")
	_, _, err = readProbeFindings(results, &proto.SegmentMetadata{State: proto.SegmentState_Completed, LastEntryId: 9})
	require.Error(t, err)
}
