package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// lacFixture writes the metadata a segment's quorum view is derived from, stands up one stub
// node per durable position, and returns the pieces the command core needs.
func lacFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, positions []int64) (*client.Client, *client.Memberlist) {
	t.Helper()
	const logName, logID, segID = "mylog", int64(7), int64(3)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey(logName), &proto.LogMeta{LogId: logID})

	nodes := make([]string, 0, len(positions))
	members := make([]client.Member, 0, len(positions))
	for i, pos := range positions {
		body := fmt.Sprintf(`{"segments":[{"log_id":%d,"segment_id":%d,"backend":"stagedstorage",`+
			`"writable":true,"last_entry_id":%d,"first_entry_id":0}]}`, logID, segID, pos)
		mux := http.NewServeMux()
		mux.HandleFunc("/admin/logstore/segments", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(body))
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		svcAddr := fmt.Sprintf("127.0.0.1:1808%d", i)
		nodes = append(nodes, svcAddr)
		members = append(members, client.Member{
			ID:          fmt.Sprintf("node-%d", i+1),
			ServiceAddr: svcAddr,
			GossipAddr:  fmt.Sprintf("127.0.0.1:1794%d", i),
			Tags:        map[string]string{"admin_port": extractPort(t, srv.URL)},
		})
	}
	// Production stores the quorum inline on the segment and never writes the quorums/ keyspace:
	// storeNewSegmentMeta sets only Quorum, quorumId is deprecated, and StoreQuorumInfo has no
	// production caller. A fixture that writes a separate record describes a layout that does
	// not exist.
	put(kb.BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segID)), &proto.SegmentMetadata{
		SegNo: segID, State: proto.SegmentState_Active, LastEntryId: -1,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: nodes},
	})

	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 3 * time.Second}),
		&client.Memberlist{Members: members}
}

// TestSegmentLAC_QuorumValueIsTheAqthHighest pins the arithmetic the whole command exists for.
// A position is confirmed readable once Aq nodes have durably reached it, so with Aq=2 and
// positions 4900, 4821, 4650 the answer is 4821. The three are deliberately distinct: with a
// tie at the top, returning the maximum would produce the same number and the test would pass
// without exercising the rule.
func TestSegmentLAC_QuorumValueIsTheAqthHighest(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 3 * time.Second}

	ac, members := lacFixture(t, cli, kb, []int64{4900, 4821, 4650})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentLAC(cmd, cli, kb, ac, members, "mylog", 3))

	s := out.String()
	require.Contains(t, s, "4900", "each node's durable position must be shown")
	require.Contains(t, s, "4650", "a lagging replica is the finding, not noise")
	require.Regexp(t, `(?i)quorum lac[^0-9]*4821`, s,
		"the quorum value is the aq-th highest, not the max (4900) or the min (4650)")
}

// TestSegmentLAC_BelowAqReachableRefusesAValue covers the case where too few nodes answered to
// compute anything: with Aq=2 and one node reachable, no position can be called confirmed, and
// reporting the one answer as the quorum value would overstate what is readable.
func TestSegmentLAC_BelowAqReachableRefusesAValue(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members := lacFixture(t, cli, kb, []int64{4821, 4821, 4650})
	// Strand two of the three: their admin port now points at a closed port.
	members.Members[1].Tags["admin_port"] = "1"
	members.Members[2].Tags["admin_port"] = "1"
	cmd, out, _ := markingTestCmd()

	err := runSegmentLAC(cmd, cli, kb, ac, members, "mylog", 3)

	s := out.String()
	require.NotRegexp(t, `(?i)quorum lac[^0-9]*4821`, s,
		"one node's position is not a quorum value")
	require.Contains(t, s+errText(err), "unreachable", "the missing nodes must be named")
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sealedFixture describes a segment past Active whose end is recorded in metadata, with nodes
// that answer but hold no writer for it -- what compaction, a processor close or idle eviction
// all leave behind.
func sealedFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, lastEntryID int64) (*client.Client, *client.Memberlist) {
	t.Helper()
	const logName, logID, segID = "mylog", int64(7), int64(3)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey(logName), &proto.LogMeta{LogId: logID})

	nodes := make([]string, 0, 3)
	members := make([]client.Member, 0, 3)
	for i := 0; i < 3; i++ {
		mux := http.NewServeMux()
		// 200 with the segment absent: the node is healthy, it just has no writer for it.
		mux.HandleFunc("/admin/logstore/segments", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"segments":[]}`))
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		svcAddr := fmt.Sprintf("127.0.0.1:1808%d", i)
		nodes = append(nodes, svcAddr)
		members = append(members, client.Member{
			ID: fmt.Sprintf("node-%d", i+1), ServiceAddr: svcAddr,
			Tags: map[string]string{"admin_port": extractPort(t, srv.URL)},
		})
	}
	put(kb.BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segID)), &proto.SegmentMetadata{
		SegNo: segID, State: proto.SegmentState_Sealed, LastEntryId: lastEntryID,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: nodes},
	})
	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 3 * time.Second}),
		&client.Memberlist{Members: members}
}

// TestSegmentLAC_SealedSegmentUsesMetadata covers a finished segment. Every node answers and
// none holds a writer, which is the normal state after compaction or idle eviction. Treating
// that as nobody answering would report a healthy segment as a network fault.
func TestSegmentLAC_SealedSegmentUsesMetadata(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 3 * time.Second}

	ac, members := sealedFixture(t, cli, kb, 7310)
	cmd, out, errOut := markingTestCmd()

	require.NoError(t, runSegmentLAC(cmd, cli, kb, ac, members, "mylog", 3),
		"a sealed segment whose nodes all answered is not a network failure")

	s := out.String()
	require.Contains(t, s, "7310", "a sealed segment's end comes from metadata")
	require.Contains(t, s, "no writer", "the nodes must be shown as having no writer, not unreachable")
	// The partial-view warning goes to stderr, so asserting only on stdout would pass even with
	// these nodes counted as unreachable -- the table would still read "no writer".
	require.Empty(t, errOut.String(),
		"nodes that answered without a writer are not a partial view")
}

// TestSegmentLAC_JSONExitsNonZeroWhenUnresolved pins the exit code in json mode. A script
// reading it must not take a payload with no quorum_lac for a successful query.
func TestSegmentLAC_JSONExitsNonZeroWhenUnresolved(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second, Output: "json"}

	ac, members := lacFixture(t, cli, kb, []int64{4900, 4821, 4650})
	members.Members[1].Tags["admin_port"] = "1"
	members.Members[2].Tags["admin_port"] = "1"
	cmd, out, _ := markingTestCmd()

	err := runSegmentLAC(cmd, cli, kb, ac, members, "mylog", 3)

	require.Error(t, err, "json mode must fail the same way text mode does")
	require.NotContains(t, out.String(), "quorum_lac",
		"an unresolved value is left out rather than guessed")
}
