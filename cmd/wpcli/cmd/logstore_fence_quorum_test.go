// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// How one stub node answers a fence request.
type fenceNodeBehaviour int

const (
	nodeFences  fenceNodeBehaviour = iota // answers 200
	nodeDown                              // admin port points at nothing
	nodeRefuses                           // answers 409 with a reason, as a node with no live segment processor does
)

// fenceFixture writes a segment whose quorum has the given shape and stands up one node per
// member, each counting the fence requests it received.
func fenceFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, es, wq, aq int32, behaviours []fenceNodeBehaviour) (*client.Client, *client.Memberlist, []*atomic.Int64) {
	t.Helper()
	const logName, logID, segID = "mylog", int64(7), int64(3)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey(logName), &proto.LogMeta{LogId: logID})

	nodes := make([]string, 0, len(behaviours))
	members := make([]client.Member, 0, len(behaviours))
	counters := make([]*atomic.Int64, 0, len(behaviours))
	for i, behaviour := range behaviours {
		counter := &atomic.Int64{}
		counters = append(counters, counter)
		refuses := behaviour == nodeRefuses
		mux := http.NewServeMux()
		mux.HandleFunc("/admin/logstore/fence", func(w http.ResponseWriter, r *http.Request) {
			counter.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if refuses {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"segment 7:3 not found"}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"fence completed"}`))
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		port := "1"
		if behaviour != nodeDown {
			port = extractPort(t, srv.URL)
		}
		svcAddr := fmt.Sprintf("127.0.0.1:1808%d", i)
		nodes = append(nodes, svcAddr)
		members = append(members, client.Member{
			ID: fmt.Sprintf("node-%d", i+1), ServiceAddr: svcAddr,
			Tags: map[string]string{"admin_port": port},
		})
	}
	put(kb.BuildSegmentInstanceKey(logName, fmt.Sprintf("%d", segID)), &proto.SegmentMetadata{
		SegNo: segID, State: proto.SegmentState_Active, LastEntryId: -1,
		Quorum: &proto.QuorumInfo{Id: 1, Es: es, Wq: wq, Aq: aq, Nodes: nodes},
	})
	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 2 * time.Second}),
		&client.Memberlist{Members: members}, counters
}

// TestFenceQuorum_FencesEnoughNodesToBreakTheAckQuorum is the arithmetic the command exists for.
// A write completes once aq replicas acknowledge, so fencing fewer than wq-aq+1 leaves enough
// acknowledging nodes for the writer to carry on. With wq=3 and aq=2 that is two of three.
func TestFenceQuorum_FencesEnoughNodesToBreakTheAckQuorum(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 2, []fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", confirmed: true,
	}))

	fenced := 0
	for _, c := range counters {
		if c.Load() > 0 {
			fenced++
		}
	}
	require.GreaterOrEqual(t, fenced, 2, "wq-aq+1 = 2 nodes must be fenced to interrupt the write")
	require.Contains(t, out.String(), "2", "the report must state how many were needed")
}

// TestFenceQuorum_SaysWhenItCouldNotReachEnough covers the outcome that must not look like
// success. Fencing fewer than required breaks nothing and corrupts nothing, but it also does not
// stop the write, so reporting it as done would leave the operator believing otherwise.
func TestFenceQuorum_SaysWhenItCouldNotReachEnough(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	// Only one of three answers; two are required.
	ac, members, _ := fenceFixture(t, cli, kb, 3, 3, 2, []fenceNodeBehaviour{nodeFences, nodeDown, nodeDown})
	cmd, out, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", confirmed: true,
	})

	require.Error(t, err, "reaching fewer nodes than the ack quorum needs is not a completed interruption")
	require.Contains(t, out.String()+err.Error(), "1", "the report must say how many it reached")
}

// TestFenceQuorum_HonoursAnExplicitNodeList lets an operator who knows which replicas are the
// problem name them, instead of taking the quorum's own order.
func TestFenceQuorum_HonoursAnExplicitNodeList(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 2, []fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})
	cmd, _, _ := markingTestCmd()

	// Name the third and second members only.
	explicit := []string{members.Members[2].ServiceAddr, members.Members[1].ServiceAddr}
	require.NoError(t, runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", nodes: explicit, confirmed: true,
	}))

	require.Zero(t, counters[0].Load(), "a node the operator did not name must be left alone")
	require.Positive(t, counters[1].Load())
	require.Positive(t, counters[2].Load())
}

// TestFenceQuorum_ReportsAQuorumMemberMissingFromTheMemberlist keeps the command from dialling a
// service address as though it were an admin one: the quorum records service addresses, and only a
// memberlist entry can supply the admin port.
func TestFenceQuorum_ReportsAQuorumMemberMissingFromTheMemberlist(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, _ := fenceFixture(t, cli, kb, 3, 3, 2, []fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})
	members.Members = members.Members[:1] // two quorum members are no longer known
	cmd, out, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", confirmed: true,
	})

	require.Error(t, err)
	require.Contains(t, out.String(), "not in memberlist")
}

// TestFenceQuorum_StatesTheCostBeforeActing covers the preview an operator decides on. Without -y
// nothing is fenced, and the count that matters -- how many nodes it takes to stop the write -- has
// to be on screen before the decision, not after it.
func TestFenceQuorum_StatesTheCostBeforeActing(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 2, []fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})
	cmd, out, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer",
	})

	require.Error(t, err, "an unconfirmed destructive operation must not report success")
	for i, c := range counters {
		require.Zero(t, c.Load(), "node %d was fenced without confirmation", i+1)
	}
	s := out.String()
	require.Contains(t, s, "2 of 3 nodes must be fenced", "the preview must state what interrupting a write costs")
	require.Contains(t, s, "stalled writer", "the reason recorded on every node must be shown back")
}

// TestFenceQuorum_RefusesANodeOutsideTheQuorum keeps a mistyped or stale node name from being
// fenced: a node that holds none of the segment's data stops no write, and silently fencing it
// would report progress the operator does not have.
func TestFenceQuorum_RefusesANodeOutsideTheQuorum(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 2, []fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})
	members.Members = append(members.Members, client.Member{
		ID: "node-9", ServiceAddr: "127.0.0.1:18099", Tags: map[string]string{"admin_port": "1"},
	})
	cmd, _, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer",
		nodes: []string{"node-9"}, confirmed: true,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not a member of this segment's quorum")
	for i, c := range counters {
		require.Zero(t, c.Load(), "node %d must be untouched when the target list is rejected", i+1)
	}
}

// TestFenceQuorum_ARefusingNodeDoesNotCount covers a node that answers but does not fence: it
// resolves the segment through its live segment processor, and one that has none refuses. Counting
// that towards the required number would report an interruption that did not happen, and dropping
// the node's own reason would leave the operator without the one fact that explains it.
func TestFenceQuorum_ARefusingNodeDoesNotCount(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	// One fences, two refuse; two are required.
	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 2,
		[]fenceNodeBehaviour{nodeFences, nodeRefuses, nodeRefuses})
	cmd, out, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", confirmed: true,
	})

	require.Error(t, err, "a node that answered without fencing has not interrupted anything")
	for i, c := range counters {
		require.Positive(t, c.Load(), "node %d was never asked", i+1)
	}
	s := out.String()
	require.Contains(t, s, "refused")
	require.Contains(t, s, "segment 7:3 not found", "the node's own reason is what explains the refusal")
}

// TestFenceQuorum_RefusesAnUnusableQuorumShape covers metadata that cannot say how many nodes must
// be fenced. Treating aq=0 as a number would produce a target derived from nothing, and reporting
// success against it would be meaningless.
func TestFenceQuorum_RefusesAnUnusableQuorumShape(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 0,
		[]fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})
	cmd, _, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", confirmed: true,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "aq=0")
	for i, c := range counters {
		require.Zero(t, c.Load(), "node %d must be untouched when the quorum shape is unusable", i+1)
	}
}

// TestFenceQuorum_JSONShortfallStillFails covers the machine-readable path: a script reads the
// counts from the payload, and the exit code has to agree with them. Rendering a payload and
// returning success would tell a caller checking only the status that the write was interrupted.
func TestFenceQuorum_JSONShortfallStillFails(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second, Output: "json"}

	ac, members, _ := fenceFixture(t, cli, kb, 3, 3, 2,
		[]fenceNodeBehaviour{nodeFences, nodeDown, nodeDown})
	cmd, out, _ := markingTestCmd()

	err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
		logName: "mylog", segmentID: 3, reason: "stalled writer", confirmed: true,
	})

	require.Error(t, err)
	var payload struct {
		Required int `json:"required"`
		Fenced   int `json:"fenced"`
		Nodes    []struct {
			State string `json:"state"`
		} `json:"nodes"`
	}
	// The preview lines precede the payload, so decode from where the object starts.
	s := out.String()
	require.NoError(t, json.Unmarshal([]byte(s[strings.Index(s, "{"):]), &payload))
	require.Equal(t, 2, payload.Required)
	require.Equal(t, 1, payload.Fenced)
	require.Len(t, payload.Nodes, 3, "every targeted node must appear, whatever became of it")
}

// TestFenceQuorum_RefusesWhatItCannotRead covers the metadata the command depends on being absent
// or unusable. Each case has to name the thing that was missing: a destructive operation that says
// only "failed" leaves the operator guessing whether they mistyped a name or lost a record.
func TestFenceQuorum_RefusesWhatItCannotRead(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	ac, members, counters := fenceFixture(t, cli, kb, 3, 3, 2,
		[]fenceNodeBehaviour{nodeFences, nodeFences, nodeFences})

	put := func(segmentID int64, m *proto.SegmentMetadata) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), kb.BuildSegmentInstanceKey("mylog", fmt.Sprintf("%d", segmentID)), string(b))
		require.NoError(t, err)
	}
	// Segment 4 predates the inline quorum; segment 5 carries one that names no nodes.
	put(4, &proto.SegmentMetadata{SegNo: 4, State: proto.SegmentState_Active, LastEntryId: -1})
	put(5, &proto.SegmentMetadata{
		SegNo: 5, State: proto.SegmentState_Active, LastEntryId: -1,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2},
	})

	// wantsNot matters as much as wants: the messages of successive guards overlap, so a case that
	// only looked for a substring would pass with its own guard removed and the next one answering.
	cases := []struct {
		name      string
		logName   string
		segmentID int64
		wants     []string
		wantsNot  string
	}{
		{"unknown log", "nosuchlog", 3, []string{"log nosuchlog", "not found at"}, "segment"},
		{"unknown segment", "mylog", 99, []string{"segment 99 of log mylog", "not found at"}, "quorum"},
		{"segment with no quorum", "mylog", 4, []string{"carries no quorum"}, "not found at"},
		{"quorum naming no nodes", "mylog", 5, []string{"lists no nodes"}, "carries no quorum"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, _, _ := markingTestCmd()
			err := runFenceQuorum(cmd, cli, kb, ac, members, fenceQuorumRequest{
				logName: tc.logName, segmentID: tc.segmentID, reason: "stalled writer", confirmed: true,
			})
			require.Error(t, err)
			for _, want := range tc.wants {
				require.Contains(t, err.Error(), want)
			}
			require.NotContains(t, err.Error(), tc.wantsNot,
				"a later guard answered, so this one is not what refused")
		})
	}
	for i, c := range counters {
		require.Zero(t, c.Load(), "node %d must be untouched when the metadata cannot be read", i+1)
	}
}

// TestAdminErrorText covers what a refusal is reported as. The node's own message is the useful
// part, but a peer answering through something that is not the admin endpoint -- a proxy error
// page, a closed connection mid-body -- must still produce a reason rather than an empty cell.
func TestAdminErrorText(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		status int
		wants  string
	}{
		{"the node's own message", `{"error":"segment 7:3 not found"}`, 409, "segment 7:3 not found"},
		{"a body that is not ours", "<html>502 Bad Gateway</html>", 502, "status 502: <html>502 Bad Gateway</html>"},
		{"no body at all", "", 503, "status 503"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wants, adminErrorText([]byte(tc.body), tc.status))
		})
	}
}
