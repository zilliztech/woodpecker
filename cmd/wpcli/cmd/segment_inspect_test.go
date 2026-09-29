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

// inspectFixture stands up one stub node per quorum member, each answering with the survey given
// for it.
func inspectFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, answers []probeAnswer) (*client.Client, *client.Memberlist) {
	t.Helper()
	const logName, logID, segID = "mylog", int64(7), int64(3)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey(logName), &proto.LogMeta{LogId: logID})

	nodes := make([]string, 0, len(answers))
	members := make([]client.Member, 0, len(answers))
	for i, answer := range answers {
		reply := answer
		mux := http.NewServeMux()
		mux.HandleFunc("/admin/logstore/segment/inspect", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(reply.status)
			_, _ = w.Write([]byte(reply.body))
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
		SegNo: segID, State: proto.SegmentState_Completed, LastEntryId: 29,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: nodes},
	})
	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 2 * time.Second}),
		&client.Memberlist{Members: members}
}

// surveyBody is one node's answer: a block per status given, ten entries each.
func surveyBody(stopReason string, statuses ...string) string {
	blocks := make([]string, 0, len(statuses))
	for i, status := range statuses {
		detail := ""
		if status != "ok" {
			detail = "block CRC mismatch"
		}
		blocks = append(blocks, fmt.Sprintf(
			`{"block":%d,"offset":%d,"bytes":100,"first_entry_id":%d,"last_entry_id":%d,"status":%q,"detail":%q}`,
			i, i*100, i*10, i*10+9, status, detail))
	}
	stoppedEarly := stopReason != "end_of_segment"
	return fmt.Sprintf(`{"node_id":"n","source":"local_staged","survey":{"blocks":[%s],`+
		`"sealed":true,"total_blocks_known":%d,"lac":29,"stopped_early":%t,"stop_reason":%q}}`,
		joinComma(blocks), len(statuses), stoppedEarly, stopReason)
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

func inspectTestGlobals(t *testing.T) {
	t.Helper()
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals = GlobalFlags{Timeout: 2 * time.Second}
}

// TestSegmentInspect_DamageOnOneReplicaIsRepairable is the reading no single replica's view can
// give: the same block is bad here and good there, so the data exists and failover is serving it.
func TestSegmentInspect_DamageOnOneReplicaIsRepairable(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, surveyBody("end_of_segment", "ok", "ok", "ok")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "checksum_failed", "ok")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "ok", "ok")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0),
		"a block that is intact somewhere is not a dead end")

	s := out.String()
	require.Contains(t, s, "Block 1 is damaged on node-2")
	require.Contains(t, s, "intact on node-1, node-3")
	require.Contains(t, s, "the data exists")
}

// TestSegmentInspect_BlockBadEverywhereIsALoss covers the case only a skip range can get past, and
// names the entries it costs.
func TestSegmentInspect_BlockBadEverywhereIsALoss(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, surveyBody("end_of_segment", "ok", "checksum_failed", "ok")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "checksum_failed", "ok")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "checksum_failed", "ok")},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err)
	s := out.String() + err.Error()
	require.Contains(t, s, "No replica holds a readable copy of block(s) 1")
	require.Contains(t, s, "entries 10-19", "the cost of skipping has to be stated in entries")
	require.Contains(t, s, "Readable data resumes at entry 20 (block 2)",
		"where reading resumes is what a skip range has to cover")
}

// TestSegmentInspect_BrokenChainIsNotDamageBeyondIt is the boundary of what a survey can see. An
// active segment has no index, so a block whose header cannot be read ends the walk -- and the
// blocks past it have not been looked at.
func TestSegmentInspect_BrokenChainIsNotDamageBeyondIt(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, surveyBody("chain_broken", "ok", "header_missing")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "ok", "ok")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "ok", "ok")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "could not walk past block 1")
	require.Contains(t, s, "have not been looked at",
		"a survey that could not continue must not report the rest as damaged")
}

// TestSegmentInspect_BoundIsReportedAsTheBound keeps a survey that ran out of budget from reading as
// a segment that ran out of blocks.
func TestSegmentInspect_BoundIsReportedAsTheBound(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, surveyBody("bound", "ok", "ok")},
		{http.StatusOK, surveyBody("bound", "ok", "ok")},
		{http.StatusOK, surveyBody("bound", "ok", "ok")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))
	require.Contains(t, out.String(), "stopped at the block limit")
}

// TestSegmentInspect_SilentReplicaBlocksWholeQuorumClaims covers replicas that never answered: the
// ones that did cannot speak for the quorum.
func TestSegmentInspect_SilentReplicaBlocksWholeQuorumClaims(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, surveyBody("end_of_segment", "ok", "checksum_failed")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "ok")},
		{http.StatusOK, surveyBody("end_of_segment", "ok", "ok")},
	})
	members.Members[1].Tags["admin_port"] = "1"
	members.Members[2].Tags["admin_port"] = "1"
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	s := out.String() + errText(err)
	require.Contains(t, s, "did not answer")
	require.NotContains(t, s, "only skipping them gets a reader past",
		"two replicas were never asked, so nothing is known to be unreadable everywhere")
}
