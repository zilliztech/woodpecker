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
		lastGood := int64(i*10 + 9)
		if status != "ok" {
			lastGood = int64(i*10 + 5) // six records survived inside the damaged block
		}
		blocks = append(blocks, fmt.Sprintf(
			`{"block":%d,"offset":%d,"bytes":100,"first_entry_id":%d,"last_entry_id":%d,`+
				`"records_ok":6,"last_good_entry_id":%d,"status":%q,"detail":%q}`,
			i, i*100, i*10, i*10+9, lastGood, status, detail))
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
	require.Contains(t, s, "node-2 lost 16-19",
		"replicas are compared by the entries they hold, not by block numbers that do not line up")
	require.Contains(t, s, "readable on another replica")
	require.Contains(t, s, "the data exists")
	require.NotContains(t, s, "Block 1", "a block number means nothing across replicas")
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
	require.Contains(t, s, "No replica can read entries 16-19",
		"the block's own records survive its checksum, so the loss is narrower than the block")
	require.Contains(t, s, "every replica looked",
		"the claim is only allowed because every replica examined those entries")
	require.Contains(t, s, "Readable data resumes at entry 20",
		"where reading resumes is what a skip range has to cover")
}

// TestSegmentInspect_BrokenChainIsNotDamageBeyondIt is the boundary of what a survey can see. An
// active segment has no index, so a block whose header cannot be read ends the walk -- and the
// blocks past it have not been looked at.
func TestSegmentInspect_BrokenChainIsNotDamageBeyondIt(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	// The server reports where it gave up and does not include a block it could not read.
	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, blocksBody("chain_broken", 2110, [4]int64{0, 9, 9, 1})},
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 19, 1})},
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 19, 1})},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "could not walk past offset 2110")
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

// blocksBody builds one node's survey from explicit block descriptions, so replicas can be given
// different block boundaries -- which is the normal case, since each node decides its own.
func blocksBody(stopReason string, stopOffset int64, blocks ...[4]int64) string {
	parts := make([]string, 0, len(blocks))
	for i, blk := range blocks {
		// [first entry, last entry, last good entry (-1 for none), ok flag]
		status, detail := "ok", ""
		if blk[3] == 0 {
			status, detail = "checksum_failed", "block CRC mismatch"
		}
		parts = append(parts, fmt.Sprintf(
			`{"block":%d,"offset":%d,"bytes":100,"first_entry_id":%d,"last_entry_id":%d,`+
				`"records_ok":0,"last_good_entry_id":%d,"status":%q,"detail":%q}`,
			i, i*100, blk[0], blk[1], blk[2], status, detail))
	}
	return fmt.Sprintf(`{"node_id":"n","source":"local_staged","survey":{"blocks":[%s],`+
		`"sealed":true,"total_blocks_known":%d,"lac":19,"stopped_early":%t,"stop_reason":%q,"stop_offset":%d}}`,
		joinComma(parts), len(blocks), stopReason != "end_of_segment", stopReason, stopOffset)
}

// TestSegmentInspect_ReplicasPackEntriesIntoDifferentBlocks covers the normal case that block
// numbers do not line up: each node flushes on its own timer and size, so the same entries land in
// different blocks. Comparing replicas by block number would call entries lost that another replica
// holds, and call them safe when they are not.
func TestSegmentInspect_ReplicasPackEntriesIntoDifferentBlocks(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		// node-1 packs 0-9 and 10-19; its second block is damaged from entry 12 on.
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 11, 0})},
		// node-2 packs the same entries in three blocks, all intact.
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 4, 4, 1}, [4]int64{5, 14, 14, 1}, [4]int64{15, 19, 19, 1})},
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 4, 4, 1}, [4]int64{5, 14, 14, 1}, [4]int64{15, 19, 19, 1})},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.NoError(t, err, "every entry is readable on some replica")
	s := out.String()
	require.Contains(t, s, "12-19", "the entries node-1 lost are what matters, not its block numbers")
	require.NotContains(t, s, "cannot be read anywhere")
}

// TestSegmentInspect_UnexaminedIsNotUnreadable covers a replica that stopped before reaching the
// entries in question. Counting its silence as evidence turns "nobody looked" into "nobody can read
// it" -- the same mistake one level up from the block walk.
func TestSegmentInspect_UnexaminedIsNotUnreadable(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		// node-1 walked the whole segment and lost entries 10-19.
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, -1, 0})},
		// node-2 and node-3 could not walk past their first block, so they never saw 10-19.
		{http.StatusOK, blocksBody("chain_broken", 512, [4]int64{0, 9, 9, 1})},
		{http.StatusOK, blocksBody("chain_broken", 512, [4]int64{0, 9, 9, 1})},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.NoError(t, err, "two replicas never looked at those entries, so nothing is known to be lost")
	s := out.String()
	require.NotContains(t, s, "cannot be read anywhere")
	require.Contains(t, s, "have not been looked at on that replica",
		"the report has to say the other replicas never got that far")
}

// TestSegmentInspect_ChainBreakNamesWhereItStopped covers the server's current answer shape: it
// reports the offset it gave up at and does not include a block it could not read.
func TestSegmentInspect_ChainBreakNamesWhereItStopped(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, blocksBody("chain_broken", 2110, [4]int64{0, 9, 9, 1})},
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 19, 1})},
		{http.StatusOK, blocksBody("end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 19, 1})},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "offset 2110", "where the walk gave up is the actionable part")
	require.NotContains(t, s, "block -1", "there is no block to name when none could be read there")
}
