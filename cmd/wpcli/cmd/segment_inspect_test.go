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
	require.Contains(t, s, "node-2 lost 10-19",
		"a reader drops a block that fails its checksum whole, so the whole block's entries are lost there")
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
	require.Contains(t, s, "No replica can read entries 10-19",
		"every backend drops a block that fails its checksum whole, so the skip has to cover all of it")
	require.Contains(t, s, "entries 10-15 are still intact at record level",
		"what a repair could salvage is worth knowing, and is not what a reader can serve")
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
	require.Contains(t, s, "node-1 lost 10-19",
		"the entries node-1 lost are what matters, not its block numbers")
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

// blocksBodyFull is blocksBody with the footer facts a sealed replica carries: the quorum-confirmed
// tail, and whether its index could be read in full.
func blocksBodyFull(sealed bool, lac int64, indexUsable bool, stopReason string, stopOffset int64, blocks ...[4]int64) string {
	parts := make([]string, 0, len(blocks))
	for i, blk := range blocks {
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
		`"sealed":%t,"total_blocks_known":%d,"index_usable":%t,"lac":%d,`+
		`"stopped_early":%t,"stop_reason":%q,"stop_offset":%d}}`,
		joinComma(parts), sealed, len(blocks), indexUsable, lac,
		stopReason != "end_of_segment", stopReason, stopOffset)
}

// TestSegmentInspect_LaggingSealedReplicaConfirmsTheLoss covers a finalized replica that holds less
// than the quorum confirmed. Its footer carries the LAC the majority acknowledged, so the entries it
// does not have are known to exist and known to be absent there -- not "unexamined". Treating them
// as unexamined shrinks the intersection and the loss disappears from the report entirely.
func TestSegmentInspect_LaggingSealedReplicaConfirmsTheLoss(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, -1, 0})},
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, -1, 0})},
		// node-3 finalized behind the quorum: its blocks stop at entry 9, its footer says 19.
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1})},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err, "entries 10-19 exist and no replica can read them")
	s := out.String() + err.Error()
	require.Contains(t, s, "10-19")
	require.Regexp(t, `(?i)no replica can read`, s)
}

// TestSegmentInspect_ReplicaWithNoLocalBlocksStillCounts covers a replica whose local data is gone.
// It answered, and its footer says what the segment holds, so it has told us it has none of it --
// which must not weaken the conclusion the other replicas support.
func TestSegmentInspect_ReplicaWithNoLocalBlocksStillCounts(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, -1, 0})},
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, -1, 0})},
		{http.StatusOK, `{"node_id":"n","source":"none","survey":{"blocks":[],"sealed":true,` +
			`"total_blocks_known":2,"index_usable":true,"lac":19,"stopped_early":true,"stop_reason":"no_local_blocks","stop_offset":0}}`},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err, "a replica holding nothing does not make the other two inconclusive")
	s := out.String() + err.Error()
	require.Contains(t, s, "10-19")
}

// TestSegmentInspect_SealedButIndexUnreadableIsNotAnActiveSegment covers the wording for a sealed
// replica whose index is damaged. Calling it a segment with no index describes an active one, and an
// operator would wait for writes to finish instead of resyncing the replica.
func TestSegmentInspect_SealedButIndexUnreadableIsNotAnActiveSegment(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, blocksBodyFull(true, 19, false, "chain_broken", 2110, [4]int64{0, 9, 9, 1})},
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 19, 1})},
		{http.StatusOK, blocksBodyFull(true, 19, true, "end_of_segment", 0, [4]int64{0, 9, 9, 1}, [4]int64{10, 19, 19, 1})},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "index could not be read in full")
	require.NotContains(t, s, "has no index",
		"a sealed replica with a damaged trailer is not an active segment")
}

// TestSegmentInspect_BoundedSurveyDoesNotConfirmAbsence covers a sealed replica that stopped at the
// block limit. Its footer says what the segment holds, but it did not finish looking, so the entries
// beyond its horizon are unexamined -- not missing. Claiming otherwise would turn a small
// --max-blocks into a report of data loss.
func TestSegmentInspect_BoundedSurveyDoesNotConfirmAbsence(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, blocksBodyFull(true, 19, true, "bound", 0, [4]int64{0, 9, 9, 1})},
		{http.StatusOK, blocksBodyFull(true, 19, true, "bound", 0, [4]int64{0, 9, 9, 1})},
		{http.StatusOK, blocksBodyFull(true, 19, true, "bound", 0, [4]int64{0, 9, 9, 1})},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0),
		"nobody looked past the limit, so nothing is known to be missing")
	s := out.String()
	require.NotContains(t, s, "needs resyncing")
	require.NotContains(t, s, "No replica can read")
	require.Contains(t, s, "stopped at the block limit")
}

// TestSegmentInspect_IncompleteTailIsNotALoss covers the block whose header is written and whose data
// is not. Those entries are not written yet; counting them as data the replica holds and cannot read
// would report a writer in flight as damage.
func TestSegmentInspect_IncompleteTailIsNotALoss(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	tail := `{"node_id":"n","source":"local_staged","survey":{"blocks":[` +
		`{"block":0,"offset":25,"bytes":100,"first_entry_id":0,"last_entry_id":9,"records_ok":10,"last_good_entry_id":9,"status":"ok"},` +
		`{"block":1,"offset":200,"bytes":100,"first_entry_id":10,"last_entry_id":19,"records_ok":0,"last_good_entry_id":-1,` +
		`"status":"data_incomplete","detail":"header promises 100 bytes of data, the file ends 100 short"}],` +
		`"sealed":false,"total_blocks_known":-1,"index_usable":false,"lac":-1,` +
		`"stopped_early":false,"stop_reason":"end_of_segment","stop_offset":0}}`

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, tail}, {http.StatusOK, tail}, {http.StatusOK, tail},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 0, 0),
		"a header whose data has not landed yet is not damage")
	s := out.String()
	require.NotContains(t, s, "No replica can read")
	require.NotContains(t, s, "lost 10-19")
}

// TestSegmentInspect_FromBlockPastTheEndIsNotALoss covers a page that starts past a replica's last
// block. The server returns no blocks for it, which says nothing about what the replica holds --
// only that this page is empty there. Deriving absence from a replica's footer needs a survey that
// started at the beginning, or a healthy segment reads as total loss the moment an operator pages
// forward.
func TestSegmentInspect_FromBlockPastTheEndIsNotALoss(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	// A healthy sealed segment: every replica holds three blocks and confirms LAC 99.
	empty := `{"node_id":"n","source":"local_staged","survey":{"blocks":[],"sealed":true,` +
		`"total_blocks_known":3,"index_usable":true,"lac":99,` +
		`"stopped_early":false,"stop_reason":"end_of_segment","stop_offset":0}}`
	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, empty}, {http.StatusOK, empty}, {http.StatusOK, empty},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 5, 0)

	require.NoError(t, err, "paging past the last block is not data loss")
	s := out.String()
	require.NotContains(t, s, "No replica can read")
	require.NotContains(t, s, "needs resyncing")
}

// TestSegmentInspect_PagedSurveyDoesNotBlameAShorterReplica covers the case the report's own advice
// leads to: replicas hold different numbers of blocks, so paging forward empties one of them while
// the others still have data on that page.
func TestSegmentInspect_PagedSurveyDoesNotBlameAShorterReplica(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	inspectTestGlobals(t)

	ac, members := inspectFixture(t, cli, kb, []probeAnswer{
		// node-1 has nothing on this page; the others still do, and one of their blocks is damaged.
		{http.StatusOK, `{"node_id":"n","source":"local_staged","survey":{"blocks":[],"sealed":true,` +
			`"total_blocks_known":3,"index_usable":true,"lac":99,` +
			`"stopped_early":false,"stop_reason":"end_of_segment","stop_offset":0}}`},
		{http.StatusOK, blocksBodyFull(true, 99, true, "end_of_segment", 0, [4]int64{50, 59, -1, 0})},
		{http.StatusOK, blocksBodyFull(true, 99, true, "end_of_segment", 0, [4]int64{50, 59, 59, 1})},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentInspect(cmd, cli, kb, ac, members, "mylog", 3, 5, 0)

	require.NoError(t, err)
	s := out.String()
	require.NotContains(t, s, "needs resyncing", "an empty page is not a missing replica")
	require.NotContains(t, s, "No replica can read",
		"one replica was not asked about these entries at all on this page")
}
