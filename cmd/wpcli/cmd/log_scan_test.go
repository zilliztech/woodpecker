package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// seg builds one segment's scan input: what metadata says, and what each replica's structure or read
// reported.
func seg(id int64, state proto.SegmentState, metaLast int64, nodes ...scanNode) scanSegment {
	return scanSegment{id: id, state: state, metaLast: metaLast, nodes: nodes}
}

func replica(label string, from, to int64) scanNode {
	n := scanNode{label: label, answered: true}
	if to >= from {
		n.coverage = n.coverage.add(entryRange{from, to})
	}
	return n
}

func silentReplica(label, why string) scanNode {
	return scanNode{label: label, state: why}
}

// TestReconcileScan_CleanLogSaysSoBriefly covers the common case. A sound log should not produce a
// wall of text, or the one line that matters somewhere else will be missed.
func TestReconcileScan_CleanLogSaysSoBriefly(t *testing.T) {
	segments := []scanSegment{
		seg(0, proto.SegmentState_Completed, 99, replica("node-1", 0, 99), replica("node-2", 0, 99)),
		seg(1, proto.SegmentState_Active, -1, replica("node-1", 0, 37), replica("node-2", 0, 37)),
	}

	rows, findings, err := reconcileScan(segments, -1, 0)

	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, scanVerdictOK, rows[0].verdict)
	require.Equal(t, scanVerdictOpen, rows[1].verdict, "an active segment has no final entry to be measured against")
	require.Len(t, findings, 1, "a clean log gets one line, not a report")
	require.Regexp(t, `(?i)reads through|no problem`, findings[0])
}

// TestReconcileScan_DataShortOfMetadata is the condition a sweep exists to find: the segment is
// finalized, so its metadata states what it holds, and the data does not go that far.
func TestReconcileScan_DataShortOfMetadata(t *testing.T) {
	segments := []scanSegment{
		seg(3, proto.SegmentState_Completed, 99, replica("node-1", 0, 87), replica("node-2", 0, 87)),
	}

	rows, findings, err := reconcileScan(segments, -1, 0)

	require.Error(t, err, "data that does not reach what metadata promises is a finding")
	require.Equal(t, scanVerdictShort, rows[0].verdict)
	require.Contains(t, joinFindings(findings), "0-87")
	require.Contains(t, joinFindings(findings), "99")
	require.Contains(t, joinFindings(findings), "wp segment inspect",
		"the sweep has to name the command that answers the next question")
}

// TestReconcileScan_OneReplicaShortIsNotTheSegmentShort covers a replica that holds less than the
// others. A read is served by any replica, so the segment is still whole -- but that replica needs
// resyncing, and saying nothing would leave it to rot.
func TestReconcileScan_OneReplicaShortIsNotTheSegmentShort(t *testing.T) {
	segments := []scanSegment{
		seg(3, proto.SegmentState_Completed, 99, replica("node-1", 0, 99), replica("node-2", 0, 87)),
	}

	rows, findings, err := reconcileScan(segments, -1, 0)

	require.NoError(t, err, "some replica holds all of it, so a reader is served")
	require.Equal(t, scanVerdictOK, rows[0].verdict)
	require.Contains(t, joinFindings(findings), "node-2")
	require.Regexp(t, `(?i)resync|behind`, joinFindings(findings))
}

// TestReconcileScan_HoleInsideASegment covers coverage that starts at 0 and reaches the end with a
// gap in between, which a per-segment last entry alone would never reveal.
func TestReconcileScan_HoleInsideASegment(t *testing.T) {
	node := scanNode{label: "node-1", answered: true}
	node.coverage = node.coverage.add(entryRange{0, 40}).add(entryRange{60, 99})
	segments := []scanSegment{seg(3, proto.SegmentState_Completed, 99, node)}

	_, findings, err := reconcileScan(segments, -1, 0)

	require.Error(t, err)
	require.Contains(t, joinFindings(findings), "41-59")
}

// TestReconcileScan_MissingSegmentIdsBelowTruncationAreExpected keeps ordinary retention from
// reading as data loss, while a hole above the truncation point still does not.
func TestReconcileScan_MissingSegmentIdsBelowTruncationAreExpected(t *testing.T) {
	segments := []scanSegment{
		seg(5, proto.SegmentState_Completed, 9, replica("node-1", 0, 9)),
		seg(7, proto.SegmentState_Completed, 9, replica("node-1", 0, 9)),
	}

	t.Run("truncated through 6", func(t *testing.T) {
		_, findings, err := reconcileScan(segments, 6, 0)
		require.NoError(t, err, "segments at or below the truncation point are expected to be gone")
		require.NotRegexp(t, `(?i)unexpectedly missing`, joinFindings(findings))
	})

	t.Run("truncated through 4", func(t *testing.T) {
		_, findings, err := reconcileScan(segments, 4, 0)
		require.Error(t, err)
		require.Contains(t, joinFindings(findings), "6")
		require.Regexp(t, `(?i)missing`, joinFindings(findings))
	})
}

// TestReconcileScan_StatesThatCannotBeRight covers metadata that contradicts itself: two segments
// being written at once, and an open segment that is not the newest.
func TestReconcileScan_StatesThatCannotBeRight(t *testing.T) {
	segments := []scanSegment{
		seg(0, proto.SegmentState_Active, -1, replica("node-1", 0, 5)),
		seg(1, proto.SegmentState_Active, -1, replica("node-1", 0, 5)),
	}

	_, findings, err := reconcileScan(segments, -1, 0)

	require.Error(t, err)
	require.Regexp(t, `(?i)more than one .*active|two .*active`, joinFindings(findings))
}

// TestReconcileScan_NoReplicaAnsweredClaimsNothing keeps an unreachable quorum from reading as an
// empty segment.
func TestReconcileScan_NoReplicaAnsweredClaimsNothing(t *testing.T) {
	segments := []scanSegment{
		seg(3, proto.SegmentState_Completed, 99,
			silentReplica("node-1", "unreachable"), silentReplica("node-2", "unreachable")),
	}

	rows, findings, err := reconcileScan(segments, -1, 0)

	require.NoError(t, err, "nothing was learned, which is not a finding about the data")
	require.Equal(t, scanVerdictUnknown, rows[0].verdict)
	require.Contains(t, joinFindings(findings), "node-1")
}

func joinFindings(findings []string) string {
	out := ""
	for _, f := range findings {
		out += f + "\n"
	}
	return out
}

// scanFixture writes a log with the given segments and stands up one stub node per quorum member.
// Each node answers the inspect endpoint from blocksFor, and records the queries it received so a
// test can check which mode was asked for.
func scanFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder,
	segs []scanFixtureSegment, replicas int,
) (*client.Client, *client.Memberlist, *[]string) {
	t.Helper()
	const logName, logID = "mylog", int64(7)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey(logName), &proto.LogMeta{LogId: logID, TruncatedSegmentId: -1, TruncatedEntryId: -1})

	queries := &[]string{}
	nodes := make([]string, 0, replicas)
	members := make([]client.Member, 0, replicas)
	for i := 0; i < replicas; i++ {
		index := i
		mux := http.NewServeMux()
		mux.HandleFunc("/admin/logstore/segment/inspect", func(w http.ResponseWriter, r *http.Request) {
			*queries = append(*queries, r.URL.RawQuery)
			segID, _ := strconv.ParseInt(r.URL.Query().Get("segment_id"), 10, 64)
			w.Header().Set("Content-Type", "application/json")
			// The server reports not_verified only for the coverage pass; a verifying walk reports
			// ok or the failure it found. A stub that ignored the mode would let raw mode pass
			// against an answer no node produces.
			verified := r.URL.Query().Get("verify") != "false"
			for _, s := range segs {
				if s.id != segID {
					continue
				}
				_, _ = w.Write([]byte(s.bodyFor(index, verified)))
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"this node holds no data for it"}`))
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

	for _, s := range segs {
		put(kb.BuildSegmentInstanceKey(logName, strconv.FormatInt(s.id, 10)), &proto.SegmentMetadata{
			SegNo: s.id, State: s.state, LastEntryId: s.metaLast,
			Quorum: &proto.QuorumInfo{Id: 1, Es: int32(replicas), Wq: int32(replicas), Aq: int32(replicas/2 + 1), Nodes: nodes},
		})
	}
	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 2 * time.Second}),
		&client.Memberlist{Members: members}, queries
}

// scanFixtureSegment describes one segment: what metadata says, and what each replica reports.
type scanFixtureSegment struct {
	id       int64
	state    proto.SegmentState
	metaLast int64
	// perReplica[i] is that replica's blocks as [firstEntry, lastEntry, okFlag].
	perReplica [][][3]int64
}

func (s scanFixtureSegment) bodyFor(replica int, verified bool) string {
	blocks := s.perReplica[0]
	if replica < len(s.perReplica) {
		blocks = s.perReplica[replica]
	}
	parts := make([]string, 0, len(blocks))
	for i, b := range blocks {
		status := "not_verified"
		if verified {
			status = "ok"
		}
		if b[2] == 0 {
			status = "checksum_failed"
		}
		parts = append(parts, fmt.Sprintf(
			`{"block":%d,"offset":%d,"bytes":100,"first_entry_id":%d,"last_entry_id":%d,`+
				`"records_ok":0,"last_good_entry_id":-1,"status":%q}`, i, i*100, b[0], b[1], status,
		))
	}
	return fmt.Sprintf(`{"node_id":"n","source":"local_staged","survey":{"blocks":[%s],`+
		`"sealed":true,"total_blocks_known":%d,"index_usable":true,"lac":%d,`+
		`"stopped_early":false,"stop_reason":"end_of_segment","stop_offset":0}}`,
		strings.Join(parts, ","), len(blocks), s.metaLast)
}

func scanTestGlobals(t *testing.T) {
	t.Helper()
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals = GlobalFlags{Timeout: 2 * time.Second}
}

// TestLogScan_QuickModeReadsNoData is the promise the sweep rests on: quick mode must ask every node
// for the pass that reads no entry data, or running it on a whole log is not affordable.
func TestLogScan_QuickModeReadsNoData(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	scanTestGlobals(t)

	ac, members, queries := scanFixture(t, cli, kb, []scanFixtureSegment{
		{
			id: 0, state: proto.SegmentState_Completed, metaLast: 9,
			perReplica: [][][3]int64{{{0, 9, 1}}},
		},
	}, 2)
	cmd, _, _ := markingTestCmd()

	require.NoError(t, runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeQuick, 0, -1))

	require.NotEmpty(t, *queries)
	for _, q := range *queries {
		require.Contains(t, q, "verify=false", "quick mode must not ask a node to read block data")
	}
}

// TestLogScan_RawModeAsksForVerification covers the other mode: it must not send verify=false, or it
// would report unverified structure as if a read had got through it.
func TestLogScan_RawModeAsksForVerification(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	scanTestGlobals(t)

	ac, members, queries := scanFixture(t, cli, kb, []scanFixtureSegment{
		{
			id: 0, state: proto.SegmentState_Completed, metaLast: 9,
			perReplica: [][][3]int64{{{0, 9, 1}}},
		},
	}, 2)
	cmd, _, _ := markingTestCmd()

	_ = runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeRaw, 0, -1)

	require.NotEmpty(t, *queries)
	for _, q := range *queries {
		require.NotContains(t, q, "verify=false")
	}
}

// TestLogScan_RawModeCountsOnlyVerifiedBlocks is the difference between the modes in one case: the
// same segment reads clean in quick mode, because the structure claims it, and short in raw mode,
// because the block does not verify.
func TestLogScan_RawModeCountsOnlyVerifiedBlocks(t *testing.T) {
	kb := meta.NewKeyBuilder("wptest")
	segs := []scanFixtureSegment{
		{
			id: 0, state: proto.SegmentState_Completed, metaLast: 19,
			perReplica: [][][3]int64{{{0, 9, 1}, {10, 19, 0}}},
		}, // second block fails its checksum
	}

	t.Run("quick trusts the structure", func(t *testing.T) {
		cli := startTestEtcd(t)
		scanTestGlobals(t)
		ac, members, _ := scanFixture(t, cli, kb, segs, 2)
		cmd, out, _ := markingTestCmd()

		require.NoError(t, runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeQuick, 0, -1),
			"quick mode checks nothing, so it cannot call this damaged")
		require.Contains(t, out.String(), "0-19")
	})

	t.Run("raw finds the block does not verify", func(t *testing.T) {
		cli := startTestEtcd(t)
		scanTestGlobals(t)
		ac, members, _ := scanFixture(t, cli, kb, segs, 2)
		cmd, out, _ := markingTestCmd()

		err := runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeRaw, 0, -1)

		require.Error(t, err)
		s := out.String()
		require.Contains(t, s, "10-19", "the entries a read cannot get are the finding")
		require.Contains(t, s, "wp segment inspect")
	})
}

// TestLogScan_NoSegmentsIsNotAnError covers a log that has never been written to.
func TestLogScan_NoSegmentsIsNotAnError(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	scanTestGlobals(t)

	ac, members, _ := scanFixture(t, cli, kb, nil, 2)
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeQuick, 0, -1))
	require.Contains(t, out.String(), "no segments")
}

// TestLogScan_SegmentRangeIsHonoured keeps a sweep of one segment from paying for the whole log.
func TestLogScan_SegmentRangeIsHonoured(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	scanTestGlobals(t)

	segs := []scanFixtureSegment{
		{id: 0, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
		{id: 1, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
		{id: 2, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
	}
	ac, members, queries := scanFixture(t, cli, kb, segs, 1)
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeQuick, 1, 1))

	for _, q := range *queries {
		require.Contains(t, q, "segment_id=1", "only the segment asked for may be visited")
	}
	require.Contains(t, out.String(), "1 segments")
}

// TestLogScan_OrphanDataInAGapIsNamed covers a segment id that metadata does not know about while a
// node still holds its data. Reporting only that the id is missing leaves the operator unable to
// tell a hole in metadata from data nobody owns — and the second needs cleaning up.
func TestLogScan_OrphanDataInAGapIsNamed(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	scanTestGlobals(t)

	// Metadata knows segments 0 and 2; the nodes also hold data for 1, which metadata lost.
	segs := []scanFixtureSegment{
		{id: 0, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
		{id: 1, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
		{id: 2, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
	}
	ac, members, _ := scanFixture(t, cli, kb, segs, 2)
	// Remove segment 1's metadata record, leaving its data on the nodes.
	_, err := cli.Delete(context.Background(), kb.BuildSegmentInstanceKey("mylog", "1"))
	require.NoError(t, err)
	cmd, out, _ := markingTestCmd()

	err = runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeQuick, 0, -1)

	require.Error(t, err)
	s := out.String()
	require.Contains(t, s, "segment id 1", "the id has to be named in the singular")
	require.Contains(t, s, "data with no metadata record",
		"data nobody owns is a different problem from a hole in metadata")
	require.NotContains(t, s, "no node holds data",
		"the nodes do hold it; the negative sentence says the opposite")
}

// TestLogScan_GapWithNoDataAnywhereSaysSo covers the other half: an id metadata lost and no node
// holds, which is a hole in the records rather than data nobody owns. The nodes answer for the id --
// with no blocks -- so the distinction rests on what the answer contains, not on whether there was
// one.
func TestLogScan_GapWithNoDataAnywhereSaysSo(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	scanTestGlobals(t)

	segs := []scanFixtureSegment{
		{id: 0, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
		// The nodes know segment 1 and answer for it holding nothing.
		{id: 1, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{}}},
		{id: 2, state: proto.SegmentState_Completed, metaLast: 9, perReplica: [][][3]int64{{{0, 9, 1}}}},
	}
	ac, members, _ := scanFixture(t, cli, kb, segs, 2)
	_, err := cli.Delete(context.Background(), kb.BuildSegmentInstanceKey("mylog", "1"))
	require.NoError(t, err)
	cmd, out, _ := markingTestCmd()

	err = runLogScan(cmd, cli, kb, ac, members, "mylog", scanModeQuick, 0, -1)

	require.Error(t, err)
	s := out.String()
	require.Contains(t, s, "segment id 1")
	require.Contains(t, s, "no node holds data for it")
	require.NotContains(t, s, "data with no metadata record",
		"nothing holds it, so there is no orphan data to clean up")
}
