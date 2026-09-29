package cmd

import (
	"context"
	"encoding/json"
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

// probeAnswer is what one stub node replies with.
type probeAnswer struct {
	status int
	body   string
}

// probeFixture writes a 3-node quorum and stands up one stub node per member, each answering with
// the reply given for it.
func probeFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, answers []probeAnswer) (*client.Client, *client.Memberlist) {
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
		mux.HandleFunc("/admin/logstore/segment/probe", func(w http.ResponseWriter, r *http.Request) {
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
		SegNo: segID, State: proto.SegmentState_Active, LastEntryId: -1,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: nodes},
	})
	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 2 * time.Second}),
		&client.Memberlist{Members: members}
}

// sealSegment rewrites the fixture's segment as completed with a known last entry, which is what
// lets the command tell "not written yet" from "this copy cannot serve what exists".
func sealSegment(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, lastEntry int64, nodes []string) {
	t.Helper()
	b, err := pb.Marshal(&proto.SegmentMetadata{
		SegNo: 3, State: proto.SegmentState_Completed, LastEntryId: lastEntry,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: nodes},
	})
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), kb.BuildSegmentInstanceKey("mylog", "3"), string(b))
	require.NoError(t, err)
}

// quorumNodes is the fixture's node list, in quorum order.
func quorumNodes(members *client.Memberlist) []string {
	nodes := make([]string, 0, len(members.Members))
	for _, m := range members.Members {
		nodes = append(nodes, m.ServiceAddr)
	}
	return nodes
}

func probeBody(source string, first, last int64, outcome, errText string) string {
	return probeBodyLocal(source, first, last, outcome, errText, true, false, false)
}

// probeBodyLocal is the node's answer including what it holds on disk.
func probeBodyLocal(source string, first, last int64, outcome, errText string, dataLog, compacted, deleted bool) string {
	entries := int64(0)
	if first >= 0 && last >= first {
		entries = last - first + 1
	}
	return fmt.Sprintf(`{"node_id":"n","bucket_name":"bkt","root_path":"inst",`+
		`"local":{"data_log":%t,"data_log_bytes":4096,"compacted_mark":%t,"delete_marked":%t},`+
		`"source":%q,"from_entry":0,"first_entry":%d,"last_entry":%d,`+
		`"entries_read":%d,"outcome":%q,"error":%q,"elapsed_ms":12}`,
		dataLog, compacted, deleted, source, first, last, entries, outcome, errText)
}

func probeTestGlobals(t *testing.T) {
	t.Helper()
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals = GlobalFlags{Timeout: 2 * time.Second}
}

// TestSegmentProbe_OneDamagedReplicaIsNamed is the case nothing else reports: failover hides a
// single bad replica, so a read keeps working while one copy is unreadable.
func TestSegmentProbe_OneDamagedReplicaIsNamed(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "node-2", "the damaged replica has to be named")
	require.Contains(t, s, "crc mismatch in block 7")
	require.Contains(t, s, "1 of 3 replicas that answered cannot serve the segment",
		"a replica stopping early with an error is the finding, not a footnote")
	require.NotContains(t, s, "not a damaged one",
		"a failed read is damage; the wording for a replica that is merely behind says the opposite")
}

// TestSegmentProbe_UnreadableEverywhereIsSaidPlainly covers the case a reader waits on forever: the
// same entry fails on every replica, so no failover can help.
func TestSegmentProbe_UnreadableEverywhereIsSaidPlainly(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err, "a position no replica can serve is a finding, not a clean report")
	require.Regexp(t, `(?i)no replica`, out.String()+err.Error())
	require.Contains(t, out.String()+err.Error(), "1201", "the first unreadable entry is what a reader is stuck on")
}

// TestSegmentProbe_AgreementWithoutErrorsIsNotDamage covers the healthy tail. Every replica stops at
// the same entry because that is where the data ends, and calling that damage would make every
// caught-up log look broken.
func TestSegmentProbe_AgreementWithoutErrorsIsNotDamage(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Regexp(t, `(?i)has not been written yet`, s)
	require.NotRegexp(t, `(?i)damaged`, s)
}

// TestSegmentProbe_SharedCopyIsNotThreeConfirmations covers a compacted segment: every replica reads
// the one object-storage copy, so three agreeing answers are one answer.
func TestSegmentProbe_SharedCopyIsNotThreeConfirmations(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("object_storage", 0, 4821, "end_of_file", "")},
		{http.StatusOK, probeBody("object_storage", 0, 4821, "end_of_file", "")},
		{http.StatusOK, probeBody("object_storage", 0, 4821, "end_of_file", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	require.Regexp(t, `(?i)shared|same copy|one copy`, out.String(),
		"agreement between replicas reading one copy is not independent confirmation")
}

// TestSegmentProbe_NodeThatCannotAnswerIsShownNotDropped covers a node that holds nothing for the
// segment. Dropping it would silently shrink the quorum the report claims to cover.
func TestSegmentProbe_NodeThatCannotAnswerIsShownNotDropped(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusNotFound, `{"error":"this node holds no local data for log 7 segment 3"}`},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "node-2")
	require.Contains(t, s, "no local data", "the node's own reason is what makes it actionable")
}

// TestSegmentProbe_PassesTheBoundsThrough covers the two flags that decide how much work every node
// in the quorum does.
func TestSegmentProbe_PassesTheBoundsThrough(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/logstore/segment/probe", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(probeBody("local_staged", 4000, 4100, "cap_reached", "")))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey("mylog"), &proto.LogMeta{LogId: 7})
	put(kb.BuildSegmentInstanceKey("mylog", "3"), &proto.SegmentMetadata{
		SegNo: 3, State: proto.SegmentState_Active, LastEntryId: -1,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 1, Wq: 1, Aq: 1, Nodes: []string{"127.0.0.1:18080"}},
	})
	members := &client.Memberlist{Members: []client.Member{{
		ID: "node-1", ServiceAddr: "127.0.0.1:18080",
		Tags: map[string]string{"admin_port": extractPort(t, srv.URL)},
	}}}
	ac := client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 2 * time.Second})
	cmd, _, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 4000, 100))

	require.Contains(t, gotQuery, "from_entry=4000")
	require.Contains(t, gotQuery, "max_entries=100")
	require.Contains(t, gotQuery, "log_id=7")
	require.Contains(t, gotQuery, "segment_id=3")
}

// TestSegmentProbe_ReplicaBehindIsNotCalledDamaged covers replicas holding different amounts with no
// read failing. A replica that is simply behind gets repaired by the normal path; calling it damaged
// would send an operator after a fault that is not there.
func TestSegmentProbe_ReplicaBehindIsNotCalledDamaged(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4000, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "not a damaged one", "no read failed, so nothing here is damage")
	require.Contains(t, s, "node-2 (has 4000)", "the replica that is behind has to be named")
	require.NotContains(t, s, "cannot serve the segment")
}

// TestSegmentProbe_NoReplicaAnsweredClaimsNothing covers every node being unreachable. A report that
// drew a conclusion from zero answers would be inventing one.
func TestSegmentProbe_NoReplicaAnsweredClaimsNothing(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	for i := range members.Members {
		members.Members[i].Tags["admin_port"] = "1" // every node stranded
	}
	cmd, out, _ := markingTestCmd()

	err := runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err)
	require.Contains(t, out.String(), "No replica answered")
	require.NotRegexp(t, `(?i)damaged|data ends`, out.String(),
		"nothing answered, so nothing can be concluded about the data")
}

// TestSegmentProbe_JSONFindingStillFails covers the machine-readable path: a caller reading the
// payload must not be told by the exit code that a position no replica can read is fine.
func TestSegmentProbe_JSONFindingStillFails(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second, Output: "json"}

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err)
	var payload struct {
		Findings []string `json:"findings"`
		Nodes    []struct {
			StopReason string `json:"stop_reason"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &payload), "stdout has to be the payload alone")
	require.Len(t, payload.Nodes, 3)
	require.NotEmpty(t, payload.Findings, "the reading is part of the payload, not only of the table")
}

// TestSegmentProbe_QuorumMemberMissingFromMemberlistIsReported keeps the command from dialling a
// service address as though it were an admin one, as lac and fence-quorum do.
func TestSegmentProbe_QuorumMemberMissingFromMemberlistIsReported(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	members.Members = members.Members[:1]
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))
	require.Contains(t, out.String(), "not in memberlist")
}

// TestSegmentProbe_SealedSegmentExposesAShortReplica is the reading the node cannot make. A missing
// file, a block that failed its checksum and a caught-up tail all reach it as "entry not found", so
// a replica that cannot serve what a sealed segment provably holds would otherwise be reported as
// healthy -- the exact confusion this command exists to remove.
func TestSegmentProbe_SealedSegmentExposesAShortReplica(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "end_of_file", "")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "end_of_file", "")},
	})
	sealSegment(t, cli, kb, 4821, quorumNodes(members))
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "cannot serve the segment", "the replica cannot serve data the segment provably holds")
	require.Contains(t, s, "stops after 1200, but the segment ends at 4821")
	require.NotContains(t, s, "has not been written yet",
		"a sealed segment's data was written; a replica that cannot serve it is not waiting for it")
}

// TestSegmentProbe_ReplicaHoldingNothingIsNotWaiting covers a replica with neither a data.log nor a
// compacted mark. Today's read path answers "entry not found" for that, which reads as "nothing has
// been written"; the local facts say otherwise.
func TestSegmentProbe_ReplicaHoldingNothingIsNotWaiting(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBodyLocal("none", -1, -1, "no_local_data", "", false, false, false)},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "no data here")
	require.Contains(t, s, "holds none of the segment")
	require.Contains(t, s, "none", "what the replica holds locally has to be on screen")
}

// TestSegmentProbe_DeletedLogIsNotDamage covers a replica holding nothing because the log was
// deleted there. Expected, not a fault, and sending an operator after it would waste their time.
func TestSegmentProbe_DeletedLogIsNotDamage(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBodyLocal("none", -1, -1, "no_local_data", "", false, false, true)},
		{http.StatusOK, probeBodyLocal("none", -1, -1, "no_local_data", "", false, false, true)},
		{http.StatusOK, probeBodyLocal("none", -1, -1, "no_local_data", "", false, false, true)},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0),
		"a deleted log is not a finding")
	require.Contains(t, out.String(), "log deleted here")
}

// TestSegmentProbe_SilentReplicaBlocksWholeQuorumClaims covers replicas that never answered. A
// reader can still be served by one of them, so the replicas that did answer cannot speak for the
// quorum and an exit code must not say they can.
func TestSegmentProbe_SilentReplicaBlocksWholeQuorumClaims(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "entry_not_found", "")},
	})
	members.Members[1].Tags["admin_port"] = "1" // unreachable
	members.Members[2].Tags["admin_port"] = "1" // unreachable
	cmd, out, _ := markingTestCmd()

	err := runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.NoError(t, err, "one failing replica out of a quorum only partly asked is not a dead end")
	s := out.String()
	require.Contains(t, s, "did not answer")
	require.NotContains(t, s, "waits forever",
		"a reader can still be served by a replica that was never asked")
}

// TestSegmentProbe_StuckPositionIsAfterTheFurthestReplica covers failover: a reader moves to the
// next replica, so the first entry nobody can serve is the one after the furthest any replica
// reached, not after the nearest.
func TestSegmentProbe_StuckPositionIsAfterTheFurthestReplica(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 9, "error", "crc mismatch in block 1")},
		{http.StatusOK, probeBody("local_staged", 0, 19, "error", "crc mismatch in block 2")},
		{http.StatusOK, probeBody("local_staged", 0, 14, "error", "crc mismatch in block 3")},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0)

	require.Error(t, err)
	s := out.String() + err.Error()
	require.Contains(t, s, "entry 20", "entry 10 is still served by the replica that reached 19")
	require.NotContains(t, s, "entry 10")
}

// TestSegmentProbe_StuckPositionRespectsFromEntry covers a probe that starts partway through. A
// replica that serves nothing from there is stuck at the position asked for, not at entry zero.
func TestSegmentProbe_StuckPositionRespectsFromEntry(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBodyLocal("local_staged", -1, 499, "error", "crc mismatch in block 7", true, false, false)},
		{http.StatusOK, probeBodyLocal("local_staged", -1, 499, "error", "crc mismatch in block 7", true, false, false)},
		{http.StatusOK, probeBodyLocal("local_staged", -1, 499, "error", "crc mismatch in block 7", true, false, false)},
	})
	cmd, out, _ := markingTestCmd()

	err := runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 500, 0)

	require.Error(t, err)
	s := out.String() + err.Error()
	require.Contains(t, s, "entry 500")
	require.NotContains(t, s, "entry 0:")
}

// TestSegmentProbe_SamePositionDifferentReasons covers replicas agreeing on the data but not on why
// they stopped. Calling that "a replica that is behind" would name no replica at all, because none
// of them is.
func TestSegmentProbe_SamePositionDifferentReasons(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("local_staged", 0, 99, "end_of_file", "")},
		{http.StatusOK, probeBody("local_staged", 0, 99, "entry_not_found", "")},
		{http.StatusOK, probeBody("local_staged", 0, 99, "end_of_file", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "same data, up to entry 99")
	require.NotContains(t, s, "is behind", "no replica holds less than another here")
}
