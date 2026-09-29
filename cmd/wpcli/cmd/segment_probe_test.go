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

func probeBody(source string, first, last int64, stop, errText string) string {
	return fmt.Sprintf(`{"node_id":"n","source":%q,"from_entry":0,"first_entry":%d,"last_entry":%d,`+
		`"entries_read":%d,"stop_reason":%q,"error":%q,"elapsed_ms":12}`,
		source, first, last, last-first+1, stop, errText)
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
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
		{http.StatusOK, probeBody("local_staged", 0, 1200, "error", "crc mismatch in block 7")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Contains(t, s, "node-2", "the damaged replica has to be named")
	require.Contains(t, s, "crc mismatch in block 7")
	require.Contains(t, s, "1 of 3 replicas are damaged",
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
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
	})
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSegmentProbe(cmd, cli, kb, ac, members, "mylog", 3, 0, 0))

	s := out.String()
	require.Regexp(t, `(?i)not been written|data ends|nothing is wrong`, s)
	require.NotRegexp(t, `(?i)damaged`, s)
}

// TestSegmentProbe_SharedCopyIsNotThreeConfirmations covers a compacted segment: every replica reads
// the one object-storage copy, so three agreeing answers are one answer.
func TestSegmentProbe_SharedCopyIsNotThreeConfirmations(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	probeTestGlobals(t)

	ac, members := probeFixture(t, cli, kb, []probeAnswer{
		{http.StatusOK, probeBody("object_storage", 0, 4821, "end_of_segment", "")},
		{http.StatusOK, probeBody("object_storage", 0, 4821, "end_of_segment", "")},
		{http.StatusOK, probeBody("object_storage", 0, 4821, "end_of_segment", "")},
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
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
		{http.StatusNotFound, `{"error":"this node holds no local data for log 7 segment 3"}`},
		{http.StatusOK, probeBody("local_staged", 0, 4821, "not_yet_written", "")},
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
