package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// skipFixture builds a log with one segment whose replicas answer the inspect endpoint with the
// blocks given per replica as [firstEntry, lastEntry, okFlag]. A nil perReplica means the node
// answers 404, the shape of a node that holds nothing and serves no writer for the segment.
func skipFixture(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder,
	segmentID int64, perReplica [][][3]int64,
) (*client.Client, *client.Memberlist) {
	ac, members, _ := skipFixtureWithQueries(t, cli, kb, segmentID, perReplica)
	return ac, members
}

func skipFixtureWithQueries(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder,
	segmentID int64, perReplica [][][3]int64,
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
	nodes := make([]string, 0, len(perReplica))
	members := make([]client.Member, 0, len(perReplica))
	for i := range perReplica {
		blocks := perReplica[i]
		mux := http.NewServeMux()
		mux.HandleFunc("/admin/logstore/segment/inspect", func(w http.ResponseWriter, r *http.Request) {
			*queries = append(*queries, r.URL.RawQuery)
			w.Header().Set("Content-Type", "application/json")
			if blocks == nil {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"this node holds no data for it"}`))
				return
			}
			if skipFixtureStopReason != "" {
				// A survey that did not reach the end of the segment: the node stopped at its own
				// block bound, or the chain broke, or it holds no local copy at all.
				_, _ = w.Write([]byte(fmt.Sprintf(
					`{"node_id":"n","source":"local_staged","survey":{"blocks":[],"sealed":false,`+
						`"total_blocks_known":-1,"index_usable":false,"lac":-1,"stopped_early":true,`+
						`"stop_reason":%q,"stop_offset":0}}`, skipFixtureStopReason,
				)))
				return
			}
			parts := make([]string, 0, len(blocks))
			for bi, b := range blocks {
				status := "ok"
				if b[2] == 0 {
					status = "checksum_failed"
				}
				parts = append(parts, fmt.Sprintf(
					`{"block":%d,"offset":%d,"bytes":100,"first_entry_id":%d,"last_entry_id":%d,`+
						`"records_ok":0,"last_good_entry_id":-1,"status":%q}`, bi, bi*100, b[0], b[1], status,
				))
			}
			_, _ = w.Write([]byte(fmt.Sprintf(
				`{"node_id":"n","source":"local_staged","survey":{"blocks":[%s],"sealed":true,`+
					`"total_blocks_known":%d,"index_usable":true,"lac":99,"stopped_early":false,`+
					`"stop_reason":"end_of_segment","stop_offset":0}}`, joinComma(parts), len(blocks),
			)))
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		svcAddr := fmt.Sprintf("127.0.0.1:1908%d", i)
		nodes = append(nodes, svcAddr)
		members = append(members, client.Member{
			ID: fmt.Sprintf("node-%d", i+1), ServiceAddr: svcAddr,
			Tags: map[string]string{"admin_port": extractPort(t, srv.URL)},
		})
	}

	put(kb.BuildSegmentInstanceKey(logName, strconv.FormatInt(segmentID, 10)), &proto.SegmentMetadata{
		SegNo: segmentID, State: proto.SegmentState_Completed, LastEntryId: 99,
		Quorum: &proto.QuorumInfo{
			Id: 1, Es: int32(len(nodes)), Wq: int32(len(nodes)), Aq: int32(len(nodes)/2 + 1), Nodes: nodes,
		},
	})
	return client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: 2 * time.Second}),
		&client.Memberlist{Members: members}, queries
}

// skipFixtureStopReason makes every replica answer with a survey that stopped before the end of
// the segment, which is what a node's own block bound, a broken chain, or no local copy all look
// like to the caller.
var skipFixtureStopReason string

func skipTestGlobals(t *testing.T) {
	t.Helper()
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals = GlobalFlags{Timeout: 2 * time.Second}
}

func declaredRanges(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, logID, segID int64) []*proto.SkipRange {
	t.Helper()
	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	return segmentRangesOf(rec.set, logID, segID)
}

// TestSkipRangeAdd_RefusesWhenAReplicaCanStillRead is the gate that makes this command safe to
// hand to an operator. A read is served by any one replica, so a range some replica can still
// serve is not lost data -- declaring it would throw away entries failover was already covering.
func TestSkipRangeAdd_RefusesWhenAReplicaCanStillRead(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	// node-1 cannot read 10-19; node-2 can.
	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{
		{{0, 9, 1}, {10, 19, 0}},
		{{0, 9, 1}, {10, 19, 1}},
	})
	cmd, _, errOut := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	})

	require.Error(t, err)
	require.Equal(t, 4, wperrors.ExitCodeFor(err), "a range a replica still serves is a state conflict")
	require.Contains(t, err.Error(), "node-2", "the replica that can still read it has to be named")
	require.Empty(t, declaredRanges(t, cli, kb, 7, 3), "nothing was written")
	require.NotContains(t, errOut.String(), "become unreachable",
		"the refusal comes before the warning, so an operator is not told data was given up")
}

// TestSkipRangeAdd_RefusesWhenAReplicaDidNotLookAtTheWholeRange is the gate's real premise. A
// replica reports only what it surveyed, and a survey stops at the node's own block bound -- 64 by
// default, which a full segment exceeds at about 128. Entries past that never appear as readable,
// so a gate that rejects only on what came back as readable would wave through a range on a
// perfectly healthy segment. The same shape covers a broken chain, a compacted segment served from
// object storage, and a deployment with no local copies at all.
func TestSkipRangeAdd_RefusesWhenAReplicaDidNotLookAtTheWholeRange(t *testing.T) {
	for _, stop := range []string{"bound", "chain_broken", "no_local_blocks"} {
		t.Run(stop, func(t *testing.T) {
			cli := startTestEtcd(t)
			kb := meta.NewKeyBuilder("wptest")
			skipTestGlobals(t)
			skipFixtureStopReason = stop
			t.Cleanup(func() { skipFixtureStopReason = "" })

			ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 9, 1}}, {{0, 9, 1}}})
			cmd, _, _ := markingTestCmd()

			err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
				logName: "mylog", segmentID: 3, from: 5000, to: 5099, reason: "bad disk", confirmed: true,
			})

			require.Error(t, err)
			require.Equal(t, 4, wperrors.ExitCodeFor(err))
			require.Regexp(t, `(?i)did not|could not`, err.Error(),
				"the refusal has to say the range was never looked at, not that it is readable")
			require.Empty(t, declaredRanges(t, cli, kb, 7, 3), "nothing was written")
		})
	}
}

// TestSkipRangeAdd_HungReplicaDoesNotEatTheWriteBudget covers the one deadline this command has to
// split. Asking the replicas goes over HTTP on the same budget the command was given, and that
// client takes no context, so a replica that accepts the connection and never answers spends the
// whole budget before the record is even read -- the write would then fail with a deadline error
// that has nothing to do with etcd, and raising --timeout would not help because both budgets
// scale together.
func TestSkipRangeAdd_HungReplicaDoesNotEatTheWriteBudget(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals = GlobalFlags{Timeout: time.Second}

	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1200 * time.Millisecond) // longer than the budget, so the client gives up first
	}))
	t.Cleanup(hung.Close)
	members := &client.Memberlist{Members: []client.Member{{
		ID: "node-hung", ServiceAddr: hung.Listener.Addr().String(),
		Tags: map[string]string{"admin_port": extractPort(t, hung.URL)},
	}}}
	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey("mylog"), &proto.LogMeta{LogId: 7, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	put(kb.BuildSegmentInstanceKey("mylog", "3"), &proto.SegmentMetadata{
		SegNo: 3, State: proto.SegmentState_Completed, LastEntryId: 99,
		Quorum: &proto.QuorumInfo{Id: 1, Es: 1, Wq: 1, Aq: 1, Nodes: []string{members.Members[0].ServiceAddr}},
	})
	ac := client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: time.Second})
	cmd, _, _ := markingTestCmd()

	// --force carries the decision past the refusal the silence earns, so the write is reached.
	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk",
		confirmed: true, force: true,
	})

	require.NoError(t, err, "the metadata write must not inherit what the inspection spent")
	require.Len(t, declaredRanges(t, cli, kb, 7, 3), 1)
}

// TestSkipRangeAdd_AsksForTheWholeSegment pins the request, not the judgement. A verifying survey
// takes the node's own bound of 64 blocks when none is asked for, and a full segment holds about
// 128, so a declaration that inherited that default would have most of the segment unaccounted for
// on every call -- and the refusal it triggers would fire on healthy segments instead of the ones
// it is for.
func TestSkipRangeAdd_AsksForTheWholeSegment(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members, queries := skipFixtureWithQueries(t, cli, kb, 3, [][][3]int64{{{0, 99, 0}}, {{0, 99, 0}}})
	cmd, _, _ := markingTestCmd()

	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	}))

	require.NotEmpty(t, *queries)
	for _, q := range *queries {
		require.Contains(t, q, fmt.Sprintf("max_blocks=%d", skipRangeMaxBlocks),
			"without asking, the node stops at 64 blocks and most of a segment goes unaccounted for")
	}
}

// TestSkipRangeAdd_RefusesWhenNoReplicaAnswers covers the state a rolling restart produces. All
// three replicas may hold the data intact; nothing was established, so nothing may be declared.
// `wp segment inspect` already refuses this state, and two standards in one tool is worse than
// either.
func TestSkipRangeAdd_RefusesWhenNoReplicaAnswers(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{nil, nil})
	cmd, _, _ := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	})

	require.Error(t, err)
	require.Empty(t, declaredRanges(t, cli, kb, 7, 3))
}

// TestSkipRangeAdd_RefusesBeyondWhatTheReplicasAccountFor is the typo that would otherwise abandon
// data not yet written: entries above what any replica surveyed are not established as gone, and
// once a reader honours the record every entry later written into that range becomes invisible.
func TestSkipRangeAdd_RefusesBeyondWhatTheReplicasAccountFor(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	// The replicas hold 0-99 and vouch for nothing above it.
	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 99, 0}}, {{0, 99, 0}}})
	cmd, _, _ := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 1999, reason: "typed 1999 for 199", confirmed: true,
	})

	require.Error(t, err)
	require.Empty(t, declaredRanges(t, cli, kb, 7, 3))

	// Within what they accounted for, the same declaration is accepted.
	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 99, reason: "bad disk", confirmed: true,
	}))
	require.Len(t, declaredRanges(t, cli, kb, 7, 3), 1)
}

// TestSkipRangeAdd_EmptyQuorumIsNotAgreement covers a segment whose metadata names no replica:
// nobody was consulted, which is not the same as nobody objecting.
func TestSkipRangeAdd_EmptyQuorumIsNotAgreement(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 99, 0}}})
	// Rewrite the segment metadata with no quorum at all.
	b, err := pb.Marshal(&proto.SegmentMetadata{
		SegNo: 3, State: proto.SegmentState_Completed, LastEntryId: 99,
	})
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), kb.BuildSegmentInstanceKey("mylog", "3"), string(b))
	require.NoError(t, err)
	cmd, _, _ := markingTestCmd()

	err = runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	})

	require.Error(t, err)
	require.Empty(t, declaredRanges(t, cli, kb, 7, 3))
}

// TestSkipRangeAdd_OnlyReplicasThatCanReadTheRangeAreNamed keeps the refusal actionable. Naming a
// replica whose readable entries lie outside the range sends the operator to inspect the wrong one.
func TestSkipRangeAdd_OnlyReplicasThatCanReadTheRangeAreNamed(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	// node-1 reads 0-9 and fails 10-19; node-2 reads both.
	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{
		{{0, 9, 1}, {10, 19, 0}},
		{{0, 9, 1}, {10, 19, 1}},
	})
	cmd, _, _ := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "node-2")
	require.NotContains(t, err.Error(), "node-1",
		"node-1 cannot read the range, so naming it points at the wrong replica")
}

// TestSkipRangeAdd_PreviewCountsWhatIsNewlyGivenUp covers the line an operator decides on. The
// preview is computed from the record as it stands, and the merge that produces the new record
// rewrites those same objects, so a preview taken afterwards reports nothing newly lost.
func TestSkipRangeAdd_PreviewCountsWhatIsNewlyGivenUp(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 99, 0}}, {{0, 99, 0}}})
	cmd, _, _ := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "first", confirmed: true,
	}))

	// Extending the range upward: 20-30 is newly given up.
	second, _, errOut := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(second, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 15, to: 30, reason: "wider", confirmed: true,
	}))

	s := errOut.String()
	require.Contains(t, s, "20-30", "the entries that are newly abandoned have to be named")
	require.NotContains(t, s, "nothing more becomes unreachable",
		"20-30 was not declared before, so this is not a no-op")
}

// TestSkipRangeAdd_ReasonTruncatesOnARuneBoundary covers a reason in a language whose characters
// are multi-byte. Cutting at a byte count can land mid-character, and protobuf refuses to marshal
// a string field that is not valid UTF-8, so the command would fail after printing its preview.
func TestSkipRangeAdd_ReasonTruncatesOnARuneBoundary(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 99, 0}}, {{0, 99, 0}}})
	cmd, _, _ := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19,
		reason: strings.Repeat("\u4e2d", 90), confirmed: true, // 270 bytes, 90 runes
	})

	require.NoError(t, err, "a reason over the budget is truncated, not a reason to fail the write")
	got := declaredRanges(t, cli, kb, 7, 3)
	require.Len(t, got, 1)
	require.True(t, utf8.ValidString(got[0].Reason), "the stored reason has to be valid UTF-8")
	require.LessOrEqual(t, len(got[0].Reason), meta.MaxSkipRangeReasonBytes)
}

// TestSkipRangeRemove_ByLogIdReachesADeletedLogsRanges is what makes the over-limit error's advice
// possible. Ranges outlive their log by design -- log ids are never reused, so they are inert --
// but `list` says they stay until removed and the size refusal says to remove them, so something
// has to be able to.
func TestSkipRangeRemove_ByLogIdReachesADeletedLogsRanges(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	putSegmentRanges(rec.set, 99, 1, []*proto.SkipRange{{FromEntryId: 0, ToEntryId: 5, Reason: "gone log"}})
	require.NoError(t, writeSkipRanges(context.Background(), cli, kb, rec))
	cmd, _, _ := markingTestCmd()

	require.NoError(t, runSkipRangeRemove(cmd, cli, kb, "", 99, 1, 0, 5))

	require.Empty(t, declaredRanges(t, cli, kb, 99, 1))
}

// TestSkipRangeAdd_ForceOverridesTheRefusal keeps the override explicit: an operator who has
// decided anyway can proceed, but only by saying so.
func TestSkipRangeAdd_ForceOverridesTheRefusal(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 19, 1}}, {{0, 19, 1}}})
	cmd, _, _ := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk",
		confirmed: true, force: true,
	})

	require.NoError(t, err)
	require.Len(t, declaredRanges(t, cli, kb, 7, 3), 1)
}

// TestSkipRangeAdd_UnreachableReplicaIsNotAgreement covers the incomplete view. A replica that
// could not be asked has not said the data is gone, so it must not stand in for one that did: the
// same rule as `wp instance data --strict`, an incomplete picture does not support a destructive
// decision. Warning and writing anyway was the first version of this, and it was wrong -- a silent
// replica and an examined-and-unreadable one were indistinguishable to the gate.
func TestSkipRangeAdd_UnreachableReplicaIsNotAgreement(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	// One replica cannot read the range; the other does not answer at all.
	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 9, 1}, {10, 19, 0}}, nil})
	cmd, _, errOut := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	})

	require.Error(t, err, "one replica never answered, so the range is not established as unreadable")
	require.Equal(t, 4, wperrors.ExitCodeFor(err))
	require.Contains(t, err.Error(), "node-2", "the replica that could not be asked has to be named")
	require.Empty(t, declaredRanges(t, cli, kb, 7, 3))

	// --force carries the decision, and says that it is doing so.
	forced, _, forcedErr := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(forced, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk",
		confirmed: true, force: true,
	}))
	require.Contains(t, forcedErr.String(), "--force is overriding")
	require.Len(t, declaredRanges(t, cli, kb, 7, 3), 1)
	_ = errOut
}

// TestSkipRangeAdd_WithoutConfirmationWritesNothing covers the prompt. This is the only wp
// command that makes data unreachable, so the default has to be to stop.
func TestSkipRangeAdd_WithoutConfirmationWritesNothing(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 9, 1}, {10, 19, 0}}, {{0, 9, 1}, {10, 19, 0}}})
	cmd, _, errOut := markingTestCmd()

	err := runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk",
	})

	require.Error(t, err)
	require.Equal(t, 7, wperrors.ExitCodeFor(err), "stopping at the prompt is a user abort")
	require.Empty(t, declaredRanges(t, cli, kb, 7, 3))
	require.Contains(t, errOut.String(), "10-19",
		"the entries that would be given up have to be printed before the prompt")
	require.Contains(t, errOut.String(), "become unreachable")
}

// TestSkipRangeAdd_OverlappingDeclarationsCoalesce keeps the record from growing a new entry per
// edit, and keeps both accounts of the incident.
func TestSkipRangeAdd_OverlappingDeclarationsCoalesce(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, _, _ := markingTestCmd()
	add := func(from, to int64, reason string) error {
		return runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
			logName: "mylog", segmentID: 3, from: from, to: to, reason: reason, confirmed: true,
		})
	}

	require.NoError(t, add(10, 19, "first pass"))
	require.NoError(t, add(15, 24, "second pass"))

	got := declaredRanges(t, cli, kb, 7, 3)
	require.Len(t, got, 1, "the two declarations overlap, so they are one range")
	require.EqualValues(t, 10, got[0].FromEntryId)
	require.EqualValues(t, 24, got[0].ToEntryId)
	require.Contains(t, got[0].Reason, "first pass")
	require.Contains(t, got[0].Reason, "second pass", "neither account of the incident is lost")
}

// TestSkipRangeAdd_AdjacentDeclarationsStaySeparate is the other half: two ranges that merely
// touch may be separate incidents, and merging them would lose one reason and one timestamp.
func TestSkipRangeAdd_AdjacentDeclarationsStaySeparate(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, _, _ := markingTestCmd()
	add := func(from, to int64, reason string) error {
		return runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
			logName: "mylog", segmentID: 3, from: from, to: to, reason: reason, confirmed: true,
		})
	}

	require.NoError(t, add(10, 19, "first disk"))
	require.NoError(t, add(20, 29, "second disk"))

	got := declaredRanges(t, cli, kb, 7, 3)
	require.Len(t, got, 2)
	require.Equal(t, "first disk", got[0].Reason)
	require.Equal(t, "second disk", got[1].Reason)
}

// TestSkipRangeAdd_DeclarationBelowAnExistingOneIsNotSwallowed is why the ranges are sorted
// before they are coalesced. Merging walks forward and only ever extends the last range's end, so
// an unsorted list lets a declaration that starts *below* an existing one be folded into it and
// its lower half disappear -- the operator would be told the range was declared while the record
// never covered it.
func TestSkipRangeAdd_DeclarationBelowAnExistingOneIsNotSwallowed(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, _, _ := markingTestCmd()
	add := func(from, to int64, reason string) error {
		return runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
			logName: "mylog", segmentID: 3, from: from, to: to, reason: reason, confirmed: true,
		})
	}

	require.NoError(t, add(20, 29, "found second"))
	require.NoError(t, add(10, 25, "then found it started earlier"))

	got := declaredRanges(t, cli, kb, 7, 3)
	require.Len(t, got, 1, "the two overlap, so they are one range")
	require.EqualValues(t, 10, got[0].FromEntryId, "the earlier start must not be lost")
	require.EqualValues(t, 29, got[0].ToEntryId)
}

// TestSkipRangeRemove_WithdrawingTheMiddleSplitsTheRange covers the shape a replica rebuild
// leaves: part of a declared range is readable again. The operator should not have to reproduce
// the original boundaries to say so.
func TestSkipRangeRemove_WithdrawingTheMiddleSplitsTheRange(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, out, _ := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 29, reason: "bad disk", confirmed: true,
	}))

	require.NoError(t, runSkipRangeRemove(cmd, cli, kb, "mylog", -1, 3, 15, 19))

	got := declaredRanges(t, cli, kb, 7, 3)
	require.Len(t, got, 2)
	require.EqualValues(t, 10, got[0].FromEntryId)
	require.EqualValues(t, 14, got[0].ToEntryId)
	require.EqualValues(t, 20, got[1].FromEntryId)
	require.EqualValues(t, 29, got[1].ToEntryId)
	require.Contains(t, out.String(), "try those entries again")
}

// TestSkipRangeRemove_WithdrawingEverythingClearsTheSegment keeps the record from retaining an
// empty shell, which would otherwise accumulate one per segment ever touched.
func TestSkipRangeRemove_WithdrawingEverythingClearsTheSegment(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, _, _ := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	}))

	require.NoError(t, runSkipRangeRemove(cmd, cli, kb, "mylog", -1, 3, 0, 99))

	require.Empty(t, declaredRanges(t, cli, kb, 7, 3))
	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	require.Empty(t, rec.set.GetByLogId(), "the log's entry goes too, not just the segment's")
}

// TestSkipRangeRemove_NonOverlappingWithdrawalIsNotFound stops a typo from reporting success
// while leaving the declared range in place.
func TestSkipRangeRemove_NonOverlappingWithdrawalIsNotFound(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, _, _ := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "bad disk", confirmed: true,
	}))

	err := runSkipRangeRemove(cmd, cli, kb, "mylog", -1, 3, 40, 50)

	require.Error(t, err)
	require.Equal(t, 3, wperrors.ExitCodeFor(err))
	require.Len(t, declaredRanges(t, cli, kb, 7, 3), 1, "the declared range is untouched")
}

// TestSkipRangeList_NamesItsSourceAndListsOrphans covers two things a listing has to say. The
// source matters because a host application can supply ranges of its own at runtime and those
// never reach this record. The orphan matters because a deleted log's ranges stay behind, and
// listing is how an operator finds them.
func TestSkipRangeList_NamesItsSourceAndListsOrphans(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	putSegmentRanges(rec.set, 99, 1, []*proto.SkipRange{{
		FromEntryId: 0, ToEntryId: 5, CreationTimestamp: uint64(time.Now().Unix()), Reason: "gone log",
	}})
	require.NoError(t, writeSkipRanges(context.Background(), cli, kb, rec))
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSkipRangeList(cmd, cli, kb, ""))

	s := out.String()
	require.Contains(t, s, kb.AllSkipRangesKey(), "the listing has to say which source it read")
	require.Contains(t, s, "99", "a range whose log no longer exists is still listed")
	require.Contains(t, s, "0-5")
	require.Contains(t, s, "gone log")
}

// TestSkipRangeList_NothingDeclaredSaysSo covers what an operator sees most of the time. An empty
// table would read as a command that failed to find the record rather than a record with nothing
// in it.
func TestSkipRangeList_NothingDeclaredSaysSo(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSkipRangeList(cmd, cli, kb, ""))

	require.Contains(t, out.String(), "None declared")
	require.Contains(t, out.String(), kb.AllSkipRangesKey(), "the source is named even when it is empty")
}

// TestSkipRangeList_OneLogFiltersTheRest covers naming a log: an operator looking at one incident
// should not have to read every other log's ranges, and a mistyped name has to be distinguishable
// from a log with no ranges.
func TestSkipRangeList_OneLogFiltersTheRest(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 29, 0}}, {{0, 29, 0}}})
	cmd, _, _ := markingTestCmd()
	require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
		logName: "mylog", segmentID: 3, from: 10, to: 19, reason: "mine", confirmed: true,
	}))
	// Another log's range, written straight into the record.
	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	putSegmentRanges(rec.set, 42, 0, []*proto.SkipRange{{FromEntryId: 0, ToEntryId: 3, Reason: "someone else"}})
	require.NoError(t, writeSkipRanges(context.Background(), cli, kb, rec))

	named, out, _ := markingTestCmd()
	require.NoError(t, runSkipRangeList(named, cli, kb, "mylog"))
	require.Contains(t, out.String(), "mine")
	require.NotContains(t, out.String(), "someone else", "another log's ranges are not this log's problem")

	missing, _, _ := markingTestCmd()
	err = runSkipRangeList(missing, cli, kb, "nosuchlog")
	require.Error(t, err)
	require.Equal(t, 3, wperrors.ExitCodeFor(err), "a mistyped log name is target-not-found, not an empty list")
}

// TestSkipRangeAdd_JoinedReasonsStayWithinTheBudget is the bound that keeps repeated edits from
// growing the record without limit: every overlapping declaration joins its reason onto the one
// already there.
func TestSkipRangeAdd_JoinedReasonsStayWithinTheBudget(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	ac, members := skipFixture(t, cli, kb, 3, [][][3]int64{{{0, 99, 0}}, {{0, 99, 0}}})
	cmd, _, _ := markingTestCmd()
	// Distinct reasons each time: identical ones are deduplicated on the way in, so reusing one
	// string would exercise that instead of the growth this is about.
	for i := 0; i < 12; i++ {
		require.NoError(t, runSkipRangeAdd(cmd, cli, kb, ac, members, skipRangeAddRequest{
			logName: "mylog", segmentID: 3, from: int64(i), to: int64(i + 40),
			reason: fmt.Sprintf("pass %d %s", i, strings.Repeat("x", 60)), confirmed: true,
		}))
	}

	got := declaredRanges(t, cli, kb, 7, 3)
	require.Len(t, got, 1)
	require.Contains(t, got[0].Reason, "pass 0", "the first account is still there")
	require.LessOrEqual(t, len(got[0].Reason), meta.MaxSkipRangeReasonBytes,
		"twelve overlapping declarations must not grow the reason past its budget")
}

// TestSkipRangeWrite_OversizedRecordIsRefused covers the size bound on the command's own write
// path, which carries its own copy of the check: a bound enforced in only one of the two places
// drifts the first time one of them changes.
func TestSkipRangeWrite_OversizedRecordIsRefused(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)

	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	huge := strings.Repeat("x", meta.MaxSkipRangeReasonBytes)
	for logID := int64(0); logID < 4000; logID++ {
		putSegmentRanges(rec.set, logID, 0, []*proto.SkipRange{{FromEntryId: 0, ToEntryId: 9, Reason: huge}})
	}

	err = writeSkipRanges(context.Background(), cli, kb, rec)

	require.Error(t, err)
	require.Equal(t, 4, wperrors.ExitCodeFor(err))
	require.Contains(t, err.Error(), "over the")
	require.Contains(t, err.Error(), "remove ranges that no longer apply", "the refusal says what to do")
}

// TestSkipRangeList_JSONIsThePayloadAlone covers the machine-readable path: a caller pipes it.
func TestSkipRangeList_JSONIsThePayloadAlone(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals = GlobalFlags{Timeout: 2 * time.Second, Output: "json"}

	rec, err := readSkipRanges(context.Background(), cli, kb)
	require.NoError(t, err)
	putSegmentRanges(rec.set, 7, 3, []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19, Reason: "bad disk"}})
	require.NoError(t, writeSkipRanges(context.Background(), cli, kb, rec))
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runSkipRangeList(cmd, cli, kb, ""))

	var payload struct {
		Source string `json:"source"`
		Ranges []struct {
			LogID     int64 `json:"log_id"`
			SegmentID int64 `json:"segment_id"`
			From      int64 `json:"from_entry_id"`
			To        int64 `json:"to_entry_id"`
			Entries   int64 `json:"entries"`
		} `json:"ranges"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &payload),
		"stdout has to be the payload alone, or a caller cannot pipe it anywhere")
	require.Equal(t, kb.AllSkipRangesKey(), payload.Source)
	require.Len(t, payload.Ranges, 1)
	require.EqualValues(t, 7, payload.Ranges[0].LogID)
	require.EqualValues(t, 3, payload.Ranges[0].SegmentID)
	require.EqualValues(t, 10, payload.Ranges[0].Entries, "an inclusive 10-19 is ten entries")
}

// TestSkipRangeWrite_StaleRecordIsRefused covers the single-key consequence: two operators
// editing at once must not drop each other's ranges.
func TestSkipRangeWrite_StaleRecordIsRefused(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	skipTestGlobals(t)
	ctx := context.Background()

	first, err := readSkipRanges(ctx, cli, kb)
	require.NoError(t, err)
	stale, err := readSkipRanges(ctx, cli, kb)
	require.NoError(t, err)

	putSegmentRanges(first.set, 7, 3, []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19}})
	require.NoError(t, writeSkipRanges(ctx, cli, kb, first))

	putSegmentRanges(stale.set, 8, 1, []*proto.SkipRange{{FromEntryId: 0, ToEntryId: 5}})
	err = writeSkipRanges(ctx, cli, kb, stale)

	require.Error(t, err)
	require.Equal(t, 4, wperrors.ExitCodeFor(err))
	require.Contains(t, err.Error(), "re-run")
}
