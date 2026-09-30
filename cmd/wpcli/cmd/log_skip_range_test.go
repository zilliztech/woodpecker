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
	t.Helper()
	const logName, logID = "mylog", int64(7)

	put := func(key string, m pb.Message) {
		b, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(b))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey(logName), &proto.LogMeta{LogId: logID, TruncatedSegmentId: -1, TruncatedEntryId: -1})

	nodes := make([]string, 0, len(perReplica))
	members := make([]client.Member, 0, len(perReplica))
	for i := range perReplica {
		blocks := perReplica[i]
		mux := http.NewServeMux()
		mux.HandleFunc("/admin/logstore/segment/inspect", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if blocks == nil {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"this node holds no data for it"}`))
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
		&client.Memberlist{Members: members}
}

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
// could not be asked has not said the data is gone, and the same rule as `wp instance data
// --strict` applies: an incomplete picture does not support a destructive decision silently.
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

	require.NoError(t, err, "no replica contradicted the operator, so the declaration stands")
	require.Contains(t, errOut.String(), "could not be asked",
		"but the operator has to be told the view was incomplete")
	require.Contains(t, errOut.String(), "node-2")
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

	require.NoError(t, runSkipRangeRemove(cmd, cli, kb, "mylog", 3, 15, 19))

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

	require.NoError(t, runSkipRangeRemove(cmd, cli, kb, "mylog", 3, 0, 99))

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

	err := runSkipRangeRemove(cmd, cli, kb, "mylog", 3, 40, 50)

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
