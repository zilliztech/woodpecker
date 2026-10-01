package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/proto"
)

// TestSkipSpanFor_ScansWhateverOrderTheRangesCameIn pins the one thing the reader's lookup may not
// depend on. The write path sorts and coalesces, but a record written by an older command, or edited
// by hand, may arrive unsorted or overlapping -- and a reader that answered wrongly for such a
// record would skip the wrong entries rather than merely take an extra hop.
func TestSkipSpanFor_ScansWhateverOrderTheRangesCameIn(t *testing.T) {
	ranges := config.LogSkipRanges{3: []config.SkipSpan{
		{FromEntryID: 40, ToEntryID: 49},
		{FromEntryID: 10, ToEntryID: 19},
		{FromEntryID: 15, ToEntryID: 25}, // overlaps the one before it
	}}

	for _, tc := range []struct {
		entry int64
		found bool
		to    int64
	}{
		{entry: 9, found: false},
		{entry: 10, found: true, to: 19},
		{entry: 22, found: true, to: 25},
		{entry: 30, found: false},
		{entry: 45, found: true, to: 49},
		{entry: 50, found: false},
	} {
		span, found := skipSpanFor(ranges, 3, tc.entry)
		require.Equal(t, tc.found, found, "entry %d", tc.entry)
		if tc.found {
			require.Equal(t, tc.to, span.ToEntryID, "entry %d", tc.entry)
		}
	}

	_, found := skipSpanFor(ranges, 4, 10)
	require.False(t, found, "another segment's entry 10 is not this segment's")
	_, found = skipSpanFor(nil, 3, 10)
	require.False(t, found, "no ranges declared at all")
}

// TestSkipRangesOf_ConvertsTheRecordAndNothingElse covers the boundary between the stored record
// and the shape the reader consults, including every nil level: the record is absent on almost
// every cluster, so the conversion has to answer nil rather than panic.
func TestSkipRangesOf_ConvertsTheRecordAndNothingElse(t *testing.T) {
	require.Nil(t, skipRangesOf(nil), "no record")
	require.Nil(t, skipRangesOf(&proto.LogSkipRanges{}), "a record with no segments")

	held := skipRangesOf(&proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{
		3: {Ranges: []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19, Reason: "bad disk"}}},
		4: {},
	}})
	require.Len(t, held, 2)
	require.Equal(t, []config.SkipSpan{{FromEntryID: 10, ToEntryID: 19}}, held[3])
	require.Empty(t, held[4], "a segment listed with no ranges declares nothing")
}

// TestReaderSkipsPastADeclaredRange covers the jump itself: the position lands after the range, and
// the cached batch is dropped.
//
// Dropping the batch is not housekeeping. LastReadState caches a physical location -- block,
// offset, node -- and the test that decides whether to reuse it compares only the segment id, so a
// jump within one segment would otherwise resume from a stale block offset. That is why this case
// is a jump inside a segment rather than across one: a cross-segment jump invalidates the cache by
// itself and would pass either way.
func TestReaderSkipsPastADeclaredRange(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("skip-log").Maybe()
	logHandle.On("GetId").Return(int64(88)).Maybe()

	r, err := NewLogBatchReader(context.Background(), logHandle, nil,
		&LogMessageId{SegmentId: 3, EntryId: 10}, "skip-reader",
		&fakeReaderTempSession{logId: 88, readerName: "skip-reader"}, cfg)
	require.NoError(t, err)
	reader := r.(*logBatchReaderImpl)
	reader.batch = &proto.BatchReadResult{
		LastReadState: &proto.LastReadState{SegmentId: 3, LastBlockId: 7, BlockOffset: 4096},
	}
	reader.skips = config.LogSkipRanges{3: []config.SkipSpan{{FromEntryID: 10, ToEntryID: 19}}}

	moved := reader.skipPast(context.Background(), 3, 10)

	require.True(t, moved)
	require.EqualValues(t, 3, reader.pendingReadSegmentId, "the jump stays inside the segment")
	require.EqualValues(t, 20, reader.pendingReadEntryId)
	require.Nil(t, reader.batch,
		"the cached block offset is for entry 10, and the reuse test only compares the segment id")
	require.Zero(t, reader.next)
}

// TestReaderDoesNotSkipWhatIsNotDeclared is the other half: the lookup runs on every resolved
// position, so it has to leave a position outside any range exactly as it was.
func TestReaderDoesNotSkipWhatIsNotDeclared(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("skip-log").Maybe()
	logHandle.On("GetId").Return(int64(89)).Maybe()

	r, err := NewLogBatchReader(context.Background(), logHandle, nil,
		&LogMessageId{SegmentId: 3, EntryId: 5}, "no-skip-reader",
		&fakeReaderTempSession{logId: 89, readerName: "no-skip-reader"}, cfg)
	require.NoError(t, err)
	reader := r.(*logBatchReaderImpl)
	held := &proto.BatchReadResult{LastReadState: &proto.LastReadState{SegmentId: 3, LastBlockId: 1}}
	reader.batch = held
	reader.skips = config.LogSkipRanges{3: []config.SkipSpan{{FromEntryID: 10, ToEntryID: 19}}}

	require.False(t, reader.skipPast(context.Background(), 3, 5), "5 is below the range")
	require.False(t, reader.skipPast(context.Background(), 3, 20), "20 is above it")
	require.False(t, reader.skipPast(context.Background(), 4, 15), "another segment's 15")
	require.EqualValues(t, 3, reader.pendingReadSegmentId)
	require.EqualValues(t, 5, reader.pendingReadEntryId)
	require.Same(t, held, reader.batch, "nothing moved, so nothing was invalidated")
}

// TestReaderOpensWithNoRangesHeld is the cost discipline, asserted where it is decided rather than
// described in a comment. A reader that is making progress must never ask for the ranges: asking
// is a metadata read, ErrEntryNotFound is also the steady state of a reader tailing an idle log,
// and there may be many readers per log. It is also what keeps a range declared over readable data
// from costing anything -- a reader that never asks can never jump one.
func TestReaderOpensWithNoRangesHeld(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("quiet-log").Maybe()
	logHandle.On("GetId").Return(int64(90)).Maybe()
	logHandle.skipRanges = config.LogSkipRanges{3: []config.SkipSpan{{FromEntryID: 0, ToEntryID: 99}}}

	r, err := NewLogBatchReader(context.Background(), logHandle, nil,
		&LogMessageId{SegmentId: 3, EntryId: 10}, "quiet-reader",
		&fakeReaderTempSession{logId: 90, readerName: "quiet-reader"}, cfg)
	require.NoError(t, err)
	reader := r.(*logBatchReaderImpl)

	require.Zero(t, logHandle.skipRangeReads.Load(),
		"opening a reader must not read the skip ranges")
	require.Nil(t, reader.skips)
	require.False(t, reader.skipPast(context.Background(), 3, 10),
		"a reader that has not stalled holds no ranges, so it cannot jump one")
	require.EqualValues(t, 10, reader.pendingReadEntryId)
}

// TestReaderOpeningPositionIsTheFirstBaseline covers the first stall. The trigger compares the
// current position against where the reader was on the previous tick, so the opening position has
// to be that baseline -- otherwise the first tick compares against a zero value, sees a change, and
// a reader stuck from the moment it opened waits an extra interval.
func TestReaderOpeningPositionIsTheFirstBaseline(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("tick-log").Maybe()
	logHandle.On("GetId").Return(int64(91)).Maybe()

	r, err := NewLogBatchReader(context.Background(), logHandle, nil,
		&LogMessageId{SegmentId: 6, EntryId: 42}, "tick-reader",
		&fakeReaderTempSession{logId: 91, readerName: "tick-reader"}, cfg)
	require.NoError(t, err)
	reader := r.(*logBatchReaderImpl)

	require.EqualValues(t, 6, reader.lastReportedSegmentId)
	require.EqualValues(t, 42, reader.lastReportedEntryId)
}

// TestReaderAsksOnlyWhileItIsNotMovingOn covers the trigger, which is where the cost of this whole
// mechanism is decided. A reader whose position advances must never ask for the ranges; one whose
// position is where it was at the last report must.
func TestReaderAsksOnlyWhileItIsNotMovingOn(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	logHandle := &testLogHandleMock{}
	logHandle.Test(t)
	logHandle.On("GetName").Return("trigger-log").Maybe()
	logHandle.On("GetId").Return(int64(92)).Maybe()
	logHandle.skipRanges = config.LogSkipRanges{3: []config.SkipSpan{{FromEntryID: 10, ToEntryID: 19}}}

	r, err := NewLogBatchReader(context.Background(), logHandle, nil,
		&LogMessageId{SegmentId: 3, EntryId: 10}, "trigger-reader",
		&fakeReaderTempSession{logId: 92, readerName: "trigger-reader"}, cfg)
	require.NoError(t, err)
	reader := r.(*logBatchReaderImpl)
	ctx := context.Background()

	// Advancing: nothing is asked, whatever the record holds.
	reader.refreshSkipRangesIfStuck(ctx, 3, 11)
	require.Zero(t, logHandle.skipRangeReads.Load(), "a reader that moved on has no reason to ask")
	require.Nil(t, reader.skips)

	// Same position as the last report: asked once, and now holds the ranges.
	reader.refreshSkipRangesIfStuck(ctx, 3, 11)
	require.EqualValues(t, 1, logHandle.skipRangeReads.Load())
	require.NotNil(t, reader.skips)

	// Moving again stops it asking, and the next tick at the new position asks once more.
	reader.refreshSkipRangesIfStuck(ctx, 3, 30)
	require.EqualValues(t, 1, logHandle.skipRangeReads.Load())
	reader.refreshSkipRangesIfStuck(ctx, 3, 30)
	require.EqualValues(t, 2, logHandle.skipRangeReads.Load())
}

// TestGetSkipRanges_HostOverrideWinsAndCostsNoRead covers the hook an embedding application binds.
// When it has an opinion the client must not read its own record at all -- otherwise a host that
// manages these itself would still pay for, and be affected by, a record it does not use.
func TestGetSkipRanges_HostOverrideWinsAndCostsNoRead(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Client.SkipRanges.WithSource(func() (map[int64]config.LogSkipRanges, bool) {
		return map[int64]config.LogSkipRanges{
			7: {3: []config.SkipSpan{{FromEntryID: 1, ToEntryID: 2}}},
		}, true
	})

	// A provider that fails the test if it is consulted.
	handle := &logHandleImpl{Name: "override-log", Id: 7, cfg: cfg, Metadata: nil}

	got := handle.GetSkipRanges(context.Background())

	require.Equal(t, config.LogSkipRanges{3: []config.SkipSpan{{FromEntryID: 1, ToEntryID: 2}}}, got)
	require.Nil(t, handle.GetSkipRanges(context.Background())[4], "a segment the host did not name")
}
