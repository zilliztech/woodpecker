package log

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

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
	reader.skips = &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{3: {Ranges: []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19}}}}}

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
	reader.skips = &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{3: {Ranges: []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19}}}}}

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
	logHandle.skipRanges = &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{3: {Ranges: []*proto.SkipRange{{FromEntryId: 0, ToEntryId: 99}}}}}

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
	logHandle.skipRanges = &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{3: {Ranges: []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19}}}}}

	r, err := NewLogBatchReader(context.Background(), logHandle, nil,
		&LogMessageId{SegmentId: 3, EntryId: 10}, "trigger-reader",
		&fakeReaderTempSession{logId: 92, readerName: "trigger-reader"}, cfg)
	require.NoError(t, err)
	reader := r.(*logBatchReaderImpl)
	ctx := context.Background()

	// Advancing: nothing is asked, whatever the record holds.
	reader.onReportTick(ctx, time.Now().UnixMilli(), 3, 11)
	require.Zero(t, logHandle.skipRangeReads.Load(), "a reader that moved on has no reason to ask")
	require.Nil(t, reader.skips)

	// Same position as the last report: asked once, and now holds the ranges.
	reader.onReportTick(ctx, time.Now().UnixMilli(), 3, 11)
	require.EqualValues(t, 1, logHandle.skipRangeReads.Load())
	require.NotNil(t, reader.skips)

	// Moving again clears what the stall fetched, so a range held from the stalled position cannot
	// act at the new one; the next tick at that new position then asks once more.
	reader.onReportTick(ctx, time.Now().UnixMilli(), 3, 30)
	require.EqualValues(t, 1, logHandle.skipRangeReads.Load())
	require.Nil(t, reader.skips, "a moving reader drops the ranges fetched while it was stuck")
	reader.onReportTick(ctx, time.Now().UnixMilli(), 3, 30)
	require.EqualValues(t, 2, logHandle.skipRangeReads.Load())
	require.NotNil(t, reader.skips)

	// The tick also carries the timestamp the next tick is scheduled against, so it is the one
	// place the whole lastReported set is maintained and nothing at the call site has to agree.
	reader.onReportTick(ctx, 1234, 3, 31)
	require.EqualValues(t, 1234, reader.lastReported)
	require.EqualValues(t, 3, reader.lastReportedSegmentId)
	require.EqualValues(t, 31, reader.lastReportedEntryId)
}

// TestGetSkipRangesDelegatesToTheProvider covers the one thing the handle still decides: which
// collaborator answers what a log's readers should pass over. The ranges themselves now come
// straight from the metadata provider for this log, and the handle hands them back unchanged.
func TestGetSkipRangesDelegatesToTheProvider(t *testing.T) {
	ranges := &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{
		3: {Ranges: []*proto.SkipRange{{FromEntryId: 1, ToEntryId: 2}}},
	}}
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockMeta.EXPECT().GetLogSkipRanges(mock.Anything, int64(7)).Return(ranges).Once()

	h := &logHandleImpl{Id: 7, Metadata: mockMeta}

	require.Same(t, ranges, h.GetSkipRanges(context.Background()))
}

// TestGetSkipRangesNothingDeclaredReadsAsNothing covers the normal case: no ranges were declared for
// this log, so the reader is handed nil and keeps reading. The handle must not invent an empty value,
// because the reader decides whether to ask by whether the result is nil.
func TestGetSkipRangesNothingDeclaredReadsAsNothing(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockMeta.EXPECT().GetLogSkipRanges(mock.Anything, int64(7)).Return(nil).Once()

	h := &logHandleImpl{Id: 7, Metadata: mockMeta}

	require.Nil(t, h.GetSkipRanges(context.Background()))
}

// TestReadNextSkipsPastADeclaredRange drives the skip-range jump through ReadNext itself rather than
// calling skipPast/onReportTick directly, which is the wiring the unit tests above leave to
// reasoning: the report tick at the stale position fetches the ranges, skipPast is consulted on the
// resolved position and moves it, and the next batch read is asked for the entry after the range.
func TestReadNextSkipsPastADeclaredRange(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	writeMsg := &WriteMessage{Payload: []byte("after the skip")}
	data, err := MarshalMessage(writeMsg)
	require.NoError(t, err)

	reader := &logBatchReaderImpl{
		logName:               "skip-log",
		logId:                 7,
		logIdStr:              "7",
		logHandle:             mockLogHandle,
		pendingReadSegmentId:  3,
		pendingReadEntryId:    10,
		currentSegmentHandle:  mockSegHandle,
		readerName:            "skip-reader",
		readerTempSession:     &fakeReaderTempSession{logId: 7, readerName: "skip-reader"},
		logNs:                 "",
		lastRead:              time.Now().UnixMilli(),
		lastReported:          time.Now().UnixMilli() - UpdateReaderInfoIntervalMs - 1000,
		lastReportedSegmentId: 3,
		lastReportedEntryId:   10,
	}
	// The ranges the stalled tick fetches cover 10-19, so the reader should resume at 20.
	mockLogHandle.skipRanges = &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{
		3: {Ranges: []*proto.SkipRange{{FromEntryId: 10, ToEntryId: 19, Reason: "bad disk"}}},
	}}

	// The current segment handle holds the pending position, so resolution stops there.
	mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(4), nil)
	mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(3)).Maybe()
	mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Active, LastEntryId: 100},
	}).Maybe()

	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().UpdateReaderTempInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// The only batch read must be for entry 20: the range moved the position before reading, so the
	// reader never asks for entry 10.
	mockSegHandle.EXPECT().ReadBatchAdv(mock.Anything, int64(20), int64(DefaultBatchEntriesLimit), mock.Anything).
		Return(&proto.BatchReadResult{
			Entries: []*proto.LogEntry{{SegId: 3, EntryId: 20, Values: data}},
		}, nil,
		).Once()

	msg, readErr := reader.ReadNext(context.Background())
	require.NoError(t, readErr)
	require.NotNil(t, msg)
	require.Equal(t, int64(3), msg.Id.SegmentId)
	require.Equal(t, int64(20), msg.Id.EntryId, "the reader resumed after the range, not at its start")
	require.Equal(t, writeMsg.Payload, msg.Payload)
}
