package log

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/skiprange"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
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
	reader.skips = skiprange.BySegment{3: []skiprange.Span{{FromEntryID: 10, ToEntryID: 19}}}

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
	reader.skips = skiprange.BySegment{3: []skiprange.Span{{FromEntryID: 10, ToEntryID: 19}}}

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
	logHandle.skipRanges = skiprange.BySegment{3: []skiprange.Span{{FromEntryID: 0, ToEntryID: 99}}}

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
	logHandle.skipRanges = skiprange.BySegment{3: []skiprange.Span{{FromEntryID: 10, ToEntryID: 19}}}

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

	// Moving again stops it asking, and the next tick at the new position asks once more.
	reader.onReportTick(ctx, time.Now().UnixMilli(), 3, 30)
	require.EqualValues(t, 1, logHandle.skipRangeReads.Load())
	reader.onReportTick(ctx, time.Now().UnixMilli(), 3, 30)
	require.EqualValues(t, 2, logHandle.skipRangeReads.Load())

	// The tick also carries the timestamp the next tick is scheduled against, so it is the one
	// place the whole lastReported set is maintained and nothing at the call site has to agree.
	reader.onReportTick(ctx, 1234, 3, 31)
	require.EqualValues(t, 1234, reader.lastReported)
	require.EqualValues(t, 3, reader.lastReportedSegmentId)
	require.EqualValues(t, 31, reader.lastReportedEntryId)
}

// TestGetSkipRanges_HostOverrideWinsAndCostsNoRead covers the hook an embedding application binds.
// When it has an opinion the client must not read its own record at all -- otherwise a host that
// manages these itself would still pay for, and be affected by, a record it does not use.
// fixedSkipRanges is a host-supplied source: whatever it was built with, for every log it names.
type fixedSkipRanges map[int64]skiprange.BySegment

func (f fixedSkipRanges) For(_ context.Context, logID int64) skiprange.BySegment { return f[logID] }

func TestGetSkipRanges_HostOverrideWinsAndCostsNoRead(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	// A source of its own, and a nil metadata provider: if the handle consulted the record instead,
	// the default source would be reached and this would answer nothing.
	handle := &logHandleImpl{
		Name: "override-log", Id: 7, cfg: cfg, Metadata: nil,
		skipRanges: fixedSkipRanges{7: {3: []skiprange.Span{{FromEntryID: 1, ToEntryID: 2}}}},
	}

	got := handle.GetSkipRanges(context.Background())

	require.Equal(t, skiprange.BySegment{3: []skiprange.Span{{FromEntryID: 1, ToEntryID: 2}}}, got)
	require.Nil(t, handle.GetSkipRanges(context.Background())[4], "a segment the host did not name")
}

// TestNilSkipRangeSourceFallsBackToTheRecord keeps the client's option harmless when no application
// supplied one: openLogUnsafe passes whatever the client holds, which is usually nothing, so the
// handle has to end up with the provider's source rather than with nil.
func TestNilSkipRangeSourceFallsBackToTheRecord(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockMeta.EXPECT().SkipRangeSource().Return(fixedSkipRanges{}).Once()

	h := NewLogHandle("nil-option-log", 7, map[int64]*meta.SegmentMeta{}, mockMeta, nil, cfg, nil, nil,
		WithSkipRangeSource(nil))
	t.Cleanup(func() { _ = h.Close(context.Background()) })

	require.NotNil(t, h.(*logHandleImpl).skipRanges,
		"a nil source would panic on the first stalled read")
}

// TestGetSkipRangesWithoutASourceReadsAsNothing covers the one way a nil source can reach a handle:
// a provider that answers nil, which the type system cannot rule out for an interface. The read path
// has to treat that as "nothing declared" rather than panic -- a reader that cannot be told what to
// skip must keep reading.
func TestGetSkipRangesWithoutASourceReadsAsNothing(t *testing.T) {
	h := &logHandleImpl{Id: 7}

	require.Nil(t, h.GetSkipRanges(context.Background()))
}

// TestNewLogHandleWiresTheSkipRangeSource covers the construction path, which the tests that build a
// logHandleImpl by hand all bypass. Two things are only decided here: that a handle built with no
// option reads woodpecker's record, and that one built with an option reads that instead and never
// touches the record.
func TestNewLogHandleWiresTheSkipRangeSource(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	segments := map[int64]*meta.SegmentMeta{}

	t.Run("no option: the provider's source", func(t *testing.T) {
		mockMeta := mocks_meta.NewMetadataProvider(t)
		mockMeta.EXPECT().SkipRangeSource().Return(
			fixedSkipRanges{4: {1: []skiprange.Span{{FromEntryID: 5, ToEntryID: 6}}}},
		).Once()

		h := NewLogHandle("wired-log", 4, segments, mockMeta, nil, cfg, nil, nil)
		t.Cleanup(func() { _ = h.Close(context.Background()) })

		require.Equal(t, skiprange.BySegment{1: []skiprange.Span{{FromEntryID: 5, ToEntryID: 6}}},
			h.GetSkipRanges(context.Background()))
	})

	t.Run("with an option: that source, and the provider is never asked", func(t *testing.T) {
		// No EXPECT for SkipRangeSource: the mock fails the test if it is called.
		mockMeta := mocks_meta.NewMetadataProvider(t)

		h := NewLogHandle("wired-log", 4, segments, mockMeta, nil, cfg, nil, nil,
			WithSkipRangeSource(fixedSkipRanges{4: {1: []skiprange.Span{{FromEntryID: 9, ToEntryID: 9}}}}))
		t.Cleanup(func() { _ = h.Close(context.Background()) })

		require.Equal(t, skiprange.BySegment{1: []skiprange.Span{{FromEntryID: 9, ToEntryID: 9}}},
			h.GetSkipRanges(context.Background()))
	})
}
