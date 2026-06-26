// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

// testLogHandleMock is an in-package mock for LogHandle to avoid circular imports.
type testLogHandleMock struct {
	mock.Mock
}

func (m *testLogHandleMock) GetName() string {
	args := m.Called()
	return args.String(0)
}

func (m *testLogHandleMock) GetId() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *testLogHandleMock) GetSegments(ctx context.Context) (map[int64]*meta.SegmentMeta, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[int64]*meta.SegmentMeta), args.Error(1)
}

func (m *testLogHandleMock) OpenLogWriter(ctx context.Context) (LogWriter, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(LogWriter), args.Error(1)
}

func (m *testLogHandleMock) OpenLogReader(ctx context.Context, from *LogMessageId, readerBaseName string) (LogReader, error) {
	args := m.Called(ctx, from, readerBaseName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(LogReader), args.Error(1)
}

func (m *testLogHandleMock) GetLastRecordId(ctx context.Context) (*LogMessageId, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*LogMessageId), args.Error(1)
}

func (m *testLogHandleMock) Truncate(ctx context.Context, recordId *LogMessageId) error {
	args := m.Called(ctx, recordId)
	return args.Error(0)
}

func (m *testLogHandleMock) GetTruncatedRecordId(ctx context.Context) (*LogMessageId, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*LogMessageId), args.Error(1)
}

func (m *testLogHandleMock) CheckAndSetSegmentTruncatedIfNeed(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testLogHandleMock) GetNextSegmentId(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *testLogHandleMock) GetMetadataProvider() meta.MetadataProvider {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(meta.MetadataProvider)
}

func (m *testLogHandleMock) GetOrCreateWritableSegmentHandle(ctx context.Context, writerInvalidationNotifier func(context.Context, string)) (segment.SegmentHandle, error) {
	args := m.Called(ctx, writerInvalidationNotifier)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(segment.SegmentHandle), args.Error(1)
}

func (m *testLogHandleMock) GetExistsReadonlySegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	args := m.Called(ctx, segmentId)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(segment.SegmentHandle), args.Error(1)
}

func (m *testLogHandleMock) GetRecoverableSegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	args := m.Called(ctx, segmentId)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(segment.SegmentHandle), args.Error(1)
}

func (m *testLogHandleMock) CompleteAllActiveSegmentIfExists(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testLogHandleMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testLogHandleMock) GetCurrentWritableSegmentHandle(ctx context.Context) segment.SegmentHandle {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(segment.SegmentHandle)
}

// ---- test helpers ----

func newTestConfig() *config.Configuration {
	return &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxInterval: config.NewDurationSecondsFromInt(10),
					MaxSize:     64 * 1024 * 1024,
					MaxBlocks:   1000,
				},
				Auditor: config.AuditorConfig{
					MaxInterval: config.NewDurationSecondsFromInt(5),
				},
				SessionMonitor: config.SessionMonitorConfig{
					CheckInterval: config.NewDurationSecondsFromInt(3),
					MaxFailures:   5,
				},
			},
		},
	}
}

// ---- Tests ----

func TestLogReader_GetName(t *testing.T) {
	reader := &logBatchReaderImpl{
		readerName: "my-reader",
	}
	assert.Equal(t, "my-reader", reader.GetName())
}

func TestLogReader_ReadNext_NilLogHandle(t *testing.T) {
	reader := &logBatchReaderImpl{
		logHandle: nil,
		logIdStr:  "1",
		logNs:     "",
	}

	ctx := context.Background()
	msg, err := reader.ReadNext(ctx)
	assert.Nil(t, msg)
	assert.Error(t, err)
	assert.True(t, werr.ErrInternalError.Is(err))
}

func TestLogReader_ReadNext_ContextCancelled(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		readerName:           "test-reader",
		logNs:                "",
		batch:                nil,
		next:                 0,
		lastRead:             time.Now().UnixMilli(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	msg, err := reader.ReadNext(ctx)
	assert.Nil(t, msg)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestLogReader_ReadNext_FromCachedBatch(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)

	// Prepare a valid marshalled message
	writeMsg := &WriteMessage{
		Payload:    []byte("test payload"),
		Properties: map[string]string{"key": "value"},
	}
	data, err := MarshalMessage(writeMsg)
	require.NoError(t, err)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		readerName:           "test-reader",
		logNs:                "",
		batch: &proto.BatchReadResult{
			Entries: []*proto.LogEntry{
				{SegId: 0, EntryId: 0, Values: data},
			},
		},
		next:     0,
		lastRead: time.Now().UnixMilli(),
	}

	ctx := context.Background()
	msg, err := reader.ReadNext(ctx)
	require.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, int64(0), msg.Id.SegmentId)
	assert.Equal(t, int64(0), msg.Id.EntryId)
	assert.Equal(t, writeMsg.Payload, msg.Payload)
	assert.Equal(t, writeMsg.Properties, msg.Properties)
	// Verify cursor was advanced
	assert.Equal(t, int64(1), reader.pendingReadEntryId)
	assert.Equal(t, 1, reader.next)
}

func TestLogReader_ReadNext_CachedBatchCorruptedData(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		readerName:           "test-reader",
		logNs:                "",
		batch: &proto.BatchReadResult{
			Entries: []*proto.LogEntry{
				{SegId: 0, EntryId: 0, Values: []byte("corrupted data that is not protobuf")},
			},
		},
		next:     0,
		lastRead: time.Now().UnixMilli(),
	}

	ctx := context.Background()
	msg, err := reader.ReadNext(ctx)
	assert.Nil(t, msg)
	assert.Error(t, err)
	assert.True(t, werr.ErrLogReaderReadFailed.Is(err))
	// Verify batch was cleared
	assert.Nil(t, reader.batch)
	assert.Equal(t, 0, reader.next)
}

func TestLogReader_ReadNext_FreshBatchRead(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	// Prepare a valid marshalled message
	writeMsg := &WriteMessage{
		Payload:    []byte("fresh batch data"),
		Properties: map[string]string{"source": "test"},
	}
	data, err := MarshalMessage(writeMsg)
	require.NoError(t, err)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		currentSegmentHandle: mockSegHandle,
		readerName:           "test-reader",
		logNs:                "",
		batch:                nil,
		next:                 0,
		lastRead:             time.Now().UnixMilli(),
	}

	// Mock getNextSegHandleAndIDs: GetNextSegmentId returns nextSegId=1 (latest=0)
	mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(1), nil)
	// current segment handle matches pendingReadSegmentId
	mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()
	mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			State:       proto.SegmentState_Active,
			LastEntryId: 10,
		},
	}).Maybe()
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().UpdateReaderTempInfo(mock.Anything, int64(1), "test-reader", int64(0), int64(0)).Return(nil).Maybe()

	// ReadBatchAdv returns a batch with one entry
	mockSegHandle.EXPECT().ReadBatchAdv(mock.Anything, int64(0), int64(DefaultBatchEntriesLimit), mock.Anything).Return(
		&proto.BatchReadResult{
			Entries: []*proto.LogEntry{
				{SegId: 0, EntryId: 0, Values: data},
			},
		}, nil,
	)

	ctx := context.Background()
	msg, readErr := reader.ReadNext(ctx)
	require.NoError(t, readErr)
	assert.NotNil(t, msg)
	assert.Equal(t, int64(0), msg.Id.SegmentId)
	assert.Equal(t, int64(0), msg.Id.EntryId)
	assert.Equal(t, writeMsg.Payload, msg.Payload)
}

func TestLogReader_ReadNext_SegmentEOF_MovesToNextSegment(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockSegHandle0 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegHandle1 := mocks_segment_handle.NewSegmentHandle(t)

	// Prepare valid marshalled message for segment 1
	writeMsg := &WriteMessage{
		Payload:    []byte("data in segment 1"),
		Properties: map[string]string{"seg": "1"},
	}
	data, err := MarshalMessage(writeMsg)
	require.NoError(t, err)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   5,
		currentSegmentHandle: mockSegHandle0,
		readerName:           "test-reader",
		logNs:                "",
		batch:                nil,
		next:                 0,
		lastRead:             time.Now().UnixMilli(),
	}

	// First call: GetNextSegmentId returns 2 (latest=1)
	mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(2), nil)
	mockSegHandle0.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()
	mockSegHandle0.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			State:       proto.SegmentState_Active,
			LastEntryId: 10,
		},
	}).Maybe()
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().UpdateReaderTempInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	// First ReadBatchAdv returns EOF
	mockSegHandle0.EXPECT().ReadBatchAdv(mock.Anything, int64(5), int64(DefaultBatchEntriesLimit), mock.Anything).Return(
		nil, werr.ErrFileReaderEndOfFile,
	)

	// Second iteration: get segment handle for segment 1
	mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(1)).Return(mockSegHandle1, nil)
	mockSegHandle1.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			State:       proto.SegmentState_Active,
			LastEntryId: 5,
		},
	}).Maybe()
	mockSegHandle1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()

	// ReadBatchAdv on segment 1 returns data
	mockSegHandle1.EXPECT().ReadBatchAdv(mock.Anything, int64(0), int64(DefaultBatchEntriesLimit), mock.Anything).Return(
		&proto.BatchReadResult{
			Entries: []*proto.LogEntry{
				{SegId: 1, EntryId: 0, Values: data},
			},
		}, nil,
	)

	ctx := context.Background()
	msg, readErr := reader.ReadNext(ctx)
	require.NoError(t, readErr)
	assert.NotNil(t, msg)
	assert.Equal(t, int64(1), msg.Id.SegmentId)
	assert.Equal(t, int64(0), msg.Id.EntryId)
}

func TestLogReader_UnmarshalAndCreateLogMessage(t *testing.T) {
	reader := &logBatchReaderImpl{
		logName:    "test-log",
		logId:      1,
		logIdStr:   "1",
		readerName: "test-reader",
	}

	t.Run("Success", func(t *testing.T) {
		writeMsg := &WriteMessage{
			Payload:    []byte("test"),
			Properties: map[string]string{"k": "v"},
		}
		data, err := MarshalMessage(writeMsg)
		require.NoError(t, err)

		ctx := context.Background()
		msg, err := reader.unmarshalAndCreateLogMessage(ctx, data, 5, 10)
		require.NoError(t, err)
		assert.NotNil(t, msg)
		assert.Equal(t, int64(5), msg.Id.SegmentId)
		assert.Equal(t, int64(10), msg.Id.EntryId)
		assert.Equal(t, writeMsg.Payload, msg.Payload)
		assert.Equal(t, writeMsg.Properties, msg.Properties)
	})

	t.Run("InvalidData", func(t *testing.T) {
		ctx := context.Background()
		msg, err := reader.unmarshalAndCreateLogMessage(ctx, []byte("bad data"), 5, 10)
		assert.Nil(t, msg)
		assert.Error(t, err)
		assert.True(t, werr.ErrLogReaderReadFailed.Is(err))
	})
}

func TestLogReader_WaitWithContext(t *testing.T) {
	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		readerName:           "test-reader",
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
	}

	t.Run("ContextCancelledDuringWait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		err := reader.waitWithContext(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("NormalWaitCompletes", func(t *testing.T) {
		ctx := context.Background()
		start := time.Now()
		err := reader.waitWithContext(ctx)
		elapsed := time.Since(start)
		assert.NoError(t, err)
		// Should wait at least NoDataReadWaitIntervalMs
		assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(NoDataReadWaitIntervalMs-50))
	})
}

func TestLogReader_IsEntryInCurrentSegment(t *testing.T) {
	t.Run("CompletedSegment_EntryInRange", func(t *testing.T) {
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
		mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Completed,
				LastEntryId: 10,
			},
		})
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			readerName:           "test-reader",
			currentSegmentHandle: mockSegHandle,
			pendingReadEntryId:   5,
		}

		ctx := context.Background()
		result := reader.isEntryInCurrentSegment(ctx)
		assert.True(t, result)
	})

	t.Run("CompletedSegment_EntryOutOfRange", func(t *testing.T) {
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
		mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Completed,
				LastEntryId: 10,
			},
		})

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			readerName:           "test-reader",
			currentSegmentHandle: mockSegHandle,
			pendingReadEntryId:   15,
		}

		ctx := context.Background()
		result := reader.isEntryInCurrentSegment(ctx)
		assert.False(t, result)
	})

	t.Run("ActiveSegment_AlwaysTrue", func(t *testing.T) {
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
		mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Active,
				LastEntryId: 0,
			},
		})
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			readerName:           "test-reader",
			currentSegmentHandle: mockSegHandle,
			pendingReadEntryId:   100,
		}

		ctx := context.Background()
		result := reader.isEntryInCurrentSegment(ctx)
		assert.True(t, result)
	})

	t.Run("SealedSegment_EntryInRange", func(t *testing.T) {
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
		mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Sealed,
				LastEntryId: 50,
			},
		})
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			readerName:           "test-reader",
			currentSegmentHandle: mockSegHandle,
			pendingReadEntryId:   25,
		}

		ctx := context.Background()
		result := reader.isEntryInCurrentSegment(ctx)
		assert.True(t, result)
	})
}

func TestLogReader_FindNextReadableSegment(t *testing.T) {
	t.Run("NoSegmentsExist", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   0,
		}

		ctx := context.Background()
		// latestSegmentId = -1, so loop does not execute
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, -1)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
	})

	t.Run("FoundReadableSegment", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   5,
		}

		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(mockSegHandle, nil)
		mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Active,
				LastEntryId: 10,
			},
		})
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 0)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(0), segId)
		assert.Equal(t, int64(5), entryId)
		assert.NoError(t, err)
	})

	t.Run("ReadTruncatedSegmentNotGCed", func(t *testing.T) {
		// A segment that is logically Truncated but still physically present must be
		// read directly from the requested position. It is only skipped once it has
		// been physically GC'd (ErrSegmentNotFound). See issue #209.
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockTruncatedSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   3,
		}

		// Segment 0 is marked Truncated but still exists -> must be returned as-is
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(mockTruncatedSegHandle, nil)
		mockTruncatedSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Truncated,
				LastEntryId: 10,
			},
		})
		mockTruncatedSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 0)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(0), segId)
		assert.Equal(t, int64(3), entryId) // unchanged: read directly from the requested position
		assert.NoError(t, err)
		// The segment is still present, so no truncation-point lookup is needed.
		mockLogHandle.AssertNotCalled(t, "GetTruncatedRecordId", mock.Anything)
	})

	t.Run("SkipGCedSegmentToOldestLiveSegment", func(t *testing.T) {
		// Requested position falls on physically GC'd segments (ErrSegmentNotFound)
		// below the truncation point -> skip forward to the oldest segment that still
		// exists and continue from its first entry. See issue #209.
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockLiveSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   7,
		}

		// Truncation point at segment 5; segments 0 and 1 are GC'd, segment 2 is live.
		mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 100}, nil)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(nil, werr.ErrSegmentNotFound)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(1)).Return(nil, werr.ErrSegmentNotFound)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(2)).Return(mockLiveSegHandle, nil)
		mockLiveSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Active,
				LastEntryId: 20,
			},
		})
		mockLiveSegHandle.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 5)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(2), segId)
		assert.Equal(t, int64(0), entryId) // reset to 0 because we advanced past GC'd segments
		assert.NoError(t, err)
	})

	t.Run("WaitWhenMissingSegmentBeyondTruncationPoint", func(t *testing.T) {
		// A missing segment that is *beyond* the truncation point is "not written yet"
		// (future) -> wait, do NOT skip forward even if a later segment already exists.
		// This preserves normal tail-read behavior. See issue #209.
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSeg7 := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 6,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 3, EntryId: 50}, nil)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(6)).Return(nil, werr.ErrSegmentNotFound)
		// Segment 7 exists but must NOT be reached, because 6 > truncatedPoint(3).
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(7)).Return(mockSeg7, nil).Maybe()
		mockSeg7.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Active, LastEntryId: 5},
		}).Maybe()
		mockSeg7.EXPECT().GetId(mock.Anything).Return(int64(7)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 7)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
	})

	t.Run("NoTruncationPointMissingSegmentWaits", func(t *testing.T) {
		// When the log was never truncated (truncated point == -1) there is no GC
		// region; a missing segment is "not written yet" -> wait, do not move the read
		// position (unchanged legacy behavior). See issue #209.
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSeg1 := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: -1, EntryId: -1}, nil)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(nil, werr.ErrSegmentNotFound)
		// Segment 1 exists but must NOT be reached because there is no GC region.
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(1)).Return(mockSeg1, nil).Maybe()
		mockSeg1.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Active, LastEntryId: 5},
		}).Maybe()
		mockSeg1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 1)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
	})

	t.Run("TruncationSegmentItselfGCedSkipsForward", func(t *testing.T) {
		// Boundary: the missing segment id == truncated point's segment id. After the
		// truncation point advances, the old truncation segment can itself be GC'd, so
		// a missing segment AT the truncation point must skip forward (<=, not <).
		// See issue #209.
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSeg6 := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 5,
			pendingReadEntryId:   2,
		}

		mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 0}, nil)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(5)).Return(nil, werr.ErrSegmentNotFound)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(6)).Return(mockSeg6, nil)
		mockSeg6.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Active, LastEntryId: 5},
		})
		mockSeg6.EXPECT().GetId(mock.Anything).Return(int64(6)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 6)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(6), segId)
		assert.Equal(t, int64(0), entryId)
		assert.NoError(t, err)
	})

	t.Run("GetTruncatedRecordIdErrorFallsBackToSkip", func(t *testing.T) {
		// If the truncation point cannot be read, degrade to skipping the missing
		// segment (legacy bounded-scan behavior) rather than blocking forever.
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSeg1 := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(nil, werr.ErrInternalError)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(nil, werr.ErrSegmentNotFound)
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(1)).Return(mockSeg1, nil)
		mockSeg1.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Active, LastEntryId: 5},
		})
		mockSeg1.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 1)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(1), segId)
		assert.Equal(t, int64(0), entryId)
		assert.NoError(t, err)
	})

	t.Run("IdleReaderBeyondLatestDoesNotFetchTruncationPoint", func(t *testing.T) {
		// pending(5) > latest(4): the scan loop never runs, so an idle tail reader must
		// return ErrSegmentNotFound WITHOUT reading the truncation point (no per-poll
		// etcd read, cf. #189/#190).
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 5,
			pendingReadEntryId:   0,
		}

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 4)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
		mockLogHandle.AssertNotCalled(t, "GetTruncatedRecordId", mock.Anything)
		mockLogHandle.AssertNotCalled(t, "GetExistsReadonlySegmentHandle", mock.Anything, mock.Anything)
	})

	t.Run("GetSegmentHandleReturnsOtherError", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(nil, werr.ErrInternalError)

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.findNextReadableSegment(ctx, 0)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.Error(t, err)
		assert.True(t, werr.ErrInternalError.Is(err))
	})
}

func TestLogReader_HandleTailRead(t *testing.T) {
	t.Run("NoSegmentsExist", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: LatestLogMessageID().SegmentId,
			pendingReadEntryId:   0,
		}

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.handleTailRead(ctx, -1)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
		// pendingReadSegmentId should be updated to 0
		assert.Equal(t, int64(0), reader.pendingReadSegmentId)
	})

	t.Run("SegmentExistsWithEntries", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: LatestLogMessageID().SegmentId,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(2)).Return(mockSegHandle, nil)
		mockSegHandle.EXPECT().GetLastAddConfirmed(mock.Anything).Return(int64(5), nil)
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(2)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.handleTailRead(ctx, 2)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(2), segId)
		assert.Equal(t, int64(6), entryId) // lastConfirmed + 1
		assert.NoError(t, err)
	})

	t.Run("SegmentExistsNoEntries", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: LatestLogMessageID().SegmentId,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(0)).Return(mockSegHandle, nil)
		mockSegHandle.EXPECT().GetLastAddConfirmed(mock.Anything).Return(int64(0), werr.ErrFileReaderNoBlockFound)
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.handleTailRead(ctx, 0)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(0), segId)
		assert.Equal(t, int64(0), entryId)
		assert.NoError(t, err)
	})

	t.Run("SegmentNotFound", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: LatestLogMessageID().SegmentId,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(3)).Return(nil, werr.ErrSegmentNotFound)

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.handleTailRead(ctx, 3)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.Error(t, err)
		assert.True(t, werr.ErrSegmentNotFound.Is(err))
		assert.Equal(t, int64(3), reader.pendingReadSegmentId)
	})

	t.Run("GetLastAddConfirmedError", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: LatestLogMessageID().SegmentId,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(1)).Return(mockSegHandle, nil)
		mockSegHandle.EXPECT().GetLastAddConfirmed(mock.Anything).Return(int64(0), werr.ErrInternalError)
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.handleTailRead(ctx, 1)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.Error(t, err)
		assert.True(t, werr.ErrInternalError.Is(err))
	})
}

func TestLogReader_GetNextSegHandleAndIDs(t *testing.T) {
	t.Run("GetNextSegmentIdError", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 0,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(0), werr.ErrInternalError)

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.getNextSegHandleAndIDs(ctx)
		assert.Nil(t, segHandle)
		assert.Equal(t, int64(-1), segId)
		assert.Equal(t, int64(-1), entryId)
		assert.Error(t, err)
	})

	t.Run("TailReadMode", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: LatestLogMessageID().SegmentId,
			pendingReadEntryId:   0,
		}

		mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(2), nil) // nextSegId=2, latest=1
		mockLogHandle.On("GetExistsReadonlySegmentHandle", mock.Anything, int64(1)).Return(mockSegHandle, nil)
		mockSegHandle.EXPECT().GetLastAddConfirmed(mock.Anything).Return(int64(3), nil)
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(1)).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.getNextSegHandleAndIDs(ctx)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(1), segId)
		assert.Equal(t, int64(4), entryId) // lastConfirmed + 1
		assert.NoError(t, err)
	})

	t.Run("CurrentSegmentContainsEntry", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

		reader := &logBatchReaderImpl{
			logName:              "test-log",
			logId:                1,
			logIdStr:             "1",
			logHandle:            mockLogHandle,
			readerName:           "test-reader",
			pendingReadSegmentId: 5,
			pendingReadEntryId:   3,
			currentSegmentHandle: mockSegHandle,
		}

		mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(10), nil)
		mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(5)).Maybe()
		mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
			Metadata: &proto.SegmentMetadata{
				State:       proto.SegmentState_Completed,
				LastEntryId: 10,
			},
		}).Maybe()

		ctx := context.Background()
		segHandle, segId, entryId, err := reader.getNextSegHandleAndIDs(ctx)
		assert.NotNil(t, segHandle)
		assert.Equal(t, int64(5), segId)
		assert.Equal(t, int64(3), entryId)
		assert.NoError(t, err)
	})
}

func TestNewLogBatchReader(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	cfg := newTestConfig()

	mockLogHandle.On("GetName").Return("test-log")
	mockLogHandle.On("GetId").Return(int64(1))

	from := &LogMessageId{SegmentId: 0, EntryId: 0}
	reader, err := NewLogBatchReader(context.Background(), mockLogHandle, mockSegHandle, from, "test-reader", cfg)
	require.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, "test-reader", reader.GetName())
}

func TestLogReader_Close(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockMetadata := mocks_meta.NewMetadataProvider(t)

		reader := &logBatchReaderImpl{
			logName:    "test-log",
			logId:      1,
			logIdStr:   "1",
			logHandle:  mockLogHandle,
			readerName: "test-reader",
			logNs:      "",
		}

		mockLogHandle.On("GetMetadataProvider").Return(mockMetadata)
		mockLogHandle.On("GetId").Return(int64(1))
		mockMetadata.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), "test-reader").Return(nil)

		err := reader.Close(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Error", func(t *testing.T) {
		mockLogHandle := &testLogHandleMock{}
		mockLogHandle.Test(t)
		mockMetadata := mocks_meta.NewMetadataProvider(t)

		reader := &logBatchReaderImpl{
			logName:    "test-log",
			logId:      1,
			logIdStr:   "1",
			logHandle:  mockLogHandle,
			readerName: "test-reader",
			logNs:      "",
		}

		mockLogHandle.On("GetMetadataProvider").Return(mockMetadata)
		mockLogHandle.On("GetId").Return(int64(1))
		mockMetadata.EXPECT().DeleteReaderTempInfo(mock.Anything, int64(1), "test-reader").Return(werr.ErrInternalError)

		err := reader.Close(context.Background())
		assert.Error(t, err)
	})
}

func TestLogReader_ReadNext_EntryNotFound_ContextCancelled(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		currentSegmentHandle: mockSegHandle,
		readerName:           "test-reader",
		logNs:                "",
		batch:                nil,
		next:                 0,
		lastRead:             time.Now().UnixMilli(),
	}

	mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(1), nil)
	mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()
	mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			State:       proto.SegmentState_Active,
			LastEntryId: 10,
		},
	}).Maybe()
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().UpdateReaderTempInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// ReadBatchAdv returns ErrEntryNotFound
	mockSegHandle.EXPECT().ReadBatchAdv(mock.Anything, int64(0), int64(DefaultBatchEntriesLimit), mock.Anything).Return(
		nil, werr.ErrEntryNotFound,
	)

	// Use a context that will be cancelled during waitWithContext
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	msg, err := reader.ReadNext(ctx)
	assert.Nil(t, msg)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestLogReader_ReadNext_OtherReadError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		currentSegmentHandle: mockSegHandle,
		readerName:           "test-reader",
		logNs:                "",
		batch:                nil,
		next:                 0,
		lastRead:             time.Now().UnixMilli(),
	}

	mockLogHandle.On("GetNextSegmentId", mock.Anything).Return(int64(1), nil)
	mockSegHandle.EXPECT().GetId(mock.Anything).Return(int64(0)).Maybe()
	mockSegHandle.EXPECT().GetMetadata(mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			State:       proto.SegmentState_Active,
			LastEntryId: 10,
		},
	}).Maybe()
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().UpdateReaderTempInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// ReadBatchAdv returns a generic error
	mockSegHandle.EXPECT().ReadBatchAdv(mock.Anything, int64(0), int64(DefaultBatchEntriesLimit), mock.Anything).Return(
		nil, werr.ErrInternalError,
	)

	ctx := context.Background()
	msg, err := reader.ReadNext(ctx)
	assert.Nil(t, msg)
	assert.Error(t, err)
	assert.True(t, werr.ErrInternalError.Is(err))
}

func TestLogReader_ReadNext_MultipleBatchEntries(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)

	// Prepare multiple valid marshalled messages
	writeMsg1 := &WriteMessage{Payload: []byte("msg1"), Properties: map[string]string{"idx": "0"}}
	writeMsg2 := &WriteMessage{Payload: []byte("msg2"), Properties: map[string]string{"idx": "1"}}
	data1, _ := MarshalMessage(writeMsg1)
	data2, _ := MarshalMessage(writeMsg2)

	reader := &logBatchReaderImpl{
		logName:              "test-log",
		logId:                1,
		logIdStr:             "1",
		logHandle:            mockLogHandle,
		pendingReadSegmentId: 0,
		pendingReadEntryId:   0,
		readerName:           "test-reader",
		logNs:                "",
		batch: &proto.BatchReadResult{
			Entries: []*proto.LogEntry{
				{SegId: 0, EntryId: 0, Values: data1},
				{SegId: 0, EntryId: 1, Values: data2},
			},
		},
		next:     0,
		lastRead: time.Now().UnixMilli(),
	}

	ctx := context.Background()

	// Read first entry
	msg1, err := reader.ReadNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("msg1"), msg1.Payload)
	assert.Equal(t, int64(0), msg1.Id.EntryId)

	// Read second entry (from same batch)
	msg2, err := reader.ReadNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte("msg2"), msg2.Payload)
	assert.Equal(t, int64(1), msg2.Id.EntryId)
}
