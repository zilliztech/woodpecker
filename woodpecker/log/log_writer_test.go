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
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

// mockCleanupManager is a test helper mock for SegmentCleanupManager
type mockCleanupManager struct {
	mock.Mock
}

func (m *mockCleanupManager) CleanupSegment(ctx context.Context, logName string, logId int64, segmentId int64) error {
	args := m.Called(ctx, logName, logId, segmentId)
	return args.Error(0)
}

func (m *mockCleanupManager) CleanupOrphanedStatuses(ctx context.Context, logId int64, minSegmentId int64) error {
	args := m.Called(ctx, logId, minSegmentId)
	return args.Error(0)
}

// createTestInternalWriter creates an internalLogWriterImpl for testing without goroutines.
func createTestInternalWriter(t *testing.T, logHandle LogHandle, cleanupMgr segment.SegmentCleanupManager) *internalLogWriterImpl {
	cfg := newTestConfig()
	w := &internalLogWriterImpl{
		logIdStr:           "1",
		logHandle:          logHandle,
		auditorMaxInterval: cfg.Woodpecker.Client.Auditor.MaxInterval.Seconds(),
		cfg:                cfg,
		metricsNamespace:   "",
		writerClose:        make(chan struct{}, 1),
		cleanupManager:     cleanupMgr,
	}
	w.isWriterValid.Store(true)
	w.onWriterInvalidated = func(ctx context.Context, reason string) {
		w.isWriterValid.Store(false)
	}
	return w
}

func TestInternalLogWriter_Write_InvalidWriter(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.isWriterValid.Store(false) // Mark as invalid

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err))
	assert.Nil(t, result.LogMessageId)
}

func TestInternalLogWriter_Write_Success(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	w := createTestInternalWriter(t, mockLogHandle, nil)

	// Mock GetOrCreateWritableSegmentHandle
	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	// Mock AppendAsync: immediately call callback with success
	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(0, 5, nil)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.NotNil(t, result.LogMessageId)
	assert.Equal(t, int64(0), result.LogMessageId.SegmentId)
	assert.Equal(t, int64(5), result.LogMessageId.EntryId)
}

func TestInternalLogWriter_Write_GetWritableSegmentError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrInternalError.Is(result.Err))
}

func TestInternalLogWriter_Write_InvalidMessage(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	w := createTestInternalWriter(t, mockLogHandle, nil)

	// GetOrCreateWritableSegmentHandle succeeds
	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	ctx := context.Background()
	// Both payload and properties empty => invalid
	msg := &WriteMessage{
		Payload:    nil,
		Properties: nil,
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrInvalidMessage.Is(result.Err))
}

func TestInternalLogWriter_Write_AppendError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	// Mock AppendAsync to callback with error
	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(-1, -1, werr.ErrInternalError)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
}

func TestInternalLogWriter_WriteAsync_InvalidWriter(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.isWriterValid.Store(false)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test"),
		Properties: map[string]string{"k": "v"},
	}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err))
}

func TestInternalLogWriter_WriteAsync_Success(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	// Mock AppendAsync: callback with success
	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(0, 3, nil)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("async test data"),
		Properties: map[string]string{"k": "v"},
	}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	require.NoError(t, result.Err)
	assert.Equal(t, int64(0), result.LogMessageId.SegmentId)
	assert.Equal(t, int64(3), result.LogMessageId.EntryId)
}

func TestInternalLogWriter_WriteAsync_InvalidMessage(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    nil,
		Properties: nil,
	}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrInvalidMessage.Is(result.Err))
}

func TestInternalLogWriter_WriteAsync_GetWritableSegmentError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrInternalError.Is(result.Err))
}

func TestInternalLogWriter_Close(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
	// After close, writer should be invalid
	assert.False(t, w.isWriterValid.Load())
}

func TestInternalLogWriter_Close_WithErrors(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrInternalError)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.Error(t, err) // Should combine errors
}

func TestInternalLogWriter_Close_SegmentNotFoundIgnored(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	// ErrSegmentNotFound should be silently ignored during close
	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrSegmentNotFound)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
}

func TestInternalLogWriter_Close_MetadataRevisionInvalidIgnored(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	// ErrMetadataRevisionInvalid should be silently ignored during close (fenced writer)
	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrMetadataRevisionInvalid)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
}

func TestInternalLogWriter_Close_Idempotent(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	// First close should succeed
	err1 := w.Close(context.Background())
	assert.NoError(t, err1)

	// Second close should also succeed (idempotent via closeOnce)
	err2 := w.Close(context.Background())
	assert.NoError(t, err2)
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_NoTruncationPoint(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)

	// No truncation point set (negative values)
	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: -1, EntryId: -1}, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	// Should return early - no cleanup attempted
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_GetTruncatedRecordIdError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_AlreadyInProgress(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)

	// Mark cleanup as already in progress
	w.cleanupMutex.Lock()
	w.cleanupInProgress = true
	w.cleanupMutex.Unlock()

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	// Should return immediately since cleanup is in progress
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_WithEligibleSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)
	// Set TTL to 0 so all segments are eligible
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	// Segments: 0 (truncated, old), 3 (truncated, old), 5 (active - at truncation point)
	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated, CreateTime: 1000}},
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Truncated, CreateTime: 1000}},
		5: {Metadata: &proto.SegmentMetadata{SegNo: 5, State: proto.SegmentState_Active, CreateTime: time.Now().UnixMilli()}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(0)).Return(nil)
	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(3)).Return(nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNumberOfCalls(t, "CleanupSegment", 2)
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_ReadersProtectSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()

	// Reader is still reading segment 2
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{
		{RecentReadSegmentId: 2},
	}, nil)

	// Segments: 0 (truncated, old), 3 (truncated, old)
	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated, CreateTime: 1000}},
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Truncated, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	// Only segment 0 should be cleaned (segment 3 >= reader's minSegmentId=2)
	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(0)).Return(nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNumberOfCalls(t, "CleanupSegment", 1)
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_GetSegmentsError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)
	mockLogHandle.On("GetSegments", mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_CleanupTruncatedSegmentsIfNecessary_GetReadersError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_OnWriterInvalidated(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	assert.True(t, w.isWriterValid.Load())

	// Trigger invalidation
	w.onWriterInvalidated(context.Background(), "test reason")
	assert.False(t, w.isWriterValid.Load())
}

func TestInternalLogWriter_Write_SegmentNotWritable_TriggersInvalidation(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	invalidated := atomic.Bool{}
	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.onWriterInvalidated = func(ctx context.Context, reason string) {
		invalidated.Store(true)
		w.isWriterValid.Store(false)
	}

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	// ErrSegmentHandleSegmentClosed is a "not writable" error
	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(-1, -1, werr.ErrSegmentHandleSegmentClosed)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.Error(t, result.Err)
	assert.True(t, invalidated.Load(), "Writer should be invalidated on segment not writable error")
}

func TestWriteResult_Structure(t *testing.T) {
	t.Run("SuccessResult", func(t *testing.T) {
		result := &WriteResult{
			LogMessageId: &LogMessageId{SegmentId: 1, EntryId: 5},
			Err:          nil,
		}
		assert.NoError(t, result.Err)
		assert.Equal(t, int64(1), result.LogMessageId.SegmentId)
		assert.Equal(t, int64(5), result.LogMessageId.EntryId)
	})

	t.Run("ErrorResult", func(t *testing.T) {
		result := &WriteResult{
			LogMessageId: nil,
			Err:          werr.ErrInternalError,
		}
		assert.Error(t, result.Err)
		assert.Nil(t, result.LogMessageId)
	})
}

// === logWriterImpl (session-based writer) tests ===

// createTestSessionWriter creates a logWriterImpl for testing without goroutines.
func createTestSessionWriter(t *testing.T, logHandle LogHandle, cleanupMgr segment.SegmentCleanupManager, sessionLock *meta.SessionLock) *logWriterImpl {
	cfg := newTestConfig()
	w := &logWriterImpl{
		logIdStr:           "1",
		logHandle:          logHandle,
		auditorMaxInterval: cfg.Woodpecker.Client.Auditor.MaxInterval.Seconds(),
		cfg:                cfg,
		metricsNamespace:   "",
		writerClose:        make(chan struct{}, 1),
		cleanupManager:     cleanupMgr,
		sessionLock:        sessionLock,
	}
	w.onWriterInvalidated = func(ctx context.Context, reason string) {
		if sessionLock != nil {
			sessionLock.MarkInvalid()
		}
	}
	return w
}

func TestLogWriter_Write_NilSessionLock(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestSessionWriter(t, mockLogHandle, nil, nil)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err))
	assert.Nil(t, result.LogMessageId)
}

func TestLogWriter_Write_InvalidSessionLock(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	sessionLock.MarkInvalid()
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err))
}

func TestLogWriter_Write_Success(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(0, 5, nil)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.NotNil(t, result)
	assert.NoError(t, result.Err)
	assert.Equal(t, int64(0), result.LogMessageId.SegmentId)
	assert.Equal(t, int64(5), result.LogMessageId.EntryId)
}

func TestLogWriter_Write_GetWritableSegmentError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	result := w.Write(ctx, msg)
	assert.Error(t, result.Err)
}

func TestLogWriter_Write_InvalidMessage(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	ctx := context.Background()
	msg := &WriteMessage{Payload: nil, Properties: nil}
	result := w.Write(ctx, msg)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrInvalidMessage.Is(result.Err))
}

func TestLogWriter_Write_AppendError_SegmentRolling(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(-1, -1, werr.ErrSegmentHandleSegmentRolling)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{Payload: []byte("test"), Properties: map[string]string{"k": "v"}}
	result := w.Write(ctx, msg)
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrSegmentHandleSegmentRolling.Is(result.Err))
}

func TestLogWriter_Write_SegmentNotWritable_TriggersInvalidation(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(-1, -1, werr.ErrSegmentHandleSegmentClosed)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{Payload: []byte("test"), Properties: map[string]string{"k": "v"}}
	result := w.Write(ctx, msg)
	assert.Error(t, result.Err)
	assert.False(t, sessionLock.IsValid(), "Session lock should be invalidated on segment not writable error")
}

func TestLogWriter_WriteAsync_NilSessionLock(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestSessionWriter(t, mockLogHandle, nil, nil)

	ctx := context.Background()
	msg := &WriteMessage{Payload: []byte("test"), Properties: map[string]string{"k": "v"}}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrLogWriterLockLost.Is(result.Err))
}

func TestLogWriter_WriteAsync_Success(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(0, 3, nil)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{Payload: []byte("async test"), Properties: map[string]string{"k": "v"}}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.NoError(t, result.Err)
	assert.Equal(t, int64(0), result.LogMessageId.SegmentId)
	assert.Equal(t, int64(3), result.LogMessageId.EntryId)
}

func TestLogWriter_WriteAsync_InvalidMessage(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	ctx := context.Background()
	msg := &WriteMessage{Payload: nil, Properties: nil}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
	assert.True(t, werr.ErrInvalidMessage.Is(result.Err))
}

func TestLogWriter_WriteAsync_GetWritableSegmentError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	msg := &WriteMessage{Payload: []byte("test"), Properties: map[string]string{"k": "v"}}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
}

func TestLogWriter_WriteAsync_AppendError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(-1, -1, werr.ErrInternalError)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{Payload: []byte("test"), Properties: map[string]string{"k": "v"}}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
}

func TestLogWriter_Close_Success(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
}

func TestLogWriter_Close_WithErrors(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrInternalError)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.Error(t, err)
}

func TestLogWriter_Close_SegmentNotFoundIgnored(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrSegmentNotFound)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
}

func TestLogWriter_Close_ProcessorNoWriterIgnored(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrSegmentProcessorNoWriter)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
}

func TestLogWriter_Close_MetadataRevisionInvalidIgnored(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(werr.ErrMetadataRevisionInvalid)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.NoError(t, err)
}

func TestLogWriter_Close_ReleaseLockError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(werr.ErrInternalError)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err := w.Close(context.Background())
	assert.Error(t, err)
}

func TestLogWriter_Close_LogHandleCloseError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(werr.ErrInternalError)

	err := w.Close(context.Background())
	assert.Error(t, err)
}

func TestLogWriter_Close_Idempotent(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("CompleteAllActiveSegmentIfExists", mock.Anything).Return(nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().ReleaseLogWriterLock(mock.Anything, "test-log").Return(nil)
	mockLogHandle.On("Close", mock.Anything).Return(nil)

	err1 := w.Close(context.Background())
	assert.NoError(t, err1)
	err2 := w.Close(context.Background())
	assert.NoError(t, err2)
}

func TestLogWriter_CleanupTruncatedSegmentsIfNecessary_WithEligibleSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated, CreateTime: 1000}},
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Truncated, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(0)).Return(nil)
	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(3)).Return(nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNumberOfCalls(t, "CleanupSegment", 2)
}

func TestLogWriter_CleanupTruncatedSegmentsIfNecessary_CleanupError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(0)).Return(werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNumberOfCalls(t, "CleanupSegment", 1)
}

func TestLogWriter_CleanupTruncatedSegmentsIfNecessary_TTLProtection(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 3600 // 1 hour

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	// Segment created recently - should be TTL-protected
	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated, CreateTime: time.Now().UnixMilli()}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	// No segments should be cleaned due to TTL protection
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestLogWriter_CleanupTruncatedSegmentsIfNecessary_CompletionTimeTTL(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 3600

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	// Old create time but recent completion time - should still be TTL-protected
	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{
			SegNo:          0,
			State:          proto.SegmentState_Truncated,
			CreateTime:     1000,
			CompletionTime: time.Now().UnixMilli(),
		}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestLogWriter_CleanupTruncatedSegmentsIfNecessary_SealedTimeTTL(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 3600

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	// Old create/completion time but recent sealed time - should still be TTL-protected
	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{
			SegNo:          0,
			State:          proto.SegmentState_Truncated,
			CreateTime:     1000,
			CompletionTime: 2000,
			SealedTime:     time.Now().UnixMilli(),
		}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_WriteAsync_AppendError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetOrCreateWritableSegmentHandle", mock.Anything, mock.Anything).Return(mockSegHandle, nil)

	// Mock AppendAsync to callback with error
	mockSegHandle.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, bytes []byte, callback func(int64, int64, error)) {
		callback(-1, -1, werr.ErrInternalError)
	}).Return()

	ctx := context.Background()
	msg := &WriteMessage{
		Payload:    []byte("test data"),
		Properties: map[string]string{"k": "v"},
	}
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	assert.Error(t, result.Err)
}

// === runAuditor tests ===

func TestInternalLogWriter_RunAuditor_FullCycle_NoSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 1 // 1 second

	// Mock auditor cycle: CheckAndSetSegmentTruncatedIfNeed succeeds, GetSegments returns empty
	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	mockLogHandle.On("GetSegments", mock.Anything).Return(map[int64]*meta.SegmentMeta{}, nil)

	// Start auditor in goroutine
	go w.runAuditor()

	// Wait for at least one tick
	time.Sleep(1500 * time.Millisecond)

	// Stop auditor
	w.writerClose <- struct{}{}

	// Verify the auditor executed
	mockLogHandle.AssertCalled(t, "CheckAndSetSegmentTruncatedIfNeed", mock.Anything)
	mockLogHandle.AssertCalled(t, "GetSegments", mock.Anything)
}

func TestInternalLogWriter_RunAuditor_CheckTruncatedError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 1

	// CheckAndSetSegmentTruncatedIfNeed fails → should continue (skip rest of cycle)
	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(werr.ErrInternalError)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockLogHandle.AssertCalled(t, "CheckAndSetSegmentTruncatedIfNeed", mock.Anything)
	// GetSegments should NOT be called since CheckAndSetSegmentTruncatedIfNeed failed
	mockLogHandle.AssertNotCalled(t, "GetSegments", mock.Anything)
}

func TestInternalLogWriter_RunAuditor_GetSegmentsError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 1

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	mockLogHandle.On("GetSegments", mock.Anything).Return(nil, werr.ErrInternalError)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockLogHandle.AssertCalled(t, "GetSegments", mock.Anything)
}

func TestInternalLogWriter_RunAuditor_CompactCompletedSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 1

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)

	// Return a completed segment that needs compaction
	segmentMap := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Completed}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)
	mockLogHandle.On("GetRecoverableSegmentHandle", mock.Anything, int64(1)).Return(mockSegHandle, nil)
	mockSegHandle.EXPECT().Compact(mock.Anything).Return(nil)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockLogHandle.AssertCalled(t, "GetRecoverableSegmentHandle", mock.Anything, int64(1))
	mockSegHandle.AssertCalled(t, "Compact", mock.Anything)
}

func TestInternalLogWriter_RunAuditor_CompactError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 1

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	segmentMap := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Completed}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)
	mockLogHandle.On("GetRecoverableSegmentHandle", mock.Anything, int64(1)).Return(mockSegHandle, nil)
	mockSegHandle.EXPECT().Compact(mock.Anything).Return(werr.ErrInternalError)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockSegHandle.AssertCalled(t, "Compact", mock.Anything)
}

func TestInternalLogWriter_RunAuditor_GetRecoverableError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 1

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	segmentMap := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Completed}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)
	mockLogHandle.On("GetRecoverableSegmentHandle", mock.Anything, int64(1)).Return(nil, werr.ErrSegmentNotFound)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockLogHandle.AssertCalled(t, "GetRecoverableSegmentHandle", mock.Anything, int64(1))
}

func TestInternalLogWriter_RunAuditor_TruncatedSegmentsCleanup(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)
	w.auditorMaxInterval = 1
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	// Return truncated segments
	segmentMap := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Truncated, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)
	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)
	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(1)).Return(nil)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	cleanupMgr.AssertCalled(t, "CleanupSegment", mock.Anything, "test-log", int64(1), int64(1))
}

func TestLogWriter_RunAuditor_FullCycle(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.auditorMaxInterval = 1

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	mockLogHandle.On("GetSegments", mock.Anything).Return(map[int64]*meta.SegmentMeta{}, nil)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockLogHandle.AssertCalled(t, "CheckAndSetSegmentTruncatedIfNeed", mock.Anything)
	mockLogHandle.AssertCalled(t, "GetSegments", mock.Anything)
}

func TestLogWriter_RunAuditor_WithCompletedAndTruncated(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.auditorMaxInterval = 1
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("CheckAndSetSegmentTruncatedIfNeed", mock.Anything).Return(nil)
	segmentMap := map[int64]*meta.SegmentMeta{
		1: {Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Completed}},
		2: {Metadata: &proto.SegmentMetadata{SegNo: 2, State: proto.SegmentState_Truncated, CreateTime: 1000}},
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Active}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)
	mockLogHandle.On("GetRecoverableSegmentHandle", mock.Anything, int64(1)).Return(mockSegHandle, nil)
	mockSegHandle.EXPECT().Compact(mock.Anything).Return(nil)
	// Truncated segment cleanup
	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)
	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(2)).Return(nil)

	go w.runAuditor()
	time.Sleep(1500 * time.Millisecond)
	w.writerClose <- struct{}{}

	mockSegHandle.AssertCalled(t, "Compact", mock.Anything)
	cleanupMgr.AssertCalled(t, "CleanupSegment", mock.Anything, "test-log", int64(1), int64(2))
}

// === cleanupTruncatedSegmentsIfNecessary remaining paths ===

func TestLogWriter_CleanupTruncatedSegments_AlreadyInProgress(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	// Mark cleanup as already in progress
	w.cleanupMutex.Lock()
	w.cleanupInProgress = true
	w.cleanupMutex.Unlock()

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	// Should return early without calling GetTruncatedRecordId
	mockLogHandle.AssertNotCalled(t, "GetTruncatedRecordId", mock.Anything)
}

func TestLogWriter_CleanupTruncatedSegments_GetTruncatedRecordIdError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
}

func TestLogWriter_CleanupTruncatedSegments_NoTruncationPoint(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	// Truncation point not set (negative values)
	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: -1, EntryId: -1}, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	// Should return early without calling GetMetadataProvider
	mockLogHandle.AssertNotCalled(t, "GetMetadataProvider")
}

func TestLogWriter_CleanupTruncatedSegments_GetReadersError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestLogWriter_CleanupTruncatedSegments_GetSegmentsError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)
	mockLogHandle.On("GetSegments", mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestLogWriter_CleanupTruncatedSegments_ReadersProtectSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, cleanupMgr, sessionLock)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()

	// Reader is reading segment 2 → protects segments >= 2
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{
		{RecentReadSegmentId: 2},
	}, nil)

	segmentMap := map[int64]*meta.SegmentMeta{
		0: {Metadata: &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Truncated, CreateTime: 1000}},
		3: {Metadata: &proto.SegmentMetadata{SegNo: 3, State: proto.SegmentState_Truncated, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	// Only segment 0 should be cleaned (segment 3 >= reader's minSegmentId=2)
	cleanupMgr.On("CleanupSegment", mock.Anything, "test-log", int64(1), int64(0)).Return(nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNumberOfCalls(t, "CleanupSegment", 1)
}

func TestInternalLogWriter_CleanupTruncatedSegments_AlreadyInProgress(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	w.cleanupMutex.Lock()
	w.cleanupInProgress = true
	w.cleanupMutex.Unlock()

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	mockLogHandle.AssertNotCalled(t, "GetTruncatedRecordId", mock.Anything)
}

func TestInternalLogWriter_CleanupTruncatedSegments_GetTruncatedRecordIdError(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(nil, werr.ErrInternalError)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
}

func TestInternalLogWriter_CleanupTruncatedSegments_NoTruncationPoint(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: -1, EntryId: -1}, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)
	mockLogHandle.AssertNotCalled(t, "GetMetadataProvider")
}

func TestInternalLogWriter_CleanupTruncatedSegments_NoEligibleSegments(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	// Only active/sealed segments, no truncated segments eligible
	segmentMap := map[int64]*meta.SegmentMeta{
		5: {Metadata: &proto.SegmentMetadata{SegNo: 5, State: proto.SegmentState_Active, CreateTime: 1000}},
		6: {Metadata: &proto.SegmentMetadata{SegNo: 6, State: proto.SegmentState_Sealed, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

func TestInternalLogWriter_CleanupTruncatedSegments_SegmentAtTruncationPoint(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	cleanupMgr := &mockCleanupManager{}
	cleanupMgr.On("CleanupOrphanedStatuses", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, cleanupMgr)
	w.cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 0

	mockLogHandle.On("GetTruncatedRecordId", mock.Anything).Return(&LogMessageId{SegmentId: 5, EntryId: 10}, nil)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetadata).Maybe()
	mockMetadata.EXPECT().GetAllReaderTempInfoForLog(mock.Anything, int64(1)).Return([]*proto.ReaderTempInfo{}, nil)

	// Segment at truncation point (segId=5 >= minTruncatedSegmentId=5) should NOT be cleaned
	segmentMap := map[int64]*meta.SegmentMeta{
		5: {Metadata: &proto.SegmentMetadata{SegNo: 5, State: proto.SegmentState_Truncated, CreateTime: 1000}},
	}
	mockLogHandle.On("GetSegments", mock.Anything).Return(segmentMap, nil)

	ctx := context.Background()
	w.cleanupTruncatedSegmentsIfNecessary(ctx)

	cleanupMgr.AssertNotCalled(t, "CleanupSegment")
}

// === GetWriterSessionForTest ===

func TestLogWriter_GetWriterSessionForTest(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)

	session := w.GetWriterSessionForTest()
	assert.Nil(t, session) // nil session from test lock
}

// === RunAuditor writerClose path ===

func TestInternalLogWriter_RunAuditor_StopsOnWriterClose(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestInternalWriter(t, mockLogHandle, nil)
	w.auditorMaxInterval = 60 // Long interval so ticker won't fire

	done := make(chan struct{})
	go func() {
		w.runAuditor()
		close(done)
	}()

	// Immediately close writer
	w.writerClose <- struct{}{}

	select {
	case <-done:
		// Success - auditor stopped
	case <-time.After(3 * time.Second):
		t.Fatal("runAuditor did not stop within timeout")
	}
}

func TestLogWriter_RunAuditor_StopsOnWriterClose(t *testing.T) {
	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	sessionLock := meta.NewSessionLockForTest(nil)
	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.auditorMaxInterval = 60

	done := make(chan struct{})
	go func() {
		w.runAuditor()
		close(done)
	}()

	w.writerClose <- struct{}{}

	select {
	case <-done:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("runAuditor did not stop within timeout")
	}
}

// === monitorSession tests ===

// newTestSession creates a concurrency.Session with a controllable Done channel for testing.
// Uses unsafe to set the unexported donec field since Session is a concrete struct.
func newTestSession(donec <-chan struct{}) *concurrency.Session {
	s := new(concurrency.Session)
	sType := reflect.TypeOf(*s)
	f, _ := sType.FieldByName("donec")
	*(*<-chan struct{})(unsafe.Add(unsafe.Pointer(s), f.Offset)) = donec
	return s
}

func TestMonitorSession_WriterClose(t *testing.T) {
	// Session with never-closing Done channel
	session := newTestSession(make(chan struct{}))
	sessionLock := meta.NewSessionLockForTest(session)

	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.cfg.Woodpecker.Client.SessionMonitor.CheckInterval = config.NewDurationSecondsFromInt(100)

	done := make(chan struct{})
	go func() {
		w.monitorSession()
		close(done)
	}()

	w.writerClose <- struct{}{}

	select {
	case <-done:
		// monitorSession exited
	case <-time.After(5 * time.Second):
		t.Fatal("monitorSession did not exit after writerClose signal")
	}

	assert.False(t, sessionLock.IsValid())
}

func TestMonitorSession_SessionDone(t *testing.T) {
	donec := make(chan struct{})
	session := newTestSession(donec)
	sessionLock := meta.NewSessionLockForTest(session)

	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.cfg.Woodpecker.Client.SessionMonitor.CheckInterval = config.NewDurationSecondsFromInt(100)

	done := make(chan struct{})
	go func() {
		w.monitorSession()
		close(done)
	}()

	// Close the session's Done channel to simulate session expiration
	close(donec)

	select {
	case <-done:
		// monitorSession exited
	case <-time.After(5 * time.Second):
		t.Fatal("monitorSession did not exit after session Done")
	}

	assert.False(t, sessionLock.IsValid())
}

func TestMonitorSession_CheckAlive_LeaseExpired(t *testing.T) {
	session := newTestSession(make(chan struct{}))
	sessionLock := meta.NewSessionLockForTest(session)

	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetaProvider := mocks_meta.NewMetadataProvider(t)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetaProvider).Maybe()

	// Lease is expired: alive=false, err=nil
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).Return(false, nil)

	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.cfg.Woodpecker.Client.SessionMonitor.CheckInterval = config.NewDurationSecondsFromInt(1)

	done := make(chan struct{})
	go func() {
		w.monitorSession()
		close(done)
	}()

	select {
	case <-done:
		// monitorSession exited due to lease expired
	case <-time.After(5 * time.Second):
		t.Fatal("monitorSession did not exit after lease expired")
	}

	assert.False(t, sessionLock.IsValid())
}

func TestMonitorSession_CheckAlive_ConsecutiveFailures(t *testing.T) {
	session := newTestSession(make(chan struct{}))
	sessionLock := meta.NewSessionLockForTest(session)

	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetaProvider := mocks_meta.NewMetadataProvider(t)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetaProvider).Maybe()

	// Always return error
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).Return(false, fmt.Errorf("etcd unreachable"))

	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.cfg.Woodpecker.Client.SessionMonitor.CheckInterval = config.NewDurationSecondsFromInt(1)
	w.cfg.Woodpecker.Client.SessionMonitor.MaxFailures = 2

	done := make(chan struct{})
	go func() {
		w.monitorSession()
		close(done)
	}()

	// Should exit after 2 consecutive failures (~2s)
	select {
	case <-done:
		// monitorSession exited after consecutive failures
	case <-time.After(10 * time.Second):
		t.Fatal("monitorSession did not exit after consecutive failures")
	}

	assert.False(t, sessionLock.IsValid())
}

func TestMonitorSession_CheckAlive_FailureResetOnSuccess(t *testing.T) {
	session := newTestSession(make(chan struct{}))
	sessionLock := meta.NewSessionLockForTest(session)

	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetaProvider := mocks_meta.NewMetadataProvider(t)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetaProvider).Maybe()

	// Call 1: fail (consecutiveFailures=1)
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).Return(false, fmt.Errorf("etcd unreachable")).Once()
	// Call 2: success (consecutiveFailures resets to 0)
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).Return(true, nil).Once()
	// Call 3: fail (consecutiveFailures=1)
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).Return(false, fmt.Errorf("etcd unreachable")).Once()
	// Call 4: fail (consecutiveFailures=2 >= maxFailures=2, exits)
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).Return(false, fmt.Errorf("etcd unreachable")).Once()

	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.cfg.Woodpecker.Client.SessionMonitor.CheckInterval = config.NewDurationSecondsFromInt(1)
	w.cfg.Woodpecker.Client.SessionMonitor.MaxFailures = 2

	done := make(chan struct{})
	go func() {
		w.monitorSession()
		close(done)
	}()

	// Should exit after 4 ticks (~4s): fail, success(reset), fail, fail(exit)
	select {
	case <-done:
		// monitorSession exited
	case <-time.After(10 * time.Second):
		t.Fatal("monitorSession did not exit")
	}

	assert.False(t, sessionLock.IsValid())
	mockMetaProvider.AssertNumberOfCalls(t, "CheckSessionLockAlive", 4)
}

func TestMonitorSession_CheckAlive_Success(t *testing.T) {
	session := newTestSession(make(chan struct{}))
	sessionLock := meta.NewSessionLockForTest(session)

	mockLogHandle := &testLogHandleMock{}
	mockLogHandle.Test(t)
	mockLogHandle.On("GetName").Return("test-log").Maybe()
	mockLogHandle.On("GetId").Return(int64(1)).Maybe()

	mockMetaProvider := mocks_meta.NewMetadataProvider(t)
	mockLogHandle.On("GetMetadataProvider").Return(mockMetaProvider).Maybe()

	// Track that at least one check was performed
	checked := make(chan struct{}, 1)
	mockMetaProvider.On("CheckSessionLockAlive", mock.Anything, sessionLock).
		Return(true, nil).
		Run(func(args mock.Arguments) {
			select {
			case checked <- struct{}{}:
			default:
			}
		})

	w := createTestSessionWriter(t, mockLogHandle, nil, sessionLock)
	w.cfg.Woodpecker.Client.SessionMonitor.CheckInterval = config.NewDurationSecondsFromInt(1)

	done := make(chan struct{})
	go func() {
		w.monitorSession()
		close(done)
	}()

	// Wait for at least one check to complete
	select {
	case <-checked:
	case <-time.After(5 * time.Second):
		t.Fatal("no check was performed")
	}

	// Session should still be valid
	assert.True(t, sessionLock.IsValid())

	// Stop the monitor via writerClose
	w.writerClose <- struct{}{}

	select {
	case <-done:
		// monitorSession exited
	case <-time.After(5 * time.Second):
		t.Fatal("monitorSession did not exit after writerClose")
	}
}
