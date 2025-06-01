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

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_log_handle"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// NOTE: not integration test, file move here because cycle deps if in woodpecker/log pkg.TODO move to woodpecker/log/log_reader_test.go

// One active segment#0 with entries 0,1
func TestActiveSegmentRead(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	// mock metadata
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	//mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, 0).Return(true, nil)
	mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(1)).Return(false, nil)
	// mock logHandle
	mockLogHandle := mocks_log_handle.NewLogHandle(t)
	mockLogHandle.EXPECT().GetMetadataProvider().Return(mockMetadata)
	//mockLogHandle.EXPECT().GetNextSegmentId().Return(int64(1), nil) // next new segmentId is #1
	mockLogHandle.EXPECT().GetName().Return("test_log")
	mockLogHandle.EXPECT().GetId().Return(1)
	// mock segmentHandle
	mockSegmentHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle.EXPECT().GetId(mock.Anything).Return(int64(0)) // segment#0
	mockSegmentHandle.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		State:       proto.SegmentState_Active,
		LastEntryId: int64(-1), // it is active, not completed
	})
	mockSegmentHandle.EXPECT().RefreshAndGetMetadata(mock.Anything).Return(nil)
	msg0 := &log.WriterMessage{
		Payload:    []byte("test0"),
		Properties: make(map[string]string),
	}
	msg0data, _ := log.MarshalMessage(msg0)
	mockSegmentHandle.EXPECT().ReadBatch(mock.Anything, int64(0) /*from*/, int64(-1)). /*autoBatchSize*/ Return([]*processor.SegmentEntry{
		{
			SegmentId: 0,
			EntryId:   0, // segment#0 has entries 0
			Data:      msg0data,
		},
	}, nil)
	msg1 := &log.WriterMessage{
		Payload:    []byte("test1"),
		Properties: make(map[string]string),
	}
	msg1data, _ := log.MarshalMessage(msg1)
	mockSegmentHandle.EXPECT().ReadBatch(mock.Anything, int64(1) /*from*/, int64(-1) /*autoBatchSize*/).Return([]*processor.SegmentEntry{
		{
			SegmentId: 0,
			EntryId:   1, // segment#0 has entries 0
			Data:      msg1data,
		},
	}, nil)
	mockSegmentHandle.EXPECT().ReadBatch(mock.Anything, int64(2) /*from*/, int64(-1) /*autoBatchSize*/).Return(nil, werr.ErrEntryNotFound)

	// Test LogReader read entries 0,1 from segment#0
	ctx := context.Background()
	earliest := log.EarliestLogMessageID()
	logReader, newReaderErr := log.NewLogBatchReader(ctx, mockLogHandle, mockSegmentHandle, &earliest, "TestActiveSegmentRead", cfg)
	assert.NoError(t, newReaderErr)
	msg, readErr := logReader.ReadNext(ctx) // read entry#0
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(0))
	assert.Equal(t, msg.Id.EntryId, int64(0))
	msg, readErr = logReader.ReadNext(ctx) // read entry#1
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(0))
	assert.Equal(t, msg.Id.EntryId, int64(1))

	// no more data to read, block until timeout
	more := false
	go func() {
		m, e := logReader.ReadNext(ctx)
		if e == nil {
			more = true
			logger.Ctx(ctx).Error(fmt.Sprintf("ReadNext should not block, but read  %v", m))
		}
	}()
	time.Sleep(1 * time.Second)
	assert.False(t, more)
}

// empty in segment#0, state=active , means in abnormal state because seg#1 exists
// entries 0,1 in segment#1, state=active, means it is in-progress because seg#2 does not exists
func TestSegmentInExceptionState(t *testing.T) {
	// mock metadata
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	//mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, 0).Return(true, nil)
	mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(1)).Return(true, nil)
	mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(2)).Return(false, nil)
	// mock logHandle
	mockLogHandle := mocks_log_handle.NewLogHandle(t)
	mockLogHandle.EXPECT().GetMetadataProvider().Return(mockMetadata)
	mockLogHandle.EXPECT().GetNextSegmentId().Return(int64(2), nil) // next new segmentId is #2
	mockLogHandle.EXPECT().GetName().Return("test_log")
	mockLogHandle.EXPECT().GetId().Return(1)
	// mock segmentHandle#0 , no entries
	mockSegmentHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle.EXPECT().GetId(mock.Anything).Return(int64(0)) // segment#0
	mockSegmentHandle.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		SegNo:       int64(0),
		State:       proto.SegmentState_Active,
		LastEntryId: int64(-1), // it is active, not completed
	})
	mockSegmentHandle.EXPECT().RefreshAndGetMetadata(mock.Anything).Return(nil)
	mockSegmentHandle.EXPECT().ReadBatch(mock.Anything, int64(0) /*from*/, int64(-1) /*autoBatchSize*/).Return(nil, werr.ErrEntryNotFound)
	// mock segmentHandle#1 , entries 0,1 in it
	mockSegmentHandle1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle1.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		SegNo:       int64(1),
		State:       proto.SegmentState_Active,
		LastEntryId: int64(-1), // it is active, not completed
	})
	mockSegmentHandle1.EXPECT().GetId(mock.Anything).Return(1)
	msg0 := &log.WriterMessage{
		Payload:    []byte("test0"),
		Properties: make(map[string]string),
	}
	mockSegmentHandle1.EXPECT().RefreshAndGetMetadata(mock.Anything).Return(nil)
	msg0data, _ := log.MarshalMessage(msg0)
	mockSegmentHandle1.EXPECT().ReadBatch(mock.Anything, int64(0) /*from*/, int64(-1)). /*autoBatchSize*/ Return([]*processor.SegmentEntry{
		{
			SegmentId: 1,
			EntryId:   0, // segment#0 has entries 0
			Data:      msg0data,
		},
	}, nil)
	msg1 := &log.WriterMessage{
		Payload:    []byte("test1"),
		Properties: make(map[string]string),
	}
	msg1data, _ := log.MarshalMessage(msg1)
	mockSegmentHandle1.EXPECT().ReadBatch(mock.Anything, int64(1) /*from*/, int64(-1) /*autoBatchSize*/).Return([]*processor.SegmentEntry{
		{
			SegmentId: 1,
			EntryId:   1, // segment#0 has entries 0
			Data:      msg1data,
		},
	}, nil)
	mockSegmentHandle1.EXPECT().ReadBatch(mock.Anything, int64(2) /*from*/, int64(-1) /*autoBatchSize*/).Return(nil, werr.ErrEntryNotFound)
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(1)).Return(mockSegmentHandle1, nil)

	// Test LogReader read entries 0,1 from segment#1, bypass segment#0
	ctx := context.Background()
	earliest := log.EarliestLogMessageID()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	logReader, newReaderErr := log.NewLogBatchReader(ctx, mockLogHandle, mockSegmentHandle, &earliest, "TestSegmentInExceptionState", cfg)
	assert.NoError(t, newReaderErr)

	msg, readErr := logReader.ReadNext(ctx) // read entry#0
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(1))
	assert.Equal(t, msg.Id.EntryId, int64(0))
	msg, readErr = logReader.ReadNext(ctx) // read entry#1
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(1))
	assert.Equal(t, msg.Id.EntryId, int64(1))

	// no more data to read, block until timeout
	more := false
	go func() {
		m, e := logReader.ReadNext(ctx)
		if e == nil {
			more = true
			logger.Ctx(ctx).Error(fmt.Sprintf("ReadNext should not block, but read  %v", m))
		}
	}()
	time.Sleep(1 * time.Second)
	assert.False(t, more)
}

// read from seg#0,entry#0
// but no segment #0,#1
// and entries 0,1 in segment#2, seg state=active
func TestReadFromEarlyNotExistsPoint(t *testing.T) {
	// mock metadata
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	//mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, 0).Return(false, nil)
	//mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(1)).Return(false, nil)
	//mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(2)).Return(true, nil)
	mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(3)).Return(false, nil)
	mockMetadata.EXPECT().UpdateReaderTempInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// mock logHandle
	mockLogHandle := mocks_log_handle.NewLogHandle(t)
	mockLogHandle.EXPECT().GetMetadataProvider().Return(mockMetadata)
	mockLogHandle.EXPECT().GetNextSegmentId().Return(int64(3), nil) // next new segmentId is #3
	mockLogHandle.EXPECT().GetName().Return("test_log")
	mockLogHandle.EXPECT().GetId().Return(int64(1))
	// mock segment #0, segment does not exists
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(0)).Return(nil, werr.ErrSegmentNotFound.WithCauseErrMsg("seg 0 not found"))
	// mock segment #1, segment does not exists
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(1)).Return(nil, werr.ErrSegmentNotFound.WithCauseErrMsg("seg 1 not found"))
	// mock segment #2 , entries 0,1 in it
	mockSegmentHandle2 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle2.EXPECT().GetId(mock.Anything).Return(int64(0)) // segment#0
	mockSegmentHandle2.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		State:       proto.SegmentState_Active,
		LastEntryId: int64(-1), // it is active, not completed
	})
	mockSegmentHandle2.EXPECT().RefreshAndGetMetadata(mock.Anything).Return(nil)
	msg0 := &log.WriterMessage{
		Payload:    []byte("test0"),
		Properties: make(map[string]string),
	}
	msg0data, _ := log.MarshalMessage(msg0)
	mockSegmentHandle2.EXPECT().ReadBatch(mock.Anything, int64(0) /*from*/, int64(-1)). /*autoBatchSize*/ Return([]*processor.SegmentEntry{
		{
			SegmentId: 2,
			EntryId:   0, // segment#0 has entries 0
			Data:      msg0data,
		},
	}, nil)
	msg1 := &log.WriterMessage{
		Payload:    []byte("test1"),
		Properties: make(map[string]string),
	}
	msg1data, _ := log.MarshalMessage(msg1)
	mockSegmentHandle2.EXPECT().ReadBatch(mock.Anything, int64(1) /*from*/, int64(-1) /*autoBatchSize*/).Return([]*processor.SegmentEntry{
		{
			SegmentId: 2,
			EntryId:   1, // segment#0 has entries 0
			Data:      msg1data,
		},
	}, nil)
	mockSegmentHandle2.EXPECT().ReadBatch(mock.Anything, int64(2) /*from*/, int64(-1) /*autoBatchSize*/).Return(nil, werr.ErrEntryNotFound)
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(2)).Return(mockSegmentHandle2, nil)

	// Test LogReader read entries 0,1 from segment#2
	ctx := context.Background()
	earliest := log.EarliestLogMessageID()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	logReader, newReaderErr := log.NewLogBatchReader(ctx, mockLogHandle, nil, &earliest, "TestReadFromEarlyNotExistsPoint", cfg)
	assert.NoError(t, newReaderErr)
	msg, readErr := logReader.ReadNext(ctx) // read entry#0
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(2))
	assert.Equal(t, msg.Id.EntryId, int64(0))
	msg, readErr = logReader.ReadNext(ctx) // read entry#1
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(2))
	assert.Equal(t, msg.Id.EntryId, int64(1))

	// no more data to read, block until timeout
	more := false
	go func() {
		m, e := logReader.ReadNext(ctx)
		if e == nil {
			more = true
			logger.Ctx(ctx).Error(fmt.Sprintf("ReadNext should not block, but read  %v", m))
		}
	}()
	time.Sleep(1 * time.Second)
	assert.False(t, more)
}

// read from seg#0,entry#0
// but no segment #0,#1
// and entries 0,1 in segment#2, seg state=active
func TestReadFromSeekPoint(t *testing.T) {
	// mock metadata
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(3)).Return(false, nil)
	// mock logHandle
	mockLogHandle := mocks_log_handle.NewLogHandle(t)
	mockLogHandle.EXPECT().GetMetadataProvider().Return(mockMetadata)
	mockLogHandle.EXPECT().GetNextSegmentId().Return(int64(3), nil) // next new segmentId is #3
	mockLogHandle.EXPECT().GetName().Return("test_log")
	mockLogHandle.EXPECT().GetId().Return(1)
	// mock segment #2
	mockSegmentHandle2 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle2.EXPECT().GetId(mock.Anything).Return(int64(0)) // segment#0
	mockSegmentHandle2.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		State:       proto.SegmentState_Active,
		LastEntryId: int64(-1), // it is active, not completed
	})
	mockSegmentHandle2.EXPECT().RefreshAndGetMetadata(mock.Anything).Return(nil)
	msg1 := &log.WriterMessage{
		Payload:    []byte("test1"),
		Properties: make(map[string]string),
	}
	msg1data, _ := log.MarshalMessage(msg1)
	mockSegmentHandle2.EXPECT().ReadBatch(mock.Anything, int64(1) /*from*/, int64(-1) /*autoBatchSize*/).Return([]*processor.SegmentEntry{
		{
			SegmentId: 2,
			EntryId:   1, // segment#0 has entries 0
			Data:      msg1data,
		},
	}, nil)
	mockSegmentHandle2.EXPECT().ReadBatch(mock.Anything, int64(2) /*from*/, int64(-1) /*autoBatchSize*/).Return(nil, werr.ErrEntryNotFound)
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(2)).Return(mockSegmentHandle2, nil)

	// Test LogReader read entries 1 from segment#2
	ctx := context.Background()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	logReader, newReaderErr := log.NewLogBatchReader(ctx, mockLogHandle, nil, &log.LogMessageId{
		SegmentId: int64(2),
		EntryId:   int64(1),
	}, "TestReadFromSeekPoint", cfg)
	assert.NoError(t, newReaderErr)
	msg, readErr := logReader.ReadNext(ctx) // read entry#1
	assert.NoError(t, readErr)
	assert.Equal(t, msg.Id.SegmentId, int64(2))
	assert.Equal(t, msg.Id.EntryId, int64(1))

	// no more data to read, block until timeout
	more := false
	go func() {
		m, e := logReader.ReadNext(ctx)
		if e == nil {
			more = true
			logger.Ctx(ctx).Error(fmt.Sprintf("ReadNext should not block, but read  %v", m))
		}
	}()
	time.Sleep(1 * time.Second)
	assert.False(t, more)
}

// entries 0,1 in segment #0
// no data in segment #1, segment #1 state=completed
func TestReadFromLatestWhenLatestIsCompleted(t *testing.T) {
	// mock logHandle
	mockLogHandle := mocks_log_handle.NewLogHandle(t)
	mockLogHandle.EXPECT().GetNextSegmentId().Return(int64(2), nil) // next new segmentId is #2
	mockLogHandle.EXPECT().GetName().Return("test_log")
	mockLogHandle.EXPECT().GetId().Return(1)

	// mock segment #1, no data, state=completed
	mockSegmentHandle1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle1.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		State:       proto.SegmentState_Completed,
		LastEntryId: int64(1), // completed, no data
	})
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(1)).Return(mockSegmentHandle1, nil)

	// Test LogReader read latest should block
	ctx := context.Background()
	latest := log.LatestLogMessageID()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	logReader, newReaderErr := log.NewLogBatchReader(ctx, mockLogHandle, nil, &latest, "TestReadFromLatestWhenLatestIsCompleted", cfg)
	assert.NoError(t, newReaderErr)
	// no data to read, block until timeout
	more := false
	go func() {
		m, e := logReader.ReadNext(ctx)
		if e == nil {
			more = true
			logger.Ctx(ctx).Error(fmt.Sprintf("ReadNext should not block, but read  %v", m))
		}
	}()
	time.Sleep(2 * time.Second)
	assert.False(t, more)
}

// entries 0,1 in segment #0
// no data in segment #1, segment #1 state=active
func TestReadFromLatestWhenLatestIsActive(t *testing.T) {
	// mock metadata
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().CheckSegmentExists(mock.Anything, mock.Anything, int64(2)).Return(false, nil)
	// mock logHandle
	mockLogHandle := mocks_log_handle.NewLogHandle(t)
	mockLogHandle.EXPECT().GetMetadataProvider().Return(mockMetadata)
	mockLogHandle.EXPECT().GetNextSegmentId().Return(int64(2), nil) // next new segmentId is #2
	mockLogHandle.EXPECT().GetName().Return("test_log")
	mockLogHandle.EXPECT().GetId().Return(1)

	// mock segment #1, no data, state=completed
	mockSegmentHandle1 := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle1.EXPECT().GetMetadata(mock.Anything).Return(&proto.SegmentMetadata{
		State:       proto.SegmentState_Active,
		LastEntryId: int64(-1), // active
	})
	mockSegmentHandle1.EXPECT().GetId(mock.Anything).Return(int64(1)) // segment#0
	mockSegmentHandle1.EXPECT().RefreshAndGetMetadata(mock.Anything).Return(nil)
	mockSegmentHandle1.EXPECT().ReadBatch(mock.Anything, int64(0) /*from*/, int64(-1)). /*autoBatchSize*/ Return(nil, werr.ErrEntryNotFound)
	mockSegmentHandle1.EXPECT().GetLastAddConfirmed(mock.Anything).Return(-1, nil)
	mockLogHandle.EXPECT().GetExistsReadonlySegmentHandle(mock.Anything, int64(1)).Return(mockSegmentHandle1, nil)

	// Test LogReader read latest should block
	ctx := context.Background()
	latest := log.LatestLogMessageID()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	logReader, newReaderErr := log.NewLogBatchReader(ctx, mockLogHandle, nil, &latest, "TestReadFromLatestWhenLatestIsActive", cfg)
	assert.NoError(t, newReaderErr)
	// no data to read, block until timeout
	more := false
	go func() {
		m, e := logReader.ReadNext(ctx)
		if e == nil {
			more = true
			logger.Ctx(ctx).Error(fmt.Sprintf("ReadNext should not block, but read  %v", m))
		}
	}()
	time.Sleep(2 * time.Second)
	assert.False(t, more)
}
