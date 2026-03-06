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

package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/channel"
	mocks_server "github.com/zilliztech/woodpecker/mocks/mocks_server"
	"github.com/zilliztech/woodpecker/proto"
)

func newLocalClientWithMock(t *testing.T) (*logStoreClientLocal, *mocks_server.LogStore) {
	mockStore := mocks_server.NewLogStore(t)
	client := &logStoreClientLocal{store: mockStore}
	return client, mockStore
}

func TestLocalClient_IsRemoteClient(t *testing.T) {
	client, _ := newLocalClientWithMock(t)
	assert.False(t, client.IsRemoteClient())
}

func TestLocalClient_Close(t *testing.T) {
	client, _ := newLocalClientWithMock(t)
	err := client.Close(context.Background())
	assert.NoError(t, err)
}

func TestLocalClient_AppendEntry_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{
		SegId:   1,
		EntryId: 10,
		Values:  []byte("test-data"),
	}
	resultCh := channel.NewLocalResultChannel("test-ch")

	mockStore.EXPECT().AddEntry(mock.Anything, "bucket", "root", int64(1), entry, resultCh).
		Return(int64(10), nil)

	entryId, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), entryId)
}

func TestLocalClient_AppendEntry_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{
		SegId:   1,
		EntryId: 10,
		Values:  []byte("test-data"),
	}
	resultCh := channel.NewLocalResultChannel("test-ch")
	expectedErr := fmt.Errorf("append failed")

	mockStore.EXPECT().AddEntry(mock.Anything, "bucket", "root", int64(1), entry, resultCh).
		Return(int64(-1), expectedErr)

	entryId, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), entryId)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_ReadEntriesBatchAdv_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{
			{SegId: 1, EntryId: 0, Values: []byte("entry-0")},
			{SegId: 1, EntryId: 1, Values: []byte("entry-1")},
		},
	}
	lastReadState := &proto.LastReadState{}

	mockStore.EXPECT().GetBatchEntriesAdv(mock.Anything, "bucket", "root", int64(1), int64(0), int64(0), int64(100), lastReadState).
		Return(expectedResult, nil)

	result, err := client.ReadEntriesBatchAdv(ctx, "bucket", "root", 1, 0, 0, 100, lastReadState)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
	assert.Len(t, result.Entries, 2)
}

func TestLocalClient_ReadEntriesBatchAdv_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("read failed")
	lastReadState := &proto.LastReadState{}

	mockStore.EXPECT().GetBatchEntriesAdv(mock.Anything, "bucket", "root", int64(1), int64(0), int64(0), int64(100), lastReadState).
		Return(nil, expectedErr)

	result, err := client.ReadEntriesBatchAdv(ctx, "bucket", "root", 1, 0, 0, 100, lastReadState)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_ReadEntriesBatchAdv_NilLastReadState(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{
			{SegId: 1, EntryId: 5, Values: []byte("data")},
		},
	}

	mockStore.EXPECT().GetBatchEntriesAdv(mock.Anything, "bucket", "root", int64(1), int64(0), int64(5), int64(50), (*proto.LastReadState)(nil)).
		Return(expectedResult, nil)

	result, err := client.ReadEntriesBatchAdv(ctx, "bucket", "root", 1, 0, 5, 50, nil)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func TestLocalClient_CompleteSegment_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	mockStore.EXPECT().CompleteSegment(mock.Anything, "bucket", "root", int64(1), int64(0), int64(99)).
		Return(int64(99), nil)

	lastEntryId, err := client.CompleteSegment(ctx, "bucket", "root", 1, 0, 99)
	assert.NoError(t, err)
	assert.Equal(t, int64(99), lastEntryId)
}

func TestLocalClient_CompleteSegment_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("complete failed")

	mockStore.EXPECT().CompleteSegment(mock.Anything, "bucket", "root", int64(1), int64(0), int64(99)).
		Return(int64(-1), expectedErr)

	lastEntryId, err := client.CompleteSegment(ctx, "bucket", "root", 1, 0, 99)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lastEntryId)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_FenceSegment_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	mockStore.EXPECT().FenceSegment(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(50), nil)

	lastEntryId, err := client.FenceSegment(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), lastEntryId)
}

func TestLocalClient_FenceSegment_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("fence failed")

	mockStore.EXPECT().FenceSegment(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(-1), expectedErr)

	lastEntryId, err := client.FenceSegment(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lastEntryId)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_GetLastAddConfirmed_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	mockStore.EXPECT().GetSegmentLastAddConfirmed(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(42), nil)

	lac, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), lac)
}

func TestLocalClient_GetLastAddConfirmed_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("get lac failed")

	mockStore.EXPECT().GetSegmentLastAddConfirmed(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(-1), expectedErr)

	lac, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), lac)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_GetBlockCount_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	mockStore.EXPECT().GetSegmentBlockCount(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(5), nil)

	count, err := client.GetBlockCount(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestLocalClient_GetBlockCount_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("get block count failed")

	mockStore.EXPECT().GetSegmentBlockCount(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(-1), expectedErr)

	count, err := client.GetBlockCount(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), count)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_SegmentCompact_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedMeta := &proto.SegmentMetadata{
		SegNo: 0,
		State: proto.SegmentState_Sealed,
	}

	mockStore.EXPECT().CompactSegment(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(expectedMeta, nil)

	meta, err := client.SegmentCompact(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, meta)
	assert.Equal(t, int64(0), meta.SegNo)
}

func TestLocalClient_SegmentCompact_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("compact failed")

	mockStore.EXPECT().CompactSegment(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(nil, expectedErr)

	meta, err := client.SegmentCompact(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_SegmentClean_Success(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	mockStore.EXPECT().CleanSegment(mock.Anything, "bucket", "root", int64(1), int64(0), 1).
		Return(nil)

	err := client.SegmentClean(ctx, "bucket", "root", 1, 0, 1)
	assert.NoError(t, err)
}

func TestLocalClient_SegmentClean_Error(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	expectedErr := fmt.Errorf("clean failed")

	mockStore.EXPECT().CleanSegment(mock.Anything, "bucket", "root", int64(1), int64(0), 1).
		Return(expectedErr)

	err := client.SegmentClean(ctx, "bucket", "root", 1, 0, 1)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestLocalClient_SegmentClean_DifferentFlags(t *testing.T) {
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	// Test with flag 0
	mockStore.EXPECT().CleanSegment(mock.Anything, "bucket", "root", int64(1), int64(0), 0).
		Return(nil)
	err := client.SegmentClean(ctx, "bucket", "root", 1, 0, 0)
	assert.NoError(t, err)

	// Test with flag 2
	mockStore.EXPECT().CleanSegment(mock.Anything, "bucket", "root", int64(1), int64(0), 2).
		Return(nil)
	err = client.SegmentClean(ctx, "bucket", "root", 1, 0, 2)
	assert.NoError(t, err)
}

func TestLocalClient_UpdateLastAddConfirmed_NoOp(t *testing.T) {
	client, _ := newLocalClientWithMock(t)
	ctx := context.Background()

	// UpdateLastAddConfirmed is a NO-OP in local mode - should always return nil
	err := client.UpdateLastAddConfirmed(ctx, "bucket", "root", 1, 0, 42)
	assert.NoError(t, err)
}

func TestLocalClient_SelectNodes_NoOp(t *testing.T) {
	client, _ := newLocalClientWithMock(t)
	ctx := context.Background()

	// SelectNodes is a NO-OP in local mode - should always return nil, nil
	nodes, err := client.SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, nil)
	assert.NoError(t, err)
	assert.Nil(t, nodes)
}

func TestLocalClient_SelectNodes_WithFilters(t *testing.T) {
	client, _ := newLocalClientWithMock(t)
	ctx := context.Background()

	filters := []*proto.NodeFilter{
		{Az: "us-east-1"},
	}

	// SelectNodes is still a NO-OP in local mode regardless of filters
	nodes, err := client.SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_HARD, filters)
	assert.NoError(t, err)
	assert.Nil(t, nodes)
}

func TestLocalClient_ImplementsInterface(t *testing.T) {
	// Verify that logStoreClientLocal implements LogStoreClient
	var _ LogStoreClient = (*logStoreClientLocal)(nil)
}

func TestLocalClient_MultipleOperations(t *testing.T) {
	// Test that a single local client can handle multiple sequential operations
	client, mockStore := newLocalClientWithMock(t)
	ctx := context.Background()

	// Append
	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch1")
	mockStore.EXPECT().AddEntry(mock.Anything, "bucket", "root", int64(1), entry, resultCh).
		Return(int64(0), nil)
	entryId, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), entryId)

	// Get LAC
	mockStore.EXPECT().GetSegmentLastAddConfirmed(mock.Anything, "bucket", "root", int64(1), int64(1)).
		Return(int64(0), nil)
	lac, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), lac)

	// Complete
	mockStore.EXPECT().CompleteSegment(mock.Anything, "bucket", "root", int64(1), int64(1), int64(0)).
		Return(int64(0), nil)
	lastId, err := client.CompleteSegment(ctx, "bucket", "root", 1, 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), lastId)

	// Close
	err = client.Close(ctx)
	assert.NoError(t, err)
}
