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

package segment

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/segment"
)

func TestAppendOp_Execute_Success(t *testing.T) {
	ctx := context.Background()
	mockLogStoreClient := mocks_logstore_client.NewLogStoreClient(t)
	mockLogStoreClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegmentHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle.EXPECT().SendAppendSuccessCallbacks(int64(0)).Return()
	quorumInfo := &proto.QuorumInfo{
		Nodes: []string{"node1"},
		Wq:    1,
	}
	mockSegmentHandle.EXPECT().GetQuorumInfo(ctx).Return(quorumInfo, nil)
	mockLogStoreClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockLogStoreClient, nil)
	ch := make(chan int64, 1)
	ch <- int64(0)
	close(ch)
	// mock append success
	mockLogStoreClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
		SegmentId: int64(1),
		EntryId:   int64(0),
		Data:      []byte("test"),
	}).Return(int64(0), ch, nil)

	appendOp := &AppendOp{
		logId:      int64(1),
		segmentId:  int64(1),
		entryId:    int64(0),
		value:      []byte("test"),
		callback:   func(segmentId int64, entryId int64, err error) {},
		clientPool: mockLogStoreClientPool,
		handle:     mockSegmentHandle,
		ackSet:     &bitset.BitSet{},
		quorumInfo: quorumInfo,
	}
	appendOp.Execute()

	// wait for a while to let other goroutines to finish
	time.Sleep(1 * time.Second)
	// check result
	assert.True(t, appendOp.ackSet.Count() == 1)
	assert.True(t, appendOp.completed.Load())
	assert.Nil(t, appendOp.err)
}

func TestAppendOp_Execute_Error(t *testing.T) {
	ctx := context.Background()
	mockLogStoreClient := mocks_logstore_client.NewLogStoreClient(t)
	mockLogStoreClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegmentHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockSegmentHandle.EXPECT().SendAppendErrorCallbacks(int64(0), mock.Anything).Return()
	quorumInfo := &proto.QuorumInfo{
		Nodes: []string{"node1"},
		Wq:    1,
	}
	mockSegmentHandle.EXPECT().GetQuorumInfo(ctx).Return(quorumInfo, nil)
	mockLogStoreClientPool.EXPECT().GetLogStoreClient(mock.Anything).Return(mockLogStoreClient, nil)
	ch := make(chan int64, 1)
	ch <- int64(-1)
	close(ch)
	// mock append error
	mockLogStoreClient.EXPECT().AppendEntry(mock.Anything, int64(1), &segment.SegmentEntry{
		SegmentId: int64(1),
		EntryId:   int64(0),
		Data:      []byte("test"),
	}).Return(int64(-1), ch, errors.New("append error"))

	appendOp := &AppendOp{
		logId:      int64(1),
		segmentId:  int64(1),
		entryId:    int64(0),
		value:      []byte("test"),
		callback:   func(segmentId int64, entryId int64, err error) {},
		clientPool: mockLogStoreClientPool,
		handle:     mockSegmentHandle,
		ackSet:     &bitset.BitSet{},
		quorumInfo: quorumInfo,
	}
	appendOp.Execute()

	// wait for a while to let other goroutines to finish
	time.Sleep(1 * time.Second)
	// check result
	assert.Equal(t, 0, appendOp.ackSet.Count())
	assert.False(t, appendOp.completed.Load())
	assert.Error(t, appendOp.err)
}
