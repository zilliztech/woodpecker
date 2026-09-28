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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func shrinkReadTimeouts(t *testing.T, active, settled time.Duration) {
	oldActive, oldSettled := activeReadTimeout, settledReadTimeout
	activeReadTimeout, settledReadTimeout = active, settled
	t.Cleanup(func() { activeReadTimeout, settledReadTimeout = oldActive, oldSettled })
}

func readTimeoutTestHandle(t *testing.T, state proto.SegmentState, pool *mocks_logstore_client.LogStoreClientPool) SegmentHandle {
	return readTimeoutTestHandleWithNodes(t, state, pool, "node1", "node2")
}

func readTimeoutTestHandleWithNodes(t *testing.T, state proto.SegmentState, pool *mocks_logstore_client.LogStoreClientPool, nodes ...string) SegmentHandle {
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       state,
			LastEntryId: -1,
			Quorum:      &proto.QuorumInfo{Id: 1, Aq: 2, Es: int32(len(nodes)), Wq: int32(len(nodes)), Nodes: nodes},
		},
		Revision: 1,
	}
	return NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mocks_meta.NewMetadataProvider(t), pool, cfg, false, nil)
}

// silentRead answers like a replica that vanished with its connection still
// READY: nothing until the call's context ends.
func silentRead(ctx context.Context, _ string, _ string, _ int64, _ int64, _ int64, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func oneEntryResult() *proto.BatchReadResult {
	return &proto.BatchReadResult{
		Entries:       []*proto.LogEntry{{SegId: 1, EntryId: 0, Values: []byte("entry_0")}},
		LastReadState: &proto.LastReadState{},
	}
}

// A replica that never answers must not pin the reader of an active segment:
// the call is bounded, its connection is dropped, and the next replica serves.
func TestReadBatchAdv_ActiveSegment_SilentReplicaFailsOver(t *testing.T) {
	shrinkReadTimeouts(t, 200*time.Millisecond, 10*time.Second)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli1 := mocks_logstore_client.NewLogStoreClient(t)
	cli2 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(cli1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(cli2, nil)
	pool.EXPECT().Clear(mock.Anything, "node1").Return().Once()
	cli1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(silentRead)
	cli2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(oneEntryResult(), nil)

	h := readTimeoutTestHandle(t, proto.SegmentState_Active, pool)
	start := time.Now()
	result, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.NoError(t, err)
	assert.Equal(t, "node2", result.LastReadState.Node)
	assert.Less(t, time.Since(start), 2*time.Second, "the silent replica was not bounded by the active read timeout")
}

// The handle's view of the state can lag: a segment it believes active may be
// served from object storage already. When every replica runs out of the short
// bound, the read is retried once with the long one rather than failed.
func TestReadBatchAdv_ActiveSegment_AllSlowEscalatesToSettledTimeout(t *testing.T) {
	shrinkReadTimeouts(t, 100*time.Millisecond, 5*time.Second)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli1 := mocks_logstore_client.NewLogStoreClient(t)
	cli2 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(cli1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(cli2, nil)
	pool.EXPECT().Clear(mock.Anything, mock.Anything).Return()

	// Every replica takes 300ms: longer than the short bound, well within the long one.
	var calls atomic.Int32
	slow := func(ctx context.Context, _ string, _ string, _ int64, _ int64, _ int64, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		calls.Add(1)
		select {
		case <-time.After(300 * time.Millisecond):
			return oneEntryResult(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	cli1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(slow)
	cli2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(slow).Maybe()

	h := readTimeoutTestHandle(t, proto.SegmentState_Active, pool)
	result, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.NoError(t, err, "a slow but answering storage must not fail the read")
	assert.Len(t, result.Entries, 1)
	assert.Equal(t, int32(3), calls.Load(), "two short attempts, then the first replica again with the long bound")
}

// A segment that is no longer active may be served from object storage, where
// a slow answer is the storage, not the replica: the long bound applies from
// the first attempt and the replica keeps its connection.
func TestReadBatchAdv_SettledSegment_UsesLongTimeout(t *testing.T) {
	shrinkReadTimeouts(t, 100*time.Millisecond, 5*time.Second)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli1 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(cli1, nil)
	cli1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ string, _ string, _ int64, _ int64, _ int64, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
			select {
			case <-time.After(300 * time.Millisecond):
				return oneEntryResult(), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})

	h := readTimeoutTestHandle(t, proto.SegmentState_Completed, pool)
	result, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.NoError(t, err)
	assert.Equal(t, "node1", result.LastReadState.Node)
	pool.AssertNotCalled(t, "Clear", mock.Anything, mock.Anything)
}

// At the tail of an active segment the live replicas answer that the next
// entry does not exist yet. A replica that cannot be reached must not turn that
// into a read failure just because it was tried last.
func TestReadBatchAdv_NotFoundFromLiveReplicasWinsOverUnreachableOne(t *testing.T) {
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	live1 := mocks_logstore_client.NewLogStoreClient(t)
	live2 := mocks_logstore_client.NewLogStoreClient(t)
	dead := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(live1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(live2, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(dead, nil)
	live1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	live2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	dead.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, status.Error(codes.Unavailable, "connection refused"))

	h := readTimeoutTestHandleWithNodes(t, proto.SegmentState_Active, pool, "node1", "node2", "node3")
	_, err := h.ReadBatchAdv(context.Background(), 5, 10, nil)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "expected entry-not-found, got %v", err)
}

// With no replica answering at all, the read does fail, with the replica's error.
func TestReadBatchAdv_AllUnreachableStillFails(t *testing.T) {
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli1 := mocks_logstore_client.NewLogStoreClient(t)
	cli2 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(cli1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(cli2, nil)
	cli1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, status.Error(codes.Unavailable, "connection refused"))
	cli2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, status.Error(codes.Unavailable, "connection refused"))

	h := readTimeoutTestHandle(t, proto.SegmentState_Active, pool)
	_, err := h.ReadBatchAdv(context.Background(), 5, 10, nil)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}
