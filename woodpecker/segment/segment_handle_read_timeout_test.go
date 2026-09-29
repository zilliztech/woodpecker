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

// testStorageType is the storage type of the handles built by these tests. The
// short active bound only applies in service mode.
var testStorageType = "service"

// testSegmentRead is the read configuration the handles built by these tests
// get; zero bounds take the defaults.
var testSegmentRead config.SegmentReadConfig

func shrinkReadTimeouts(t *testing.T, active, settled time.Duration) {
	old := testSegmentRead
	testSegmentRead = config.SegmentReadConfig{
		ActiveTimeout:  config.NewDurationMillisecondsFromInt(int(active.Milliseconds())),
		SettledTimeout: config.NewDurationMillisecondsFromInt(int(settled.Milliseconds())),
	}
	t.Cleanup(func() { testSegmentRead = old })
}

func readTimeoutTestHandle(t *testing.T, state proto.SegmentState, pool *mocks_logstore_client.LogStoreClientPool) SegmentHandle {
	return readTimeoutTestHandleWithNodes(t, state, pool, "node1", "node2")
}

func readTimeoutTestHandleWithNodes(t *testing.T, state proto.SegmentState, pool *mocks_logstore_client.LogStoreClientPool, nodes ...string) SegmentHandle {
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
				SegmentRead:   testSegmentRead,
			},
			Storage: config.StorageConfig{Type: testStorageType},
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
	pool.AssertNotCalled(t, "Clear", mock.Anything, mock.Anything)
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
// entry does not exist yet. With enough of them saying so (here 2 of 3 at an
// ack quorum of 2), an unreachable replica must not turn that into a read
// failure.
func TestReadBatchAdv_NotFoundFromLiveReplicasWinsOverUnreachableOne(t *testing.T) {
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	dead := mocks_logstore_client.NewLogStoreClient(t)
	live1 := mocks_logstore_client.NewLogStoreClient(t)
	live2 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(dead, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(live1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(live2, nil)
	dead.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, status.Error(codes.Unavailable, "connection refused"))
	live1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	live2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)

	h := readTimeoutTestHandleWithNodes(t, proto.SegmentState_Active, pool, "node1", "node2", "node3")
	_, err := h.ReadBatchAdv(context.Background(), 5, 10, nil)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "expected entry-not-found, got %v", err)
}

// One replica's "not found" does not stand for the segment at an ack quorum of
// 2 of 3: the entry may be acked on the other two. A restarted replica answers
// exactly this way for entries it never received. When those two are slow,
// the read must wait for them with the longer bound rather than report "no
// data yet", which the reader would poll on forever.
func TestReadBatchAdv_OneNotFoundDoesNotHideSlowReplicas(t *testing.T) {
	shrinkReadTimeouts(t, 100*time.Millisecond, 5*time.Second)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	restarted := mocks_logstore_client.NewLogStoreClient(t)
	slow1 := mocks_logstore_client.NewLogStoreClient(t)
	slow2 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(restarted, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(slow1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(slow2, nil).Maybe()
	restarted.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	answersAfter300ms := func(ctx context.Context, _ string, _ string, _ int64, _ int64, _ int64, _ int64, _ *proto.LastReadState) (*proto.BatchReadResult, error) {
		select {
		case <-time.After(300 * time.Millisecond):
			return oneEntryResult(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	slow1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(answersAfter300ms)
	slow2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(answersAfter300ms).Maybe()

	h := readTimeoutTestHandleWithNodes(t, proto.SegmentState_Active, pool, "node1", "node2", "node3")
	result, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.NoError(t, err, "an entry acked on the slow replicas must be read, not reported as missing")
	assert.Len(t, result.Entries, 1)
}

// If even the longer bound ends with too few "not found" answers and the rest
// timed out, the read reports the timeout: an error the reader surfaces, not
// the "no data yet" it would silently poll on.
func TestReadBatchAdv_SplitAnswerAfterLongPassIsATimeout(t *testing.T) {
	shrinkReadTimeouts(t, 50*time.Millisecond, 100*time.Millisecond)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	restarted := mocks_logstore_client.NewLogStoreClient(t)
	silent1 := mocks_logstore_client.NewLogStoreClient(t)
	silent2 := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(restarted, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(silent1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(silent2, nil)
	restarted.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	silent1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(silentRead)
	silent2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(silentRead)

	h := readTimeoutTestHandleWithNodes(t, proto.SegmentState_Active, pool, "node1", "node2", "node3")
	_, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.Error(t, err)
	assert.False(t, werr.ErrEntryNotFound.Is(err), "a split answer must not read as 'no data yet': %v", err)
	assert.True(t, werr.ErrTimeoutError.Is(err), "expected a timeout, got %v", err)
	pool.AssertNotCalled(t, "Clear", mock.Anything, mock.Anything)
}

// A replica a read timed out on is asked last from then on, so a silent
// replica costs one poll its bound, not every poll.
func TestReadBatchAdv_TimedOutReplicaIsAskedLast(t *testing.T) {
	shrinkReadTimeouts(t, 100*time.Millisecond, 100*time.Millisecond)

	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	silent := mocks_logstore_client.NewLogStoreClient(t)
	live := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(silent, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(live, nil)
	silent.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(silentRead).Once()
	live.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(oneEntryResult(), nil)

	h := readTimeoutTestHandle(t, proto.SegmentState_Active, pool)
	result, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.NoError(t, err)
	assert.Equal(t, "node2", result.LastReadState.Node)

	start := time.Now()
	for i := 0; i < 3; i++ {
		result, err = h.ReadBatchAdv(context.Background(), 0, 10, nil)
		require.NoError(t, err)
		assert.Equal(t, "node2", result.LastReadState.Node)
	}
	assert.Less(t, time.Since(start), 100*time.Millisecond, "later polls still asked the silent replica first")
}

// Every replica is asked even after enough of them said "not found": a replica
// serves entries only up to its own LAC, so the ones whose LAC lags answer
// "not found" for an entry another replica can already serve.
func TestReadBatchAdv_NotFoundDoesNotSkipRemainingReplicas(t *testing.T) {
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	lagging1 := mocks_logstore_client.NewLogStoreClient(t)
	lagging2 := mocks_logstore_client.NewLogStoreClient(t)
	caughtUp := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(lagging1, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node2").Return(lagging2, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "node3").Return(caughtUp, nil)
	lagging1.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	lagging2.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, werr.ErrEntryNotFound)
	caughtUp.EXPECT().ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(oneEntryResult(), nil)

	h := readTimeoutTestHandleWithNodes(t, proto.SegmentState_Active, pool, "node1", "node2", "node3")
	result, err := h.ReadBatchAdv(context.Background(), 0, 10, nil)
	require.NoError(t, err)
	assert.Equal(t, "node3", result.LastReadState.Node)
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

// The bounds come from the client configuration, and a zero bound takes its
// default.
func TestReadTimeouts_FromConfig(t *testing.T) {
	pool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Embedded modes read an active segment from this process's own logstore,
	// possibly from object storage, with no other replica to move to: one pass
	// with the long bound.
	testStorageType = "minio"
	h0 := readTimeoutTestHandle(t, proto.SegmentState_Active, pool).(*segmentHandleImpl)
	assert.Equal(t, []time.Duration{defaultSettledReadTimeout}, h0.readTimeouts())
	testStorageType = "service"

	h := readTimeoutTestHandle(t, proto.SegmentState_Active, pool).(*segmentHandleImpl)
	assert.Equal(t, []time.Duration{defaultActiveReadTimeout, defaultSettledReadTimeout}, h.readTimeouts())

	shrinkReadTimeouts(t, 150*time.Millisecond, 900*time.Millisecond)
	h = readTimeoutTestHandle(t, proto.SegmentState_Active, pool).(*segmentHandleImpl)
	assert.Equal(t, []time.Duration{150 * time.Millisecond, 900 * time.Millisecond}, h.readTimeouts())

	h = readTimeoutTestHandle(t, proto.SegmentState_Completed, pool).(*segmentHandleImpl)
	assert.Equal(t, []time.Duration{900 * time.Millisecond}, h.readTimeouts())
}
