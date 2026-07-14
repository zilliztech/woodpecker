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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

// TestBatchAppendOp_Execute_QuorumAck verifies that a BatchAppendOp sends every
// entry to each replica via a single AppendEntries call per replica, and that
// each entry still reaches quorum and is acknowledged through the reused
// per-entry receivedAckCallback / SendAppendSuccessCallbacks machinery.
func TestBatchAppendOp_Execute_QuorumAck(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    3,
		Aq:    2, // quorum reached after 2 of 3 replicas ack
		Es:    3,
		Nodes: []string{"node1", "node2", "node3"},
	}

	const batchN = 4
	const firstEntryId = int64(10)
	ops := make([]*AppendOp, batchN)
	for i := 0; i < batchN; i++ {
		ops[i] = NewAppendOp("a-bucket", "files", 1, 2, firstEntryId+int64(i), []byte("val"),
			func(int64, int64, error) {}, mockPool, mockHandle, quorumInfo)
	}

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

	// One AppendEntries call per replica carries all entries; simulate the server
	// streaming back a Synced result for each entry.
	var appendEntriesCalls atomic.Int32
	mockClient.EXPECT().
		AppendEntries(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, entries []*proto.LogEntry, chs []channel.ResultChannel) ([]int64, error) {
			appendEntriesCalls.Add(1)
			ids := make([]int64, len(entries))
			for i, e := range entries {
				ids[i] = e.EntryId
				ch := chs[i]
				eid := e.EntryId
				go func() {
					_ = ch.SendResult(context.Background(), &channel.AppendResult{SyncedId: eid})
				}()
			}
			return ids, nil
		})

	var wg sync.WaitGroup
	wg.Add(batchN)
	for i := 0; i < batchN; i++ {
		eid := firstEntryId + int64(i)
		mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, eid).
			Run(func(context.Context, int64) { wg.Done() }).Return().Once()
	}

	NewBatchAppendOp(ops).Execute()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for batch quorum acks")
	}

	// Exactly one AppendEntries per replica (3), not one per entry.
	assert.Equal(t, int32(len(quorumInfo.Nodes)), appendEntriesCalls.Load())
	for i := 0; i < batchN; i++ {
		assert.True(t, ops[i].completed.Load(), "op %d should be completed", i)
	}
}

// TestSequentialExecutor_CoalescesAppendOps verifies that when batching is
// enabled the executor coalesces consecutive queued AppendOps into a single
// BatchAppendOp: the batch sends one AppendEntries per replica instead of one
// AppendEntry per entry. The single-entry AppendEntry path is intentionally not
// mocked, so any non-batched send would fail the test loudly.
func TestSequentialExecutor_CoalescesAppendOps(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)

	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Wq:    3,
		Aq:    2,
		Es:    3,
		Nodes: []string{"node1", "node2", "node3"},
	}

	const batchN = 5
	const firstEntryId = int64(100)
	ops := make([]*AppendOp, batchN)
	for i := 0; i < batchN; i++ {
		ops[i] = NewAppendOp("a-bucket", "files", 1, 2, firstEntryId+int64(i), []byte("v"),
			func(int64, int64, error) {}, mockPool, mockHandle, quorumInfo)
	}

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)

	var appendEntriesCalls atomic.Int32
	mockClient.EXPECT().
		AppendEntries(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, entries []*proto.LogEntry, chs []channel.ResultChannel) ([]int64, error) {
			appendEntriesCalls.Add(1)
			ids := make([]int64, len(entries))
			for i, e := range entries {
				ids[i] = e.EntryId
				ch := chs[i]
				eid := e.EntryId
				go func() {
					_ = ch.SendResult(context.Background(), &channel.AppendResult{SyncedId: eid})
				}()
			}
			return ids, nil
		})

	var wg sync.WaitGroup
	wg.Add(batchN)
	for i := 0; i < batchN; i++ {
		eid := firstEntryId + int64(i)
		mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, eid).
			Run(func(context.Context, int64) { wg.Done() }).Return().Once()
	}

	exec := NewBatchingSequentialExecutor(64, 8, 0)
	ctx := context.Background()
	// Queue all ops before starting the worker, so they are coalesced
	// deterministically into a single batch when the worker drains.
	for _, op := range ops {
		assert.True(t, exec.Submit(ctx, op))
	}
	exec.Start(ctx)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for coalesced batch acks")
	}
	exec.Stop(ctx)

	assert.Equal(t, int32(len(quorumInfo.Nodes)), appendEntriesCalls.Load(),
		"the whole batch should send one AppendEntries per replica")
}

// waitWG waits for wg with a timeout, failing the test rather than hanging.
func waitWG(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for async callbacks")
	}
}

func newBatchOps(t *testing.T, n int, pool *mocks_logstore_client.LogStoreClientPool, handle *mocks_segment_handle.SegmentHandle, quorumInfo *proto.QuorumInfo) []*AppendOp {
	t.Helper()
	ops := make([]*AppendOp, n)
	for i := 0; i < n; i++ {
		ops[i] = NewAppendOp("a-bucket", "files", 1, 2, int64(10+i), []byte("v"),
			func(int64, int64, error) {}, pool, handle, quorumInfo)
	}
	return ops
}

// TestBatchAppendOp_InstallsLocalResultChannelInOpSlot pins the precondition that
// the retry channel-type fix relies on: a successful batch leaves a
// LocalResultChannel in each op's per-replica slot. If this ever changes, the
// retry rebuild (TestAppendOp_Retry_RebuildsResultChannelForRemoteClient) is
// testing a scenario that can no longer occur.
func TestBatchAppendOp_InstallsLocalResultChannelInOpSlot(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	quorumInfo := &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"node1"}}

	const batchN = 2
	ops := newBatchOps(t, batchN, mockPool, mockHandle, quorumInfo)

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().
		AppendEntries(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, entries []*proto.LogEntry, chs []channel.ResultChannel) ([]int64, error) {
			ids := make([]int64, len(entries))
			for i, e := range entries {
				ids[i] = e.EntryId
				ch := chs[i]
				eid := e.EntryId
				go func() { _ = ch.SendResult(context.Background(), &channel.AppendResult{SyncedId: eid}) }()
			}
			return ids, nil
		})

	var wg sync.WaitGroup
	wg.Add(batchN)
	for i := 0; i < batchN; i++ {
		mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(10+i)).
			Run(func(context.Context, int64) { wg.Done() }).Return().Once()
	}

	NewBatchAppendOp(ops).Execute()
	waitWG(t, &wg, 5*time.Second)

	for i, op := range ops {
		assert.NotNil(t, op.resultChannels[0], "op %d should have a channel in slot 0", i)
		_, isLocal := op.resultChannels[0].(*channel.LocalResultChannel)
		assert.True(t, isLocal, "op %d slot should hold a LocalResultChannel after batch send", i)
	}
}

// TestBatchAppendOp_GetClientFails_AllOpsRoutedToFailure covers the send-side
// failure where the client pool can't produce a client: every op in the batch
// must be routed through HandleAppendRequestFailure for that node, with the
// error recorded per op.
func TestBatchAppendOp_GetClientFails_AllOpsRoutedToFailure(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	quorumInfo := &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"node1"}}

	const batchN = 3
	ops := newBatchOps(t, batchN, mockPool, mockHandle, quorumInfo)

	clientErr := errors.New("no client available")
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(nil, clientErr)

	var wg sync.WaitGroup
	wg.Add(batchN)
	mockHandle.EXPECT().
		HandleAppendRequestFailure(mock.Anything, mock.Anything, clientErr, 0, "node1").
		Run(func(context.Context, int64, error, int, string) { wg.Done() }).Return().Times(batchN)

	NewBatchAppendOp(ops).Execute()
	waitWG(t, &wg, 5*time.Second)

	for i, op := range ops {
		assert.Equal(t, clientErr, op.channelErrors[0], "op %d should record the client error", i)
	}
}

// TestBatchAppendOp_AppendEntriesError_AllOpsRoutedToFailure covers the batch RPC
// itself failing: every op must be routed through HandleAppendRequestFailure.
func TestBatchAppendOp_AppendEntriesError_AllOpsRoutedToFailure(t *testing.T) {
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	quorumInfo := &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"node1"}}

	const batchN = 3
	ops := newBatchOps(t, batchN, mockPool, mockHandle, quorumInfo)

	sendErr := errors.New("append entries failed")
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().
		AppendEntries(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, sendErr)

	var wg sync.WaitGroup
	wg.Add(batchN)
	mockHandle.EXPECT().
		HandleAppendRequestFailure(mock.Anything, mock.Anything, sendErr, 0, "node1").
		Run(func(context.Context, int64, error, int, string) { wg.Done() }).Return().Times(batchN)

	NewBatchAppendOp(ops).Execute()
	waitWG(t, &wg, 5*time.Second)

	for i, op := range ops {
		assert.Equal(t, sendErr, op.channelErrors[0], "op %d should record the send error", i)
	}
}
