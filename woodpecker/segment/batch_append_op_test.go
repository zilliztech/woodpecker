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
