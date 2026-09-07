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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

// The tests above cover each piece of the ack path on its own. These cover the
// sequences the pieces exist for: a replica that accepts an entry and then goes
// quiet, and what the retry machinery does with it.

// shrinkAppendAckReadTimeout makes the single-entry ack wait finish in test time.
func shrinkAppendAckReadTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	previous := appendAckReadTimeout
	appendAckReadTimeout = d
	t.Cleanup(func() { appendAckReadTimeout = previous })
}

// TestAppendOp_AckReadTimeout_IsReportedAsRetryable covers a replica that
// accepted the entry and never sent its durability ack.
//
// The read has to give up on its own - Recv observes only the stream's context -
// and how it reports that decides everything downstream. A bare gRPC status
// would be classified as neither a timeout nor retryable, so the entry would go
// terminal with none of its retries spent.
func TestAppendOp_AckReadTimeout_IsReportedAsRetryable(t *testing.T) {
	shrinkAppendAckReadTimeout(t, 150*time.Millisecond)

	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"),
		func(int64, int64, error) {}, nil, mockHandle,
		&proto.QuorumInfo{Wq: 3, Aq: 2, Es: 3, Nodes: []string{"n1", "n2", "n3"}}, nil)

	reported := make(chan error, 1)
	mockHandle.EXPECT().
		HandleAppendRequestFailure(mock.Anything, int64(3), mock.Anything, 0, "n1").
		Run(func(_ context.Context, _ int64, failure error, _ int, _ string) { reported <- failure }).
		Return().Once()

	silent, _ := inFlightReplicas(t, op, 1)
	op.receivedAckCallback(context.Background(), time.Now(), 3, silent[0], nil, 0, "n1")

	var failure error
	select {
	case failure = <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("a read that ran out of its budget must be reported, not swallowed")
	}

	require.Error(t, failure)
	assert.True(t, werr.IsRetryableErr(failure),
		"a timeout is not a terminal failure: the entry must keep its retries")
	assert.ErrorIs(t, failure, context.DeadlineExceeded,
		"the read-timeout log branch tests for this")
	assert.True(t, werr.IsRetryableErr(op.channelErrors[0]),
		"the error kept for the retry decision must be the same one")
}

// TestAppendOp_Retry_SecondAttemptSucceeds covers the sequence a fresh channel
// per attempt exists for: the first attempt's goroutine retires its own channel
// while the retry is already opening the next one. Sharing the slot let that
// close land on the retry's stream, failing an attempt that had just started.
func TestAppendOp_Retry_SecondAttemptSucceeds(t *testing.T) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"node1"}}

	op := NewAppendOp("bucket", "root", 1, 2, 10, []byte("v"),
		func(int64, int64, error) {}, mockPool, mockHandle, quorumInfo, nil)
	op.resultChannels = make([]channel.ResultChannel, 1)

	mockClient.EXPECT().IsRemoteClient().Return(false).Maybe()
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)

	var mu sync.Mutex
	handedOut := make([]channel.ResultChannel, 0, 2)
	firstFailed := make(chan struct{})
	mockClient.EXPECT().
		AppendEntry(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, _ *proto.LogEntry, ch channel.ResultChannel) (int64, error) {
			mu.Lock()
			attempt := len(handedOut)
			handedOut = append(handedOut, ch)
			mu.Unlock()
			if attempt == 0 {
				// A retryable send failure: no ack will ever arrive on this one.
				return -1, werr.ErrSegmentHandleSegmentRolling
			}
			// The retry's replica answers.
			require.NoError(t, ch.SendResult(context.Background(), &channel.AppendResult{SyncedId: 10}))
			return 10, nil
		})

	// The first attempt's failure is what schedules the retry; run it inline so
	// the two attempts interleave the way the executor makes them.
	mockHandle.EXPECT().
		HandleAppendRequestFailure(mock.Anything, int64(10), mock.Anything, 0, "node1").
		Run(func(context.Context, int64, error, int, string) { close(firstFailed) }).Return().Once()
	acked := make(chan struct{})
	mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(10)).
		Run(func(context.Context, int64) { close(acked) }).Return().Once()

	op.Execute()
	select {
	case <-firstFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("the first attempt should have failed")
	}

	NewAppendRequestRetryOp(context.Background(), 0, op).Execute()
	select {
	case <-acked:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry's ack never reached the quorum: an earlier attempt's close took its channel")
	}

	// Reaching the ack at all is the proof: the first attempt's goroutine retired
	// its channel while the retry was opening the next one, and the retry's ack
	// still landed. Sharing the slot is what used to make that close reach the
	// retry's stream instead.
	assert.True(t, op.completed.Load(), "the op completes on the retry")
	waitAckGoroutinesDone(t, op)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, handedOut, 2)
	assert.NotSame(t, handedOut[0], handedOut[1], "each attempt gets its own channel")
	// Each attempt's own goroutine retires the channel it was handed, so both end
	// closed - the point is that they were never the same object.
	assert.True(t, handedOut[0].IsClosed())
	assert.True(t, handedOut[1].IsClosed())
}

// TestSegmentHandle_AckReadTimeout_FailsTheEntryAfterItsRetries drives the whole
// sequence through a real segment handle: a replica accepts every attempt and
// answers none, so each attempt costs one ack budget and the entry is given up
// on once its retries are spent.
//
// This is what the caller experiences from an unresponsive replica. Before the
// read had a budget it experienced nothing at all - the ack goroutine blocked
// for as long as the connection survived and the append never came back.
func TestSegmentHandle_AckReadTimeout_FailsTheEntryAfterItsRetries(t *testing.T) {
	shrinkAppendAckReadTimeout(t, 150*time.Millisecond)

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()
	mockClient.EXPECT().UpdateLastAddConfirmed(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()
	// Giving up on an entry puts the segment into rolling, which fences and
	// completes it; that is the existing failure path, not what this test is
	// about, so it only needs to be allowed to happen.
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	mockClient.EXPECT().CompleteSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMetadata.EXPECT().GetSegmentMetadata(mock.Anything, mock.Anything, mock.Anything).Return(&meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Completed, LastEntryId: -1},
		Revision: 2,
	}, nil).Maybe()

	// Every attempt is accepted and then never answered, which is what a peer
	// that buffers the entry and stops responding looks like from here.
	var mu sync.Mutex
	attempts := 0
	mockClient.EXPECT().
		AppendEntry(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, entry *proto.LogEntry, ch channel.ResultChannel) (int64, error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			return entry.EntryId, nil // buffered; no durability ack will follow
		})

	const maxRetries = 2
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: maxRetries},
			},
		},
	}
	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"127.0.0.1"}},
		},
		Revision: 1,
	}
	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockPool, cfg, true, nil)

	failed := make(chan error, 1)
	segmentHandle.AppendAsync(context.Background(), []byte("stalled"), func(_ int64, _ int64, err error) {
		failed <- err
	})

	select {
	case err := <-failed:
		require.Error(t, err, "an entry no replica ever acknowledged must come back as a failure")
	case <-time.After(30 * time.Second):
		t.Fatal("the entry never came back: an unanswered ack must not block the caller forever")
	}

	mu.Lock()
	attemptsMade := attempts
	mu.Unlock()
	assert.Equal(t, maxRetries, attemptsMade,
		"the entry spends every retry it has before being given up on, one ack budget each")

	// The give-up rolls the segment on a background goroutine; let it finish so
	// the mocks are not exercised after the test returns.
	assert.Eventually(t, func() bool {
		segImpl := segmentHandle.(*segmentHandleImpl)
		segImpl.RLock()
		defer segImpl.RUnlock()
		return segImpl.appendOpsQueue.Len() == 0
	}, 10*time.Second, 10*time.Millisecond)
}
