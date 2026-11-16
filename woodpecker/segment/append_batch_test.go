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

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

func holdExecutorForBatch(h *segmentHandleImpl, size int) {
	h.appender.executor.Stop(context.Background())
	h.appender.executor = NewBatchingSequentialExecutor(size, size, 0, h.appender.dispatch)
}

func TestAppendBatchPartialFailureRetriesOnlyUnconfirmedEntry(t *testing.T) {
	var mu sync.Mutex
	var sends [][]int64
	c := &appenderTestClient{send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
		ids := make([]int64, len(es))
		for i, e := range es {
			ids[i] = e.EntryId
		}
		mu.Lock()
		sends = append(sends, ids)
		initial := len(sends) == 1
		mu.Unlock()
		for i, e := range es {
			result := &channel.AppendResult{SyncedId: e.EntryId}
			if initial && e.EntryId == 1 {
				result.Err = werr.ErrFileWriterSyncFailed
			}
			if err := chs[i].SendResult(ctx, result); err != nil {
				return err
			}
		}
		return nil
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, c)
	holdExecutorForBatch(h, 8)
	callbacks := make(chan int64, 3)
	for range 3 {
		h.AppendAsync(context.Background(), []byte("v"), func(_, id int64, err error) { require.NoError(t, err); callbacks <- id })
	}
	h.appender.executor.Start(context.Background())
	for i := range 3 {
		require.Equal(t, int64(i), receive(t, callbacks))
	}
	require.False(t, h.rollingState.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][]int64{{0, 1, 2}, {1}}, sends)
}

func TestAppendBatchBrokenStreamRetriesUnknownSuffix(t *testing.T) {
	old := batchAckReadTimeout
	batchAckReadTimeout = 40 * time.Millisecond
	t.Cleanup(func() { batchAckReadTimeout = old })
	var mu sync.Mutex
	var sends [][]int64
	c := &appenderTestClient{send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
		ids := make([]int64, len(es))
		for i, e := range es {
			ids[i] = e.EntryId
		}
		mu.Lock()
		sends = append(sends, ids)
		initial := len(sends) == 1
		mu.Unlock()
		if initial {
			return chs[0].SendResult(ctx, &channel.AppendResult{SyncedId: es[0].EntryId})
		}
		return ackEntries(ctx, es, chs)
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 2}, c)
	holdExecutorForBatch(h, 8)
	callbacks := make(chan int64, 3)
	for range 3 {
		h.AppendAsync(context.Background(), []byte("v"), func(_, id int64, err error) { require.NoError(t, err); callbacks <- id })
	}
	h.appender.executor.Start(context.Background())
	for i := range 3 {
		require.Equal(t, int64(i), receive(t, callbacks))
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][]int64{{0, 1, 2}, {1}, {2}}, sends)
	require.False(t, h.rollingState.Load())
}

func TestAppendBatchEachEntryGetsItsACKBudget(t *testing.T) {
	old := batchAckReadTimeout
	batchAckReadTimeout = 200 * time.Millisecond
	t.Cleanup(func() { batchAckReadTimeout = old })
	c := &appenderTestClient{send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
		go func() {
			for i, e := range es {
				timer := time.NewTimer(130 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
				_ = chs[i].SendResult(ctx, &channel.AppendResult{SyncedId: e.EntryId})
			}
		}()
		return nil
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 1}, c)
	holdExecutorForBatch(h, 8)
	first := appendForTest(h, []byte("a"))
	second := appendForTest(h, []byte("b"))
	h.appender.executor.Start(context.Background())
	require.NoError(t, receive(t, first))
	require.NoError(t, receive(t, second))
	require.False(t, h.rollingState.Load())
}

func TestReplicaAppenderIgnoresStaleAttemptResult(t *testing.T) {
	// A result identifies both the task and its generation; an obsolete read
	// cannot complete or retire a newer attempt, nor return its payload credit.
	op := NewAppendOp("b", "r", 1, 1, 0, []byte("v"), func(int64, int64, error) {}, &proto.QuorumInfo{Wq: 1, Aq: 1}, nil)
	task := &replicaTask{op: op, attempts: 2}
	g := &segmentAppender{ctx: context.Background()}
	r := &replicaAppender{group: g, pending: map[int64]*replicaTask{0: task}}
	r.apply(replicaAttemptResult{task: task, attempt: 1, err: werr.ErrFileWriterNoSpace})
	require.Same(t, task, r.pending[0])
	require.NoError(t, r.failure)
}
