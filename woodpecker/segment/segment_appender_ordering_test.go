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

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

type capturedAppend struct {
	ctx      context.Context
	entries  []*proto.LogEntry
	channels []channel.ResultChannel
}

func controlledAppenderClient(remote bool) (*appenderTestClient, <-chan capturedAppend) {
	sends := make(chan capturedAppend, 16)
	c := &appenderTestClient{remote: remote, send: func(ctx context.Context, entries []*proto.LogEntry, channels []channel.ResultChannel) error {
		select {
		case sends <- capturedAppend{ctx: ctx, entries: entries, channels: channels}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	return c, sends
}

type appendCallbackResult struct {
	id  int64
	err error
}

func appendSequenceForTest(h *segmentHandleImpl, count int) <-chan appendCallbackResult {
	callbacks := make(chan appendCallbackResult, count+1)
	for range count {
		h.AppendAsync(context.Background(), []byte("v"), func(_, id int64, err error) {
			callbacks <- appendCallbackResult{id: id, err: err}
		})
	}
	return callbacks
}

func waitForPendingQuorum(t *testing.T, h *segmentHandleImpl, id int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		h.RLock()
		defer h.RUnlock()
		e := h.findAppendOpById(context.Background(), id)
		return e != nil && e.Value.(*AppendOp).completed.Load() && e.Value.(*AppendOp).terminalErr == nil
	}, 5*time.Second, time.Millisecond, "entry %d must reach quorum while waiting behind the hole", id)
}

func requireAppenderCredits(t *testing.T, h *segmentHandleImpl, count int, bytes int64) {
	t.Helper()
	h.appender.mu.Lock()
	gotCount, gotBytes, gotRecords := h.appender.count, h.appender.bytes, len(h.appender.records)
	h.appender.mu.Unlock()
	require.Equal(t, count, gotCount)
	require.Equal(t, bytes, gotBytes)
	require.Equal(t, count, gotRecords)
}

func waitForAppenderDrained(t *testing.T, h *segmentHandleImpl) {
	t.Helper()
	require.Eventually(t, func() bool {
		h.RLock()
		callbacksDone := h.appendOpsQueue.Len() == 0 && !h.callbackRunning
		h.RUnlock()
		h.appender.mu.Lock()
		defer h.appender.mu.Unlock()
		return callbacksDone && h.appender.count == 0 && h.appender.bytes == 0 && len(h.appender.records) == 0
	}, 5*time.Second, time.Millisecond, "callbacks and all replica outcomes must release credits before shutdown")
}

func TestSegmentAppenderOutOfOrderACKsWaitForContiguousPrefix(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "embed", true: "service"}[service], func(t *testing.T) {
			replicas := 1
			if service {
				replicas = 3
			}
			clients := make([]*appenderTestClient, replicas)
			sends := make([]<-chan capturedAppend, replicas)
			for i := range clients {
				clients[i], sends[i] = controlledAppenderClient(service)
			}
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 3, MaxRetries: 2, MaxBatchEntries: 1}, clients...)
			callbacks := appendSequenceForTest(h, 3)
			acks := make([][]channel.ResultChannel, replicas)
			for r := range clients {
				for id := range 3 {
					sent := receive(t, sends[r])
					require.Len(t, sent.entries, 1)
					require.Equal(t, int64(id), sent.entries[0].EntryId)
					acks[r] = append(acks[r], sent.channels[0])
				}
			}
			for _, id := range []int64{2, 1} {
				for r := range clients {
					require.NoError(t, acks[r][id].SendResult(context.Background(), &channel.AppendResult{SyncedId: id}))
				}
				waitForPendingQuorum(t, h, id)
				require.Equal(t, int64(-1), h.lastAddConfirmed.Load())
				require.Empty(t, callbacks, "later quorum success cannot overtake entry 0")
				requireAppenderCredits(t, h, 3, 3)
			}
			for r := range clients {
				require.NoError(t, acks[r][0].SendResult(context.Background(), &channel.AppendResult{SyncedId: 0}))
			}
			for id := range 3 {
				result := receive(t, callbacks)
				require.Equal(t, int64(id), result.id)
				require.NoError(t, result.err)
			}
			waitForAppenderDrained(t, h)
			require.Empty(t, callbacks)
			require.Equal(t, int64(2), h.lastAddConfirmed.Load())
			require.False(t, h.rollingState.Load())
		})
	}
}

func TestSegmentAppenderMiddleHoleFailsAlreadyAcknowledgedSuffix(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "embed", true: "service"}[service], func(t *testing.T) {
			replicas, failing := 1, 1
			if service {
				replicas, failing = 3, 2
			}
			clients := make([]*appenderTestClient, replicas)
			sends := make([]<-chan capturedAppend, replicas)
			for i := range clients {
				clients[i], sends[i] = controlledAppenderClient(service)
			}
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 3, MaxRetries: 2, MaxBatchEntries: 1}, clients...)
			callbacks := appendSequenceForTest(h, 3)
			acks := make([][]channel.ResultChannel, replicas)
			for r := range clients {
				for id := range 3 {
					sent := receive(t, sends[r])
					require.Len(t, sent.entries, 1)
					require.Equal(t, int64(id), sent.entries[0].EntryId)
					acks[r] = append(acks[r], sent.channels[0])
				}
			}
			for r := range clients {
				require.NoError(t, acks[r][0].SendResult(context.Background(), &channel.AppendResult{SyncedId: 0}))
				require.NoError(t, acks[r][2].SendResult(context.Background(), &channel.AppendResult{SyncedId: 2}))
				if r >= failing {
					require.NoError(t, acks[r][1].SendResult(context.Background(), &channel.AppendResult{SyncedId: 1}))
				}
			}
			first := receive(t, callbacks)
			require.Equal(t, int64(0), first.id)
			require.NoError(t, first.err)
			waitForPendingQuorum(t, h, 2)
			require.Equal(t, int64(0), h.lastAddConfirmed.Load())
			require.Empty(t, callbacks)

			retries := make([]capturedAppend, failing)
			for r := range failing {
				require.NoError(t, acks[r][1].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrFileWriterSyncFailed}))
				retries[r] = receive(t, sends[r])
				require.Len(t, retries[r].entries, 1)
				require.Equal(t, int64(1), retries[r].entries[0].EntryId)
				require.NotSame(t, acks[r][1], retries[r].channels[0])
			}
			require.False(t, h.rollingState.Load(), "retryable attempts must not roll the segment")
			require.Empty(t, callbacks)
			for _, retry := range retries {
				require.NoError(t, retry.channels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrFileWriterSyncFailed}))
			}
			for _, id := range []int64{1, 2} {
				result := receive(t, callbacks)
				require.Equal(t, id, result.id)
				require.ErrorIs(t, result.err, werr.ErrFileWriterSyncFailed)
			}
			waitForAppenderDrained(t, h)
			require.Empty(t, callbacks)
			require.Equal(t, int64(0), h.lastAddConfirmed.Load(), "a successful suffix cannot commit across a permanent hole")
			require.True(t, h.rollingState.Load())
			for _, sent := range sends {
				require.Empty(t, sent, "only the failed entry is retried, within its attempt budget")
			}
		})
	}
}

func TestReplicaAppenderLateACKAfterTimeoutCannotCompleteRetry(t *testing.T) {
	shrinkAppendAckReadTimeout(t, 40*time.Millisecond)
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "embed", true: "service"}[service], func(t *testing.T) {
			c, sends := controlledAppenderClient(service)
			send := c.send
			var attempts atomic.Int32
			releaseRetry := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseRetry) })
			defer release()
			c.send = func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
				attempt := attempts.Add(1)
				if err := send(ctx, es, chs); err != nil {
					return err
				}
				if attempt > 1 {
					// Hold Buffered so the new ACK budget cannot expire while we inspect the retired attempt.
					select {
					case <-releaseRetry:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			clients := []*appenderTestClient{c}
			if service {
				clients = append(clients, &appenderTestClient{remote: true, send: ackEntries}, &appenderTestClient{remote: true, send: ackEntries})
			}
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 2}, clients...)
			callbacks := appendSequenceForTest(h, 1)
			first, retry := receive(t, sends), receive(t, sends)
			if service {
				result := receive(t, callbacks)
				require.Equal(t, int64(0), result.id)
				require.NoError(t, result.err, "healthy replicas can complete before the retry finishes")
			}
			require.NotSame(t, first.channels[0], retry.channels[0])
			require.True(t, first.channels[0].IsClosed())
			for _, late := range []*channel.AppendResult{
				{SyncedId: 0},
				{SyncedId: -1, Err: werr.ErrFileWriterNoSpace},
			} {
				require.Error(t, first.channels[0].SendResult(context.Background(), late), "retired attempts reject late success and failure")
			}
			require.False(t, retry.channels[0].IsClosed())
			require.NoError(t, retry.ctx.Err())
			requireAppenderCredits(t, h, 1, 1)
			require.Empty(t, callbacks)
			require.False(t, h.rollingState.Load())
			require.NoError(t, ackEntries(retry.ctx, retry.entries, retry.channels))
			release()
			if !service {
				result := receive(t, callbacks)
				require.Equal(t, int64(0), result.id)
				require.NoError(t, result.err)
			}
			waitForAppenderDrained(t, h)
			require.Empty(t, callbacks, "late outcomes must not invoke another callback")
			require.Equal(t, int32(2), attempts.Load())
			require.Equal(t, int64(0), h.lastAddConfirmed.Load())
			require.True(t, retry.channels[0].IsClosed())
			require.False(t, h.rollingState.Load())
		})
	}
}

func TestReplicaAppenderRemoteBatchRetryUsesFreshRemoteChannel(t *testing.T) {
	c, sends := controlledAppenderClient(true)
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 3, MaxRetries: 2}, c)
	holdExecutorForBatch(h, 3)
	callbacks := appendSequenceForTest(h, 3)
	h.appender.executor.Start(context.Background())
	batch := receive(t, sends)
	require.Len(t, batch.entries, 3)
	for i, entry := range batch.entries {
		require.Equal(t, int64(i), entry.EntryId)
		require.IsType(t, (*channel.LocalResultChannel)(nil), batch.channels[i])
		result := &channel.AppendResult{SyncedId: entry.EntryId}
		if i == 1 {
			result.Err = werr.ErrFileWriterSyncFailed
		}
		require.NoError(t, batch.channels[i].SendResult(context.Background(), result))
	}
	first := receive(t, callbacks)
	require.Equal(t, int64(0), first.id)
	require.NoError(t, first.err)
	retry := receive(t, sends)
	require.Len(t, retry.entries, 1)
	require.Equal(t, int64(1), retry.entries[0].EntryId)
	require.IsType(t, (*channel.RemoteResultChannel)(nil), retry.channels[0])
	require.NotSame(t, batch.channels[1], retry.channels[0])
	require.True(t, batch.channels[1].IsClosed())
	waitForPendingQuorum(t, h, 2)
	require.Empty(t, callbacks)
	require.Equal(t, int64(0), h.lastAddConfirmed.Load())
	require.NoError(t, ackEntries(retry.ctx, retry.entries, retry.channels))
	for _, id := range []int64{1, 2} {
		result := receive(t, callbacks)
		require.Equal(t, id, result.id)
		require.NoError(t, result.err)
	}
	waitForAppenderDrained(t, h)
	require.Empty(t, callbacks)
	require.Empty(t, sends, "successful batch entries must not be retried")
	require.Equal(t, int64(2), h.lastAddConfirmed.Load())
	require.False(t, h.rollingState.Load())
	for _, ch := range append(batch.channels, retry.channels...) {
		require.True(t, ch.IsClosed())
	}
}
