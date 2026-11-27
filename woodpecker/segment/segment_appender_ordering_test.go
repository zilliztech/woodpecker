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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/topology"
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
		h.appender.mu.Lock()
		defer h.appender.mu.Unlock()
		e := h.appender.findAppendOpByIDUnsafe(id)
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
		h.appender.mu.Lock()
		defer h.appender.mu.Unlock()
		callbacksDone := h.appender.appendOpsQueue.Len() == 0 && !h.appender.callbackRunning
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
				require.Equal(t, int64(-1), h.confirmedLAC())
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
			require.Equal(t, int64(2), h.confirmedLAC())
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
			require.Equal(t, int64(0), h.confirmedLAC())
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
			require.Equal(t, int64(0), h.confirmedLAC(), "a successful suffix cannot commit across a permanent hole")
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
			require.Equal(t, int64(0), h.confirmedLAC())
			require.True(t, retry.channels[0].IsClosed())
			require.False(t, h.rollingState.Load())
		})
	}
}

func TestReplicaAppenderRemoteBatchRetryUsesFreshLocalChannel(t *testing.T) {
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
	require.IsType(t, (*channel.LocalResultChannel)(nil), retry.channels[0])
	require.NotSame(t, batch.channels[1], retry.channels[0])
	require.True(t, batch.channels[1].IsClosed())
	waitForPendingQuorum(t, h, 2)
	require.Empty(t, callbacks)
	require.Equal(t, int64(0), h.confirmedLAC())
	require.NoError(t, ackEntries(retry.ctx, retry.entries, retry.channels))
	for _, id := range []int64{1, 2} {
		result := receive(t, callbacks)
		require.Equal(t, id, result.id)
		require.NoError(t, result.err)
	}
	waitForAppenderDrained(t, h)
	require.Empty(t, callbacks)
	require.Empty(t, sends, "successful batch entries must not be retried")
	require.Equal(t, int64(2), h.confirmedLAC())
	require.False(t, h.rollingState.Load())
	for _, ch := range append(batch.channels, retry.channels...) {
		require.True(t, ch.IsClosed())
	}
}

func TestReplicaAppenderACKProgressWhileLaterSendBlocked(t *testing.T) {
	for _, service := range []bool{false, true} {
		for _, retrySend := range []bool{false, true} {
			t.Run(fmt.Sprintf("service=%t/retry=%t", service, retrySend), func(t *testing.T) {
				c, sends := controlledAppenderClient(service)
				send := c.send
				releaseSend := make(chan struct{})
				release := sync.OnceFunc(func() { close(releaseSend) })
				defer release()
				blocked, exited := make(chan struct{}), make(chan struct{})
				var laterAttempts atomic.Int32
				c.send = func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
					if err := send(ctx, es, chs); err != nil {
						return err
					}
					if es[0].EntryId == 1 {
						attempt := laterAttempts.Add(1)
						if retrySend && attempt == 1 {
							return werr.ErrTimeoutError
						}
						close(blocked)
						defer close(exited)
						select {
						case <-releaseSend:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}
				clients := []*appenderTestClient{c}
				replicaSends := []<-chan capturedAppend{sends}
				if service {
					for range 2 {
						other, otherSends := controlledAppenderClient(true)
						clients = append(clients, other)
						replicaSends = append(replicaSends, otherSends)
					}
				}
				h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 2, MaxRetries: 2, MaxBatchEntries: 1}, clients...)
				callbacks := appendSequenceForTest(h, 2)
				firsts, seconds := make([]capturedAppend, len(clients)), make([]capturedAppend, len(clients))
				for i := range clients {
					firsts[i], seconds[i] = receive(t, replicaSends[i]), receive(t, replicaSends[i])
					require.Equal(t, int64(0), firsts[i].entries[0].EntryId)
					require.Equal(t, int64(1), seconds[i].entries[0].EntryId)
					if i == 0 && retrySend {
						seconds[i] = receive(t, replicaSends[i])
						require.Equal(t, int64(1), seconds[i].entries[0].EntryId)
					}
				}
				receive(t, blocked)
				// In service mode, the blocked replica must supply the second quorum vote.
				for i := 0; i < int(h.quorumInfo.Aq); i++ {
					require.NoError(t, ackEntries(firsts[i].ctx, firsts[i].entries, firsts[i].channels))
				}
				first := receive(t, callbacks)
				require.Equal(t, int64(0), first.id)
				require.NoError(t, first.err)
				require.Equal(t, int64(0), h.confirmedLAC())
				select {
				case <-exited:
					t.Fatal("entry 0 must complete while entry 1 is still blocked in send")
				default:
				}
				for i := int(h.quorumInfo.Aq); i < len(clients); i++ {
					require.NoError(t, ackEntries(firsts[i].ctx, firsts[i].entries, firsts[i].channels))
				}
				require.Eventually(t, func() bool {
					h.appender.mu.Lock()
					defer h.appender.mu.Unlock()
					return h.appender.count == 1
				}, 5*time.Second, time.Millisecond)
				requireAppenderCredits(t, h, 1, 1)
				require.Empty(t, callbacks)
				for _, second := range seconds {
					require.NoError(t, ackEntries(second.ctx, second.entries, second.channels))
				}
				release()
				second := receive(t, callbacks)
				require.Equal(t, int64(1), second.id)
				require.NoError(t, second.err)
				waitForAppenderDrained(t, h)
				require.Empty(t, callbacks)
				require.False(t, h.rollingState.Load())
			})
		}
	}
}

func TestReplicaAppenderACKFailureDuringSendRetriesBeforeQueuedEntries(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			c, sends := controlledAppenderClient(remote)
			send := c.send
			releaseSend := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseSend) })
			defer release()
			blocked := make(chan struct{})
			var firstAttempts, active atomic.Int32
			var overlapped atomic.Bool
			c.send = func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
				if active.Add(1) > 1 {
					overlapped.Store(true)
				}
				defer active.Add(-1)
				if err := send(ctx, es, chs); err != nil {
					return err
				}
				if es[0].EntryId == 0 && firstAttempts.Add(1) == 1 {
					return nil
				}
				if es[0].EntryId == 1 {
					close(blocked)
					select {
					case <-releaseSend:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return ackEntries(ctx, es, chs)
			}
			h := appenderFixture(t, remote, config.SegmentAppendConfig{QueueSize: 3, MaxRetries: 2, MaxBatchEntries: 1}, c)
			errors := metrics.WpClientReplicaAppendTotal.WithLabelValues(h.logNs, "1", topology.ScopeUnknown, "error")
			before := testutil.ToFloat64(errors)
			callbacks := appendSequenceForTest(h, 3)
			first, second := receive(t, sends), receive(t, sends)
			require.Equal(t, int64(0), first.entries[0].EntryId)
			require.Equal(t, int64(1), second.entries[0].EntryId)
			receive(t, blocked)
			require.NoError(t, first.channels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrFileWriterSyncFailed}))
			// Observe apply(), not just channel retirement, before unblocking the send.
			require.Eventually(t, func() bool { return testutil.ToFloat64(errors) == before+1 }, 5*time.Second, time.Millisecond)
			require.False(t, h.rollingState.Load())
			require.Empty(t, callbacks)
			require.Empty(t, sends, "a retry cannot overlap the active send")
			release()
			retry, third := receive(t, sends), receive(t, sends)
			require.Equal(t, int64(0), retry.entries[0].EntryId, "retry the known failure before entry 2")
			require.Equal(t, int64(2), third.entries[0].EntryId)
			require.NotSame(t, first.channels[0], retry.channels[0])
			for id := range 3 {
				result := receive(t, callbacks)
				require.Equal(t, int64(id), result.id)
				require.NoError(t, result.err)
			}
			waitForAppenderDrained(t, h)
			require.False(t, overlapped.Load())
			require.Equal(t, int32(2), firstAttempts.Load())
			require.Empty(t, sends)
			require.Empty(t, callbacks)
			require.Equal(t, int64(2), h.confirmedLAC())
			require.False(t, h.rollingState.Load())
		})
	}
}

func TestReplicaAppenderTerminalACKCancelsBlockedSend(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "embed", true: "service"}[service], func(t *testing.T) {
			c, sends := controlledAppenderClient(service)
			send := c.send
			cancelled := make(chan struct{})
			c.send = func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
				if err := send(ctx, es, chs); err != nil {
					return err
				}
				if es[0].EntryId == 1 {
					<-ctx.Done()
					close(cancelled)
					return ctx.Err()
				}
				return nil
			}
			clients := []*appenderTestClient{c}
			if service {
				clients = append(clients, &appenderTestClient{remote: true, send: ackEntries}, &appenderTestClient{remote: true, send: ackEntries})
			}
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 2, MaxRetries: 3, MaxBatchEntries: 1}, clients...)
			callbacks := appendSequenceForTest(h, 2)
			first, second := receive(t, sends), receive(t, sends)
			require.Equal(t, int64(0), first.entries[0].EntryId)
			require.Equal(t, int64(1), second.entries[0].EntryId)
			if service {
				for id := range 2 {
					result := receive(t, callbacks)
					require.Equal(t, int64(id), result.id)
					require.NoError(t, result.err)
				}
			}
			require.NoError(t, first.channels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrSegmentFenced}))
			receive(t, cancelled)
			require.Eventually(t, h.rollingState.Load, 5*time.Second, time.Millisecond)
			if !service {
				for id := range 2 {
					result := receive(t, callbacks)
					require.Equal(t, int64(id), result.id)
					require.ErrorIs(t, result.err, werr.ErrSegmentFenced)
				}
			}
			waitForAppenderDrained(t, h)
			h.appender.stop()
			h.appender.wait()
			require.True(t, first.channels[0].IsClosed())
			require.True(t, second.channels[0].IsClosed(), "shutdown joins the cancelled send and retires its channel")
			require.Empty(t, sends, "terminal errors must not retry")
			require.Empty(t, callbacks, "late terminal failure must not repeat quorum callbacks")
		})
	}
}
