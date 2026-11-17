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

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

func TestSegmentAppenderOwnsQuorumAndOrderedCompletion(t *testing.T) {
	q := &proto.QuorumInfo{Wq: 3, Aq: 2, Es: 3, Nodes: []string{"a", "b", "c"}}
	pool := &appenderTestPool{clients: make(map[string]*appenderTestClient)}
	sends := make([]<-chan capturedAppend, 3)
	for i, address := range q.Nodes {
		pool.clients[address], sends[i] = controlledAppenderClient(true)
	}
	failures, drained := make(chan string, 4), make(chan struct{}, 4)
	cfg := testAppenderConfiguration(config.SegmentAppendConfig{QueueSize: 2, MaxRetries: 1, MaxBatchEntries: 1}, true)
	g := newSegmentAppender(pool, cfg, 1, &proto.SegmentMetadata{SegNo: 1, LastEntryId: -1, Quorum: q}, segmentAppendCallbacks{
		onReplicaFailure: func(address string, _ error) { failures <- address },
		onDrained:        func() { drained <- struct{}{} },
	})
	t.Cleanup(func() { g.closePending(-1, werr.ErrSegmentHandleSegmentClosed); g.wait() })
	callbacks := make(chan appendCallbackResult, 3)
	for range 2 {
		reservation, err := g.reserve(context.Background(), 1)
		require.NoError(t, err)
		require.NoError(t, g.append(context.Background(), []byte("v"), func(_, id int64, err error) {
			callbacks <- appendCallbackResult{id: id, err: err}
		}, reservation))
	}
	firsts, seconds := make([]capturedAppend, 3), make([]capturedAppend, 3)
	for i := range sends {
		firsts[i], seconds[i] = receive(t, sends[i]), receive(t, sends[i])
		require.Equal(t, int64(0), firsts[i].entries[0].EntryId)
		require.Equal(t, int64(1), seconds[i].entries[0].EntryId)
	}
	for i := range 2 {
		require.NoError(t, ackEntries(seconds[i].ctx, seconds[i].entries, seconds[i].channels))
	}
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		e := g.findAppendOpByIDUnsafe(1)
		return e != nil && e.Value.(*AppendOp).completed.Load()
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, int64(-1), g.lastAddConfirmed.Load())
	require.Empty(t, callbacks)
	for i := range 2 {
		require.NoError(t, ackEntries(firsts[i].ctx, firsts[i].entries, firsts[i].channels))
	}
	for id := range 2 {
		result := receive(t, callbacks)
		require.Equal(t, int64(id), result.id)
		require.NoError(t, result.err)
	}
	require.Equal(t, int64(1), g.lastAddConfirmed.Load())
	require.Equal(t, int64(2), g.commitedSize.Load())
	require.Zero(t, g.submittedSize.Load())
	// A late replica failure still requests rolling even without a handle or pending logical ops.
	require.NoError(t, firsts[2].channels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrSegmentFenced}))
	require.Equal(t, "c", receive(t, failures))
	receive(t, drained)
	_, err := g.reserve(context.Background(), 1)
	require.ErrorIs(t, err, werr.ErrSegmentHandleSegmentRolling)
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.count == 0 && g.bytes == 0 && len(g.records) == 0
	}, 5*time.Second, time.Millisecond)
	require.Empty(t, callbacks)
}

func TestSegmentAppenderFailureNotificationDoesNotHoldAppenderLock(t *testing.T) {
	clients := make([]*appenderTestClient, 3)
	sends := make([]<-chan capturedAppend, 3)
	for i := range clients {
		clients[i], sends[i] = controlledAppenderClient(true)
	}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 1}, clients...)
	result := appendForTest(h, []byte("v"))
	attempts := make([]capturedAppend, 3)
	for i := range sends {
		attempts[i] = receive(t, sends[i])
	}
	h.Lock()
	unlock := sync.OnceFunc(h.Unlock)
	defer unlock()
	require.NoError(t, attempts[0].channels[0].SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrSegmentFenced}))
	require.Eventually(t, func() bool {
		h.appender.mu.Lock()
		defer h.appender.mu.Unlock()
		return h.appender.admissionStopped
	}, 5*time.Second, time.Millisecond)
	for i := 1; i < 3; i++ {
		require.NoError(t, ackEntries(attempts[i].ctx, attempts[i].entries, attempts[i].channels))
	}
	require.NoError(t, receive(t, result), "quorum completion must not wait for the handle lifecycle lock")
	require.Equal(t, int64(0), h.confirmedLAC())
	unlock()
	require.Eventually(t, h.rollingState.Load, 5*time.Second, time.Millisecond)
	waitForAppenderDrained(t, h)
}

func TestSegmentAppenderCloseRejectsReservedAppendWithoutAllocatingID(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(fmt.Sprintf("service=%t", service), func(t *testing.T) {
			c, sends := controlledAppenderClient(service)
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 1}, c)
			h.Lock()
			unlock := sync.OnceFunc(h.Unlock)
			defer unlock()
			done := make(chan appendCallbackResult, 1)
			go h.AppendAsync(context.Background(), []byte("v"), func(_, id int64, err error) {
				done <- appendCallbackResult{id: id, err: err}
			})
			require.Eventually(t, func() bool {
				h.appender.mu.Lock()
				defer h.appender.mu.Unlock()
				return h.appender.count == 1 && len(h.appender.records) == 0
			}, 5*time.Second, time.Millisecond)
			require.True(t, h.stopWritingUnsafe(context.Background(), -1))
			unlock()
			result := receive(t, done)
			require.Equal(t, int64(-1), result.id)
			require.ErrorIs(t, result.err, werr.ErrSegmentHandleSegmentClosed)
			h.appender.wait()
			require.Equal(t, int64(-1), h.appender.lastPushed.Load())
			requireAppenderCredits(t, h, 0, 0)
			require.Empty(t, sends)
		})
	}
}

func TestSegmentAppenderCloseCompletesOnlyFinalDurablePrefix(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(fmt.Sprintf("service=%t", service), func(t *testing.T) {
			c, sends := controlledAppenderClient(service)
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 2, MaxRetries: 1, MaxBatchEntries: 1}, c)
			callbacks := appendSequenceForTest(h, 2)
			first, second := receive(t, sends), receive(t, sends)
			h.appender.closePending(0, werr.ErrSegmentFenced)
			h.appender.wait()
			result := receive(t, callbacks)
			require.Equal(t, int64(0), result.id)
			require.NoError(t, result.err)
			result = receive(t, callbacks)
			require.Equal(t, int64(1), result.id)
			require.ErrorIs(t, result.err, werr.ErrSegmentFenced)
			require.Equal(t, int64(0), h.confirmedLAC())
			require.Equal(t, int64(1), h.GetSize(context.Background()))
			require.Zero(t, h.appender.submittedSize.Load())
			requireAppenderCredits(t, h, 0, 0)
			require.True(t, first.channels[0].IsClosed())
			require.True(t, second.channels[0].IsClosed())
			require.Empty(t, callbacks)
		})
	}
}

func TestSegmentAppenderRollingWaitsForCallbacksButNotReplicaIO(t *testing.T) {
	silent := &appenderTestClient{send: func(ctx context.Context, _ []*proto.LogEntry, _ []channel.ResultChannel) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 1}, silent, &appenderTestClient{send: ackEntries}, &appenderTestClient{send: ackEntries})
	h.completionMgr = &completionManager{triggerCh: make(chan struct{}, 1)}
	entered, releaseCallback := make(chan error, 1), make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseCallback) })
	defer release()
	h.AppendAsync(context.Background(), []byte("v"), func(_, _ int64, err error) {
		entered <- err
		<-releaseCallback
	})
	require.NoError(t, receive(t, entered))
	require.NoError(t, h.SetRollingReady(context.Background()))
	require.Empty(t, h.completionMgr.triggerCh, "rolling waits for the ordered callback")
	requireAppenderCredits(t, h, 1, 1)
	release()
	receive(t, h.completionMgr.triggerCh)
	require.True(t, h.appender.drained())
	requireAppenderCredits(t, h, 1, 1) // the silent replica is still in flight
}

func TestSegmentAppenderIgnoresDuplicateFinalReplicaResults(t *testing.T) {
	for _, failedFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed-first=%t", failedFirst), func(t *testing.T) {
			c, _ := controlledAppenderClient(true)
			h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1}, c, c, c)
			g := h.appender
			var failures atomic.Int32
			notifyFailure := g.notify.onReplicaFailure
			g.notify.onReplicaFailure = func(address string, err error) {
				failures.Add(1)
				notifyFailure(address, err)
			}
			callbacks := appendSequenceForTest(h, 1)
			result := replicaResult{entryID: 0, replica: 0, address: "replica-0"}
			if failedFirst {
				result.err = werr.ErrSegmentFenced
			}
			g.handleReplicaResult(result)
			g.handleReplicaResult(result)
			// Even a contradictory duplicate must not change the first outcome.
			if failedFirst {
				result.err = nil
			} else {
				result.err = werr.ErrSegmentFenced
			}
			g.handleReplicaResult(result)
			require.Empty(t, callbacks, "one replica cannot form a quorum or a permanent hole")
			require.Equal(t, int64(-1), h.confirmedLAC())
			requireAppenderCredits(t, h, 1, 1)
			if failedFirst {
				require.Equal(t, int32(1), failures.Load())
			} else {
				require.Zero(t, failures.Load())
			}
			require.Equal(t, failedFirst, h.rollingState.Load())
			g.handleReplicaResult(replicaResult{entryID: 0, replica: 1, address: "replica-1"})
			if failedFirst {
				require.Empty(t, callbacks)
			}
			g.handleReplicaResult(replicaResult{entryID: 0, replica: 2, address: "replica-2"})
			completion := receive(t, callbacks)
			require.Equal(t, int64(0), completion.id)
			require.NoError(t, completion.err)
			waitForAppenderDrained(t, h)
			require.Equal(t, int64(0), h.confirmedLAC())
			require.Equal(t, int64(1), g.commitedSize.Load())
			require.Zero(t, g.submittedSize.Load())
			require.Empty(t, callbacks)
		})
	}
}

func TestSegmentAppenderCloseRacesWithFinalResult(t *testing.T) {
	for _, order := range []string{"close-first", "result-first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			for range 20 {
				c, _ := controlledAppenderClient(false)
				h := appenderFixture(t, false, config.SegmentAppendConfig{QueueSize: 1}, c)
				callbacks := appendSequenceForTest(h, 1)
				g := h.appender
				closeWriter := func() { g.closePending(0, werr.ErrSegmentHandleSegmentClosed) }
				finish := func() { g.handleReplicaResult(replicaResult{entryID: 0, replica: 0}) }
				switch order {
				case "close-first":
					closeWriter()
					finish()
				case "result-first":
					finish()
					closeWriter()
				case "concurrent":
					start, finished := make(chan struct{}), make(chan struct{})
					go func() { <-start; finish(); close(finished) }()
					close(start)
					closeWriter()
					receive(t, finished)
				}
				g.wait()
				// Neither a late success nor a late failure can rewrite Close's outcome.
				finish()
				g.handleReplicaResult(replicaResult{entryID: 0, replica: 0, err: werr.ErrSegmentFenced})
				completion := receive(t, callbacks)
				require.Equal(t, int64(0), completion.id)
				require.NoError(t, completion.err)
				require.Empty(t, callbacks)
				require.Equal(t, int64(0), h.confirmedLAC())
				require.Equal(t, int64(1), g.commitedSize.Load())
				require.Zero(t, g.submittedSize.Load())
				require.False(t, h.rollingState.Load())
				requireAppenderCredits(t, h, 0, 0)
			}
		})
	}
}

func TestSegmentAppenderCloseDuringFailureNotification(t *testing.T) {
	c, _ := controlledAppenderClient(false)
	h := appenderFixture(t, false, config.SegmentAppendConfig{QueueSize: 2}, c)
	g := h.appender
	entered, releaseHook := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseHook) })
	defer release()
	g.notify.onReplicaFailure = func(string, error) { close(entered); <-releaseHook }
	callbacks := appendSequenceForTest(h, 2)
	finished := make(chan struct{})
	go func() {
		g.handleReplicaResult(replicaResult{entryID: 0, replica: 0, err: werr.ErrSegmentFenced})
		close(finished)
	}()
	receive(t, entered) // handleReplicaResult has released g.mu to notify its owner
	g.closePending(0, werr.ErrSegmentHandleSegmentClosed)
	release()
	receive(t, finished)
	g.wait()
	first, second := receive(t, callbacks), receive(t, callbacks)
	require.Equal(t, int64(0), first.id)
	require.NoError(t, first.err, "Close's durable prefix wins over the in-flight failure")
	require.Equal(t, int64(1), second.id)
	require.ErrorIs(t, second.err, werr.ErrSegmentHandleSegmentClosed)
	require.Equal(t, int64(0), h.confirmedLAC())
	require.Equal(t, int64(1), g.commitedSize.Load())
	require.Zero(t, g.submittedSize.Load())
	requireAppenderCredits(t, h, 0, 0)
	require.Empty(t, callbacks)
}
