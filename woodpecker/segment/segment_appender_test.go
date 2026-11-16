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
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/topology"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

type appenderTestClient struct {
	client.LogStoreClient
	remote bool
	send   func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error
}

func (c *appenderTestClient) IsRemoteClient() bool { return c.remote }
func (c *appenderTestClient) AppendEntry(ctx context.Context, _, _ string, _ int64, e *proto.LogEntry, ch channel.ResultChannel) (int64, error) {
	return e.EntryId, c.send(ctx, []*proto.LogEntry{e}, []channel.ResultChannel{ch})
}

func (c *appenderTestClient) AppendEntries(ctx context.Context, _, _ string, _ int64, es []*proto.LogEntry, chs []channel.ResultChannel) ([]int64, error) {
	return nil, c.send(ctx, es, chs)
}

type appenderTestPool struct {
	client.LogStoreClientPool
	clients map[string]*appenderTestClient
	getErr  error
	gets    atomic.Int32
}

func (p *appenderTestPool) GetLogStoreClient(_ context.Context, address string) (client.LogStoreClient, error) {
	p.gets.Add(1)
	if p.getErr != nil {
		return nil, p.getErr
	}
	return p.clients[address], nil
}

func ackEntries(ctx context.Context, entries []*proto.LogEntry, chs []channel.ResultChannel) error {
	for i, e := range entries {
		if err := chs[i].SendResult(ctx, &channel.AppendResult{SyncedId: e.EntryId}); err != nil {
			return err
		}
	}
	return nil
}

func appenderFixture(t *testing.T, service bool, cfg config.SegmentAppendConfig, clients ...*appenderTestClient) *segmentHandleImpl {
	t.Helper()
	q := &proto.QuorumInfo{Es: int32(len(clients)), Wq: int32(len(clients)), Aq: int32(len(clients)/2 + 1)}
	pool := &appenderTestPool{clients: make(map[string]*appenderTestClient)}
	for i, c := range clients {
		address := fmt.Sprintf("replica-%d", i)
		q.Nodes = append(q.Nodes, address)
		pool.clients[address] = c
	}
	h := &segmentHandleImpl{
		bucketName: "b", rootPath: "r", logId: 1, segmentId: 1,
		logNs: metrics.BuildLogNs("b", "r"), appendOpsQueue: list.New(), quorumInfo: q, ClientPool: pool,
		cfg: &config.Configuration{}, lacSyncNotify: make(chan struct{}, 1),
	}
	h.lastPushed.Store(-1)
	h.lastAddConfirmed.Store(-1)
	h.canWriteState.Store(true)
	h.lacSyncStarted.Store(true) // no metadata/network LAC sync in this fixture
	h.segmentMetaCache.Store(&meta.SegmentMeta{Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1, Quorum: q}})
	h.appender = newSegmentAppender(pool, q, cfg, service, h.handleReplicaResult)
	t.Cleanup(func() {
		h.Lock()
		h.fastFailAppendOpsUnsafe(context.Background(), -1, werr.ErrSegmentHandleSegmentClosed)
		h.appender.stop()
		h.Unlock()
		h.appender.wait()
		h.callbackWG.Wait()
		h.appender.mu.Lock()
		defer h.appender.mu.Unlock()
		require.Zero(t, h.appender.count, "all admission credits must be released")
		require.Zero(t, h.appender.bytes)
		require.Empty(t, h.appender.records)
	})
	return h
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for append progress")
		var zero T
		return zero
	}
}

func appendForTest(h *segmentHandleImpl, value []byte) <-chan error {
	done := make(chan error, 1)
	h.AppendAsync(context.Background(), value, func(_, _ int64, err error) { done <- err })
	return done
}

func TestSegmentAppenderSilentReplicaDoesNotBlockQuorum(t *testing.T) {
	for _, batch := range []int{1, 4} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			entered := make(chan struct{})
			var once sync.Once
			silent := &appenderTestClient{send: func(ctx context.Context, _ []*proto.LogEntry, _ []channel.ResultChannel) error {
				once.Do(func() { close(entered) })
				<-ctx.Done()
				return ctx.Err()
			}}
			h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 16, MaxRetries: 3, MaxBatchEntries: batch}, silent, &appenderTestClient{send: ackEntries}, &appenderTestClient{send: ackEntries})
			done := make(chan int64, 8)
			for range 8 {
				h.AppendAsync(context.Background(), []byte("value"), func(_, id int64, err error) { require.NoError(t, err); done <- id })
			}
			receive(t, entered)
			for i := range 8 {
				require.Equal(t, int64(i), receive(t, done))
			}
			require.False(t, h.rollingState.Load())
			h.appender.mu.Lock()
			require.Equal(t, 8, h.appender.count, "quorum success retains the silent replica's payloads")
			h.appender.mu.Unlock()
		})
	}
}

func TestReplicaAppenderRetriesBeforeLaterSends(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			entered, failNow := make(chan struct{}), make(chan struct{})
			var mu sync.Mutex
			var sent []int64
			var channels []channel.ResultChannel
			c := &appenderTestClient{remote: remote, send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
				mu.Lock()
				sent = append(sent, es[0].EntryId)
				channels = append(channels, chs[0])
				attempt := len(sent)
				mu.Unlock()
				if attempt == 1 {
					close(entered)
					select {
					case <-failNow:
						return werr.ErrTimeoutError
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return ackEntries(ctx, es, chs)
			}}
			h := appenderFixture(t, remote, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, c)
			first := appendForTest(h, []byte("first"))
			receive(t, entered)
			second := appendForTest(h, []byte("second"))
			close(failNow)
			require.NoError(t, receive(t, first))
			require.NoError(t, receive(t, second))
			require.False(t, h.rollingState.Load())
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []int64{0, 0, 1}, sent)
			require.NotSame(t, channels[0], channels[1], "each attempt owns a fresh channel")
			require.True(t, channels[0].IsClosed())
		})
	}
}

func TestReplicaAppenderACKRetriesAfterQuorumSuccess(t *testing.T) {
	releaseRetry, retryStarted := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	a := &appenderTestClient{send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
		if attempts.Add(1) == 1 {
			return chs[0].SendResult(ctx, &channel.AppendResult{SyncedId: -1, Err: werr.ErrFileWriterSyncFailed})
		}
		close(retryStarted)
		select {
		case <-releaseRetry:
			return ackEntries(ctx, es, chs)
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 3}, a, &appenderTestClient{send: ackEntries}, &appenderTestClient{send: ackEntries})
	require.NoError(t, receive(t, appendForTest(h, []byte("v"))))
	receive(t, retryStarted)
	require.False(t, h.rollingState.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := h.appender.reserve(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(releaseRetry)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	reservation, err := h.appender.reserve(ctx2, 1)
	require.NoError(t, err)
	reservation.release()
	require.Equal(t, int32(2), attempts.Load())
	require.False(t, h.rollingState.Load())
}

func TestReplicaAppenderRetryExhaustionAndNonRetryableErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		attempts int
	}{
		{"retryable", werr.ErrTimeoutError, 3},
		{"fenced", werr.ErrSegmentFenced, 1},
		{"not-writable", werr.ErrSegmentHandleSegmentClosed, 1},
		{"raw-grpc-unavailable", status.Error(codes.Unavailable, "disconnected"), 1},
		{"connection-failed", werr.ErrWoodpeckerClientConnectionFailed, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			c := &appenderTestClient{send: func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error {
				attempts.Add(1)
				return tc.err
			}}
			h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, c)
			require.Error(t, receive(t, appendForTest(h, []byte("v"))))
			require.Equal(t, int32(tc.attempts), attempts.Load())
			require.True(t, h.rollingState.Load())
		})
	}
}

func TestReplicaAppenderRetryDoesNotBlockHealthyReplicas(t *testing.T) {
	retrying := make(chan struct{})
	var attempts atomic.Int32
	a := &appenderTestClient{send: func(ctx context.Context, _ []*proto.LogEntry, _ []channel.ResultChannel) error {
		if attempts.Add(1) == 1 {
			return werr.ErrTimeoutError
		}
		close(retrying)
		<-ctx.Done()
		return ctx.Err()
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 4, MaxRetries: 3}, a, &appenderTestClient{send: ackEntries}, &appenderTestClient{send: ackEntries})
	require.NoError(t, receive(t, appendForTest(h, []byte("first"))))
	receive(t, retrying)
	for range 3 {
		require.NoError(t, receive(t, appendForTest(h, []byte("next"))))
	}
	require.Equal(t, int64(3), h.lastAddConfirmed.Load())
	require.False(t, h.rollingState.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := h.appender.reserve(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the lagging replica must still bound admitted work")
}

func TestReplicaAppenderLateExhaustionRollsCompletedEntry(t *testing.T) {
	release := make(chan struct{})
	var attempts, callbacks atomic.Int32
	a := &appenderTestClient{send: func(ctx context.Context, _ []*proto.LogEntry, _ []channel.ResultChannel) error {
		attempts.Add(1)
		select {
		case <-release:
			return werr.ErrTimeoutError
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, a, &appenderTestClient{send: ackEntries}, &appenderTestClient{send: ackEntries})
	done := make(chan struct{})
	h.AppendAsync(context.Background(), []byte("v"), func(_, _ int64, err error) { require.NoError(t, err); callbacks.Add(1); close(done) })
	receive(t, done)
	require.False(t, h.rollingState.Load())
	close(release)
	require.Eventually(t, h.rollingState.Load, time.Second, time.Millisecond)
	require.Equal(t, int32(3), attempts.Load())
	require.Equal(t, int32(1), callbacks.Load())
}

func TestReplicaAppenderClientLookupFailureUsesRetryBudget(t *testing.T) {
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, &appenderTestClient{send: ackEntries})
	pool := h.ClientPool.(*appenderTestPool)
	pool.getErr = werr.ErrWoodpeckerClientConnectionFailed
	require.ErrorIs(t, receive(t, appendForTest(h, []byte("v"))), werr.ErrWoodpeckerClientConnectionFailed)
	require.Equal(t, int32(3), pool.gets.Load())
	require.True(t, h.rollingState.Load())
}

func TestSegmentAppenderOrderedCallbacksRunOutsideLock(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{})
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 2, MaxRetries: 2}, &appenderTestClient{send: ackEntries})
	callbacks := make(chan int64, 2)
	h.AppendAsync(context.Background(), []byte("first"), func(_, id int64, err error) {
		require.NoError(t, err)
		h.RLock()
		_ = h.lastAddConfirmed.Load()
		h.RUnlock() // callback may inspect its segment
		close(entered)
		<-release
		callbacks <- id
	})
	receive(t, entered)
	h.AppendAsync(context.Background(), []byte("second"), func(_, id int64, err error) { require.NoError(t, err); callbacks <- id })
	require.Eventually(t, func() bool { return h.lastAddConfirmed.Load() == 1 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := h.appender.reserve(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded, "completed replication still retains ordered callback credit")
	close(release)
	require.Equal(t, int64(0), receive(t, callbacks))
	require.Equal(t, int64(1), receive(t, callbacks))
}

func TestSegmentAppenderByteBudgetAndOversizedEntry(t *testing.T) {
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 3, MaxPendingBytes: config.ByteSize(10)}, &appenderTestClient{send: ackEntries})
	first, err := h.appender.reserve(context.Background(), 8)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = h.appender.reserve(ctx, 3)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	first.release()
	first.release() // abandonment is idempotent
	large, err := h.appender.reserve(context.Background(), 20)
	require.NoError(t, err)
	large.release()
}

func TestSegmentAppenderStopUnblocksAdmissionAndACKReaders(t *testing.T) {
	entered := make(chan struct{})
	c := &appenderTestClient{send: func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error { close(entered); return nil }}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 3}, c)
	result := appendForTest(h, []byte("v"))
	receive(t, entered)
	waiting := make(chan error, 1)
	go func() { _, err := h.appender.reserve(context.Background(), 1); waiting <- err }()
	h.Lock()
	h.fastFailAppendOpsUnsafe(context.Background(), -1, werr.ErrSegmentHandleSegmentClosed)
	h.appender.stop()
	h.Unlock()
	require.Error(t, receive(t, waiting))
	require.Error(t, receive(t, result))
	joined := make(chan struct{})
	go func() { h.appender.wait(); close(joined) }()
	receive(t, joined)
	require.False(t, h.rollingState.Load(), "local cancellation must not create a failure-driven roll")
}

func TestSegmentAppenderStopReclaimsWithoutExplicitJoin(t *testing.T) {
	ack := make(chan channel.ResultChannel, 1)
	c := &appenderTestClient{send: func(_ context.Context, _ []*proto.LogEntry, chs []channel.ResultChannel) error {
		ack <- chs[0]
		return nil
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 3}, c)
	result := appendForTest(h, []byte("v"))
	ch := receive(t, ack)
	h.Lock()
	h.fastFailAppendOpsUnsafe(context.Background(), -1, werr.ErrSegmentFenced)
	h.appender.stop()
	h.Unlock()
	require.ErrorIs(t, receive(t, result), werr.ErrSegmentFenced)
	require.Eventually(t, func() bool {
		h.appender.mu.Lock()
		defer h.appender.mu.Unlock()
		return len(h.appender.records) == 0 && h.appender.count == 0 && h.appender.bytes == 0
	}, time.Second, time.Millisecond)
	require.True(t, ch.IsClosed())
}

func TestSegmentAppenderQuorumFailureFailsSuffixInOrder(t *testing.T) {
	release := make(chan struct{})
	fail := &appenderTestClient{send: func(ctx context.Context, _ []*proto.LogEntry, _ []channel.ResultChannel) error {
		select {
		case <-release:
			return werr.ErrFileWriterNoSpace
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, fail, fail, &appenderTestClient{send: ackEntries})
	done := make(chan int64, 4)
	for range 4 {
		h.AppendAsync(context.Background(), []byte("v"), func(_, id int64, err error) { require.Error(t, err); done <- id })
	}
	close(release)
	for i := range 4 {
		require.Equal(t, int64(i), receive(t, done))
	}
	require.Equal(t, int64(-1), h.lastAddConfirmed.Load())
	require.True(t, h.rollingState.Load())
}

func TestSegmentAppenderCancelledAdmissionDoesNotAllocateEntryID(t *testing.T) {
	acks := make(chan channel.ResultChannel, 2)
	c := &appenderTestClient{send: func(_ context.Context, _ []*proto.LogEntry, chs []channel.ResultChannel) error {
		acks <- chs[0]
		return nil
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1}, c)
	first := appendForTest(h, []byte("first"))
	ack := receive(t, acks)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var rejectedID int64
	var rejectedErr error
	h.AppendAsync(ctx, []byte("cancelled"), func(_, id int64, err error) { rejectedID, rejectedErr = id, err })
	require.ErrorIs(t, rejectedErr, context.DeadlineExceeded)
	require.Equal(t, int64(-1), rejectedID)
	require.Equal(t, int64(0), h.lastPushed.Load())
	require.NoError(t, ack.SendResult(context.Background(), &channel.AppendResult{SyncedId: 0}))
	require.NoError(t, receive(t, first))
	next := appendForTest(h, []byte("next"))
	require.Equal(t, int64(1), h.lastPushed.Load())
	require.NoError(t, receive(t, acks).SendResult(context.Background(), &channel.AppendResult{SyncedId: 1}))
	require.NoError(t, receive(t, next))
}

func TestSegmentAppenderEmbeddedAdmissionDoesNotWaitForDurableACK(t *testing.T) {
	acks := make(chan channel.ResultChannel, 2)
	c := &appenderTestClient{send: func(_ context.Context, _ []*proto.LogEntry, chs []channel.ResultChannel) error {
		acks <- chs[0]
		return nil
	}}
	h := appenderFixture(t, false, config.SegmentAppendConfig{QueueSize: 1, MaxPendingBytes: 1}, c)
	first := appendForTest(h, []byte("larger than the service byte limit"))
	firstACK := receive(t, acks)
	second := make(chan error, 1)
	go h.AppendAsync(context.Background(), []byte("another oversized entry"), func(_, _ int64, err error) { second <- err })
	secondACK := receive(t, acks) // must send before the first entry is durable
	require.Equal(t, int64(-1), h.lastAddConfirmed.Load())
	require.NoError(t, firstACK.SendResult(context.Background(), &channel.AppendResult{SyncedId: 0}))
	require.NoError(t, secondACK.SendResult(context.Background(), &channel.AppendResult{SyncedId: 1}))
	require.NoError(t, receive(t, first))
	require.NoError(t, receive(t, second))
}

func TestReplicaAppenderLateOutcomesAndLocalCancellationMetrics(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			lateACK := make(chan channel.ResultChannel, 1)
			c := &appenderTestClient{send: func(_ context.Context, _ []*proto.LogEntry, chs []channel.ResultChannel) error {
				lateACK <- chs[0]
				return nil
			}}
			ok := metrics.WpClientReplicaAppendTotal.WithLabelValues(metrics.BuildLogNs("b", "r"), "1", topology.ScopeUnknown, "success")
			failed := metrics.WpClientReplicaAppendTotal.WithLabelValues(metrics.BuildLogNs("b", "r"), "1", topology.ScopeUnknown, "error")
			bytes := metrics.WpClientReplicaAppendBytesTotal.WithLabelValues(metrics.BuildLogNs("b", "r"), "1", topology.ScopeUnknown)
			beforeOK, beforeFailed, beforeBytes := testutil.ToFloat64(ok), testutil.ToFloat64(failed), testutil.ToFloat64(bytes)
			h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 1, MaxRetries: 1}, c, &appenderTestClient{send: ackEntries}, &appenderTestClient{send: ackEntries})
			require.NoError(t, receive(t, appendForTest(h, []byte("value"))))
			ack := receive(t, lateACK)
			require.False(t, ack.IsClosed(), "quorum success must preserve the third replica's channel")
			wantOK, wantFailed := 2.0, 0.0
			switch outcome {
			case "success":
				require.NoError(t, ack.SendResult(context.Background(), &channel.AppendResult{SyncedId: 0}))
				wantOK++
			case "failure":
				require.NoError(t, ack.SendResult(context.Background(), &channel.AppendResult{SyncedId: -1, Err: werr.ErrFileWriterSyncFailed}))
				wantFailed++
			case "shutdown":
				h.appender.stop()
				h.appender.wait()
			}
			require.Eventually(t, func() bool {
				h.appender.mu.Lock()
				defer h.appender.mu.Unlock()
				return len(h.appender.records) == 0
			}, time.Second, time.Millisecond)
			require.True(t, ack.IsClosed())
			require.Equal(t, wantOK, testutil.ToFloat64(ok)-beforeOK)
			require.Equal(t, wantFailed, testutil.ToFloat64(failed)-beforeFailed)
			require.Equal(t, wantOK*5, testutil.ToFloat64(bytes)-beforeBytes)
		})
	}
}
