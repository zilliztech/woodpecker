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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

type senderTestClient struct {
	client.LogStoreClient
	send func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error
}

func (*senderTestClient) IsRemoteClient() bool { return false }
func (c *senderTestClient) AppendEntry(ctx context.Context, _, _ string, _ int64, e *proto.LogEntry, ch channel.ResultChannel) (int64, error) {
	return e.EntryId, c.send(ctx, []*proto.LogEntry{e}, []channel.ResultChannel{ch})
}

func (c *senderTestClient) AppendEntries(ctx context.Context, _, _ string, _ int64, e []*proto.LogEntry, ch []channel.ResultChannel) ([]int64, error) {
	return nil, c.send(ctx, e, ch)
}

type senderTestPool struct {
	client.LogStoreClientPool
	clients map[string]*senderTestClient
}

func (p *senderTestPool) GetLogStoreClient(_ context.Context, addr string) (client.LogStoreClient, error) {
	return p.clients[addr], nil
}

type senderTestHandle struct {
	SegmentHandle
	success func(int64)
	failure func(int64, error, int, string)
}

func (h *senderTestHandle) SendAppendSuccessCallbacks(_ context.Context, id int64) { h.success(id) }
func (h *senderTestHandle) HandleAppendRequestFailure(_ context.Context, id int64, e error, i int, a string) {
	h.failure(id, e, i, a)
}

func TestFastSuccessDoesNotWaitForSendingMutex(t *testing.T) {
	var calls atomic.Int32
	op := NewAppendOp("b", "r", 1, 1, 0, nil, func(int64, int64, error) { calls.Add(1) }, nil, nil, &proto.QuorumInfo{Nodes: []string{"a"}}, nil)
	op.mu.Lock()
	done := make(chan struct{})
	go func() { op.FastSuccess(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		op.mu.Unlock()
		t.Fatal("durable success waited for send mutex")
	}
	op.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op.FastFail(context.Background(), errors.New("late"))
			op.FastSuccess(context.Background())
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
}

func TestReplicaSenderIsolatesBlockedReplica(t *testing.T) {
	for _, batch := range []int{1, 4} {
		t.Run(string(rune('0'+batch)), func(t *testing.T) {
			q := &proto.QuorumInfo{Nodes: []string{"a", "b", "c"}, Es: 3, Wq: 3, Aq: 2}
			g := newReplicaSenders(&segmentHandleImpl{quorumInfo: q}, 16, 1024)
			executor := NewBatchingSequentialExecutor(16, batch, 1024)
			executor.replicas = g
			blocked := make(chan struct{})
			failure := make(chan struct{}, 16)
			var once sync.Once
			healthy := func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
				for i, e := range es {
					require.NoError(t, chs[i].SendResult(ctx, &channel.AppendResult{SyncedId: e.EntryId}))
				}
				return nil
			}
			pool := &senderTestPool{clients: map[string]*senderTestClient{
				"a": {send: func(ctx context.Context, _ []*proto.LogEntry, _ []channel.ResultChannel) error {
					once.Do(func() { close(blocked) })
					<-ctx.Done()
					return ctx.Err()
				}},
				"b": {send: healthy}, "c": {send: healthy},
			}}
			ops := make([]*AppendOp, 8)
			callbacks := make(chan int64, 8)
			h := &senderTestHandle{success: func(id int64) { ops[id].FastSuccess(context.Background()) }, failure: func(int64, error, int, string) { failure <- struct{}{} }}
			for i := range ops {
				ops[i] = NewAppendOp("b", "r", 1, 1, int64(i), []byte("value"), func(_ int64, id int64, err error) { require.NoError(t, err); callbacks <- id }, pool, h, q, nil)
				lease, err := g.reserve(context.Background(), 5)
				require.NoError(t, err)
				ops[i].replicaLease = lease
				require.True(t, executor.Submit(context.Background(), ops[i]))
			}
			executor.Start(context.Background())
			t.Cleanup(func() { executor.Stop(context.Background()); g.stop(); g.wait() })
			<-blocked
			seen := map[int64]bool{}
			for range ops {
				select {
				case id := <-callbacks:
					require.False(t, seen[id])
					seen[id] = true
				case <-time.After(time.Second):
					t.Fatal("healthy quorum blocked behind silent replica")
				}
			}
			g.mu.Lock()
			retained := g.count
			g.mu.Unlock()
			require.Equal(t, 8, retained, "quorum success must retain third-replica credits")
			executor.Stop(context.Background())
			g.stop()
			g.wait()
			g.mu.Lock()
			retained = g.count
			g.mu.Unlock()
			require.Zero(t, retained)
			require.Empty(t, failure, "local cancellation must not mark a replica unhealthy")
		})
	}
}

func TestReplicaSenderLateFailureRollsCompletedEntry(t *testing.T) {
	q := &proto.QuorumInfo{Nodes: []string{"a", "b", "c"}, Es: 3, Wq: 3, Aq: 2}
	h := &segmentHandleImpl{quorumInfo: q, appendOpsQueue: list.New(), cfg: &config.Configuration{}}
	g := newReplicaSenders(h, 2, 10)
	h.replicaSenders = g
	t.Cleanup(func() { g.stop(); g.wait() })
	var calls atomic.Int32
	op := NewAppendOp("b", "r", 1, 1, 0, []byte("v"), func(int64, int64, error) { calls.Add(1) }, nil, h, q, nil)
	lease, err := g.reserve(context.Background(), 1)
	require.NoError(t, err)
	op.replicaLease = lease
	op.FastSuccess(context.Background())
	g.lanes[0].finish(op, time.Now(), nil, context.DeadlineExceeded)
	require.True(t, h.rollingState.Load())
	require.Contains(t, h.UnreachableReplicas(), "a")
	require.Equal(t, int32(1), calls.Load())
	_, err = g.reserve(context.Background(), 1)
	require.Error(t, err)
	lease.release()
}

func TestReplicaSenderAdmissionIncludesInFlightBytes(t *testing.T) {
	g := newReplicaSenders(&segmentHandleImpl{quorumInfo: &proto.QuorumInfo{Nodes: []string{"a"}}}, 3, 10)
	t.Cleanup(func() { g.stop(); g.wait() })
	first, err := g.reserve(context.Background(), 8)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = g.reserve(ctx, 3)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	first.release()
	second, err := g.reserve(context.Background(), 3)
	require.NoError(t, err)
	second.release()
	g.stopAdmission()
	_, err = g.reserve(context.Background(), 1)
	require.Error(t, err)
}

// A replica can accept Buffered and then never confirm durability. Its credit
// must remain occupied, and its eventual timeout must still roll after success.
func TestReplicaSenderBoundsMissingDurabilityACK(t *testing.T) {
	old := appendAckReadTimeout
	appendAckReadTimeout = 150 * time.Millisecond
	defer func() { appendAckReadTimeout = old }()
	q := &proto.QuorumInfo{Nodes: []string{"a", "b", "c"}, Es: 3, Wq: 3, Aq: 2}
	g := newReplicaSenders(&segmentHandleImpl{quorumInfo: q}, 1, 100)
	defer func() { g.stop(); g.wait() }()
	success := make(chan struct{})
	failed := make(chan error, 1)
	var op *AppendOp
	h := &senderTestHandle{success: func(int64) { op.FastSuccess(context.Background()) }, failure: func(_ int64, err error, _ int, _ string) { failed <- err }}
	healthy := func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
		return chs[0].SendResult(ctx, &channel.AppendResult{SyncedId: es[0].EntryId})
	}
	pool := &senderTestPool{clients: map[string]*senderTestClient{
		"a": {send: func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error { return nil }},
		"b": {send: healthy}, "c": {send: healthy},
	}}
	op = NewAppendOp("b", "r", 1, 1, 0, []byte("v"), func(int64, int64, error) { close(success) }, pool, h, q, nil)
	lease, err := g.reserve(context.Background(), 1)
	require.NoError(t, err)
	op.replicaLease = lease
	g.dispatch([]*AppendOp{op})
	select {
	case <-success:
	case <-time.After(time.Second):
		t.Fatal("no quorum success")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = g.reserve(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case err = <-failed:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("missing ACK never retired")
	}
}

func TestReplicaSenderTwoFailuresCompletePendingCallbacks(t *testing.T) {
	q := &proto.QuorumInfo{Nodes: []string{"a", "b", "c"}, Es: 3, Wq: 3, Aq: 2}
	h := &segmentHandleImpl{quorumInfo: q, appendOpsQueue: list.New(), cfg: &config.Configuration{}}
	h.lastAddConfirmed.Store(-1)
	g := newReplicaSenders(h, 8, 100)
	h.replicaSenders = g
	defer func() { g.stop(); g.wait() }()
	fail := func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error {
		return context.DeadlineExceeded
	}
	pool := &senderTestPool{clients: map[string]*senderTestClient{
		"a": {send: fail}, "b": {send: fail},
		"c": {send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
			for i, e := range es {
				_ = chs[i].SendResult(ctx, &channel.AppendResult{SyncedId: e.EntryId})
			}
			return nil
		}},
	}}
	callbacks := make(chan int64, 8)
	ops := make([]*AppendOp, 8)
	for i := range ops {
		op := NewAppendOp("b", "r", 1, 1, int64(i), []byte("v"), func(_ int64, id int64, err error) { require.Error(t, err); callbacks <- id }, pool, h, q, nil)
		lease, err := g.reserve(context.Background(), 1)
		require.NoError(t, err)
		op.replicaLease = lease
		ops[i] = op
		h.appendOpsQueue.PushBack(op)
	}
	g.dispatch(ops)
	seen := map[int64]bool{}
	for range ops {
		select {
		case id := <-callbacks:
			require.False(t, seen[id])
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatal("pending append left without callback")
		}
	}
	g.stop()
	g.wait()
	require.Empty(t, callbacks)
	require.True(t, h.rollingState.Load())
	require.Zero(t, h.appendOpsQueue.Len())
}

func TestReplicaSenderAdmissionIncludesOrderedCallbackWait(t *testing.T) {
	q := &proto.QuorumInfo{Nodes: []string{"a", "b", "c"}, Es: 3, Wq: 3, Aq: 2}
	g := newReplicaSenders(&segmentHandleImpl{quorumInfo: q}, 1, 10)
	defer func() { g.stop(); g.wait() }()
	// Reaching quorum does not imply the ordered callback ran: an earlier entry
	// can still be waiting for its ACKs. Keep the reservation until delivery.
	h := &senderTestHandle{success: func(int64) {}}
	op := NewAppendOp("b", "r", 1, 1, 1, []byte("v"), func(int64, int64, error) {}, nil, h, q, nil)
	lease, err := g.reserve(context.Background(), 1)
	require.NoError(t, err)
	op.replicaLease = lease
	for _, lane := range g.lanes {
		lane.finish(op, time.Now(), &channel.AppendResult{SyncedId: 1}, nil)
	}
	require.True(t, op.completed.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = g.reserve(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	op.FastSuccess(context.Background())
	next, err := g.reserve(context.Background(), 1)
	require.NoError(t, err)
	next.release()
}
