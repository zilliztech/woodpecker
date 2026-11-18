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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

type ackTimeoutCall struct {
	ids         []int64
	batch       bool
	hasDeadline bool
}

// The silent peer buffers requests and ACKs a batch's prefix, but never ACKs
// its last entry. The actual remote client and result channels enforce budgets.
type ackTimeoutServer struct {
	proto.UnimplementedLogStoreServer
	silent     bool
	attempts   atomic.Int32
	calls      chan ackTimeoutCall
	cancelled  chan struct{}
	allowRetry chan struct{}
}

func (s *ackTimeoutServer) beforeBuffered(ctx context.Context, ids []int64, batch bool) error {
	if !s.silent {
		return nil
	}
	_, hasDeadline := ctx.Deadline()
	attempt := s.attempts.Add(1)
	s.calls <- ackTimeoutCall{ids: ids, batch: batch, hasDeadline: hasDeadline}
	if attempt > 1 {
		// Hold the retry before Buffered so the test can observe that the first
		// ACK timeout caused a retry without already triggering rolling.
		select {
		case <-s.allowRetry:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *ackTimeoutServer) waitForCancellation(ctx context.Context) error {
	<-ctx.Done()
	s.cancelled <- struct{}{}
	return ctx.Err()
}

func (s *ackTimeoutServer) AddEntry(req *proto.AddEntryRequest, stream grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
	id := req.GetEntry().GetEntryId()
	if err := s.beforeBuffered(stream.Context(), []int64{id}, false); err != nil {
		return err
	}
	if err := stream.Send(&proto.AddEntryResponse{EntryId: id, State: proto.AddEntryState_Buffered}); err != nil {
		return err
	}
	if s.silent {
		return s.waitForCancellation(stream.Context())
	}
	return stream.Send(&proto.AddEntryResponse{EntryId: id, State: proto.AddEntryState_Synced, Status: werr.Success()})
}

func (s *ackTimeoutServer) AddEntries(req *proto.AddEntriesRequest, stream grpc.ServerStreamingServer[proto.AddEntriesResponse]) error {
	ids := make([]int64, len(req.GetEntries()))
	for i, entry := range req.GetEntries() {
		ids[i] = entry.GetEntryId()
	}
	if err := s.beforeBuffered(stream.Context(), ids, true); err != nil {
		return err
	}
	if err := stream.Send(&proto.AddEntriesResponse{EntryId: ids, State: proto.AddEntryState_Buffered}); err != nil {
		return err
	}
	acked := ids
	if s.silent {
		acked = ids[:len(ids)-1]
	}
	if err := stream.Send(&proto.AddEntriesResponse{EntryId: acked, State: proto.AddEntryState_Synced, Status: werr.Success()}); err != nil {
		return err
	}
	if s.silent {
		return s.waitForCancellation(stream.Context())
	}
	return nil
}

func TestReplicaAppenderRealGRPCACKTimeoutRetriesBeforeRolling(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			silent := &ackTimeoutServer{
				silent: true, calls: make(chan ackTimeoutCall, 8),
				cancelled: make(chan struct{}, 8), allowRetry: make(chan struct{}),
			}
			allowRetry := sync.OnceFunc(func() { close(silent.allowRetry) })
			defer allowRetry()
			quorum := &proto.QuorumInfo{Wq: 3, Aq: 2, Es: 3}
			for _, implementation := range []*ackTimeoutServer{silent, {}, {}} {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				server := grpc.NewServer()
				proto.RegisterLogStoreServer(server, implementation)
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(server.Stop)
				quorum.Nodes = append(quorum.Nodes, listener.Addr().String())
			}
			pool := client.NewLogStoreClientPool(1<<20, 1<<20)
			t.Cleanup(func() { require.NoError(t, pool.Close(context.Background())) })
			h := appenderFixtureWithPool(t, true, config.SegmentAppendConfig{QueueSize: 2, MaxRetries: 2}, pool, quorum)
			h.completionMgr = &completionManager{triggerCh: make(chan struct{}, 1)}
			const ackBudget = 80 * time.Millisecond
			h.appender.replicas[0].ackTimeout = ackBudget
			h.appender.replicas[0].batchAckTimeout = ackBudget
			failures := make(chan error, 2)
			notifyFailure := h.appender.notify.onReplicaFailure
			h.appender.notify.onReplicaFailure = func(address string, err error) {
				notifyFailure(address, err)
				failures <- err
			}
			entries := 1
			if batch {
				entries = 2
				holdExecutorForBatch(h, entries)
			}
			started := time.Now()
			callbacks := appendSequenceForTest(h, entries)
			if batch {
				h.appender.executor.Start(context.Background())
			}
			initial := receive(t, silent.calls)
			require.Equal(t, batch, initial.batch)
			require.Len(t, initial.ids, entries)
			require.False(t, initial.hasDeadline, "the stream must not inherit an admission or ACK deadline")
			for id := range entries {
				completion := receive(t, callbacks)
				require.Equal(t, int64(id), completion.id)
				require.NoError(t, completion.err, "the healthy quorum must not wait for the silent replica's ACK")
			}
			retry := receive(t, silent.calls)
			require.False(t, retry.batch)
			require.Equal(t, []int64{int64(entries - 1)}, retry.ids, "only the unacknowledged tail is retried")
			require.False(t, retry.hasDeadline)
			receive(t, silent.cancelled)
			require.False(t, h.rollingState.Load(), "an intermediate ACK timeout must not roll the segment")
			require.Empty(t, h.UnreachableReplicas())
			require.Empty(t, failures)
			allowRetry()
			err := receive(t, failures)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.True(t, werr.IsRetryableErr(err), "timeout classification must survive a real gRPC stream")
			require.Equal(t, int32(2), silent.attempts.Load())
			require.GreaterOrEqual(t, time.Since(started), 2*ackBudget)
			require.True(t, h.rollingState.Load())
			require.Equal(t, []string{quorum.Nodes[0]}, h.UnreachableReplicas())
			receive(t, h.completionMgr.triggerCh)
			receive(t, silent.cancelled)
			waitForAppenderDrained(t, h)
			require.Empty(t, callbacks, "late retry exhaustion must not revoke or duplicate quorum success")
			require.Empty(t, silent.calls)
			require.Empty(t, failures)
		})
	}
}
