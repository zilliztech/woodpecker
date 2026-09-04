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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

// An append is acknowledged to the caller as soon as Aq replicas have answered,
// which with Es=3,Aq=2 normally leaves exactly one replica still answering. What
// happens to that replica is what these tests pin: it must be left to finish and
// then retired by whoever was reading it, never cancelled by the completion of
// the quorum it was not part of.

// silentAckStream stands in for a replica that accepted the entry and has not
// sent its durability ack yet: Recv blocks until the stream's own context ends,
// exactly as a real gRPC stream does.
type silentAckStream struct {
	streamCtx context.Context
}

func (s *silentAckStream) Recv() (*proto.AddEntryResponse, error) {
	<-s.streamCtx.Done()
	return nil, s.streamCtx.Err()
}

func (s *silentAckStream) CloseSend() error             { return nil }
func (s *silentAckStream) Header() (metadata.MD, error) { return nil, nil }
func (s *silentAckStream) Trailer() metadata.MD         { return nil }
func (s *silentAckStream) Context() context.Context     { return s.streamCtx }
func (s *silentAckStream) SendMsg(m any) error          { return nil }
func (s *silentAckStream) RecvMsg(m any) error          { return nil }

// inFlightReplicas installs n RemoteResultChannels on the op, each holding a
// stream that has not answered yet, and reports which of their cancels have
// fired.
func inFlightReplicas(t *testing.T, op *AppendOp, n int) ([]*channel.RemoteResultChannel, []*atomic.Bool) {
	t.Helper()
	channels := make([]*channel.RemoteResultChannel, n)
	cancelled := make([]*atomic.Bool, n)
	op.resultChannels = make([]channel.ResultChannel, n)
	for i := 0; i < n; i++ {
		flag := &atomic.Bool{}
		streamCtx, streamCancel := context.WithCancel(context.Background())
		rc := channel.NewRemoteResultChannel(op.Identifier())
		rc.InitResponseStream(&silentAckStream{streamCtx: streamCtx}, streamCtx, func() {
			flag.Store(true)
			streamCancel()
		})
		channels[i] = rc
		cancelled[i] = flag
		op.resultChannels[i] = rc
		t.Cleanup(streamCancel)
	}
	return channels, cancelled
}

// TestAppendOp_FastSuccess_DoesNotCancelInFlightReplicaStreams is the regression
// test for the defect itself. FastSuccess used to close every result channel,
// and for a RemoteResultChannel Close cancels the replica's gRPC stream — so the
// replica outside the quorum had its normal completion turned into
// "rpc error: code = Canceled", logged as a failure once per entry.
func TestAppendOp_FastSuccess_DoesNotCancelInFlightReplicaStreams(t *testing.T) {
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"),
		func(int64, int64, error) {}, nil, nil,
		&proto.QuorumInfo{Wq: 3, Aq: 2, Es: 3, Nodes: []string{"n1", "n2", "n3"}}, nil)
	channels, cancelled := inFlightReplicas(t, op, 3)

	op.FastSuccess(context.Background())

	for i := range channels {
		assert.False(t, channels[i].IsClosed(), "replica %d: FastSuccess must not close the channel", i)
		assert.False(t, cancelled[i].Load(), "replica %d: FastSuccess must not cancel the stream", i)
	}
}

// TestAppendOp_FastFail_CancelsInFlightReplicaStreams is the deliberate contrast.
// The entry is abandoned there, so every waiter should stop at once rather than
// sit out its read budget.
func TestAppendOp_FastFail_CancelsInFlightReplicaStreams(t *testing.T) {
	op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"),
		func(int64, int64, error) {}, nil, nil,
		&proto.QuorumInfo{Wq: 3, Aq: 2, Es: 3, Nodes: []string{"n1", "n2", "n3"}}, nil)
	channels, cancelled := inFlightReplicas(t, op, 3)

	op.FastFail(context.Background(), werr.ErrSegmentFenced)

	for i := range channels {
		assert.True(t, channels[i].IsClosed(), "replica %d: FastFail must close the channel", i)
		assert.True(t, cancelled[i].Load(), "replica %d: FastFail must cancel the stream", i)
	}
}

// TestAppendOp_receivedAckCallback_ClosesItsOwnChannel pins the other half of the
// exchange: with FastSuccess no longer closing, the reading goroutine must, or
// the stream and the goroutine behind it are never released.
func TestAppendOp_receivedAckCallback_ClosesItsOwnChannel(t *testing.T) {
	t.Run("after a normal ack", func(t *testing.T) {
		mockHandle := mocks_segment_handle.NewSegmentHandle(t)
		mockHandle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, int64(3)).Return().Once()
		op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"),
			func(int64, int64, error) {}, nil, mockHandle,
			&proto.QuorumInfo{Wq: 1, Aq: 1, Es: 1, Nodes: []string{"n1"}}, nil)

		rc := channel.NewLocalResultChannel(op.Identifier())
		require.NoError(t, rc.SendResult(context.Background(), &channel.AppendResult{SyncedId: 3}))
		op.resultChannels = []channel.ResultChannel{rc}

		op.receivedAckCallback(context.Background(), time.Now(), 3, rc, nil, 0, "n1")

		assert.True(t, rc.IsClosed(), "the reader owns the channel and must close it on the success path")
	})

	t.Run("after a send error", func(t *testing.T) {
		op := NewAppendOp("a-bucket", "files", 1, 2, 3, []byte("test"),
			func(int64, int64, error) {}, nil, nil,
			&proto.QuorumInfo{Wq: 1, Aq: 1, Es: 1, Nodes: []string{"n1"}}, nil)
		// fastCalled short-circuits the failure handling, so this exercises the
		// earliest return in the function — the close must still happen.
		op.fastCalled.Store(true)

		rc := channel.NewLocalResultChannel(op.Identifier())
		op.resultChannels = []channel.ResultChannel{rc}

		op.receivedAckCallback(context.Background(), time.Now(), 3, rc, werr.ErrInternalError, 0, "n1")

		assert.True(t, rc.IsClosed(), "the close must cover every exit path, not just the happy one")
	})
}
