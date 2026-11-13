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
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

// replicaSenders belongs to one writable service segment. The sequential
// executor retains batching and dispatch order; each replica progresses on its
// own lane through Buffered, while durability results are drained separately.
// Admission covers queued AND in-flight entries, including quorum successes
// still waiting for the third replica, and fully replicated entries waiting for
// an earlier entry's ordered callback. A full lane therefore cannot block the
// dispatcher: every accepted entry already reserved space in every lane.
type replicaSenders struct {
	handle           *segmentHandleImpl
	ctx              context.Context
	cancel           context.CancelFunc
	lanes            []*replicaSender
	workers          sync.WaitGroup
	drains           sync.WaitGroup
	stopOnce         sync.Once
	mu               sync.Mutex
	count            int
	bytes            int64
	maxCount         int
	maxBytes         int64
	admissionStopped bool
	changed          chan struct{}
}

type replicaReservation struct {
	owner     *replicaSenders
	size      int64
	admitted  time.Time
	remaining atomic.Int32
	once      sync.Once
}

func (r *replicaReservation) release() {
	r.once.Do(func() {
		g := r.owner
		g.mu.Lock()
		g.count--
		g.bytes -= r.size
		close(g.changed)
		g.changed = make(chan struct{})
		g.mu.Unlock()
	})
}

func (r *replicaReservation) replicaDone() {
	if r.remaining.Add(-1) == 0 {
		r.release()
	}
}

type replicaSender struct {
	group   *replicaSenders
	index   int
	queue   chan []*AppendOp
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	failure error
}

func newReplicaSenders(handle *segmentHandleImpl, maxCount int, maxBytes int64) *replicaSenders {
	if maxCount < 1 {
		maxCount = 1
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &replicaSenders{
		handle: handle, ctx: ctx, cancel: cancel,
		maxCount: maxCount, maxBytes: maxBytes, changed: make(chan struct{}),
	}
	for i := range handle.quorumInfo.Nodes {
		laneCtx, laneCancel := context.WithCancel(ctx)
		s := &replicaSender{group: g, index: i, queue: make(chan []*AppendOp, maxCount), ctx: laneCtx, cancel: laneCancel}
		g.lanes = append(g.lanes, s)
		g.workers.Add(1)
		go func() { defer g.workers.Done(); s.run() }()
	}
	return g
}

// reserve runs before assigning an entry ID, without holding the segment lock.
// Allow one oversized entry when empty, preserving the existing entry-size API.
func (g *replicaSenders) reserve(ctx context.Context, size int64) (*replicaReservation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.mu.Lock()
		if g.admissionStopped {
			g.mu.Unlock()
			return nil, werr.ErrSegmentHandleSegmentRolling
		}
		if g.count < g.maxCount && (g.count == 0 || size <= g.maxBytes-g.bytes) {
			g.count++
			g.bytes += size
			g.mu.Unlock()
			r := &replicaReservation{owner: g, size: size, admitted: time.Now()}
			r.remaining.Store(int32(len(g.lanes) + 1)) // replica outcomes plus the ordered terminal callback
			return r, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// Only the sequential executor calls dispatch, and stop runs after it drains.
func (g *replicaSenders) dispatch(ops []*AppendOp) {
	for _, lane := range g.lanes {
		lane.queue <- ops
	}
}

func (g *replicaSenders) stopAdmission() {
	g.mu.Lock()
	if !g.admissionStopped {
		g.admissionStopped = true
		close(g.changed)
		g.changed = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *replicaSenders) stop() {
	g.stopOnce.Do(func() {
		g.stopAdmission()
		g.cancel()
		for _, lane := range g.lanes {
			close(lane.queue)
		}
	})
}

// wait must run outside the segment lock: drains can still be applying ACKs.
func (g *replicaSenders) wait() {
	g.workers.Wait() // no further drains.Add after workers exit
	g.drains.Wait()
}

func (s *replicaSender) failed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}

func (s *replicaSender) fail(err error) error {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	err = s.failure
	s.mu.Unlock()
	s.group.stopAdmission()
	s.cancel() // retire every stream on this lane; other replicas keep draining
	return err
}

func (s *replicaSender) finish(op *AppendOp, start time.Time, result *channel.AppendResult, err error) {
	defer op.replicaLease.replicaDone()
	if s.group.ctx.Err() != nil {
		return
	} // local shutdown is not a replica failure
	if err == nil && (result == nil || result.Err != nil || result.SyncedId < op.entryId) {
		err = werr.ErrUnknownError
		if result != nil && result.Err != nil {
			err = result.Err
		}
	}
	if err != nil {
		err = s.fail(err)
		op.channelErrors[s.index] = err
		op.recordReplicaResult(s.index, "error")
		// Do not skip this when fastCalled is set: rolling is independent of the
		// application's quorum callback, and a failed lane is final for this segment.
		op.handle.HandleAppendRequestFailure(context.Background(), op.entryId, err, s.index, op.quorumInfo.Nodes[s.index])
		return
	}
	op.applyNodeAck(s.group.ctx, start, result, nil, s.index, op.quorumInfo.Nodes[s.index])
}

func (s *replicaSender) run() {
	for ops := range s.queue {
		if s.group.ctx.Err() != nil {
			for _, op := range ops {
				op.replicaLease.replicaDone()
			}
			continue
		}
		if err := s.failed(); err != nil {
			for _, op := range ops {
				s.finish(op, time.Now(), nil, err)
			}
			continue
		}
		s.send(ops)
	}
}

func (s *replicaSender) send(ops []*AppendOp) {
	first := ops[0]
	start := time.Now()
	// Bound total retention, including queue time, using the existing ACK budget.
	// The remote client's existing sendTimeout still bounds Buffered separately.
	deadline := first.replicaLease.admitted.Add(appendAckReadTimeout)
	for _, op := range ops[1:] {
		if d := op.replicaLease.admitted.Add(appendAckReadTimeout); d.Before(deadline) {
			deadline = d
		}
	}
	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	links := make([]trace.Link, 0, len(ops))
	for _, op := range ops {
		if op.traceParent.IsValid() {
			links = append(links, trace.Link{SpanContext: op.traceParent})
		}
	}
	ctx, sp := logger.NewIntentCtxWithOptions(ctx, "ReplicaSender", "send", trace.WithLinks(links...), trace.WithAttributes(
		attribute.Int64("log_id", first.logId), attribute.Int64("segment_id", first.segmentId),
		attribute.String("node", first.quorumInfo.Nodes[s.index]),
		attribute.Int64("first_entry", first.entryId), attribute.Int64("last_entry", ops[len(ops)-1].entryId),
		attribute.Float64("queue_wait_ms", float64(start.Sub(first.replicaLease.admitted))/float64(time.Millisecond)),
	))
	defer sp.End()
	cli, err := first.clientPool.GetLogStoreClient(ctx, first.quorumInfo.Nodes[s.index])
	chs := make([]channel.ResultChannel, len(ops))
	if err == nil {
		entries := make([]*proto.LogEntry, len(ops))
		for i, op := range ops {
			entries[i] = op.toLogEntry()
			if len(ops) == 1 && cli.IsRemoteClient() {
				chs[i] = channel.NewRemoteResultChannel(op.Identifier())
			} else {
				chs[i] = channel.NewLocalResultChannel(op.Identifier())
			}
		}
		if len(ops) == 1 {
			_, err = cli.AppendEntry(ctx, first.bucketName, first.rootPath, first.logId, entries[0], chs[0])
		} else {
			_, err = cli.AppendEntries(ctx, first.bucketName, first.rootPath, first.logId, entries, chs)
		}
	}
	if err != nil {
		cancel()
		for i, op := range ops {
			op.retireResultChannel(context.Background(), chs[i], first.quorumInfo.Nodes[s.index])
			s.finish(op, start, nil, err)
		}
		return
	}
	s.group.drains.Add(1)
	go func() {
		defer s.group.drains.Done()
		defer cancel()
		for i, op := range ops {
			result, readErr := chs[i].ReadResult(ctx)
			op.retireResultChannel(context.Background(), chs[i], first.quorumInfo.Nodes[s.index])
			s.finish(op, start, result, readErr)
		}
	}()
}
