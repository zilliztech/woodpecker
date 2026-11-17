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
	"time"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// replicaResult is a final per-entry, per-replica outcome. Attempts and transport
// resources never cross the boundary into the segment's quorum state machine.
type replicaResult struct {
	entryID int64
	replica int
	address string
	err     error
	elapsed time.Duration
}

// segmentAppender owns the append execution pipeline. The segment owns logical
// completion; admission credits remain held until all replica outcomes and
// ordered callback delivery finish, in both service and embedded modes.
type segmentAppender struct {
	ctx              context.Context
	cancel           context.CancelFunc
	executor         *SequentialExecutor
	replicas         []*replicaAppender
	pool             client.LogStoreClientPool
	onResult         func(replicaResult)
	workers          sync.WaitGroup
	stopOnce         sync.Once
	joinOnce         sync.Once
	mu               sync.Mutex
	records          map[int64]*appendReservation
	count            int
	bytes            int64
	maxCount         int
	maxBytes         int64
	admissionStopped bool
	changed          chan struct{}
}

// appendReservation belongs only to segmentAppender. Before Submit, its caller
// owns the provisional credit and must abandon it on error. Afterwards the
// appender's entry table owns it; AppendOp never stores a lease or a deadline.
type appendReservation struct {
	owner        *segmentAppender
	size         int64
	admitted     time.Time
	released     bool // protected by owner.mu
	callbackDone bool
	replicasDone []bool
}

func newSegmentAppender(pool client.LogStoreClientPool, quorum *proto.QuorumInfo, cfg config.SegmentAppendConfig, service bool, onResult func(replicaResult)) *segmentAppender {
	ctx, cancel := context.WithCancel(context.Background())
	g := &segmentAppender{
		ctx: ctx, cancel: cancel, pool: pool, onResult: onResult,
		records: make(map[int64]*appendReservation), maxCount: max(cfg.QueueSize, 1),
		maxBytes: cfg.MaxPendingBytes.Int64(), changed: make(chan struct{}),
	}
	if g.maxBytes <= 0 {
		g.maxBytes = 64 << 20
	}
	// Buffer independent service replica queues; the single embedded replica
	// receives directly. Both use the same count and byte admission budgets.
	queueSize := 0
	if service {
		queueSize = g.maxCount
	}
	for i, address := range quorum.GetNodes() {
		r := newReplicaAppender(g, i, address, queueSize, max(cfg.MaxRetries, 1))
		g.replicas = append(g.replicas, r)
		g.workers.Add(1)
		go func() { defer g.workers.Done(); r.run() }()
	}
	g.executor = NewBatchingSequentialExecutor(g.maxCount, cfg.MaxBatchEntries, cfg.MaxBatchBytes.Int64(), g.dispatch)
	g.executor.Start(context.Background())
	return g
}

// reserve waits outside the segment lock and before an entry ID is allocated.
func (g *segmentAppender) reserve(ctx context.Context, size int64) (*appendReservation, error) {
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
			r := &appendReservation{owner: g, size: size, admitted: time.Now(), replicasDone: make([]bool, len(g.replicas))}
			g.mu.Unlock()
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

func (r *appendReservation) release() {
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()
	r.owner.releaseUnsafe(r)
}

func (g *segmentAppender) releaseUnsafe(r *appendReservation) {
	if r.released {
		return
	}
	r.released = true
	g.count--
	g.bytes -= r.size
	close(g.changed)
	g.changed = make(chan struct{})
}

// submit publishes an already registered logical append. Reservation guarantees
// room in the input queue; the segment never waits for I/O while publishing it.
func (g *segmentAppender) submit(ctx context.Context, op *AppendOp, r *appendReservation) bool {
	g.mu.Lock()
	if g.admissionStopped {
		g.mu.Unlock()
		return false
	}
	g.records[op.entryId] = r
	g.mu.Unlock()
	if g.executor.Submit(ctx, op) {
		return true
	}
	g.mu.Lock()
	delete(g.records, op.entryId)
	g.mu.Unlock()
	return false
}

func (g *segmentAppender) dispatch(ops []*AppendOp) {
	for _, r := range g.replicas {
		select {
		case r.queue <- ops:
		case <-g.ctx.Done():
			return
		}
	}
}

func (g *segmentAppender) replicaDone(result replicaResult) {
	g.mu.Lock()
	r := g.records[result.entryID]
	if r == nil || r.replicasDone[result.replica] {
		g.mu.Unlock()
		return
	}
	r.replicasDone[result.replica] = true
	g.reclaimUnsafe(result.entryID, r)
	g.mu.Unlock()
	// No appender lock is held across the segment callback.
	if g.ctx.Err() == nil && g.onResult != nil {
		g.onResult(result)
	}
}

func (g *segmentAppender) callbackDone(entryID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r := g.records[entryID]; r != nil {
		r.callbackDone = true
		g.reclaimUnsafe(entryID, r)
	}
}

func (g *segmentAppender) retainedFor(entryID int64, now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r := g.records[entryID]; r != nil {
		return now.Sub(r.admitted)
	}
	return 0
}

func (g *segmentAppender) reclaimUnsafe(entryID int64, r *appendReservation) {
	if !r.callbackDone {
		return
	}
	for _, done := range r.replicasDone {
		if !done {
			return
		}
	}
	g.releaseUnsafe(r)
	delete(g.records, entryID)
}

func (g *segmentAppender) stopAdmission() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.admissionStopped {
		g.admissionStopped = true
		close(g.changed)
		g.changed = make(chan struct{})
	}
}

// stop only signals shutdown and is safe under the segment lock. Joining the
// executor, senders and ACK readers is deliberately separate from that lock.
func (g *segmentAppender) stop() {
	g.stopOnce.Do(func() {
		g.stopAdmission()
		g.cancel()
		g.executor.stop()
		// Reclaim transport resources even when the caller only signals stop
		// (for example, direct fencing). An explicit wait joins this same work.
		go g.wait()
	})
}

func (g *segmentAppender) wait() {
	g.joinOnce.Do(g.join)
}

func (g *segmentAppender) join() {
	g.executor.wait()
	g.workers.Wait()
	for _, r := range g.replicas {
		r.drains.Wait()
		clear(r.pending)
		r.retries = nil
		for len(r.queue) > 0 {
			<-r.queue
		}
		for len(r.results) > 0 {
			<-r.results
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, record := range g.records {
		for i := range record.replicasDone {
			record.replicasDone[i] = true
		}
		g.reclaimUnsafe(id, record)
	}
}
