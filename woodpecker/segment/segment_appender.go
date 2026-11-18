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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// replicaResult is a final per-entry, per-replica outcome consumed by the appender.
type replicaResult struct {
	entryID int64
	replica int
	address string
	err     error
	elapsed time.Duration
}

// Notifications run outside the appender lock; the handle owns segment lifecycle
// and metadata, while the appender owns all per-entry write state.
type segmentAppendCallbacks struct {
	onReplicaFailure func(address string, err error)
	onConfirmed      func(lac int64)
	onDrained        func() // logical writes and callbacks drained after admission stops
}

// segmentAppender owns admission, entry IDs, replication, quorum, the committed
// prefix and ordered callbacks. Credits cover final replica outcomes and callbacks.
type segmentAppender struct {
	// Segment identity and replica configuration, fixed at construction.
	bucketName string
	rootPath   string
	logId      int64
	segmentId  int64
	logNs      string
	quorumInfo *proto.QuorumInfo
	nodeScopes []string

	// Pipeline dependencies and shutdown: signal once, then join all workers.
	pool     client.LogStoreClientPool
	notify   segmentAppendCallbacks
	executor *SequentialExecutor
	replicas []*replicaAppender
	ctx      context.Context
	cancel   context.CancelFunc
	workers  sync.WaitGroup // replica schedulers; I/O workers are tracked by replicaAppender.drains
	stopOnce sync.Once
	joinOnce sync.Once

	// Progress is updated under mu and can be read without taking it.
	lastPushed       atomic.Int64
	lastAddConfirmed atomic.Int64
	commitedSize     atomic.Int64
	submittedSize    atomic.Int64

	// Protects admission, pending ops, quorum state and callback scheduling.
	mu sync.Mutex

	// Admission limits and usage; count/bytes include unpublished reservations.
	maxCount         int
	maxBytes         int64
	count            int
	bytes            int64
	records          map[int64]*appendReservation // published entries retaining credits
	admissionStopped bool
	admissionErr     error
	changed          chan struct{} // closed and replaced to wake admission waiters
	closed           bool          // pending callbacks are fixed; ignore late replica results

	// Logical writes awaiting ordered completion; scheduling is protected by mu.
	appendOpsQueue  *list.List
	callbacks       []appendCompletion
	callbackRunning bool
	callbackWG      sync.WaitGroup // joins callback delivery separately from replica I/O
}

// appendReservation belongs only to segmentAppender. Before Submit, its caller
// owns the provisional credit and must abandon it on error. Afterwards the
// appender's entry table owns it; AppendOp never stores a lease or a deadline.
type appendReservation struct {
	// Fixed when capacity is reserved.
	owner    *segmentAppender
	size     int64
	admitted time.Time

	// Completion and release state, protected by owner.mu.
	released     bool
	callbackDone bool
	replicasDone []bool
}

func newSegmentAppender(pool client.LogStoreClientPool, cfg *config.Configuration, logID int64, initial *proto.SegmentMetadata, notify segmentAppendCallbacks) *segmentAppender {
	ctx, cancel := context.WithCancel(context.Background())
	appendCfg := cfg.Woodpecker.Client.SegmentAppend
	quorum := initial.Quorum
	g := &segmentAppender{
		ctx: ctx, cancel: cancel, pool: pool, notify: notify,
		bucketName: cfg.Minio.BucketName, rootPath: cfg.Minio.RootPath,
		logId: logID, segmentId: initial.SegNo, quorumInfo: quorum, nodeScopes: quorumNodeScopes(quorum),
		logNs: metrics.BuildLogNs(cfg.Minio.BucketName, cfg.Minio.RootPath), appendOpsQueue: list.New(),
		records: make(map[int64]*appendReservation), maxCount: max(appendCfg.QueueSize, 1),
		maxBytes: appendCfg.MaxPendingBytes.Int64(), changed: make(chan struct{}),
	}
	g.lastPushed.Store(initial.LastEntryId)
	g.lastAddConfirmed.Store(initial.LastEntryId)
	g.commitedSize.Store(initial.Size)
	if g.maxBytes <= 0 {
		g.maxBytes = 64 << 20
	}
	// Buffer independent service replica queues; the single embedded replica
	// receives directly. Both use the same count and byte admission budgets.
	queueSize := 0
	if cfg.Woodpecker.Storage.IsStorageService() {
		queueSize = g.maxCount
	}
	for i, address := range quorum.GetNodes() {
		r := newReplicaAppender(g, i, address, queueSize, max(appendCfg.MaxRetries, 1))
		g.replicas = append(g.replicas, r)
		g.workers.Add(1)
		go func() { defer g.workers.Done(); r.run() }()
	}
	g.executor = NewBatchingSequentialExecutor(g.maxCount, appendCfg.MaxBatchEntries, appendCfg.MaxBatchBytes.Int64(), g.dispatch)
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
			err := g.admissionErr
			g.mu.Unlock()
			return nil, err
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

// append allocates an ID and publishes the op under one lock, after reservation.
func (g *segmentAppender) append(ctx context.Context, value []byte, callback func(int64, int64, error), r *appendReservation) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.admissionStopped {
		return g.admissionErr
	}
	op := g.createPendingAppendOp(ctx, value, g.lastPushed.Load()+1, callback)
	if !g.submitUnsafe(ctx, op, r) {
		return werr.ErrSegmentHandleSegmentClosed
	}
	return nil
}

func (g *segmentAppender) createPendingAppendOp(ctx context.Context, value []byte, entryID int64, callback func(int64, int64, error)) *AppendOp {
	op := NewAppendOp(g.bucketName, g.rootPath, g.logId, g.segmentId, entryID, value, callback, g.quorumInfo, g.nodeScopes)
	op.traceParent = trace.SpanContextFromContext(ctx)
	return op
}

// Caller holds mu. The reservation guarantees queue space, so publication never waits for I/O.
func (g *segmentAppender) submitUnsafe(ctx context.Context, op *AppendOp, r *appendReservation) bool {
	if g.admissionStopped {
		return false
	}
	element := g.appendOpsQueue.PushBack(op)
	g.records[op.entryId] = r
	if g.executor.Submit(ctx, op) {
		g.lastPushed.Store(op.entryId)
		g.submittedSize.Add(op.Size())
		logID := strconv.FormatInt(g.logId, 10)
		metrics.WpClientAppendRequestsTotal.WithLabelValues(g.logNs, logID).Inc()
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(g.logNs, logID).Inc()
		return true
	}
	delete(g.records, op.entryId)
	g.appendOpsQueue.Remove(element)
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
	g.stopAdmissionUnsafe(werr.ErrSegmentHandleSegmentRolling)
}

func (g *segmentAppender) stopAdmissionUnsafe(err error) {
	if !g.admissionStopped {
		g.admissionStopped = true
		g.admissionErr = err
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
	for id, record := range g.records {
		for i := range record.replicasDone {
			record.replicasDone[i] = true
		}
		g.reclaimUnsafe(id, record)
	}
	g.mu.Unlock()
	g.callbackWG.Wait()
}
