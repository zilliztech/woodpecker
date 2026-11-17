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
	"container/heap"
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/topology"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

// These budgets apply in both modes after the send call returns. Each entry's
// result-channel read gets a fresh budget, including reads within one batch;
// this is not a shared batch deadline. Admission, send time and previous attempts
// are excluded. SegmentAppend.SendTimeout only bounds the remote send phase.
var (
	appendAckReadTimeout = 30 * time.Second
	batchAckReadTimeout  = 30 * time.Second
)

type replicaTask struct {
	op       *AppendOp
	attempts int
	retry    bool
}

type replicaAttemptResult struct {
	task    *replicaTask
	attempt int
	result  *channel.AppendResult
	err     error
	started time.Time
}

// replicaAppender has one scheduling owner. Send workers and ACK readers report
// attempt results to that owner; they never schedule retries or modify a logical AppendOp.
type replicaAppender struct {
	group           *segmentAppender
	index           int
	address         string
	queue           chan []*AppendOp
	results         chan replicaAttemptResult
	ctx             context.Context
	cancel          context.CancelFunc
	drains          sync.WaitGroup // active sends and ACK readers
	pending         map[int64]*replicaTask
	retries         replicaRetryQueue
	failure         error
	maxAttempts     int
	ackTimeout      time.Duration
	batchAckTimeout time.Duration
}

func newReplicaAppender(group *segmentAppender, index int, address string, queueSize, maxAttempts int) *replicaAppender {
	ctx, cancel := context.WithCancel(group.ctx)
	return &replicaAppender{
		group: group, index: index, address: address, ctx: ctx, cancel: cancel,
		queue: make(chan []*AppendOp, queueSize), results: make(chan replicaAttemptResult, group.maxCount),
		pending: make(map[int64]*replicaTask), maxAttempts: maxAttempts,
		ackTimeout: appendAckReadTimeout, batchAckTimeout: batchAckReadTimeout,
	}
}

func (r *replicaAppender) run() {
	defer r.cancel()
	var nextBatch []*AppendOp
	var sendDone <-chan struct{}
	for {
		// Ready results and retries take priority over a dequeued batch.
		r.drainResults()
		if r.group.ctx.Err() != nil {
			return
		}
		if sendDone == nil {
			if retry := r.nextRetry(); retry != nil {
				retry.retry = false
				sendDone = r.startSend([]*replicaTask{retry})
				continue
			}
			if nextBatch != nil {
				sendDone = r.sendNewBatch(nextBatch)
				nextBatch = nil
				continue
			}
		}
		queue := r.queue
		if sendDone != nil || nextBatch != nil {
			queue = nil
		}
		select {
		case <-r.group.ctx.Done():
			return
		case result := <-r.results:
			r.apply(result)
		case <-sendDone:
			sendDone = nil
		case nextBatch = <-queue:
		}
	}
}

func (r *replicaAppender) drainResults() {
	for {
		select {
		case result := <-r.results:
			r.apply(result)
		default:
			return
		}
	}
}

func (r *replicaAppender) sendNewBatch(ops []*AppendOp) <-chan struct{} {
	tasks := make([]*replicaTask, 0, len(ops))
	for _, op := range ops {
		if r.failure != nil {
			r.finish(op, r.failure, 0)
			continue
		}
		task := &replicaTask{op: op}
		r.pending[op.entryId] = task
		tasks = append(tasks, task)
	}
	if len(tasks) > 0 {
		return r.startSend(tasks)
	}
	return nil
}

// Only run starts sends, with at most one in flight; attempt numbers are frozen
// before handing I/O to a worker so ACK processing can continue independently.
func (r *replicaAppender) startSend(tasks []*replicaTask) <-chan struct{} {
	generations := make([]int, len(tasks))
	for i, task := range tasks {
		task.attempts++
		generations[i] = task.attempts
	}
	done := make(chan struct{})
	r.drains.Add(1)
	go func() {
		defer r.drains.Done()
		defer close(done)
		r.send(tasks, generations)
	}()
	return done
}

func (r *replicaAppender) nextRetry() *replicaTask {
	for len(r.retries) > 0 {
		task := heap.Pop(&r.retries).(*replicaTask)
		if task.retry && r.pending[task.op.entryId] == task {
			return task
		}
	}
	return nil
}

type replicaRetryQueue []*replicaTask

func (q replicaRetryQueue) Len() int           { return len(q) }
func (q replicaRetryQueue) Less(i, j int) bool { return q[i].op.entryId < q[j].op.entryId }
func (q replicaRetryQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *replicaRetryQueue) Push(task any)     { *q = append(*q, task.(*replicaTask)) }
func (q *replicaRetryQueue) Pop() any {
	old := *q
	task := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return task
}

func (r *replicaAppender) apply(result replicaAttemptResult) {
	if r.group.ctx.Err() != nil {
		return
	} // local shutdown is not a node failure
	task := result.task
	if r.pending[task.op.entryId] != task || result.attempt != task.attempts || task.retry {
		return
	}
	err := result.err
	if err == nil {
		if result.result == nil {
			err = werr.ErrUnknownError
		} else if result.result.Err != nil {
			err = result.result.Err
		} else if result.result.SyncedId < task.op.entryId {
			err = werr.ErrUnknownError
		}
	}
	if err == nil {
		r.recordResult(task.op, "success")
		delete(r.pending, task.op.entryId)
		r.finish(task.op, nil, time.Since(result.started))
		return
	}
	r.recordResult(task.op, "error")
	if task.attempts < r.maxAttempts && werr.IsRetryableErr(err) && !werr.IsSegmentNotWritableErr(err) {
		task.retry = true
		heap.Push(&r.retries, task)
		return
	}
	// This lane can no longer supply a complete prefix. Report a FINAL failure,
	// including for entries whose application callback already succeeded. Only
	// the segment decides to roll and closes global admission in response.
	r.failure = err
	r.cancel()
	ids := make([]int64, 0, len(r.pending))
	for id := range r.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		pending := r.pending[id]
		delete(r.pending, id)
		r.finish(pending.op, err, time.Since(result.started))
	}
}

func (r *replicaAppender) finish(op *AppendOp, err error, elapsed time.Duration) {
	r.group.replicaDone(replicaResult{entryID: op.entryId, replica: r.index, address: r.address, err: err, elapsed: elapsed})
}

func retireAppendResult(ctx context.Context, ch channel.ResultChannel) {
	if ch != nil {
		if err := ch.Close(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to retire append result", zap.Error(err))
		}
	}
}

func (r *replicaAppender) send(tasks []*replicaTask, generations []int) {
	if r.ctx.Err() != nil {
		return
	}
	started := time.Now()
	first := tasks[0].op
	ops := make([]*AppendOp, len(tasks))
	for i, task := range tasks {
		ops[i] = task.op
	}
	ctx, cancel := context.WithCancel(r.ctx)
	var links []trace.Link
	if len(ops) == 1 {
		// Do not also link the parent: Jaeger can retain that duplicate as
		// FOLLOWS_FROM instead of CHILD_OF, losing the single-entry ancestry.
		ctx = trace.ContextWithSpanContext(ctx, first.traceParent)
	} else {
		links = appendTraceLinks(ops)
	}
	ctx, sp := logger.NewIntentCtxWithOptions(ctx, "ReplicaAppender", "send", trace.WithLinks(links...), trace.WithAttributes(
		attribute.Int64("log_id", first.logId), attribute.Int64("segment_id", first.segmentId),
		attribute.String("node", r.address), attribute.Int64("first_entry", first.entryId),
		attribute.Int64("last_entry", ops[len(ops)-1].entryId), attribute.Int("append.entries", len(ops)),
		attribute.Int("attempt", generations[0]),
		attribute.Float64("retained_ms", float64(r.group.retainedFor(first.entryId, started))/float64(time.Millisecond)),
	))
	defer sp.End()
	cli, err := r.group.pool.GetLogStoreClient(ctx, r.address)
	channels := make([]channel.ResultChannel, len(tasks))
	if err == nil {
		entries := make([]*proto.LogEntry, len(tasks))
		for i, task := range tasks {
			entries[i] = task.op.toLogEntry()
			if len(tasks) == 1 && cli.IsRemoteClient() {
				channels[i] = channel.NewRemoteResultChannel(task.op.Identifier())
			} else {
				channels[i] = channel.NewLocalResultChannel(task.op.Identifier())
			}
		}
		if len(tasks) == 1 {
			_, err = cli.AppendEntry(ctx, first.bucketName, first.rootPath, first.logId, entries[0], channels[0])
		} else {
			_, err = cli.AppendEntries(ctx, first.bucketName, first.rootPath, first.logId, entries, channels)
		}
	}
	if err != nil {
		cancel()
		for i, task := range tasks {
			retireAppendResult(context.Background(), channels[i])
			r.reportResult(replicaAttemptResult{task: task, attempt: generations[i], err: err, started: started})
		}
		return
	}
	timeout := r.ackTimeout
	if len(tasks) > 1 {
		timeout = r.batchAckTimeout
	}
	r.drains.Add(1)
	go func() {
		defer r.drains.Done()
		defer cancel()
		var streamErr error
		for i, task := range tasks {
			var result *channel.AppendResult
			err := streamErr
			if err == nil {
				readCtx, readCancel := context.WithTimeout(ctx, timeout)
				result, err = channels[i].ReadResult(readCtx)
				readCancel()
				// A broken stream cannot deliver the suffix. Retire it once,
				// report each unknown entry, and retry only those entries.
				if err != nil {
					streamErr = err
					cancel()
				}
			}
			retireAppendResult(context.Background(), channels[i])
			r.reportResult(replicaAttemptResult{task: task, attempt: generations[i], result: result, err: err, started: started})
			tasks[i] = nil
		}
	}()
}

func (r *replicaAppender) reportResult(result replicaAttemptResult) {
	select {
	case r.results <- result:
	case <-r.ctx.Done():
		// Cancellation discards the outcome; the worker still retires its channels.
	}
}

// Count actual attempt outcomes, including the replica that answers after the
// application already reached quorum. Locally cancelled readers are discarded
// by the scheduler before this point.
func (r *replicaAppender) recordResult(op *AppendOp, status string) {
	scope := topology.ScopeUnknown
	if r.index < len(op.nodeScopes) {
		scope = op.nodeScopes[r.index]
	}
	logID := strconv.FormatInt(op.logId, 10)
	metrics.WpClientReplicaAppendTotal.WithLabelValues(op.logNs, logID, scope, status).Inc()
	if status == "success" {
		metrics.WpClientReplicaAppendBytesTotal.WithLabelValues(op.logNs, logID, scope).Add(float64(op.Size()))
	}
}

// appendTraceLinks preserves distinct request spans when one send batches
// multiple appends, without adding links for untraced or duplicate inputs.
func appendTraceLinks(ops []*AppendOp) []trace.Link {
	type identity struct {
		traceID trace.TraceID
		spanID  trace.SpanID
	}
	seen := make(map[identity]bool)
	links := make([]trace.Link, 0, len(ops))
	for _, op := range ops {
		sc := op.traceParent
		key := identity{sc.TraceID(), sc.SpanID()}
		if sc.IsValid() && !seen[key] {
			seen[key] = true
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	return links
}
