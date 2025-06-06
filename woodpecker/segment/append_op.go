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
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

type Operation interface {
	// Identifier returns a unique identifier for this operation.
	Identifier() string
	// Execute executes the operation.
	Execute()
}

var _ Operation = (*AppendOp)(nil)

// AppendOp represents an operation to append data to a log segment.
// Once all LogStores have successfully acknowledged the append operation,
// it checks if it is at the head of the pending adds queue.
// If it is, it sends an acknowledgment back to the application.
// If a LogStore fails, it retries multiple times.
type AppendOp struct {
	mu        sync.Mutex
	logId     int64
	segmentId int64
	entryId   int64
	value     []byte
	callback  func(segmentId int64, entryId int64, err error)

	clientPool     client.LogStoreClientPool
	handle         SegmentHandle
	ackSet         *bitset.BitSet
	quorumInfo     *proto.QuorumInfo
	resultChannels []chan int64

	completed  atomic.Bool
	fastCalled atomic.Bool // Prevent repeated calls to FastFail/FastSuccess
	err        error

	attempt int // attemptId
}

func NewAppendOp(logId int64, segmentId int64, entryId int64, value []byte, callback func(segmentId int64, entryId int64, err error),
	clientPool client.LogStoreClientPool, handle SegmentHandle, quorumInfo *proto.QuorumInfo, attempt int) *AppendOp {
	op := &AppendOp{
		logId:     logId,
		segmentId: segmentId,
		entryId:   entryId,
		value:     value,
		callback:  callback,

		clientPool:     clientPool,
		handle:         handle,
		ackSet:         &bitset.BitSet{},
		quorumInfo:     quorumInfo,
		resultChannels: make([]chan int64, 0),

		attempt: attempt,
	}
	op.completed.Store(false)
	return op
}

func (op *AppendOp) Identifier() string {
	return fmt.Sprintf("%d/%d/%d", op.logId, op.segmentId, op.entryId)
}

func (op *AppendOp) Execute() {
	ctx, sp := logger.NewIntentCtx("AppendOp", fmt.Sprintf("%d/%d/%d", op.logId, op.segmentId, op.entryId))
	defer sp.End()
	op.mu.Lock()
	defer op.mu.Unlock()
	// get ES/WQ/AQ
	quorumInfo, err := op.handle.GetQuorumInfo(ctx)
	if err != nil {
		op.err = err
		op.handle.SendAppendErrorCallbacks(ctx, op.entryId, err)
		return
	}

	// Update quorumInfo to ensure consistency
	op.quorumInfo = quorumInfo

	// current only 1
	op.resultChannels = make([]chan int64, len(quorumInfo.Nodes))

	for i := 0; i < len(quorumInfo.Nodes); i++ {
		// get client from clientPool according node addr
		cli, clientErr := op.clientPool.GetLogStoreClient(quorumInfo.Nodes[i])
		if clientErr != nil {
			op.err = clientErr
			op.handle.SendAppendErrorCallbacks(ctx, op.entryId, clientErr)
			return
		}
		// send request to the node
		op.sendWriteRequest(ctx, cli, i)
	}
}

func (op *AppendOp) sendWriteRequest(ctx context.Context, cli client.LogStoreClient, serverIndex int) {
	startRequestTime := time.Now()
	syncedResultCh := make(chan int64, 1)
	op.resultChannels[serverIndex] = syncedResultCh
	// order request
	entryId, err := cli.AppendEntry(ctx, op.logId, op.toSegmentEntry(), syncedResultCh)
	// async received ack without order
	go op.receivedAckCallback(ctx, startRequestTime, entryId, syncedResultCh, err, serverIndex)
}

func (op *AppendOp) receivedAckCallback(ctx context.Context, startRequestTime time.Time, entryId int64, syncedCh <-chan int64, err error, serverIndex int) {
	// sync call error, return directly
	if err != nil {
		op.err = err
		op.handle.SendAppendErrorCallbacks(ctx, op.entryId, err)
		return
	}
	// async call error, wait until syncedCh closed
	ticker := time.NewTicker(30 * time.Second) // Log slow append warning every 30s, TODO should be configurable
	defer ticker.Stop()

	for {
		select {
		case syncedId, ok := <-syncedCh:
			if op.fastCalled.Load() {
				logger.Ctx(ctx).Debug(fmt.Sprintf("received ack for log:%d seg:%d entry:%d, but already fast completed", op.logId, op.segmentId, op.entryId))
				return
			}
			if !ok {
				logger.Ctx(ctx).Debug(fmt.Sprintf("synced chan for log:%d seg:%d entry:%d closed", op.logId, op.segmentId, op.entryId))
				return
			}
			if syncedId == -1 {
				logger.Ctx(ctx).Debug(fmt.Sprintf("synced failed for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId))
				op.handle.SendAppendErrorCallbacks(ctx, op.entryId, werr.ErrSegmentWriteException)
				return
			}
			if syncedId != -1 && syncedId >= op.entryId {
				op.ackSet.Set(serverIndex)
				if op.ackSet.Count() >= int(op.quorumInfo.Wq) {
					// Use atomic operation to ensure SendAppendSuccessCallbacks is called only once
					if op.completed.CompareAndSwap(false, true) {
						op.handle.SendAppendSuccessCallbacks(ctx, op.entryId)
						cost := time.Now().Sub(startRequestTime)
						metrics.WpClientAppendLatency.WithLabelValues(fmt.Sprintf("%d", op.logId)).Observe(float64(cost.Milliseconds()))
						metrics.WpClientAppendBytes.WithLabelValues(fmt.Sprintf("%d", op.logId)).Observe(float64(len(op.value)))
					}
				}
				logger.Ctx(ctx).Debug(fmt.Sprintf("synced received:%d for log:%d seg:%d entry:%d ", syncedId, op.logId, op.segmentId, op.entryId))
				return
			}
			logger.Ctx(ctx).Debug(fmt.Sprintf("synced received:%d for log:%d seg:%d entry:%d, keep async waiting", syncedId, op.logId, op.segmentId, op.entryId))
		case <-ticker.C:
			elapsed := time.Now().Sub(startRequestTime)
			logger.Ctx(ctx).Warn(fmt.Sprintf("slow append detected for log:%d seg:%d entry:%d, elapsed: %v, failed and retry", op.logId, op.segmentId, op.entryId, elapsed))
			if op.fastCalled.Load() {
				logger.Ctx(ctx).Debug(fmt.Sprintf("append fast completed for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId))
				return
			}
			op.handle.SendAppendErrorCallbacks(ctx, op.entryId, werr.ErrSegmentWriteError.WithCauseErrMsg("slow append error"))
			return
		}
	}
}

func (op *AppendOp) FastFail(ctx context.Context, err error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	// Use atomic operation to ensure it is executed only once
	if !op.fastCalled.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug(fmt.Sprintf("FastFail already called for log:%d seg:%d entry:%d, skipping", op.logId, op.segmentId, op.entryId))
		return // Already called
	}

	logger.Ctx(ctx).Debug(fmt.Sprintf("FastFail called for log:%d seg:%d entry:%d, processing %d channels", op.logId, op.segmentId, op.entryId, len(op.resultChannels)), zap.Error(err))

	for i, ch := range op.resultChannels {
		// Safely handle channel operations
		func(ch chan int64, index int) {
			defer func() {
				if r := recover(); r != nil {
					logger.Ctx(ctx).Debug(fmt.Sprintf("FastFail recovered from panic on channel %d for log:%d seg:%d entry:%d: %v", index, op.logId, op.segmentId, op.entryId, r))
				}
			}()

			select {
			case ch <- -1:
				logger.Ctx(ctx).Debug(fmt.Sprintf("FastFail sent failure signal to channel %d for log:%d seg:%d entry:%d", index, op.logId, op.segmentId, op.entryId))
			default:
				logger.Ctx(ctx).Debug(fmt.Sprintf("FastFail channel %d full or closed for log:%d seg:%d entry:%d", index, op.logId, op.segmentId, op.entryId))
			}
			close(ch)
		}(ch, i)
	}

	op.callback(op.segmentId, op.entryId, err)
	logger.Ctx(ctx).Debug(fmt.Sprintf("FastFail completed for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId), zap.Error(err))
}

func (op *AppendOp) FastSuccess(ctx context.Context) {
	op.mu.Lock()
	defer op.mu.Unlock()
	// Use atomic operation to ensure it is executed only once
	if !op.fastCalled.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug(fmt.Sprintf("FastSuccess already called for log:%d seg:%d entry:%d, skipping", op.logId, op.segmentId, op.entryId))
		return // Already called
	}

	logger.Ctx(ctx).Debug(fmt.Sprintf("FastSuccess called for log:%d seg:%d entry:%d, processing %d channels", op.logId, op.segmentId, op.entryId, len(op.resultChannels)))

	for i, ch := range op.resultChannels {
		// Safely handle channel operations
		func(ch chan int64, index int) {
			defer func() {
				if r := recover(); r != nil {
					logger.Ctx(ctx).Debug(fmt.Sprintf("FastSuccess recovered from panic on channel %d for log:%d seg:%d entry:%d: %v", index, op.logId, op.segmentId, op.entryId, r))
				}
			}()

			select {
			case ch <- op.entryId:
				logger.Ctx(ctx).Debug(fmt.Sprintf("FastSuccess sent success signal (%d) to channel %d for log:%d seg:%d entry:%d", op.entryId, index, op.logId, op.segmentId, op.entryId))
			default:
				logger.Ctx(ctx).Debug(fmt.Sprintf("FastSuccess channel %d full or closed for log:%d seg:%d entry:%d", index, op.logId, op.segmentId, op.entryId))
			}
			close(ch)
		}(ch, i)
	}

	op.callback(op.segmentId, op.entryId, nil)
	logger.Ctx(ctx).Debug(fmt.Sprintf("FastSuccess completed for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId))
}

func (op *AppendOp) toSegmentEntry() *processor.SegmentEntry {
	return &processor.SegmentEntry{
		SegmentId: op.segmentId,
		EntryId:   op.entryId,
		Data:      op.value,
	}
}
