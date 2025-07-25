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
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
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
	resultChannels []channel.ResultChannel

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
		resultChannels: make([]channel.ResultChannel, 0),

		attempt: attempt,
	}
	op.completed.Store(false)
	return op
}

func (op *AppendOp) Identifier() string {
	return fmt.Sprintf("%d/%d/%d", op.logId, op.segmentId, op.entryId)
}

func (op *AppendOp) Execute() {
	ctx, sp := logger.NewIntentCtx("AppendOp", "Execute")
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

	// Initialize result channels for each node if not already done
	if len(op.resultChannels) == 0 {
		op.resultChannels = make([]channel.ResultChannel, len(quorumInfo.Nodes))
	}

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
	ctx, sp := logger.NewIntentCtx("AppendOp", "sendWriteRequest")
	defer sp.End()
	startRequestTime := time.Now()

	// TODO currently only support Local ResultChannel
	if len(op.resultChannels) > serverIndex && op.resultChannels[serverIndex] == nil {
		// create new result channel for this server if not exists
		resultChannel := channel.NewLocalResultChannel(op.Identifier())
		op.resultChannels[serverIndex] = resultChannel
	}

	// order request
	entryId, err := cli.AppendEntry(ctx, op.logId, op.toSegmentEntry(), op.resultChannels[serverIndex])
	sp.AddEvent("AppendEntryCall", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startRequestTime).Milliseconds()), attribute.Int("serverIndex", serverIndex)))

	// TODO: Consider using a centralized register and notification mechanism for improved efficiency
	// async received ack without order
	go op.receivedAckCallback(ctx, startRequestTime, entryId, op.resultChannels[serverIndex], err, serverIndex)
}

func (op *AppendOp) receivedAckCallback(ctx context.Context, startRequestTime time.Time, entryId int64, resultChan channel.ResultChannel, err error, serverIndex int) {
	ctx, sp := logger.NewIntentCtx("AppendOp", "receivedAckCallback")
	defer sp.End()
	// sync call error, return directly
	if err != nil {
		op.err = err
		op.handle.SendAppendErrorCallbacks(ctx, op.entryId, err)
		return
	}
	// async call error, wait until syncedCh closed
	subCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // TODO configurable
	defer cancel()
	syncedResult, readChanErr := resultChan.ReadResult(subCtx)
	sp.AddEvent("wait callback", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startRequestTime).Milliseconds()), attribute.Int("serverIndex", serverIndex)))
	if readChanErr != nil {
		if errors.IsAny(readChanErr, context.Canceled, context.DeadlineExceeded) {
			// read chan timeout, retry
			logger.Ctx(ctx).Warn(fmt.Sprintf("read chan timeout for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId))
			op.err = err
			op.handle.SendAppendErrorCallbacks(ctx, op.entryId, err)
			return
		}
		// chan already close, just return
		logger.Ctx(ctx).Warn(fmt.Sprintf("chan already close for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId))
		return
	}

	if op.fastCalled.Load() {
		logger.Ctx(ctx).Debug(fmt.Sprintf("received ack:%d for log:%d seg:%d entry:%d, but already fast completed", syncedResult.SyncedId, op.logId, op.segmentId, op.entryId))
		return
	}

	if syncedResult.SyncedId == -1 || syncedResult.Err != nil {
		op.err = syncedResult.Err
		op.handle.SendAppendErrorCallbacks(ctx, op.entryId, syncedResult.Err)
		return
	}
	if syncedResult.SyncedId != -1 && syncedResult.SyncedId >= op.entryId {
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
		logger.Ctx(ctx).Debug(fmt.Sprintf("synced received:%d for log:%d seg:%d entry:%d ", syncedResult.SyncedId, op.logId, op.segmentId, op.entryId))
		return
	}

	logger.Ctx(ctx).Debug(fmt.Sprintf("synced received:%d for log:%d seg:%d entry:%d, keep async waiting", syncedResult.SyncedId, op.logId, op.segmentId, op.entryId))
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

	for index, ch := range op.resultChannels {
		sendErr := ch.SendResult(ctx, &channel.AppendResult{
			SyncedId: -1,
			Err:      err,
		})
		if sendErr != nil {
			logger.Ctx(ctx).Warn(fmt.Sprintf("Send FastFail result to channel failed %d for log:%d seg:%d entry:%d: %v", index, op.logId, op.segmentId, op.entryId, sendErr))
		} else {
			logger.Ctx(ctx).Debug(fmt.Sprintf("Send FastFail result to to channel finish %d for log:%d seg:%d entry:%d: ", index, op.logId, op.segmentId, op.entryId))
		}
		closeErr := ch.Close(ctx)
		if closeErr != nil {
			logger.Ctx(ctx).Warn(fmt.Sprintf("failed to close channel %d for log:%d seg:%d entry:%d: %v", index, op.logId, op.segmentId, op.entryId, closeErr))
		} else {
			logger.Ctx(ctx).Debug(fmt.Sprintf("finish to close channel %d for log:%d seg:%d entry:%d: ", index, op.logId, op.segmentId, op.entryId))
		}
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

	for index, ch := range op.resultChannels {
		sendErr := ch.SendResult(ctx, &channel.AppendResult{
			SyncedId: op.entryId,
			Err:      nil,
		})
		if sendErr != nil {
			logger.Ctx(ctx).Warn(fmt.Sprintf("Send FastSuccess result to channel failed %d for log:%d seg:%d entry:%d: %v", index, op.logId, op.segmentId, op.entryId, sendErr))
		} else {
			logger.Ctx(ctx).Debug(fmt.Sprintf("Send FastSuccess result to to channel finish %d for log:%d seg:%d entry:%d: ", index, op.logId, op.segmentId, op.entryId))
		}
		closeErr := ch.Close(ctx)
		if closeErr != nil {
			logger.Ctx(ctx).Warn(fmt.Sprintf("failed to close channel %d for log:%d seg:%d entry:%d: %v", index, op.logId, op.segmentId, op.entryId, closeErr))
		} else {
			logger.Ctx(ctx).Debug(fmt.Sprintf("finish to close channel %d for log:%d seg:%d entry:%d: ", index, op.logId, op.segmentId, op.entryId))
		}
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
