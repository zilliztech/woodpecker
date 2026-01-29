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
	"strconv"
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
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
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
	mu         sync.Mutex
	bucketName string
	rootPath   string
	logId      int64
	segmentId  int64
	entryId    int64
	value      []byte
	callback   func(segmentId int64, entryId int64, err error)

	clientPool      client.LogStoreClientPool
	handle          SegmentHandle
	ackSet          *bitset.BitSet
	quorumInfo      *proto.QuorumInfo
	resultChannels  []channel.ResultChannel
	channelAttempts []int
	channelErrors   []error // Each channel has its own error
	finalFailureSet *bitset.BitSet

	completed  atomic.Bool
	fastCalled atomic.Bool // Prevent repeated calls to FastFail/FastSuccess
}

func NewAppendOp(bucketName string, rootPath string, logId int64, segmentId int64, entryId int64, value []byte, callback func(segmentId int64, entryId int64, err error),
	clientPool client.LogStoreClientPool, handle SegmentHandle, quorumInfo *proto.QuorumInfo) *AppendOp {
	op := &AppendOp{
		bucketName: bucketName,
		rootPath:   rootPath,
		logId:      logId,
		segmentId:  segmentId,
		entryId:    entryId,
		value:      value,
		callback:   callback,

		clientPool:      clientPool,
		handle:          handle,
		ackSet:          &bitset.BitSet{},
		quorumInfo:      quorumInfo,
		resultChannels:  make([]channel.ResultChannel, 0),
		channelAttempts: make([]int, len(quorumInfo.Nodes)),
		channelErrors:   make([]error, len(quorumInfo.Nodes)),
		finalFailureSet: &bitset.BitSet{},
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

	// Initialize result channels for each node if not already done
	if len(op.resultChannels) == 0 {
		op.resultChannels = make([]channel.ResultChannel, len(op.quorumInfo.Nodes))
	}

	for i := 0; i < len(op.quorumInfo.Nodes); i++ {
		// send request to the node
		op.sendWriteRequestRetry(ctx, i)
	}
}

// sendWriteRequestRetry used for retry single request
func (op *AppendOp) sendWriteRequestRetry(ctx context.Context, serverIndex int) {
	// clear channel error before start send
	op.channelErrors[serverIndex] = nil
	// get client from clientPool according node addr
	serverAddr := op.quorumInfo.Nodes[serverIndex]
	cli, clientErr := op.clientPool.GetLogStoreClient(ctx, serverAddr)
	if clientErr != nil {
		op.channelErrors[serverIndex] = clientErr
		// segHandle failure async
		go op.handle.HandleAppendRequestFailure(ctx, op.entryId, clientErr, serverIndex, serverAddr)
		return
	}
	// send request to the node
	op.sendWriteRequest(ctx, cli, serverIndex, serverAddr)
}

func (op *AppendOp) sendWriteRequest(ctx context.Context, cli client.LogStoreClient, serverIndex int, serverAddr string) {
	ctx, sp := logger.NewIntentCtx("AppendOp", "sendWriteRequest")
	defer sp.End()
	startRequestTime := time.Now()

	if len(op.resultChannels) > serverIndex && op.resultChannels[serverIndex] == nil {
		// create new result channel for this server if not exists
		if cli.IsRemoteClient() {
			resultChannel := channel.NewRemoteResultChannel(op.Identifier())
			op.resultChannels[serverIndex] = resultChannel
		} else {
			resultChannel := channel.NewLocalResultChannel(op.Identifier())
			op.resultChannels[serverIndex] = resultChannel
		}
	}

	// order request
	entryId, err := cli.AppendEntry(ctx, op.bucketName, op.rootPath, op.logId, op.toLogEntry(), op.resultChannels[serverIndex])
	sp.AddEvent("AppendEntryCall", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startRequestTime).Milliseconds()), attribute.Int("serverIndex", serverIndex)))

	// TODO: Consider using a centralized register and notification mechanism for improved efficiency
	// async received ack without order
	go op.receivedAckCallback(ctx, startRequestTime, entryId, op.resultChannels[serverIndex], err, serverIndex, serverAddr)
}

func (op *AppendOp) receivedAckCallback(ctx context.Context, startRequestTime time.Time, entryId int64, resultChan channel.ResultChannel, err error, serverIndex int, serverAddr string) {
	ctx, sp := logger.NewIntentCtx("AppendOp", "receivedAckCallback")
	defer sp.End()
	// sync call error, return directly
	if err != nil {
		op.channelErrors[serverIndex] = err
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, err, serverIndex, serverAddr)
		return
	}
	// async call error, wait until syncedCh closed
	subCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // TODO configurable
	defer cancel()
	syncedResult, readChanErr := resultChan.ReadResult(subCtx)
	sp.AddEvent("wait callback", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startRequestTime).Milliseconds()), attribute.Int("serverIndex", serverIndex), attribute.String("serverAddr", serverAddr)))
	if readChanErr != nil {
		if errors.IsAny(readChanErr, context.Canceled, context.DeadlineExceeded) {
			// read chan timeout, retry
			logger.Ctx(ctx).Warn("read chan timeout",
				zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
		}
		if werr.ErrAppendOpResultChannelClosed.Is(readChanErr) {
			// chan already close
			logger.Ctx(ctx).Warn("chan already closed",
				zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
		}
		// read chan error, retry if necessary
		op.channelErrors[serverIndex] = readChanErr
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, readChanErr, serverIndex, serverAddr)
		return
	}

	if op.fastCalled.Load() {
		logger.Ctx(ctx).Debug("received ack but already fast completed",
			zap.Int64("syncedId", syncedResult.SyncedId), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
		return
	}

	if syncedResult.SyncedId == -1 || syncedResult.Err != nil {
		op.channelErrors[serverIndex] = syncedResult.Err
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, syncedResult.Err, serverIndex, serverAddr)
		return
	}

	// set and count if ack >= aq
	if syncedResult.SyncedId != -1 && syncedResult.SyncedId >= op.entryId {
		ackCount := op.ackSet.SetAndCount(serverIndex)
		if ackCount >= int(op.quorumInfo.Aq) {
			// Use atomic operation to ensure SendAppendSuccessCallbacks is called only once
			if op.completed.CompareAndSwap(false, true) {
				op.handle.SendAppendSuccessCallbacks(ctx, op.entryId)
				cost := time.Since(startRequestTime)
				metrics.WpClientAppendLatency.WithLabelValues(strconv.FormatInt(op.logId, 10)).Observe(float64(cost.Milliseconds()))
				metrics.WpClientAppendBytes.WithLabelValues(strconv.FormatInt(op.logId, 10)).Observe(float64(len(op.value)))
			}
		}
		logger.Ctx(ctx).Debug("synced received",
			zap.Int64("syncedId", syncedResult.SyncedId), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
		return
	}

	logger.Ctx(ctx).Debug("synced received, keep async waiting",
		zap.Int64("syncedId", syncedResult.SyncedId), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
}

func (op *AppendOp) FastFail(ctx context.Context, err error) {
	logger.Ctx(ctx).Debug("FastFail start calling",
		zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(err))
	op.mu.Lock()
	defer op.mu.Unlock()
	// Use atomic operation to ensure it is executed only once
	if !op.fastCalled.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug("FastFail already called, skipping",
			zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
		return // Already called
	}

	logger.Ctx(ctx).Debug("FastFail called, processing channels",
		zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Int("channels", len(op.resultChannels)), zap.Error(err))

	for index, ch := range op.resultChannels {
		if ch == nil {
			logger.Ctx(ctx).Info("FastFail channel is nil, skipping",
				zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
			continue
		}
		sendErr := ch.SendResult(ctx, &channel.AppendResult{
			SyncedId: -1,
			Err:      err,
		})
		if sendErr != nil {
			logger.Ctx(ctx).Warn("send FastFail result to channel failed",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(sendErr))
		} else {
			logger.Ctx(ctx).Debug("send FastFail result to channel finish",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
		}
		closeErr := ch.Close(ctx)
		if closeErr != nil {
			logger.Ctx(ctx).Warn("failed to close channel in FastFail",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(closeErr))
		} else {
			logger.Ctx(ctx).Debug("close channel in FastFail finish",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
		}
	}

	op.callback(op.segmentId, op.entryId, err)
	logger.Ctx(ctx).Debug("FastFail completed",
		zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(err))
}

func (op *AppendOp) FastSuccess(ctx context.Context) {
	op.mu.Lock()
	defer op.mu.Unlock()
	// Use atomic operation to ensure it is executed only once
	if !op.fastCalled.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug("FastSuccess already called, skipping",
			zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
		return // Already called
	}

	logger.Ctx(ctx).Debug("FastSuccess called, processing channels",
		zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Int("channels", len(op.resultChannels)))

	for index, ch := range op.resultChannels {
		sendErr := ch.SendResult(ctx, &channel.AppendResult{
			SyncedId: op.entryId,
			Err:      nil,
		})
		if sendErr != nil {
			logger.Ctx(ctx).Warn("send FastSuccess result to channel failed",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(sendErr))
		} else {
			logger.Ctx(ctx).Debug("send FastSuccess result to channel finish",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
		}
		closeErr := ch.Close(ctx)
		if closeErr != nil {
			logger.Ctx(ctx).Warn("failed to close channel in FastSuccess",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(closeErr))
		} else {
			logger.Ctx(ctx).Debug("close channel in FastSuccess finish",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
		}
	}

	op.callback(op.segmentId, op.entryId, nil)
	logger.Ctx(ctx).Debug("FastSuccess completed",
		zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
}

func (op *AppendOp) toLogEntry() *proto.LogEntry {
	return &proto.LogEntry{
		SegId:   op.segmentId,
		EntryId: op.entryId,
		Values:  op.value,
	}
}
