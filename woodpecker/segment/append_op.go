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
	"github.com/zilliztech/woodpecker/common/topology"
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
	logNs      string

	clientPool      client.LogStoreClientPool
	handle          SegmentHandle
	ackSet          *bitset.BitSet
	quorumInfo      *proto.QuorumInfo
	nodeScopes      []string // per node index: how far that replica sits from this client
	resultChannels  []channel.ResultChannel
	channelAttempts []int
	channelErrors   []error // Each channel has its own error
	finalFailureSet *bitset.BitSet

	completed  atomic.Bool
	fastCalled atomic.Bool // Prevent repeated calls to FastFail/FastSuccess
}

func NewAppendOp(bucketName string, rootPath string, logId int64, segmentId int64, entryId int64, value []byte, callback func(segmentId int64, entryId int64, err error),
	clientPool client.LogStoreClientPool, handle SegmentHandle, quorumInfo *proto.QuorumInfo, nodeScopes []string,
) *AppendOp {
	op := &AppendOp{
		bucketName: bucketName,
		rootPath:   rootPath,
		logId:      logId,
		segmentId:  segmentId,
		entryId:    entryId,
		value:      value,
		callback:   callback,
		logNs:      metrics.BuildLogNs(bucketName, rootPath),

		clientPool:      clientPool,
		handle:          handle,
		ackSet:          &bitset.BitSet{},
		quorumInfo:      quorumInfo,
		nodeScopes:      nodeScopes,
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

// recordReplicaResult counts one replica's outcome for this entry, labelled by
// how far that replica sits from this client. A quorum append writes to every
// replica, so this is where cross-AZ write traffic becomes visible.
func (op *AppendOp) recordReplicaResult(serverIndex int, status string) {
	scope := topology.ScopeUnknown
	if serverIndex >= 0 && serverIndex < len(op.nodeScopes) {
		scope = op.nodeScopes[serverIndex]
	}
	logIdStr := strconv.FormatInt(op.logId, 10)
	metrics.WpClientReplicaAppendTotal.WithLabelValues(op.logNs, logIdStr, scope, status).Inc()
	if status == "success" {
		metrics.WpClientReplicaAppendBytesTotal.WithLabelValues(op.logNs, logIdStr, scope).Add(float64(len(op.value)))
	}
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

	// Fan out the per-replica sends concurrently.
	//
	// Each sendWriteRequestRetry blocks inside cli.AppendEntry until that
	// replica returns its first (Buffered) response. Issuing them sequentially
	// made an op's critical path the SUM of the per-replica round-trips, which
	// caps single-log append throughput at roughly 1/(ensembleSize * RTT). The
	// durable acks are already handled asynchronously (see receivedAckCallback),
	// so only these initial sends were serialized.
	//
	// Sending to all replicas in parallel makes the critical path the MAX
	// round-trip instead of the sum, lifting single-log throughput by up to the
	// ensemble size. Each goroutine only touches its own serverIndex slot
	// (resultChannels[i], channelErrors[i], channelAttempts[i]), so there is no
	// shared-state race. Ordering is unaffected: entry IDs are pre-assigned in
	// AppendAsync and acknowledged in order by SendAppendSuccessCallbacks.
	nodeCount := len(op.quorumInfo.Nodes)
	if nodeCount == 1 {
		// Fast path: avoid goroutine/WaitGroup overhead for a single replica.
		op.sendWriteRequestRetry(ctx, 0)
		return
	}
	var wg sync.WaitGroup
	wg.Add(nodeCount)
	for i := 0; i < nodeCount; i++ {
		go func(serverIndex int) {
			defer wg.Done()
			// send request to the node
			op.sendWriteRequestRetry(ctx, serverIndex)
		}(i)
	}
	wg.Wait()
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
		op.recordReplicaResult(serverIndex, "error")
		// segHandle failure async
		go op.handle.HandleAppendRequestFailure(ctx, op.entryId, clientErr, serverIndex, serverAddr)
		return
	}
	// send request to the node
	op.sendWriteRequest(ctx, cli, serverIndex, serverAddr)
}

func (op *AppendOp) sendWriteRequest(ctx context.Context, cli client.LogStoreClient, serverIndex int, serverAddr string) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, "AppendOp", "sendWriteRequest")
	defer sp.End()
	startRequestTime := time.Now()

	if len(op.resultChannels) > serverIndex {
		// A fresh result channel per attempt, never a reused one.
		//
		// The goroutine spawned below owns the channel it is handed and closes it
		// on the way out, and that close runs outside op.mu. Reusing the slot would
		// let the previous attempt's close land on THIS attempt's channel and cancel
		// the stream it just opened - which fails this attempt, spends another retry,
		// and can repeat until the budget is gone.
		//
		// Nothing worth carrying over survives an attempt anyway. Every attempt opens
		// its own stream, context and cancel; InitResponseStream overwrites all three
		// without cancelling the previous ones, and leaves any result already recorded
		// in place for the next read to return instead of the new stream's.
		//
		// Building it fresh also picks the type the client requires, which is what the
		// type check here existed for: a remote client's single-entry AppendEntry
		// rejects the LocalResultChannel the batch path leaves in this slot.
		if cli.IsRemoteClient() {
			op.resultChannels[serverIndex] = channel.NewRemoteResultChannel(op.Identifier())
		} else {
			op.resultChannels[serverIndex] = channel.NewLocalResultChannel(op.Identifier())
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
	ctx, sp := logger.NewIntentCtxWithParent(ctx, "AppendOp", "receivedAckCallback")
	defer sp.End()

	// This goroutine is the only reader of resultChan and the last user of the
	// stream behind it, so it owns retiring both.
	//
	// Retiring them HERE rather than in FastSuccess is the point. FastSuccess runs
	// the moment the ack quorum is reached, while the replica outside that quorum
	// is still answering perfectly normally; for a RemoteResultChannel, Close()
	// cancels that replica's gRPC stream, so its normal completion came back as
	// "rpc error: code = Canceled" and was logged as a failure once per entry.
	//
	// The deferred call guarantees it happens on every exit path; the explicit
	// calls below make it happen as soon as the replica's answer is in, so the
	// stream is not held across the segment-handle bookkeeping that follows -
	// which takes that handle's lock.
	//
	// FastFail still closes early, deliberately: there the entry is abandoned and
	// the waiters should stop at once rather than sit out their read budget.
	// Close is idempotent, so an early close and this one cannot conflict.
	channelRetired := false
	retireResultChannel := func() {
		if channelRetired || resultChan == nil {
			return
		}
		channelRetired = true
		if closeErr := resultChan.Close(ctx); closeErr != nil {
			logger.Ctx(ctx).Warn("failed to close append result channel",
				zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId),
				zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr), zap.Error(closeErr))
		}
	}
	defer retireResultChannel()

	// sync call error, return directly
	if err != nil {
		// The send never got far enough for an ack to arrive, so nothing is
		// waiting on this channel any more.
		retireResultChannel()
		// Skip further processing if operation already completed via FastFail/FastSuccess
		if op.fastCalled.Load() {
			return
		}
		op.channelErrors[serverIndex] = err
		op.recordReplicaResult(serverIndex, "error")
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, err, serverIndex, serverAddr)
		return
	}
	// async call error, wait until syncedCh closed
	subCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // TODO configurable
	defer cancel()
	syncedResult, readChanErr := resultChan.ReadResult(subCtx)
	sp.AddEvent("wait callback", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startRequestTime).Milliseconds()), attribute.Int("serverIndex", serverIndex), attribute.String("serverAddr", serverAddr)))
	// The read was the last use of this channel and of the stream behind it.
	retireResultChannel()

	// If operation already completed via FastFail/FastSuccess, skip further processing
	if op.fastCalled.Load() {
		logger.Ctx(ctx).Debug("received ack but already fast completed",
			zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
		return
	}

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
		op.recordReplicaResult(serverIndex, "error")
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, readChanErr, serverIndex, serverAddr)
		return
	}

	if syncedResult.SyncedId == -1 || syncedResult.Err != nil {
		op.channelErrors[serverIndex] = syncedResult.Err
		op.recordReplicaResult(serverIndex, "error")
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, syncedResult.Err, serverIndex, serverAddr)
		return
	}

	// set and count if ack >= aq
	if syncedResult.SyncedId != -1 && syncedResult.SyncedId >= op.entryId {
		op.recordReplicaResult(serverIndex, "success")
		ackCount := op.ackSet.SetAndCount(serverIndex)
		if ackCount >= int(op.quorumInfo.Aq) {
			// Use atomic operation to ensure SendAppendSuccessCallbacks is called only once
			if op.completed.CompareAndSwap(false, true) {
				op.handle.SendAppendSuccessCallbacks(ctx, op.entryId)
				cost := time.Since(startRequestTime)
				metrics.WpClientAppendLatency.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(cost.Milliseconds()))
				metrics.WpClientAppendBytes.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(len(op.value)))
			}
		}
		logger.Ctx(ctx).Debug("synced received",
			zap.Int64("syncedId", syncedResult.SyncedId), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
		return
	}

	logger.Ctx(ctx).Debug("synced received, keep async waiting",
		zap.Int64("syncedId", syncedResult.SyncedId), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.String("serverAddr", serverAddr))
}

// applyNodeAck processes a single node's durability result for this op: quorum
// counting and, on reaching Aq, the in-order SendAppendSuccessCallbacks. It is
// the post-read half of receivedAckCallback, factored out so the batch path can
// drive many ops' acks from ONE per-node goroutine (reading the results in
// entry-id order) instead of spawning a goroutine per op.
func (op *AppendOp) applyNodeAck(ctx context.Context, startRequestTime time.Time, result *channel.AppendResult, readErr error, serverIndex int, serverAddr string) {
	if op.fastCalled.Load() {
		return
	}
	if readErr != nil {
		op.channelErrors[serverIndex] = readErr
		op.recordReplicaResult(serverIndex, "error")
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, readErr, serverIndex, serverAddr)
		return
	}
	if result.SyncedId == -1 || result.Err != nil {
		op.channelErrors[serverIndex] = result.Err
		op.recordReplicaResult(serverIndex, "error")
		op.handle.HandleAppendRequestFailure(ctx, op.entryId, result.Err, serverIndex, serverAddr)
		return
	}
	if result.SyncedId >= op.entryId {
		op.recordReplicaResult(serverIndex, "success")
		if op.ackSet.SetAndCount(serverIndex) >= int(op.quorumInfo.Aq) {
			if op.completed.CompareAndSwap(false, true) {
				op.handle.SendAppendSuccessCallbacks(ctx, op.entryId)
				cost := time.Since(startRequestTime)
				metrics.WpClientAppendLatency.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(cost.Milliseconds()))
				metrics.WpClientAppendBytes.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(len(op.value)))
			}
		}
	}
}

func (op *AppendOp) FastFail(ctx context.Context, err error) {
	logger.Ctx(ctx).Debug("FastFail start calling",
		zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(err))
	op.mu.Lock()
	defer op.mu.Unlock()
	// Use atomic operation to ensure it is executed only once
	if !op.fastCalled.CompareAndSwap(false, true) {
		return // Already called
	}

	for index, ch := range op.resultChannels {
		if ch == nil {
			logger.Ctx(ctx).Debug("FastFail channel is nil, skipping",
				zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId))
			continue
		}
		if ch.IsClosed() {
			// Its reader has already finished with it and retired it: there is
			// nobody left to wake, and both SendResult and Close on a closed
			// channel would only produce a warning.
			continue
		}
		sendErr := ch.SendResult(ctx, &channel.AppendResult{
			SyncedId: -1,
			Err:      err,
		})
		if sendErr != nil {
			logger.Ctx(ctx).Warn("send FastFail result to channel failed",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(sendErr))
		}
		closeErr := ch.Close(ctx)
		if closeErr != nil {
			logger.Ctx(ctx).Warn("failed to close channel in FastFail",
				zap.Int("channelIndex", index), zap.Int64("logId", op.logId), zap.Int64("segId", op.segmentId), zap.Int64("entryId", op.entryId), zap.Error(closeErr))
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
		return // Already called
	}

	// FastSuccess deliberately touches no result channel, unlike FastFail.
	//
	// Quorum is satisfied, so there is nothing left to hurry. The replicas that
	// answered have already retired their own channels, and writing into those
	// only fails and logs - once per replica per entry, on every successful
	// append, which is the very cost this change exists to remove. The replicas
	// still answering are the whole point: a synthetic success written to one of
	// them would be read INSTEAD of that replica's real answer, and on the batch
	// path it would land in a one-slot buffer, so the drain would consume the
	// synthetic result while the replica's actual ack was dropped as
	// "channel is full".
	//
	// Closing is likewise left to each channel's reader; for a
	// RemoteResultChannel, Close cancels the replica's stream mid-answer.

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
