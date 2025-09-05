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
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

const (
	SegmentHandleScopeName = "SegmentHandle"
)

//go:generate mockery --dir=./woodpecker/segment --name=SegmentHandle --structname=SegmentHandle --output=mocks/mocks_woodpecker/mocks_segment_handle --filename=mock_segment_handle.go --with-expecter=true  --outpkg=mocks_segment_handle
type SegmentHandle interface {
	// GetLogName to which the segment belongs
	GetLogName() string
	// GetId of the segment
	GetId(ctx context.Context) int64
	// AppendAsync data to the segment asynchronously
	AppendAsync(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error))
	// ReadBatchAdv num of entries from the segment
	ReadBatchAdv(ctx context.Context, from int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error)
	// GetLastAddConfirmed entryId for the segment
	GetLastAddConfirmed(ctx context.Context) (int64, error)
	// GetLastAddPushed entryId for the segment
	GetLastAddPushed(ctx context.Context) (int64, error)
	// GetMetadata of the segment
	GetMetadata(ctx context.Context) *meta.SegmentMeta
	// RefreshAndGetMetadata of the segment
	RefreshAndGetMetadata(ctx context.Context) error
	// GetQuorumInfo of the segment if it's a active segment
	GetQuorumInfo(ctx context.Context) (*proto.QuorumInfo, error)
	// IsWritable check if the segment is writable
	IsWritable(ctx context.Context) (bool, error)
	// ForceCompleteAndClose the segment
	ForceCompleteAndClose(ctx context.Context) error
	// SendAppendSuccessCallbacks called when an appendOp operation is successful
	SendAppendSuccessCallbacks(ctx context.Context, triggerEntryId int64)
	// SendAppendErrorCallbacks called when an appendOp operation fails
	// Deprecated, use detail failure handle methods
	SendAppendErrorCallbacks(ctx context.Context, triggerEntryId int64, err error)
	// HandleAppendRequestFailure called when an appendOp operation fails
	HandleAppendRequestFailure(ctx context.Context, triggerEntryId int64, err error, serverIndex int, serverAddr string)
	// GetSize get the size of the segment
	GetSize(ctx context.Context) int64
	// GetBlocksCount get the number of blocks in the segment
	GetBlocksCount(ctx context.Context) int64
	// Complete the segment writing
	// Deprecated, no used in main logic
	Complete(ctx context.Context) (int64, error)
	// FenceAndComplete the segment in all nodes
	FenceAndComplete(ctx context.Context) (int64, error)
	// Compact is a recovery or compaction operation
	Compact(ctx context.Context) error
	// SetRollingReady set the segment as ready for rolling
	SetRollingReady(ctx context.Context)
	// IsForceRollingReady check if the segment is ready for rolling
	IsForceRollingReady(ctx context.Context) bool
	// GetLastAccessTime get the last access time of the segment
	GetLastAccessTime() int64
	// SetWriterInvalidationNotifier set the expired trigger
	SetWriterInvalidationNotifier(ctx context.Context, f func(ctx context.Context, reason string))
}

func NewSegmentHandle(ctx context.Context, logId int64, logName string, segmentMeta *meta.SegmentMeta, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration, canWrite bool) SegmentHandle {
	executeRequestMaxQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	segmentHandle := &segmentHandleImpl{
		logId:          logId,
		logName:        logName,
		segmentId:      segmentMeta.Metadata.SegNo,
		metadata:       metadata,
		ClientPool:     clientPool,
		appendOpsQueue: list.New(),
		quorumInfo:     segmentMeta.Metadata.Quorum,
		executor:       NewSequentialExecutor(executeRequestMaxQueueSize),
		cfg:            cfg,
	}
	segmentHandle.lastPushed.Store(segmentMeta.Metadata.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.Metadata.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Metadata.Size)
	segmentHandle.submittedSize.Store(0)
	segmentHandle.segmentMetaCache.Store(segmentMeta)
	segmentHandle.fencedState.Store(false)
	segmentHandle.rollingState.Store(false)
	if canWrite {
		segmentHandle.canWriteState.Store(true)
		segmentHandle.executor.Start(ctx)
	}
	return segmentHandle
}

// NewSegmentHandleWithAppendOpsQueue TODO TestOnly
func NewSegmentHandleWithAppendOpsQueue(ctx context.Context, logId int64, logName string, segmentMeta *meta.SegmentMeta, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration, testAppendOpsQueue *list.List) SegmentHandle {
	return NewSegmentHandleWithAppendOpsQueueWithWritable(ctx, logId, logName, segmentMeta, metadata, clientPool, cfg, testAppendOpsQueue, false)
}

// NewSegmentHandleWithAppendOpsQueue TODO TestOnly
func NewSegmentHandleWithAppendOpsQueueWithWritable(ctx context.Context, logId int64, logName string, segmentMeta *meta.SegmentMeta, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration, testAppendOpsQueue *list.List, writable bool) SegmentHandle {
	executeRequestMaxQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	segmentHandle := &segmentHandleImpl{
		logId:          logId,
		logName:        logName,
		segmentId:      segmentMeta.Metadata.SegNo,
		metadata:       metadata,
		ClientPool:     clientPool,
		appendOpsQueue: testAppendOpsQueue,
		quorumInfo: &proto.QuorumInfo{
			Id: 1,
			Wq: 1,
			Aq: 1,
			Es: 1,
			Nodes: []string{
				"127.0.0.1",
			},
		},
		executor: NewSequentialExecutor(executeRequestMaxQueueSize),
		cfg:      cfg,
	}
	segmentHandle.lastPushed.Store(segmentMeta.Metadata.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.Metadata.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Metadata.Size)
	segmentHandle.submittedSize.Store(0)
	segmentHandle.segmentMetaCache.Store(segmentMeta)
	segmentHandle.fencedState.Store(false)
	segmentHandle.canWriteState.Store(writable)
	segmentHandle.rollingState.Store(false)
	segmentHandle.lastAccessTime.Store(time.Now().UnixMilli())
	return segmentHandle
}

var _ SegmentHandle = (*segmentHandleImpl)(nil)

type segmentHandleImpl struct {
	logId            int64
	logName          string
	segmentId        int64
	segmentMetaCache atomic.Pointer[meta.SegmentMeta]
	metadata         meta.MetadataProvider
	ClientPool       client.LogStoreClientPool
	quorumInfo       *proto.QuorumInfo
	cfg              *config.Configuration

	sync.RWMutex
	lastPushed       atomic.Int64
	lastAddConfirmed atomic.Int64
	appendOpsQueue   *list.List
	commitedSize     atomic.Int64
	submittedSize    atomic.Int64

	fencedState    atomic.Bool // For fence state: true confirms it is fenced, while false requires verification by checking the storage backend for a fence flag file/object.
	canWriteState  atomic.Bool
	rollingState   atomic.Bool // For rolling ready state: true confirms it is rolling ready, once all appendOPs are completed and the segment is going to close.
	expiredTrigger func(ctx context.Context, reason string)

	executor *SequentialExecutor

	doingCompact atomic.Bool

	lastAccessTime atomic.Int64
}

func (s *segmentHandleImpl) GetLogName() string {
	return s.logName
}

func (s *segmentHandleImpl) GetId(ctx context.Context) int64 {
	return s.segmentId
}

func (s *segmentHandleImpl) AppendAsync(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error)) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "AppendAsync")
	defer sp.End()
	s.updateAccessTime()

	// Check fenced state first without lock to avoid deadlock
	if s.fencedState.Load() {
		callback(s.segmentId, -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("segmentHandle[%d/%d] fenced", s.logId, s.segmentId)))
		return
	}
	if s.rollingState.Load() {
		callback(s.segmentId, -1, werr.ErrSegmentHandleSegmentRolling.WithCauseErrMsg(fmt.Sprintf("segmentHandle[%d/%d] rolling", s.logId, s.segmentId)))
		return
	}

	s.Lock()
	defer s.Unlock()
	currentSegmentMeta := s.segmentMetaCache.Load()

	// Check segment state
	if currentSegmentMeta.Metadata.State != proto.SegmentState_Active {
		callback(currentSegmentMeta.Metadata.SegNo, -1, werr.ErrSegmentHandleSegmentStateInvalid)
		return
	}

	// Double-check fenced state under lock
	if s.fencedState.Load() {
		callback(s.segmentId, -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("segmentHandle[%d/%d] fenced", s.logId, s.segmentId)))
		return
	}
	if s.rollingState.Load() {
		callback(s.segmentId, -1, werr.ErrSegmentHandleSegmentRolling.WithCauseErrMsg(fmt.Sprintf("segmentHandle[%d/%d] rolling", s.logId, s.segmentId)))
		return
	}

	// Create pending append operation
	appendOp := s.createPendingAppendOp(ctx, bytes, callback)

	// Try to submit first, only add to queue if successful
	if submitOk := s.executor.Submit(ctx, appendOp); !submitOk {
		callback(currentSegmentMeta.Metadata.SegNo, -1, werr.ErrSegmentHandleSegmentClosed.WithCauseErrMsg("submit append failed, segment closed"))
		return
	}

	// Only add to queue and update metrics after successful submit
	s.appendOpsQueue.PushBack(appendOp)
	s.submittedSize.Add(int64(len(bytes)))
	metrics.WpClientAppendEntriesTotal.WithLabelValues(fmt.Sprintf("%d", s.logId)).Inc()
	metrics.WpClientAppendRequestsTotal.WithLabelValues(fmt.Sprintf("%d", s.logId)).Inc()
	metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(fmt.Sprintf("%d", s.logId)).Inc()
}

func (s *segmentHandleImpl) createPendingAppendOp(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error)) *AppendOp {
	pendingAppendOp := NewAppendOp(
		s.logId,
		s.GetId(ctx),
		s.lastPushed.Add(1),
		bytes,
		callback,
		s.ClientPool,
		s,
		s.quorumInfo,
		1)
	return pendingAppendOp
}

// SendAppendSuccessCallbacks will do ack sequentially
func (s *segmentHandleImpl) SendAppendSuccessCallbacks(ctx context.Context, triggerEntryId int64) {
	logger.Ctx(ctx).Debug("SendAppendSuccessCallbacks trigger", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("triggerEntryId", triggerEntryId))
	s.Lock()
	defer s.Unlock()
	s.updateAccessTime()

	// success executed one by one in sequence
	elementsToRemove := make([]*list.Element, 0)
	if s.appendOpsQueue.Len() <= 0 {
		logger.Ctx(ctx).Debug("SendAppendSuccessCallbacks with appendOps empty queue, skip", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("triggerEntryId", triggerEntryId))
		return
	}
	newLac := int64(-1)
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		op, ok := e.Value.(*AppendOp)
		if !ok {
			logger.Ctx(ctx).Debug("SendAppendSuccessCallbacks op is not append op", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.String("op", fmt.Sprintf("%v", e.Value)))
		}
		if !op.completed.Load() {
			logger.Ctx(ctx).Debug("SendAppendSuccessCallbacks not completed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
			break
		}
		// Check if it is the next entry in the sequence.
		if op.entryId == s.lastAddConfirmed.Load()+1 {
			elementsToRemove = append(elementsToRemove, e)
			// update lac
			s.lastAddConfirmed.Store(op.entryId)
			newLac = op.entryId
			// update size
			s.commitedSize.Add(int64(len(op.value)))
			s.submittedSize.Add(-int64(len(op.value)))
			// success callback
			op.FastSuccess(ctx)
		} else if op.entryId <= s.lastAddConfirmed.Load() {
			elementsToRemove = append(elementsToRemove, e)
			// update size
			s.commitedSize.Add(int64(len(op.value)))
			s.submittedSize.Add(-int64(len(op.value)))
			// success callback
			op.FastSuccess(ctx)
		} else {
			logger.Ctx(ctx).Debug("SendAppendSuccessCallbacks not the next lac", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
			break
		}
	}
	// sync lac for this round
	if newLac >= 0 {
		s.syncLAC(ctx, newLac)
	}
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
		op := element.Value.(*AppendOp)
		logger.Ctx(ctx).Debug("SendAppendSuccessCallbacks remove", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(fmt.Sprintf("%d", s.logId)).Dec()
	}

	// when seg rolling state mark, if no pending appendOps, close segment safely
	if s.rollingState.Load() && s.appendOpsQueue.Len() == 0 {
		completeAndCloseErr := s.doCompleteAndCloseUnsafe(ctx)
		if completeAndCloseErr != nil && !werr.ErrSegmentHandleSegmentClosed.Is(completeAndCloseErr) {
			logger.Ctx(ctx).Warn("completeAndCloseUnsafe failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Error(completeAndCloseErr))
		} else {
			logger.Ctx(ctx).Debug("completeAndCloseUnsafe finish", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		}
	}
}

func (s *segmentHandleImpl) findAppendOpById(ctx context.Context, triggerEntryId int64) *list.Element {
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() { // TODO should be faster by introducing a map
		element := e
		op := element.Value.(*AppendOp)
		if op.entryId == triggerEntryId {
			return element
		}
	}
	return nil
}

func (s *segmentHandleImpl) HandleAppendRequestFailure(ctx context.Context, triggerEntryId int64, err error, serverIndex int, serverAddr string) {
	s.Lock()
	defer s.Unlock()
	s.updateAccessTime()
	logger.Ctx(ctx).Info("HandleAppendRequestFailure", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("triggerEntryId", triggerEntryId), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr), zap.Error(err))

	// all after triggerEntryId will be removed
	elementsToRetry := make([]*list.Element, 0)      // retry if OP request not reach maxAttempts
	finalFailureElements := make([]*list.Element, 0) // final failure if OP request reach maxAttempts
	elementsToRemove := make([]*list.Element, 0)     // remove if OP's requests fail count reach quorum size
	minRemoveId := int64(math.MaxInt64)

	// found the triggerEntryId
	element := s.findAppendOpById(ctx, triggerEntryId)
	if element == nil {
		logger.Ctx(ctx).Info("appendOp not found in queue", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("triggerEntryId", triggerEntryId), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr), zap.Error(err))
		return
	}
	op := element.Value.(*AppendOp)
	if op.channelAttempts[serverIndex]+1 < s.cfg.Woodpecker.Client.SegmentAppend.MaxRetries &&
		(op.err == nil || op.err != nil && werr.IsRetryableErr(op.err)) &&
		(err == nil || !werr.IsSegmentWritable(err)) {
		logger.Ctx(ctx).Debug("appendOp should retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId), zap.Int("attempt", op.channelAttempts[serverIndex]), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr), zap.Error(err))
		op.channelAttempts[serverIndex]++
		elementsToRetry = append(elementsToRetry, element)
	} else {
		// retry max times, or encounter non-retryable error
		finalFailureElements = append(finalFailureElements, element)
		logger.Ctx(ctx).Debug("appendOp fail after retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId), zap.Int("attempt", op.channelAttempts[serverIndex]), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr), zap.Error(err))
		failCount := op.finalFailureSet.SetAndCount(serverIndex)
		if int32(failCount) >= (s.quorumInfo.Wq - s.quorumInfo.Aq + 1) {
			logger.Ctx(ctx).Debug("appendOp should remove", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId), zap.Int("attempt", op.channelAttempts[serverIndex]), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr), zap.Error(err))
			elementsToRemove = append(elementsToRemove, element)
			if minRemoveId == math.MaxInt64 || op.entryId < minRemoveId {
				minRemoveId = op.entryId
			}
		}
	}

	// do not remove, just resubmit to retry again
	for _, element := range elementsToRetry {
		op := element.Value.(*AppendOp)
		s.executor.Submit(ctx, NewAppendRequestRetryOp(ctx, serverIndex, op))
		logger.Ctx(ctx).Debug("append retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr))
	}

	// fast fail other queue appends which Id greater then minRemoveId.
	// because without the hole entry, their will never success
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		element := e
		op := element.Value.(*AppendOp)
		if op.entryId > minRemoveId {
			logger.Ctx(ctx).Debug(fmt.Sprintf("append entry:%d fast fail, cause entry:%d already failed", op.entryId, minRemoveId), zap.Int64("triggerId", triggerEntryId))
			elementsToRemove = append(elementsToRemove, element)
		}
	}

	// send error callback to all elementsToRemove
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
		op := element.Value.(*AppendOp)
		op.FastFail(ctx, err)
		logger.Ctx(ctx).Debug("append fail after retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(fmt.Sprintf("%d", s.logId)).Dec()
	}

	// mark rolling to prevent later append, and trigger rolling segment before all appendOps in queue
	if len(finalFailureElements) > 0 {
		// trigger rolling segment, mark rolling
		logger.Ctx(ctx).Info("rolling segment", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("triggerId", triggerEntryId), zap.Int("serverIndex", serverIndex), zap.String("serverAddr", serverAddr), zap.Error(err))
		s.rollingState.Store(true)
	}

	// when seg rolling state mark, if no pending appendOps, close segment safely
	if s.rollingState.Load() && s.appendOpsQueue.Len() == 0 {
		completeAndCloseErr := s.doCompleteAndCloseUnsafe(ctx)
		if completeAndCloseErr != nil && !werr.ErrSegmentHandleSegmentClosed.Is(completeAndCloseErr) {
			logger.Ctx(ctx).Warn("rolling completeAndCloseUnsafe failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Error(completeAndCloseErr))
		} else {
			logger.Ctx(ctx).Debug("rolling completeAndCloseUnsafe finish", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		}
	}
}

// SendAppendErrorCallbacks will do error callback and remove from pendingAppendOps sequentially
// Deprecated, use detail failure handle methods
func (s *segmentHandleImpl) SendAppendErrorCallbacks(ctx context.Context, triggerEntryId int64, err error) {
	s.Lock()
	defer s.Unlock()
	s.updateAccessTime()
	logger.Ctx(ctx).Info("SendAppendFailedCallbacks", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("triggerEntryId", triggerEntryId), zap.Error(err))

	// all after triggerEntryId will be removed
	elementsToRemove := make([]*list.Element, 0)
	elementsToRetry := make([]*list.Element, 0)
	minRemoveId := int64(math.MaxInt64)
	// check exists in queue currently, avoid delay ack
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() { // TODO should be faster
		element := e
		op := element.Value.(*AppendOp)
		if op.entryId != triggerEntryId {
			continue
		}

		// found the triggerEntryId
		if op.attempt < s.cfg.Woodpecker.Client.SegmentAppend.MaxRetries &&
			(op.err == nil || op.err != nil && werr.IsRetryableErr(op.err)) &&
			(err == nil || !werr.ErrSegmentHandleSegmentClosed.Is(err) && !werr.ErrSegmentFenced.Is(err)) {
			logger.Ctx(ctx).Debug("appendOp should retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId), zap.Int("attempt", op.attempt), zap.Error(err))
			op.attempt++
			elementsToRetry = append(elementsToRetry, element)
		} else {
			// retry max times, or encounter non-retryable error
			logger.Ctx(ctx).Debug("appendOp should remove", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId), zap.Int("attempt", op.attempt), zap.Error(err))
			elementsToRemove = append(elementsToRemove, element)
			if minRemoveId == math.MaxInt64 || op.entryId < minRemoveId {
				minRemoveId = op.entryId
			}
		}
		break
	}

	// do not remove, just resubmit to retry again
	for _, element := range elementsToRetry {
		op := element.Value.(*AppendOp)
		if op.entryId == triggerEntryId {
			s.executor.Submit(ctx, op)
			logger.Ctx(ctx).Debug("append retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", triggerEntryId), zap.Int64("triggerId", triggerEntryId))
			continue
		}
		if op.entryId < minRemoveId {
			s.executor.Submit(ctx, op)
			logger.Ctx(ctx).Debug("append retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", triggerEntryId), zap.Int64("triggerId", triggerEntryId))
		} else {
			logger.Ctx(ctx).Debug(fmt.Sprintf("append entry:%d fast fail, cause entry:%d already failed", op.entryId, minRemoveId), zap.Int64("triggerId", triggerEntryId))
			elementsToRemove = append(elementsToRemove, element)
		}
	}

	// fast fail other queue appends which Id greater then minRemoveId.
	// because without the hole entry, their will never success
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		element := e
		op := element.Value.(*AppendOp)
		if op.entryId > minRemoveId {
			logger.Ctx(ctx).Debug(fmt.Sprintf("append entry:%d fast fail, cause entry:%d already failed", op.entryId, minRemoveId), zap.Int64("triggerId", triggerEntryId))
			elementsToRemove = append(elementsToRemove, element)
		}
	}
	// send error callback to all elementsToRemove
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
		op := element.Value.(*AppendOp)
		op.FastFail(ctx, err)
		logger.Ctx(ctx).Debug("append fail after retry", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(fmt.Sprintf("%d", s.logId)).Dec()
	}

	// mark rolling to prevent later append, and trigger rolling segment before all appendOps in queue
	if len(elementsToRemove) > 0 && minRemoveId < int64(math.MaxInt64) {
		// trigger rolling segment, mark rolling
		s.rollingState.Store(true)
	}

	// when seg rolling state mark, if no pending appendOps, close segment safely
	if s.rollingState.Load() && s.appendOpsQueue.Len() == 0 {
		completeAndCloseErr := s.doCompleteAndCloseUnsafe(ctx)
		if completeAndCloseErr != nil && !werr.ErrSegmentHandleSegmentClosed.Is(completeAndCloseErr) {
			logger.Ctx(ctx).Warn("completeAndCloseUnsafe failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Error(completeAndCloseErr))
		} else {
			logger.Ctx(ctx).Debug("completeAndCloseUnsafe finish", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		}
	}
}

func (s *segmentHandleImpl) ReadBatchAdv(ctx context.Context, from int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "ReadBatch")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("start read batch", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("from", from), zap.Int64("maxEntries", maxEntries))
	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return nil, err
	}

	// quorum read with intelligent node selection
	// Try to continue from the last successful node first, then try others
	nodeCount := len(quorumInfo.Nodes)
	var lastError error

	// Find the starting node index from lastReadState
	var startNodeIndex int = 0
	if lastReadState != nil && lastReadState.Node != "" {
		for i, node := range quorumInfo.Nodes {
			if node == lastReadState.Node {
				startNodeIndex = i
				break
			}
		}
		logger.Ctx(ctx).Debug("prioritizing last read node",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.String("lastReadNode", lastReadState.Node),
			zap.Int("startNodeIndex", startNodeIndex))
	}

	// Cycle through nodes starting from the preferred index
	for i := 0; i < nodeCount; i++ {
		nodeIndex := (startNodeIndex + i) % nodeCount
		node := quorumInfo.Nodes[nodeIndex]
		logger.Ctx(ctx).Debug("attempting to read from node",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.String("node", node),
			zap.Int("nodeIndex", nodeIndex),
			zap.Int("attempt", i+1),
			zap.Bool("isLastReadNode", i == 0 && lastReadState != nil && lastReadState.Node == node))

		cli, err := s.ClientPool.GetLogStoreClient(ctx, node)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to get client for node, trying next",
				zap.String("logName", s.logName),
				zap.Int64("logId", s.logId),
				zap.Int64("segId", s.segmentId),
				zap.String("node", node),
				zap.Error(err))
			lastError = err
			continue
		}

		// Only pass lastReadState if it's from the same node
		var nodeLastReadState *proto.LastReadState
		if lastReadState != nil && lastReadState.Node == node {
			nodeLastReadState = lastReadState
		}

		batchResult, err := cli.ReadEntriesBatchAdv(ctx, s.logId, s.segmentId, from, maxEntries, nodeLastReadState)
		if err != nil {
			// For other errors, return immediately
			logger.Ctx(ctx).Warn("read batch failed on node",
				zap.String("logName", s.logName),
				zap.Int64("logId", s.logId),
				zap.Int64("segId", s.segmentId),
				zap.String("node", node),
				zap.Error(err))
			lastError = err
			if werr.ErrFileReaderEndOfFile.Is(err) {
				// encounter EOF, stop reading immediately
				break
			}
			continue
		}

		// Success! Update the LastReadState with current node and return the result
		if batchResult.LastReadState != nil {
			batchResult.LastReadState.Node = node
		}

		logger.Ctx(ctx).Debug("finish read batch successfully",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.Int64("from", from),
			zap.Int64("maxEntries", maxEntries),
			zap.String("successfulNode", node),
			zap.Int("attempt", i+1),
			zap.Int("count", len(batchResult.Entries)))
		return batchResult, nil
	}

	// All nodes failed
	logger.Ctx(ctx).Warn("read batch failed on all quorum nodes",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segmentId),
		zap.Int("totalNodes", nodeCount),
		zap.Error(lastError))

	if lastError != nil {
		return nil, lastError
	}
	return nil, werr.ErrFileReaderNoBlockFound.WithCauseErrMsg("all quorum nodes failed to read batch")
}

// GetLastAddConfirmed call by reader
func (s *segmentHandleImpl) GetLastAddConfirmed(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "GetLastAddConfirmed")
	defer sp.End()
	s.updateAccessTime()
	currentSegmentMeta := s.segmentMetaCache.Load()
	// should get from meta if seg completed, other wise get from data
	if currentSegmentMeta.Metadata.State != proto.SegmentState_Active {
		return s.lastAddConfirmed.Load(), nil
	}

	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return -1, err
	}

	// TODO use one of strategies for read，default use quorum read
	//if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
	//	return -1, werr.ErrOperationNotSupported.WithCauseErrMsg("Currently only support embed standalone mode")
	//}
	cli, err := s.ClientPool.GetLogStoreClient(ctx, quorumInfo.Nodes[0]) // TODO currently read first node directly, then read from other nodes?
	if err != nil {
		return -1, err
	}

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	lac, err := cli.GetLastAddConfirmed(ctx, s.logId, currentSegmentMeta.Metadata.SegNo)
	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "get_lac", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "get_lac", "success").Observe(float64(time.Since(start).Milliseconds()))
	return lac, err
}

// Deprecated
func (s *segmentHandleImpl) GetLastAddPushed(ctx context.Context) (int64, error) {
	return s.lastPushed.Load(), nil
}

func (s *segmentHandleImpl) GetMetadata(ctx context.Context) *meta.SegmentMeta {
	s.RLock()
	defer s.RUnlock()
	return s.segmentMetaCache.Load()
}

func (s *segmentHandleImpl) RefreshAndGetMetadata(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	return s.refreshAndGetMetadataUnsafe(ctx)
}

func (s *segmentHandleImpl) refreshAndGetMetadataUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "RefreshAndGetMetadata")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	newMeta, err := s.metadata.GetSegmentMetadata(ctx, s.logName, s.segmentId)
	if err != nil {
		logger.Ctx(ctx).Warn("refresh segment meta failed",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.Error(err))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "refresh_meta", "success").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "refresh_meta", "success").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}
	s.segmentMetaCache.Store(newMeta)
	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "refresh_meta", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "refresh_meta", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (s *segmentHandleImpl) GetQuorumInfo(ctx context.Context) (*proto.QuorumInfo, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "GetQuorumInfo")
	defer sp.End()
	s.updateAccessTime()
	if s.quorumInfo != nil {
		return s.quorumInfo, nil
	}
	quorumId := s.segmentMetaCache.Load().Metadata.GetQuorumId()
	if quorumId <= 0 {
		s.quorumInfo = &proto.QuorumInfo{
			Id: 0,
			Es: 1,
			Wq: 1,
			Aq: 1,
			Nodes: []string{
				"127.0.0.1",
			},
		}
		return s.quorumInfo, nil
	}
	return s.metadata.GetQuorumInfo(ctx, quorumId)
}

func (s *segmentHandleImpl) IsWritable(ctx context.Context) (bool, error) {
	return s.canWriteState.Load(), nil
}

func (s *segmentHandleImpl) ForceCompleteAndClose(ctx context.Context) error {
	// complete this segment and close
	s.Lock()
	defer s.Unlock()
	if s.canWriteState.Load() == false {
		// no need to complete this readonly segment
		return nil
	}
	return s.doCompleteAndCloseUnsafe(ctx)
}

func (s *segmentHandleImpl) doCloseWritingAndUpdateMetaIfNecessaryUnsafe(ctx context.Context, lastFlushedEntryId int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Close")
	defer sp.End()
	s.updateAccessTime()
	if !s.canWriteState.CompareAndSwap(true, false) {
		return nil
	}

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	// fast fail all pending append operations
	s.fastFailAppendOpsUnsafe(ctx, lastFlushedEntryId, werr.ErrSegmentHandleSegmentClosed)

	// shutdown segment executor
	s.executor.Stop(ctx)

	// update metadata as completed
	currentSegmentMeta := s.segmentMetaCache.Load()
	if currentSegmentMeta.Metadata.State != proto.SegmentState_Active {
		// not active writable segmentHandle, just return
		return nil
	}

	if lastFlushedEntryId == -1 {
		// Segment metadata is updated immediately with an exact last flushed entry ID; otherwise, it is updated asynchronously by the auditor.
		return nil
	}

	newSegmentMetadata := currentSegmentMeta.Metadata.CloneVT()
	newSegmentMetadata.State = proto.SegmentState_Completed
	newSegmentMetadata.Size = s.commitedSize.Load() // Update with approximate segment file size, only use for metrics
	newSegmentMetadata.LastEntryId = lastFlushedEntryId
	newSegmentMetadata.CompletionTime = time.Now().UnixMilli()
	newSegMeta := &meta.SegmentMeta{
		Metadata: newSegmentMetadata,
		Revision: currentSegmentMeta.Revision,
	}
	err := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegMeta)
	if err != nil && werr.ErrMetadataRevisionInvalid.Is(err) {
		// metadata revision is invalid or outdated, some one updated it, trigger fence this segmentHandle to let client reopen new writer
		// new append will fail with ErrSegmentFenced, and application client should reopen new logWriter instead.
		s.NotifyWriterInvalidation(ctx, fmt.Sprintf("segment:%d meta update revision invalid", s.segmentId))
		logger.Ctx(ctx).Warn("segment close failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastFlushedEntryId", lastFlushedEntryId), zap.Int64("lastAddConfirmed", s.lastAddConfirmed.Load()), zap.Int64("completionTime", newSegmentMetadata.CompletionTime), zap.Error(err))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "close", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "close", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	if err != nil {
		logger.Ctx(ctx).Warn("segment close failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastFlushedEntryId", lastFlushedEntryId), zap.Int64("lastAddConfirmed", s.lastAddConfirmed.Load()), zap.Int64("completionTime", newSegmentMetadata.CompletionTime), zap.Error(err))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "close", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "close", "error").Observe(float64(time.Since(start).Milliseconds()))
	} else {
		logger.Ctx(ctx).Debug("segment closed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastFlushedEntryId", lastFlushedEntryId), zap.Int64("lastAddConfirmed", s.lastAddConfirmed.Load()), zap.Int64("completionTime", newSegmentMetadata.CompletionTime))
		s.segmentMetaCache.Store(newSegMeta)
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "close", "success").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "close", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return err
}

func (s *segmentHandleImpl) doCompleteAndCloseUnsafe(ctx context.Context) error {
	var lastError error
	// complete this segment and close
	lastFlushedEntryId, err := s.doCompleteUnsafe(ctx)
	if err != nil {
		logger.Ctx(ctx).Info("Complete segment failed when closing logHandle",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.Int64("lastFlushedEntryId", lastFlushedEntryId),
			zap.Error(err))
		lastError = err
		s.NotifyWriterInvalidation(ctx, fmt.Sprintf("segment:%d complete failed", s.segmentId))
	}
	err = s.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(ctx, lastFlushedEntryId)
	if err != nil {
		logger.Ctx(ctx).Info("close segment failed when closing logHandle",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.Int64("lastFlushedEntryId", lastFlushedEntryId),
			zap.Error(err))
		if lastError == nil {
			lastError = werr.Combine(err, lastError)
		}
	}

	// mark segment as readonly
	if lastError == nil {
		s.canWriteState.Store(false)
	}
	return lastError
}

func (s *segmentHandleImpl) fastFailAppendOpsUnsafe(ctx context.Context, lastEntryId int64, err error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "fastFailAppendOpsUnsafe")
	defer sp.End()
	logger.Ctx(ctx).Debug("fastFailAppendOps start", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Error(err))
	// check exists in queue currently, avoid delay ack
	elementsToRemove := make([]*list.Element, 0)
	failCount := 0
	successCount := 0
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		element := e
		op := element.Value.(*AppendOp)
		if lastEntryId == -1 {
			// fast fail all
			op.FastFail(ctx, err)
			failCount += 1
		} else if op.entryId <= lastEntryId {
			// fast success flushed appendOp
			op.FastSuccess(ctx)
			successCount += 1
		} else {
			// fast fail unflushed appendOp
			op.FastFail(ctx, err)
			failCount += 1
		}
		elementsToRemove = append(elementsToRemove, element)
	}
	// Clear the queue
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(fmt.Sprintf("%d", s.logId)).Dec()
	}
	logger.Ctx(ctx).Debug("fastFailAppendOps finish", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int("fastFailOps", len(elementsToRemove)), zap.Int("successCount", successCount), zap.Int("failCount", failCount), zap.Error(err))
}

// GetSize returns the size of the segment
func (s *segmentHandleImpl) GetSize(ctx context.Context) int64 {
	return s.submittedSize.Load() + s.commitedSize.Load()
}

// GetBlocksCount returns the number of blocks in the segment
// If an error occurs, return 0 to indicate that blocks count should not be used as a basis for rolling
// TODO If in the future it is in remote mode, frequent access is not necessary, only once every 5-10 seconds is ok
func (s *segmentHandleImpl) GetBlocksCount(ctx context.Context) int64 {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "GetLastAddConfirmed")
	defer sp.End()
	s.updateAccessTime()
	currentSegmentMeta := s.segmentMetaCache.Load()
	// should get from meta if seg completed, other wise get from data
	if currentSegmentMeta.Metadata.State != proto.SegmentState_Active {
		// quick return, this method is only used during the data writing process of active segments, otherwise the call is meaningless
		return 0
	}

	if len(s.quorumInfo.Nodes) > 1 {
		// In service mode, writes go to local disk and are not limited by block count,
		// rolling is only controlled by size and time constraints
		return 0
	}

	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get quorum info during get blocks count, return 0",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return 0
	}

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		logger.Ctx(ctx).Warn("Unsupported quorum configuration during get blocks count, return 0",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("quorumId", quorumInfo.Id),
			zap.Int("nodeCount", len(quorumInfo.Nodes)))
		return 0
	}

	cli, err := s.ClientPool.GetLogStoreClient(ctx, quorumInfo.Nodes[0])
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get blocks count",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return 0
	}

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	currentBlockCounts, err := cli.GetBlockCount(ctx, s.logId, currentSegmentMeta.Metadata.SegNo)
	if err != nil {
		logger.Ctx(ctx).Info("Failed to get blocks count",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return 0
	}
	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "get_blocks_count", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "get_blocks_count", "success").Observe(float64(time.Since(start).Milliseconds()))
	return currentBlockCounts
}

// Complete segment
// Deprecated
func (s *segmentHandleImpl) Complete(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Complete")
	defer sp.End()
	s.Lock()
	defer s.Unlock()
	return s.doCompleteUnsafe(ctx)
}

func (s *segmentHandleImpl) doCompleteUnsafe(ctx context.Context) (int64, error) {
	// Get quorum information for complete operation
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get quorum info during doComplete",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return -1, err
	}

	// TODO maybe not use fence? use mark completed? 是否有必要和fence区分，不然正常close和fence可能会出现并发行为，不知道谁的优先级更高？
	lastAddConfirmed, fencedErr := s.fenceSegmentQuorum(ctx, quorumInfo)
	if fencedErr != nil {
		logger.Ctx(ctx).Error("quorum fence segment failed",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(fencedErr))
		return -1, fencedErr
	}

	logger.Ctx(ctx).Info("Starting quorum complete segment operation",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("quorumId", quorumInfo.Id),
		zap.Int("nodeCount", len(quorumInfo.Nodes)),
		zap.Int32("ackQuorum", quorumInfo.Aq))

	// Send CompleteSegment requests to all quorum nodes in parallel
	return lastAddConfirmed, s.completeSegmentQuorum(ctx, quorumInfo, lastAddConfirmed)
}

// completeSegmentQuorum sends CompleteSegment requests to all quorum nodes
// and calculates the Log All Committed (LAC) based on majority consensus
func (s *segmentHandleImpl) completeSegmentQuorum(ctx context.Context, quorumInfo *proto.QuorumInfo, lac int64) error {
	nodeCount := len(quorumInfo.Nodes)
	ackQuorum := int(quorumInfo.Aq)

	// Channel to collect results from all nodes
	resultChan := make(chan nodeCompleteResult, nodeCount)

	// Send requests to all nodes in parallel
	for _, node := range quorumInfo.Nodes {
		go s.completeSegmentOnNode(ctx, node, lac, resultChan)
	}

	// Collect results from nodes
	var successResults []int64
	var lastError error

	for i := 0; i < nodeCount; i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				logger.Ctx(ctx).Warn("Failed to complete segment on node",
					zap.String("logName", s.logName),
					zap.Int64("logId", s.logId),
					zap.Int64("segmentId", s.segmentId),
					zap.String("node", result.node),
					zap.Error(result.err))
				lastError = result.err
			} else {
				successResults = append(successResults, result.lastEntryId)
				logger.Ctx(ctx).Info("Successfully completed segment on node",
					zap.String("logName", s.logName),
					zap.Int64("logId", s.logId),
					zap.Int64("segmentId", s.segmentId),
					zap.String("node", result.node),
					zap.Int64("lastEntryId", result.lastEntryId))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Check if we have enough successful responses for ack quorum
	if len(successResults) < ackQuorum {
		logger.Ctx(ctx).Error("Insufficient successful responses for quorum complete",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int("successCount", len(successResults)),
			zap.Int("requiredAckQuorum", ackQuorum))
		if lastError != nil {
			return lastError
		}
		return werr.ErrAppendOpQuorumFailed.WithCauseErrMsg("insufficient successful complete responses")
	}

	// Calculate LAC (Log All Committed) from successful results
	completedLAC := s.calculateLAC(successResults, ackQuorum)
	logger.Ctx(ctx).Info("Calculated LAC from quorum complete responses",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("lac", lac),
		zap.Int("successCount", len(successResults)),
		zap.Int("ackQuorum", ackQuorum),
		zap.Int64s("allResults", successResults),
		zap.Int64("completedLAC", completedLAC))
	s.lastAddConfirmed.Store(lac)
	return nil
}

func (s *segmentHandleImpl) syncLAC(ctx context.Context, lac int64) {
	// Asynchronously sync LAC updates to all quorum nodes
	go s.syncLACToQuorumAsync(ctx, lac)
}

// syncLACToQuorumAsync asynchronously syncs LAC to all quorum nodes
func (s *segmentHandleImpl) syncLACToQuorumAsync(ctx context.Context, lac int64) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "syncLACToQuorumAsync")
	defer sp.End()

	// Get quorum info
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get quorum info for LAC sync",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("lac", lac),
			zap.Error(err))
		return
	}

	// Create result channel to collect sync results
	resultChan := make(chan nodeLACSyncResult, len(quorumInfo.Nodes))

	// Send LAC sync requests to all quorum nodes in parallel
	for _, node := range quorumInfo.Nodes {
		go s.syncLACToNode(ctx, node, lac, resultChan)
	}

	// Collect results (fire-and-forget style, just log results)
	successCount := 0
	totalNodes := len(quorumInfo.Nodes)

	for i := 0; i < totalNodes; i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				logger.Ctx(ctx).Warn("Failed to sync LAC to node",
					zap.String("logName", s.logName),
					zap.Int64("logId", s.logId),
					zap.Int64("segmentId", s.segmentId),
					zap.String("node", result.node),
					zap.Int64("lac", lac),
					zap.Error(result.err))
			} else {
				successCount++
				logger.Ctx(ctx).Debug("Successfully synced LAC to node",
					zap.String("logName", s.logName),
					zap.Int64("logId", s.logId),
					zap.Int64("segmentId", s.segmentId),
					zap.String("node", result.node),
					zap.Int64("lac", lac))
			}
		case <-ctx.Done():
			logger.Ctx(ctx).Warn("LAC sync context cancelled",
				zap.String("logName", s.logName),
				zap.Int64("logId", s.logId),
				zap.Int64("segmentId", s.segmentId),
				zap.Int64("lac", lac),
				zap.Error(ctx.Err()))
			return
		}
	}

	logger.Ctx(ctx).Debug("LAC sync to quorum completed",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("lac", lac),
		zap.Int("successCount", successCount),
		zap.Int("totalNodes", totalNodes))
}

// nodeLACSyncResult represents the result of LAC sync operation on a single node
type nodeLACSyncResult struct {
	node string
	err  error
}

// syncLACToNode sends UpdateLastAddConfirmed request to a single node
func (s *segmentHandleImpl) syncLACToNode(ctx context.Context, node string, lac int64, resultChan chan<- nodeLACSyncResult) {
	cli, err := s.ClientPool.GetLogStoreClient(ctx, node)
	if err != nil {
		resultChan <- nodeLACSyncResult{
			node: node,
			err:  err,
		}
		return
	}

	err = cli.UpdateLastAddConfirmed(ctx, s.logId, s.segmentId, lac)
	resultChan <- nodeLACSyncResult{
		node: node,
		err:  err,
	}
}

// nodeCompleteResult represents the result of CompleteSegment operation on a single node
type nodeCompleteResult struct {
	node        string
	lastEntryId int64
	err         error
}

// completeSegmentOnNode sends CompleteSegment request to a single node
func (s *segmentHandleImpl) completeSegmentOnNode(ctx context.Context, node string, lac int64, resultChan chan<- nodeCompleteResult) {
	cli, err := s.ClientPool.GetLogStoreClient(ctx, node)
	if err != nil {
		resultChan <- nodeCompleteResult{
			node: node,
			err:  err,
		}
		return
	}

	lastEntryId, err := cli.CompleteSegment(ctx, s.logId, s.segmentId, lac)
	resultChan <- nodeCompleteResult{
		node:        node,
		lastEntryId: lastEntryId,
		err:         err,
	}
}

// calculateLAC calculates the Log All Committed (LAC) based on the quorum responses.
// LAC is the highest entry ID that is guaranteed to be committed on a majority of nodes.
// For example: if nodes return [4, 6, 7] and ackQuorum is 2, then LAC is 6
// because entries 1-6 are committed on at least 2 nodes (majority).
func (s *segmentHandleImpl) calculateLAC(results []int64, ackQuorum int) int64 {
	if len(results) == 0 {
		return -1
	}

	// Sort results in ascending order
	sortedResults := make([]int64, len(results))
	copy(sortedResults, results)
	sort.Slice(sortedResults, func(i, j int) bool {
		return sortedResults[i] < sortedResults[j]
	})

	// LAC is the (len(results) - ackQuorum + 1)th smallest value
	// This ensures that at least ackQuorum nodes have committed up to this entry
	lacIndex := len(sortedResults) - ackQuorum
	if lacIndex < 0 {
		lacIndex = 0
	}

	return sortedResults[lacIndex]
}

// fenceSegmentQuorum sends FenceSegment requests to all quorum nodes
// and calculates the Log All Committed (LAC) based on majority consensus
func (s *segmentHandleImpl) fenceSegmentQuorum(ctx context.Context, quorumInfo *proto.QuorumInfo) (int64, error) {
	nodeCount := len(quorumInfo.Nodes)                         // always es=len(nodes)
	ensembleCoverage := int(quorumInfo.Es - quorumInfo.Aq + 1) // currently always es=3=wq,aq=2, so ec=es-aq+1=2=aq

	// Channel to collect results from all nodes
	resultChan := make(chan nodeFenceResult, nodeCount)

	// Send requests to all nodes in parallel
	for _, node := range quorumInfo.Nodes {
		go s.fenceSegmentOnNode(ctx, node, resultChan)
	}

	// Collect results from nodes
	var successResults []int64
	var lastError error

	for i := 0; i < nodeCount; i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				logger.Ctx(ctx).Warn("Failed to fence segment on node",
					zap.String("logName", s.logName),
					zap.Int64("logId", s.logId),
					zap.Int64("segmentId", s.segmentId),
					zap.String("node", result.node),
					zap.Error(result.err))
				lastError = result.err
			} else {
				successResults = append(successResults, result.lastEntryId)
				logger.Ctx(ctx).Info("Successfully fenced segment on node",
					zap.String("logName", s.logName),
					zap.Int64("logId", s.logId),
					zap.Int64("segmentId", s.segmentId),
					zap.String("node", result.node),
					zap.Int64("lastEntryId", result.lastEntryId))
			}
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}

	// Check if we have enough successful responses for ack quorum
	if len(successResults) < ensembleCoverage {
		logger.Ctx(ctx).Error("Insufficient successful responses for quorum fence",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int("successCount", len(successResults)),
			zap.Int("requiredEC", ensembleCoverage))
		if lastError != nil {
			return -1, lastError
		}
		return -1, werr.ErrAppendOpQuorumFailed.WithCauseErrMsg("insufficient successful fence responses")
	}

	// Calculate LAC (Log All Committed) from successful results
	lac := s.calculateLAC(successResults, ensembleCoverage)

	logger.Ctx(ctx).Info("Calculated LAC from quorum fence responses",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("lac", lac),
		zap.Int("successCount", len(successResults)),
		zap.Int("ensembleCoverage", ensembleCoverage),
		zap.Int64s("allResults", successResults))

	return lac, nil
}

// nodeFenceResult represents the result of FenceSegment operation on a single node
type nodeFenceResult struct {
	node        string
	lastEntryId int64
	err         error
}

// fenceSegmentOnNode sends FenceSegment request to a single node
func (s *segmentHandleImpl) fenceSegmentOnNode(ctx context.Context, node string, resultChan chan<- nodeFenceResult) {
	cli, err := s.ClientPool.GetLogStoreClient(ctx, node)
	if err != nil {
		resultChan <- nodeFenceResult{
			node: node,
			err:  err,
		}
		return
	}

	lastEntryId, err := cli.FenceSegment(ctx, s.logId, s.segmentId)
	resultChan <- nodeFenceResult{
		node:        node,
		lastEntryId: lastEntryId,
		err:         err,
	}
}

// compactSegmentQuorum tries SegmentCompact on each quorum node sequentially
// until one succeeds. Returns the result from the first successful node.
func (s *segmentHandleImpl) compactSegmentQuorum(ctx context.Context, quorumInfo *proto.QuorumInfo) (*proto.SegmentMetadata, error) {
	var lastError error

	// Try each node sequentially until one succeeds
	for i, node := range quorumInfo.Nodes {
		logger.Ctx(ctx).Info("Attempting compaction on node",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("node", node),
			zap.Int("nodeIndex", i+1),
			zap.Int("totalNodes", len(quorumInfo.Nodes)))

		cli, err := s.ClientPool.GetLogStoreClient(ctx, node)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to get logstore client for compaction",
				zap.String("logName", s.logName),
				zap.Int64("logId", s.logId),
				zap.Int64("segmentId", s.segmentId),
				zap.String("node", node),
				zap.Error(err))
			lastError = err
			continue
		}

		// Try compaction on this node
		compactSegMetaInfo, compactErr := cli.SegmentCompact(ctx, s.logId, s.segmentId)
		if compactErr != nil {
			logger.Ctx(ctx).Warn("Segment compaction failed on node, trying next",
				zap.String("logName", s.logName),
				zap.Int64("logId", s.logId),
				zap.Int64("segmentId", s.segmentId),
				zap.String("node", node),
				zap.Int("nodeIndex", i+1),
				zap.Int("totalNodes", len(quorumInfo.Nodes)),
				zap.Error(compactErr))
			lastError = compactErr
			continue
		}

		// Success! Log and return the result
		logger.Ctx(ctx).Info("Segment compaction succeeded on node",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("successfulNode", node),
			zap.Int("nodeIndex", i+1),
			zap.Int("totalNodes", len(quorumInfo.Nodes)),
			zap.Int64("compactedSize", compactSegMetaInfo.Size),
			zap.Int64("lastEntryId", compactSegMetaInfo.LastEntryId))

		return compactSegMetaInfo, nil
	}

	// All nodes failed
	logger.Ctx(ctx).Error("Compaction failed on all quorum nodes",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int("totalNodes", len(quorumInfo.Nodes)))

	if lastError != nil {
		return nil, lastError
	}
	return nil, werr.ErrSegmentHandleCompactionFailed.WithCauseErrMsg("all quorum nodes failed compaction")
}

func (s *segmentHandleImpl) FenceAndComplete(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Fence")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Info("Starting segment fence operation",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId))

	// Acquire lock to ensure mutual exclusion with AppendAsync
	s.Lock()
	defer s.Unlock()

	// check cached fenced flag to return fast
	if s.fencedState.Load() {
		logger.Ctx(ctx).Info("Segment already fenced, returning cached result",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("lastAddConfirmed", s.lastAddConfirmed.Load()))
		return s.lastAddConfirmed.Load(), nil
	}

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)

	// fence segment, prevent new append operations
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		// Revert fenced state on error
		logger.Ctx(ctx).Warn("Failed to get quorum info during fence, reverting fenced state",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return -1, err
	}
	logger.Ctx(ctx).Info("Starting quorum fence segment operation",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("quorumId", quorumInfo.Id),
		zap.Int("nodeCount", len(quorumInfo.Nodes)),
		zap.Int32("ackQuorum", quorumInfo.Aq))

	// Send FenceSegment requests to all quorum nodes in parallel
	lastEntryId, fencedErr := s.fenceSegmentQuorum(ctx, quorumInfo)
	if fencedErr != nil {
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "fence", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "fence", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Error("quorum fence segment failed",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(fencedErr))
		return -1, fencedErr
	}

	completeErr := s.completeSegmentQuorum(ctx, quorumInfo, lastEntryId)
	if completeErr != nil {
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "complete", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "complete", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Error("quorum complete segment failed",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(completeErr))
		return -1, completeErr
	}

	// update segment state and meta
	currentMeta := s.segmentMetaCache.Load()
	newSegmentMetadata := currentMeta.Metadata.CloneVT()
	newSegmentMetadata.LastEntryId = lastEntryId
	newSegmentMetadata.State = proto.SegmentState_Completed
	newSegmentMetadata.CompletionTime = time.Now().UnixMilli()
	newSegMeta := &meta.SegmentMeta{
		Metadata: newSegmentMetadata,
		Revision: currentMeta.Revision,
	}
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegMeta)
	if updateMetaErr != nil {
		logger.Ctx(ctx).Warn("Failed to update segment metadata after fence",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(updateMetaErr))
		return -1, updateMetaErr
	} else {
		logger.Ctx(ctx).Info("Successfully updated segment metadata after fence",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
	}

	logger.Ctx(ctx).Info("finish fence segment to completed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastEntryId", lastEntryId))
	// update segmentHandle meta cache
	refreshErr := s.refreshAndGetMetadataUnsafe(ctx)
	if refreshErr != nil {
		logger.Ctx(ctx).Warn("Failed to refresh segment metadata cache after compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(refreshErr))
	}

	if lastEntryId != -1 && s.lastAddConfirmed.Load() < lastEntryId {
		logger.Ctx(ctx).Info("Updating last add confirmed from fence result",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("previousLastAddConfirmed", s.lastAddConfirmed.Load()),
			zap.Int64("newLastAddConfirmed", lastEntryId))
		s.lastAddConfirmed.Store(lastEntryId)
	}
	// Use the actual lastEntryId returned from FenceSegment, even in error cases
	// because these "errors" indicate the segment was already fenced, not a failure
	s.fastFailAppendOpsUnsafe(ctx, lastEntryId, werr.ErrSegmentFenced)
	logger.Ctx(ctx).Info("Segment fence completed successfully",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("lastEntryId", lastEntryId),
		zap.Duration("duration", time.Since(start)))
	s.fencedState.Store(true) // Set fenced state to prevent new append operations
	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "fence", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "fence", "success").Observe(float64(time.Since(start).Milliseconds()))
	return lastEntryId, nil
}

// Compact used for the auditor of this Log
func (s *segmentHandleImpl) Compact(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Compact")
	defer sp.End()
	s.updateAccessTime()
	if s.cfg.Woodpecker.Storage.IsStorageLocal() {
		logger.Ctx(ctx).Info("Local storage detected, skipping compaction (not yet implemented)",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		// TODO: currently local fs no need to compact due to it is already a single file.
		// If local backend support compaction in the future, it may be necessary to use a last flushed entry id for safe compaction,
		// because in local fs mode, data might be written but the flush could fail,
		// while the system might have fsync it to disk. This data is beyond the business's flush entry id.
		return nil
	}

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	logger.Ctx(ctx).Info("Starting segment compaction from Completed to Sealed",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId))
	err := s.RefreshAndGetMetadata(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to refresh segment metadata",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return err
	}
	currentSegmentMeta := s.GetMetadata(ctx)
	logger.Ctx(ctx).Info("Validating segment state for compaction",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("currentState", currentSegmentMeta.Metadata.State.String()),
		zap.Int64("currentLastEntryId", currentSegmentMeta.Metadata.LastEntryId),
		zap.Int64("currentSize", currentSegmentMeta.Metadata.Size))

	if currentSegmentMeta.Metadata.State != proto.SegmentState_Completed {
		logger.Ctx(ctx).Info("segment is not in completed state, compaction skip", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		return werr.ErrSegmentHandleSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("segment state is not in completed. logId:%s, segId:%d", s.logName, s.segmentId))
	}
	if !s.doingCompact.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("Recovery or compact operation already in progress, skipping compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		return nil
	}
	defer func() {
		s.doingCompact.Store(false)
		logger.Ctx(ctx).Info("Compaction operation completed, released operation lock",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
	}()

	logger.Ctx(ctx).Info("request compact segment from completed to sealed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get quorum info during compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return err
	}
	logger.Ctx(ctx).Info("Starting quorum compaction operation",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("quorumId", quorumInfo.Id),
		zap.Int("nodeCount", len(quorumInfo.Nodes)),
		zap.Int64("lastEntryId", currentSegmentMeta.Metadata.LastEntryId))

	// Try compaction on each node sequentially until one succeeds
	compactSegMetaInfo, compactErr := s.compactSegmentQuorum(ctx, quorumInfo)
	if compactErr != nil {
		logger.Ctx(ctx).Error("All nodes failed compaction operation",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Duration("duration", time.Since(start)),
			zap.Error(compactErr))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "compact_to_sealed", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "compact_to_sealed", "error").Observe(float64(time.Since(start).Milliseconds()))
		return compactErr
	}

	logger.Ctx(ctx).Info("Segment compaction completed, updating metadata",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("compactedSize", compactSegMetaInfo.Size),
		zap.Int64("completionTime", compactSegMetaInfo.SealedTime))

	// update segment state and meta
	newSegmentMetadata := currentSegmentMeta.Metadata.CloneVT()
	newSegmentMetadata.State = proto.SegmentState_Sealed
	newSegmentMetadata.SealedTime = compactSegMetaInfo.CompletionTime
	newSegmentMetadata.Size = compactSegMetaInfo.Size
	newSegMeta := &meta.SegmentMeta{
		Metadata: newSegmentMetadata,
		Revision: currentSegmentMeta.Revision,
	}
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegMeta)
	if updateMetaErr != nil {
		logger.Ctx(ctx).Warn("Failed to update segment metadata after compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(updateMetaErr))
	} else {
		logger.Ctx(ctx).Info("Successfully updated segment metadata after compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
	}

	logger.Ctx(ctx).Info("finish compact segment to sealed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastEntryId", compactSegMetaInfo.LastEntryId))
	// update segmentHandle meta cache
	refreshErr := s.RefreshAndGetMetadata(ctx)
	if refreshErr != nil {
		logger.Ctx(ctx).Warn("Failed to refresh segment metadata cache after compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(refreshErr))
	}

	compactionDuration := time.Since(start)
	logger.Ctx(ctx).Info("Segment compaction operation completed successfully",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Duration("totalDuration", compactionDuration),
		zap.Int64("finalLastEntryId", compactSegMetaInfo.LastEntryId),
		zap.Int64("finalSize", compactSegMetaInfo.Size),
		zap.String("finalState", "Sealed"))

	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "compact_to_sealed", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "compact_to_sealed", "success").Observe(float64(compactionDuration.Milliseconds()))
	return updateMetaErr
}

// SetRollingReady set the segment as ready for rolling
func (s *segmentHandleImpl) SetRollingReady(ctx context.Context) {
	s.Lock()
	defer s.Unlock()
	logger.Ctx(ctx).Info("setting segment to rolling_ready state", zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId), zap.Int("queueSize", s.appendOpsQueue.Len()))
	s.rollingState.Store(true)
	if s.appendOpsQueue.Len() > 0 {
		logger.Ctx(ctx).Warn("Segment is not empty, will rolling later", zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId))
		return
	}
	// trigger immediately
	err := s.doCompleteAndCloseUnsafe(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to complete segment after setting to rolling_ready state", zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId), zap.Error(err))
	}
}

// IsForceRollingReady check if the segment is ready for rolling
func (s *segmentHandleImpl) IsForceRollingReady(ctx context.Context) bool {
	s.RLock()
	defer s.RUnlock()
	return s.rollingState.Load()
}

func (s *segmentHandleImpl) updateAccessTime() {
	s.lastAccessTime.Store(time.Now().UnixMilli())
}

func (s *segmentHandleImpl) GetLastAccessTime() int64 {
	return s.lastAccessTime.Load()
}

func (s *segmentHandleImpl) SetWriterInvalidationNotifier(ctx context.Context, f func(ctx context.Context, reason string)) {
	s.expiredTrigger = f
}

func (s *segmentHandleImpl) NotifyWriterInvalidation(ctx context.Context, reason string) {
	if s.expiredTrigger != nil {
		s.expiredTrigger(ctx, reason)
	}
}
