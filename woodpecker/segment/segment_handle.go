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
	"github.com/zilliztech/woodpecker/server/processor"
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
	GetId(context.Context) int64
	// AppendAsync data to the segment asynchronously
	AppendAsync(context.Context, []byte, func(int64, int64, error))
	// ReadBatchAdv num of entries from the segment
	ReadBatchAdv(context.Context, int64, int64, *processor.LastReadState) (*processor.BatchData, error)
	// GetLastAddConfirmed entryId for the segment
	GetLastAddConfirmed(context.Context) (int64, error)
	// GetLastAddPushed entryId for the segment
	GetLastAddPushed(context.Context) (int64, error)
	// GetMetadata of the segment
	GetMetadata(context.Context) *meta.SegmentMeta
	// RefreshAndGetMetadata of the segment
	RefreshAndGetMetadata(context.Context) error
	// GetQuorumInfo of the segment if it's a active segment
	GetQuorumInfo(context.Context) (*proto.QuorumInfo, error)
	// IsWritable check if the segment is writable
	IsWritable(context.Context) (bool, error)
	// ForceCompleteAndClose the segment
	ForceCompleteAndClose(ctx context.Context) error
	// SendAppendSuccessCallbacks called when an appendOp operation is successful
	SendAppendSuccessCallbacks(context.Context, int64)
	// SendAppendErrorCallbacks called when an appendOp operation fails
	SendAppendErrorCallbacks(context.Context, int64, error)
	// GetSize get the size of the segment
	GetSize(context.Context) int64
	// Complete the segment writing
	Complete(context.Context) (int64, error)
	// Fence the segment in all nodes
	Fence(context.Context) (int64, error)
	// Compact is a recovery or compaction operation
	Compact(context.Context) error
	// SetRollingReady set the segment as ready for rolling
	SetRollingReady(context.Context)
	// IsForceRollingReady check if the segment is ready for rolling
	IsForceRollingReady(context.Context) bool
	// GetLastAccessTime get the last access time of the segment
	GetLastAccessTime() int64
	// SetWriterInvalidationNotifier set the expired trigger
	SetWriterInvalidationNotifier(context.Context, func(ctx context.Context, reason string))
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
		quorumInfo: &proto.QuorumInfo{ // TODO: get from metadata in cluster mode
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

// SendAppendErrorCallbacks will do error callback and remove from pendingAppendOps sequentially
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

func (s *segmentHandleImpl) ReadBatchAdv(ctx context.Context, from int64, maxSize int64, lastReadState *processor.LastReadState) (*processor.BatchData, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "ReadBatch")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("start read batch", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("from", from), zap.Int64("maxSize", maxSize))
	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return nil, err
	}

	// TODO support quorum read, and read state for each
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return nil, werr.ErrOperationNotSupported.WithCauseErrMsg("Currently only support embed standalone mode")
	}

	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return nil, err
	}

	batchResult, err := cli.ReadEntriesBatchAdv(ctx, s.logId, s.segmentId, from, maxSize, lastReadState)
	if err != nil {
		return nil, err
	}
	logger.Ctx(ctx).Debug("finish read batch", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("from", from), zap.Int64("maxSize", maxSize), zap.Int("count", len(batchResult.Entries)))
	return batchResult, nil
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

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return -1, werr.ErrOperationNotSupported.WithCauseErrMsg("Currently only support embed standalone mode")
	}

	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
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

func (s *segmentHandleImpl) Complete(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Complete")
	defer sp.End()
	s.Lock()
	defer s.Unlock()
	return s.doCompleteUnsafe(ctx)
}

func (s *segmentHandleImpl) doCompleteUnsafe(ctx context.Context) (int64, error) {
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
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		// Revert fenced state on error
		logger.Ctx(ctx).Warn("Unsupported quorum configuration during fence, reverting fenced state",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("quorumId", quorumInfo.Id),
			zap.Int("nodeCount", len(quorumInfo.Nodes)))
		return -1, werr.ErrOperationNotSupported.WithCauseErrMsg("currently only support embed standalone mode")
	}
	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		// Revert fenced state on error
		logger.Ctx(ctx).Warn("Failed to get logstore client during fence, reverting fenced state",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("node", quorumInfo.Nodes[0]),
			zap.Error(err))
		return -1, err
	}

	logger.Ctx(ctx).Info("Sending fence request to logstore",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("targetNode", quorumInfo.Nodes[0]))

	return cli.CompleteSegment(ctx, s.logId, s.segmentId)
}

func (s *segmentHandleImpl) Fence(ctx context.Context) (int64, error) {
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
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		// Revert fenced state on error
		logger.Ctx(ctx).Warn("Unsupported quorum configuration during fence, reverting fenced state",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("quorumId", quorumInfo.Id),
			zap.Int("nodeCount", len(quorumInfo.Nodes)))
		return -1, werr.ErrOperationNotSupported.WithCauseErrMsg("currently only support embed standalone mode")
	}
	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		// Revert fenced state on error
		logger.Ctx(ctx).Warn("Failed to get logstore client during fence, reverting fenced state",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("node", quorumInfo.Nodes[0]),
			zap.Error(err))
		return -1, err
	}

	logger.Ctx(ctx).Info("Sending fence request to logstore",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("targetNode", quorumInfo.Nodes[0]))

	// fence and get last entry
	lastEntryId, fencedErr := cli.FenceSegment(ctx, s.logId, s.segmentId)

	logger.Ctx(ctx).Info("Received fence response from logstore",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("lastEntryId", lastEntryId),
		zap.Error(fencedErr))

	if fencedErr != nil {
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "fence", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "fence", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Info("segment fence fail", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Error(fencedErr))
		return -1, fencedErr
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
	// choose on LogStore to compaction
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		logger.Ctx(ctx).Warn("Unsupported quorum configuration for compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("quorumId", quorumInfo.Id),
			zap.Int("nodeCount", len(quorumInfo.Nodes)))
		return werr.ErrOperationNotSupported.WithCauseErrMsg("currently only support embed standalone mode")
	}
	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get logstore client for compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("targetNode", quorumInfo.Nodes[0]),
			zap.Error(err))
		return err
	}
	// wait compaction success
	logger.Ctx(ctx).Info("request compact segment from completed to sealed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastEntryId", currentSegmentMeta.Metadata.LastEntryId))
	compactSegMetaInfo, compactErr := cli.SegmentCompact(ctx, s.logId, s.segmentId)
	if compactErr != nil {
		logger.Ctx(ctx).Warn("Segment compaction operation failed",
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
