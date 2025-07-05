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
	// ReadBatch num of entries from the segment
	ReadBatch(context.Context, int64, int64) ([]*processor.SegmentEntry, error)
	// GetLastAddConfirmed entryId for the segment
	GetLastAddConfirmed(context.Context) (int64, error)
	// GetLastAddPushed entryId for the segment
	GetLastAddPushed(context.Context) (int64, error)
	// GetMetadata of the segment
	GetMetadata(context.Context) *proto.SegmentMetadata
	// RefreshAndGetMetadata of the segment
	RefreshAndGetMetadata(context.Context) error
	// GetQuorumInfo of the segment if it's a active segment
	GetQuorumInfo(context.Context) (*proto.QuorumInfo, error)
	// IsWritable check if the segment is writable
	IsWritable(context.Context) (bool, error)
	// CloseWritingAndUpdateMetaIfNecessary the segment using the last known flushed entryId, if available
	CloseWritingAndUpdateMetaIfNecessary(context.Context, int64) error
	// SendAppendSuccessCallbacks called when an appendOp operation is successful
	SendAppendSuccessCallbacks(context.Context, int64)
	// SendAppendErrorCallbacks called when an appendOp operation fails
	SendAppendErrorCallbacks(context.Context, int64, error)
	// GetSize get the size of the segment
	GetSize(context.Context) int64
	// RequestCompactionAsync request compaction for the segment asynchronously
	RequestCompactionAsync(context.Context) error
	// Complete the segment writing
	Complete(context.Context) (int64, error)
	// Fence the segment in all nodes
	Fence(context.Context) (int64, error)
	// IsFence check if the segment is fenced
	IsFence(context.Context) (bool, error)
	// RecoveryOrCompact is a recovery or compaction operation
	RecoveryOrCompact(context.Context) error
	// SetRollingReady set the segment as ready for rolling
	SetRollingReady(context.Context)
	// IsForceRollingReady check if the segment is ready for rolling
	IsForceRollingReady(context.Context) bool
	// GetLastAccessTime get the last access time of the segment
	GetLastAccessTime() int64
}

func NewSegmentHandle(ctx context.Context, logId int64, logName string, segmentMeta *proto.SegmentMetadata, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration, canWrite bool) SegmentHandle {
	executeRequestMaxQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	segmentHandle := &segmentHandleImpl{
		logId:          logId,
		logName:        logName,
		segmentId:      segmentMeta.SegNo,
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
	segmentHandle.lastPushed.Store(segmentMeta.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Size)
	segmentHandle.segmentMetaCache.Store(segmentMeta)
	segmentHandle.fencedState.Store(false)
	segmentHandle.rollingState.Store(false)
	segmentHandle.rollingReadyState.Store(false)
	if canWrite {
		segmentHandle.canWriteState.Store(true)
		segmentHandle.executor.Start(ctx)
	}
	return segmentHandle
}

// NewSegmentHandleWithAppendOpsQueue TODO TestOnly
func NewSegmentHandleWithAppendOpsQueue(ctx context.Context, logId int64, logName string, segmentMeta *proto.SegmentMetadata, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration, testAppendOpsQueue *list.List) SegmentHandle {
	executeRequestMaxQueueSize := cfg.Woodpecker.Client.SegmentAppend.QueueSize
	segmentHandle := &segmentHandleImpl{
		logId:          logId,
		logName:        logName,
		segmentId:      segmentMeta.SegNo,
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
	segmentHandle.lastPushed.Store(segmentMeta.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Size)
	segmentHandle.segmentMetaCache.Store(segmentMeta)
	segmentHandle.fencedState.Store(false)
	segmentHandle.canWriteState.Store(false)
	segmentHandle.rollingState.Store(false)
	segmentHandle.lastAccessTime.Store(time.Now().UnixMilli())
	return segmentHandle
}

var _ SegmentHandle = (*segmentHandleImpl)(nil)

type segmentHandleImpl struct {
	logId            int64
	logName          string
	segmentId        int64
	segmentMetaCache atomic.Pointer[proto.SegmentMetadata]
	metadata         meta.MetadataProvider
	ClientPool       client.LogStoreClientPool
	quorumInfo       *proto.QuorumInfo
	cfg              *config.Configuration

	sync.RWMutex
	lastPushed       atomic.Int64
	lastAddConfirmed atomic.Int64
	appendOpsQueue   *list.List
	commitedSize     atomic.Int64
	pendingSize      atomic.Int64

	fencedState       atomic.Bool // For fence state: true confirms it is fenced, while false requires verification by checking the storage backend for a fence flag file/object.
	canWriteState     atomic.Bool
	rollingState      atomic.Bool
	rollingReadyState atomic.Bool

	executor *SequentialExecutor

	doingRecoveryOrCompact atomic.Bool

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
		callback(s.segmentId, -1, werr.ErrSegmentRolling.WithCauseErrMsg(fmt.Sprintf("segmentHandle[%d/%d] rolling", s.logId, s.segmentId)))
		return
	}

	s.Lock()
	defer s.Unlock()
	currentSegmentMeta := s.segmentMetaCache.Load()

	// Check segment state
	if currentSegmentMeta.State != proto.SegmentState_Active {
		callback(currentSegmentMeta.SegNo, -1, werr.ErrSegmentStateInvalid)
		return
	}

	// Double-check fenced state under lock
	if s.fencedState.Load() {
		callback(currentSegmentMeta.SegNo, -1, werr.ErrSegmentFenced)
		return
	}

	// Create pending append operation
	appendOp := s.createPendingAppendOp(ctx, bytes, callback)

	// Try to submit first, only add to queue if successful
	if submitOk := s.executor.Submit(ctx, appendOp); !submitOk {
		callback(currentSegmentMeta.SegNo, -1, werr.ErrSegmentClosed.WithCauseErrMsg("submit append failed, segment closed"))
		return
	}

	// Only add to queue and update metrics after successful submit
	s.appendOpsQueue.PushBack(appendOp)
	s.pendingSize.Add(int64(len(bytes)))
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
			// success callback
			op.FastSuccess(ctx)
		} else if op.entryId <= s.lastAddConfirmed.Load() {
			elementsToRemove = append(elementsToRemove, e)
			// update size
			s.commitedSize.Add(int64(len(op.value)))
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
			!werr.ErrSegmentClosed.Is(err) && !werr.ErrSegmentFenced.Is(err) {
			op.attempt++
			elementsToRetry = append(elementsToRetry, element)
		} else {
			// retry max times, or encounter non-retryable error
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
		// trigger rolling segment
		// mark rolling
		s.rollingState.Store(true)
		// submit rolling task to queue
		s.executor.Submit(ctx, NewRollingSegmentOp(ctx, fmt.Sprintf("rolling_%d/%d_trigger_%d", s.logId, s.segmentId, minRemoveId), s))
	}

}

// ReadBatch reads batch entries from segment.
func (s *segmentHandleImpl) ReadBatch(ctx context.Context, from int64, size int64) ([]*processor.SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "ReadBatch")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("start read batch", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("from", from), zap.Int64("size", size))
	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return nil, err
	}

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return nil, werr.ErrNotSupport.WithCauseErrMsg("Currently only support embed standalone mode")
	}

	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return nil, err
	}

	if size != -1 {
		return nil, werr.ErrNotSupport.WithCauseErrMsg("support size=-1 as auto batch size currently")
	}

	segmentEntryList, err := cli.ReadEntriesBatch(ctx, s.logId, s.segmentId, from, size)
	if err != nil {
		return nil, err
	}
	logger.Ctx(ctx).Debug("finish read batch", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("from", from), zap.Int64("size", size), zap.Int("count", len(segmentEntryList)))
	return segmentEntryList, nil
}

// GetLastAddConfirmed call by reader
func (s *segmentHandleImpl) GetLastAddConfirmed(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "GetLastAddConfirmed")
	defer sp.End()
	s.updateAccessTime()
	currentSegmentMeta := s.segmentMetaCache.Load()
	// should get from meta if seg completed, other wise get from data
	if currentSegmentMeta.State != proto.SegmentState_Active {
		return s.lastAddConfirmed.Load(), nil
	}

	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return -1, err
	}

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return -1, werr.ErrNotSupport.WithCauseErrMsg("Currently only support embed standalone mode")
	}

	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return -1, err
	}

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	lac, err := cli.GetLastAddConfirmed(ctx, s.logId, currentSegmentMeta.SegNo)
	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "get_lac", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "get_lac", "success").Observe(float64(time.Since(start).Milliseconds()))
	return lac, err
}

// Deprecated
func (s *segmentHandleImpl) GetLastAddPushed(ctx context.Context) (int64, error) {
	return s.lastPushed.Load(), nil
}

func (s *segmentHandleImpl) GetMetadata(ctx context.Context) *proto.SegmentMetadata {
	s.RLock()
	defer s.RUnlock()
	return s.segmentMetaCache.Load()
}

func (s *segmentHandleImpl) RefreshAndGetMetadata(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "RefreshAndGetMetadata")
	defer sp.End()
	s.updateAccessTime()
	s.Lock()
	defer s.Unlock()
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
	quorumId := s.segmentMetaCache.Load().GetQuorumId()
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

func (s *segmentHandleImpl) CloseWritingAndUpdateMetaIfNecessary(ctx context.Context, lastFlushedEntryId int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Close")
	defer sp.End()
	s.updateAccessTime()
	if !s.canWriteState.CompareAndSwap(true, false) {
		return nil
	}

	// Acquire lock to ensure mutual exclusion with AppendAsync
	s.Lock()
	defer s.Unlock()

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	// fast fail all pending append operations
	s.fastFailAppendOpsUnsafe(ctx, lastFlushedEntryId, werr.ErrSegmentClosed)

	// shutdown segment executor
	s.executor.Stop(ctx)

	// update metadata as completed
	currentSegmentMeta := s.segmentMetaCache.Load()
	if currentSegmentMeta.State != proto.SegmentState_Active {
		// not active writable segmentHandle, just return
		return nil
	}

	if lastFlushedEntryId == -1 {
		// Segment metadata is updated immediately with an exact last flushed entry ID; otherwise, it is updated asynchronously by the auditor.
		return nil
	}

	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = proto.SegmentState_Completed
	newSegmentMeta.Size = s.commitedSize.Load()
	newSegmentMeta.LastEntryId = lastFlushedEntryId
	newSegmentMeta.CompletionTime = time.Now().UnixMilli()
	err := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
	if err != nil {
		logger.Ctx(ctx).Warn("segment close failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastFlushedEntryId", lastFlushedEntryId), zap.Int64("lastAddConfirmed", s.lastAddConfirmed.Load()), zap.Int64("completionTime", newSegmentMeta.CompletionTime), zap.Error(err))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "close", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "close", "error").Observe(float64(time.Since(start).Milliseconds()))
	} else {
		logger.Ctx(ctx).Debug("segment closed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastFlushedEntryId", lastFlushedEntryId), zap.Int64("lastAddConfirmed", s.lastAddConfirmed.Load()), zap.Int64("completionTime", newSegmentMeta.CompletionTime))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "close", "success").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "close", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return err
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

func (s *segmentHandleImpl) GetSize(ctx context.Context) int64 {
	return s.pendingSize.Load()
}

func (s *segmentHandleImpl) RequestCompactionAsync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "RequestCompactionAsync")
	defer sp.End()
	s.updateAccessTime()
	// select one node to compact segment asynchronously
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	_, err = s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}

	// TODO should maintain a async compactions queue
	if s.doingRecoveryOrCompact.Load() {
		logger.Ctx(ctx).Debug("segment is doing recovery or compact, skip", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		return nil
	}

	go func() {
		compactErr := s.compactToSealed(ctx)
		if compactErr != nil {
			logger.Ctx(ctx).Warn("segment compaction failed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Error(compactErr))
		}
	}()
	return nil
}

func (s *segmentHandleImpl) Complete(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "Complete")
	defer sp.End()

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
		return -1, werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
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
		return -1, werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
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
	} else {
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
}

func (s *segmentHandleImpl) IsFence(ctx context.Context) (bool, error) {
	//ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "IsFence")
	//defer sp.End()
	//s.updateAccessTime()
	//start := time.Now()
	//logIdStr := fmt.Sprintf("%d", s.logId)
	//if s.fencedState.Load() {
	//	// fast return if fenced state set locally
	//	return true, nil
	//}
	//
	//// fence segment, prevent new append operations
	//quorumInfo, err := s.GetQuorumInfo(ctx)
	//if err != nil {
	//	return false, err
	//}
	//if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
	//	return false, werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	//}
	//cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	//if err != nil {
	//	return false, err
	//}
	//isSegFenced, err := cli.IsSegmentFenced(ctx, s.logId, s.segmentMetaCache.Load().SegNo)
	//if err != nil {
	//	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "is_fence", "error").Inc()
	//	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "is_fence", "error").Observe(float64(time.Since(start).Milliseconds()))
	//	return false, err
	//}
	//
	//// Sync local state with remote state
	//if isSegFenced && !s.fencedState.Load() {
	//	s.Lock()
	//	// Double-check under lock
	//	if !s.fencedState.Load() {
	//		logger.Ctx(ctx).Info("found segment in server fenced, fast fail all start", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
	//		s.fastFailAppendOpsUnsafe(ctx, -1, werr.ErrSegmentFenced)
	//		logger.Ctx(ctx).Info("found segment in server fenced, fast fail all finish", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
	//		s.fencedState.Store(true)
	//	}
	//	s.Unlock()
	//}
	//
	//metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "is_fence", "success").Inc()
	//metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "is_fence", "success").Observe(float64(time.Since(start).Milliseconds()))
	//return isSegFenced, err
	return false, nil
}

// RecoveryOrCompact used for the auditor of this Log
func (s *segmentHandleImpl) RecoveryOrCompact(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "RecoveryOrCompact")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Info("Starting segment recovery or compact operation",
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

	logger.Ctx(ctx).Info("Segment metadata refreshed, determining operation type",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("currentState", currentSegmentMeta.State.String()),
		zap.Int64("lastEntryId", currentSegmentMeta.LastEntryId),
		zap.Int64("size", currentSegmentMeta.Size))

	if currentSegmentMeta.State == proto.SegmentState_Active {
		logger.Ctx(ctx).Info("Segment is in Active state, starting recovery operation",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		return s.recoveryFromInProgress(ctx)
	} else if currentSegmentMeta.State == proto.SegmentState_Completed {
		logger.Ctx(ctx).Info("Segment is in Completed state, starting compaction operation",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		return s.compactToSealed(ctx)
	}

	logger.Ctx(ctx).Info("Segment state does not require maintenance",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("currentState", currentSegmentMeta.State.String()))
	return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("no need to maintain the segment in state:%s , logName:%s logId:%d, segId:%d", currentSegmentMeta.State, s.logName, s.logId, currentSegmentMeta.SegNo))
}

func (s *segmentHandleImpl) recoveryFromInProgress(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "recoveryFromInProgress")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	currentSegmentMeta := s.GetMetadata(ctx)

	logger.Ctx(ctx).Info("Starting segment recovery from Active state",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("currentState", currentSegmentMeta.State.String()),
		zap.Int64("currentLastEntryId", currentSegmentMeta.LastEntryId),
		zap.Int64("currentSize", currentSegmentMeta.Size))

	if currentSegmentMeta.State != proto.SegmentState_Active {
		logger.Ctx(ctx).Warn("Segment state is not Active, cannot perform recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("actualState", currentSegmentMeta.State.String()))
		return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("segment state is not in InProgress. logName:%s logId:%d, segId:%d", s.logName, s.logId, s.segmentId))
	}
	// only one recovery operation at the same time
	if !s.doingRecoveryOrCompact.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("Recovery or compact operation already in progress, skipping",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		return nil
	}
	defer func() {
		s.doingRecoveryOrCompact.Store(false)
		logger.Ctx(ctx).Info("Recovery operation completed, released operation lock",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
	}()

	// select one node to compact segment asynchronously
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get quorum info during recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		logger.Ctx(ctx).Warn("Unsupported quorum configuration for recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Int64("quorumId", quorumInfo.Id),
			zap.Int("nodeCount", len(quorumInfo.Nodes)))
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	cli, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get logstore client for recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.String("targetNode", quorumInfo.Nodes[0]),
			zap.Error(err))
		return err
	}

	logger.Ctx(ctx).Info("start recover segment to completed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.String("state", fmt.Sprintf("%s", currentSegmentMeta.State)))
	recoverySegMetaInfo, recoveryErr := cli.SegmentRecoveryFromInProgress(ctx, s.logId, s.segmentId)
	if recoveryErr != nil {
		logger.Ctx(ctx).Warn("Segment recovery operation failed",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Duration("duration", time.Since(start)),
			zap.Error(recoveryErr))
		metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "recover_from_in_progress", "error").Inc()
		metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "recover_from_in_progress", "error").Observe(float64(time.Since(start).Milliseconds()))
		return recoveryErr
	}

	logger.Ctx(ctx).Info("Segment recovery completed, updating metadata",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("newState", recoverySegMetaInfo.State.String()),
		zap.Int64("recoveredLastEntryId", recoverySegMetaInfo.LastEntryId),
		zap.Int64("recoveredSize", recoverySegMetaInfo.Size),
		zap.Int64("completionTime", recoverySegMetaInfo.CompletionTime))

	// update meta
	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = recoverySegMetaInfo.State
	newSegmentMeta.LastEntryId = recoverySegMetaInfo.LastEntryId
	newSegmentMeta.CompletionTime = recoverySegMetaInfo.CompletionTime
	newSegmentMeta.Size = recoverySegMetaInfo.Size
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
	if updateMetaErr != nil {
		logger.Ctx(ctx).Warn("Failed to update segment metadata after recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(updateMetaErr))
	} else {
		logger.Ctx(ctx).Info("Successfully updated segment metadata after recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
	}

	logger.Ctx(ctx).Info("finish recover segment to completed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastEntryId", recoverySegMetaInfo.LastEntryId))
	// update segmentHandle meta cache
	refreshErr := s.RefreshAndGetMetadata(ctx)
	if refreshErr != nil {
		logger.Ctx(ctx).Warn("Failed to refresh segment metadata cache after recovery",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(refreshErr))
	}

	recoveryDuration := time.Since(start)
	logger.Ctx(ctx).Info("Segment recovery operation completed successfully",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Duration("totalDuration", recoveryDuration),
		zap.Int64("finalLastEntryId", recoverySegMetaInfo.LastEntryId),
		zap.Int64("finalSize", recoverySegMetaInfo.Size))

	metrics.WpSegmentHandleOperationsTotal.WithLabelValues(logIdStr, "recover_from_in_progress", "success").Inc()
	metrics.WpSegmentHandleOperationLatency.WithLabelValues(logIdStr, "recover_from_in_progress", "success").Observe(float64(recoveryDuration.Milliseconds()))
	return updateMetaErr
}

// TODO: It may be necessary to use a last flushed entry id for safe compaction,
// because in local fs mode, data might be written but the flush could fail,
// while the system might have fsynced it to disk.
// This data is beyond the business's flush entry id.
func (s *segmentHandleImpl) compactToSealed(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentHandleScopeName, "compactToSealed")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)

	logger.Ctx(ctx).Info("Starting segment compaction from Completed to Sealed",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId))

	if s.cfg.Woodpecker.Storage.IsStorageLocal() {
		logger.Ctx(ctx).Info("Local storage detected, skipping compaction (not yet implemented)",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		// TODO: Add support for local storage compact once implemented merge/compact function, skip currently
		return nil
	}
	currentSegmentMeta := s.GetMetadata(ctx)

	logger.Ctx(ctx).Info("Validating segment state for compaction",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.String("currentState", currentSegmentMeta.State.String()),
		zap.Int64("currentLastEntryId", currentSegmentMeta.LastEntryId),
		zap.Int64("currentSize", currentSegmentMeta.Size))

	if currentSegmentMeta.State != proto.SegmentState_Completed {
		logger.Ctx(ctx).Info("segment is not in completed state, compaction skip", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("segment state is not in completed. logId:%s, segId:%d", s.logName, s.segmentId))
	}
	if !s.doingRecoveryOrCompact.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("Recovery or compact operation already in progress, skipping compaction",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		return nil
	}
	defer func() {
		s.doingRecoveryOrCompact.Store(false)
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
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
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
	logger.Ctx(ctx).Info("request compact segment from completed to sealed", zap.String("logName", s.logName), zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("lastEntryId", currentSegmentMeta.LastEntryId))
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
		zap.Int64("compactedLastEntryId", compactSegMetaInfo.LastEntryId),
		zap.Int64("compactedSize", compactSegMetaInfo.Size),
		zap.Int64("completionTime", compactSegMetaInfo.CompletionTime),
		zap.Int("entryOffsetCount", len(compactSegMetaInfo.EntryOffset)),
		zap.Int("fragmentOffsetCount", len(compactSegMetaInfo.FragmentOffset)))

	// update segment state and meta
	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = proto.SegmentState_Sealed
	newSegmentMeta.LastEntryId = compactSegMetaInfo.LastEntryId
	newSegmentMeta.CompletionTime = compactSegMetaInfo.CompletionTime
	newSegmentMeta.Size = compactSegMetaInfo.Size
	newSegmentMeta.EntryOffset = compactSegMetaInfo.EntryOffset       // sparse index
	newSegmentMeta.FragmentOffset = compactSegMetaInfo.FragmentOffset //
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
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
	s.rollingReadyState.Store(true)
}

// IsRollingReady check if the segment is ready for rolling
func (s *segmentHandleImpl) IsForceRollingReady(ctx context.Context) bool {
	s.RLock()
	defer s.RUnlock()
	return s.rollingReadyState.Load()
}

func (s *segmentHandleImpl) updateAccessTime() {
	s.lastAccessTime.Store(time.Now().UnixMilli())
}

func (s *segmentHandleImpl) GetLastAccessTime() int64 {
	return s.lastAccessTime.Load()
}
