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
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/server/segment"
)

//go:generate mockery --dir=./woodpecker/segment --name=SegmentHandle --structname=SegmentHandle --output=mocks/mocks_woodpecker/mocks_segment_handle --filename=mock_segment_handle.go --with-expecter=true  --outpkg=mocks_segment_handle
type SegmentHandle interface {
	// GetLogName to which the segment belongs
	GetLogName() string
	// GetId of the segment
	GetId(context.Context) int64
	// AppendAsync data to the segment asynchronously
	AppendAsync(context.Context, []byte, func(int64, int64, error))
	// Read range entries from the segment
	Read(context.Context, int64, int64) ([]*segment.SegmentEntry, error)
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
	// IsClosed check if the segment is closed
	IsClosed(context.Context) (bool, error)
	// Close the segment
	Close(context.Context) error
	// SendAppendSuccessCallbacks called when an appendOp operation is successful
	SendAppendSuccessCallbacks(int64)
	// SendAppendErrorCallbacks called when an appendOp operation fails
	SendAppendErrorCallbacks(int64, error)
	// GetSize get the size of the segment
	GetSize(context.Context) int64
	// RequestCompactionAsync request compaction for the segment asynchronously
	RequestCompactionAsync(context.Context) error
	// Fence the segment in all nodes
	Fence(context.Context) error
	IsFence(context.Context) (bool, error)
	// RecoveryOrCompact is a recovery or compaction operation
	RecoveryOrCompact(todo context.Context) error
}

func NewSegmentHandle(ctx context.Context, logId int64, logName string, segmentMeta *proto.SegmentMetadata, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration) SegmentHandle {
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
		executor:             NewSequentialExecutor(executeRequestMaxQueueSize),
		failedAppendOpsQueue: list.New(),
		cfg:                  cfg,
	}
	segmentHandle.lastPushed.Store(segmentMeta.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Size)
	segmentHandle.segmentMetaCache.Store(segmentMeta)
	segmentHandle.executor.Start()
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
		executor:             NewSequentialExecutor(executeRequestMaxQueueSize),
		failedAppendOpsQueue: list.New(),
		cfg:                  cfg,
	}
	segmentHandle.lastPushed.Store(segmentMeta.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Size)
	segmentHandle.segmentMetaCache.Store(segmentMeta)
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

	executor *SequentialExecutor

	failedAppendOpsQueue *list.List

	doingRecoveryOrCompact atomic.Bool
}

func (s *segmentHandleImpl) GetLogName() string {
	return s.logName
}

func (s *segmentHandleImpl) GetId(ctx context.Context) int64 {
	return s.segmentId
}

func (s *segmentHandleImpl) AppendAsync(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error)) {
	s.Lock()
	defer s.Unlock()
	currentSegmentMeta := s.segmentMetaCache.Load()

	// Check segment state
	if currentSegmentMeta.State != proto.SegmentState_Active {
		callback(currentSegmentMeta.SegNo, -1, werr.ErrSegmentStateInvalid)
		return
	}

	// check segment is fenced
	isFenced, _ := s.IsFence(ctx)
	if isFenced {
		callback(currentSegmentMeta.SegNo, -1, werr.ErrSegmentFenced)
		return
	}

	// Create pending append operation and execute asynchronously
	appendOp := s.createPendingAppendOp(ctx, bytes, callback)
	//fmt.Printf("pushed %d \n", appendOp.entryId)
	s.appendOpsQueue.PushBack(appendOp)
	metrics.WpAppendOpEntriesGauge.WithLabelValues(fmt.Sprintf("%d", s.logId)).Add(float64(len(bytes)))
	metrics.WpAppendOpRequestsGauge.WithLabelValues(fmt.Sprintf("%d", s.logId)).Inc()
	s.executor.Submit(appendOp)
	s.pendingSize.Add(int64(len(bytes)))
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
func (s *segmentHandleImpl) SendAppendSuccessCallbacks(triggerEntryId int64) {
	logger.Ctx(context.TODO()).Debug(fmt.Sprintf("SendAppendSuccessCallbacks by: %d \n", triggerEntryId))
	s.Lock()
	defer s.Unlock()
	// success executed one by one in sequence
	elementsToRemove := []*list.Element{}
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		op := e.Value.(*AppendOp)
		if !op.completed.Load() {
			logger.Ctx(context.TODO()).Debug("SendAppendSuccessCallbacks not completed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
			break
		}
		// Check if it is the next entry in the sequence.
		if op.entryId != s.lastAddConfirmed.Load()+1 {
			logger.Ctx(context.TODO()).Debug("SendAppendSuccessCallbacks not the next lac", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
			break
		}

		elementsToRemove = append(elementsToRemove, e)
		// update lac
		s.lastAddConfirmed.Store(op.entryId)
		// update size
		s.commitedSize.Add(int64(len(op.value)))
		// callback
		op.callback(op.segmentId, op.entryId, nil)
	}
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
		op := element.Value.(*AppendOp)
		logger.Ctx(context.TODO()).Debug("SendAppendSuccessCallbacks remove", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
		metrics.WpAppendOpEntriesGauge.WithLabelValues(fmt.Sprintf("%d", s.logId)).Sub(float64(len(op.value)))
		metrics.WpAppendOpRequestsGauge.WithLabelValues(fmt.Sprintf("%d", s.logId)).Dec()
	}
}

// SendAppendErrorCallbacks will do error callback and remove from pendingAppendOps sequentially
func (s *segmentHandleImpl) SendAppendErrorCallbacks(triggerEntryId int64, err error) {
	s.Lock()
	defer s.Unlock()
	logger.Ctx(context.Background()).Warn("SendAppendFailedCallbacks", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("triggerEntryId", triggerEntryId), zap.String("msg", err.Error()))

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
		if op.attempt < s.cfg.Woodpecker.Client.SegmentAppend.MaxRetries && (op.err == nil || op.err != nil && werr.IsRetryableErr(op.err)) {
			op.attempt++
			elementsToRetry = append(elementsToRetry, element)
		} else {
			// retry max times, or encounter non-retryable error
			elementsToRemove = append(elementsToRemove, element)
			if minRemoveId == -1 || op.entryId < minRemoveId {
				minRemoveId = op.entryId
			}
		}
		break
	}

	// do not remove, just resubmit to retry again
	for _, element := range elementsToRetry {
		op := element.Value.(*AppendOp)
		if op.entryId == triggerEntryId {
			s.executor.Submit(op)
			logger.Ctx(context.Background()).Debug("append retry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", triggerEntryId), zap.Int64("triggerId", triggerEntryId))
			continue
		}
		if op.entryId < minRemoveId {
			s.executor.Submit(op)
			logger.Ctx(context.Background()).Debug("append retry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", triggerEntryId), zap.Int64("triggerId", triggerEntryId))
		} else {
			logger.Ctx(context.Background()).Debug(fmt.Sprintf("append entry:%d fast fail, cause entry:%d already failed", op.entryId, minRemoveId), zap.Int64("triggerId", triggerEntryId))
			elementsToRemove = append(elementsToRemove, element)
		}
	}

	// fast fail other queue appends which Id greater then minRemoveId.
	// because without the hole entry, their will never success
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		element := e
		op := element.Value.(*AppendOp)
		if op.entryId > minRemoveId {
			logger.Ctx(context.Background()).Debug(fmt.Sprintf("append entry:%d fast fail, cause entry:%d already failed", op.entryId, minRemoveId), zap.Int64("triggerId", triggerEntryId))
			elementsToRemove = append(elementsToRemove, element)
		}
	}
	// send error callback to all elementsToRemove
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
		op := element.Value.(*AppendOp)
		op.callback(s.segmentId, op.entryId, err)
		logger.Ctx(context.Background()).Debug("append fail after retry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId), zap.Int64("entryId", op.entryId), zap.Int64("triggerId", triggerEntryId))
		metrics.WpAppendOpEntriesGauge.WithLabelValues(fmt.Sprintf("%d", s.logId)).Sub(float64(len(op.value)))
		metrics.WpAppendOpRequestsGauge.WithLabelValues(fmt.Sprintf("%d", s.logId)).Dec()
	}
}

func (s *segmentHandleImpl) Read(ctx context.Context, from int64, to int64) ([]*segment.SegmentEntry, error) {
	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return nil, err
	}

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return nil, werr.ErrNotSupport.WithCauseErrMsg("Currently only support embed standalone mode")
	}

	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return nil, err
	}

	if from > to {
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg("from must be less than to")
	}

	if from == to {
		segmentEntry, err := client.ReadEntry(ctx, s.logId, s.segmentId, from)
		if err != nil {
			return nil, err
		}
		return []*segment.SegmentEntry{segmentEntry}, nil
	}

	segmentEntryList, err := client.ReadBatchEntries(ctx, s.logId, s.segmentId, from, to)
	if err != nil {
		return nil, err
	}
	return segmentEntryList, nil
}

// GetLastAddConfirmed call by reader
func (s *segmentHandleImpl) GetLastAddConfirmed(ctx context.Context) (int64, error) {
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

	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return -1, err
	}

	return client.GetLastAddConfirmed(ctx, s.logId, currentSegmentMeta.SegNo)
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
	s.Lock()
	defer s.Unlock()
	newMeta, err := s.metadata.GetSegmentMetadata(ctx, s.logName, s.segmentId)
	if err != nil {
		logger.Ctx(ctx).Warn("refresh segment meta failed",
			zap.String("logName", s.logName),
			zap.Int64("segId", s.segmentId),
			zap.Error(err))
		return err
	}
	s.segmentMetaCache.Store(newMeta)
	return nil
}

func (s *segmentHandleImpl) GetQuorumInfo(ctx context.Context) (*proto.QuorumInfo, error) {
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

func (s *segmentHandleImpl) IsClosed(ctx context.Context) (bool, error) {
	return s.segmentMetaCache.Load().State >= proto.SegmentState_Completed, nil
}

func (s *segmentHandleImpl) Close(ctx context.Context) error {
	// error out all pending append operations
	s.SendAppendErrorCallbacks(-1, werr.ErrSegmentClosed)

	// update metadata as completed
	currentSegmentMeta := s.segmentMetaCache.Load()
	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = proto.SegmentState_Completed
	newSegmentMeta.Size = s.commitedSize.Load()
	newSegmentMeta.LastEntryId = s.lastAddConfirmed.Load()
	newSegmentMeta.CompletionTime = time.Now().UnixMilli()
	return s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
}

func (s *segmentHandleImpl) GetSize(ctx context.Context) int64 {
	//return s.size.Load()
	return s.pendingSize.Load()
}

func (s *segmentHandleImpl) RequestCompactionAsync(ctx context.Context) error {
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
		logger.Ctx(ctx).Debug("segment is doing recovery or compact, skip", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		return nil
	}

	go s.compactToSealed(ctx)
	return nil
}

func (s *segmentHandleImpl) Fence(ctx context.Context) error {
	// fence segment, prevent new append operations
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}
	err = client.FenceSegment(ctx, s.logId, s.segmentId)
	if err != nil {
		return err
	}
	return nil
}

func (s *segmentHandleImpl) IsFence(ctx context.Context) (bool, error) {
	// fence segment, prevent new append operations
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return false, err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return false, werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return false, err
	}
	return client.IsSegmentFenced(ctx, s.logId, s.segmentMetaCache.Load().SegNo)
}

// RecoveryOrCompact used for the auditor of this Log
func (s *segmentHandleImpl) RecoveryOrCompact(ctx context.Context) error {
	currentSegmentMeta := s.GetMetadata(ctx)
	if currentSegmentMeta.State == proto.SegmentState_Active {
		return s.recoveryFromInProgress(ctx)
	} else if currentSegmentMeta.State == proto.SegmentState_Completed {
		return s.compactToSealed(ctx)
	} else if currentSegmentMeta.State == proto.SegmentState_InRecovery {
		return s.recoveryFromInRecovery(ctx)
	}
	return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("no need to maintain the segment in state:%s , logId:%s, segId:%d", currentSegmentMeta.State, s.logName, currentSegmentMeta.SegNo))
}

func (s *segmentHandleImpl) recoveryFromInProgress(ctx context.Context) error {
	currentSegmentMeta := s.GetMetadata(ctx)
	if currentSegmentMeta.State != proto.SegmentState_Active {
		return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("segment state is not in InProgress. logId:%s, segId:%d", s.logName, s.segmentId))
	}
	// only one recovery operation at the same time
	if s.doingRecoveryOrCompact.Load() {
		logger.Ctx(ctx).Debug("segment is doing recovery or compact, skip", zap.Int64("logId", s.logId), zap.Int64("segId", s.segmentId))
		return nil
	}
	defer s.doingRecoveryOrCompact.Store(false)
	// select one node to compact segment asynchronously
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}
	recoverySegMetaInfo, recoveryErr := client.SegmentRecoveryFromInProgress(ctx, s.logId, s.segmentId)
	if recoveryErr != nil {
		return recoveryErr
	}
	// update meta
	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = recoverySegMetaInfo.State
	newSegmentMeta.LastEntryId = recoverySegMetaInfo.LastEntryId
	newSegmentMeta.CompletionTime = recoverySegMetaInfo.CompletionTime
	newSegmentMeta.Size = recoverySegMetaInfo.Size
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
	return updateMetaErr
}

func (s *segmentHandleImpl) compactToSealed(ctx context.Context) error {
	currentSegmentMeta := s.GetMetadata(ctx)
	if currentSegmentMeta.State != proto.SegmentState_Completed {
		return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("segment state is not in completed. logId:%s, segId:%d", s.logName, s.segmentId))
	}
	if s.doingRecoveryOrCompact.Load() {
		return nil
	}
	defer s.doingRecoveryOrCompact.Store(false)

	logger.Ctx(ctx).Info("request compact segment from completed to sealed", zap.String("logName", s.logName), zap.Int64("segId", s.segmentId))
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	// choose on LogStore to compaction
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}
	// wait compaction success
	compactSegMetaInfo, compactErr := client.SegmentCompact(ctx, s.logId, s.segmentId)
	if compactErr != nil {
		return compactErr
	}
	// update segment state and meta
	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = proto.SegmentState_Sealed
	newSegmentMeta.LastEntryId = compactSegMetaInfo.LastEntryId
	newSegmentMeta.CompletionTime = compactSegMetaInfo.CompletionTime
	newSegmentMeta.Size = compactSegMetaInfo.Size
	newSegmentMeta.EntryOffset = compactSegMetaInfo.EntryOffset       // sparse index
	newSegmentMeta.FragmentOffset = compactSegMetaInfo.FragmentOffset //
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
	logger.Ctx(ctx).Info("finish compact segment to sealed", zap.String("logName", s.logName), zap.Int64("segId", s.segmentId), zap.Int64("lastEntryId", compactSegMetaInfo.LastEntryId))
	return updateMetaErr
}

func (s *segmentHandleImpl) recoveryFromInRecovery(ctx context.Context) error {
	currentSegmentMeta := s.GetMetadata(ctx)
	if currentSegmentMeta.State != proto.SegmentState_InRecovery {
		return werr.ErrSegmentStateInvalid.WithCauseErrMsg(fmt.Sprintf("segment state is not in InRecovery, skip this compaction. logId:%s, segId:%d", s.logName, s.segmentId))
	}
	if s.doingRecoveryOrCompact.Load() {
		return nil
	}
	defer s.doingRecoveryOrCompact.Store(false)

	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return werr.ErrNotSupport.WithCauseErrMsg("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}
	recoverySegMetaInfo, recoveryErr := client.SegmentRecoveryFromInRecovery(ctx, s.logId, s.segmentId)
	if recoveryErr != nil {
		return recoveryErr
	}
	// update segment state and meta
	newSegmentMeta := currentSegmentMeta.CloneVT()
	newSegmentMeta.State = recoverySegMetaInfo.State
	newSegmentMeta.LastEntryId = recoverySegMetaInfo.LastEntryId
	newSegmentMeta.CompletionTime = recoverySegMetaInfo.CompletionTime
	newSegmentMeta.Size = recoverySegMetaInfo.Size
	updateMetaErr := s.metadata.UpdateSegmentMetadata(ctx, s.logName, newSegmentMeta)
	return updateMetaErr
}
