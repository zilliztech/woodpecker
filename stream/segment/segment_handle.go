package segment

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/server/segment"
)

type SegmentHandle interface {
	// GetLogName to which the segment belongs
	GetLogName() string
	// GetId of the segment
	GetId(context.Context) int64
	// Append data to the segment
	Append(context.Context, []byte) (int64, error)
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
	RequestCompactionAsync(context.Context, func(int64, int64, error)) error
	// Fence the segment in all nodes
	Fence(context.Context) error
}

func NewSegmentHandle(ctx context.Context, logId int64, logName string, segmentMeta *proto.SegmentMetadata, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool) SegmentHandle {
	segmentHandle := &segmentHandleImpl{
		logId:            logId,
		logName:          logName,
		segmentMetaCache: segmentMeta,
		metadata:         metadata,
		ClientPool:       clientPool,
		appendOpsQueue:   list.New(),
		quorumInfo: &proto.QuorumInfo{ // TODO: get from metadata in cluster mode
			Id: 1,
			Wq: 1,
			Aq: 1,
			Es: 1,
			Nodes: []string{
				"127.0.0.1",
			},
		},
		orderExecutor: NewOrderExecutor(10000),
	}
	segmentHandle.lastPushed.Store(segmentMeta.LastEntryId)
	segmentHandle.lastAddConfirmed.Store(segmentMeta.LastEntryId)
	segmentHandle.commitedSize.Store(segmentMeta.Size)
	segmentHandle.orderExecutor.Start(1)
	return segmentHandle
}

var _ SegmentHandle = (*segmentHandleImpl)(nil)

type segmentHandleImpl struct {
	logId            int64
	logName          string
	segmentMetaCache *proto.SegmentMetadata
	metadata         meta.MetadataProvider
	ClientPool       client.LogStoreClientPool
	quorumInfo       *proto.QuorumInfo

	sync.RWMutex
	lastPushed       atomic.Int64
	lastAddConfirmed atomic.Int64
	appendOpsQueue   *list.List
	commitedSize     atomic.Int64
	pendingSize      atomic.Int64

	orderExecutor *OrderExecutor
}

func (s *segmentHandleImpl) GetLogName() string {
	return s.logName
}

func (s *segmentHandleImpl) GetId(ctx context.Context) int64 {
	return s.segmentMetaCache.SegNo
}

// Deprecated: use AppendAsync instead
func (s *segmentHandleImpl) Append(ctx context.Context, bytes []byte) (int64, error) {
	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return 0, err
	}

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return -1, errors.New("Currently only support embed standalone mode")
	}

	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return -1, err
	}
	segmentEntry := &segment.SegmentEntry{
		SegmentId: s.segmentMetaCache.SegNo,
		EntryId:   s.lastPushed.Add(1),
		Data:      bytes,
	}
	entryId, syncedCh, err := client.AppendEntry(ctx, s.logId, segmentEntry)
	if err != nil {
		return -1, err
	}
	for {
		select {
		case syncedId, ok := <-syncedCh:
			if !ok {
				return entryId, errors.New("synced channel closed")
			}
			if syncedId >= entryId {
				s.commitedSize.Add(int64(len(bytes)))
				return entryId, nil
			}
		}
	}
}

func (s *segmentHandleImpl) AppendAsync(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error)) {
	s.Lock()
	defer s.Unlock()
	// Check segment state
	if s.segmentMetaCache.State != proto.SegmentState_Active {
		callback(s.segmentMetaCache.SegNo, -1, errors.New("segment not active for write"))
		return
	}
	// Create pending append operation and execute asynchronously
	appendOp := s.createPendingAppendOp(ctx, bytes, callback)
	//fmt.Printf("pushed %d \n", appendOp.entryId)
	s.appendOpsQueue.PushBack(appendOp)
	s.orderExecutor.Submit(appendOp)
	s.pendingSize.Add(int64(len(bytes)))
}

func (s *segmentHandleImpl) createPendingAppendOp(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error)) *AppendOp {
	pendingAppendOp := &AppendOp{
		logId:      s.logId,
		segmentId:  s.GetId(ctx),
		entryId:    s.lastPushed.Add(1),
		value:      bytes,
		callback:   callback,
		handle:     s,
		clientPool: s.ClientPool,
		ackSet:     &bitset.BitSet{},
		quorumInfo: s.quorumInfo,
		completed:  false,
	}
	return pendingAppendOp
}

func (s *segmentHandleImpl) SendAppendSuccessCallbacks(triggerEntryId int64) {
	fmt.Printf("SendAppendSuccessCallbacks by: %d \n", triggerEntryId)
	s.Lock()
	defer s.Unlock()
	// success executed one by one in sequence
	elementsToRemove := []*list.Element{} // 新增：用于存储需要删除的元素
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		op := e.Value.(*AppendOp)
		if !op.completed {
			//fmt.Printf("SendAppendSuccessCallbacks not compeleted by: %d \n", op.entryId)
			break
		}
		// Check if it is the next entry in the sequence.
		if op.entryId != s.lastAddConfirmed.Load()+1 {
			//fmt.Printf("SendAppendSuccessCallbacks not the next lac by: %d \n", op.entryId)
			break
		}

		elementsToRemove = append(elementsToRemove, e) // 新增：将需要删除的元素添加到切片中
		// update lac
		s.lastAddConfirmed.Store(op.entryId)
		// update size
		s.commitedSize.Add(int64(len(op.value)))
		//fmt.Printf("current Segment %d size %d \n", s.segmentMetaCache.SegNo, s.size.Load())
		// callback
		op.callback(op.segmentId, op.entryId, nil)
	}
	for _, element := range elementsToRemove { // 新增：遍历切片并删除元素
		//fmt.Printf("SendAppendSuccessCallbacks remove: %v \n", element)
		s.appendOpsQueue.Remove(element)
	}
}

func (s *segmentHandleImpl) SendAppendErrorCallbacks(triggerEntryId int64, err error) {
	s.Lock()
	defer s.Unlock()
	// fail, and call pendingAppendOps callback(nil,err)
	fmt.Printf("entryId %d append error: %v\n", triggerEntryId, err)
	elementsToRemove := []*list.Element{}
	for e := s.appendOpsQueue.Front(); e != nil; e = e.Next() {
		element := e
		op := element.Value.(*AppendOp)
		op.callback(s.segmentMetaCache.SegNo, op.entryId, err)
		elementsToRemove = append(elementsToRemove, element)
	}
	for _, element := range elementsToRemove {
		s.appendOpsQueue.Remove(element)
	}
}

func (s *segmentHandleImpl) Read(ctx context.Context, from int64, to int64) ([]*segment.SegmentEntry, error) {
	// write data to quorum
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return nil, err
	}

	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return nil, errors.New("Currently only support embed standalone mode")
	}

	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return nil, err
	}

	if from > to {
		return nil, errors.New("from must be less than to")
	}

	if from == to {
		segmentEntry, err := client.ReadEntry(ctx, s.logId, s.segmentMetaCache.SegNo, from)
		if err != nil {
			return nil, err
		}
		return []*segment.SegmentEntry{segmentEntry}, nil
	}

	segmentEntryList, err := client.ReadBatchEntries(ctx, s.logId, s.segmentMetaCache.SegNo, from, to)
	if err != nil {
		return nil, err
	}
	return segmentEntryList, nil
}

func (s *segmentHandleImpl) GetLastAddConfirmed(ctx context.Context) (int64, error) {
	return s.lastAddConfirmed.Load(), nil
}

func (s *segmentHandleImpl) GetLastAddPushed(ctx context.Context) (int64, error) {
	return s.lastPushed.Load(), nil
}

func (s *segmentHandleImpl) GetMetadata(ctx context.Context) *proto.SegmentMetadata {
	return s.segmentMetaCache
}

func (s *segmentHandleImpl) GetQuorumInfo(ctx context.Context) (*proto.QuorumInfo, error) {
	if s.quorumInfo != nil {
		return s.quorumInfo, nil
	}
	quorumId := s.segmentMetaCache.GetQuorumId()
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
	return s.segmentMetaCache.State >= proto.SegmentState_Completed, nil
}

func (s *segmentHandleImpl) Close(ctx context.Context) error {
	// error out all pending append operations
	s.SendAppendErrorCallbacks(-1, errors.New("segment closed"))

	// update metadata as completed
	s.segmentMetaCache.State = proto.SegmentState_Completed
	s.segmentMetaCache.Size = s.commitedSize.Load()
	s.segmentMetaCache.LastEntryId = s.lastAddConfirmed.Load()
	s.segmentMetaCache.CompletionTime = time.Now().UnixMilli()
	return s.metadata.UpdateSegmentMetadata(ctx, s.logName, s.segmentMetaCache)
}

func (s *segmentHandleImpl) GetSize(ctx context.Context) int64 {
	//return s.size.Load()
	return s.pendingSize.Load()
}

func (s *segmentHandleImpl) RequestCompactionAsync(ctx context.Context, callback func(int64, int64, error)) error {
	// select one node to compact segment asynchronously
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return errors.New("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}

	// TODO should maintain a async compactions queue
	go func() {
		err = client.RequestCompaction(ctx, s.logId, s.segmentMetaCache.SegNo)
		callback(s.logId, s.segmentMetaCache.SegNo, err)
	}()
	return nil
}

func (s *segmentHandleImpl) Fence(ctx context.Context) error {
	// fence segment, prevent new append operations
	quorumInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return err
	}
	if len(quorumInfo.Nodes) != 1 || quorumInfo.Wq != 1 || quorumInfo.Aq != 1 || quorumInfo.Es != 1 {
		return errors.New("currently only support embed standalone mode")
	}
	client, err := s.ClientPool.GetLogStoreClient(quorumInfo.Nodes[0])
	if err != nil {
		return err
	}
	err = client.FenceSegment(ctx, s.logId, s.segmentMetaCache.SegNo)
	if err != nil {
		return err
	}
	return nil
}
