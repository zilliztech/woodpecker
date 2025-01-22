package segment

import (
	"container/list"
	"context"
	"errors"
	"github.com/milvus-io/woodpecker/common/bitset"
	"sync"

	"github.com/milvus-io/woodpecker/meta"
	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server/client"
	"github.com/milvus-io/woodpecker/server/segment"
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
	// BatchRead entries from the segment
	BatchRead(context.Context, int64, int32, int32) ([]*segment.SegmentEntry, error)
	// GetLastAddConfirmed entryId for the segment
	GetLastAddConfirmed(context.Context) (int64, error)
	// GetLastAddPushed entryId for the segment
	GetLastAddPushed(context.Context) (int64, error)
	// GetMetadata of the segment
	GetMetadata(context.Context) (*proto.SegmentMetadata, error)
	// GetQuorumInfo of the segment if it's a active segment
	GetQuorumInfo(context.Context) (*proto.QuorumInfo, error)
	// IsClosed check if the segment is closed
	IsClosed(context.Context) (bool, error)
	// Close the segment
	Close(context.Context) error
	// SendAppendSuccessCallbacks called when an appendOp operation is successful
	SendAppendSuccessCallbacks()
	// SendAppendErrorCallbacks called when an appendOp operation fails
	SendAppendErrorCallbacks(err error)
}

func NewSegmentHandle(ctx context.Context, logId int64, logName string, segmentMeta *proto.SegmentMetadata, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool) SegmentHandle {
	return &segmentHandleImpl{
		logId:            logId,
		logName:          logName,
		segmentMetaCache: segmentMeta,
		metadata:         metadata,
		ClientPool:       clientPool,
		lastPushed:       *segmentMeta.LastEntryId,
		lastAddConfirmed: *segmentMeta.LastEntryId,
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
	}
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
	lastPushed       int64
	lastAddConfirmed int64
	appendOpsQueue   *list.List
}

func (s *segmentHandleImpl) GetLogName() string {
	return s.logName
}

func (s *segmentHandleImpl) GetId(ctx context.Context) int64 {
	return s.segmentMetaCache.SegNo
}

func (s *segmentHandleImpl) Append(ctx context.Context, bytes []byte) (int64, error) {
	// write data to quorum
	quorunInfo, err := s.GetQuorumInfo(ctx)
	if err != nil {
		return 0, err
	}

	if len(quorunInfo.Nodes) != 1 || quorunInfo.Wq != 1 || quorunInfo.Aq != 1 || quorunInfo.Es != 1 {
		return -1, errors.New("Currently only support embed standalone mode")
	}

	client, err := s.ClientPool.GetLogStoreClient(quorunInfo.Nodes[0])
	if err != nil {
		return -1, err
	}
	segmentEntry := &segment.SegmentEntry{
		SegmentId: s.segmentMetaCache.SegNo,
		EntryId:   s.lastPushed + 1,
		Data:      bytes,
	}
	entryId, err := client.AppendEntry(ctx, s.logId, segmentEntry)
	if err != nil {
		return -1, err
	}
	s.lastPushed = s.lastPushed + 1
	s.lastAddConfirmed = s.lastPushed
	return entryId, nil
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
	s.appendOpsQueue.PushBack(appendOp)
	appendOp.Execute()
}

func (s *segmentHandleImpl) createPendingAppendOp(ctx context.Context, bytes []byte, callback func(segmentId int64, entryId int64, err error)) *AppendOp {
	pendingAppendOp := &AppendOp{
		logId:      s.logId,
		segmentId:  s.GetId(ctx),
		entryId:    s.lastPushed + 1,
		value:      bytes,
		callback:   callback,
		handle:     s,
		clientPool: s.ClientPool,
		ackSet:     &bitset.BitSet{},
		quorumInfo: s.quorumInfo,
		completed:  false,
	}
	s.lastPushed = s.lastPushed + 1
	return pendingAppendOp
}

func (s *segmentHandleImpl) SendAppendSuccessCallbacks() {
	// success executed one by one in sequence
	for e := s.appendOpsQueue.Front(); e != nil; {
		op := e.Value.(*AppendOp)
		if !op.completed {
			break
		}
		// Check if it is the next entry in the sequence.
		if op.entryId != s.lastAddConfirmed+1 {
			break
		}

		s.appendOpsQueue.Remove(e)
		// update lac
		s.lastAddConfirmed = op.entryId
		// callback
		op.callback(op.segmentId, op.entryId, nil)
	}
}

func (s *segmentHandleImpl) SendAppendErrorCallbacks(err error) {
	// fail, and call pendingAppendOps callback(nil,err)
	for e := s.appendOpsQueue.Front(); e != nil; {
		element := s.appendOpsQueue.Remove(e)
		element.(*AppendOp).callback(s.segmentMetaCache.SegNo, -1, err)
	}
}

func (s *segmentHandleImpl) Read(ctx context.Context, i int64, i2 int64) ([]*segment.SegmentEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (s *segmentHandleImpl) BatchRead(ctx context.Context, i int64, i2 int32, i3 int32) ([]*segment.SegmentEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (s *segmentHandleImpl) GetLastAddConfirmed(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (s *segmentHandleImpl) GetLastAddPushed(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (s *segmentHandleImpl) GetMetadata(ctx context.Context) (*proto.SegmentMetadata, error) {
	return s.segmentMetaCache, nil
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
	//TODO implement me
	panic("implement me")
}

func (s *segmentHandleImpl) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
