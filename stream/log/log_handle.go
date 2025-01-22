package log

import (
	"context"
	"sync"

	"github.com/milvus-io/woodpecker/meta"
	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server/client"
	"github.com/milvus-io/woodpecker/stream/segment"
)

type LogHandle interface {
	GetName() string
	GetSegments(context.Context) ([]*proto.SegmentMetadata, error)
	OpenLogWriter(context.Context) (LogWriter, error)
	OpenLogReader(context.Context, int64) (LogReader, error)
	GetLastRecordId(context.Context) (*LogMessageId, error)
	Truncate(context.Context, int64) error
}

func NewLogHandle(name string, logMeta *proto.LogMeta, segments []*proto.SegmentMetadata, meta meta.MetadataProvider, clientPool client.LogStoreClientPool) *logHandleImpl {
	return &logHandleImpl{
		Name:              name,
		logMetaCache:      logMeta,
		SegmentsCache:     segments,
		SegmentHandles:    make(map[int64]segment.SegmentHandle),
		WritableSegmentId: -1,
		Metadata:          meta,
		ClientPool:        clientPool,
	}
}

var _ LogHandle = (*logHandleImpl)(nil)

type logHandleImpl struct {
	sync.RWMutex

	Name           string
	logMetaCache   *proto.LogMeta
	SegmentsCache  []*proto.SegmentMetadata
	SegmentHandles map[int64]segment.SegmentHandle
	// active writable segment handle index
	WritableSegmentId int64
	Metadata          meta.MetadataProvider
	ClientPool        client.LogStoreClientPool
}

func (l *logHandleImpl) GetLastRecordId(ctx context.Context) (*LogMessageId, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logHandleImpl) GetName() string {
	return l.Name
}

func (l *logHandleImpl) GetSegments(ctx context.Context) ([]*proto.SegmentMetadata, error) {
	return l.SegmentsCache, nil
}

func (l *logHandleImpl) OpenLogWriter(ctx context.Context) (LogWriter, error) {
	// getOrCreate writable segment handle
	_, err := l.getOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		return nil, err
	}
	// return LogWriter instance if writableSegmentHandle is created
	return NewLogWriter(ctx, l), nil
}

func (l *logHandleImpl) getOrCreateWritableSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
	writeableSegmentHandle, writableExists := l.SegmentHandles[l.WritableSegmentId]
	if writableExists {
		return writeableSegmentHandle, nil
	}
	return l.createWritableSegmentHandle(ctx)
}

func (l *logHandleImpl) createWritableSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
	newSegMeta, err := l.createNewSegmentMeta()
	if err != nil {
		return nil, err
	}
	newSegHandle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, newSegMeta, l.Metadata, l.ClientPool)
	l.SegmentHandles[newSegMeta.SegNo] = newSegHandle
	l.WritableSegmentId = newSegMeta.SegNo
	return newSegHandle, nil
}

func (l *logHandleImpl) createNewSegmentMeta() (*proto.SegmentMetadata, error) {
	// construct new segment metadata
	lastEntryId := int64(-1)
	quorumId := int64(0)
	segmentNo := int64(len(l.SegmentsCache) + 1)
	newSegmentMeta := &proto.SegmentMetadata{
		SegNo:       segmentNo,
		LastEntryId: &lastEntryId,
		State:       proto.SegmentState_Active,
		QuorumId:    &quorumId,
		Offset:      make([]int32, 0),
	}
	// create segment metadata
	l.Metadata.StoreSegmentMetadata(context.Background(), l.Name, newSegmentMeta)
	// update local segment metadata cache
	l.SegmentsCache = append(l.SegmentsCache, newSegmentMeta)
	// return
	return newSegmentMeta, nil
}

func (l *logHandleImpl) OpenLogReader(ctx context.Context, fromEntryId int64) (LogReader, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logHandleImpl) Truncate(ctx context.Context, minEntryIdToKeep int64) error {
	//TODO implement me
	panic("implement me")
}
