package log

import (
	"context"
	"fmt"
	"github.com/zilliztech/woodpecker/common/werr"
	"sync"
	"time"

	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/stream/segment"
)

type LogHandle interface {
	// GetName returns the name of the log.
	GetName() string
	// GetSegments retrieves the segment metadata for the log.
	GetSegments(context.Context) (map[int64]*proto.SegmentMetadata, error)
	// OpenLogWriter opens a writer for the log.
	OpenLogWriter(context.Context) (LogWriter, error)
	// OpenLogReader opens a reader for the log with the specified log message ID.
	OpenLogReader(context.Context, *LogMessageId) (LogReader, error)
	// GetLastRecordId returns the last record ID of the log.
	GetLastRecordId(context.Context) (*LogMessageId, error)
	// Truncate truncates the log to the specified offset.
	Truncate(context.Context, int64) error
}

var _ LogHandle = (*logHandleImpl)(nil)

type logHandleImpl struct {
	sync.RWMutex

	Name           string
	logMetaCache   *proto.LogMeta
	SegmentsCache  map[int64]*proto.SegmentMetadata
	SegmentHandles map[int64]segment.SegmentHandle
	// active writable segment handle index
	WritableSegmentId int64
	Metadata          meta.MetadataProvider
	ClientPool        client.LogStoreClientPool

	// rolling policy
	lastRolloverTimeMs int64
	rollingPolicy      segment.RollingPolicy
}

func NewLogHandle(name string, logMeta *proto.LogMeta, segments map[int64]*proto.SegmentMetadata, meta meta.MetadataProvider, clientPool client.LogStoreClientPool) *logHandleImpl {
	// default 10min or 64MB rollover segment
	defaultRollingPolicy := segment.NewDefaultRollingPolicy(10_000, 128_000_000)
	return &logHandleImpl{
		Name:               name,
		logMetaCache:       logMeta,
		SegmentsCache:      segments,
		SegmentHandles:     make(map[int64]segment.SegmentHandle),
		WritableSegmentId:  -1,
		Metadata:           meta,
		ClientPool:         clientPool,
		lastRolloverTimeMs: -1,
		rollingPolicy:      defaultRollingPolicy,
	}
}

func (l *logHandleImpl) GetLastRecordId(ctx context.Context) (*LogMessageId, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logHandleImpl) GetName() string {
	return l.Name
}

func (l *logHandleImpl) GetSegments(ctx context.Context) (map[int64]*proto.SegmentMetadata, error) {
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
	l.Lock()
	defer l.Unlock()
	writeableSegmentHandle, writableExists := l.SegmentHandles[l.WritableSegmentId]

	// create new segment handle if not exists
	if !writableExists {
		return l.createAndCacheNewSegmentHandle(ctx)
	}

	// check if writable segment handle should be closed and create new segment handle
	if l.shouldCloseAndCreateNewSegment(ctx, writeableSegmentHandle) {
		// close old writable segment handle
		newSeg, err := l.doCloseAndCreateNewSegment(ctx, writeableSegmentHandle)
		if err != nil {
			return nil, err
		}
		return newSeg, nil
	}

	// return existing writable segment handle
	return writeableSegmentHandle, nil
}

func (l *logHandleImpl) getOrCreateReadonlySegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	l.Lock()
	defer l.Unlock()
	readableSegmentHandle, exists := l.SegmentHandles[segmentId]
	// create readable segment handle if not exists
	if !exists {
		segmentMeta, metaExists := l.SegmentsCache[segmentId]
		if !metaExists {
			return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("segment %d not found", segmentId))
		}
		// create a readonly segmentHandle and cache it
		handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta, l.Metadata, l.ClientPool)
		l.SegmentHandles[segmentId] = handle
		return handle, nil
	}
	return readableSegmentHandle, nil
}

func (l *logHandleImpl) createAndCacheNewSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
	newSegMeta, err := l.createNewSegmentMeta()
	if err != nil {
		return nil, err
	}
	fmt.Println("create new segment ", newSegMeta.SegNo)
	newSegHandle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, newSegMeta, l.Metadata, l.ClientPool)
	l.SegmentHandles[newSegMeta.SegNo] = newSegHandle
	l.WritableSegmentId = newSegMeta.SegNo
	l.lastRolloverTimeMs = newSegMeta.CreateTime
	fmt.Println("create new segment handle success", newSegHandle.GetId(ctx))
	return newSegHandle, nil
}

func (l *logHandleImpl) shouldCloseAndCreateNewSegment(ctx context.Context, segmentHandle segment.SegmentHandle) bool {
	size := segmentHandle.GetSize(ctx)
	last := max(l.lastRolloverTimeMs, l.SegmentsCache[segmentHandle.GetId(ctx)].CreateTime)
	return l.rollingPolicy.ShouldRollover(size, last)
}

// doCloseAndCreateNewSegment fast close segment and create new segment, no need to wait for segment async compaction
func (l *logHandleImpl) doCloseAndCreateNewSegment(ctx context.Context, oldSegmentHandle segment.SegmentHandle) (segment.SegmentHandle, error) {
	fmt.Println("doCloseAndCreateNewSegment, start closing segment", oldSegmentHandle.GetId(ctx))
	// 1. close segmentHandle,
	//  it will send fence request to logStores
	//  and error out all pendingAppendOps with segmentCloseError
	err := oldSegmentHandle.Close(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Println("doCloseAndCreateNewSegment, start fencing segment", oldSegmentHandle.GetId(ctx))
	err = oldSegmentHandle.Fence(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("doCloseAndCreateNewSegment, request segment compaction", oldSegmentHandle.GetId(ctx))
	// 2. select one logStore to async compact segment
	err = oldSegmentHandle.RequestCompactionAsync(ctx, l.segmentCompactionCompletedCallback)
	if err != nil {
		return nil, err
	}

	fmt.Println("doCloseAndCreateNewSegment, create new segment")
	// 3. create new segMeta(active)
	newSegmentHandle, err := l.createAndCacheNewSegmentHandle(ctx)
	if err != nil {
		return nil, err
	}

	// 4. return new segmentHandle
	return newSegmentHandle, nil
}

func (l *logHandleImpl) segmentCompactionCompletedCallback(logId int64, segmentId int64, err error) {
	// TODO update meta or retry if failed
}

func (l *logHandleImpl) createNewSegmentMeta() (*proto.SegmentMetadata, error) {
	// construct new segment metadata
	segmentNo := l.getNextSegmentId()
	newSegmentMeta := &proto.SegmentMetadata{
		SegNo:       segmentNo,
		CreateTime:  time.Now().UnixMilli(),
		QuorumId:    -1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
		Size:        0,
		Offset:      make([]int32, 0),
	}
	// create segment metadata
	err := l.Metadata.StoreSegmentMetadata(context.Background(), l.Name, newSegmentMeta)
	if err != nil {
		return nil, err
	}
	// update local segment metadata cache
	l.SegmentsCache[segmentNo] = newSegmentMeta
	// return
	return newSegmentMeta, nil
}

// getNextSegmentId get the next id according to max seq No
func (l *logHandleImpl) getNextSegmentId() int64 {
	maxSeqNo := int64(0)
	for _, seg := range l.SegmentsCache {
		if seg.SegNo > maxSeqNo {
			maxSeqNo = seg.SegNo
		}
	}
	return maxSeqNo + 1
}

func (l *logHandleImpl) OpenLogReader(ctx context.Context, from *LogMessageId) (LogReader, error) {
	readableSegmentHandle, err := l.getOrCreateReadonlySegmentHandle(ctx, from.SegmentId)
	if err != nil {
		return nil, err
	}
	return NewLogReader(ctx, l, readableSegmentHandle, from), nil
}

func (l *logHandleImpl) Truncate(ctx context.Context, minEntryIdToKeep int64) error {
	//TODO implement me
	panic("implement me")
}

func (l *logHandleImpl) CloseAndCompleteCurrentWritableSegment(ctx context.Context) error {
	l.Lock()
	defer l.Unlock()

	writeableSegmentHandle, writableExists := l.SegmentHandles[l.WritableSegmentId]
	if !writableExists {
		return nil
	}
	// 1. close segmentHandle,
	//  it will send fence request to logStores
	//  and error out all pendingAppendOps with segmentCloseError
	err := writeableSegmentHandle.Close(ctx)
	if err != nil {
		fmt.Println("close segment failed", err)
		return err
	}
	err = writeableSegmentHandle.Fence(ctx)
	if err != nil {
		fmt.Println("fence segment failed", err)
		return err
	}

	// 2. select one logStore to async compact segment
	err = writeableSegmentHandle.RequestCompactionAsync(ctx, l.segmentCompactionCompletedCallback)
	if err != nil {
		fmt.Println("WARN: request compaction failed", err)
	}

	// 3. clear cache
	l.WritableSegmentId = -1
	return nil
}
