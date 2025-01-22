package log

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
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
	// GetNextSegmentId returns the next new segment ID for the log.
	GetNextSegmentId() (int64, error)
	// GetMetadataProvider returns the metadata provider instance.
	GetMetadataProvider() meta.MetadataProvider
	// GetOrCreateWritableSegmentHandle returns the writable segment handle for the log, means creating a new one
	GetOrCreateWritableSegmentHandle(context.Context) (segment.SegmentHandle, error)
	// GetExistsReadonlySegmentHandle returns the segment handle for the specified segment ID if it exists.
	GetExistsReadonlySegmentHandle(context.Context, int64) (segment.SegmentHandle, error)
	// GetRecoverableSegmentHandle returns the segment handle for the specified segment ID if it exists and is in recovery state.
	GetRecoverableSegmentHandle(context.Context, int64) (segment.SegmentHandle, error)
	// CloseAndCompleteCurrentWritableSegment closes and completes the current writable segment handle.
	CloseAndCompleteCurrentWritableSegment(context.Context) error
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
	cfg                *config.Configuration
}

func NewLogHandle(name string, logMeta *proto.LogMeta, segments map[int64]*proto.SegmentMetadata, meta meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration) *logHandleImpl {
	// default 10min or 64MB rollover segment
	maxInterval := cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval

	defaultRollingPolicy := segment.NewDefaultRollingPolicy(int64(maxInterval*1000), int64(cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize))
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
		cfg:                cfg,
	}
}

func (l *logHandleImpl) GetLastRecordId(ctx context.Context) (*LogMessageId, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logHandleImpl) GetName() string {
	return l.Name
}

func (l *logHandleImpl) GetMetadataProvider() meta.MetadataProvider {
	return l.Metadata
}

func (l *logHandleImpl) GetSegments(ctx context.Context) (map[int64]*proto.SegmentMetadata, error) {
	return l.SegmentsCache, nil
}

func (l *logHandleImpl) OpenLogWriter(ctx context.Context) (LogWriter, error) {
	acquireLockErr := l.Metadata.AcquireLogWriterLock(ctx, l.Name)
	if acquireLockErr != nil {
		logger.Ctx(ctx).Warn(fmt.Sprintf("failed to acquire log writer lock for logName:%s", l.Name))
		return nil, acquireLockErr
	}
	// getOrCreate writable segment handle
	_, err := l.GetOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		return nil, err
	}
	// return LogWriter instance if writableSegmentHandle is created
	return NewLogWriter(ctx, l, l.cfg), nil
}

func (l *logHandleImpl) GetOrCreateWritableSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
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

// getRecoverableSegmentHandle get exists segmentHandle for recover, only logWriter can use this method
func (l *logHandleImpl) GetRecoverableSegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	s, err := l.GetExistsReadonlySegmentHandle(ctx, segmentId)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("segment not found for logName:%s segmentId:%d,it may have been deleted", l.Name, segmentId))
	}
	// set recoverable
	segMeta := s.GetMetadata(ctx)
	segMeta.State = proto.SegmentState_InRecovery
	err = l.Metadata.UpdateSegmentMetadata(ctx, l.Name, segMeta)
	if err != nil {
		return nil, err
	}

	//
	return s, nil
}

// Deprecated
func (l *logHandleImpl) GetOrCreateReadonlySegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	l.Lock()
	defer l.Unlock()
	readableSegmentHandle, exists := l.SegmentHandles[segmentId]
	// create readable segment handle if not exists
	if !exists {
		segmentMeta, metaExists := l.SegmentsCache[segmentId]
		if metaExists {
			// create a readonly segmentHandle and cache it
			handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta, l.Metadata, l.ClientPool, l.cfg)
			l.SegmentHandles[segmentId] = handle
			return handle, nil
		}

		// try to get the earliest readable segments
		minSegId, maxSegId, err := l.getCurrentMinMaxSegmentId(ctx)
		if err != nil {
			return nil, err
		}
		if segmentId < minSegId {
			segmentId = minSegId
		} else if segmentId > maxSegId && l.WritableSegmentId != -1 {
			segmentId = l.WritableSegmentId
		} else if segmentId > maxSegId {
			segmentId = maxSegId + 1
			return nil, nil
		}
		segmentMeta, metaExists = l.SegmentsCache[segmentId]
		if metaExists {
			// create a readonly segmentHandle and cache it
			handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta, l.Metadata, l.ClientPool, l.cfg)
			l.SegmentHandles[segmentId] = handle
			return handle, nil
		} else {
			return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("segment not found for logName:%s segmentId:%d,it may have been deleted", l.Name, segmentId))
		}
	}
	return readableSegmentHandle, nil
}

func (l *logHandleImpl) GetExistsReadonlySegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	l.Lock()
	defer l.Unlock()
	// get from cache directly
	readableSegmentHandle, exists := l.SegmentHandles[segmentId]
	if exists {
		return readableSegmentHandle, nil
	}
	// create from exists meta cache
	segmentMeta, metaExists := l.SegmentsCache[segmentId]
	if metaExists {
		// create a readonly segmentHandle and cache it
		handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta, l.Metadata, l.ClientPool, l.cfg)
		l.SegmentHandles[segmentId] = handle
		return handle, nil
	}
	// refresh meta and create handle
	segMeta, err := l.Metadata.GetSegmentMetadata(ctx, l.Name, segmentId)
	if err != nil && werr.ErrSegmentNotFound.Is(err) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if segMeta != nil {
		l.SegmentsCache[segmentId] = segMeta
		handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta, l.Metadata, l.ClientPool, l.cfg)
		l.SegmentHandles[segmentId] = handle
		return handle, nil
	}
	return nil, nil
}

func (l *logHandleImpl) createAndCacheNewSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
	newSegMeta, err := l.createNewSegmentMeta()
	if err != nil {
		return nil, err
	}
	newSegHandle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, newSegMeta, l.Metadata, l.ClientPool, l.cfg)
	l.SegmentHandles[newSegMeta.SegNo] = newSegHandle
	l.WritableSegmentId = newSegMeta.SegNo
	l.lastRolloverTimeMs = newSegMeta.CreateTime
	logger.Ctx(ctx).Debug("create and cache new SegmentHandle success", zap.String("logName", l.Name), zap.Int64("segmentId", newSegMeta.SegNo))
	return newSegHandle, nil
}

func (l *logHandleImpl) shouldCloseAndCreateNewSegment(ctx context.Context, segmentHandle segment.SegmentHandle) bool {
	size := segmentHandle.GetSize(ctx)
	last := max(l.lastRolloverTimeMs, l.SegmentsCache[segmentHandle.GetId(ctx)].CreateTime)
	return l.rollingPolicy.ShouldRollover(size, last)
}

// doCloseAndCreateNewSegment fast close segment and create new segment, no need to wait for segment async compaction
func (l *logHandleImpl) doCloseAndCreateNewSegment(ctx context.Context, oldSegmentHandle segment.SegmentHandle) (segment.SegmentHandle, error) {
	start := time.Now()
	logger.Ctx(ctx).Debug("start to close segment",
		zap.String("logName", l.Name),
		zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
	// 1. close segmentHandle,
	//  it will send fence request to logStores
	//  and error out all pendingAppendOps with segmentCloseError
	err := oldSegmentHandle.Close(ctx)
	if err != nil {
		return nil, err
	}
	logger.Ctx(ctx).Debug("start to fence segment",
		zap.String("logName", l.Name),
		zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
	err = oldSegmentHandle.Fence(ctx)
	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Debug("request segment compaction",
		zap.String("logName", l.Name),
		zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
	// 2. select one logStore to async compact segment
	err = oldSegmentHandle.RequestCompactionAsync(ctx)
	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Debug("create new segment handle", zap.String("logName", l.Name))
	// 3. create new segMeta(active)
	newSegmentHandle, err := l.createAndCacheNewSegmentHandle(ctx)
	if err != nil {
		return nil, err
	}

	cost := time.Now()
	metrics.WpSegmentRollingLatency.WithLabelValues(l.Name).Observe(float64(cost.Sub(start).Milliseconds()))
	// 4. return new segmentHandle

	logger.Ctx(ctx).Debug("doCloseAndCreateNewSegment success",
		zap.String("logName", l.Name),
		zap.Int64("oldSegmentId", oldSegmentHandle.GetId(ctx)),
		zap.Int64("newSegmentId", newSegmentHandle.GetId(ctx)))
	return newSegmentHandle, nil
}

func (l *logHandleImpl) segmentCompactionCompletedCallback(logId int64, segmentMeta *proto.SegmentMetadata, err error) {
	// TODO update meta or retry if failed
}

func (l *logHandleImpl) createNewSegmentMeta() (*proto.SegmentMetadata, error) {
	// construct new segment metadata
	segmentNo, err := l.GetNextSegmentId()
	if err != nil {
		return nil, err
	}
	newSegmentMeta := &proto.SegmentMetadata{
		SegNo:          segmentNo,
		CreateTime:     time.Now().UnixMilli(),
		QuorumId:       -1,
		State:          proto.SegmentState_Active,
		LastEntryId:    -1,
		Size:           0,
		EntryOffset:    make([]int32, 0),
		FragmentOffset: make([]int32, 0),
	}
	// create segment metadata
	err = l.Metadata.StoreSegmentMetadata(context.Background(), l.Name, newSegmentMeta)
	if err != nil {
		return nil, err
	}
	// update local segment metadata cache
	l.SegmentsCache[segmentNo] = newSegmentMeta
	// return
	return newSegmentMeta, nil
}

// GetNextSegmentId get the next id according to max seq No
func (l *logHandleImpl) GetNextSegmentId() (int64, error) {
	// refresh meta
	maxSeqNo := int64(-1)
	for _, seg := range l.SegmentsCache {
		if seg.SegNo > maxSeqNo {
			maxSeqNo = seg.SegNo
		}
	}
	nextSeqNo := maxSeqNo + 1
	// try get dynamic max seqNo
	for {
		meta, err := l.Metadata.GetSegmentMetadata(context.Background(), l.Name, nextSeqNo)
		if err == nil && meta != nil {
			l.SegmentsCache[nextSeqNo] = meta
			nextSeqNo++
		} else if err != nil && werr.ErrSegmentNotFound.Is(err) {
			break
		} else if err != nil {
			// get meta err
			return -1, err
		}
	}
	return nextSeqNo, nil
}

func (l *logHandleImpl) OpenLogReader(ctx context.Context, from *LogMessageId) (LogReader, error) {
	//readableSegmentHandle, err := l.getOrCreateReadonlySegmentHandle(ctx, from.SegmentId)
	//if err != nil {
	//	return nil, err
	//}
	//return NewLogReader(ctx, l, readableSegmentHandle, from), nil
	return NewLogReader(ctx, l, nil, from), nil
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
		logger.Ctx(ctx).Warn("close segment failed",
			zap.String("logName", l.Name),
			zap.Int64("segId", writeableSegmentHandle.GetId(ctx)),
			zap.Error(err))
		return err
	}
	err = writeableSegmentHandle.Fence(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("fence segment failed",
			zap.String("logName", l.Name),
			zap.Int64("segId", writeableSegmentHandle.GetId(ctx)),
			zap.Error(err))
		return err
	}

	// 2. select one logStore to async compact segment
	err = writeableSegmentHandle.RequestCompactionAsync(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("request compaction failed",
			zap.String("logName", l.Name),
			zap.Int64("segId", writeableSegmentHandle.GetId(ctx)),
			zap.Error(err))
	}

	// 3. clear cache
	l.WritableSegmentId = -1
	return nil
}

func (l *logHandleImpl) getCurrentMinMaxSegmentId(ctx context.Context) (int64, int64, error) {
	// TODO
	return 0, -1, nil
}
