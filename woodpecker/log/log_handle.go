package log

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

//go:generate mockery --dir=./woodpecker/log --name=LogHandle --structname=LogHandle --output=mocks/mocks_woodpecker/mocks_log_handle --filename=mock_log_handle.go --with-expecter=true  --outpkg=mocks_log_handle
type LogHandle interface {
	// GetName returns the name of the log.
	GetName() string
	// GetSegments retrieves the segment metadata for the log.
	GetSegments(context.Context) (map[int64]*proto.SegmentMetadata, error)
	// OpenLogWriter opens a writer for the log.
	OpenLogWriter(context.Context) (LogWriter, error)
	// OpenLogReader opens a reader for the log with the specified log message ID.
	OpenLogReader(context.Context, *LogMessageId, string) (LogReader, error)
	// GetLastRecordId returns the last record ID of the log.
	GetLastRecordId(context.Context) (*LogMessageId, error)
	// Truncate truncates the log to the specified record ID (inclusive).
	Truncate(context.Context, *LogMessageId) error
	// GetTruncatedRecordId returns the last truncated record ID of the log.
	GetTruncatedRecordId(context.Context) (*LogMessageId, error)
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

	Name              string
	logMetaCache      *proto.LogMeta
	SegmentMetasCache sync.Map
	SegmentHandles    map[int64]segment.SegmentHandle
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

	defaultRollingPolicy := segment.NewDefaultRollingPolicy(int64(maxInterval*1000), cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	l := &logHandleImpl{
		Name:               name,
		logMetaCache:       logMeta,
		SegmentMetasCache:  sync.Map{},
		SegmentHandles:     make(map[int64]segment.SegmentHandle),
		WritableSegmentId:  -1,
		Metadata:           meta,
		ClientPool:         clientPool,
		lastRolloverTimeMs: -1,
		rollingPolicy:      defaultRollingPolicy,
		cfg:                cfg,
	}
	for _, segmentMeta := range segments {
		l.SegmentMetasCache.Store(segmentMeta.SegNo, segmentMeta)
	}
	return l
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
	cacheSegmentMetaSnapshot := make(map[int64]*proto.SegmentMetadata)
	l.SegmentMetasCache.Range(func(key, value interface{}) bool {
		cacheSegmentMetaSnapshot[key.(int64)] = value.(*proto.SegmentMetadata)
		return true
	})
	return cacheSegmentMetaSnapshot, nil
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
		segmentMeta, metaExists := l.SegmentMetasCache.Load(segmentId)
		if metaExists {
			// create a readonly segmentHandle and cache it
			handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta.(*proto.SegmentMetadata), l.Metadata, l.ClientPool, l.cfg)
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
		segmentMeta, metaExists = l.SegmentMetasCache.Load(segmentId)
		if metaExists {
			// create a readonly segmentHandle and cache it
			handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta.(*proto.SegmentMetadata), l.Metadata, l.ClientPool, l.cfg)
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
	segmentMeta, metaExists := l.SegmentMetasCache.Load(segmentId)
	if metaExists {
		// create a readonly segmentHandle and cache it
		handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segmentMeta.(*proto.SegmentMetadata), l.Metadata, l.ClientPool, l.cfg)
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
		l.SegmentMetasCache.Store(segmentId, segMeta)
		handle := segment.NewSegmentHandle(ctx, l.logMetaCache.LogId, l.Name, segMeta, l.Metadata, l.ClientPool, l.cfg)
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
	last := l.lastRolloverTimeMs
	sm, exists := l.SegmentMetasCache.Load(segmentHandle.GetId(ctx))
	if exists {
		last = max(l.lastRolloverTimeMs, sm.(*proto.SegmentMetadata).CreateTime)
	}
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
	logger.Ctx(ctx).Debug("start to fence segment", zap.String("logName", l.Name), zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
	err = oldSegmentHandle.Fence(ctx)
	if err != nil {
		return nil, err
	}
	lac, err := oldSegmentHandle.GetLastAddConfirmed(ctx)
	if err == nil {
		logger.Ctx(ctx).Debug("fence segment finish", zap.String("logName", l.Name), zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)), zap.Int64("lastAddConfirmed", lac))
	}

	logger.Ctx(ctx).Debug("request segment compaction", zap.String("logName", l.Name), zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
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
	l.SegmentMetasCache.Store(segmentNo, newSegmentMeta)
	// return
	return newSegmentMeta, nil
}

// GetNextSegmentId get the next id according to max seq No
func (l *logHandleImpl) GetNextSegmentId() (int64, error) {
	// refresh meta
	maxSeqNo := int64(-1)
	l.SegmentMetasCache.Range(func(key, value interface{}) bool {
		if segMeta, ok := value.(*proto.SegmentMetadata); ok {
			if segMeta.SegNo > maxSeqNo {
				maxSeqNo = segMeta.SegNo
			}
		}
		return true
	})
	nextSeqNo := maxSeqNo + 1
	// try get dynamic max seqNo
	for {
		meta, err := l.Metadata.GetSegmentMetadata(context.Background(), l.Name, nextSeqNo)
		if err == nil && meta != nil {
			l.SegmentMetasCache.Store(nextSeqNo, meta)
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

func (l *logHandleImpl) OpenLogReader(ctx context.Context, from *LogMessageId, readerBaseName string) (LogReader, error) {
	readerName := fmt.Sprintf("%s-r-%d", l.Name, time.Now().UnixNano())
	if len(readerBaseName) > 0 {
		readerName = fmt.Sprintf("%s-r-%s-%d", l.Name, readerBaseName, time.Now().UnixNano())
	}
	return NewLogReader(ctx, l, nil, from, readerName), nil
}

// Truncate truncate log by recordId
func (l *logHandleImpl) Truncate(ctx context.Context, recordId *LogMessageId) error {
	l.Lock()
	defer l.Unlock()

	logger.Ctx(ctx).Info("Request truncation",
		zap.String("logName", l.Name),
		zap.Int64("truncationSegmentId", recordId.SegmentId),
		zap.Int64("truncationEntryId", recordId.EntryId))

	// 1. Get current LogMeta
	logMeta, err := l.Metadata.GetLogMeta(ctx, l.Name)
	if err != nil {
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// 2. Check if the requested truncation point is valid
	// If we're going backward, that's fine, but we should log a warning
	if logMeta.TruncatedSegmentId > recordId.SegmentId ||
		(logMeta.TruncatedSegmentId == recordId.SegmentId && logMeta.TruncatedEntryId > recordId.EntryId) {
		logger.Ctx(ctx).Warn("Truncation point is behind current truncation position",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.TruncatedEntryId),
			zap.Int64("requestedTruncSegId", recordId.SegmentId),
			zap.Int64("requestedTruncEntryId", recordId.EntryId))
		return nil
	}

	// 3. Check if the requested truncation point exists
	segMeta, err := l.Metadata.GetSegmentMetadata(ctx, l.Name, recordId.SegmentId)
	if err != nil {
		if werr.ErrSegmentNotFound.Is(err) {
			logger.Ctx(ctx).Warn("Requested truncation point does not exist, skip", zap.String("logName", l.Name))
			return nil
		}
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// 4. Validate entry ID is within valid range
	if (segMeta.State == proto.SegmentState_Completed || segMeta.State == proto.SegmentState_Sealed) &&
		(recordId.EntryId > segMeta.LastEntryId || recordId.EntryId < 0) {
		logger.Ctx(ctx).Error("truncation entry ID invalid",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.TruncatedEntryId),
			zap.Int64("requestedTruncSegId", recordId.SegmentId),
			zap.Int64("requestedTruncEntryId", recordId.EntryId))
		return werr.ErrTruncateLog.WithCauseErrMsg(
			fmt.Sprintf("truncation entry ID %d exceeds last entry ID %d for segment %d",
				recordId.EntryId, segMeta.LastEntryId, recordId.SegmentId))
	}

	// 5. Update LogMeta with new truncation point
	logMeta.TruncatedSegmentId = recordId.SegmentId
	logMeta.TruncatedEntryId = recordId.EntryId
	logMeta.ModificationTimestamp = uint64(time.Now().Unix())

	// 6. Store the updated metadata
	err = l.Metadata.UpdateLogMeta(ctx, l.Name, logMeta)
	if err != nil {
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// 7. Mark segments as truncated in metadata
	// Get all segments that are before the truncation point
	segments, err := l.GetSegments(ctx)
	if err != nil {
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	for segId, segMetadata := range segments {
		// Skip segments at or after truncation point
		if segId > recordId.SegmentId {
			continue
		}

		// Skip current truncation segment (we'll keep it, but read from the entry ID)
		if segId == recordId.SegmentId {
			continue
		}

		// Mark this segment as truncated
		segMetadata.State = proto.SegmentState_Truncated
		err = l.Metadata.UpdateSegmentMetadata(ctx, l.Name, segMetadata)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to update segment metadata during truncation",
				zap.String("logName", l.Name),
				zap.Int64("segmentId", segId),
				zap.Error(err))
			// Continue with other segments, we'll log the error but not fail the operation
		}

		// Update local cache
		l.SegmentMetasCache.Store(segId, segMetadata)
		l.SegmentHandles[segId] = nil

		logger.Ctx(ctx).Debug("Marked segment as truncated",
			zap.String("logName", l.Name),
			zap.Int64("segmentId", segId))
	}

	// 8. Update logMetaCache
	l.logMetaCache = logMeta

	logger.Ctx(ctx).Info("Truncation completed",
		zap.String("logName", l.Name),
		zap.Int64("truncatedSegmentId", recordId.SegmentId),
		zap.Int64("truncatedEntryId", recordId.EntryId))

	return nil
}

func (l *logHandleImpl) GetTruncatedRecordId(ctx context.Context) (*LogMessageId, error) {
	l.RLock()
	defer l.RUnlock()

	// If we have cached metadata, use it
	if l.logMetaCache != nil {
		return &LogMessageId{
			SegmentId: l.logMetaCache.TruncatedSegmentId,
			EntryId:   l.logMetaCache.TruncatedEntryId,
		}, nil
	}

	// Otherwise, fetch the latest metadata
	logMeta, err := l.Metadata.GetLogMeta(ctx, l.Name)
	if err != nil {
		return nil, werr.ErrGetTruncationPoint.WithCauseErr(err)
	}

	// Update cache
	l.logMetaCache = logMeta

	// Return the truncation point
	return &LogMessageId{
		SegmentId: logMeta.TruncatedSegmentId,
		EntryId:   logMeta.TruncatedEntryId,
	}, nil
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
