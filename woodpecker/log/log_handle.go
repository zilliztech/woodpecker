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

package log

import (
	"context"
	"fmt"
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
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

//go:generate mockery --dir=./woodpecker/log --name=LogHandle --structname=LogHandle --output=mocks/mocks_woodpecker/mocks_log_handle --filename=mock_log_handle.go --with-expecter=true  --outpkg=mocks_log_handle
type LogHandle interface {
	// GetName returns the name of the log.
	GetName() string
	// GetId returns the ID of the log.
	GetId() int64
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
	// CheckAndSetSegmentTruncatedIfNeed checks if the segment needs to be truncated and sets the truncated flag accordingly.
	CheckAndSetSegmentTruncatedIfNeed(ctx context.Context) error
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
	// Close closes the log handle.
	Close(context.Context) error
}

const (
	LogHandleScopeName = "LogHandle"
)

var _ LogHandle = (*logHandleImpl)(nil)

type logHandleImpl struct {
	sync.RWMutex

	Name           string
	Id             int64
	LastSegmentId  atomic.Int64 // Holds the last segment ID for cache only; only used to fetch the next New segment ID.
	SegmentHandles map[int64]segment.SegmentHandle
	// Track last access time for segment handles to enable cleanup
	SegmentHandleLastAccess map[int64]time.Time
	// Track last cleanup time to avoid too frequent cleanup
	lastCleanupTime time.Time
	// active writable segment handle index
	WritableSegmentId int64
	Metadata          meta.MetadataProvider
	ClientPool        client.LogStoreClientPool

	// rolling policy
	lastRolloverTimeMs int64
	rollingPolicy      segment.RollingPolicy
	cfg                *config.Configuration
}

func NewLogHandle(name string, logId int64, segments map[int64]*proto.SegmentMetadata, meta meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration) LogHandle {
	// default 10min or 64MB rollover segment
	maxInterval := cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval
	defaultRollingPolicy := segment.NewDefaultRollingPolicy(int64(maxInterval*1000), cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize)
	lastSegmentNo := int64(-1)
	for _, segmentMeta := range segments {
		if lastSegmentNo < segmentMeta.SegNo {
			lastSegmentNo = segmentMeta.SegNo
		}
	}
	l := &logHandleImpl{
		Name:                    name,
		Id:                      logId,
		SegmentHandles:          make(map[int64]segment.SegmentHandle),
		SegmentHandleLastAccess: make(map[int64]time.Time),
		lastCleanupTime:         time.Now(),
		WritableSegmentId:       -1,
		Metadata:                meta,
		ClientPool:              clientPool,
		lastRolloverTimeMs:      -1,
		rollingPolicy:           defaultRollingPolicy,
		cfg:                     cfg,
	}
	l.LastSegmentId.Store(lastSegmentNo)
	return l
}

func (l *logHandleImpl) GetLastRecordId(ctx context.Context) (*LogMessageId, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logHandleImpl) GetName() string {
	return l.Name
}

func (l *logHandleImpl) GetId() int64 {
	return l.Id
}

func (l *logHandleImpl) GetMetadataProvider() meta.MetadataProvider {
	return l.Metadata
}

func (l *logHandleImpl) GetSegments(ctx context.Context) (map[int64]*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "GetSegments")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)
	result, err := l.Metadata.GetAllSegmentMetadata(ctx, l.Name)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_segments", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_segments", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("failed to get segments", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
		return nil, err
	}
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_segments", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_segments", "success").Observe(float64(time.Since(start).Milliseconds()))
	return result, err
}

func (l *logHandleImpl) OpenLogWriter(ctx context.Context) (LogWriter, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "OpenLogWriter")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	se, acquireLockErr := l.Metadata.AcquireLogWriterLock(ctx, l.Name)
	if acquireLockErr != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "lock_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "lock_error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("failed to acquire log writer lock", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(acquireLockErr))
		return nil, acquireLockErr
	}

	// getOrCreate writable segment handle
	_, err := l.GetOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		// Release the acquired lock before returning error
		if releaseErr := l.Metadata.ReleaseLogWriterLock(ctx, l.Name); releaseErr != nil {
			logger.Ctx(ctx).Warn("failed to release log writer lock after error",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Error(releaseErr))
		}
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("failed to acquire log writer lock", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
		return nil, err
	}
	// return LogWriter instance if writableSegmentHandle is created
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "success").Observe(float64(time.Since(start).Milliseconds()))
	return NewLogWriter(ctx, l, l.cfg, se), nil
}

func (l *logHandleImpl) GetOrCreateWritableSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "GetOrCreateWritableSegmentHandle")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	l.Lock()
	defer l.Unlock()

	// Clean up idle segment handles periodically, same as in GetExistsReadonlySegmentHandle
	// This ensures cleanup happens even in write-only scenarios
	l.tryCleanupIdleSegmentHandles(ctx)

	writeableSegmentHandle, writableExists := l.SegmentHandles[l.WritableSegmentId]

	// create new segment handle if not exists
	if !writableExists {
		handle, err := l.createAndCacheWritableSegmentHandle(ctx)
		if err != nil {
			metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_writable_segment", "create_error").Inc()
			metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_writable_segment", "create_error").Observe(float64(time.Since(start).Milliseconds()))
			logger.Ctx(ctx).Warn("get or create writable segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
			return nil, err
		}
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_writable_segment", "success").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_writable_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
		return handle, nil
	}

	// check if writable segment handle should be closed and create new segment handle
	if l.shouldCloseAndCreateWritableSegmentHandle(ctx, writeableSegmentHandle) {
		// close old writable segment handle
		newSeg, err := l.doCloseAndCreateWritableSegmentHandle(ctx, writeableSegmentHandle)
		if err != nil {
			metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_writable_segment", "rollover_error").Inc()
			metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_writable_segment", "rollover_error").Observe(float64(time.Since(start).Milliseconds()))
			logger.Ctx(ctx).Warn("get or create writable segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
			return nil, err
		}
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_writable_segment", "success").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_writable_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
		return newSeg, nil
	}

	// return existing writable segment handle
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_writable_segment", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_writable_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	return writeableSegmentHandle, nil
}

// GetRecoverableSegmentHandle get exists segmentHandle for recover, only logWriter can use this method
func (l *logHandleImpl) GetRecoverableSegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "GetRecoverableSegmentHandle")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	s, err := l.GetExistsReadonlySegmentHandle(ctx, segmentId)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_recoverable_segment", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_recoverable_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get recoverable segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", segmentId), zap.Error(err))
		return nil, err
	}
	if s == nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_recoverable_segment", "not_found").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_recoverable_segment", "not_found").Observe(float64(time.Since(start).Milliseconds()))
		errMsg := fmt.Sprintf("segment not found for logName:%s segmentId:%d,it may have been deleted", l.Name, segmentId)
		getSegErr := werr.ErrSegmentNotFound.WithCauseErrMsg(errMsg)
		logger.Ctx(ctx).Warn("get exists read only segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", segmentId), zap.Error(getSegErr))
		return nil, getSegErr
	}

	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_recoverable_segment", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_recoverable_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	return s, nil
}

func (l *logHandleImpl) GetExistsReadonlySegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "GetExistsReadonlySegmentHandle")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	l.Lock()
	defer l.Unlock()

	// Clean up idle segment handles periodically based on time, not access count
	// This ensures cleanup happens even if we don't hit exact multiples
	l.tryCleanupIdleSegmentHandles(ctx)

	// get from cache directly
	readableSegmentHandle, exists := l.SegmentHandles[segmentId]
	if exists {
		// Update last access time
		l.SegmentHandleLastAccess[segmentId] = time.Now()
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_readonly_segment", "cache_hit").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_readonly_segment", "cache_hit").Observe(float64(time.Since(start).Milliseconds()))
		return readableSegmentHandle, nil
	}
	// get segment meta and create handle
	segMeta, err := l.Metadata.GetSegmentMetadata(ctx, l.Name, segmentId)
	if err != nil && werr.ErrSegmentNotFound.Is(err) {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_readonly_segment", "not_found").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_readonly_segment", "not_found").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get exists read only segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", segmentId), zap.Error(err))
		return nil, err
	}
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_readonly_segment", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_readonly_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get exists read only segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", segmentId), zap.Error(err))
		return nil, err
	}
	if segMeta != nil {
		handle := segment.NewSegmentHandle(ctx, l.Id, l.Name, segMeta, l.Metadata, l.ClientPool, l.cfg, false)
		l.SegmentHandles[segmentId] = handle
		l.SegmentHandleLastAccess[segmentId] = time.Now()
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_readonly_segment", "created").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_readonly_segment", "created").Observe(float64(time.Since(start).Milliseconds()))
		return handle, nil
	}
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_readonly_segment", "null_meta").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_readonly_segment", "null_meta").Observe(float64(time.Since(start).Milliseconds()))
	return nil, nil
}

func (l *logHandleImpl) createAndCacheWritableSegmentHandle(ctx context.Context) (segment.SegmentHandle, error) {
	newSegMeta, err := l.createNewSegmentMeta(ctx)
	if err != nil {
		return nil, err
	}
	newSegHandle := segment.NewSegmentHandle(ctx, l.Id, l.Name, newSegMeta, l.Metadata, l.ClientPool, l.cfg, true)
	l.SegmentHandles[newSegMeta.SegNo] = newSegHandle
	l.SegmentHandleLastAccess[newSegMeta.SegNo] = time.Now()
	l.WritableSegmentId = newSegMeta.SegNo
	l.lastRolloverTimeMs = newSegMeta.CreateTime
	logger.Ctx(ctx).Debug("create and cache new SegmentHandle success", zap.String("logName", l.Name), zap.Int64("segmentId", newSegMeta.SegNo))
	return newSegHandle, nil
}

func (l *logHandleImpl) shouldCloseAndCreateWritableSegmentHandle(ctx context.Context, segmentHandle segment.SegmentHandle) bool {
	size := segmentHandle.GetSize(ctx)
	last := l.lastRolloverTimeMs
	return l.rollingPolicy.ShouldRollover(ctx, size, last)
}

// doCloseAndCreateWritableSegmentHandle fast close segment and create new segment, no need to wait for segment async compaction
func (l *logHandleImpl) doCloseAndCreateWritableSegmentHandle(ctx context.Context, oldSegmentHandle segment.SegmentHandle) (segment.SegmentHandle, error) {
	logger.Ctx(ctx).Debug("start to close segment",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.GetId()),
		zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
	// 1. close segmentHandle,
	//  it will send fence request to logStores
	//  and error out all pendingAppendOps with segmentCloseError
	logger.Ctx(ctx).Debug("start to fence segment", zap.String("logName", l.Name), zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)))
	lastFlushedEntryId, err := oldSegmentHandle.Fence(ctx)
	if err != nil && !werr.ErrSegmentNotFound.Is(err) {
		return nil, err
	}
	logger.Ctx(ctx).Debug("start to close segment", zap.String("logName", l.Name), zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)), zap.Int64("lastFlushedEntryId", lastFlushedEntryId))
	err = oldSegmentHandle.CloseWritingAndUpdateMetaIfNecessary(ctx, lastFlushedEntryId)
	if err != nil {
		return nil, err
	}
	lac, err := oldSegmentHandle.GetLastAddConfirmed(ctx)
	if err == nil {
		logger.Ctx(ctx).Debug("close segment finish", zap.String("logName", l.Name), zap.Int64("segmentId", oldSegmentHandle.GetId(ctx)), zap.Int64("lastAddConfirmed", lac))
	}

	logger.Ctx(ctx).Debug("create new segment handle", zap.String("logName", l.Name))
	// 2. create new segMeta(active)
	newSegmentHandle, err := l.createAndCacheWritableSegmentHandle(ctx)
	if err != nil {
		return nil, err
	}

	// 3. return new segmentHandle
	logger.Ctx(ctx).Debug("doCloseAndCreateNewSegment success",
		zap.String("logName", l.Name),
		zap.Int64("oldSegmentId", oldSegmentHandle.GetId(ctx)),
		zap.Int64("newSegmentId", newSegmentHandle.GetId(ctx)))
	return newSegmentHandle, nil
}

func (l *logHandleImpl) createNewSegmentMeta(ctx context.Context) (*proto.SegmentMetadata, error) {
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
	err = l.Metadata.StoreSegmentMetadata(ctx, l.Name, newSegmentMeta)
	if err != nil {
		return nil, err
	}
	// return
	return newSegmentMeta, nil
}

// TODO To be optimized, reduce meta access, maybe use a param to indicate whether to refresh lastSegmentId, most of the time, the lastSegmentId is not changed
// GetNextSegmentId get the next id according to max seq No
func (l *logHandleImpl) GetNextSegmentId() (int64, error) {
	// try get dynamic max seqNo
	existsSeg, err := l.GetSegments(context.Background())
	if err != nil {
		return -1, err
	}
	maxSegNo := int64(-1)
	for _, seg := range existsSeg {
		if maxSegNo < seg.SegNo {
			maxSegNo = seg.SegNo
		}
	}
	return maxSegNo + 1, nil
}

func (l *logHandleImpl) OpenLogReader(ctx context.Context, from *LogMessageId, readerBaseName string) (LogReader, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "OpenLogReader")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	readerName := fmt.Sprintf("%s-r-%d", l.Name, time.Now().UnixNano())
	if len(readerBaseName) > 0 {
		readerName = fmt.Sprintf("%s-r-%s-%d", l.Name, readerBaseName, time.Now().UnixNano())
	}
	startPoint := l.adjustPendingReadPointIfTruncated(ctx, readerName, from)
	r, err := NewLogBatchReader(ctx, l, nil, startPoint, readerName, l.cfg)
	if err != nil {
		logger.Ctx(ctx).Warn("open log batch reader failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", startPoint.SegmentId), zap.Int64("entryId", startPoint.EntryId), zap.String("readerName", readerName), zap.Error(err))
		return nil, err
	}

	err = l.Metadata.CreateReaderTempInfo(ctx, readerName, l.Id, startPoint.SegmentId, startPoint.EntryId)
	if err != nil {
		// Clean up the created reader before returning error
		if closeErr := r.Close(ctx); closeErr != nil {
			logger.Ctx(ctx).Warn("failed to close reader after CreateReaderTempInfo error",
				zap.String("logName", l.Name),
				zap.String("readerName", readerName),
				zap.Error(closeErr))
		}
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_reader", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_reader", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("open log reader failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", startPoint.SegmentId), zap.Int64("entryId", startPoint.EntryId), zap.String("readerName", readerName), zap.Error(err))
		return nil, werr.ErrReaderTempInfoError.WithCauseErr(err)
	}

	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_reader", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_reader", "success").Observe(float64(time.Since(start).Milliseconds()))
	logger.Ctx(ctx).Debug("open log reader success", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", startPoint.SegmentId), zap.Int64("entryId", startPoint.EntryId), zap.String("readerName", readerName))
	return r, nil
}

// Truncate truncate log by recordId
func (l *logHandleImpl) Truncate(ctx context.Context, recordId *LogMessageId) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "Truncate")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	l.Lock()
	defer l.Unlock()

	logger.Ctx(ctx).Info("Request truncation",
		zap.String("logName", l.Name),
		zap.Int64("truncationSegmentId", recordId.SegmentId),
		zap.Int64("truncationEntryId", recordId.EntryId))

	// 1. Get current LogMeta
	logMeta, err := l.Metadata.GetLogMeta(ctx, l.Name)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
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
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "behind_current").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "success").Observe(float64(time.Since(start).Milliseconds()))
		return nil
	}

	// 3. Check if the requested truncation point exists
	segMeta, err := l.Metadata.GetSegmentMetadata(ctx, l.Name, recordId.SegmentId)
	if err != nil {
		if werr.ErrSegmentNotFound.Is(err) {
			logger.Ctx(ctx).Warn("Requested truncation point does not exist, skip", zap.String("logName", l.Name), zap.Int64("logId", l.Id))
			metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "segment_not_found").Inc()
			metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
			return nil
		}
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "segment_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("Requested truncation failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id))
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// 4. Validate entry ID is within valid range
	if (segMeta.State == proto.SegmentState_Completed || segMeta.State == proto.SegmentState_Sealed) &&
		(recordId.EntryId > segMeta.LastEntryId || recordId.EntryId < 0) {
		logger.Ctx(ctx).Warn("Request truncation failed, entry ID invalid",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.TruncatedEntryId),
			zap.Int64("requestedTruncSegId", recordId.SegmentId),
			zap.Int64("requestedTruncEntryId", recordId.EntryId))
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "invalid_entry_id").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
		invalidErr := werr.ErrTruncateLog.WithCauseErrMsg(
			fmt.Sprintf("truncation entry ID %d exceeds last entry ID %d for segment %d",
				recordId.EntryId, segMeta.LastEntryId, recordId.SegmentId))
		return invalidErr
	}

	// 5. Update LogMeta with new truncation point
	logMeta.TruncatedSegmentId = recordId.SegmentId
	logMeta.TruncatedEntryId = recordId.EntryId
	logMeta.ModificationTimestamp = uint64(time.Now().Unix())

	// 6. Store the updated metadata
	err = l.Metadata.UpdateLogMeta(ctx, l.Name, logMeta)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "update_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("Request truncation failed",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.TruncatedEntryId),
			zap.Int64("requestedTruncSegId", recordId.SegmentId),
			zap.Int64("requestedTruncEntryId", recordId.EntryId),
			zap.Error(err))
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// 7. Update logMetaCache
	logger.Ctx(ctx).Info("Request Truncation completed",
		zap.String("logName", l.Name),
		zap.Int64("truncatedSegmentId", recordId.SegmentId),
		zap.Int64("truncatedEntryId", recordId.EntryId))

	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (l *logHandleImpl) CheckAndSetSegmentTruncatedIfNeed(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "CheckAndSetSegmentTruncatedIfNeed")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	// 1. Get current LogMeta
	logMeta, err := l.Metadata.GetLogMeta(ctx, l.Name)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "meta_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("check segment truncated state failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// 2. Check if the requested truncation point is set
	if logMeta.TruncatedSegmentId <= 0 {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "no_truncation").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil
	}

	// 3. Mark segments as truncated in metadata
	// Get all segments that are before the truncation point
	segments, err := l.GetSegments(ctx)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "segments_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "error").Observe(float64(time.Since(start).Milliseconds()))
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	segmentsTruncated := 0
	for segId, segMetadata := range segments {
		// Skip segments at or after truncation point
		if segId > logMeta.TruncatedSegmentId {
			continue
		}

		// Skip current truncation segment (we'll keep it, but read from the entry ID)
		if segId == logMeta.TruncatedSegmentId {
			continue
		}

		if segMetadata.State == proto.SegmentState_Truncated {
			// already truncated, skip
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
		} else {
			segmentsTruncated++
		}

		logger.Ctx(ctx).Debug("Marked segment as truncated",
			zap.String("logName", l.Name),
			zap.Int64("segmentId", segId))
	}

	if segmentsTruncated > 0 {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "segments_truncated").Inc()
	} else {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "no_segments_truncated").Inc()
	}
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (l *logHandleImpl) adjustPendingReadPointIfTruncated(ctx context.Context, readerName string, from *LogMessageId) *LogMessageId {
	truncatedId, err := l.GetTruncatedRecordId(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get truncated record ID, continuing open reader without truncation check",
			zap.String("logName", l.GetName()),
			zap.String("readerName", readerName),
			zap.Int64("pendingReadSegmentId", from.SegmentId),
			zap.Int64("pendingReadEntryId", from.EntryId),
			zap.Error(err))
	} else if truncatedId != nil {
		// If trying to read from before truncation point, adjust to next valid position
		if l.isBeforeTruncationPoint(from, truncatedId) {
			newPoint := &LogMessageId{
				SegmentId: from.SegmentId,
				EntryId:   from.EntryId,
			}
			// If we're trying to read from a truncated segment that's been completely truncated,
			// move to the next segment
			if newPoint.EntryId < truncatedId.SegmentId {
				newPoint.SegmentId = truncatedId.SegmentId
				newPoint.EntryId = 0
			}

			// If we're in the truncation segment but before truncation point, move to after it
			if newPoint.SegmentId == truncatedId.SegmentId && newPoint.EntryId <= truncatedId.EntryId {
				newPoint.EntryId = truncatedId.EntryId + 1
			}

			logger.Ctx(ctx).Info("Adjusting opening reader start position to after truncation point",
				zap.Int64("originalPendingReadSegmentId", from.SegmentId),
				zap.Int64("originalPendingReadEntryId", from.EntryId),
				zap.Int64("truncatedSegmentId", truncatedId.SegmentId),
				zap.Int64("truncatedEntryId", truncatedId.EntryId),
				zap.Int64("pendingReadSegmentId", newPoint.SegmentId),
				zap.Int64("pendingReadEntryId", newPoint.EntryId))

			return newPoint
		}
	}

	//
	return from
}

// isBeforeTruncationPoint checks if reading position is before the truncation point
func (l *logHandleImpl) isBeforeTruncationPoint(from *LogMessageId, truncatedId *LogMessageId) bool {
	if from.SegmentId < truncatedId.SegmentId {
		return true
	}

	if from.SegmentId == truncatedId.SegmentId && from.EntryId <= truncatedId.EntryId {
		return true
	}
	return false
}

func (l *logHandleImpl) GetTruncatedRecordId(ctx context.Context) (*LogMessageId, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "GetTruncatedRecordId")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	l.RLock()
	defer l.RUnlock()
	// fetch the latest metadata
	logMeta, err := l.Metadata.GetLogMeta(ctx, l.Name)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_truncated_record_id", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_truncated_record_id", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, werr.ErrGetTruncationPoint.WithCauseErr(err)
	}

	// Return the truncation point
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_truncated_record_id", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_truncated_record_id", "success").Observe(float64(time.Since(start).Milliseconds()))
	return &LogMessageId{
		SegmentId: logMeta.TruncatedSegmentId,
		EntryId:   logMeta.TruncatedEntryId,
	}, nil
}

func (l *logHandleImpl) CloseAndCompleteCurrentWritableSegment(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "CloseAndCompleteCurrentWritableSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)

	l.Lock()
	defer l.Unlock()
	defer func() {
		// finally clear writable segment index
		l.WritableSegmentId = -1
	}()

	writeableSegmentHandle, writableExists := l.SegmentHandles[l.WritableSegmentId]
	if !writableExists {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "close_writable_segment", "not_exists").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "close_writable_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil
	}
	// 1. fence segmentHandle
	// Send fence request to log stores and fail pending append operations
	lastFlushedEntryId, err := writeableSegmentHandle.Fence(ctx) // fence first, which will wait for all writing request to be done
	if err != nil && !werr.ErrSegmentNotFound.Is(err) {
		logger.Ctx(ctx).Info("fence segment failed",
			zap.String("logName", l.Name),
			zap.Int64("segId", writeableSegmentHandle.GetId(ctx)),
			zap.Int64("lastFlushedEntryId", lastFlushedEntryId),
			zap.Error(err))
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "close_writable_segment", "fence_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "close_writable_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}
	// 2. close segmentHandle,
	err = writeableSegmentHandle.CloseWritingAndUpdateMetaIfNecessary(ctx, lastFlushedEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("close segment failed",
			zap.String("logName", l.Name),
			zap.Int64("segId", writeableSegmentHandle.GetId(ctx)),
			zap.Int64("lastFlushedEntryId", lastFlushedEntryId),
			zap.Error(err))
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "close_writable_segment", "close_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "close_writable_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}
	//
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "close_writable_segment", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "close_writable_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (l *logHandleImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "Close")
	defer sp.End()

	var lastError error
	// close all segment handles
	for _, segmentHandle := range l.SegmentHandles {
		lastFlushedEntryId, err := segmentHandle.Fence(ctx)
		if err != nil {
			logger.Ctx(ctx).Info("fence segment failed when closing logHandle",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Int64("segId", segmentHandle.GetId(ctx)),
				zap.Int64("lastFlushedEntryId", lastFlushedEntryId),
				zap.Error(err))
			if lastError == nil {
				lastError = err
			}
		}
		err = segmentHandle.CloseWritingAndUpdateMetaIfNecessary(ctx, lastFlushedEntryId)
		if err != nil {
			logger.Ctx(ctx).Info("close segment failed when closing logHandle",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Int64("segId", segmentHandle.GetId(ctx)),
				zap.Int64("lastFlushedEntryId", lastFlushedEntryId),
				zap.Error(err))
			if lastError == nil {
				lastError = err
			}
		}
	}

	// Clear the segment handles map to prevent memory leaks
	l.SegmentHandles = make(map[int64]segment.SegmentHandle)
	l.SegmentHandleLastAccess = make(map[int64]time.Time)
	l.WritableSegmentId = -1

	return lastError
}

// tryCleanupIdleSegmentHandles attempts to clean up idle segment handles if conditions are met
// This method should be called while holding the write lock
func (l *logHandleImpl) tryCleanupIdleSegmentHandles(ctx context.Context) {
	// Cleanup configuration - centralized for easy modification
	const cleanupInterval = 30 * time.Second // How often to check for cleanup
	const maxIdleTime = 1 * time.Minute      // How long a segment can be idle before cleanup
	const minSegmentThreshold = 5            // Minimum segments before cleanup is considered

	now := time.Now()
	if len(l.SegmentHandles) > minSegmentThreshold && now.Sub(l.lastCleanupTime) > cleanupInterval {
		l.cleanupIdleSegmentHandles(ctx, maxIdleTime)
		l.lastCleanupTime = now
	}
}

// cleanupIdleSegmentHandles removes segment handles that haven't been accessed for the specified duration
// This method should be called while holding the write lock
func (l *logHandleImpl) cleanupIdleSegmentHandles(ctx context.Context, maxIdleTime time.Duration) {
	now := time.Now()
	var toRemove []int64

	for segmentId, lastAccess := range l.SegmentHandleLastAccess {
		// Don't cleanup the current writable segment
		if segmentId == l.WritableSegmentId {
			continue
		}

		if now.Sub(lastAccess) > maxIdleTime {
			toRemove = append(toRemove, segmentId)
		}
	}

	for _, segmentId := range toRemove {
		if handle, exists := l.SegmentHandles[segmentId]; exists {
			// Try to close the handle gracefully
			if err := handle.CloseWritingAndUpdateMetaIfNecessary(ctx, -1); err != nil {
				logger.Ctx(ctx).Warn("failed to close segment handle during cleanup",
					zap.String("logName", l.Name),
					zap.Int64("segmentId", segmentId),
					zap.Error(err))
			}
			delete(l.SegmentHandles, segmentId)
			delete(l.SegmentHandleLastAccess, segmentId)
			logger.Ctx(ctx).Debug("cleaned up idle segment handle",
				zap.String("logName", l.Name),
				zap.Int64("segmentId", segmentId))
		}
	}

	if len(toRemove) > 0 {
		logger.Ctx(ctx).Info("cleaned up idle segment handles",
			zap.String("logName", l.Name),
			zap.Int("cleanedCount", len(toRemove)),
			zap.Int("remainingCount", len(l.SegmentHandles)))
	}
}
