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
	"sort"
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
	GetSegments(ctx context.Context) (map[int64]*meta.SegmentMeta, error)
	// OpenLogWriter opens a writer for the log.
	OpenLogWriter(ctx context.Context) (LogWriter, error)
	// OpenInternalLogWriter opens an internal writer for the log.
	OpenInternalLogWriter(ctx context.Context) (LogWriter, error)
	// OpenLogReader opens a reader for the log with the specified log message ID.
	OpenLogReader(ctx context.Context, from *LogMessageId, readerBaseName string) (LogReader, error)
	// GetLastRecordId returns the last record ID of the log.
	GetLastRecordId(ctx context.Context) (*LogMessageId, error)
	// Truncate truncates the log to the specified record ID (inclusive).
	Truncate(ctx context.Context, recordId *LogMessageId) error
	// GetTruncatedRecordId returns the last truncated record ID of the log.
	GetTruncatedRecordId(ctx context.Context) (*LogMessageId, error)
	// CheckAndSetSegmentTruncatedIfNeed checks if the segment needs to be truncated and sets the truncated flag accordingly.
	CheckAndSetSegmentTruncatedIfNeed(ctx context.Context) error
	// GetNextSegmentId returns the next new segment ID for the log.
	GetNextSegmentId() (int64, error)
	// GetMetadataProvider returns the metadata provider instance.
	GetMetadataProvider() meta.MetadataProvider
	// GetOrCreateWritableSegmentHandle returns the writable segment handle for the log, means creating a new one
	GetOrCreateWritableSegmentHandle(ctx context.Context, writerInvalidationNotifier func(ctx context.Context, reason string)) (segment.SegmentHandle, error)
	// GetExistsReadonlySegmentHandle returns the segment handle for the specified segment ID if it exists.
	GetExistsReadonlySegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error)
	// GetRecoverableSegmentHandle returns the segment handle for the specified segment ID if it exists and is in recovery state.
	GetRecoverableSegmentHandle(ctx context.Context, segmentId int64) (segment.SegmentHandle, error)
	// CompleteAllActiveSegmentIfExists completes all active segment handles for the log.
	CompleteAllActiveSegmentIfExists(ctx context.Context) error
	// Close closes the log handle.
	Close(ctx context.Context) error
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
	// active writable segment handle index
	WritableSegmentId int64
	Metadata          meta.MetadataProvider
	ClientPool        client.LogStoreClientPool

	// rolling policy
	lastRolloverTimeMs int64
	rollingPolicy      segment.RollingPolicy
	cfg                *config.Configuration

	// Background cleanup goroutine management
	ctx         context.Context
	cancel      context.CancelFunc
	cleanupWg   sync.WaitGroup
	cleanupDone chan struct{}
}

func NewLogHandle(name string, logId int64, segments map[int64]*meta.SegmentMeta, meta meta.MetadataProvider, clientPool client.LogStoreClientPool, cfg *config.Configuration) LogHandle {
	// default 10min or 64MB rollover segment
	maxInterval := cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval
	defaultRollingPolicy := segment.NewDefaultRollingPolicy(int64(maxInterval*1000), cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize, cfg.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks)
	lastSegmentNo := int64(-1)
	for _, segmentMeta := range segments {
		if lastSegmentNo < segmentMeta.Metadata.SegNo {
			lastSegmentNo = segmentMeta.Metadata.SegNo
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &logHandleImpl{
		Name:               name,
		Id:                 logId,
		SegmentHandles:     make(map[int64]segment.SegmentHandle),
		WritableSegmentId:  -1,
		Metadata:           meta,
		ClientPool:         clientPool,
		lastRolloverTimeMs: -1,
		rollingPolicy:      defaultRollingPolicy,
		cfg:                cfg,
		ctx:                ctx,
		cancel:             cancel,
		cleanupDone:        make(chan struct{}),
	}
	l.LastSegmentId.Store(lastSegmentNo)
	l.startBackgroundCleanup()
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

func (l *logHandleImpl) GetSegments(ctx context.Context) (map[int64]*meta.SegmentMeta, error) {
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
	logger.Ctx(ctx).Info("open log writer start", zap.String("logName", l.Name), zap.Int64("logId", l.Id))

	se, acquireLockErr := l.Metadata.AcquireLogWriterLock(ctx, l.Name)
	if acquireLockErr != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "lock_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "lock_error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("failed to acquire log writer lock", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(acquireLockErr))
		return nil, acquireLockErr
	}

	// Fence all active segments to prevent split-brain scenarios
	err := l.fenceAllActiveSegments(ctx)
	if err != nil {
		// Release the acquired lock before returning error
		if releaseErr := l.Metadata.ReleaseLogWriterLock(ctx, l.Name); releaseErr != nil {
			logger.Ctx(ctx).Warn("failed to release log writer lock after fence error",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Error(releaseErr))
		}
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "fence_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "fence_error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("failed to fence all active segments", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
		return nil, err
	}

	// return LogWriter instance if writableSegmentHandle is created
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "success").Observe(float64(time.Since(start).Milliseconds()))
	logger.Ctx(ctx).Info("open log writer success", zap.String("logName", l.Name), zap.Int64("logId", l.Id))
	return NewLogWriter(ctx, l, l.cfg, se), nil
}

func (l *logHandleImpl) OpenInternalLogWriter(ctx context.Context) (LogWriter, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "OpenInternalLogWriter")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", l.Id)
	logger.Ctx(ctx).Info("open internal log writer start", zap.String("logName", l.Name), zap.Int64("logId", l.Id))

	// Fence all active segments to prevent split-brain scenarios
	err := l.fenceAllActiveSegments(ctx)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "fence_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "fence_error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("failed to fence all active segments", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
		return nil, err
	}

	// return LogWriter instance if writableSegmentHandle is created
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "open_log_writer", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "open_log_writer", "success").Observe(float64(time.Since(start).Milliseconds()))
	logger.Ctx(ctx).Info("open log writer success", zap.String("logName", l.Name), zap.Int64("logId", l.Id))
	return NewInternalLogWriter(ctx, l, l.cfg), nil
}

// fenceAllActiveSegments fences all active segments to prevent split-brain scenarios
func (l *logHandleImpl) fenceAllActiveSegments(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "fenceAllActiveSegments")
	defer sp.End()

	// Get all segment metadata to find active segments
	segments, err := l.GetSegments(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to get segments for fencing",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.Id),
			zap.Error(err))
		return err
	}

	// Find all active segments
	var activeSegmentIds []int64
	for segId, segMeta := range segments {
		if segMeta.Metadata.State == proto.SegmentState_Active {
			activeSegmentIds = append(activeSegmentIds, segId)
		}
	}

	// Sort active segment IDs for consistent processing order
	sort.Slice(activeSegmentIds, func(i, j int) bool {
		return activeSegmentIds[i] < activeSegmentIds[j]
	})

	if len(activeSegmentIds) == 0 {
		logger.Ctx(ctx).Debug("no active segments exist, skipping fence operation",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.Id))
		return nil
	}

	logger.Ctx(ctx).Info("fencing all active segments",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id),
		zap.Int64s("activeSegmentIds", activeSegmentIds))

	// Use concurrent goroutines to fence segments
	type fenceResult struct {
		segmentId   int64
		lastEntryId int64
		err         error
	}

	resultChan := make(chan fenceResult, len(activeSegmentIds))
	var wg sync.WaitGroup

	// Start concurrent fence operations
	for _, segmentId := range activeSegmentIds {
		wg.Add(1)
		go func(segId int64) {
			defer wg.Done()

			result := fenceResult{segmentId: segId}

			// Get segment handle for fencing
			segmentHandle, err := l.GetExistsReadonlySegmentHandle(ctx, segId)
			if err != nil {
				logger.Ctx(ctx).Warn("failed to get segment handle for fencing",
					zap.String("logName", l.Name),
					zap.Int64("segmentId", segId),
					zap.Error(err))
				result.err = fmt.Errorf("failed to get segment handle %d: %w", segId, err)
				resultChan <- result
				return
			}

			if segmentHandle == nil {
				logger.Ctx(ctx).Debug("segment handle not found, skipping fence",
					zap.String("logName", l.Name),
					zap.Int64("segmentId", segId))
				result.err = fmt.Errorf("segment handle %d not found", segId)
				resultChan <- result
				return
			}

			// Fence the segment
			lastEntryId, err := segmentHandle.Fence(ctx)
			if err != nil {
				logger.Ctx(ctx).Warn("failed to fence segment",
					zap.String("logName", l.Name),
					zap.Int64("segmentId", segId),
					zap.Error(err))
				result.err = fmt.Errorf("failed to fence segment %d: %w", segId, err)
				resultChan <- result
				return
			}

			logger.Ctx(ctx).Info("successfully fenced segment",
				zap.String("logName", l.Name),
				zap.Int64("segmentId", segId),
				zap.Int64("lastEntryId", lastEntryId))

			result.lastEntryId = lastEntryId
			resultChan <- result
		}(segmentId)
	}

	// Wait for all fence operations to complete
	wg.Wait()
	close(resultChan)

	// Collect results
	var fenceErrors []error
	successfullyFenced := 0
	fencedSegments := make(map[int64]int64) // segmentId -> lastEntryId

	for result := range resultChan {
		if result.err != nil {
			fenceErrors = append(fenceErrors, result.err)
		} else {
			successfullyFenced++
			fencedSegments[result.segmentId] = result.lastEntryId
		}
	}

	// Update metadata for successfully fenced segments
	for segmentId, lastEntryId := range fencedSegments {
		segMeta := segments[segmentId]
		segMeta.Metadata.State = proto.SegmentState_Sealed
		segMeta.Metadata.LastEntryId = lastEntryId
		err = l.Metadata.UpdateSegmentMetadata(ctx, l.Name, segMeta)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to update segment metadata after fence",
				zap.String("logName", l.Name),
				zap.Int64("segmentId", segmentId),
				zap.Error(err))
			// Don't treat metadata update failure as critical error, but log it
		}
	}

	// If we had errors fencing segments, return a combined error
	if len(fenceErrors) > 0 {
		logger.Ctx(ctx).Error("encountered errors while fencing active segments",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.Id),
			zap.Int("totalActiveSegments", len(activeSegmentIds)),
			zap.Int("successfullyFenced", successfullyFenced),
			zap.Int("errorCount", len(fenceErrors)))

		// Return standardized error with combined error details
		combinedErr := werr.Combine(fenceErrors...)
		return werr.ErrLogHandleFenceFailed.WithCauseErr(combinedErr)
	}

	logger.Ctx(ctx).Info("successfully fenced all active segments",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id),
		zap.Int("totalFenced", successfullyFenced))

	return nil
}

func (l *logHandleImpl) GetOrCreateWritableSegmentHandle(ctx context.Context, writerInvalidationNotifier func(ctx context.Context, reason string)) (segment.SegmentHandle, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "GetOrCreateWritableSegmentHandle")
	defer sp.End()

	l.Lock()
	defer l.Unlock()

	writeableSegmentHandle, writableExists := l.SegmentHandles[l.WritableSegmentId]

	// create new segment handle if not exists
	if !writableExists {
		handle, err := l.createAndCacheWritableSegmentHandle(ctx, writerInvalidationNotifier)
		if err != nil {
			logger.Ctx(ctx).Warn("get or create writable segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
			return nil, err
		}
		return handle, nil
	}

	// Check if the writable segment handle needs to be rolling close and create a new one
	if l.shouldRollingCloseAndCreateWritableSegmentHandle(ctx, writeableSegmentHandle) {
		logger.Ctx(ctx).Debug("start to close segment",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.GetId()),
			zap.Int64("segmentId", writeableSegmentHandle.GetId(ctx)))
		// 1. close segmentHandle,
		//  it will send complete request to logStores
		//  and error out all pendingAppendOps with segmentCloseError
		logger.Ctx(ctx).Debug("mark segment rolling", zap.String("logName", l.Name), zap.Int64("segmentId", writeableSegmentHandle.GetId(ctx)))
		writeableSegmentHandle.SetRollingReady(ctx)

		// 2. create new segMeta(active)
		logger.Ctx(ctx).Debug("create new segment handle", zap.String("logName", l.Name))
		newSegmentHandle, err := l.createAndCacheWritableSegmentHandle(ctx, writerInvalidationNotifier)
		if err != nil {
			return nil, err
		}

		// 3. return new segmentHandle
		logger.Ctx(ctx).Debug("doAsyncRollingCloseAndCreateWritableSegmentHandleUnsafe success",
			zap.String("logName", l.Name),
			zap.Int64("oldSegmentId", writeableSegmentHandle.GetId(ctx)),
			zap.Int64("newSegmentId", newSegmentHandle.GetId(ctx)))
		return newSegmentHandle, nil
	}

	// return existing writable segment handle
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
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_recoverable_segment", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_recoverable_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
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

	l.Lock()
	defer l.Unlock()

	// get from cache directly
	readableSegmentHandle, exists := l.SegmentHandles[segmentId]
	if exists {
		// Update last access time
		return readableSegmentHandle, nil
	}
	// get segment meta and create handle
	segMeta, err := l.Metadata.GetSegmentMetadata(ctx, l.Name, segmentId)
	if err != nil && werr.ErrSegmentNotFound.Is(err) {
		logger.Ctx(ctx).Warn("get exists read only segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", segmentId), zap.Error(err))
		return nil, err
	}
	if err != nil {
		logger.Ctx(ctx).Warn("get exists read only segment handle failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Int64("segmentId", segmentId), zap.Error(err))
		return nil, err
	}
	if segMeta != nil {
		handle := segment.NewSegmentHandle(ctx, l.Id, l.Name, segMeta, l.Metadata, l.ClientPool, l.cfg, false)
		l.SegmentHandles[segmentId] = handle
		return handle, nil
	}
	return nil, nil
}

func (l *logHandleImpl) createAndCacheWritableSegmentHandle(ctx context.Context, writerInvalidationNotifier func(ctx context.Context, reason string)) (segment.SegmentHandle, error) {
	newSegMeta, err := l.createNewSegmentMeta(ctx)
	if err != nil {
		return nil, err
	}
	newSegHandle := segment.NewSegmentHandle(ctx, l.Id, l.Name, newSegMeta, l.Metadata, l.ClientPool, l.cfg, true)
	newSegHandle.SetWriterInvalidationNotifier(ctx, writerInvalidationNotifier)
	l.SegmentHandles[newSegMeta.Metadata.SegNo] = newSegHandle
	l.WritableSegmentId = newSegMeta.Metadata.SegNo
	l.lastRolloverTimeMs = newSegMeta.Metadata.CreateTime
	logger.Ctx(ctx).Debug("create and cache new SegmentHandle success", zap.String("logName", l.Name), zap.Int64("segmentId", newSegMeta.Metadata.SegNo))
	return newSegHandle, nil
}

// TODO It also depends on the number of segments that are rolling but not yet completed.
// If there are too many, it can no longer be rolled and we have to wait for a while
func (l *logHandleImpl) shouldRollingCloseAndCreateWritableSegmentHandle(ctx context.Context, segmentHandle segment.SegmentHandle) bool {
	if segmentHandle.IsForceRollingReady(ctx) {
		// force rolling is set
		return true
	}
	size := segmentHandle.GetSize(ctx)
	blocksCount := segmentHandle.GetBlocksCount(ctx)
	last := l.lastRolloverTimeMs
	return l.rollingPolicy.ShouldRollover(ctx, size, blocksCount, last)
}

func (l *logHandleImpl) createNewSegmentMeta(ctx context.Context) (*meta.SegmentMeta, error) {
	// construct new segment metadata
	segmentNo, err := l.GetNextSegmentId()
	if err != nil {
		return nil, err
	}
	newSegmentMetadata := &proto.SegmentMetadata{
		SegNo:       segmentNo,
		CreateTime:  time.Now().UnixMilli(),
		QuorumId:    -1,
		State:       proto.SegmentState_Active,
		LastEntryId: -1,
		Size:        0,
	}
	segMeta := &meta.SegmentMeta{
		Metadata: newSegmentMetadata,
		Revision: 0,
	}
	// create segment metadata
	err = l.Metadata.StoreSegmentMetadata(ctx, l.Name, segMeta)
	if err != nil {
		return nil, err
	}
	return segMeta, nil
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
		if maxSegNo < seg.Metadata.SegNo {
			maxSegNo = seg.Metadata.SegNo
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
		return nil, werr.ErrLogReaderTempInfoError.WithCauseErr(err)
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
		return werr.ErrLogHandleTruncateFailed.WithCauseErr(err)
	}

	// 2. Check if the requested truncation point is valid
	// If we're going backward, that's fine, but we should log a warning
	if logMeta.Metadata.TruncatedSegmentId > recordId.SegmentId ||
		(logMeta.Metadata.TruncatedSegmentId == recordId.SegmentId && logMeta.Metadata.TruncatedEntryId > recordId.EntryId) {
		logger.Ctx(ctx).Warn("Truncation point is behind current truncation position",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.Metadata.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.Metadata.TruncatedEntryId),
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
		return werr.ErrLogHandleTruncateFailed.WithCauseErr(err)
	}

	// 4. Validate entry ID is within valid range
	if (segMeta.Metadata.State == proto.SegmentState_Completed || segMeta.Metadata.State == proto.SegmentState_Sealed) &&
		(recordId.EntryId > segMeta.Metadata.LastEntryId || recordId.EntryId < 0) {
		logger.Ctx(ctx).Warn("Request truncation failed, entry ID invalid",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.Metadata.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.Metadata.TruncatedEntryId),
			zap.Int64("requestedTruncSegId", recordId.SegmentId),
			zap.Int64("requestedTruncEntryId", recordId.EntryId))
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "invalid_entry_id").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
		invalidErr := werr.ErrLogHandleTruncateFailed.WithCauseErrMsg(
			fmt.Sprintf("truncation entry ID %d exceeds last entry ID %d for segment %d",
				recordId.EntryId, segMeta.Metadata.LastEntryId, recordId.SegmentId))
		return invalidErr
	}

	// 5. Update LogMeta with new truncation point
	logMeta.Metadata.TruncatedSegmentId = recordId.SegmentId
	logMeta.Metadata.TruncatedEntryId = recordId.EntryId
	logMeta.Metadata.ModificationTimestamp = uint64(time.Now().Unix())

	// 6. Store the updated metadata
	err = l.Metadata.UpdateLogMeta(ctx, l.Name, logMeta)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "truncate", "update_error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "truncate", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("Request truncation failed",
			zap.String("logName", l.Name),
			zap.Int64("currentTruncSegId", logMeta.Metadata.TruncatedSegmentId),
			zap.Int64("currentTruncEntryId", logMeta.Metadata.TruncatedEntryId),
			zap.Int64("requestedTruncSegId", recordId.SegmentId),
			zap.Int64("requestedTruncEntryId", recordId.EntryId),
			zap.Error(err))
		return werr.ErrLogHandleTruncateFailed.WithCauseErr(err)
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
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("check segment truncated state failed", zap.String("logName", l.Name), zap.Int64("logId", l.Id), zap.Error(err))
		return werr.ErrLogHandleTruncateFailed.WithCauseErr(err)
	}

	// 2. Check if the requested truncation point is set
	if logMeta.Metadata.TruncatedSegmentId <= 0 {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil
	}

	// 3. Mark segments as truncated in metadata
	// Get all segments that are before the truncation point
	segments, err := l.GetSegments(ctx)
	if err != nil {
		metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "error").Inc()
		metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "check_segment_truncated", "error").Observe(float64(time.Since(start).Milliseconds()))
		return werr.ErrLogHandleTruncateFailed.WithCauseErr(err)
	}

	segmentsTruncated := 0
	for segId, segMetadata := range segments {
		// Skip segments at or after truncation point
		if segId > logMeta.Metadata.TruncatedSegmentId {
			continue
		}

		// Skip current truncation segment (we'll keep it, but read from the entry ID)
		if segId == logMeta.Metadata.TruncatedSegmentId {
			continue
		}

		if segMetadata.Metadata.State == proto.SegmentState_Truncated {
			// already truncated, skip
			continue
		}

		// Mark this segment as truncated
		segMetadata.Metadata.State = proto.SegmentState_Truncated
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

	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "check_segment_truncated", "success").Inc()
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
			if newPoint.SegmentId < truncatedId.SegmentId {
				newPoint.SegmentId = truncatedId.SegmentId
				newPoint.EntryId = 0
			}

			// If we're in the truncation segment but before truncation point, move to after it
			if newPoint.SegmentId == truncatedId.SegmentId && newPoint.EntryId <= truncatedId.EntryId {
				newPoint.EntryId = truncatedId.EntryId + 1
			}

			logger.Ctx(ctx).Info("Adjusting opening reader start position to after truncation point",
				zap.String("logName", l.GetName()),
				zap.String("readerName", readerName),
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
		return nil, werr.ErrLogHandleGetTruncationPointFailed.WithCauseErr(err)
	}

	// Return the truncation point
	metrics.WpLogHandleOperationsTotal.WithLabelValues(logIdStr, "get_truncated_record_id", "success").Inc()
	metrics.WpLogHandleOperationLatency.WithLabelValues(logIdStr, "get_truncated_record_id", "success").Observe(float64(time.Since(start).Milliseconds()))
	return &LogMessageId{
		SegmentId: logMeta.Metadata.TruncatedSegmentId,
		EntryId:   logMeta.Metadata.TruncatedEntryId,
	}, nil

}

func (l *logHandleImpl) CompleteAllActiveSegmentIfExists(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "Close")
	defer sp.End()
	l.Lock()
	defer l.Unlock()

	var lastError error
	// close all segment handles
	for _, segmentHandle := range l.SegmentHandles {
		err := segmentHandle.ForceCompleteAndClose(ctx)
		if err != nil {
			logger.Ctx(ctx).Info("Complete active segment failed when closing logHandle",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Int64("segId", segmentHandle.GetId(ctx)),
				zap.Error(err))
			if lastError == nil {
				lastError = err
			}
		}
	}

	// Clear the segment handles map to prevent memory leaks
	l.SegmentHandles = make(map[int64]segment.SegmentHandle)
	l.WritableSegmentId = -1

	return lastError
}

func (l *logHandleImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogHandleScopeName, "Close")
	defer sp.End()
	l.Lock()
	defer l.Unlock()

	// Stop background cleanup first
	l.stopBackgroundCleanup()

	// Cancel the context to signal shutdown
	l.cancel()

	var lastError error
	// close all segment handles
	for _, segmentHandle := range l.SegmentHandles {
		err := segmentHandle.ForceCompleteAndClose(ctx)
		if err != nil {
			logger.Ctx(ctx).Info("CompleteAndClose segment failed when closing logHandle",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Int64("segId", segmentHandle.GetId(ctx)),
				zap.Error(err))
			if lastError == nil {
				lastError = err
			}
		}
	}

	// Clear the segment handles map to prevent memory leaks
	l.SegmentHandles = make(map[int64]segment.SegmentHandle)
	l.WritableSegmentId = -1

	return lastError
}

// startBackgroundCleanup starts the background cleanup goroutine
func (l *logHandleImpl) startBackgroundCleanup() {
	logger.Ctx(l.ctx).Info("Starting background segment handle cleanup goroutine",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id))

	l.cleanupWg.Add(1)
	go l.backgroundCleanupLoop()
}

// stopBackgroundCleanup stops the background cleanup goroutine and waits for it to finish
func (l *logHandleImpl) stopBackgroundCleanup() {
	// Check if cleanup is already stopped
	select {
	case <-l.cleanupDone:
		// Already closed, just return
		logger.Ctx(l.ctx).Debug("Background segment handle cleanup already stopped",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.Id))
		return
	default:
		// Channel is still open, proceed with shutdown
	}

	logger.Ctx(l.ctx).Info("Stopping background segment handle cleanup goroutine",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id))

	close(l.cleanupDone)
	l.cleanupWg.Wait()

	logger.Ctx(l.ctx).Info("Background segment handle cleanup goroutine stopped",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id))
}

// backgroundCleanupLoop runs the background cleanup logic
func (l *logHandleImpl) backgroundCleanupLoop() {
	defer l.cleanupWg.Done()

	// Cleanup configuration
	const cleanupInterval = 30 * time.Second // How often to check for cleanup
	const maxIdleTime = 1 * time.Minute      // How long a segment can be idle before cleanup

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	logger.Ctx(l.ctx).Info("Background segment handle cleanup goroutine started",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id),
		zap.Duration("cleanupInterval", cleanupInterval),
		zap.Duration("maxIdleTime", maxIdleTime))

	for {
		select {
		case <-l.cleanupDone:
			logger.Ctx(l.ctx).Info("Background segment handle cleanup goroutine received shutdown signal",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id))
			return
		case <-l.ctx.Done():
			logger.Ctx(l.ctx).Info("Background segment handle cleanup goroutine context cancelled",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id))
			return
		case <-ticker.C:
			l.performBackgroundCleanup(maxIdleTime)
		}
	}
}

// performBackgroundCleanup performs the actual cleanup logic
func (l *logHandleImpl) performBackgroundCleanup(maxIdleTime time.Duration) {
	l.Lock()
	defer l.Unlock()

	totalHandles := len(l.SegmentHandles)

	logger.Ctx(l.ctx).Debug("Starting background segment handle cleanup cycle",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id),
		zap.Int("totalHandles", totalHandles),
		zap.Duration("maxIdleTime", maxIdleTime))

	l.cleanupIdleSegmentHandlesUnsafe(l.ctx, maxIdleTime)
}

// cleanupIdleSegmentHandlesUnsafe removes segment handles that haven't been accessed for the specified duration
// This method should be called while holding the write lock
func (l *logHandleImpl) cleanupIdleSegmentHandlesUnsafe(ctx context.Context, maxIdleTime time.Duration) {
	now := time.Now()
	var toRemove []int64

	logger.Ctx(ctx).Debug("Scanning for idle segment handles to cleanup",
		zap.String("logName", l.Name),
		zap.Int64("logId", l.Id),
		zap.Duration("maxIdleTime", maxIdleTime),
		zap.Int("totalHandles", len(l.SegmentHandles)))

	// Check each segment for cleanup eligibility
	for segmentId, handle := range l.SegmentHandles {
		// Always protect the current writable segment
		if segmentId == l.WritableSegmentId {
			continue
		}

		// Get last access time from segment handle
		lastAccessTimeMs := handle.GetLastAccessTime()
		lastAccessTime := time.UnixMilli(lastAccessTimeMs)

		// Check if idle time exceeds threshold
		if now.Sub(lastAccessTime) > maxIdleTime {
			toRemove = append(toRemove, segmentId)
		}
	}

	// Perform cleanup
	for _, segmentId := range toRemove {
		if handle, exists := l.SegmentHandles[segmentId]; exists {
			// Close the segment handle
			_ = handle.ForceCompleteAndClose(ctx)

			// Remove from map
			delete(l.SegmentHandles, segmentId)

			logger.Ctx(ctx).Debug("cleaned up idle segment handle",
				zap.String("logName", l.Name),
				zap.Int64("logId", l.Id),
				zap.Int64("segmentId", segmentId))
		}
	}

	if len(toRemove) > 0 {
		logger.Ctx(ctx).Info("Idle segment handle cleanup completed",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.Id),
			zap.Int("cleanedCount", len(toRemove)),
			zap.Int("remainingHandles", len(l.SegmentHandles)))
	} else {
		logger.Ctx(ctx).Debug("Idle segment handle cleanup completed - no handles cleaned",
			zap.String("logName", l.Name),
			zap.Int64("logId", l.Id),
			zap.Int("totalHandles", len(l.SegmentHandles)))
	}
}
