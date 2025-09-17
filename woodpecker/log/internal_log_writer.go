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
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

const (
	InternalWriterScopeName = "InternalLogWriter"
)

var _ LogWriter = (*internalLogWriterImpl)(nil)

// An implementation of a writer without locks and sessions,
// which can optimize unnecessary lock and session management in scenarios,
// where the application caller controls that only one writer is writing.
type internalLogWriterImpl struct {
	sync.RWMutex
	logIdStr           string // for metrics label only
	logHandle          LogHandle
	auditorMaxInterval int
	cfg                *config.Configuration
	writerClose        chan struct{}
	cleanupManager     segment.SegmentCleanupManager

	// validation related fields
	isWriterValid       atomic.Bool
	onWriterInvalidated func(ctx context.Context, reason string)

	// Mutex to ensure only one truncation cleanup task is running at a time
	cleanupMutex      sync.Mutex
	cleanupInProgress bool
}

func NewInternalLogWriter(ctx context.Context, logHandle LogHandle, cfg *config.Configuration) LogWriter {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "NewInternalLogWriter")
	defer sp.End()
	w := &internalLogWriterImpl{
		logIdStr:           fmt.Sprintf("%d", logHandle.GetId()),
		logHandle:          logHandle,
		auditorMaxInterval: cfg.Woodpecker.Client.Auditor.MaxInterval,
		cfg:                cfg,
		writerClose:        make(chan struct{}, 1),
		cleanupManager:     segment.NewSegmentCleanupManager(logHandle.GetMetadataProvider(), logHandle.(*logHandleImpl).ClientPool),
	}
	// Set sessionValid to true
	w.isWriterValid.Store(true)

	// Set trigger expired
	onWriterInvalidated := func(ctx context.Context, reason string) {
		w.isWriterValid.Store(false)
		logger.Ctx(ctx).Warn("trigger writer lock session expired", zap.String("logName", logHandle.GetName()), zap.Int64("logId", logHandle.GetId()), zap.String("reason", reason))
	}
	w.onWriterInvalidated = onWriterInvalidated

	// Monitor keepAlive channel
	go w.runAuditor()
	logger.Ctx(ctx).Debug("log writer created", zap.String("logName", logHandle.GetName()), zap.Int64("logId", logHandle.GetId()))
	return w
}

func (l *internalLogWriterImpl) Write(ctx context.Context, msg *WriteMessage) *WriteResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "Write")
	defer sp.End()
	start := time.Now()

	// Check if session is valid
	if !l.isWriterValid.Load() {
		logger.Ctx(ctx).Warn("Writer lock session has expired",
			zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "error").Observe(float64(time.Since(start).Milliseconds()))
		return &WriteResult{
			LogMessageId: nil,
			Err:          werr.ErrLogWriterLockLost.WithCauseErrMsg("writer lock session has expired"),
		}
	}

	ch := make(chan *WriteResult, 1)
	callback := func(segmentId int64, entryId int64, err error) {
		logger.Ctx(ctx).Debug("write log entry callback",
			zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("segId", segmentId), zap.Int64("entryId", entryId), zap.Error(err))
		ch <- &WriteResult{
			LogMessageId: &LogMessageId{
				SegmentId: segmentId,
				EntryId:   entryId,
			},
			Err: err,
		}
		close(ch)
		// trigger writer expired to make this writer not writable, application should reopen a new writer to write
		if err != nil && (werr.ErrSegmentFenced.Is(err) || werr.ErrStorageNotWritable.Is(err) || werr.ErrFileWriterFinalized.Is(err) || werr.ErrFileWriterAlreadyClosed.Is(err)) {
			l.onWriterInvalidated(ctx, fmt.Sprintf("err:%s on:%d%d", err.Error(), segmentId, entryId))
		}
	}
	writableSegmentHandle, err := l.logHandle.GetOrCreateWritableSegmentHandle(ctx, l.onWriterInvalidated)
	if err != nil {
		callback(-1, -1, err)
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "error").Observe(float64(time.Since(start).Milliseconds()))
		return <-ch
	}
	bytes, err := MarshalMessage(msg)
	if err != nil {
		logger.Ctx(ctx).Warn("serialize message failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "error").Observe(float64(time.Since(start).Milliseconds()))
		return &WriteResult{
			LogMessageId: nil,
			Err:          err,
		}
	}

	writableSegmentHandle.AppendAsync(ctx, bytes, callback)
	result := <-ch

	// Update metrics based on result
	if result.Err != nil {
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "error").Observe(float64(time.Since(start).Milliseconds()))
		if werr.ErrSegmentHandleSegmentRolling.Is(result.Err) {
			logger.Ctx(ctx).Info("write to rolling segment rejected, retry later", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.String("detail", result.Err.Error()))
		} else {
			logger.Ctx(ctx).Warn("write log entry failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(result.Err))
		}
	} else {
		metrics.WpLogWriterBytesWritten.WithLabelValues(l.logIdStr).Add(float64(len(bytes)))
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "success").Observe(float64(time.Since(start).Milliseconds()))
	}

	return result
}

func (l *internalLogWriterImpl) WriteAsync(ctx context.Context, msg *WriteMessage) <-chan *WriteResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "WriteAsync")
	defer sp.End()
	start := time.Now()
	ch := make(chan *WriteResult, 1)

	// Check if session is valid
	if !l.isWriterValid.Load() {
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write_async", "error").Observe(float64(time.Since(start).Milliseconds()))
		ch <- &WriteResult{
			LogMessageId: nil,
			Err:          werr.ErrLogWriterLockLost.WithCauseErrMsg("writer lock session has expired"),
		}
		close(ch)
		logger.Ctx(ctx).Warn("Writer lock session has expired", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))
		return ch
	}

	bytes, err := MarshalMessage(msg)
	if err != nil {
		logger.Ctx(ctx).Warn("serialize message failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write_async", "error").Observe(float64(time.Since(start).Milliseconds()))
		ch <- &WriteResult{
			LogMessageId: nil,
			Err:          err,
		}
		close(ch)
		return ch
	}

	callback := func(segmentId int64, entryId int64, err error) {
		logger.Ctx(ctx).Debug("write log entry callback exec", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("segId", segmentId), zap.Int64("entryId", entryId), zap.Error(err))
		if err == nil {
			metrics.WpLogWriterBytesWritten.WithLabelValues(l.logIdStr).Add(float64(len(bytes)))
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write_async", "success").Observe(float64(time.Since(start).Milliseconds()))
		} else {
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write_async", "error").Observe(float64(time.Since(start).Milliseconds()))
		}
		ch <- &WriteResult{
			LogMessageId: &LogMessageId{
				SegmentId: segmentId,
				EntryId:   entryId,
			},
			Err: err,
		}
		close(ch)
		// trigger writer expired to make this writer not writable, application should reopen a new writer to write
		if err != nil && (werr.ErrSegmentFenced.Is(err) || werr.ErrStorageNotWritable.Is(err) || werr.ErrFileWriterFinalized.Is(err) || werr.ErrFileWriterAlreadyClosed.Is(err)) {
			l.onWriterInvalidated(ctx, fmt.Sprintf("err:%s on:%d%d", err.Error(), segmentId, entryId))
		}
	}
	writableSegmentHandle, err := l.logHandle.GetOrCreateWritableSegmentHandle(ctx, l.onWriterInvalidated)
	if err != nil {
		logger.Ctx(ctx).Warn("get writable segment failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
		callback(-1, -1, err)
		return ch
	}

	writableSegmentHandle.AppendAsync(ctx, bytes, callback)
	return ch
}

func (l *internalLogWriterImpl) runAuditor() {
	ticker := time.NewTicker(time.Duration(l.auditorMaxInterval * int(time.Second)))
	defer ticker.Stop()

	logger.Ctx(context.Background()).Info("Log auditor started",
		zap.String("logName", l.logHandle.GetName()),
		zap.Int64("logId", l.logHandle.GetId()),
		zap.Int("intervalSeconds", l.auditorMaxInterval))

	for {
		select {
		case <-ticker.C:
			ctx, sp := logger.NewIntentCtx(WriterScopeName, fmt.Sprintf("auditor_%d", l.logHandle.GetId()))
			startAudit := time.Now()

			logger.Ctx(ctx).Info("Starting auditor cycle",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("logId", l.logHandle.GetId()))

			// check and set segment truncate state if necessary
			if err := l.logHandle.CheckAndSetSegmentTruncatedIfNeed(ctx); err != nil {
				logger.Ctx(ctx).Warn("check and set segment truncated failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
				sp.End()
				continue
			}

			// get all current segments meta
			segmentMetaList, err := l.logHandle.GetSegments(ctx)
			if err != nil {
				logger.Ctx(ctx).Warn("get log segments failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
				sp.End()
				continue
			}

			logger.Ctx(ctx).Info("Auditor loaded segment metadata",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("logId", l.logHandle.GetId()),
				zap.Int("totalSegments", len(segmentMetaList)))

			// compact/recover if necessary
			truncatedSegmentExists := make([]int64, 0)
			segmentsProcessed := 0
			segmentsCompacted := 0
			segmentsFailed := 0

			for _, seg := range segmentMetaList {
				stateBefore := seg.Metadata.State
				if stateBefore == proto.SegmentState_Completed {
					segmentsProcessed++
					recoverySegmentHandle, getRecoverySegmentHandleErr := l.logHandle.GetRecoverableSegmentHandle(context.TODO(), seg.Metadata.SegNo)
					if getRecoverySegmentHandleErr != nil {
						logger.Ctx(ctx).Warn("get log segment failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("segId", seg.Metadata.SegNo), zap.Error(getRecoverySegmentHandleErr))
						segmentsFailed++
						continue
					}
					maintainErr := recoverySegmentHandle.Compact(context.TODO())
					if maintainErr != nil {
						logger.Ctx(ctx).Warn("auditor maintain the log segment failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("segId", seg.Metadata.SegNo), zap.Error(maintainErr))
						segmentsFailed++
						continue
					}

					// Check if segment was recovered or compacted by checking its new state
					// This is a best-effort attempt to track the operation type
					if stateBefore == proto.SegmentState_Completed {
						segmentsCompacted++
						logger.Ctx(ctx).Info("Successfully compacted segment",
							zap.String("logName", l.logHandle.GetName()),
							zap.Int64("logId", l.logHandle.GetId()),
							zap.Int64("segmentId", seg.Metadata.SegNo))
					}
				} else if stateBefore == proto.SegmentState_Truncated {
					truncatedSegmentExists = append(truncatedSegmentExists, seg.Metadata.SegNo)
				}
			}

			logger.Ctx(ctx).Info("Auditor segment processing completed",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("logId", l.logHandle.GetId()),
				zap.Int("segmentsProcessed", segmentsProcessed),
				zap.Int("segmentsCompacted", segmentsCompacted),
				zap.Int("segmentsFailed", segmentsFailed),
				zap.Int("truncatedSegments", len(truncatedSegmentExists)))

			// Check for truncated segments to clean up
			if len(truncatedSegmentExists) > 0 {
				logger.Ctx(ctx).Info("auditor try to clean up truncated segments", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64s("truncatedSegmentExists", truncatedSegmentExists))
				l.cleanupTruncatedSegmentsIfNecessary(ctx)
			}

			auditDuration := time.Since(startAudit)
			logger.Ctx(ctx).Info("Auditor cycle completed",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("logId", l.logHandle.GetId()),
				zap.Duration("duration", auditDuration),
				zap.Int("totalSegments", len(segmentMetaList)),
				zap.Int("truncatedSegments", len(truncatedSegmentExists)))

			sp.End()

			// Track auditor latency
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "auditor_run", "success").Observe(float64(auditDuration.Milliseconds()))
		case <-l.writerClose:
			logger.Ctx(context.TODO()).Info("Log auditor stopped",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("logId", l.logHandle.GetId()))
			return
		}
	}
}

// cleanupTruncatedSegmentsIfNecessary checks for truncated segments that can be cleaned up
// It identifies segments that are truncated and not being read by any reader
func (l *internalLogWriterImpl) cleanupTruncatedSegmentsIfNecessary(ctx context.Context) {
	// Get log Id/Name
	logId := l.logHandle.GetId()
	logName := l.logHandle.GetName()

	logger.Ctx(ctx).Info("Starting truncated segments cleanup preparation",
		zap.String("logName", logName),
		zap.Int64("logId", logId))

	// Ensure only one cleanup task runs at a time
	l.cleanupMutex.Lock()
	if l.cleanupInProgress {
		l.cleanupMutex.Unlock()
		logger.Ctx(ctx).Info("Truncation cleanup already in progress, skipping", zap.String("logName", logName), zap.Int64("logId", logId))
		return
	}

	l.cleanupInProgress = true
	l.cleanupMutex.Unlock()

	// Ensure we mark the task as complete when we're done
	defer func() {
		l.cleanupMutex.Lock()
		l.cleanupInProgress = false
		l.cleanupMutex.Unlock()
		logger.Ctx(ctx).Info("Truncated segments cleanup task completed",
			zap.String("logName", logName),
			zap.Int64("logId", logId))
	}()

	// Get the truncation point
	truncatedRecordId, err := l.logHandle.GetTruncatedRecordId(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get truncation point during cleanup preparation", zap.String("logName", logName), zap.Int64("logId", logId), zap.Error(err))
		return
	}

	// Check if truncation has been performed
	if truncatedRecordId.SegmentId < 0 || truncatedRecordId.EntryId < 0 {
		logger.Ctx(ctx).Debug("No truncation point set yet, skipping cleanup", zap.String("logName", logName), zap.Int64("logId", logId))
		return
	}

	logger.Ctx(ctx).Info("Found truncation point",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("truncatedSegmentId", truncatedRecordId.SegmentId),
		zap.Int64("truncatedEntryId", truncatedRecordId.EntryId))

	// Get all reader information for this log
	readers, err := l.logHandle.GetMetadataProvider().GetAllReaderTempInfoForLog(ctx, logId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get reader information during cleanup preparation", zap.String("logName", logName), zap.Int64("logId", logId), zap.Error(err))
		return
	}

	// Get all segments for this log
	segments, err := l.logHandle.GetSegments(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segments during cleanup preparation", zap.String("logName", logName), zap.Int64("logId", logId), zap.Error(err))
		return
	}

	logger.Ctx(ctx).Info("Loaded cleanup analysis data",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int("activeReaders", len(readers)),
		zap.Int("totalSegments", len(segments)))

	// get min segment id in use
	minTruncatedSegmentId := truncatedRecordId.SegmentId
	for _, reader := range readers {
		if reader.RecentReadSegmentId < minTruncatedSegmentId {
			minTruncatedSegmentId = reader.RecentReadSegmentId
		}
	}

	logger.Ctx(ctx).Info("Calculated minimum segment ID in use",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("minSegmentIdInUse", minTruncatedSegmentId),
		zap.Int64("originalTruncationPoint", truncatedRecordId.SegmentId))

	// Find all segments that are truncated and not needed by any reader
	// Process segments in ascending order by segment ID
	var segmentIdsToClean []int64
	truncatedSegmentCount := 0
	protectedSegmentCount := 0

	for segId, segMeta := range segments {
		// Only consider segments that are truncated
		if segMeta.Metadata.State != proto.SegmentState_Truncated {
			continue
		}

		truncatedSegmentCount++

		// Skip segments that are at or after the minTruncatedSegmentId point
		if segId >= minTruncatedSegmentId {
			logger.Ctx(ctx).Debug("Skipping truncated segment still in use by readers",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segId))
			protectedSegmentCount++
			continue
		}

		// This segment is eligible for cleanup
		segmentIdsToClean = append(segmentIdsToClean, segId)
		logger.Ctx(ctx).Info("Found truncated segment eligible for cleanup",
			zap.String("logName", logName),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segId))
	}

	logger.Ctx(ctx).Info("Cleanup eligibility analysis completed",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int("totalTruncatedSegments", truncatedSegmentCount),
		zap.Int("segmentsEligibleForCleanup", len(segmentIdsToClean)),
		zap.Int("segmentsProtectedByReaders", protectedSegmentCount))

	if len(segmentIdsToClean) == 0 {
		logger.Ctx(ctx).Info("No truncated segments eligible for cleanup",
			zap.String("logName", logName),
			zap.Int64("logId", logId))
		return
	}

	// Sort segments to clean in ascending order
	sort.Slice(segmentIdsToClean, func(i, j int) bool {
		return segmentIdsToClean[i] < segmentIdsToClean[j]
	})

	logger.Ctx(ctx).Info("Identified truncated segments eligible for cleanup",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int("count", len(segmentIdsToClean)),
		zap.Int64s("segmentIds", segmentIdsToClean))

	// Start concurrent cleanup of all eligible segments
	cleanupStartTime := time.Now()
	successCount := 0
	failureCount := 0

	for _, segmentId := range segmentIdsToClean {
		logger.Ctx(ctx).Info("Start segment cleanup",
			zap.String("logName", logName),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId))
		start := time.Now()
		err := l.cleanupManager.CleanupSegment(ctx, logName, logId, segmentId)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to start segment cleanup",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId),
				zap.Error(err))
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "cleanup_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
			failureCount++
		} else {
			logger.Ctx(ctx).Info("Finish segment cleanup",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId))
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "cleanup_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
			successCount++
		}
	}

	cleanupDuration := time.Since(cleanupStartTime)
	logger.Ctx(ctx).Info("Truncated segments cleanup execution completed",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int("totalSegmentsProcessed", len(segmentIdsToClean)),
		zap.Int("successfulCleanups", successCount),
		zap.Int("failedCleanups", failureCount),
		zap.Duration("totalCleanupDuration", cleanupDuration))
}

func (l *internalLogWriterImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "Close")
	defer sp.End()
	start := time.Now()
	logger.Ctx(ctx).Info("closing log writer", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))

	l.isWriterValid.Store(false)
	l.writerClose <- struct{}{}
	close(l.writerClose)
	status := "success"
	closeErr := l.logHandle.CompleteAllActiveSegmentIfExists(ctx)
	if closeErr != nil {
		logger.Ctx(ctx).Warn("close log writer failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(closeErr))
		status = "error"
		if werr.ErrSegmentNotFound.Is(closeErr) || werr.ErrSegmentProcessorNoWriter.Is(closeErr) {
			closeErr = nil
			status = "success"
		}
	}
	closeLogHandleErr := l.logHandle.Close(ctx)
	if closeLogHandleErr != nil {
		logger.Ctx(ctx).Warn(fmt.Sprintf("failed to close log handle of the writer for logName:%s", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))
		status = "error"
	}

	logger.Ctx(ctx).Info("log writer closed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))
	metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "close", status).Observe(float64(time.Since(start).Milliseconds()))
	return werr.Combine(closeErr, closeLogHandleErr)
}
