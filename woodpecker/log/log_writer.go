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

	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

const (
	WriterScopeName = "LogWriter"
)

//go:generate mockery --dir=./woodpecker/log --name=LogWriter --structname=LogWriter --output=mocks/mocks_woodpecker/mocks_log_handle --filename=mock_log_writer.go --with-expecter=true  --outpkg=mocks_log_handle
type LogWriter interface {
	// Write writes a log message synchronously and returns a WriteResult.
	// It takes a context and a byte slice representing the log message.
	Write(context.Context, *WriterMessage) *WriteResult

	// WriteAsync writes a log message asynchronously and returns a channel that will receive a WriteResult.
	// It takes a context and a byte slice representing the log message.
	WriteAsync(context.Context, *WriterMessage) <-chan *WriteResult

	// Close closes the log writer.
	// It takes a context and returns an error if any occurs.
	Close(context.Context) error
}

func NewLogWriter(ctx context.Context, logHandle LogHandle, cfg *config.Configuration, session *concurrency.Session) LogWriter {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "NewLogWriter")
	defer sp.End()
	w := &logWriterImpl{
		logIdStr:           fmt.Sprintf("%d", logHandle.GetId()),
		logHandle:          logHandle,
		auditorMaxInterval: cfg.Woodpecker.Client.Auditor.MaxInterval,
		cfg:                cfg,
		writerClose:        make(chan struct{}, 1),
		cleanupManager:     segment.NewSegmentCleanupManager(logHandle.GetMetadataProvider(), logHandle.(*logHandleImpl).ClientPool),
		session:            session,
	}

	// Set sessionValid to true
	w.sessionValid.Store(true)

	// Monitor keepAlive channel
	go w.monitorSession()
	go w.runAuditor()
	logger.Ctx(ctx).Debug("log writer created", zap.String("logName", logHandle.GetName()), zap.Int64("logId", logHandle.GetId()), zap.Int64("sessionId", int64(session.Lease())))
	return w
}

var _ LogWriter = (*logWriterImpl)(nil)

type logWriterImpl struct {
	sync.RWMutex
	logIdStr           string // for metrics label only
	logHandle          LogHandle
	auditorMaxInterval int
	cfg                *config.Configuration
	writerClose        chan struct{}
	cleanupManager     segment.SegmentCleanupManager

	// Session related fields
	session      *concurrency.Session
	sessionValid atomic.Bool

	// Mutex to ensure only one truncation cleanup task is running at a time
	cleanupMutex      sync.Mutex
	cleanupInProgress bool
}

func (l *logWriterImpl) monitorSession() {
	for {
		select {
		case <-l.session.Done():
			// Channel is closed, session is invalid
			l.sessionValid.Store(false)
			logger.Ctx(context.Background()).Warn("Writer lock session has expired",
				zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("sessionId", int64(l.session.Lease())))
			return
		case <-l.writerClose:
			l.sessionValid.Store(false)
			logger.Ctx(context.Background()).Debug("Monitor session end due to writer close",
				zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("sessionId", int64(l.session.Lease())))
			return
		}
	}
}

func (l *logWriterImpl) Write(ctx context.Context, msg *WriterMessage) *WriteResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "Write")
	defer sp.End()
	start := time.Now()

	// Check if session is valid
	if !l.sessionValid.Load() {
		logger.Ctx(ctx).Warn("Writer lock session has expired",
			zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("sessionId", int64(l.session.Lease())))
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "error").Observe(float64(time.Since(start).Milliseconds()))
		return &WriteResult{
			LogMessageId: nil,
			Err:          werr.ErrWriterLockLost.WithCauseErrMsg("writer lock session has expired"),
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
	}
	writableSegmentHandle, err := l.logHandle.GetOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		callback(-1, -1, err)
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "error").Observe(float64(time.Since(start).Milliseconds()))
		return <-ch
	}
	bytes, err := MarshalMessage(msg)
	if err != nil {
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
		logger.Ctx(ctx).Warn("write log entry failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(result.Err))
	} else {
		metrics.WpLogWriterBytesWritten.WithLabelValues(l.logIdStr).Add(float64(len(bytes)))
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write", "success").Observe(float64(time.Since(start).Milliseconds()))
	}

	return result
}

func (l *logWriterImpl) WriteAsync(ctx context.Context, msg *WriterMessage) <-chan *WriteResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "WriteAsync")
	defer sp.End()
	start := time.Now()
	l.Lock()
	defer l.Unlock()

	ch := make(chan *WriteResult, 1)

	// Check if session is valid
	if !l.sessionValid.Load() {
		metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "write_async", "error").Observe(float64(time.Since(start).Milliseconds()))
		ch <- &WriteResult{
			LogMessageId: nil,
			Err:          werr.ErrWriterLockLost.WithCauseErrMsg("writer lock session has expired"),
		}
		close(ch)
		logger.Ctx(ctx).Warn("Writer lock session has expired", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("sessionId", int64(l.session.Lease())))
		return ch
	}

	bytes, err := MarshalMessage(msg)
	if err != nil {
		logger.Ctx(ctx).Warn("encode msg failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
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
	}
	writableSegmentHandle, err := l.logHandle.GetOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("get writable segment failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
		callback(-1, -1, err)
		return ch
	}

	writableSegmentHandle.AppendAsync(ctx, bytes, callback)
	return ch
}

func (l *logWriterImpl) runAuditor() {
	ticker := time.NewTicker(time.Duration(l.auditorMaxInterval * int(time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, sp := logger.NewIntentCtx(WriterScopeName, fmt.Sprintf("auditor_%d", l.logHandle.GetId()))
			startAudit := time.Now()
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
			nextSegId, err := l.logHandle.GetNextSegmentId()
			if err != nil {
				logger.Ctx(ctx).Warn("get next segment id failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(err))
				sp.End()
				continue
			}

			// compact/recover if necessary
			truncatedSegmentExists := make([]int64, 0)
			for _, seg := range segmentMetaList {
				if seg.SegNo >= nextSegId-2 {
					// last segment maybe in-progress, no need to recover it
					continue
				}
				stateBefore := seg.State
				if stateBefore == proto.SegmentState_Completed || stateBefore == proto.SegmentState_Active {
					recoverySegmentHandle, getRecoverySegmentHandleErr := l.logHandle.GetRecoverableSegmentHandle(context.TODO(), seg.SegNo)
					if getRecoverySegmentHandleErr != nil {
						logger.Ctx(ctx).Warn("get log segment failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("segId", seg.SegNo), zap.Error(getRecoverySegmentHandleErr))
						continue
					}
					maintainErr := recoverySegmentHandle.RecoveryOrCompact(context.TODO())
					if err != nil {
						logger.Ctx(ctx).Warn("auditor maintain the log segment failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64("segId", seg.SegNo), zap.Error(maintainErr))
						continue
					}
				} else if stateBefore == proto.SegmentState_Truncated {
					truncatedSegmentExists = append(truncatedSegmentExists, seg.SegNo)
				}
			}

			// Check for truncated segments to clean up
			if len(truncatedSegmentExists) > 0 {
				logger.Ctx(ctx).Info("auditor try to clean up truncated segments", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Int64s("truncatedSegmentExists", truncatedSegmentExists))
				l.cleanupTruncatedSegmentsIfNecessary(ctx)
			}
			sp.End()

			// Track auditor latency
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "auditor_run", "success").Observe(float64(time.Since(startAudit).Milliseconds()))
		case <-l.writerClose:
			logger.Ctx(context.TODO()).Debug("log writer stop", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))
			return
		}
	}
}

// cleanupTruncatedSegmentsIfNecessary checks for truncated segments that can be cleaned up
// It identifies segments that are truncated and not being read by any reader
func (l *logWriterImpl) cleanupTruncatedSegmentsIfNecessary(ctx context.Context) {
	// Get log Id/Name
	logId := l.logHandle.GetId()
	logName := l.logHandle.GetName()

	// Ensure only one cleanup task runs at a time
	l.cleanupMutex.Lock()
	if l.cleanupInProgress {
		l.cleanupMutex.Unlock()
		logger.Ctx(ctx).Debug("Truncation cleanup already in progress, skipping", zap.String("logName", logName), zap.Int64("logId", logId))
		return
	}

	l.cleanupInProgress = true
	l.cleanupMutex.Unlock()

	// Ensure we mark the task as complete when we're done
	defer func() {
		l.cleanupMutex.Lock()
		l.cleanupInProgress = false
		l.cleanupMutex.Unlock()
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

	// get min segment id in use
	minTruncatedSegmentId := truncatedRecordId.SegmentId
	for _, reader := range readers {
		if reader.RecentReadSegmentId < minTruncatedSegmentId {
			minTruncatedSegmentId = reader.RecentReadSegmentId
		}
	}

	// Find all segments that are truncated and not needed by any reader
	// Process segments in ascending order by segment ID
	var segmentIdsToClean []int64
	for segId, segMeta := range segments {
		// Only consider segments that are truncated
		if segMeta.State != proto.SegmentState_Truncated {
			continue
		}

		// Skip segments that are at or after the minTruncatedSegmentId point
		if segId >= minTruncatedSegmentId {
			logger.Ctx(ctx).Debug("Skipping truncated segment still in use by readers",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segId))
			continue
		}

		// This segment is eligible for cleanup
		segmentIdsToClean = append(segmentIdsToClean, segId)
		logger.Ctx(ctx).Debug("Found truncated segment eligible for cleanup",
			zap.String("logName", logName),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segId))
	}

	if len(segmentIdsToClean) == 0 {
		logger.Ctx(ctx).Debug("No truncated segments eligible for cleanup",
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
		} else {
			logger.Ctx(ctx).Info("Finish segment cleanup",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId))
			metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "cleanup_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
		}
	}
}

func (l *logWriterImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScopeName, "Close")
	defer sp.End()
	start := time.Now()

	l.writerClose <- struct{}{}
	close(l.writerClose)
	status := "success"
	closeErr := l.logHandle.CloseAndCompleteCurrentWritableSegment(ctx)
	if closeErr != nil {
		logger.Ctx(ctx).Warn("close log writer failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()), zap.Error(closeErr))
		status = "error"
		if werr.ErrSegmentNotFound.Is(closeErr) {
			closeErr = nil
			status = "success"
		}
	}
	releaseLockErr := l.logHandle.GetMetadataProvider().ReleaseLogWriterLock(ctx, l.logHandle.GetName())
	if releaseLockErr != nil {
		logger.Ctx(ctx).Warn(fmt.Sprintf("failed to release log writer lock for logName:%s", l.logHandle.GetName()), zap.Int64("logId", l.logHandle.GetId()))
		status = "error"
	}

	metrics.WpLogWriterOperationLatency.WithLabelValues(l.logIdStr, "close", status).Observe(float64(time.Since(start).Milliseconds()))
	return werr.Combine(closeErr, releaseLockErr)
}

// GetWriterSessionForTest For Test only
func (l *logWriterImpl) GetWriterSessionForTest() *concurrency.Session {
	return l.session
}

type WriteResult struct {
	LogMessageId *LogMessageId
	Err          error
}
