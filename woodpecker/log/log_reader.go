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
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

const (
	ReaderScopeName            = "LogReader"
	UpdateReaderInfoIntervalMs = 30000
	NoDataReadWaitIntervalMs   = 200
	DefaultBatchLimit          = 200
)

//go:generate mockery --dir=./woodpecker/log --name=LogReader --structname=LogReader --output=mocks/mocks_woodpecker/mocks_log_handle --filename=mock_log_reader.go --with-expecter=true  --outpkg=mocks_log_handle
type LogReader interface {
	// ReadNext reads the next log message from the log, blocking until a message
	// is available. It returns the log message and an error if any occurs.
	ReadNext(context.Context) (*LogMessage, error)
	// Close closes the log reader.
	// It returns an error if any occurs during the closing process.
	Close(context.Context) error
	// GetName returns the name of this reader.
	GetName() string
}

var _ LogReader = (*logBatchReaderImpl)(nil)

// An efficient reader that loads fragments and yields elements one by one during traversal.
type logBatchReaderImpl struct {
	logName    string
	logId      int64
	logIdStr   string // for metrics label only
	logHandle  LogHandle
	from       *LogMessageId
	readerName string

	pendingReadSegmentId int64
	pendingReadEntryId   int64
	currentSegmentHandle segment.SegmentHandle
	batch                *processor.BatchData
	next                 int
	lastRead             int64
}

func NewLogBatchReader(ctx context.Context, logHandle LogHandle, segmentHandle segment.SegmentHandle, from *LogMessageId, readerName string, cfg *config.Configuration) (LogReader, error) {
	return &logBatchReaderImpl{
		logName:              logHandle.GetName(),
		logId:                logHandle.GetId(),
		logIdStr:             fmt.Sprintf("%d", logHandle.GetId()),
		logHandle:            logHandle,
		from:                 from,
		currentSegmentHandle: segmentHandle,
		pendingReadSegmentId: from.SegmentId,
		pendingReadEntryId:   from.EntryId,
		readerName:           readerName,
		batch:                nil,
		next:                 0,
		lastRead:             time.Now().UnixMilli(),
	}, nil
}

// ReadNext reads the next log message from the log, blocking until a message is returned.
// The main logic is: for each segment, it retrieves batches and caches them locally, then returns them one by one.
// Whether a segment is fully read is determined only when the segment EOF is encountered, at which point the next segment is read.
// If there is an error reading the current segment or an entry is not found, it indicates that the segment may not have ended,
// and it will wait and try to read again.
func (l *logBatchReaderImpl) ReadNext(ctx context.Context) (*LogMessage, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ReaderScopeName, "ReadNext")
	defer sp.End()
	start := time.Now()

	if l.logHandle == nil {
		metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, werr.ErrInternalError.WithCauseErrMsg("log handle is not initialized")
	}

	for {
		// Check if context is done
		select {
		case <-ctx.Done():
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "cancel").Observe(float64(time.Since(start).Milliseconds()))
			return nil, ctx.Err()
		default:
			// Continue with read operation
		}

		// get if cache fragment exists
		if l.batch != nil && l.next < len(l.batch.Entries) {
			readEntryData := l.batch.Entries[l.next]
			logMsg, err := UnmarshalMessage(readEntryData.Data)
			if err != nil {
				metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
				logger.Ctx(ctx).Warn("read one entry error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", readEntryData.SegmentId), zap.Int64("entryId", readEntryData.EntryId), zap.Int("cacheBatchSize", len(l.batch.Entries)), zap.Int("readIndex", l.next), zap.Error(err))
				// clear cache
				l.batch = nil
				l.next = 0
				return nil, werr.ErrLogReaderReadFailed.WithCauseErr(err)
			}
			logger.Ctx(ctx).Debug("read one message complete", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Int64("actualReadSegmentId", readEntryData.SegmentId), zap.Int64("actualReadEntryId", readEntryData.EntryId), zap.Int("cacheBatchSize", len(l.batch.Entries)), zap.Int("readIndex", l.next))
			logMsg.Id = &LogMessageId{
				SegmentId: readEntryData.SegmentId,
				EntryId:   readEntryData.EntryId,
			}
			l.pendingReadEntryId += 1
			l.next += 1
			metrics.WpLogReaderBytesRead.WithLabelValues(l.logIdStr, l.readerName).Add(float64(len(readEntryData.Data)))
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "success").Observe(float64(time.Since(start).Milliseconds()))
			return logMsg, nil
		}

		// try get next read point
		segHandle, segId, entryId, err := l.getNextSegHandleAndIDs(ctx)
		logger.Ctx(ctx).Debug("get next segment handle and ids", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Int64("actualReadSegmentId", segId), zap.Int64("actualReadEntryId", entryId), zap.Error(err))
		if err != nil && werr.ErrSegmentNotFound.Is(err) {
			// segment not found, wait and try again
			time.Sleep(NoDataReadWaitIntervalMs * time.Millisecond)
			continue
		}
		if err != nil {
			// A segment reading error is returned to the application caller, who should decide whether to attempt the call again
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			return nil, werr.ErrLogReaderReadFailed.WithCauseErr(err)
		}

		if segId > l.pendingReadSegmentId || l.lastRead+UpdateReaderInfoIntervalMs > time.Now().UnixMilli() {
			// update reader info
			updateReaderErr := l.logHandle.GetMetadataProvider().UpdateReaderTempInfo(ctx, l.logId, l.readerName, segId, entryId)
			if updateReaderErr != nil {
				logger.Ctx(ctx).Warn("update reader info failed", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("nextReadSegmentId", segId), zap.Error(updateReaderErr))
			}
		}
		if segHandle == nil {
			l.pendingReadSegmentId = segId
			l.pendingReadEntryId = entryId
			logger.Ctx(ctx).Debug("no segment to read, wait and retry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", segId), zap.Int64("pendingReadEntryId", entryId))
			// Use a ticker for backoff with context timeout support
			ticker := time.NewTicker(NoDataReadWaitIntervalMs * time.Millisecond)
			select {
			case <-ticker.C:
				ticker.Stop()
				continue
			case <-ctx.Done():
				ticker.Stop()
				metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "cancel").Observe(float64(time.Since(start).Milliseconds()))
				return nil, ctx.Err()
			}
		}

		// read next batch
		var lastReadState *processor.LastReadState
		if l.batch != nil && l.batch.ReadState != nil && l.batch.ReadState.SegmentId == segId {
			lastReadState = l.batch.ReadState
		}
		batchResult, err := segHandle.ReadBatchAdv(ctx, entryId, DefaultBatchLimit, lastReadState)
		if err != nil {
			// Check if it's end of file error - this is the only reliable way to know segment is finished
			if werr.ErrFileReaderEndOfFile.Is(err) {
				logger.Ctx(ctx).Debug("segment reached end of file, move to next segment", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId))
				l.pendingReadSegmentId = segId + 1
				l.pendingReadEntryId = 0
				continue
			}

			// If entry not found, wait and retry until EOF
			if werr.ErrEntryNotFound.Is(err) {
				// just wait and retry
				logger.Ctx(ctx).Debug("segment has no entry to read, wait and retry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId))
				// Use a ticker for backoff with context timeout support
				ticker := time.NewTicker(NoDataReadWaitIntervalMs * time.Millisecond)
				select {
				case <-ticker.C:
					ticker.Stop()
					continue
				case <-ctx.Done():
					ticker.Stop()
					metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "cancel").Observe(float64(time.Since(start).Milliseconds()))
					return nil, ctx.Err()
				}
			}

			// For other errors, return them directly
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			logger.Ctx(ctx).Warn("read entries error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId), zap.Error(err))
			return nil, werr.ErrLogReaderReadFailed.WithCauseErr(err)
		}

		if len(batchResult.Entries) < 1 {
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			readErr := werr.ErrLogReaderReadFailed.WithCauseErrMsg(fmt.Sprintf("should read at lease one entry, but got %d", len(batchResult.Entries)))
			logger.Ctx(ctx).Warn("read batch entries error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId), zap.Error(readErr))
			return nil, readErr
		}

		// update batch
		l.batch = batchResult
		l.next = 0
		// get one entry
		oneEntry := l.batch.Entries[l.next]
		logMsg, err := UnmarshalMessage(oneEntry.Data)
		if err != nil {
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			logger.Ctx(ctx).Warn("read one entry error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId), zap.Error(err))
			return nil, werr.ErrLogReaderReadFailed.WithCauseErr(err)
		}
		logger.Ctx(ctx).Debug("read one message complete", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("actualReadSegmentId", segId), zap.Int64("actualReadEntryId", entryId), zap.Int("newBatchSize", len(l.batch.Entries)), zap.Int("readIndex", l.next), zap.Any("readState", l.batch.ReadState))
		logMsg.Id = &LogMessageId{
			SegmentId: segId,
			EntryId:   entryId,
		}
		// move cursor
		l.currentSegmentHandle = segHandle
		l.pendingReadSegmentId = oneEntry.SegmentId
		l.pendingReadEntryId = oneEntry.EntryId + 1
		l.next += 1

		// update metrics
		metrics.WpLogReaderBytesRead.WithLabelValues(l.logIdStr, l.readerName).Add(float64(len(oneEntry.Data)))
		metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "success").Observe(float64(time.Since(start).Milliseconds()))
		return logMsg, nil
	}
}

func (l *logBatchReaderImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ReaderScopeName, "Close")
	defer sp.End()
	start := time.Now()

	err := l.logHandle.GetMetadataProvider().DeleteReaderTempInfo(ctx, l.logHandle.GetId(), l.readerName)
	status := "success"
	if err != nil {
		logger.Ctx(ctx).Warn("delete reader info failed",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.String("readerName", l.readerName),
			zap.Error(err))
		status = "error"
	}

	metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "close", status).Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (l *logBatchReaderImpl) getNextSegHandleAndIDs(ctx context.Context) (segment.SegmentHandle, int64, int64, error) {
	// Get latest segment ID first
	nextSegmentId, err := l.logHandle.GetNextSegmentId()
	latestSegmentId := nextSegmentId - 1
	if err != nil {
		logger.Ctx(ctx).Warn("get next segment id error",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.String("readerName", l.readerName),
			zap.Error(err))
		return nil, -1, -1, err
	}

	// Case 1: Tail read - if pending segment ID is the latest and the first time to read
	if l.pendingReadSegmentId == LatestLogMessageID().SegmentId {
		return l.handleTailRead(ctx, latestSegmentId)
	}

	// Case 2: Check if current exists segment handle contains the target entry
	if l.currentSegmentHandle != nil && l.currentSegmentHandle.GetId(context.Background()) == l.pendingReadSegmentId {
		if l.isEntryInCurrentSegment(ctx) {
			return l.currentSegmentHandle, l.pendingReadSegmentId, l.pendingReadEntryId, nil
		}
	}

	// Case 3: Find next readable segment
	return l.findNextReadableSegment(ctx, latestSegmentId)
}

// handleTailRead handles the case where we want to read from the latest active segment
func (l *logBatchReaderImpl) handleTailRead(ctx context.Context, latestSegmentId int64) (segment.SegmentHandle, int64, int64, error) {
	// Try to get the last active segment (latestSegmentId - 1)
	lastSegmentId := latestSegmentId
	if lastSegmentId < 0 {
		// No segments exist yet, wait for the first segment
		logger.Ctx(ctx).Debug("no segments exist yet, tail read from first segment",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.String("readerName", l.readerName))
		l.pendingReadSegmentId = 0
		l.pendingReadEntryId = 0
		return nil, -1, -1, werr.ErrSegmentNotFound.WithCauseErrMsg("no segments exists yet")
	}

	segHandle, err := l.logHandle.GetExistsReadonlySegmentHandle(context.Background(), lastSegmentId)
	if err != nil && werr.ErrSegmentNotFound.Is(err) {
		// Last segment doesn't exist, wait for future segment
		logger.Ctx(ctx).Debug("the segment doesn't exist yet, tail read from it's first entry",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.Int64("lastSegmentId", lastSegmentId),
			zap.String("readerName", l.readerName))
		l.pendingReadSegmentId = lastSegmentId
		l.pendingReadEntryId = 0
		return nil, -1, -1, werr.ErrSegmentNotFound.WithCauseErrMsg("last segment doesn't exist yet")
	}

	if err != nil {
		logger.Ctx(ctx).Warn("get last segment handle error",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.Int64("lastSegmentId", lastSegmentId),
			zap.String("readerName", l.readerName),
			zap.Error(err))
		return nil, -1, -1, err
	}

	// get  last confirmed entry ID of the last segment
	lastConfirmedId, err := segHandle.GetLastAddConfirmed(context.Background())
	if err != nil {
		if werr.ErrFileReaderNoBlockFound.Is(err) {
			// No entries in the segment yet, it is a new active segment,start to read it's first entry
			logger.Ctx(ctx).Debug("tail read from last active segment first entry",
				zap.String("logName", l.logName),
				zap.Int64("logId", l.logId),
				zap.Int64("segmentId", segHandle.GetId(ctx)),
				zap.String("readerName", l.readerName))
			l.pendingReadSegmentId = segHandle.GetId(context.Background())
			l.pendingReadEntryId = 0
			return segHandle, segHandle.GetId(context.Background()), 0, nil
		}
		logger.Ctx(ctx).Warn("get segment LAC failed",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.Int64("segmentId", segHandle.GetId(ctx)),
			zap.String("readerName", l.readerName),
			zap.Error(err))
		return nil, -1, -1, err
	}

	logger.Ctx(ctx).Debug("tail read from last active segment lac",
		zap.String("logName", l.logName),
		zap.Int64("logId", l.logId),
		zap.Int64("segmentId", segHandle.GetId(ctx)),
		zap.Int64("lastConfirmedId", lastConfirmedId),
		zap.String("readerName", l.readerName))
	l.pendingReadSegmentId = segHandle.GetId(context.Background())
	l.pendingReadEntryId = lastConfirmedId + 1
	return segHandle, segHandle.GetId(context.Background()), lastConfirmedId + 1, nil
}

// isEntryInCurrentSegment checks if the pending entry is within the current segment's range
func (l *logBatchReaderImpl) isEntryInCurrentSegment(ctx context.Context) bool {
	m := l.currentSegmentHandle.GetMetadata(context.Background())

	// For completed segments, fast check if entry ID is within range
	if m.Metadata.State != proto.SegmentState_Active {
		if m.Metadata.LastEntryId >= l.pendingReadEntryId {
			logger.Ctx(ctx).Debug("current completed segment contains the pending read entry",
				zap.String("logName", l.logName),
				zap.Int64("logId", l.logId),
				zap.Int64("segmentId", l.currentSegmentHandle.GetId(ctx)),
				zap.Int64("lastEntryId", m.Metadata.LastEntryId),
				zap.String("readerName", l.readerName),
				zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
			return true
		}
		return false
	}

	// For active segments, always try to read the segment
	logger.Ctx(ctx).Debug("current active segment, try to read",
		zap.String("logName", l.logName),
		zap.Int64("logId", l.logId),
		zap.Int64("segmentId", l.currentSegmentHandle.GetId(ctx)),
		zap.String("readerName", l.readerName),
		zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
	return true
}

// findNextReadableSegment finds the next segment that can be read from
func (l *logBatchReaderImpl) findNextReadableSegment(ctx context.Context, latestSegmentId int64) (segment.SegmentHandle, int64, int64, error) {
	// Start searching from the next segment (entry ID will be 0)
	nextSegmentId := l.pendingReadSegmentId
	nextEntryId := l.pendingReadEntryId

	for nextSegmentId <= latestSegmentId {
		logger.Ctx(ctx).Debug("trying to find next segment",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.Int64("nextSegmentId", nextSegmentId),
			zap.String("readerName", l.readerName))

		segHandle, err := l.logHandle.GetExistsReadonlySegmentHandle(context.Background(), nextSegmentId)
		if err != nil && !werr.ErrSegmentNotFound.Is(err) {
			logger.Ctx(ctx).Warn("get segment handle error",
				zap.String("logName", l.logName),
				zap.Int64("logId", l.logId),
				zap.Int64("segmentId", nextSegmentId),
				zap.String("readerName", l.readerName),
				zap.Error(err))
			return nil, -1, -1, err
		}

		if err != nil && werr.ErrSegmentNotFound.Is(err) {
			// Segment doesn't exist, try next one
			nextSegmentId++
			continue
		}

		// assert segHandle != nil
		if segHandle != nil {
			m := segHandle.GetMetadata(context.Background())

			// Skip truncated segments
			if m.Metadata.State == proto.SegmentState_Truncated {
				logger.Ctx(ctx).Debug("skip truncated segment",
					zap.String("logName", l.logName),
					zap.Int64("logId", l.logId),
					zap.Int64("segmentId", segHandle.GetId(ctx)),
					zap.String("readerName", l.readerName))
				nextSegmentId++
				continue
			}

			// Found a readable segment (completed or active)
			logger.Ctx(ctx).Debug("found readable segment",
				zap.String("logName", l.logName),
				zap.Int64("logId", l.logId),
				zap.Int64("segmentId", segHandle.GetId(ctx)),
				zap.String("state", m.Metadata.State.String()),
				zap.String("readerName", l.readerName))
			if nextSegmentId > l.pendingReadSegmentId {
				// move to nextSegment's first entryId
				nextEntryId = 0
			}
			return segHandle, nextSegmentId, nextEntryId, nil
		}

		// Move to next segment
		nextSegmentId++
	}

	// No existing segment found, wait for future segment
	logger.Ctx(ctx).Debug("no existing segment found, wait for future segment",
		zap.String("logName", l.logName),
		zap.Int64("logId", l.logId),
		zap.Int64("latestSegmentId", latestSegmentId),
		zap.String("readerName", l.readerName))
	return nil, -1, -1, werr.ErrSegmentNotFound.WithCauseErrMsg("no existing readable segment found")
}

func (l *logBatchReaderImpl) GetName() string {
	return l.readerName
}
