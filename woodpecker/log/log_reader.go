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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
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

func NewLogReader(ctx context.Context, logHandle LogHandle, segmentHandle segment.SegmentHandle, from *LogMessageId, readerName string) LogReader {
	return &logReaderImpl{
		logName:              logHandle.GetName(),
		logId:                logHandle.GetId(),
		logIdStr:             fmt.Sprintf("%d", logHandle.GetId()),
		logHandle:            logHandle,
		from:                 from,
		currentSegmentHandle: segmentHandle,
		pendingReadSegmentId: from.SegmentId,
		pendingReadEntryId:   from.EntryId,
		readerName:           readerName,
	}
}

var _ LogReader = (*logReaderImpl)(nil)

type logReaderImpl struct {
	logName              string
	logId                int64
	logIdStr             string // for metrics label only
	logHandle            LogHandle
	from                 *LogMessageId
	readerName           string
	pendingReadSegmentId int64
	pendingReadEntryId   int64
	currentSegmentHandle segment.SegmentHandle
}

func (l *logReaderImpl) ReadNext(ctx context.Context) (*LogMessage, error) {
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
			return nil, werr.ErrSegmentReadException.WithCauseErr(ctx.Err())
		default:
			// Continue with read operation
		}
		segHandle, segId, entryId, err := l.getNextSegHandleAndIDs(ctx)
		logger.Ctx(ctx).Debug("get next segment handle and ids",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.String("readerName", l.readerName),
			zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId),
			zap.Int64("pendingReadEntryId", l.pendingReadEntryId),
			zap.Int64("actualReadSegmentId", segId),
			zap.Int64("actualReadEntryId", entryId),
			zap.Error(err))
		if err != nil {
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			return nil, werr.ErrSegmentReadException.WithCauseErr(err)
		}
		if segId > l.pendingReadSegmentId {
			// update reader info
			updateReaderErr := l.logHandle.GetMetadataProvider().UpdateReaderTempInfo(ctx, l.logId, l.readerName, segId, entryId)
			if updateReaderErr != nil {
				logger.Ctx(ctx).Warn("update reader info failed",
					zap.String("logName", l.logName),
					zap.Int64("logId", l.logId),
					zap.String("readerName", l.readerName),
					zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId),
					zap.Int64("nextReadSegmentId", segId),
					zap.Error(updateReaderErr))
			}
		}
		if segHandle == nil {
			l.pendingReadSegmentId = segId
			l.pendingReadEntryId = entryId
			logger.Ctx(ctx).Debug("no segment to read, sleep 200ms.",
				zap.String("logName", l.logName),
				zap.Int64("logId", l.logId),
				zap.String("readerName", l.readerName),
				zap.Int64("pendingReadSegmentId", segId),
				zap.Int64("pendingReadEntryId", entryId))
			// Use a ticker for backoff with context timeout support
			ticker := time.NewTicker(200 * time.Millisecond)
			select {
			case <-ticker.C:
				ticker.Stop()
				continue
			case <-ctx.Done():
				ticker.Stop()
				metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "cancel").Observe(float64(time.Since(start).Milliseconds()))
				return nil, werr.ErrSegmentReadException.WithCauseErr(ctx.Err())
			}
		}
		readTimestamp := time.Now().UnixMilli()
		stateBeforeRead := segHandle.GetMetadata(ctx).State
		entries, err := segHandle.Read(ctx, entryId, entryId)
		if err != nil && werr.ErrEntryNotFound.Is(err) {
			// 1) if the segmentHandle is completed, move to next segment's first entry
			refreshMetaErr := segHandle.RefreshAndGetMetadata(ctx)
			if refreshMetaErr != nil && errors.IsAny(refreshMetaErr, context.Canceled, context.DeadlineExceeded) {
				metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
				return nil, refreshMetaErr
			}
			segMeta := segHandle.GetMetadata(ctx)
			stateAfterRead := segMeta.State
			if stateAfterRead != proto.SegmentState_Active {
				// if it is too close, it would be read empty, so check completion time with a little margin
				if segMeta.CompletionTime+200 < readTimestamp && stateBeforeRead == stateAfterRead {
					// safely move to next segment
					logger.Ctx(ctx).Debug("segment is completed, move to next segment's first entry.",
						zap.String("logName", l.logName),
						zap.Int64("logId", l.logId),
						zap.String("readerName", l.readerName),
						zap.Int64("pendingReadSegmentId", segId),
						zap.Int64("pendingReadEntryId", entryId),
						zap.Int64("moveToReadSegmentId", segId+1),
						zap.Int64("completionTimestamp", segMeta.CompletionTime),
						zap.Int64("readTimestamp", readTimestamp),
						zap.String("stateBefore", stateBeforeRead.String()),
						zap.String("stateAfter", stateAfterRead.String()))
					l.pendingReadSegmentId = segId + 1
					l.pendingReadEntryId = 0
					continue
				} else {
					// maybe new entry flush after this read time, so retry again
					logger.Ctx(ctx).Debug("segment is completed after this read timestamp, maybe entries flush after this read timestamp, try read again",
						zap.String("logName", l.logName),
						zap.Int64("logId", l.logId),
						zap.String("readerName", l.readerName),
						zap.Int64("pendingReadSegmentId", segId),
						zap.Int64("pendingReadEntryId", entryId),
						zap.Int64("completionTimestamp", segMeta.CompletionTime),
						zap.Int64("readTimestamp", readTimestamp))
					continue
				}
			}
			// 2) if the segmentHandle is in-progress, just wait and read again
			// 2.1) if next segment exists, indicate that current segment is completed in fact
			nextSegExists, checkErr := l.logHandle.GetMetadataProvider().CheckSegmentExists(ctx, l.logHandle.GetName(), segId+1)
			if checkErr != nil && errors.IsAny(checkErr, context.Canceled, context.DeadlineExceeded) {
				metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
				return nil, checkErr
			}
			if checkErr == nil && nextSegExists {
				logger.Ctx(ctx).Debug("read no entry from current segment, and the next segment already exists, move to next segment's first entry.",
					zap.String("logName", l.logName),
					zap.Int64("logId", l.logId),
					zap.String("readerName", l.readerName),
					zap.Int64("pendingReadSegmentId", segId),
					zap.Int64("pendingReadEntryId", entryId),
					zap.Int64("moveToReadSegmentId", segId+1))
				l.pendingReadSegmentId = segId + 1
				l.pendingReadEntryId = 0
				continue
			}
			// 2.2) if no next segment exists, just wait and read again
			logger.Ctx(ctx).Debug("no entry to read, wait with timeout.",
				zap.String("logName", l.logName),
				zap.Int64("logId", l.logId),
				zap.String("readerName", l.readerName),
				zap.Int64("pendingReadSegmentId", segId),
				zap.Int64("pendingReadEntryId", entryId),
				zap.Error(checkErr))

			// Use a ticker for backoff with context timeout support
			ticker := time.NewTicker(200 * time.Millisecond)
			select {
			case <-ticker.C:
				ticker.Stop()
				continue
			case <-ctx.Done():
				ticker.Stop()
				metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "cancel").Observe(float64(time.Since(start).Milliseconds()))
				return nil, werr.ErrSegmentReadException.WithCauseErr(ctx.Err())
			}
		}
		if err != nil {
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			logger.Ctx(ctx).Warn("read one entry error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId), zap.Error(err))
			return nil, werr.ErrSegmentReadException.WithCauseErr(err)
		}
		if len(entries) != 1 {
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			readErr := werr.ErrSegmentReadException.WithCauseErrMsg(fmt.Sprintf("should read one entry, but got %d", len(entries)))
			logger.Ctx(ctx).Warn("read one entry error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId), zap.Error(readErr))
			return nil, readErr
		}

		l.pendingReadSegmentId = entries[0].SegmentId
		l.pendingReadEntryId = entries[0].EntryId + 1
		l.currentSegmentHandle = segHandle
		logMsg, err := UnmarshalMessage(entries[0].Data)
		if err != nil {
			metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "error").Observe(float64(time.Since(start).Milliseconds()))
			logger.Ctx(ctx).Warn("read one entry error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segId), zap.Int64("entryId", entryId), zap.Error(err))
			return nil, werr.ErrSegmentReadException.WithCauseErr(err)
		}
		logger.Ctx(ctx).Debug("read one message complete",
			zap.String("logName", l.logName),
			zap.Int64("logId", l.logId),
			zap.String("readerName", l.readerName),
			zap.Int64("actualReadSegmentId", segId),
			zap.Int64("actualReadEntryId", entryId))
		logMsg.Id = &LogMessageId{
			SegmentId: segId,
			EntryId:   entryId,
		}

		// Track read bytes
		if len(entries[0].Data) > 0 {
			metrics.WpLogReaderBytesRead.WithLabelValues(l.logIdStr, l.readerName).Add(float64(len(entries[0].Data)))
		}

		// Track read latency
		metrics.WpLogReaderOperationLatency.WithLabelValues(l.logIdStr, "read_next", "success").Observe(float64(time.Since(start).Milliseconds()))
		return logMsg, nil
	}
}

func (l *logReaderImpl) Close(ctx context.Context) error {
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

func (l *logReaderImpl) getNextSegHandleAndIDs(ctx context.Context) (segment.SegmentHandle, int64, int64, error) {
	// if current segmentHandle is the same as pendingSegmentId, check if the segmentHandle is completed
	if l.currentSegmentHandle != nil && l.currentSegmentHandle.GetId(context.Background()) == l.pendingReadSegmentId {
		m := l.currentSegmentHandle.GetMetadata(context.Background())
		// current completed segmentHandle
		if m.LastEntryId > l.pendingReadEntryId {
			logger.Ctx(ctx).Debug("current segment contains the pending read entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", l.currentSegmentHandle.GetId(ctx)), zap.Int64("lastEntryId", m.LastEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
			return l.currentSegmentHandle, l.pendingReadSegmentId, l.pendingReadEntryId, nil
		}
		// current in-progress segmentHandle
		if m.LastEntryId == -1 {
			// Read from the segment directly because the segmentHandle is not yet completed.
			// The writing entryId could be any value greater than pendingReadEntryId.
			logger.Ctx(ctx).Debug("current segment is in-progress, maybe contains the pending read entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", l.currentSegmentHandle.GetId(ctx)), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
			return l.currentSegmentHandle, l.pendingReadSegmentId, l.pendingReadEntryId, nil
		}
	}
	// if pendingSegId is future segmentId, just return
	latestSegmentId, err := l.logHandle.GetNextSegmentId()
	if err != nil {
		logger.Ctx(ctx).Warn("get next segment id error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Error(err))
		return nil, -1, -1, err
	}
	// if pendingSegId is latest segmentId, return the latest active segmentHandle
	if l.pendingReadSegmentId == LatestLogMessageID().SegmentId {
		segHandle, err := l.logHandle.GetExistsReadonlySegmentHandle(context.Background(), latestSegmentId-1)
		if err != nil && werr.ErrSegmentNotFound.Is(err) {
			logger.Ctx(ctx).Debug("get exists readonly segment handle empty", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", latestSegmentId-1), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
			return nil, latestSegmentId, 0, nil
		}
		// if the latest segmentHandle is active, return it
		if segHandle.GetMetadata(context.Background()).State == proto.SegmentState_Active {
			latestEntryId, err := segHandle.GetLastAddConfirmed(context.Background())
			if err != nil {
				logger.Ctx(ctx).Warn("get segment LAC failed", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segHandle.GetId(ctx)), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Error(err))
				return nil, -1, -1, err
			}
			logger.Ctx(ctx).Debug("move to read latest in-progress segment next entry after LAC", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segHandle.GetId(ctx)), zap.Int64("latestEntryId", latestEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
			return segHandle, segHandle.GetId(context.Background()), latestEntryId + 1, nil
		}
		// otherwise return the next segment id, that segment will be the active segment in the future
		logger.Ctx(ctx).Debug("move to read latest segment first entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", latestSegmentId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
		return nil, latestSegmentId, 0, nil
	} else if l.pendingReadSegmentId >= latestSegmentId {
		logger.Ctx(ctx).Debug("move to read future segment first entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", latestSegmentId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
		return nil, latestSegmentId, 0, nil
	}
	// otherwise, move to next exits segment
	nextSegmentId := l.pendingReadSegmentId
	nextEntryId := l.pendingReadEntryId
	for {
		logger.Ctx(ctx).Debug("start to find next entry to read", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("nextSegmentId", nextSegmentId), zap.Int64("nextEntryId", nextEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
		segHandle, err := l.logHandle.GetExistsReadonlySegmentHandle(context.Background(), nextSegmentId)
		if err != nil && !werr.ErrSegmentNotFound.Is(err) {
			logger.Ctx(ctx).Warn("get exists readonly segment handle error", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", nextSegmentId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Error(err))
			return nil, -1, -1, err
		}
		if err != nil && werr.ErrSegmentNotFound.Is(err) {
			logger.Ctx(ctx).Debug("get exists readonly segment handle not found", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", nextSegmentId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Error(err))
		}
		if segHandle != nil {
			// Skip truncated segments
			m := segHandle.GetMetadata(context.Background())
			if m.State == proto.SegmentState_Truncated {
				nextSegmentId++
				nextEntryId = 0
				logger.Ctx(ctx).Debug("skip reading the truncated segment,continue to find next entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segHandle.GetId(ctx)), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
				continue
			}

			// current completed segmentHandle
			if m.LastEntryId >= nextEntryId {
				logger.Ctx(ctx).Debug("find a segment contains pending read entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segHandle.GetId(ctx)), zap.Int64("lastEntryId", m.LastEntryId), zap.Int64("nextSegmentId", nextSegmentId), zap.Int64("nextEntryId", nextEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
				return segHandle, nextSegmentId, nextEntryId, nil
			}
			// current in-progress segmentHandle
			if m.LastEntryId == -1 {
				// Read from the segment directly because the segmentHandle is not yet completed.
				// The writing entryId could be any value greater than pendingReadEntryId.
				logger.Ctx(ctx).Debug("find a segment in-progress may contains pending read entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segHandle.GetId(ctx)), zap.Int64("lastEntryId", m.LastEntryId), zap.Int64("nextSegmentId", nextSegmentId), zap.Int64("nextEntryId", nextEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
				return segHandle, nextSegmentId, nextEntryId, nil
			}
			logger.Ctx(ctx).Debug("skip segment due to this segment not contains expected entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("segmentId", segHandle.GetId(ctx)), zap.Int64("lastEntryId", m.LastEntryId), zap.String("segState", m.State.String()), zap.Int64("nextSegmentId", nextSegmentId), zap.Int64("nextEntryId", nextEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
		} else if nextSegmentId >= latestSegmentId {
			// no more exists segment
			logger.Ctx(ctx).Debug("no more segment to read", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("latestSegmentId", latestSegmentId), zap.Int64("nextSegmentId", nextSegmentId), zap.Int64("nextEntryId", nextEntryId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
			break
		}

		// move to next segment
		nextSegmentId += 1
		nextEntryId = 0 // reset entryId=0 as we are reading from the next segment
	}

	// move to next future segment, if no exists segment to read
	nextSegmentId, err = l.logHandle.GetNextSegmentId()
	if err != nil {
		logger.Ctx(ctx).Warn("get next segment id failed", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId), zap.Error(err))
		return nil, -1, -1, err
	}
	logger.Ctx(ctx).Debug("move to read future segment entry", zap.String("logName", l.logName), zap.Int64("logId", l.logId), zap.Int64("nextSegmentId", nextSegmentId), zap.String("readerName", l.readerName), zap.Int64("pendingReadSegmentId", l.pendingReadSegmentId), zap.Int64("pendingReadEntryId", l.pendingReadEntryId))
	return nil, nextSegmentId, 0, nil
}

func (l *logReaderImpl) GetName() string {
	return l.readerName
}
