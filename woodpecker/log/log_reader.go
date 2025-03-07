package log

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

type LogReader interface {
	// ReadNext reads the next log message from the log, blocking until a message
	// is available. It returns the log message and an error if any occurs.
	ReadNext(context.Context) (*LogMessage, error)
	// Close closes the log reader.
	// It returns an error if any occurs during the closing process.
	Close(context.Context) error
}

func NewLogReader(ctx context.Context, logHandle LogHandle, segmentHandle segment.SegmentHandle, from *LogMessageId) LogReader {
	return &logReaderImpl{
		logHandle:            logHandle,
		from:                 from,
		currentSegmentHandle: segmentHandle,
		pendingReadSegmentId: from.SegmentId,
		pendingReadEntryId:   from.EntryId,
	}
}

var _ LogReader = (*logReaderImpl)(nil)

type logReaderImpl struct {
	logHandle LogHandle
	from      *LogMessageId

	pendingReadSegmentId int64
	pendingReadEntryId   int64
	currentSegmentHandle segment.SegmentHandle
}

func (l *logReaderImpl) ReadNext(ctx context.Context) (*LogMessage, error) {
	if l.logHandle == nil {
		return nil, werr.ErrInternalError.WithCauseErrMsg("log handle is not initialized")
	}

	for {
		segHandle, segId, entryId, err := l.getNextSegHandleAndIDs()
		if err != nil {
			return nil, werr.ErrSegmentReadException.WithCauseErr(err)
		}
		if segHandle == nil {
			l.pendingReadSegmentId = segId
			l.pendingReadEntryId = entryId
			logger.Ctx(ctx).Debug("no segment to read, sleep 200ms.",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("pendingReadSegmentId", segId),
				zap.Int64("pendingReadEntryId", entryId))
			// TODO sleep backoff sleep(200ms)
			time.Sleep(1000 * time.Millisecond) // TODO config
			continue
		}
		entries, err := segHandle.Read(ctx, entryId, entryId)
		if err != nil && werr.ErrEntryNotFound.Is(err) {
			// 1) if the segmentHandle is completed, move to next segment's first entry
			refreshMetaErr := segHandle.RefreshAndGetMetadata(ctx)
			if refreshMetaErr != nil && errors.IsAny(refreshMetaErr, context.Canceled, context.DeadlineExceeded) {
				return nil, refreshMetaErr
			}
			if segHandle.GetMetadata(ctx).State != proto.SegmentState_Active {
				l.pendingReadSegmentId = segId + 1
				l.pendingReadEntryId = 0
				continue
			}
			// 2) if the segmentHandle is in-progress, just wait and read again
			// 2.1) if next segment exists, indicate that current segment is completed in fact
			nextSegExists, checkErr := l.logHandle.GetMetadataProvider().CheckSegmentExists(ctx, l.logHandle.GetName(), segId+1)
			if checkErr != nil && errors.IsAny(checkErr, context.Canceled, context.DeadlineExceeded) {
				return nil, checkErr
			}
			if checkErr == nil && nextSegExists {
				l.pendingReadSegmentId = segId + 1
				l.pendingReadEntryId = 0
				continue
			}
			// 2.2) if no next segment exists, just wait and read again
			logger.Ctx(ctx).Debug("no entry to read, sleep 1000ms.",
				zap.String("logName", l.logHandle.GetName()),
				zap.Int64("pendingReadSegmentId", segId),
				zap.Int64("pendingReadEntryId", entryId),
				zap.Error(checkErr))
			time.Sleep(1000 * time.Millisecond) // TODO config
			continue
		}
		if err != nil {
			return nil, werr.ErrSegmentReadException.WithCauseErr(err)
		}
		if len(entries) != 1 {
			return nil, werr.ErrSegmentReadException.WithCauseErrMsg(fmt.Sprintf("should read one entry, but got %d", len(entries)))
		}

		l.pendingReadSegmentId = entries[0].SegmentId
		l.pendingReadEntryId = entries[0].EntryId + 1
		l.currentSegmentHandle = segHandle
		logMsg, err := UnmarshalMessage(entries[0].Data)
		if err != nil {
			return nil, werr.ErrSegmentReadException.WithCauseErr(err)
		}
		logMsg.Id = &LogMessageId{
			SegmentId: segId,
			EntryId:   entryId,
		}
		return logMsg, nil
	}
}

func (l *logReaderImpl) Close(ctx context.Context) error {
	// NO-OP
	return nil
}

func (l *logReaderImpl) getNextSegHandleAndIDs() (segment.SegmentHandle, int64, int64, error) {
	if l.currentSegmentHandle != nil && l.currentSegmentHandle.GetId(context.Background()) == l.pendingReadSegmentId {
		m := l.currentSegmentHandle.GetMetadata(context.Background())
		// current completed segmentHandle
		if m.LastEntryId > l.pendingReadEntryId {
			return l.currentSegmentHandle, l.pendingReadSegmentId, l.pendingReadEntryId, nil
		}
		// current in-progress segmentHandle
		if m.LastEntryId == -1 {
			// Read from the segment directly because the segmentHandle is not yet completed.
			// The writing entryId could be any value greater than pendingReadEntryId.
			return l.currentSegmentHandle, l.pendingReadSegmentId, l.pendingReadEntryId, nil
		}
	}
	// if pendingSegId is future segmentId, just return
	latestSegmentId, err := l.logHandle.GetNextSegmentId()
	if err != nil {
		return nil, -1, -1, err
	}
	// if pendingSegId is latest segmentId, return the latest active segmentHandle
	if l.pendingReadSegmentId == LatestLogMessageID().SegmentId {
		segHandle, err := l.logHandle.GetExistsReadonlySegmentHandle(context.Background(), latestSegmentId-1)
		if err != nil && werr.ErrSegmentNotFound.Is(err) {
			return nil, latestSegmentId, 0, nil
		}
		// if the latest segmentHandle is active, return it
		if segHandle.GetMetadata(context.Background()).State == proto.SegmentState_Active {
			latestEntryId, err := segHandle.GetLastAddConfirmed(context.Background())
			if err != nil {
				return nil, -1, -1, err
			}
			return segHandle, segHandle.GetId(context.Background()), latestEntryId + 1, nil
		}
		// otherwise return the next segment id, that segment will be the active segment in the future
		return nil, latestSegmentId, 0, nil
	} else if l.pendingReadSegmentId >= latestSegmentId {
		return nil, latestSegmentId, 0, nil
	}
	// otherwise, move to next exits segment
	nextSegmentId := l.pendingReadSegmentId
	nextEntryId := l.pendingReadEntryId
	for {
		segHandle, err := l.logHandle.GetExistsReadonlySegmentHandle(context.Background(), nextSegmentId)
		if err != nil && !werr.ErrSegmentNotFound.Is(err) {
			return nil, -1, -1, err
		}
		if segHandle != nil {
			m := segHandle.GetMetadata(context.Background())
			// current completed segmentHandle
			if m.LastEntryId >= nextEntryId {
				return segHandle, nextSegmentId, nextEntryId, nil
			}
			// current in-progress segmentHandle
			if m.LastEntryId == -1 {
				// Read from the segment directly because the segmentHandle is not yet completed.
				// The writing entryId could be any value greater than pendingReadEntryId.
				return segHandle, nextSegmentId, nextEntryId, nil
			}
		} else if nextSegmentId >= latestSegmentId {
			// no more exists segment
			break
		}

		// move to next segment
		nextSegmentId += 1
		nextEntryId = 0 // reset entryId=0 as we are reading from the next segment
	}

	// move to next future segment, if no exists segment to read
	nextSegmentId, err = l.logHandle.GetNextSegmentId()
	if err != nil {
		return nil, -1, -1, err
	}
	return nil, nextSegmentId, 0, nil
}
