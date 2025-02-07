package log

import (
	"context"
	"fmt"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/stream/segment"
)

type LogReader interface {
	// ReadNext reads the next log message from the log, blocking until a message
	// is available. It returns the log message and an error if any occurs.
	ReadNext(context.Context) (*LogMessage, error)
	// Close closes the log reader.
	// It returns an error if any occurs during the closing process.
	Close(context.Context) error
}

func NewLogReader(ctx context.Context, logHandle *logHandleImpl, segmentHandle segment.SegmentHandle, from *LogMessageId) LogReader {
	return &logReaderImpl{
		logHandle:            logHandle,
		currentSegmentHandle: segmentHandle,
		pendingReadSegmentId: from.SegmentId,
		pendingReadEntryId:   from.EntryId,
	}
}

var _ LogReader = (*logReaderImpl)(nil)

type logReaderImpl struct {
	logHandle *logHandleImpl

	pendingReadSegmentId int64
	pendingReadEntryId   int64
	currentSegmentHandle segment.SegmentHandle
}

func (l *logReaderImpl) ReadNext(ctx context.Context) (*LogMessage, error) {
	if l.logHandle == nil {
		return nil, fmt.Errorf("log handle is not initialized")
	}

	if l.currentSegmentHandle == nil {
		segmentHandle, err := l.logHandle.getOrCreateReadonlySegmentHandle(ctx, l.pendingReadSegmentId)
		if err != nil {
			return nil, err
		}
		l.currentSegmentHandle = segmentHandle
	}
	if l.currentSegmentHandle.GetMetadata(ctx).State != proto.SegmentState_Active &&
		l.pendingReadEntryId >= l.currentSegmentHandle.GetMetadata(ctx).LastEntryId {
		// read from next segment
		segmentHandle, err := l.logHandle.getOrCreateReadonlySegmentHandle(ctx, l.pendingReadSegmentId+1)
		if err != nil {
			return nil, err
		}
		l.pendingReadSegmentId = l.pendingReadSegmentId + 1
		l.pendingReadEntryId = 0
		l.currentSegmentHandle = segmentHandle
	}

	entries, err := l.currentSegmentHandle.Read(ctx, l.pendingReadEntryId, l.pendingReadEntryId)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	if len(entries) > 1 {
		return nil, fmt.Errorf("read more than one entry")
	}

	l.pendingReadEntryId++
	return &LogMessage{
		Id: &LogMessageId{
			SegmentId: entries[0].SegmentId,
			EntryId:   entries[0].EntryId,
		},
		Payload: entries[0].Data,
	}, nil
}

func (l *logReaderImpl) Close(ctx context.Context) error {
	// NO-OP
	return nil
}
