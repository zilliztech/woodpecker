package log

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

type LogWriter interface {
	// Write writes a log message synchronously and returns a WriteResult.
	// It takes a context and a byte slice representing the log message.
	Write(context.Context, *WriterMessage) *WriteResult

	// WriteAsync writes a log message asynchronously and returns a channel that will receive a WriteResult.
	// It takes a context and a byte slice representing the log message.
	WriteAsync(context.Context, *WriterMessage) <-chan *WriteResult

	// Truncate truncates the log to the specified log message ID.
	// It takes a context and a LogMessageId.
	Truncate(context.Context, *LogMessageId) error

	// Close closes the log writer.
	// It takes a context and returns an error if any occurs.
	Close(context.Context) error
}

func NewLogWriter(ctx context.Context, logHandle *logHandleImpl) LogWriter {
	return &logWriterImpl{
		logHandle: logHandle,
	}
}

var _ LogWriter = (*logWriterImpl)(nil)

type logWriterImpl struct {
	sync.RWMutex
	logHandle *logHandleImpl
}

// Deprecated
func (l *logWriterImpl) WriteSync(ctx context.Context, msg *WriterMessage) *WriteResult {
	writableSegmentHandle, err := l.logHandle.getOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		return &WriteResult{
			LogMessageId: nil,
			Err:          err,
		}
	}
	bytes, err := marshalMessage(msg)
	if err != nil {
		return &WriteResult{
			LogMessageId: nil,
			Err:          err,
		}
	}
	entryId, err := writableSegmentHandle.Append(ctx, bytes)
	if err != nil {
		return &WriteResult{
			LogMessageId: nil,
			Err:          err,
		}
	}
	return &WriteResult{
		LogMessageId: &LogMessageId{
			SegmentId: writableSegmentHandle.GetId(ctx),
			EntryId:   entryId,
		},
		Err: err,
	}
}

func (l *logWriterImpl) Write(ctx context.Context, msg *WriterMessage) *WriteResult {
	ch := make(chan *WriteResult, 1)
	callback := func(segmentId int64, entryId int64, err error) {
		ch <- &WriteResult{
			LogMessageId: &LogMessageId{
				SegmentId: segmentId,
				EntryId:   entryId,
			},
			Err: err,
		}
		close(ch)
	}
	writableSegmentHandle, err := l.logHandle.getOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		callback(-1, -1, err)
		return <-ch
	}
	bytes, err := marshalMessage(msg)
	if err != nil {
		return &WriteResult{
			LogMessageId: nil,
			Err:          err,
		}
	}
	writableSegmentHandle.AppendAsync(ctx, bytes, callback)
	return <-ch
}

func (l *logWriterImpl) WriteAsync(ctx context.Context, msg *WriterMessage) <-chan *WriteResult {
	l.Lock()
	defer l.Unlock()
	ch := make(chan *WriteResult, 1)
	callback := func(segmentId int64, entryId int64, err error) {
		//fmt.Println("callback segmentId: ", segmentId, " entryId: ", entryId, " err: ", err)
		ch <- &WriteResult{
			LogMessageId: &LogMessageId{
				SegmentId: segmentId,
				EntryId:   entryId,
			},
			Err: err,
		}
		// maybe let the caller decide when to close the channel, because the server view retry automatically?
		close(ch)
	}
	writableSegmentHandle, err := l.logHandle.getOrCreateWritableSegmentHandle(ctx)
	if err != nil {
		logger.Ctx(ctx).Error("get writable segment failed", zap.String("logName", l.logHandle.Name), zap.Error(err))
		callback(-1, -1, err)
		return ch
	}
	bytes, err := marshalMessage(msg)
	if err != nil {
		logger.Ctx(ctx).Error("get writable segment failed", zap.String("logName", l.logHandle.Name), zap.Error(err))
		callback(-1, -1, err)
		return ch
	}
	writableSegmentHandle.AppendAsync(ctx, bytes, callback)
	return ch
}

func (l *logWriterImpl) Truncate(ctx context.Context, id *LogMessageId) error {
	//TODO implement me
	panic("implement me")
}

func (l *logWriterImpl) Close(ctx context.Context) error {
	return l.logHandle.CloseAndCompleteCurrentWritableSegment(ctx)
}

type WriteResult struct {
	LogMessageId *LogMessageId
	Err          error
}
