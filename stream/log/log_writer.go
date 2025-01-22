package log

import "context"

type LogWriter interface {
	Write(context.Context, []byte) *WriteResult
	WriteAsync(context.Context, []byte) <-chan *WriteResult
	Truncate(context.Context, *LogMessageId) error
	Close(context.Context) error
}

func NewLogWriter(ctx context.Context, logHandle *logHandleImpl) LogWriter {
	return &logWriterImpl{
		logHandle: logHandle,
	}
}

var _ LogWriter = (*logWriterImpl)(nil)

type logWriterImpl struct {
	logHandle *logHandleImpl
}

// Deprecated
func (l *logWriterImpl) WriteSync(ctx context.Context, bytes []byte) *WriteResult {
	writableSegmentHandle, err := l.logHandle.getOrCreateWritableSegmentHandle(ctx)
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

func (l *logWriterImpl) Write(ctx context.Context, bytes []byte) *WriteResult {
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
	writableSegmentHandle.AppendAsync(ctx, bytes, callback)
	return <-ch
}

func (l *logWriterImpl) WriteAsync(ctx context.Context, bytes []byte) <-chan *WriteResult {
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
	//TODO implement me
	panic("implement me")
}

type WriteResult struct {
	LogMessageId *LogMessageId
	Err          error
}
