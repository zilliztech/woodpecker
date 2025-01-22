package log

import "context"

type LogReader interface {
	ReadNext(context.Context) (*LogMessage, error)
	Close(context.Context) error
}

var _ LogReader = (*logReaderImpl)(nil)

type logReaderImpl struct{}

func (l *logReaderImpl) ReadNext(ctx context.Context) (*LogMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logReaderImpl) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
