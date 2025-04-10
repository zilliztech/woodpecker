package log

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

//go:generate mockery --dir=./woodpecker/log --name=LogWriter --structname=LogWriter --output=mocks/mocks_woodpecker/mocks_log_handle --filename=mock_log_writer.go --with-expecter=true  --outpkg=mocks_log_handle
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

func NewLogWriter(ctx context.Context, logHandle LogHandle, cfg *config.Configuration) LogWriter {
	w := &logWriterImpl{
		logHandle:          logHandle,
		auditorMaxInterval: cfg.Woodpecker.Client.Auditor.MaxInterval,
		cfg:                cfg,
		writerClose:        make(chan struct{}, 1),
	}
	// TODO: Add support for other storage auditors once they implement merge/compact functionality
	//if cfg.Woodpecker.Storage.IsStorageMinio() {
	//	go w.runAuditor()
	//}
	return w
}

var _ LogWriter = (*logWriterImpl)(nil)

type logWriterImpl struct {
	sync.RWMutex
	logHandle          LogHandle
	auditorMaxInterval int
	cfg                *config.Configuration
	writerClose        chan struct{}
}

func (l *logWriterImpl) Write(ctx context.Context, msg *WriterMessage) *WriteResult {
	ch := make(chan *WriteResult, 1)
	callback := func(segmentId int64, entryId int64, err error) {
		logger.Ctx(ctx).Debug("write log entry callback", zap.String("logName", l.logHandle.GetName()), zap.Int64("segId", segmentId), zap.Int64("entryId", entryId), zap.Error(err))
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
		return <-ch
	}
	bytes, err := MarshalMessage(msg)
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
		logger.Ctx(ctx).Debug("write log entry callback exec", zap.Int64("segId", segmentId), zap.Int64("entryId", entryId), zap.Error(err))
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
		logger.Ctx(ctx).Error("get writable segment failed", zap.String("logName", l.logHandle.GetName()), zap.Error(err))
		callback(-1, -1, err)
		return ch
	}
	bytes, err := MarshalMessage(msg)
	if err != nil {
		logger.Ctx(ctx).Error("get writable segment failed", zap.String("logName", l.logHandle.GetName()), zap.Error(err))
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
			segs, err := l.logHandle.GetSegments(context.TODO())
			if err != nil {
				logger.Ctx(context.TODO()).Warn("get log segments failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Error(err))
				continue
			}
			nextSegId, err := l.logHandle.GetNextSegmentId()
			if err != nil {
				logger.Ctx(context.TODO()).Warn("get next segment id failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Error(err))
				continue
			}
			for _, seg := range segs {
				if seg.SegNo >= nextSegId-1 {
					// last segment maybe in-progress, no need to recover it
					continue
				}
				stateBefore := seg.State
				if stateBefore == proto.SegmentState_Sealed || stateBefore == proto.SegmentState_Truncated {
					continue
				}
				recoverySegmentHandle, getRecoverySegmentHandleErr := l.logHandle.GetRecoverableSegmentHandle(context.TODO(), seg.SegNo)
				if getRecoverySegmentHandleErr != nil {
					logger.Ctx(context.TODO()).Warn("get log segment failed when log auditor running", zap.String("logName", l.logHandle.GetName()), zap.Int64("segId", seg.SegNo), zap.Error(getRecoverySegmentHandleErr))
					continue
				}
				maintainErr := recoverySegmentHandle.RecoveryOrCompact(context.TODO())
				if err != nil {
					logger.Ctx(context.TODO()).Warn("auditor maintain the log segment failed", zap.String("logName", l.logHandle.GetName()), zap.Int64("segId", seg.SegNo), zap.Error(maintainErr))
					continue
				}
				logger.Ctx(context.TODO()).Info("auditor maintain the log segment success", zap.String("logName", l.logHandle.GetName()), zap.Int64("segId", seg.SegNo), zap.String("stateBefore", stateBefore.String()), zap.String("stateAfter", seg.State.String()))
			}
		case <-l.writerClose:
			logger.Ctx(context.TODO()).Debug("log writer stop", zap.String("logName", l.logHandle.GetName()))
			return
		}
	}
}

func (l *logWriterImpl) Truncate(ctx context.Context, id *LogMessageId) error {
	//TODO implement me
	panic("implement me")
}

func (l *logWriterImpl) Close(ctx context.Context) error {
	l.writerClose <- struct{}{}
	close(l.writerClose)
	closeErr := l.logHandle.CloseAndCompleteCurrentWritableSegment(ctx)
	if closeErr != nil {
		logger.Ctx(ctx).Warn("close log writer failed", zap.String("logName", l.logHandle.GetName()), zap.Error(closeErr))
		if werr.ErrSegmentNotFound.Is(closeErr) {
			closeErr = nil
		}
	}
	releaseLockErr := l.logHandle.GetMetadataProvider().ReleaseLogWriterLock(ctx, l.logHandle.GetName())
	if releaseLockErr != nil {
		logger.Ctx(ctx).Warn(fmt.Sprintf("failed to release log writer lock for logName:%s", l.logHandle.GetName()))
	}
	return errors.Join(closeErr, releaseLockErr)
}

type WriteResult struct {
	LogMessageId *LogMessageId
	Err          error
}
