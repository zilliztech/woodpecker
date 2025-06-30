package objectstorage

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
	"github.com/zilliztech/woodpecker/server/storage"
)

var _ storage.Reader = (*EntryReader)(nil)

// EntryReader is responsible for reading specified entries from the segment
type EntryReader struct {
	ctx     context.Context
	opt     storage.ReaderOpt
	segment *ROSegmentImpl

	pendingReadEntryId int64
	currentFragment    *FragmentObject
}

// NewEntryReader creates a new entry reader instance.
func NewEntryReader(opt storage.ReaderOpt, segment *ROSegmentImpl) storage.Reader {
	return &EntryReader{
		opt:                opt,
		segment:            segment,
		pendingReadEntryId: opt.StartSequenceNum,
	}
}

func (o *EntryReader) HasNext(ctx context.Context) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "HasNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", o.segment.logId)
	if o.pendingReadEntryId >= int64(o.opt.EndSequenceNum) && o.opt.EndSequenceNum > 0 {
		// reach the end of range
		return false, nil
	}
	f, err := o.segment.getFragment(ctx, o.pendingReadEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get fragment",
			zap.String("segmentPrefixKey", o.segment.segmentPrefixKey),
			zap.Int64("pendingReadEntryId", o.pendingReadEntryId),
			zap.Error(err))
		return false, err
	}
	if f == nil {
		// no more fragment
		return false, nil
	}
	//
	o.currentFragment = f
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "has_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "has_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return true, nil
}

func (o *EntryReader) ReadNext(ctx context.Context) (*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", o.segment.logId)
	if o.currentFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := o.currentFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer o.currentFragment.Release(ctx)
	entryValue, err := o.currentFragment.GetEntry(ctx, o.pendingReadEntryId)
	if err != nil {
		return nil, err
	}
	entry := &proto.LogEntry{
		EntryId: o.pendingReadEntryId,
		Values:  entryValue,
	}
	o.pendingReadEntryId++
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "read_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "read_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entry, nil
}

// ReadNextBatch reads next batch of entries from the log file.
// size = -1 means auto batch,which will read all entries of a fragment for a time,
// otherwise it's the number of entries to read.
func (o *EntryReader) ReadNextBatch(ctx context.Context) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNextBatch")
	defer sp.End()
	if o.opt.BatchSize != -1 {
		// TODO add batch size limit.
		return nil, werr.ErrNotSupport.WithCauseErrMsg("custom batch size not supported currently")
	}

	startTime := time.Now()
	logId := fmt.Sprintf("%d", o.segment.logId)
	if o.currentFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := o.currentFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer o.currentFragment.Release(ctx)
	entries := make([]*proto.LogEntry, 0, 32)
	for {
		entryValue, err := o.currentFragment.GetEntry(ctx, o.pendingReadEntryId)
		if err != nil {
			if werr.ErrEntryNotFound.Is(err) {
				break
			}
			return nil, err
		}
		entry := &proto.LogEntry{
			EntryId: o.pendingReadEntryId,
			Values:  entryValue,
		}
		entries = append(entries, entry)
		o.pendingReadEntryId++
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, "read_batch_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "read_batch_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entries, nil
}

func (o *EntryReader) Close() error {
	// NO OP
	return nil
}
