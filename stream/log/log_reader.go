package log

import (
	"context"
	"fmt"
	"sort"

	"github.com/zilliztech/woodpecker/stream/segment"
)

type LogReader interface {
	ReadNext(context.Context) (*LogMessage, error)
	Close(context.Context) error
}

func NewLogReader(ctx context.Context, logHandle *logHandleImpl, segmentHandle segment.SegmentHandle, from *LogMessageId) LogReader {
	return &logReaderImpl{
		logHandle:            logHandle,
		currentSegmentHandle: segmentHandle,
		currentSegmentId:     from.SegmentId,
		currentEntryId:       from.EntryId,
	}
}

var _ LogReader = (*logReaderImpl)(nil)

type logReaderImpl struct {
	logHandle            *logHandleImpl
	currentSegmentHandle segment.SegmentHandle
	currentSegmentId     int64
	currentEntryId       int64
}

func (l *logReaderImpl) ReadNext(ctx context.Context) (*LogMessage, error) {
	if l.logHandle == nil {
		return nil, fmt.Errorf("log handle is not initialized")
	}

	if l.currentSegmentHandle == nil || l.currentEntryId >= l.currentSegmentHandle.GetMetadata(ctx).LastEntryId {
		segments, err := l.logHandle.GetSegments(ctx)
		if err != nil {
			return nil, err
		}

		// Sort segments by their ID
		var segmentIds []int64
		for id := range segments {
			segmentIds = append(segmentIds, id)
		}
		sort.Slice(segmentIds, func(i, j int) bool {
			return segmentIds[i] < segmentIds[j]
		})

		foundNext := false
		for _, segId := range segmentIds {
			if segId > l.currentSegmentId {
				l.currentSegmentId = segId
				segmentMeta := segments[segId]
				segmentHandle, err := l.logHandle.getOrCreateReadonlySegmentHandle(ctx, segmentMeta.SegNo)
				if err != nil {
					return nil, err
				}
				l.currentSegmentHandle = segmentHandle
				l.currentEntryId = 0
				foundNext = true
				break
			}
		}

		if !foundNext {
			return nil, nil // No more entries
		}
	}

	entry, err := l.currentSegmentHandle.Read(ctx, l.currentEntryId, l.currentEntryId)
	if err != nil {
		return nil, err
	}
	l.currentEntryId++

	return &LogMessage{
		Id: &LogMessageId{
			SegmentId: entry[0].SegmentId,
			EntryId:   entry[0].EntryId,
		},
		Payload: entry[0].Data,
	}, nil
}

func (l *logReaderImpl) Close(ctx context.Context) error {
	// NO-OP
	return nil
}
