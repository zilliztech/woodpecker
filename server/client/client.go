package client

import (
	"context"

	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/server/segment"
)

type LogStoreClient interface {
	AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, error)
	ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) ([]byte, error)
	FenceSegment(ctx context.Context, logId int64, segmentId int64) error
	RequestCompaction(ctx context.Context, logId int64, segmentId int64) error
}

func NewLogStoreClientLocal(store *server.LogStore) LogStoreClient {
	return &LogStoreClientLocal{
		store: store,
	}
}

var _ LogStoreClient = (*LogStoreClientLocal)(nil)

type LogStoreClientLocal struct {
	store *server.LogStore
}

func (l *LogStoreClientLocal) AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, error) {
	return l.store.AddEntry(ctx, logId, entry)
}

func (l *LogStoreClientLocal) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) ([]byte, error) {
	return l.store.GetEntry(ctx, logId, segmentId, entryId)
}

func (l *LogStoreClientLocal) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	return l.store.FenceSegment(ctx, logId, segmentId)
}

func (l *LogStoreClientLocal) RequestCompaction(ctx context.Context, logId int64, segmentId int64) error {
	return l.store.CompactSegment(ctx, logId, segmentId)
}

var _ LogStoreClient = (*LogStoreClientRemote)(nil)

type LogStoreClientRemote struct {
	innerClient proto.LogStoreClient
}

func (l *LogStoreClientRemote) AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogStoreClientRemote) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogStoreClientRemote) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	//TODO implement me
	panic("implement me")
}

func (l *LogStoreClientRemote) RequestCompaction(ctx context.Context, logId int64, segmentId int64) error {
	//TODO implement me
	panic("implement me")
}
