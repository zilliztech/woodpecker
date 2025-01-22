package client

import (
	"context"

	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server"
	"github.com/milvus-io/woodpecker/server/segment"
)

type LogStoreClient interface {
	AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, error)
	ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) ([]byte, error)
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

var _ LogStoreClient = (*LogStoreClientRemote)(nil)

type LogStoreClientRemote struct {
	innerClient proto.LogStoreClient
}

func (l LogStoreClientRemote) AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l LogStoreClientRemote) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}
