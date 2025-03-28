package client

import (
	"context"

	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/server/segment"
)

//go:generate mockery --dir=./server/client --name=LogStoreClient --structname=LogStoreClient --output=mocks/mocks_server/mocks_logstore_client --filename=mock_client.go --with-expecter=true  --outpkg=mocks_logstore_client
type LogStoreClient interface {
	// AppendEntry appends an entry to the specified log segment and returns the entry ID, a channel for synchronization, and an error if any.
	AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, <-chan int64, error)
	// ReadEntry reads an entry from the specified log segment by entry ID and returns the entry and an error if any.
	ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*segment.SegmentEntry, error)
	// ReadBatchEntries reads a batch of entries from the specified log segment within a range and returns the entries and an error if any.
	ReadBatchEntries(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, toEntryId int64) ([]*segment.SegmentEntry, error)
	// FenceSegment fences the specified log segment to prevent further writes and returns an error if any.
	FenceSegment(ctx context.Context, logId int64, segmentId int64) error
	// IsSegmentFenced checks if the specified log segment is fenced and returns a boolean value and an error if any.
	IsSegmentFenced(ctx context.Context, logId int64, segmentId int64) (bool, error)
	// SegmentCompact compacts the specified log segment and returns the updated metadata and an error if any.
	SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	// SegmentRecoveryFromInRecovery recovers the specified log segment from the InRecovery state and returns the updated metadata and an error if any.
	SegmentRecoveryFromInRecovery(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	// SegmentRecoveryFromInProgress recovers the specified log segment from the InProgress state and returns the updated metadata and an error if any.
	SegmentRecoveryFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	// GetLastAddConfirmed gets the lastAddConfirmed entryID of the specified log segment and returns it and an error if any.
	GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error)
}

var _ LogStoreClient = (*logStoreClientLocal)(nil)

// logStoreClientLocal is a local implementation of LogStoreClient,
// which will directly interact with the local LogStore instance.
type logStoreClientLocal struct {
	store server.LogStore
}

func (l *logStoreClientLocal) AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, <-chan int64, error) {
	return l.store.AddEntry(ctx, logId, entry)
}

func (l *logStoreClientLocal) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*segment.SegmentEntry, error) {
	return l.store.GetEntry(ctx, logId, segmentId, entryId)
}

func (l *logStoreClientLocal) ReadBatchEntries(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, toEntryId int64) ([]*segment.SegmentEntry, error) {
	panic("implement me")
}

func (l *logStoreClientLocal) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	return l.store.FenceSegment(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) IsSegmentFenced(ctx context.Context, logId int64, segmentId int64) (bool, error) {
	return l.store.IsSegmentFenced(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	return l.store.GetSegmentLastAddConfirmed(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return l.store.CompactSegment(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentRecoveryFromInRecovery(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return l.store.RecoverySegmentFromInRecovery(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentRecoveryFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return l.store.RecoverySegmentFromInProgress(ctx, logId, segmentId)
}

var _ LogStoreClient = (*logStoreClientRemote)(nil)

// logStoreClientRemote is a remote implementation of LogStoreClient,
// which will interact with a remote LogStoreClient instance using gRPC.
type logStoreClientRemote struct {
	innerClient proto.LogStoreClient
}

func (l *logStoreClientRemote) AppendEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, <-chan int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*segment.SegmentEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) ReadBatchEntries(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, toEntryId int64) ([]*segment.SegmentEntry, error) {
	panic("implement me")
}

func (l *logStoreClientRemote) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) IsSegmentFenced(ctx context.Context, logId int64, segmentId int64) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) SegmentRecoveryFromInRecovery(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) SegmentRecoveryFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}
