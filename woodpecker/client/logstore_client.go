// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"

	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/server/processor"
)

//go:generate mockery --dir=./woodpecker/client --name=LogStoreClient --structname=LogStoreClient --output=mocks/mocks_woodpecker/mocks_logstore_client --filename=mock_client.go --with-expecter=true  --outpkg=mocks_logstore_client
type LogStoreClient interface {
	// AppendEntry appends an entry to the specified log segment and returns the entry ID, a channel for synchronization, and an error if any.
	AppendEntry(ctx context.Context, logId int64, entry *processor.SegmentEntry, syncedResultCh chan<- int64) (int64, error)
	// ReadEntry reads an entry from the specified log segment by entry ID and returns the entry and an error if any.
	ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*processor.SegmentEntry, error)
	// ReadEntriesInRange reads a batch of entries from the specified log segment within a range and returns the entries and an error if any.
	ReadEntriesInRange(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, toEntryId int64) ([]*processor.SegmentEntry, error)
	// ReadEntriesBatch reads a batch of entries from the specified log segment and returns the entries and an error if any.
	ReadEntriesBatch(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, size int64) ([]*processor.SegmentEntry, error)
	// FenceSegment fences the specified log segment to prevent further writes and returns an error if any.
	FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error)
	// IsSegmentFenced checks if the specified log segment is fenced and returns a boolean value and an error if any.
	IsSegmentFenced(ctx context.Context, logId int64, segmentId int64) (bool, error)
	// SegmentCompact compacts the specified log segment and returns the updated metadata and an error if any.
	SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	// SegmentRecoveryFromInProgress recovers the specified log segment from the InProgress state and returns the updated metadata and an error if any.
	SegmentRecoveryFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	// SegmentClean cleans up the specified log segment and returns an error if any.
	SegmentClean(ctx context.Context, logId int64, segmentId int64, flag int) error
	// GetLastAddConfirmed gets the lastAddConfirmed entryID of the specified log segment and returns it and an error if any.
	GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error)
}

var _ LogStoreClient = (*logStoreClientLocal)(nil)

// logStoreClientLocal is a local implementation of LogStoreClient,
// which will directly interact with the local LogStore instance.
type logStoreClientLocal struct {
	store server.LogStore
}

func (l *logStoreClientLocal) AppendEntry(ctx context.Context, logId int64, entry *processor.SegmentEntry, syncedResultCh chan<- int64) (int64, error) {
	return l.store.AddEntry(ctx, logId, entry, syncedResultCh)
}

func (l *logStoreClientLocal) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*processor.SegmentEntry, error) {
	return l.store.GetEntry(ctx, logId, segmentId, entryId)
}

func (l *logStoreClientLocal) ReadEntriesInRange(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, toEntryId int64) ([]*processor.SegmentEntry, error) {
	panic("implement me")
}
func (l *logStoreClientLocal) ReadEntriesBatch(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, size int64) ([]*processor.SegmentEntry, error) {
	return l.store.GetBatchEntries(ctx, logId, segmentId, fromEntryId, size)
}

func (l *logStoreClientLocal) FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
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

func (l *logStoreClientLocal) SegmentRecoveryFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return l.store.RecoverySegmentFromInProgress(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentClean(ctx context.Context, logId int64, segmentId int64, flag int) error {
	return l.store.CleanSegment(ctx, logId, segmentId, flag)
}

var _ LogStoreClient = (*logStoreClientRemote)(nil)

// logStoreClientRemote is a remote implementation of LogStoreClient,
// which will interact with a remote LogStoreClient instance using gRPC.
type logStoreClientRemote struct {
	innerClient proto.LogStoreClient
}

func (l *logStoreClientRemote) AppendEntry(ctx context.Context, logId int64, entry *processor.SegmentEntry, syncedResultCh chan<- int64) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) ReadEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*processor.SegmentEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) ReadEntriesInRange(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, toEntryId int64) ([]*processor.SegmentEntry, error) {
	panic("implement me")
}

func (l *logStoreClientRemote) ReadEntriesBatch(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, size int64) ([]*processor.SegmentEntry, error) {
	panic("implement me")
}

func (l *logStoreClientRemote) FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
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

func (l *logStoreClientRemote) SegmentRecoveryFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (l *logStoreClientRemote) SegmentClean(ctx context.Context, logId int64, segmentId int64, flag int) error {
	//TODO implement me
	panic("implement me")
}
