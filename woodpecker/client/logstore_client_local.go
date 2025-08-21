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

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
)

var _ LogStoreClient = (*logStoreClientLocal)(nil)

// logStoreClientLocal is a local implementation of LogStoreClient,
// which will directly interact with the local LogStore instance.
type logStoreClientLocal struct {
	store server.LogStore
}

func (l *logStoreClientLocal) CompleteSegment(ctx context.Context, logId int64, segmentId int64, lac int64) (int64, error) {
	return l.store.CompleteSegment(ctx, logId, segmentId, lac)
}

func (l *logStoreClientLocal) AppendEntry(ctx context.Context, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	return l.store.AddEntry(ctx, logId, entry, syncedResultCh)
}

func (l *logStoreClientLocal) ReadEntriesBatchAdv(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	return l.store.GetBatchEntriesAdv(ctx, logId, segmentId, fromEntryId, maxEntries, lastReadState)
}

func (l *logStoreClientLocal) FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	return l.store.FenceSegment(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	return l.store.GetSegmentLastAddConfirmed(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) GetBlockCount(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	return l.store.GetSegmentBlockCount(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return l.store.CompactSegment(ctx, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentClean(ctx context.Context, logId int64, segmentId int64, flag int) error {
	return l.store.CleanSegment(ctx, logId, segmentId, flag)
}

func (l *logStoreClientLocal) UpdateLastAddConfirmed(ctx context.Context, logId int64, segmentId int64, lac int64) error {
	// NO-OP
	return nil
}

func (l *logStoreClientLocal) Close(ctx context.Context) error {
	return nil
}
