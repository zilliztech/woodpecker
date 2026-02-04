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

func (l *logStoreClientLocal) CompleteSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) (int64, error) {
	return l.store.CompleteSegment(ctx, bucketName, rootPath, logId, segmentId, lac)
}

func (l *logStoreClientLocal) AppendEntry(ctx context.Context, bucketName string, rootPath string, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	return l.store.AddEntry(ctx, bucketName, rootPath, logId, entry, syncedResultCh)
}

func (l *logStoreClientLocal) ReadEntriesBatchAdv(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	return l.store.GetBatchEntriesAdv(ctx, bucketName, rootPath, logId, segmentId, fromEntryId, maxEntries, lastReadState)
}

func (l *logStoreClientLocal) FenceSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	return l.store.FenceSegment(ctx, bucketName, rootPath, logId, segmentId)
}

func (l *logStoreClientLocal) GetLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	return l.store.GetSegmentLastAddConfirmed(ctx, bucketName, rootPath, logId, segmentId)
}

func (l *logStoreClientLocal) GetBlockCount(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	return l.store.GetSegmentBlockCount(ctx, bucketName, rootPath, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentCompact(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return l.store.CompactSegment(ctx, bucketName, rootPath, logId, segmentId)
}

func (l *logStoreClientLocal) SegmentClean(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, flag int) error {
	return l.store.CleanSegment(ctx, bucketName, rootPath, logId, segmentId, flag)
}

func (l *logStoreClientLocal) UpdateLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) error {
	// NO-OP: LAC mechanism is not needed in single-node mode
	return nil
}

func (l *logStoreClientLocal) SelectNodes(ctx context.Context, strategyType proto.StrategyType, affinityMode proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
	// NO-OP: Node selection is not required in single-node mode
	return nil, nil
}

func (l *logStoreClientLocal) IsRemoteClient() bool {
	return false
}

func (l *logStoreClientLocal) Close(ctx context.Context) error {
	return nil
}
