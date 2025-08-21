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
)

//go:generate mockery --dir=./woodpecker/client --name=LogStoreClient --structname=LogStoreClient --output=mocks/mocks_woodpecker/mocks_logstore_client --filename=mock_client.go --with-expecter=true  --outpkg=mocks_logstore_client
type LogStoreClient interface {
	// AppendEntry appends an entry to the specified log segment and returns the entry ID, a channel for synchronization, and an error if any.
	AppendEntry(ctx context.Context, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error)
	// ReadEntriesBatchAdv reads a batch of entries from the specified log segment and returns the entries and an error if any.
	ReadEntriesBatchAdv(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error)
	// CompleteSegment finalize the specified log segment and returns an error if any.
	CompleteSegment(ctx context.Context, logId int64, segmentId int64, lac int64) (int64, error)
	// FenceSegment fences the specified log segment to prevent further writes and returns an error if any.
	FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error)
	// SegmentCompact compacts the specified log segment and returns the updated metadata and an error if any.
	SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	// SegmentClean cleans up the specified log segment and returns an error if any.
	SegmentClean(ctx context.Context, logId int64, segmentId int64, flag int) error
	// GetLastAddConfirmed gets the lastAddConfirmed entryID of the specified log segment and returns it and an error if any.
	GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error)
	// GetBlockCount gets the block count of the specified log segment and returns it and an error if any.
	GetBlockCount(ctx context.Context, logId int64, segmentId int64) (int64, error)
	// UpdateLastAddConfirmed updates the lastAddConfirmed entryID of the specified log segment.
	UpdateLastAddConfirmed(ctx context.Context, logId int64, segmentId int64, lac int64) error
	// Close closes the log store client and returns an error if any.
	Close(ctx context.Context) error
}
