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
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

var _ LogStoreClient = (*logStoreClientRemote)(nil)

// logStoreClientRemote is a remote implementation of LogStoreClient,
// which will interact with a remote LogStoreClient instance using gRPC.
type logStoreClientRemote struct {
	innerClient proto.LogStoreClient
	// per-log subscription and pending routing
	mu     sync.RWMutex
	closed bool
}

func (l *logStoreClientRemote) CompleteSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) (int64, error) {
	resp, err := l.innerClient.CompleteSegment(ctx, &proto.CompleteSegmentRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId, LastAddConfirmed: lac})
	if err != nil {
		return -1, err
	}
	completeErr := werr.Error(resp.GetStatus())
	if completeErr != nil {
		return -1, completeErr
	}
	return resp.GetLastEntryId(), nil
}

func (l *logStoreClientRemote) AppendEntry(ctx context.Context, bucketName string, rootPath string, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	logger.Ctx(ctx).Debug("logStoreClientRemote: append entry", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId))
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return -1, fmt.Errorf("client is closed")
	}
	l.mu.RUnlock()

	// Create a child context with cancel for controlling the stream lifecycle
	streamCtx, streamCancel := context.WithCancel(ctx)

	// Send unary append request first to get the actual entryId
	respStream, err := l.innerClient.AddEntry(streamCtx, &proto.AddEntryRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, Entry: entry})
	logger.Ctx(ctx).Debug("logStoreClientRemote: append entry called", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId))
	if err != nil {
		streamCancel() // Cancel context on error
		return -1, err
	}
	// First, get the initial AddEntryResponse to check if entry was buffered
	addEntryFirstResponse, err := respStream.Recv()
	if err != nil {
		streamCancel() // Cancel context on error
		return -1, err
	}
	// Then use the stream for async monitoring of the second AddEntryResponse status
	if addEntryFirstResponse.GetState() == proto.AddEntryState_Buffered {
		remoteChannel, isRemote := syncedResultCh.(*channel.RemoteResultChannel)
		if !isRemote {
			streamCancel() // Cancel context if wrong channel type
			return -1, werr.ErrInternalError.WithCauseErrMsg("append result channel type invalid: expected RemoteResultChannel for remote client")
		}
		// set respStream to remoteChannel with context and cancel function
		remoteChannel.InitResponseStream(respStream, streamCtx, streamCancel)
		return addEntryFirstResponse.GetEntryId(), nil
	} else if addEntryFirstResponse.GetState() == proto.AddEntryState_Synced {
		streamCancel() // Cancel context since we don't need the stream anymore
		sendAsyncResultErr := syncedResultCh.SendResult(ctx, &channel.AppendResult{
			SyncedId: addEntryFirstResponse.GetEntryId(),
			Err:      nil,
		})
		// only log if there's actually an error
		if sendAsyncResultErr != nil {
			logger.Ctx(ctx).Warn("send async result failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId), zap.Int64("syncedId", addEntryFirstResponse.GetEntryId()), zap.Error(sendAsyncResultErr))
		}
		// return synced success
		return addEntryFirstResponse.GetEntryId(), nil
	}

	// sync failed
	streamCancel() // Cancel context on failure

	// Handle status safely to avoid nil pointer dereference
	var statusErr error = werr.ErrUnknownError
	if addEntryFirstResponse.Status != nil {
		statusErr = werr.Error(addEntryFirstResponse.Status)
	}

	logger.Ctx(ctx).Warn("write entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId), zap.Error(statusErr))
	return addEntryFirstResponse.GetEntryId(), statusErr
}

func (l *logStoreClientRemote) ReadEntriesBatchAdv(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	resp, err := l.innerClient.GetBatchEntriesAdv(ctx, &proto.GetBatchEntriesAdvRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId, FromEntryId: fromEntryId, MaxEntries: maxEntries, LastReadState: lastReadState})
	if err != nil {
		return nil, err
	}
	readBatchErr := werr.Error(resp.GetStatus())
	if readBatchErr != nil {
		return nil, readBatchErr
	}
	return resp.GetResult(), nil
}

func (l *logStoreClientRemote) FenceSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	resp, err := l.innerClient.FenceSegment(ctx, &proto.FenceSegmentRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId})
	if err != nil {
		return -1, err
	}
	fenceErr := werr.Error(resp.GetStatus())
	if fenceErr != nil {
		return -1, fenceErr
	}
	return resp.GetLastEntryId(), nil
}

func (l *logStoreClientRemote) GetLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	resp, err := l.innerClient.GetSegmentLastAddConfirmed(ctx, &proto.GetSegmentLastAddConfirmedRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId})
	if err != nil {
		return -1, err
	}
	getLacErr := werr.Error(resp.GetStatus())
	if getLacErr != nil {
		return -1, getLacErr
	}
	return resp.GetLastEntryId(), nil
}

func (l *logStoreClientRemote) GetBlockCount(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	resp, err := l.innerClient.GetSegmentBlockCount(ctx, &proto.GetSegmentBlockCountRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId})
	if err != nil {
		return -1, err
	}
	getBlockCountErr := werr.Error(resp.GetStatus())
	if getBlockCountErr != nil {
		return -1, getBlockCountErr
	}
	return resp.GetBlockCount(), nil
}

func (l *logStoreClientRemote) SegmentCompact(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	resp, err := l.innerClient.CompactSegment(ctx, &proto.CompactSegmentRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId})
	if err != nil {
		return nil, err
	}
	compactErr := werr.Error(resp.GetStatus())
	if compactErr != nil {
		return nil, compactErr
	}
	return resp.GetMetadata(), nil
}

func (l *logStoreClientRemote) SegmentClean(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, flag int) error {
	resp, err := l.innerClient.CleanSegment(ctx, &proto.CleanSegmentRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId, Flag: int32(flag)})
	if err != nil {
		return err
	}
	cleanErr := werr.Error(resp.GetStatus())
	if cleanErr != nil {
		return cleanErr
	}
	return nil
}

func (l *logStoreClientRemote) UpdateLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) error {
	resp, err := l.innerClient.UpdateLastAddConfirmed(ctx, &proto.UpdateLastAddConfirmedRequest{BucketName: bucketName, RootPath: rootPath, LogId: logId, SegmentId: segmentId, LastAddConfirmed: lac})
	if err != nil {
		return err
	}
	updateLacErr := werr.Error(resp.GetStatus())
	if updateLacErr != nil {
		return updateLacErr
	}
	return nil
}

func (l *logStoreClientRemote) SelectNodes(ctx context.Context, strategyType proto.StrategyType, affinityMode proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
	resp, err := l.innerClient.SelectNodes(ctx, &proto.SelectNodesRequest{
		Strategy:     strategyType,
		AffinityMode: affinityMode,
		Filters:      filters,
	})
	if err != nil {
		return nil, err
	}
	selectNodesErr := werr.Error(resp.GetStatus())
	if selectNodesErr != nil {
		return nil, selectNodesErr
	}
	return resp.Nodes, nil
}

func (l *logStoreClientRemote) IsRemoteClient() bool {
	return true
}

// Close implements io.Closer
func (l *logStoreClientRemote) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true

	return nil
}
