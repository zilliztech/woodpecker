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

package channel

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

var _ ResultChannel = (*RemoteResultChannel)(nil)

// RemoteResultChannel is the remote implementation that uses callback functions to send results.
// This design is more flexible and can adapt to different remote communication methods (gRPC stream, HTTP, message queue, etc.).
type RemoteResultChannel struct {
	identifier string
	ch         grpc.ServerStreamingClient[proto.AddEntryResponse]
	closed     bool
	mu         sync.RWMutex

	// synced result
	result *AppendResult

	// context for controlling the stream lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// NewRemoteResultChannel creates a remote result channel.
func NewRemoteResultChannel(identifier string) *RemoteResultChannel {
	return &RemoteResultChannel{
		identifier: identifier,
		ch:         nil,
		closed:     false,
	}
}

func (r *RemoteResultChannel) GetIdentifier() string {
	return r.identifier
}

// SendResult sends the append result directly to the channel when the gRPC stream
// has not been initialized yet. This is typically used for synchronous operations
// that complete before stream initialization.
func (r *RemoteResultChannel) SendResult(ctx context.Context, result *AppendResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("remote result channel %s is closed", r.identifier)
	}
	r.result = result
	logger.Ctx(ctx).Debug("sent result to remote channel directly",
		zap.String("identifier", r.identifier),
		zap.Int64("syncedID", result.SyncedId),
		zap.Error(result.Err))
	return nil
}

// ReadResult waits for and reads the append result from the remote gRPC stream.
// It returns the result immediately if already available, otherwise blocks until
// a response is received from the stream or an error occurs.
func (r *RemoteResultChannel) ReadResult(ctx context.Context) (*AppendResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, fmt.Errorf("remote result channel %s is closed", r.identifier)
	}
	if r.result != nil {
		return r.result, nil
	}
	if r.ch == nil {
		return nil, fmt.Errorf("remote result channel %s is not initialized", r.identifier)
	}

	// gRPC stream.Recv() automatically respects context cancellation
	// since the stream was created with the context in AppendEntry
	resultResponse, readErr := r.ch.Recv()
	if readErr != nil {
		logger.Ctx(ctx).Warn("failed to read result from remote channel",
			zap.String("identifier", r.identifier),
			zap.Error(readErr))
		return nil, readErr
	}
	if resultResponse.GetState() == proto.AddEntryState_Synced {
		return &AppendResult{
			SyncedId: resultResponse.GetEntryId(),
			Err:      nil,
		}, nil
	}

	// sync failed
	syncedErr := werr.ErrFileWriterSyncFailed.WithCauseErrMsg(fmt.Sprintf("write entry failed, err code:%d", resultResponse.Status.Code))
	return &AppendResult{
		SyncedId: resultResponse.GetEntryId(),
		Err:      syncedErr,
	}, syncedErr
}

func (r *RemoteResultChannel) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.closed {
		r.closed = true

		// Cancel the stream context first
		if r.cancel != nil {
			r.cancel()
			logger.Ctx(ctx).Debug("cancelled remote result channel context", zap.String("identifier", r.identifier))
		}

		// Then close the stream
		if r.ch != nil {
			closeErr := r.ch.CloseSend()
			if closeErr != nil {
				logger.Ctx(ctx).Warn("closed remote result channel failed", zap.String("identifier", r.identifier), zap.Error(closeErr))
			} else {
				logger.Ctx(ctx).Debug("closed remote result channel", zap.String("identifier", r.identifier))
			}
		}
	}
	return nil
}

func (r *RemoteResultChannel) IsClosed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closed
}

// InitResponseStream initializes the remote result channel with the gRPC stream.
func (r *RemoteResultChannel) InitResponseStream(responseCh grpc.ServerStreamingClient[proto.AddEntryResponse], ctx context.Context, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		// Don't initialize if channel is already closed
		if cancel != nil {
			cancel() // Cancel the context if channel is already closed
		}
		return
	}
	r.ch = responseCh
	r.ctx = ctx
	r.cancel = cancel
}
