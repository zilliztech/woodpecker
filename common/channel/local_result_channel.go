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

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
)

var _ ResultChannel = (*LocalResultChannel)(nil)

// LocalResultChannel is the local implementation that directly wraps a Go channel.
type LocalResultChannel struct {
	identifier string
	ch         chan *AppendResult
	closed     bool
	mu         sync.RWMutex
}

// NewLocalResultChannel creates a local result channel.
func NewLocalResultChannel(identifier string) *LocalResultChannel {
	return &LocalResultChannel{
		identifier: identifier,
		ch:         make(chan *AppendResult, 1),
		closed:     false,
	}
}

func (l *LocalResultChannel) GetIdentifier() string {
	return l.identifier
}

func (l *LocalResultChannel) SendResult(ctx context.Context, result *AppendResult) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = werr.ErrAppendOpResultChannelClosed.WithCauseErrMsg(fmt.Sprintf("local result channel %s underlying channel is closed", l.identifier))
			return
		}
	}()
	select {
	case l.ch <- result:
		logger.Ctx(ctx).Debug("sent result to local channel",
			zap.String("identifier", l.identifier),
			zap.Int64("syncedId", result.SyncedId),
			zap.String("ch", fmt.Sprintf("%p", l)),
			zap.Error(result.Err))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		logger.Ctx(ctx).Warn("local channel is full or closed",
			zap.String("identifier", l.identifier),
			zap.Int64("syncedId", result.SyncedId),
			zap.String("ch", fmt.Sprintf("%p", l)),
			zap.Error(result.Err))
		return fmt.Errorf("local channel %s is full or closed", l.identifier)
	}
}

func (l *LocalResultChannel) ReadResult(ctx context.Context) (*AppendResult, error) {
	select {
	case r, ok := <-l.ch:
		if !ok {
			// Channel was closed externally
			return nil, werr.ErrAppendOpResultChannelClosed.WithCauseErrMsg(fmt.Sprintf("local result channel %s underlying channel is closed", l.identifier))
		}
		logger.Ctx(ctx).Debug("read result from local channel",
			zap.String("identifier", l.identifier),
			zap.Int64("identifier", r.SyncedId),
			zap.String("ch", fmt.Sprintf("%p", l)),
			zap.Error(r.Err),
		)
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *LocalResultChannel) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.ch)
	}
	logger.Ctx(ctx).Debug("closed local result channel and underlying channel",
		zap.String("identifier", l.identifier),
		zap.String("ch", fmt.Sprintf("%p", l)))
	return nil
}

func (l *LocalResultChannel) IsClosed() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.closed
}
