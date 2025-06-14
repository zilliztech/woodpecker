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
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// ResultChannel is an abstract interface for handling asynchronous result notifications.
// The local implementation directly uses Go channels, while the remote implementation uses gRPC streams + identifiers.
type ResultChannel interface {
	// GetIdentifier returns the unique identifier of this result channel.
	GetIdentifier() string
	// SendResult sends a result to the channel.
	SendResult(ctx context.Context, result int64) error
	// Close closes the channel.
	Close(ctx context.Context) error
	// IsClosed checks if the channel is closed.
	IsClosed() bool
}

// LocalResultChannel is the local implementation that directly wraps a Go channel.
type LocalResultChannel struct {
	identifier string
	ch         chan<- int64
	closed     bool
	mu         sync.RWMutex
}

// NewLocalResultChannel creates a local result channel.
func NewLocalResultChannel(identifier string, ch chan<- int64) *LocalResultChannel {
	return &LocalResultChannel{
		identifier: identifier,
		ch:         ch,
		closed:     false,
	}
}

func (l *LocalResultChannel) GetIdentifier() string {
	return l.identifier
}

func (l *LocalResultChannel) SendResult(ctx context.Context, result int64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return fmt.Errorf("local result channel %s is closed", l.identifier)
	}

	select {
	case l.ch <- result:
		logger.Ctx(ctx).Debug("sent result to local channel",
			zap.String("identifier", l.identifier),
			zap.Int64("result", result))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		logger.Ctx(ctx).Warn("local channel is full or closed",
			zap.String("identifier", l.identifier))
		return fmt.Errorf("local channel %s is full or closed", l.identifier)
	}
}

func (l *LocalResultChannel) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.closed {
		l.closed = true
		// Note: The underlying channel is not closed here because it may be used elsewhere.
		logger.Ctx(ctx).Debug("closed local result channel",
			zap.String("identifier", l.identifier))
	}
	return nil
}

func (l *LocalResultChannel) IsClosed() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.closed
}

// RemoteResultChannel is the remote implementation that uses callback functions to send results.
// This design is more flexible and can adapt to different remote communication methods (gRPC stream, HTTP, message queue, etc.).
type RemoteResultChannel struct {
	identifier   string
	sendFunc     func(ctx context.Context, identifier string, result int64) error // Callback function to send results.
	closeFunc    func(ctx context.Context, identifier string) error               // Callback function to close the channel.
	closed       bool
	mu           sync.RWMutex
	lastActivity time.Time
}

// RemoteResultChannelConfig is the configuration for a remote result channel.
type RemoteResultChannelConfig struct {
	Identifier string
	SendFunc   func(ctx context.Context, identifier string, result int64) error
	CloseFunc  func(ctx context.Context, identifier string) error
}

// NewRemoteResultChannel creates a remote result channel.
func NewRemoteResultChannel(config RemoteResultChannelConfig) *RemoteResultChannel {
	return &RemoteResultChannel{
		identifier:   config.Identifier,
		sendFunc:     config.SendFunc,
		closeFunc:    config.CloseFunc,
		closed:       false,
		lastActivity: time.Now(),
	}
}

func (r *RemoteResultChannel) GetIdentifier() string {
	return r.identifier
}

func (r *RemoteResultChannel) SendResult(ctx context.Context, result int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("remote result channel %s is closed", r.identifier)
	}

	if r.sendFunc == nil {
		return fmt.Errorf("send function not configured for remote result channel %s", r.identifier)
	}

	// Call the send function.
	if err := r.sendFunc(ctx, r.identifier, result); err != nil {
		logger.Ctx(ctx).Warn("failed to send result to remote channel",
			zap.String("identifier", r.identifier),
			zap.Int64("result", result),
			zap.Error(err))
		return err
	}

	r.lastActivity = time.Now()
	logger.Ctx(ctx).Debug("sent result to remote channel",
		zap.String("identifier", r.identifier),
		zap.Int64("result", result))

	return nil
}

func (r *RemoteResultChannel) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.closed {
		r.closed = true

		// Call the close function.
		if r.closeFunc != nil {
			if err := r.closeFunc(ctx, r.identifier); err != nil {
				logger.Ctx(ctx).Warn("failed to close remote channel",
					zap.String("identifier", r.identifier),
					zap.Error(err))
			}
		}

		logger.Ctx(ctx).Debug("closed remote result channel",
			zap.String("identifier", r.identifier))
	}
	return nil
}

func (r *RemoteResultChannel) IsClosed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closed
}

// GetLastActivity retrieves the last activity time (used for cleaning up idle connections).
func (r *RemoteResultChannel) GetLastActivity() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastActivity
}
