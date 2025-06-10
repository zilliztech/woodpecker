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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// ResultChannelManager manages the lifecycle of remote result channels
type ResultChannelManager struct {
	mu       sync.RWMutex
	channels map[string]*RemoteResultChannel
}

// NewResultChannelManager creates a new result channel manager
func NewResultChannelManager() *ResultChannelManager {
	return &ResultChannelManager{
		channels: make(map[string]*RemoteResultChannel),
	}
}

// RegisterChannel registers a remote result channel
func (m *ResultChannelManager) RegisterChannel(channel *RemoteResultChannel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channels[channel.GetIdentifier()] = channel
}

// UnregisterChannel unregisters a remote result channel
func (m *ResultChannelManager) UnregisterChannel(identifier string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.channels, identifier)
}

// GetChannel retrieves a remote result channel
func (m *ResultChannelManager) GetChannel(identifier string) (*RemoteResultChannel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channel, exists := m.channels[identifier]
	return channel, exists
}

// CleanupIdleChannels cleans up idle remote result channels
func (m *ResultChannelManager) CleanupIdleChannels(ctx context.Context, maxIdleTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var toRemove []string

	for identifier, channel := range m.channels {
		if now.Sub(channel.GetLastActivity()) > maxIdleTime {
			toRemove = append(toRemove, identifier)
			channel.Close(ctx)
		}
	}

	for _, identifier := range toRemove {
		delete(m.channels, identifier)
		logger.Ctx(ctx).Debug("cleaned up idle remote result channel",
			zap.String("identifier", identifier))
	}

	if len(toRemove) > 0 {
		logger.Ctx(ctx).Info("cleaned up idle remote result channels",
			zap.Int("count", len(toRemove)))
	}
}
