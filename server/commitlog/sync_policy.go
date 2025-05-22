// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package commitlog

import (
	"time"
)

// SyncPolicy defines the interface for different sync policies.
type SyncPolicy interface {
	ShouldSync() bool
}

// PeriodicSyncPolicy syncs at regular intervals.
type PeriodicSyncPolicy struct {
	interval time.Duration
	lastSync time.Time
}

func NewPeriodicSyncPolicy(interval time.Duration) *PeriodicSyncPolicy {
	return &PeriodicSyncPolicy{
		interval: interval,
		lastSync: time.Now(),
	}
}

func (p *PeriodicSyncPolicy) ShouldSync() bool {
	if time.Since(p.lastSync) >= p.interval {
		p.lastSync = time.Now()
		return true
	}
	return false
}

// CountSyncPolicy syncs after a certain number of appends.
type CountSyncPolicy struct {
	count    int
	maxCount int
}

func NewCountSyncPolicy(maxCount int) *CountSyncPolicy {
	return &CountSyncPolicy{
		maxCount: maxCount,
	}
}

func (c *CountSyncPolicy) ShouldSync() bool {
	c.count++
	if c.count >= c.maxCount {
		c.count = 0
		return true
	}
	return false
}
