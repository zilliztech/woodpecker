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

package log

import (
	"context"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

// publishPendingAppends reports a writable segment's submit queue on the auditor's tick.
//
// It runs first in the tick, ahead of every step that can fail into a continue or block on a
// hung dependency, and it reads the handle without taking its lock. Both matter for the same
// reason: these numbers describe a stall, and a stall is exactly when the metadata calls above
// start failing and when AppendAsync is sitting on the write lock waiting for a queue that has
// stopped draining. Sampled anywhere later, or behind that lock, they would freeze at the moment
// they began to matter.
//
// With no writable segment the series are dropped rather than left as they were: a log between
// segments has no queue, and a gauge holding the last value sampled mid-stall goes on firing an
// age threshold against a queue that no longer exists.
func publishPendingAppends(ctx context.Context, logNs, logIdStr string, handle segment.SegmentHandle) {
	if handle == nil {
		metrics.ClearPendingAppends(logNs, logIdStr)
		return
	}
	pending, oldest, lastPushed, _ := handle.PendingAppendStats()
	metrics.SetPendingAppends(logNs, logIdStr, pending, oldest)
	metrics.SetSubmittedFrontier(logNs, logIdStr, handle.GetId(ctx), lastPushed)
}
