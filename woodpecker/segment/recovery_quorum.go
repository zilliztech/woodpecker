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

package segment

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/proto"
)

// recoveryQuorum belongs to one recovery. Complete retries share the original Fence attempts.
type recoveryQuorum struct {
	*proto.QuorumInfo
	fenceDone []chan struct{} // per node, only for quorums that can finish Fence early
}

type nodeRecoveryResult struct {
	node        string
	lastEntryId int64
	err         error
}

const backgroundRecoveryTimeout = 30 * time.Second

type recoveryFanout struct {
	ctx     context.Context
	cancel  context.CancelFunc
	results chan nodeRecoveryResult
	pending int // unread replies handed off after a successful early return
}

func newRecoveryFanout(ctx context.Context, nodes int, canReturnEarly bool) *recoveryFanout {
	f := &recoveryFanout{ctx: ctx, results: make(chan nodeRecoveryResult, nodes)}
	if canReturnEarly {
		// The foreground collector observes the caller's cancellation until quorum.
		// Once it succeeds, its caller may cancel while the remaining RPCs finish.
		f.ctx, f.cancel = context.WithCancel(context.WithoutCancel(ctx))
	}
	return f
}

func (f *recoveryFanout) finish(operation string, logID, segmentID int64) {
	if f.cancel == nil {
		return
	}
	if f.pending == 0 {
		f.cancel()
		return
	}
	timer := time.NewTimer(backgroundRecoveryTimeout)
	go func() {
		defer f.cancel()
		defer timer.Stop()
		for remaining := f.pending; remaining > 0; remaining-- {
			select {
			case result := <-f.results:
				if result.err != nil {
					logger.Ctx(f.ctx).Warn("Background segment recovery RPC failed", zap.String("operation", operation),
						zap.Int64("logId", logID), zap.Int64("segmentId", segmentID), zap.String("node", result.node), zap.Error(result.err))
				}
			case <-timer.C:
				logger.Ctx(f.ctx).Warn("Background segment recovery timed out", zap.String("operation", operation),
					zap.Int64("logId", logID), zap.Int64("segmentId", segmentID), zap.Int("pending", remaining))
				return
			}
		}
	}()
}
