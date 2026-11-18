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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/logger"
)

const backgroundRecoveryTimeout = 30 * time.Second

type nodeRecoveryResult struct {
	node        string
	lastEntryId int64
	err         error
}

// enqueueRecovery keeps a replica's Complete behind its Fence without occupying another pool worker.
func (s *segmentHandleImpl) enqueueRecovery(nodes []string, task func(string)) {
	if len(nodes) == 0 {
		return
	}
	s.recoveryMu.Lock()
	if s.recoveryPool == nil {
		s.recoveryPool = conc.NewPool[struct{}](len(nodes), conc.WithDisablePurge(true))
		s.pendingRecovery = make(map[string][]func(string), len(nodes))
	}
	pool := s.recoveryPool
	var ready []string
	for _, node := range nodes {
		if pending, busy := s.pendingRecovery[node]; busy {
			s.pendingRecovery[node] = append(pending, task)
		} else {
			s.pendingRecovery[node] = nil
			ready = append(ready, node)
		}
	}
	s.recoveryMu.Unlock()

	for _, node := range ready {
		pool.Submit(func() (struct{}, error) {
			for next := task; next != nil; next = s.nextRecovery(node) {
				next(node)
			}
			return struct{}{}, nil
		})
	}
}

func (s *segmentHandleImpl) nextRecovery(node string) func(string) {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if pending := s.pendingRecovery[node]; len(pending) > 0 {
		next := pending[0]
		pending[0] = nil
		s.pendingRecovery[node] = pending[1:]
		return next
	}
	delete(s.pendingRecovery, node)
	if len(s.pendingRecovery) == 0 {
		s.recoveryPool.Release()
		s.recoveryPool = nil
	}
	return nil
}

// submitRecovery returns one buffered reply per replica. Releasing the foreground
// wait after quorum gives unfinished tasks a bounded grace period.
func (s *segmentHandleImpl) submitRecovery(parent context.Context, nodes []string, allowEarly bool, operation string,
	rpc func(context.Context, string) (int64, error),
) (<-chan nodeRecoveryResult, func()) {
	ctx := parent
	taskDone, release := func() {}, func() {}
	if allowEarly {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.WithoutCancel(parent))
		var remaining atomic.Int32
		remaining.Store(int32(len(nodes)))
		taskDone = func() {
			if remaining.Add(-1) == 0 {
				cancel()
			}
		}
		release = func() {
			if parent.Err() != nil || ctx.Err() != nil {
				cancel()
				return
			}
			timer := time.AfterFunc(backgroundRecoveryTimeout, cancel)
			context.AfterFunc(ctx, func() { timer.Stop() })
		}
	}

	results := make(chan nodeRecoveryResult, len(nodes))
	s.enqueueRecovery(nodes, func(node string) {
		defer taskDone()
		tail := int64(-1)
		err := ctx.Err()
		if err == nil {
			tail, err = rpc(ctx, node)
		}
		if err != nil {
			logger.Ctx(ctx).Warn("Segment recovery RPC failed", zap.String("operation", operation),
				zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId), zap.String("node", node), zap.Error(err))
		}
		results <- nodeRecoveryResult{node: node, lastEntryId: tail, err: err}
	})
	return results, release
}
