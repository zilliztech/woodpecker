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

const openWriterReplicaGracePeriod = 30 * time.Second

type quorumWaitPolicy uint8

const (
	waitForAllReplicas quorumWaitPolicy = iota
	returnOnSafeQuorum
)

type completionRPCResult struct {
	node        string
	lastEntryId int64
	err         error
}

// enqueueCompletionTasks keeps a replica's Complete behind its Fence without occupying another pool worker.
func (s *segmentHandleImpl) enqueueCompletionTasks(nodes []string, task func(string)) {
	if len(nodes) == 0 {
		return
	}
	s.completionRPCMu.Lock()
	if s.completionRPCPool == nil {
		s.completionRPCPool = conc.NewPool[struct{}](len(nodes), conc.WithDisablePurge(true))
		s.pendingCompletionTasks = make(map[string][]func(string), len(nodes))
	}
	pool := s.completionRPCPool
	var ready []string
	for _, node := range nodes {
		if pending, busy := s.pendingCompletionTasks[node]; busy {
			s.pendingCompletionTasks[node] = append(pending, task)
		} else {
			s.pendingCompletionTasks[node] = nil
			ready = append(ready, node)
		}
	}
	s.completionRPCMu.Unlock()

	for _, node := range ready {
		pool.Submit(func() (struct{}, error) {
			for next := task; next != nil; next = s.nextCompletionTask(node) {
				next(node)
			}
			return struct{}{}, nil
		})
	}
}

func (s *segmentHandleImpl) nextCompletionTask(node string) func(string) {
	s.completionRPCMu.Lock()
	defer s.completionRPCMu.Unlock()
	if pending := s.pendingCompletionTasks[node]; len(pending) > 0 {
		next := pending[0]
		pending[0] = nil
		s.pendingCompletionTasks[node] = pending[1:]
		return next
	}
	delete(s.pendingCompletionTasks, node)
	if len(s.pendingCompletionTasks) == 0 {
		s.completionRPCPool.Release()
		s.completionRPCPool = nil
	}
	return nil
}

// submitCompletionRPCs returns one buffered reply per replica. Releasing the foreground
// wait after quorum gives unfinished tasks a bounded grace period.
func (s *segmentHandleImpl) submitCompletionRPCs(parent context.Context, nodes []string, allowEarly bool, operation string,
	rpc func(context.Context, string) (int64, error),
) (<-chan completionRPCResult, func()) {
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
			timer := time.AfterFunc(openWriterReplicaGracePeriod, cancel)
			context.AfterFunc(ctx, func() { timer.Stop() })
		}
	}

	results := make(chan completionRPCResult, len(nodes))
	s.enqueueCompletionTasks(nodes, func(node string) {
		defer taskDone()
		tail := int64(-1)
		err := ctx.Err()
		if err == nil {
			tail, err = rpc(ctx, node)
		}
		if err != nil {
			logger.Ctx(ctx).Warn("Segment completion RPC failed", zap.String("operation", operation),
				zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId), zap.String("node", node), zap.Error(err))
		}
		results <- completionRPCResult{node: node, lastEntryId: tail, err: err}
	})
	return results, release
}
