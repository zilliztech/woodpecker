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
	"errors"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/logger"
)

const openWriterReplicaGracePeriod = 30 * time.Second

var errReplicaGracePeriodExpired = errors.New("replica grace period expired after OpenWriter quorum")

type quorumWaitPolicy uint8

const (
	waitForAllReplicas quorumWaitPolicy = iota
	returnOnSafeQuorum
)

type replicaRPCResult struct {
	node        string
	lastEntryId int64
	err         error
}

// enqueueReplicaTasks keeps a replica's Complete behind its Fence without occupying another pool worker.
func (s *segmentHandleImpl) enqueueReplicaTasks(nodes []string, task func(string)) {
	if len(nodes) == 0 {
		return
	}
	s.quorumRPCMu.Lock()
	if s.quorumRPCPool == nil {
		s.quorumRPCPool = conc.NewPool[struct{}](len(nodes), conc.WithDisablePurge(true))
		s.pendingReplicaTasks = make(map[string][]func(string), len(nodes))
	}
	pool := s.quorumRPCPool
	var ready []string
	for _, node := range nodes {
		if pending, busy := s.pendingReplicaTasks[node]; busy {
			s.pendingReplicaTasks[node] = append(pending, task)
		} else {
			s.pendingReplicaTasks[node] = nil
			ready = append(ready, node)
		}
	}
	s.quorumRPCMu.Unlock()

	for _, node := range ready {
		pool.Submit(func() (struct{}, error) {
			for next := task; next != nil; next = s.nextReplicaTask(node) {
				next(node)
			}
			return struct{}{}, nil
		})
	}
}

func (s *segmentHandleImpl) nextReplicaTask(node string) func(string) {
	s.quorumRPCMu.Lock()
	defer s.quorumRPCMu.Unlock()
	if pending := s.pendingReplicaTasks[node]; len(pending) > 0 {
		next := pending[0]
		pending[0] = nil
		s.pendingReplicaTasks[node] = pending[1:]
		return next
	}
	delete(s.pendingReplicaTasks, node)
	if len(s.pendingReplicaTasks) == 0 {
		s.quorumRPCPool.Release()
		s.quorumRPCPool = nil
	}
	return nil
}

// submitQuorumRPCs sends the operation to every quorum member and buffers each reply.
// Releasing the foreground wait after quorum gives unfinished tasks a bounded grace period.
func (s *segmentHandleImpl) submitQuorumRPCs(parent context.Context, nodes []string, allowEarly bool, operation string, targetLAC *int64,
	rpc func(context.Context, string) (int64, error),
) (<-chan replicaRPCResult, func()) {
	ctx := parent
	taskDone, release := func() {}, func() {}
	if allowEarly {
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(context.WithoutCancel(parent))
		var remaining atomic.Int32
		remaining.Store(int32(len(nodes)))
		taskDone = func() {
			if remaining.Add(-1) == 0 {
				cancel(nil)
			}
		}
		release = func() {
			if parent.Err() != nil || ctx.Err() != nil {
				cancel(context.Cause(parent))
				return
			}
			timer := time.AfterFunc(openWriterReplicaGracePeriod, func() { cancel(errReplicaGracePeriodExpired) })
			context.AfterFunc(ctx, func() { timer.Stop() })
		}
	}

	results := make(chan replicaRPCResult, len(nodes))
	log := logger.Ctx(ctx).With(zap.String("operation", operation), zap.String("logName", s.logName),
		zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId))
	if targetLAC != nil {
		log = log.With(zap.Int64("targetLAC", *targetLAC))
	}
	submittedAt := time.Now()
	s.enqueueReplicaTasks(nodes, func(node string) {
		defer taskDone()
		result := replicaRPCResult{node: node, lastEntryId: -1, err: ctx.Err()}
		skipped := result.err != nil
		if !skipped {
			result.lastEntryId, result.err = rpc(ctx, node)
		}
		replicaLog := log.With(zap.String("node", result.node),
			zap.Int64("lastEntryId", result.lastEntryId), zap.Duration("duration", time.Since(submittedAt)))
		switch {
		case result.err != nil:
			replicaLog.Warn("Segment replica RPC failed", zap.Error(result.err),
				zap.Bool("skippedInQueue", skipped), zap.NamedError("cancelCause", context.Cause(ctx)))
		case targetLAC != nil && result.lastEntryId < *targetLAC:
			replicaLog.Warn("Replica finalized but does not locally cover target LAC", zap.Int64("localLastEntryId", result.lastEntryId))
		default:
			replicaLog.Info("Segment replica RPC succeeded")
		}
		results <- result
	})
	return results, release
}
