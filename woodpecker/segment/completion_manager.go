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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/proto"
)

// completionManager owns the segment completion lifecycle.
// One per writable segment. Callers trigger completion via TriggerCompletion(),
// and the background goroutine runs fence + complete RPCs with retry, then
// closes writing under the segment lock.
type completionManager struct {
	segment   *segmentHandleImpl
	triggerCh chan struct{} // buffered(1), coalesces multiple signals
	doneCh    chan struct{} // closed when completion finishes
	result    error         // written before doneCh is closed; read after <-doneCh (happens-before guaranteed)
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func newCompletionManager(s *segmentHandleImpl) *completionManager {
	return &completionManager{
		segment:   s,
		triggerCh: make(chan struct{}, 1),
		doneCh:    make(chan struct{}),
	}
}

// Start launches the background goroutine that waits for a completion trigger.
func (cm *completionManager) Start(ctx context.Context) {
	ctx, cm.cancel = context.WithCancel(ctx)
	cm.wg.Add(1)
	go cm.run(ctx)
}

// TriggerCompletion sends a non-blocking signal to start completion.
// Safe to call from hot paths (no lock, no blocking).
// Multiple calls coalesce into a single execution.
func (cm *completionManager) TriggerCompletion() {
	select {
	case cm.triggerCh <- struct{}{}:
	default:
		// Already triggered, signal coalesced
	}
}

// WaitForCompletion blocks until completion finishes (success or max-retry exhausted)
// and returns the final result. Safe to call from multiple goroutines.
func (cm *completionManager) WaitForCompletion() error {
	<-cm.doneCh
	return cm.result
}

// Stop cancels the background goroutine and waits for it to exit.
func (cm *completionManager) Stop() {
	if cm.cancel != nil {
		cm.cancel()
	}
	cm.wg.Wait()
}

func (cm *completionManager) run(ctx context.Context) {
	defer cm.wg.Done()
	select {
	case <-cm.triggerCh:
		cm.result = cm.executeWithRetry(ctx)
		close(cm.doneCh)
	case <-ctx.Done():
		cm.result = ctx.Err()
		close(cm.doneCh)
	}
}

func (cm *completionManager) executeWithRetry(ctx context.Context) error {
	s := cm.segment
	var quorumInfo *proto.QuorumInfo
	var targetLAC int64 = -1

	// Phase 1 (no lock): establish one authoritative target LAC. Fence may be
	// retried, but once it succeeds the target is frozen for all Complete retries.
	prepareErr := retry.Do(ctx, func() error {
		var err error
		quorumInfo, targetLAC, err = s.prepareComplete(ctx)
		return err
	}, retry.Attempts(3), retry.Sleep(500*time.Millisecond), retry.MaxSleepTime(5*time.Second))
	if prepareErr != nil {
		return cm.failAndCloseWithoutMeta(ctx, targetLAC, prepareErr)
	}

	// Phase 2 (no lock): retry only the Complete fanout against the fixed target.
	completeErr := retry.Do(ctx, func() error {
		return s.completePrepared(ctx, quorumInfo, targetLAC)
	}, retry.Attempts(3), retry.Sleep(500*time.Millisecond), retry.MaxSleepTime(5*time.Second))
	if completeErr != nil {
		return cm.failAndCloseWithoutMeta(ctx, targetLAC, completeErr)
	}

	// Phase 3 (with lock): CAS, fast-fail ops, stop executor, update metadata.
	// Reaching this point proves Aq finalized replicas locally cover targetLAC.
	s.Lock()
	defer s.Unlock()
	closeErr := s.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(ctx, targetLAC)
	if closeErr != nil {
		logger.Ctx(ctx).Warn("segment close writing failed",
			zap.String("logName", s.logName),
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segmentId),
			zap.Int64("lastFlushedEntryId", targetLAC),
			zap.Error(closeErr))
	}

	return closeErr
}

func (cm *completionManager) failAndCloseWithoutMeta(ctx context.Context, targetLAC int64, completionErr error) error {
	s := cm.segment
	logger.Ctx(ctx).Warn("segment complete failed after retries",
		zap.String("logName", s.logName),
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segmentId),
		zap.Int64("targetLAC", targetLAC),
		zap.Error(completionErr))
	s.NotifyWriterInvalidation(ctx, fmt.Sprintf("segment:%d complete failed after retries", s.segmentId))

	// Stop this local writer so it cannot accept more appends, but do not publish
	// Completed metadata: the required Aq qualified replicas were not established.
	s.Lock()
	s.doCloseWritingWithoutMetaUnsafe(ctx, targetLAC)
	s.Unlock()
	return completionErr
}
