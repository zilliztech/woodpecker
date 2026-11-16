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
	"strconv"

	"github.com/zilliztech/woodpecker/common/metrics"
)

type appendCompletion struct {
	op  *AppendOp
	err error
}

func (s *segmentHandleImpl) handleReplicaResult(result replicaResult) {
	if result.err != nil && isReplicaUnreachable(result.err) && result.address != "" {
		s.markUnreachable(result.address)
	}
	s.Lock()
	defer s.Unlock()
	if result.err != nil {
		// A final lane failure must roll even if the logical entry was already
		// completed by the other replicas and removed from the pending queue.
		s.rollingState.Store(true)
		if s.appender != nil {
			s.appender.stopAdmission()
		}
	}
	element := s.findAppendOpById(context.Background(), result.entryID)
	if element != nil {
		op := element.Value.(*AppendOp)
		wasCompleted := op.completed.Load()
		op.applyReplicaResult(result.replica, result.err)
		if !wasCompleted && op.completed.Load() && op.terminalErr == nil {
			metrics.WpClientAppendLatency.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(result.elapsed.Milliseconds()))
			metrics.WpClientAppendBytes.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(op.Size()))
		}
		if op.terminalErr != nil {
			// A hole that can no longer reach quorum prevents its suffix from
			// becoming a committed prefix, even if later entries have ACKs.
			for e := element.Next(); e != nil; e = e.Next() {
				later := e.Value.(*AppendOp)
				later.terminalErr = op.terminalErr
				later.completed.Store(true)
			}
		}
	}
	s.collectAppendCallbacksUnsafe(context.Background())
	s.triggerCompletionIfDrainedUnsafe()
}

func (s *segmentHandleImpl) collectAppendCallbacksUnsafe(ctx context.Context) {
	newLAC := int64(-1)
	for e := s.appendOpsQueue.Front(); e != nil; {
		op := e.Value.(*AppendOp)
		if !op.completed.Load() {
			break
		}
		if op.terminalErr == nil {
			if op.entryId > s.lastAddConfirmed.Load()+1 {
				break
			}
			if op.entryId == s.lastAddConfirmed.Load()+1 {
				s.lastAddConfirmed.Store(op.entryId)
				newLAC = op.entryId
			}
			s.commitedSize.Add(op.Size())
		}
		s.submittedSize.Add(-op.Size())
		s.queueAppendCallbackUnsafe(op, op.terminalErr)
		next := e.Next()
		s.appendOpsQueue.Remove(e)
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(s.logNs, strconv.FormatInt(s.logId, 10)).Dec()
		e = next
	}
	if newLAC >= 0 {
		s.syncLAC(ctx, newLAC)
		metrics.SetWriteFrontier(s.logNs, strconv.FormatInt(s.logId, 10), s.segmentId, newLAC)
	}
}

func (s *segmentHandleImpl) queueAppendCallbackUnsafe(op *AppendOp, err error) {
	s.callbacks = append(s.callbacks, appendCompletion{op: op, err: err})
	if !s.callbackRunning {
		s.callbackRunning = true
		s.callbackWG.Add(1)
		go s.runAppendCallbacks()
	}
}

func (s *segmentHandleImpl) runAppendCallbacks() {
	defer s.callbackWG.Done()
	for {
		s.Lock()
		if len(s.callbacks) == 0 {
			s.callbackRunning = false
			s.triggerCompletionIfDrainedUnsafe()
			s.Unlock()
			return
		}
		completion := s.callbacks[0]
		s.callbacks[0] = appendCompletion{}
		s.callbacks = s.callbacks[1:]
		s.Unlock()
		func() {
			if s.appender != nil {
				defer s.appender.callbackDone(completion.op.entryId)
			}
			if completion.err == nil {
				completion.op.FastSuccess(context.Background())
			} else {
				completion.op.FastFail(context.Background(), completion.err)
			}
		}()
	}
}

func (s *segmentHandleImpl) triggerCompletionIfDrainedUnsafe() {
	if s.rollingState.Load() && s.appendOpsQueue.Len() == 0 && !s.callbackRunning && s.completionMgr != nil {
		s.completionMgr.TriggerCompletion()
	}
}
