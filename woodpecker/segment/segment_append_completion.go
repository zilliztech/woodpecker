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
	"container/list"
	"context"
	"strconv"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
)

type appendCompletion struct {
	op  *AppendOp
	err error
}

func (g *segmentAppender) handleReplicaResult(result replicaResult) {
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil {
		g.mu.Unlock()
		return
	}
	record := g.records[result.entryID]
	if record != nil {
		if record.replicasDone[result.replica] {
			g.mu.Unlock()
			return
		}
		record.replicasDone[result.replica] = true
	}
	if result.err != nil {
		g.stopAdmissionUnsafe(werr.ErrSegmentHandleSegmentRolling)
		g.mu.Unlock()
		// Publish the lifecycle failure before queuing a failure callback, without
		// holding the appender lock across the handle's lifecycle lock.
		if g.notify.onReplicaFailure != nil {
			g.notify.onReplicaFailure(result.address, result.err)
		}
		g.mu.Lock()
		if g.closed || g.ctx.Err() != nil {
			g.mu.Unlock()
			return
		}
	}
	element := g.findAppendOpByIDUnsafe(result.entryID)
	if element != nil {
		op := element.Value.(*AppendOp)
		wasCompleted := op.completed.Load()
		op.applyReplicaResult(result.replica, result.err)
		if !wasCompleted && op.completed.Load() && op.terminalErr == nil {
			metrics.WpClientAppendLatency.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(result.elapsed.Milliseconds()))
			metrics.WpClientAppendBytes.WithLabelValues(op.logNs, strconv.FormatInt(op.logId, 10)).Observe(float64(op.Size()))
		}
		if op.terminalErr != nil {
			// A permanent hole also fails its already-acknowledged suffix.
			for e := element.Next(); e != nil; e = e.Next() {
				later := e.Value.(*AppendOp)
				later.terminalErr = op.terminalErr
				later.completed.Store(true)
			}
		}
	}
	lac := g.collectAppendCallbacksUnsafe()
	if record != nil {
		g.reclaimUnsafe(result.entryID, record)
	}
	g.mu.Unlock()

	g.notifyConfirmed(lac)
	g.notifyDrained()
}

func (g *segmentAppender) findAppendOpByIDUnsafe(id int64) *list.Element {
	for e := g.appendOpsQueue.Front(); e != nil; e = e.Next() {
		if e.Value.(*AppendOp).entryId == id {
			return e
		}
	}
	return nil
}

func (g *segmentAppender) completeReadyAppends() {
	g.mu.Lock()
	lac := g.collectAppendCallbacksUnsafe()
	g.mu.Unlock()
	g.notifyConfirmed(lac)
	g.notifyDrained()
}

func (g *segmentAppender) collectAppendCallbacksUnsafe() int64 {
	newLAC := int64(-1)
	for e := g.appendOpsQueue.Front(); e != nil; {
		op := e.Value.(*AppendOp)
		if !op.completed.Load() {
			break
		}
		if op.terminalErr == nil {
			if op.entryId > g.lastAddConfirmed.Load()+1 {
				break
			}
			if op.entryId == g.lastAddConfirmed.Load()+1 {
				g.lastAddConfirmed.Store(op.entryId)
				newLAC = op.entryId
			}
			g.commitedSize.Add(op.Size())
		}
		g.submittedSize.Add(-op.Size())
		g.queueAppendCallbackUnsafe(op, op.terminalErr)
		next := e.Next()
		g.appendOpsQueue.Remove(e)
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(g.logNs, strconv.FormatInt(g.logId, 10)).Dec()
		e = next
	}
	if newLAC >= 0 {
		metrics.SetWriteFrontier(g.logNs, strconv.FormatInt(g.logId, 10), g.segmentId, newLAC)
	}
	return newLAC
}

func (g *segmentAppender) notifyConfirmed(lac int64) {
	if lac >= 0 && g.notify.onConfirmed != nil {
		g.notify.onConfirmed(lac)
	}
}

func (g *segmentAppender) queueAppendCallbackUnsafe(op *AppendOp, err error) {
	g.callbacks = append(g.callbacks, appendCompletion{op: op, err: err})
	if !g.callbackRunning {
		g.callbackRunning = true
		g.callbackWG.Add(1)
		go g.runAppendCallbacks()
	}
}

func (g *segmentAppender) runAppendCallbacks() {
	defer g.callbackWG.Done()
	for {
		g.mu.Lock()
		if len(g.callbacks) == 0 {
			g.callbackRunning = false
			g.mu.Unlock()
			g.notifyDrained()
			return
		}
		completion := g.callbacks[0]
		g.callbacks[0] = appendCompletion{}
		g.callbacks = g.callbacks[1:]
		g.mu.Unlock()
		func() {
			defer g.callbackDone(completion.op.entryId)
			if completion.err == nil {
				completion.op.FastSuccess(context.Background())
			} else {
				completion.op.FastFail(context.Background(), completion.err)
			}
		}()
	}
}

// drained refers to logical writes and callbacks, not outstanding replica I/O.
func (g *segmentAppender) drained() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.appendOpsQueue.Len() == 0 && !g.callbackRunning
}

func (g *segmentAppender) notifyDrained() {
	g.mu.Lock()
	drained := g.admissionStopped && g.appendOpsQueue.Len() == 0 && !g.callbackRunning
	g.mu.Unlock()
	if drained && g.notify.onDrained != nil {
		g.notify.onDrained()
	}
}

// observeFinalLAC accepts a position proven by Fence/Complete without duplicating
// the writable segment's confirmed frontier in the handle.
func (g *segmentAppender) observeFinalLAC(lac int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if lac > g.lastAddConfirmed.Load() {
		g.lastAddConfirmed.Store(lac)
	}
}

// closePending fixes pending callbacks from the final durable prefix, then cancels I/O.
// It never waits or invokes a handle/user callback while the caller holds a segment lock.
func (g *segmentAppender) closePending(lastEntryID int64, err error) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.stopAdmissionUnsafe(err)
	g.admissionErr = err
	g.closed = true
	if lastEntryID > g.lastAddConfirmed.Load() {
		g.lastAddConfirmed.Store(lastEntryID)
	}
	for e := g.appendOpsQueue.Front(); e != nil; {
		next := e.Next()
		op := e.Value.(*AppendOp)
		terminalErr := err
		if lastEntryID >= 0 && op.entryId <= lastEntryID {
			terminalErr = nil
			g.commitedSize.Add(op.Size())
		}
		g.submittedSize.Add(-op.Size())
		g.queueAppendCallbackUnsafe(op, terminalErr)
		g.appendOpsQueue.Remove(e)
		metrics.WpSegmentHandlePendingAppendOps.WithLabelValues(g.logNs, strconv.FormatInt(g.logId, 10)).Dec()
		e = next
	}
	g.mu.Unlock()
	g.stop()
}
