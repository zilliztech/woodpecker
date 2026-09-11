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

package processor

import (
	"context"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/hardware"
	"github.com/zilliztech/woodpecker/common/logger"
)

// fallbackCompactionMemoryBudget is used when the node's memory limit cannot be read and no
// absolute budget is configured. Small enough to be safe on any node that can run a logstore at
// all, and the alternative -- an unbounded budget -- is the state this type exists to end.
const fallbackCompactionMemoryBudget = int64(256 * 1024 * 1024)

// CompactionScheduler bounds how much memory the segment compactions running on one node hold at
// the same time.
//
// Nothing else bounds it. The per-segment guard in Compact stops a single segment compacting
// twice, maxParallelUploads bounds the blocks in flight within one segment, and the auditor's
// per-cycle bounds are per log -- but the work runs here, and a node holds segments belonging to
// many logs, each driven by its own writer's auditor on its own schedule. How many arrive together
// is decided by the clients, not by the node.
//
// The resource to bound is bytes, not tasks. While a merged block is in flight the node holds the
// source blocks it read and the merged block it assembles, and maxParallelUploads of them run at
// once -- so what a compaction costs is set by the segment's own block layout, and each caller
// charges what its plan will hold (Writer.CompactionMemoryEstimate).
//
// Counting tasks instead would fix the wrong quantity twice over: a full segment costs 16MB at a
// 2MB merged-block size and 256MB at 32MB, and a segment holding a few hundred KB costs almost
// nothing at either. A byte budget charged from the plan tracks both.
//
// It peaks exactly when the node can least afford it: after compaction has been failing for a
// while every log has a backlog of Completed segments, and the moment it recovers every auditor
// starts draining at once.
//
// A compaction is refused, never queued. Queuing would spend the caller's per-attempt deadline on
// a wait, and the caller has better things to do with it -- compactSegmentQuorum walks on to the
// next replica, which may have room, and failing that the auditor retries on its next cycle with
// the segment still Completed and nothing lost.
type CompactionScheduler struct {
	// budget is the ceiling in bytes; a non-positive budget means unbounded.
	budget int64

	mu       sync.Mutex
	inFlight int64
	tasks    int64

	peak     atomic.Int64
	rejected atomic.Int64
}

// NewCompactionScheduler returns a scheduler admitting compactions until budgetBytes of estimated
// memory is in flight. A non-positive budget means unbounded.
func NewCompactionScheduler(budgetBytes int64) *CompactionScheduler {
	return &CompactionScheduler{budget: budgetBytes}
}

// NewCompactionSchedulerFromPolicy builds the node's scheduler from its configured policy,
// resolving the budget and reporting how it was arrived at -- the number is derived often enough
// that it has to be in the log, or an operator reading the metric has no way to explain it.
func NewCompactionSchedulerFromPolicy(ctx context.Context, policy *config.SegmentCompactionPolicy) *CompactionScheduler {
	budget, source, nodeMemory := resolveCompactionMemoryBudget(policy)
	logger.Ctx(ctx).Info("compaction memory budget resolved",
		zap.Int64("budgetBytes", budget),
		zap.String("source", source),
		zap.Uint64("nodeMemoryLimitBytes", nodeMemory),
		zap.Float64("ratio", policy.MaxMemoryRatio),
		// What a full segment costs, so the budget can be read as a concurrency without
		// looking the other two settings up. Smaller segments are charged less than this.
		zap.Int64("fullSegmentCompactionBytes", int64(max(policy.MaxParallelUploads, 1))*2*policy.MaxBytes.Int64()))
	return NewCompactionScheduler(budget)
}

// resolveCompactionMemoryBudget turns the policy into a byte ceiling, returning where it came from.
//
// An absolute value wins when set: an operator who wants a fixed number should get exactly that,
// on every node, whatever the pod is sized at. Otherwise it is a fraction of the node's memory
// limit -- inside a container that is the cgroup limit, which is the constraint that actually
// exists, so the default moves with the pod instead of being a number that happens to suit one
// deployment.
func resolveCompactionMemoryBudget(policy *config.SegmentCompactionPolicy) (budget int64, source string, nodeMemory uint64) {
	if policy == nil {
		return fallbackCompactionMemoryBudget, "fallback_no_policy", 0
	}
	if configured := policy.MaxMemoryBytes.Int64(); configured > 0 {
		return configured, "configured_bytes", 0
	}

	nodeMemory = hardware.GetMemoryCount()
	if nodeMemory == 0 || policy.MaxMemoryRatio <= 0 {
		// Detection failed, or the ratio is unusable on a Configuration assembled in code.
		// Falling back to a fixed budget keeps the bound; falling back to unbounded would
		// silently restore the behaviour this exists to replace.
		return fallbackCompactionMemoryBudget, "fallback_undetected", nodeMemory
	}

	budget = int64(float64(nodeMemory) * policy.MaxMemoryRatio)
	if budget <= 0 {
		return fallbackCompactionMemoryBudget, "fallback_ratio_underflow", nodeMemory
	}
	return budget, "node_memory_ratio", nodeMemory
}

// TryAcquire charges estimate against the budget without waiting, reporting whether it fit.
// A nil or unbounded scheduler always admits. Every true must be paired with one Release of the
// same estimate.
//
// One compaction is always admitted when nothing else is running, however large its estimate. A
// budget below a single compaction's footprint is a misconfiguration, and refusing every
// compaction on the node forever -- no segment ever leaving local disk, no space ever reclaimed --
// is a far worse answer to it than briefly exceeding the number.
func (s *CompactionScheduler) TryAcquire(estimate int64) bool {
	if s == nil || s.budget <= 0 {
		return true
	}
	if estimate < 0 {
		estimate = 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight > 0 && s.inFlight+estimate > s.budget {
		s.rejected.Add(1)
		return false
	}
	s.inFlight += estimate
	s.tasks++
	if s.inFlight > s.peak.Load() {
		s.peak.Store(s.inFlight)
	}
	return true
}

// Release returns the estimate charged by a matching TryAcquire.
func (s *CompactionScheduler) Release(estimate int64) {
	if s == nil || s.budget <= 0 {
		return
	}
	if estimate < 0 {
		estimate = 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tasks == 0 {
		// Unreachable while every caller pairs Release with a true from TryAcquire. Doing
		// nothing keeps a stray Release from crediting back memory that was never charged,
		// which would raise the ceiling silently and for the life of the process.
		return
	}
	s.tasks--
	s.inFlight -= estimate
	if s.inFlight < 0 || s.tasks == 0 {
		// Nothing is running, so nothing can legitimately be charged: re-anchor rather than
		// letting mismatched estimates drift the accounting in either direction.
		s.inFlight = 0
	}
}

// BudgetBytes is the resolved ceiling, 0 when unbounded.
func (s *CompactionScheduler) BudgetBytes() int64 {
	if s == nil || s.budget <= 0 {
		return 0
	}
	return s.budget
}

// InFlightBytes is the estimated memory charged to running compactions right now.
func (s *CompactionScheduler) InFlightBytes() int64 {
	if s == nil || s.budget <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFlight
}

// Running is how many compactions hold a charge right now.
func (s *CompactionScheduler) Running() int {
	if s == nil || s.budget <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.tasks)
}

// PeakBytes is the highest in-flight total seen since the process started. The budget alone does
// not say whether it is close to binding; this does.
func (s *CompactionScheduler) PeakBytes() int64 {
	if s == nil {
		return 0
	}
	return s.peak.Load()
}

// Rejected counts compactions refused for want of budget since the process started.
func (s *CompactionScheduler) Rejected() int64 {
	if s == nil {
		return 0
	}
	return s.rejected.Load()
}
