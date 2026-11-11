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
	"github.com/zilliztech/woodpecker/common/metrics"
)

// MemoryAdmission decides whether this node takes on another segment compaction.
//
// Nothing else does: every other bound is per segment or per log, while the work arrives here from
// as many logs as the node serves, on schedules the node does not control.
//
// It admits or refuses; it does not schedule. Refusing is cheap -- the caller walks on to the next
// replica, and failing that the auditor retries next cycle with the segment still Completed.
//
// Two gates, because they fail in opposite directions. The reservation ceiling moves the instant a
// compaction is taken, which is what stops callers arriving together from all being admitted
// against the same stale memory reading; it is blind to everything that is not compaction. The
// pressure gate reads memory live, which is what notices the rest of the node and an estimate that
// turned out too small; it lags, so it cannot bound a burst on its own.
//
// The ceiling is absolute rather than a share of the pod because useful concurrency saturates on
// object-storage bandwidth long before memory, and because a ratio inherits whatever
// hardware.GetMemoryCount returns -- host memory, when the container limit cannot be read. The
// pressure gate needs that limit, so it runs only where one can be trusted and is skipped
// otherwise; the ceiling holds on its own, so skipping it costs the check and nothing else.
type MemoryAdmission struct {
	// maxInflightBytes is the primary ceiling on reserved memory; non-positive means unbounded.
	maxInflightBytes int64
	// memoryLimitBytes is the node's memory limit, 0 when it could not be trusted. Read once: it
	// does not change, and the read logs a warning when it fails.
	memoryLimitBytes int64
	// highWatermark is the fraction of memoryLimitBytes at which new work stops being taken.
	highWatermark float64

	mu       sync.Mutex
	reserved int64
	tasks    int64

	peakReserved atomic.Int64
	rejectedSize atomic.Int64
	rejectedMem  atomic.Int64
}

// NewMemoryAdmission builds an admission controller. A non-positive maxInflightBytes disables the
// reservation gate; a non-positive memoryLimitBytes or highWatermark disables the pressure gate.
func NewMemoryAdmission(maxInflightBytes int64, memoryLimitBytes int64, highWatermark float64) *MemoryAdmission {
	// Normalise both "off" values to zero so the getters and the gate checks agree on what
	// unbounded looks like, whatever a caller passed.
	if maxInflightBytes < 0 {
		maxInflightBytes = 0
	}
	if memoryLimitBytes < 0 {
		memoryLimitBytes = 0
	}
	return &MemoryAdmission{
		maxInflightBytes: maxInflightBytes,
		memoryLimitBytes: memoryLimitBytes,
		highWatermark:    highWatermark,
	}
}

// NewMemoryAdmissionFromPolicy builds it from the node's configured policy, resolving the memory
// limit once and reporting what it found -- the pressure gate silently not applying is exactly the
// kind of thing that has to be visible at startup rather than inferred later from a metric.
func NewMemoryAdmissionFromPolicy(ctx context.Context, policy *config.SegmentCompactionPolicy) *MemoryAdmission {
	var maxInflight int64
	var watermark float64
	if policy != nil {
		maxInflight = policy.MaxInflightMemory.Int64()
		watermark = policy.MemoryHighWatermark
	}

	limit, trusted := trustedMemoryLimit()
	a := NewMemoryAdmission(maxInflight, limit, watermark)

	logger.Ctx(ctx).Info("compaction memory admission configured",
		zap.Int64("maxInflightBytes", maxInflight),
		zap.Bool("pressureGateEnabled", a.pressureGateEnabled()),
		zap.Int64("memoryLimitBytes", limit),
		zap.Bool("memoryLimitTrusted", trusted),
		zap.Float64("highWatermark", watermark))
	return a
}

// trustedMemoryLimit reports the node's memory limit, and whether it can be believed.
//
// hardware.GetMemoryCount falls back to the host's total when the container limit cannot be read,
// and returns it indistinguishably from a real limit. Sizing a watermark against that on a small
// pod on a large machine would be wrong by orders of magnitude, on every admission, for the life
// of the process -- so a value that is not below host memory is treated as no limit at all.
//
// That lumps together bare metal, a pod with no memory limit, and a genuine detection failure. The
// three do not need separating: the conservative answer -- skip the pressure gate and rely on the
// absolute ceiling -- is right for all of them.
func trustedMemoryLimit() (int64, bool) {
	limit := hardware.GetMemoryCount()
	host := hardware.GetHostMemoryCount()
	if limit == 0 || host == 0 || limit >= host {
		return 0, false
	}
	return int64(limit), true
}

func (a *MemoryAdmission) pressureGateEnabled() bool {
	return a != nil && a.memoryLimitBytes > 0 && a.highWatermark > 0
}

// TryAcquire reserves estimate bytes for a compaction about to start, reporting whether the node
// will take it. Every true must be paired with one Release of the same estimate.
//
// One compaction is always admitted when the node is running none. A ceiling below a single
// compaction's footprint, or memory already over the watermark because of something else entirely,
// would otherwise stop compaction on this node for good -- no segment leaving local disk, no space
// reclaimed, and eventually the disk watermark throttling writes too. Briefly exceeding a bound is
// the lesser failure.
func (a *MemoryAdmission) TryAcquire(estimate int64) bool {
	if a == nil {
		return true
	}
	if estimate < 0 {
		estimate = 0
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.tasks == 0 {
		a.admitUnsafe(estimate)
		return true
	}

	if a.maxInflightBytes > 0 && a.reserved+estimate > a.maxInflightBytes {
		a.rejectedSize.Add(1)
		metrics.WpCompactionAdmissionRejectedTotal.
			WithLabelValues(metrics.NodeID, metrics.CompactionRejectReservation).Inc()
		return false
	}
	if a.pressureGateEnabled() {
		// Read live, because the point is to notice what everything else on the node is using;
		// the reservation total cannot see writes, caches, or anything but compaction.
		used := int64(hardware.GetUsedMemoryCount())
		if used > int64(float64(a.memoryLimitBytes)*a.highWatermark) {
			a.rejectedMem.Add(1)
			metrics.WpCompactionAdmissionRejectedTotal.
				WithLabelValues(metrics.NodeID, metrics.CompactionRejectMemoryPressure).Inc()
			return false
		}
	}

	a.admitUnsafe(estimate)
	return true
}

func (a *MemoryAdmission) admitUnsafe(estimate int64) {
	a.reserved += estimate
	a.tasks++
	if a.reserved > a.peakReserved.Load() {
		a.peakReserved.Store(a.reserved)
	}
}

// Release returns the reservation taken by a matching TryAcquire.
func (a *MemoryAdmission) Release(estimate int64) {
	if a == nil {
		return
	}
	if estimate < 0 {
		estimate = 0
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tasks == 0 {
		// Unreachable while every caller pairs Release with a true from TryAcquire. Doing nothing
		// keeps a stray Release from crediting back memory that was never reserved, which would
		// raise the ceiling silently and for the life of the process.
		return
	}
	a.tasks--
	a.reserved -= estimate
	if a.reserved < 0 || a.tasks == 0 {
		// Nothing is running, so nothing can legitimately be reserved: re-anchor rather than
		// letting mismatched estimates drift the accounting in either direction.
		a.reserved = 0
	}
}

// MaxInflightBytes is the configured reservation ceiling, 0 when unbounded.
func (a *MemoryAdmission) MaxInflightBytes() int64 {
	if a == nil {
		return 0
	}
	return a.maxInflightBytes
}

// MemoryHighWatermark is the memory fraction above which this node stops taking on new
// compactions, 0 when the pressure gate is inactive -- no container memory limit readable,
// or no watermark configured. Reporting the effective value rather than the configured one
// keeps an alert rule that reads it from comparing against a threshold that never applies.
func (a *MemoryAdmission) MemoryHighWatermark() float64 {
	if !a.pressureGateEnabled() {
		return 0
	}
	return a.highWatermark
}

// ReservedBytes is what running compactions currently hold against the ceiling.
func (a *MemoryAdmission) ReservedBytes() int64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reserved
}

// Running is how many compactions hold a reservation right now.
func (a *MemoryAdmission) Running() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return int(a.tasks)
}

// PeakReservedBytes is the highest reservation total seen since the process started. The ceiling
// alone does not say whether it is close to binding; this does, and a 15s scrape of the current
// value misses the peaks it is there to catch.
func (a *MemoryAdmission) PeakReservedBytes() int64 {
	if a == nil {
		return 0
	}
	return a.peakReserved.Load()
}

// Rejected counts compactions refused, split by which gate refused them: the reservation ceiling
// means compaction itself is at capacity, memory pressure means the node is busy with something
// else. They call for different responses, so they are counted apart.
func (a *MemoryAdmission) Rejected() (bySize int64, byMemory int64) {
	if a == nil {
		return 0, 0
	}
	return a.rejectedSize.Load(), a.rejectedMem.Load()
}
