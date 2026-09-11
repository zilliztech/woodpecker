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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/hardware"
)

func TestCompactionScheduler_AdmitsUntilBudgetIsSpent(t *testing.T) {
	const mb = int64(1 << 20)
	s := NewCompactionScheduler(100 * mb)

	require.True(t, s.TryAcquire(40*mb))
	require.True(t, s.TryAcquire(40*mb))
	assert.Equal(t, 80*mb, s.InFlightBytes())
	assert.Equal(t, 2, s.Running())

	assert.False(t, s.TryAcquire(40*mb), "120MB would exceed the 100MB budget")
	assert.Equal(t, int64(1), s.Rejected())
	assert.Equal(t, 80*mb, s.InFlightBytes(), "a refusal must charge nothing")

	assert.True(t, s.TryAcquire(20*mb), "a compaction that still fits must be admitted")
	assert.Equal(t, 100*mb, s.InFlightBytes())

	s.Release(20 * mb)
	s.Release(40 * mb)
	assert.Equal(t, 40*mb, s.InFlightBytes())
	assert.Equal(t, 1, s.Running())
	assert.Equal(t, 100*mb, s.PeakBytes(), "the peak is what says the budget was close to binding")
}

// TestCompactionScheduler_AlwaysAdmitsTheFirstCompaction covers the misconfiguration that would
// otherwise be fatal: a budget below what one compaction costs. Refusing every compaction on the
// node forever -- no segment ever leaving local disk, no space ever reclaimed -- is a far worse
// answer than briefly exceeding the number.
func TestCompactionScheduler_AlwaysAdmitsTheFirstCompaction(t *testing.T) {
	s := NewCompactionScheduler(1 << 20) // 1MB budget
	huge := int64(1 << 30)               // 1GB compaction

	require.True(t, s.TryAcquire(huge), "the node must make progress even on a budget this wrong")
	assert.Equal(t, huge, s.InFlightBytes())

	assert.False(t, s.TryAcquire(1), "the exemption is for the first compaction only")
	assert.Equal(t, int64(1), s.Rejected())

	s.Release(huge)
	assert.Zero(t, s.InFlightBytes())
	assert.True(t, s.TryAcquire(huge), "and again once the node is idle")
}

// TestCompactionScheduler_NonPositiveBudgetIsUnbounded is the guard for a Configuration assembled
// in code. Validation rejects it, but degrading to the previous unbounded behaviour is the only
// safe reading -- a zero budget taken literally would stop compaction on the node entirely.
func TestCompactionScheduler_NonPositiveBudgetIsUnbounded(t *testing.T) {
	for _, budget := range []int64{0, -1} {
		s := NewCompactionScheduler(budget)
		for range 100 {
			require.True(t, s.TryAcquire(1<<30))
		}
		assert.Zero(t, s.Rejected())
		assert.Zero(t, s.BudgetBytes(), "an unbounded scheduler reports no ceiling")
		assert.Zero(t, s.InFlightBytes())
		s.Release(1 << 30)
	}
}

// TestCompactionScheduler_NilIsUnbounded pins the zero value every caller that does not opt in
// gets: a processor built without WithCompactionScheduler compacts exactly as before.
func TestCompactionScheduler_NilIsUnbounded(t *testing.T) {
	var s *CompactionScheduler

	assert.True(t, s.TryAcquire(1<<30))
	assert.NotPanics(t, func() { s.Release(1 << 30) })
	assert.Zero(t, s.Running())
	assert.Zero(t, s.BudgetBytes())
	assert.Zero(t, s.InFlightBytes())
	assert.Zero(t, s.Rejected())
}

// TestCompactionScheduler_StrayReleaseDoesNotRaiseTheCeiling guards the failure a credit-based
// bound has and a mutex does not: an unpaired Release would credit back memory that was never
// charged, and the node would run over budget for the rest of its life.
func TestCompactionScheduler_StrayReleaseDoesNotRaiseTheCeiling(t *testing.T) {
	const mb = int64(1 << 20)
	s := NewCompactionScheduler(100 * mb)

	s.Release(50 * mb) // nothing was charged
	s.Release(50 * mb)
	assert.Zero(t, s.InFlightBytes())

	require.True(t, s.TryAcquire(60*mb))
	assert.False(t, s.TryAcquire(60*mb), "the budget must still be 100MB")
	assert.Equal(t, 60*mb, s.InFlightBytes())
}

// TestCompactionScheduler_IdleResetsTheCharge keeps a mismatched Release from drifting the
// accounting: when nothing is running, nothing can legitimately be charged.
func TestCompactionScheduler_IdleResetsTheCharge(t *testing.T) {
	const mb = int64(1 << 20)
	s := NewCompactionScheduler(100 * mb)

	require.True(t, s.TryAcquire(40*mb))
	s.Release(10 * mb) // caller released less than it charged

	assert.Zero(t, s.Running())
	assert.Zero(t, s.InFlightBytes(), "an idle node holds nothing, whatever the arithmetic said")
	assert.True(t, s.TryAcquire(100*mb), "the full budget is available again")
}

// TestCompactionScheduler_ConcurrentAcquireNeverExceedsBudget is the property the whole type
// exists for, checked under -race: however many callers arrive at once, the charged total never
// passes the budget once more than one compaction is running.
func TestCompactionScheduler_ConcurrentAcquireNeverExceedsBudget(t *testing.T) {
	const mb = int64(1 << 20)
	const budget = 64 * mb
	const estimate = 16 * mb
	const goroutines = 64
	const rounds = 50

	s := NewCompactionScheduler(budget)
	var peak atomic.Int64
	var admitted atomic.Int64

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				if !s.TryAcquire(estimate) {
					continue
				}
				admitted.Add(1)
				now := s.InFlightBytes()
				for {
					high := peak.Load()
					if now <= high || peak.CompareAndSwap(high, now) {
						break
					}
				}
				s.Release(estimate)
			}
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, peak.Load(), budget, "charged memory exceeded the budget")
	assert.Zero(t, s.InFlightBytes(), "every admitted compaction released its charge")
	assert.Zero(t, s.Running())
	assert.Positive(t, admitted.Load(), "the test admitted nothing, so it proved nothing")
	assert.True(t, s.TryAcquire(estimate), "the scheduler is still usable after the storm")
}

func TestResolveCompactionMemoryBudget(t *testing.T) {
	t.Run("an absolute value is used as given", func(t *testing.T) {
		budget, source, _ := resolveCompactionMemoryBudget(&config.SegmentCompactionPolicy{
			MaxMemoryBytes: config.ByteSize(777 << 20),
			MaxMemoryRatio: 0.5,
		})
		assert.Equal(t, int64(777<<20), budget, "an explicit budget must beat the ratio")
		assert.Equal(t, "configured_bytes", source)
	})

	t.Run("otherwise it follows the node's memory limit", func(t *testing.T) {
		nodeMemory := hardware.GetMemoryCount()
		if nodeMemory == 0 {
			t.Skip("node memory limit not detectable here")
		}
		budget, source, reported := resolveCompactionMemoryBudget(&config.SegmentCompactionPolicy{
			MaxMemoryRatio: 0.1,
		})
		assert.Equal(t, "node_memory_ratio", source)
		assert.Equal(t, nodeMemory, reported)
		assert.Equal(t, int64(float64(nodeMemory)*0.1), budget)
		assert.Less(t, budget, int64(nodeMemory), "the budget is a fraction, never the whole node")
	})

	t.Run("an unusable ratio falls back to a bound, never to unbounded", func(t *testing.T) {
		for _, ratio := range []float64{0, -1} {
			budget, source, _ := resolveCompactionMemoryBudget(&config.SegmentCompactionPolicy{
				MaxMemoryRatio: ratio,
			})
			assert.Equal(t, fallbackCompactionMemoryBudget, budget)
			assert.Equal(t, "fallback_undetected", source)
		}
		budget, source, _ := resolveCompactionMemoryBudget(nil)
		assert.Equal(t, fallbackCompactionMemoryBudget, budget)
		assert.Equal(t, "fallback_no_policy", source)
	})

	t.Run("the shipped defaults leave room for a full segment", func(t *testing.T) {
		cfg, err := config.NewConfiguration()
		require.NoError(t, err)
		policy := &cfg.Woodpecker.Logstore.SegmentCompactionPolicy

		budget, _, _ := resolveCompactionMemoryBudget(policy)
		// What a segment large enough to fill the upload pool costs -- the ceiling
		// Writer.CompactionMemoryEstimate approaches, and the most any one compaction charges.
		fullSegment := int64(policy.MaxParallelUploads) * 2 * policy.MaxBytes.Int64()
		require.Positive(t, fullSegment)
		assert.GreaterOrEqual(t, budget, fullSegment,
			"the default budget must fit at least one full compaction, or every node runs on the first-task exemption")
	})
}
