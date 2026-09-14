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

const mb = int64(1 << 20)

// noPressureGate builds an admission controller with only the reservation gate, which is the
// configuration every node without a readable container limit runs.
func noPressureGate(maxInflight int64) *MemoryAdmission {
	return NewMemoryAdmission(maxInflight, 0, 0)
}

func TestMemoryAdmission_ReservationGate(t *testing.T) {
	a := noPressureGate(100 * mb)

	require.True(t, a.TryAcquire(40*mb))
	require.True(t, a.TryAcquire(40*mb))
	assert.Equal(t, 80*mb, a.ReservedBytes())
	assert.Equal(t, 2, a.Running())

	assert.False(t, a.TryAcquire(40*mb), "120MB would exceed the 100MB ceiling")
	bySize, byMem := a.Rejected()
	assert.Equal(t, int64(1), bySize)
	assert.Zero(t, byMem, "the pressure gate is off, so it cannot be the one refusing")
	assert.Equal(t, 80*mb, a.ReservedBytes(), "a refusal must reserve nothing")

	require.True(t, a.TryAcquire(20*mb), "a compaction that still fits must be taken")
	assert.Equal(t, 100*mb, a.ReservedBytes())

	a.Release(20 * mb)
	a.Release(40 * mb)
	assert.Equal(t, 40*mb, a.ReservedBytes())
	assert.Equal(t, 1, a.Running())
	assert.Equal(t, 100*mb, a.PeakReservedBytes(), "the peak is what says the ceiling was close to binding")
}

// TestMemoryAdmission_PressureGate covers the second gate: the node is busy with something that is
// not compaction, so the reservation total is near zero and only a live reading can see it.
func TestMemoryAdmission_PressureGate(t *testing.T) {
	used := int64(hardware.GetUsedMemoryCount())
	require.Positive(t, used, "this test needs a readable memory usage")

	t.Run("refuses once usage is over the watermark", func(t *testing.T) {
		// A limit low enough that current usage is already above the watermark.
		a := NewMemoryAdmission(1<<40, used, 0.5)
		require.True(t, a.TryAcquire(1), "the first is always taken, whatever memory says")
		assert.False(t, a.TryAcquire(1), "usage is over the watermark, so nothing more is taken")

		bySize, byMem := a.Rejected()
		assert.Zero(t, bySize, "the ceiling is 1TB, so it cannot be the one refusing")
		assert.Equal(t, int64(1), byMem)
	})

	t.Run("admits while usage is under the watermark", func(t *testing.T) {
		a := NewMemoryAdmission(1<<40, used*100, 0.9)
		require.True(t, a.TryAcquire(1))
		assert.True(t, a.TryAcquire(1), "plenty of headroom, both gates open")
	})

	t.Run("skipped entirely without a trustworthy limit", func(t *testing.T) {
		a := NewMemoryAdmission(1<<40, 0, 0.5)
		assert.False(t, a.pressureGateEnabled())
		require.True(t, a.TryAcquire(1))
		assert.True(t, a.TryAcquire(1), "no limit means no pressure gate, not a closed one")
	})
}

// TestMemoryAdmission_AlwaysTakesTheFirst covers the misconfiguration and the bad-neighbour case
// together. Refusing every compaction on the node -- no segment leaving local disk, no space
// reclaimed, and eventually the disk watermark throttling writes as well -- is worse than briefly
// exceeding a bound.
func TestMemoryAdmission_AlwaysTakesTheFirst(t *testing.T) {
	t.Run("a ceiling below one compaction", func(t *testing.T) {
		a := noPressureGate(1 * mb)
		require.True(t, a.TryAcquire(1<<30), "the node must make progress even on a ceiling this wrong")
		assert.False(t, a.TryAcquire(1), "the exemption is for the first only")

		a.Release(1 << 30)
		assert.True(t, a.TryAcquire(1<<30), "and again once the node is idle")
	})

	t.Run("memory already over the watermark for unrelated reasons", func(t *testing.T) {
		used := int64(hardware.GetUsedMemoryCount())
		require.Positive(t, used)
		a := NewMemoryAdmission(1<<40, used, 0.5)
		assert.True(t, a.TryAcquire(mb), "compaction must not be stopped outright by someone else's memory")
	})
}

// TestMemoryAdmission_Unbounded pins the two ways a caller ends up with no bound at all: a
// non-positive ceiling from a Configuration assembled in code, and a processor built without the
// option, which is every caller other than the logstore.
func TestMemoryAdmission_Unbounded(t *testing.T) {
	t.Run("non-positive ceiling", func(t *testing.T) {
		for _, ceiling := range []int64{0, -1} {
			a := noPressureGate(ceiling)
			for range 100 {
				require.True(t, a.TryAcquire(1<<30))
			}
			bySize, byMem := a.Rejected()
			assert.Zero(t, bySize)
			assert.Zero(t, byMem)
			assert.Zero(t, a.MaxInflightBytes())
		}
	})

	t.Run("nil", func(t *testing.T) {
		var a *MemoryAdmission
		assert.True(t, a.TryAcquire(1<<30))
		assert.NotPanics(t, func() { a.Release(1 << 30) })
		assert.Zero(t, a.Running())
		assert.Zero(t, a.MaxInflightBytes())
		assert.Zero(t, a.ReservedBytes())
		bySize, byMem := a.Rejected()
		assert.Zero(t, bySize)
		assert.Zero(t, byMem)
	})
}

// TestMemoryAdmission_StrayReleaseDoesNotRaiseTheCeiling guards the failure a credit-based bound
// has and a mutex does not: an unpaired Release would credit back memory that was never reserved,
// and the node would run over its ceiling for the rest of its life.
func TestMemoryAdmission_StrayReleaseDoesNotRaiseTheCeiling(t *testing.T) {
	a := noPressureGate(100 * mb)

	a.Release(50 * mb) // nothing was reserved
	a.Release(50 * mb)
	assert.Zero(t, a.ReservedBytes())

	require.True(t, a.TryAcquire(60*mb))
	assert.False(t, a.TryAcquire(60*mb), "the ceiling must still be 100MB")
	assert.Equal(t, 60*mb, a.ReservedBytes())
}

// TestMemoryAdmission_IdleResetsTheReservation keeps a mismatched Release from drifting the
// accounting: when nothing is running, nothing can legitimately be reserved.
func TestMemoryAdmission_IdleResetsTheReservation(t *testing.T) {
	a := noPressureGate(100 * mb)

	require.True(t, a.TryAcquire(40*mb))
	a.Release(10 * mb) // caller released less than it reserved

	assert.Zero(t, a.Running())
	assert.Zero(t, a.ReservedBytes(), "an idle node holds nothing, whatever the arithmetic said")
	assert.True(t, a.TryAcquire(100*mb), "the full ceiling is available again")
}

// TestMemoryAdmission_ConcurrentAcquireNeverExceedsTheCeiling is the property the reservation gate
// exists for, and the reason a live memory reading cannot replace it: memory is a lagging signal,
// so callers arriving together would all see the same low reading and all be admitted. The
// reservation moves the instant a compaction is taken.
func TestMemoryAdmission_ConcurrentAcquireNeverExceedsTheCeiling(t *testing.T) {
	const ceiling = 64 * mb
	const estimate = 16 * mb

	a := noPressureGate(ceiling)
	var peak atomic.Int64
	var admitted atomic.Int64

	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if !a.TryAcquire(estimate) {
					continue
				}
				admitted.Add(1)
				now := a.ReservedBytes()
				for {
					high := peak.Load()
					if now <= high || peak.CompareAndSwap(high, now) {
						break
					}
				}
				a.Release(estimate)
			}
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, peak.Load(), ceiling, "reserved memory exceeded the ceiling")
	assert.Zero(t, a.ReservedBytes(), "every admitted compaction released its reservation")
	assert.Zero(t, a.Running())
	assert.Positive(t, admitted.Load(), "the test admitted nothing, so it proved nothing")
	assert.True(t, a.TryAcquire(estimate), "still usable after the storm")
}

// TestTrustedMemoryLimit pins the check that keeps a misread limit from inflating the pressure
// gate. GetMemoryCount returns host memory when the container limit cannot be read, and returns it
// indistinguishably -- sizing a watermark against that on a small pod would be wrong by orders of
// magnitude, on every admission, for the life of the process.
func TestTrustedMemoryLimit(t *testing.T) {
	limit, trusted := trustedMemoryLimit()
	host := int64(hardware.GetHostMemoryCount())
	require.Positive(t, host, "this test needs a readable host memory size")

	if trusted {
		assert.Positive(t, limit)
		assert.Less(t, limit, host, "a trusted limit is strictly below host memory, or it is not a limit")
	} else {
		assert.Zero(t, limit, "an untrusted limit must be reported as none, never as host memory")
	}

	// Whatever this machine is, the answer must not be "host memory is the pod's limit".
	assert.NotEqual(t, host, limit)
}

func TestNewMemoryAdmissionFromPolicy(t *testing.T) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	policy := &cfg.Woodpecker.Logstore.SegmentCompactionPolicy

	a := NewMemoryAdmissionFromPolicy(t.Context(), policy)
	assert.Equal(t, policy.MaxInflightMemory.Int64(), a.MaxInflightBytes())

	// The shipped ceiling must fit more than one full-segment compaction, or every node would be
	// running on the first-compaction exemption rather than on the bound.
	//
	// How much more depends on the merged block size, and the two shipped values differ by 16x:
	// at the 2MB woodpecker.yaml sets, one compaction is 16MB and this admits dozens; at the 32MB
	// the in-code default carries, one is 256MB and it admits a handful. Both are workable, and a
	// handful is still a bound rather than an exemption, so this asserts the floor that holds
	// either way rather than a number that only holds for one of them.
	fullSegment := int64(policy.MaxParallelUploads) * 2 * policy.MaxBytes.Int64()
	require.Positive(t, fullSegment)
	assert.GreaterOrEqual(t, a.MaxInflightBytes(), 2*fullSegment)

	t.Run("a nil policy still yields a usable controller", func(t *testing.T) {
		a := NewMemoryAdmissionFromPolicy(t.Context(), nil)
		assert.Zero(t, a.MaxInflightBytes())
		assert.True(t, a.TryAcquire(mb), "unbounded, not closed")
	})
}
