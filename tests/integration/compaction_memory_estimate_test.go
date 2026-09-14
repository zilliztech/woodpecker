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

package integration

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

// TestCompactionMemoryEstimate_HoldsAgainstARealCompaction checks the premise the node's compaction
// admission rests on: that CompactionMemoryEstimate is an upper bound on what a compaction actually
// holds.
//
// Every unit test around admission takes the estimate as given and checks the arithmetic built on
// it. None of them can tell whether the estimate itself is right, and if it is systematically low
// the whole gate is decorative -- the reservation total stays small while real memory climbs, and
// the only thing left is the pressure gate noticing after the fact. So this one runs a real
// compaction against real object storage and watches the heap.
//
// The bound checked is deliberately loose. Go's heap holds garbage until a collection runs, so a
// sampled peak includes whatever the compaction has already finished with, and the sampler shares
// the process with the rest of the test binary. A tight assertion would flake; an order-of-
// magnitude one still catches the failure that matters, which is an estimate that is wrong by a
// factor rather than by a margin.
func TestCompactionMemoryEstimate_HoldsAgainstARealCompaction(t *testing.T) {
	rootDir := fmt.Sprintf("test-compact-mem-estimate-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	// Small blocks so the segment has many of them and the merge plan has real work to do:
	// a single-block segment would be bounded by its own size and prove nothing about the
	// pool-width term of the estimate.
	const blockSize = 256 * 1024
	const entrySize = 200 * 1024
	const entries = 40
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes = 2 * 1024 * 1024
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads = 4

	logId, segmentId := int64(9001), int64(90010)
	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	for i := range entries {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-compact-mem-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), generateStagedTestData(entrySize), resultCh)
		require.NoError(t, err)
		_, err = resultCh.ReadResult(ctx)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(ctx, int64(entries-1))
	require.NoError(t, err)

	estimate := writer.CompactionMemoryEstimate(int64(entries - 1))
	require.Positive(t, estimate, "a segment with %d blocks worth of data must cost something", entries)

	// Sample the heap while the compaction runs. ReadMemStats stops the world, so this is a
	// coarse 5ms cadence rather than a fine one; the peak of a handful of samples is enough to
	// catch an estimate that is out by a factor.
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	compactedSize, err := writer.Compact(ctx, int64(entries-1))
	close(stop)
	<-done
	require.NoError(t, err)
	require.Positive(t, compactedSize)

	growth := int64(0)
	if p := peak.Load(); p > before.HeapAlloc {
		growth = int64(p - before.HeapAlloc)
	}
	t.Logf("estimate=%d bytes, sampled heap growth=%d bytes, ratio=%.2f",
		estimate, growth, float64(growth)/float64(estimate))

	assert.LessOrEqual(t, growth, 4*estimate,
		"compaction held far more than it was charged for; the admission ceiling would not bound anything")

	// And the other direction, loosely: an estimate orders of magnitude above what a compaction
	// touches would refuse work for no reason. This only fires if the formula has lost its
	// footing entirely, not on ordinary over-estimation, which is expected and harmless.
	assert.LessOrEqual(t, estimate, 64*max(growth, blockSize),
		"estimate is far above anything this compaction touched")
}
