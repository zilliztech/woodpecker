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

package stagedstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// newEstimateWriter builds the smallest writer CompactionMemoryEstimate reads: the block layout
// and the compaction policy. Nothing is opened, because planning touches neither the file nor
// object storage.
func newEstimateWriter(blockSizes []uint32, targetBlockSize int64, parallelUploads int) *StagedFileWriter {
	w := &StagedFileWriter{
		compactPolicyConfig: &config.SegmentCompactionPolicy{
			MaxBytes:           config.ByteSize(targetBlockSize),
			MaxParallelUploads: parallelUploads,
		},
	}
	var offset int64
	var entryID int64
	for i, size := range blockSizes {
		w.blockIndexes = append(w.blockIndexes, &codec.IndexRecord{
			BlockNumber:  int32(i),
			StartOffset:  offset,
			BlockSize:    size,
			FirstEntryID: entryID,
			LastEntryID:  entryID,
		})
		offset += int64(size)
		entryID++
	}
	w.firstEntryID.Store(0)
	w.lastEntryID.Store(entryID - 1)
	return w
}

// TestCompactionMemoryEstimate_TracksTheSegmentNotTheConfig is why the estimate is planned rather
// than assumed: a segment holding a few hundred KB must cost a few hundred KB of budget, not a
// pool's width of full-sized merged blocks. Billing every segment the worst case would let a node
// draining a backlog of small segments run a fraction of the compactions its memory could carry.
func TestCompactionMemoryEstimate_TracksTheSegmentNotTheConfig(t *testing.T) {
	const mb = int64(1 << 20)

	t.Run("a small segment is charged for itself", func(t *testing.T) {
		// One 200KB block: a single merge task, so only that task can ever be resident.
		w := newEstimateWriter([]uint32{200 << 10}, 2*mb, 4)
		assert.Equal(t, int64(400<<10), w.CompactionMemoryEstimate(0))
	})

	t.Run("a segment large enough to fill the pool is charged the pool's width", func(t *testing.T) {
		// Twelve blocks of ~1MB against a 2MB target: the planner closes a task before it
		// would reach the target, so tasks come out around 1MB and four run at a time.
		sizes := make([]uint32, 12)
		for i := range sizes {
			sizes[i] = uint32(mb)
		}
		w := newEstimateWriter(sizes, 2*mb, 4)

		estimate := w.CompactionMemoryEstimate(int64(len(sizes) - 1))
		assert.Equal(t, 8*mb, estimate, "4 concurrent tasks x 1MB x 2 copies")
	})

	t.Run("the estimate grows with the segment until the pool caps it", func(t *testing.T) {
		var previous int64
		for _, blocks := range []int{1, 2, 3, 4, 8, 16} {
			sizes := make([]uint32, blocks)
			for i := range sizes {
				sizes[i] = uint32(mb)
			}
			w := newEstimateWriter(sizes, 2*mb, 4)
			estimate := w.CompactionMemoryEstimate(int64(blocks - 1))

			assert.GreaterOrEqual(t, estimate, previous, "a bigger segment never costs less")
			assert.LessOrEqual(t, estimate, 8*mb, "and never more than the pool can hold")
			previous = estimate
		}
		assert.Equal(t, 8*mb, previous, "the largest segments reach the cap")
	})

	t.Run("cropping to the completion boundary is reflected", func(t *testing.T) {
		// Entries beyond expectedLastEntryId are not compacted, so they are not charged.
		sizes := make([]uint32, 8)
		for i := range sizes {
			sizes[i] = uint32(mb)
		}
		w := newEstimateWriter(sizes, 2*mb, 4)

		all := w.CompactionMemoryEstimate(7)
		cropped := w.CompactionMemoryEstimate(1)
		assert.Less(t, cropped, all, "a boundary that drops most of the segment must cost less")
	})

	t.Run("nothing to compact costs nothing", func(t *testing.T) {
		w := newEstimateWriter(nil, 2*mb, 4)
		assert.Zero(t, w.CompactionMemoryEstimate(0))
	})
}
