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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompactionPeakBytes(t *testing.T) {
	const mb = int64(1 << 20)

	t.Run("more tasks than the pool: only the largest few are ever resident", func(t *testing.T) {
		// Eight 2MB tasks through a pool of four: at most four run together.
		tasks := []int64{2 * mb, 2 * mb, 2 * mb, 2 * mb, 2 * mb, 2 * mb, 2 * mb, 2 * mb}
		assert.Equal(t, 16*mb, CompactionPeakBytes(tasks, 4), "4 tasks x 2MB x 2 copies")
	})

	t.Run("fewer tasks than the pool: the segment itself is the ceiling", func(t *testing.T) {
		// This is the small-segment case the estimate exists for. A fixed worst case would
		// charge a pool's width of full blocks; the real answer is twice this segment.
		tasks := []int64{200 << 10}
		assert.Equal(t, int64(400<<10), CompactionPeakBytes(tasks, 4))

		tasks = []int64{100 << 10, 150 << 10}
		assert.Equal(t, int64(500<<10), CompactionPeakBytes(tasks, 4))
	})

	t.Run("the largest tasks are the ones counted", func(t *testing.T) {
		// The pool may schedule any of them together, so the worst case is the biggest few --
		// not the first few, which is the order the plan happens to be in.
		tasks := []int64{1 * mb, 5 * mb, 1 * mb, 4 * mb, 1 * mb}
		assert.Equal(t, 18*mb, CompactionPeakBytes(tasks, 2), "(5+4) x 2")
	})

	t.Run("the caller's plan is left in its own order", func(t *testing.T) {
		tasks := []int64{1 * mb, 5 * mb, 2 * mb}
		CompactionPeakBytes(tasks, 2)
		assert.Equal(t, []int64{1 * mb, 5 * mb, 2 * mb}, tasks,
			"sorting the caller's slice would reorder the merge plan itself")
	})

	t.Run("degenerate inputs", func(t *testing.T) {
		assert.Zero(t, CompactionPeakBytes(nil, 4))
		assert.Zero(t, CompactionPeakBytes([]int64{}, 4))
		assert.Equal(t, 4*mb, CompactionPeakBytes([]int64{2 * mb, 1 * mb}, 0),
			"a non-positive pool width still runs one task at a time")
		assert.Equal(t, 4*mb, CompactionPeakBytes([]int64{2 * mb, -1}, 4),
			"a negative span cannot subtract from the estimate")
	})
}
