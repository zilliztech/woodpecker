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

import "sort"

// CompactionPeakBytes is the peak memory a planned compaction holds, shared by the backends
// because they plan and run compaction the same way: group the segment's blocks into merge tasks,
// run parallel of them at a time, and hold two copies of each running task's bytes -- the source
// blocks it read and the merged block it assembles for upload.
//
// The peak is therefore twice the sum of the largest parallel task spans, since the pool is free
// to schedule any of them together. When a segment has fewer tasks than the pool is wide -- which
// is every small segment -- only those tasks can ever be in flight, and the estimate collapses to
// twice the segment itself rather than twice the pool's width.
func CompactionPeakBytes(taskBytes []int64, parallel int) int64 {
	if len(taskBytes) == 0 {
		return 0
	}
	if parallel <= 0 {
		parallel = 1
	}

	// Sorting a copy: the caller's plan is in segment order and has to stay that way.
	sorted := make([]int64, len(taskBytes))
	copy(sorted, taskBytes)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	if parallel > len(sorted) {
		parallel = len(sorted)
	}

	var peak int64
	for _, size := range sorted[:parallel] {
		if size > 0 {
			peak += size
		}
	}
	return peak * 2
}
