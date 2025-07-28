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
	"testing"
	"time"
)

func TestDefaultRollingPolicy_ShouldRollover(t *testing.T) {
	policy := NewDefaultRollingPolicy(1000, 100, 10) // 1000ms, 100bytes, 10blocks

	// Test case 1: When the current segment size exceeds the rollover size, ShouldRollover should return true
	lastRolloverTimeMs := time.Now().UnixMilli() - 500
	currentSegmentSize := int64(150)
	currentBlocksCount := int64(5)
	if !policy.ShouldRollover(context.TODO(), currentSegmentSize, currentBlocksCount, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true for size rollover, got false")
	}

	// Test case 2: When the current blocks count exceeds the rollover blocks count, ShouldRollover should return true
	currentSegmentSize = int64(50)
	currentBlocksCount = int64(15) // exceeds 10
	lastRolloverTimeMs = time.Now().UnixMilli() - 500
	if !policy.ShouldRollover(context.TODO(), currentSegmentSize, currentBlocksCount, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true for blocks rollover, got false")
	}

	// Test case 3: When the current segment size does not exceed the rollover size, blocks count is within limit, but the last rollover time exceeds the rollover interval, ShouldRollover should return true
	lastRolloverTimeMs = time.Now().UnixMilli() - 1500
	currentSegmentSize = int64(50)
	currentBlocksCount = int64(5)
	if !policy.ShouldRollover(context.TODO(), currentSegmentSize, currentBlocksCount, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true for time rollover, got false")
	}

	// Test case 4: When the current segment size does not exceed the rollover size, blocks count is within limit, and the last rollover time does not exceed the rollover interval, ShouldRollover should return false
	currentSegmentSize = int64(50)
	currentBlocksCount = int64(5)
	lastRolloverTimeMs = time.Now().UnixMilli() - 500
	if policy.ShouldRollover(context.TODO(), currentSegmentSize, currentBlocksCount, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return false, got true")
	}

	// Test case 5: When the current segment size is 0, blocks count is 0, and the last rollover time exceeds the rollover interval, ShouldRollover should return false
	currentSegmentSize = int64(0)
	currentBlocksCount = int64(0)
	lastRolloverTimeMs = time.Now().UnixMilli() - 1500
	if policy.ShouldRollover(context.TODO(), currentSegmentSize, currentBlocksCount, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return false for empty segment, got true")
	}
}

func TestDefaultRollingPolicy_BlocksRollover(t *testing.T) {
	// Test specifically for blocks-based rollover
	policy := NewDefaultRollingPolicy(10000, 1000000, 5) // high time/size limits, low blocks limit

	tests := []struct {
		name               string
		currentSegmentSize int64
		currentBlocksCount int64
		expectedRollover   bool
		description        string
	}{
		{
			name:               "blocks_exactly_at_limit",
			currentSegmentSize: 100,
			currentBlocksCount: 5,
			expectedRollover:   true,
			description:        "Should rollover when blocks count reaches exactly the limit",
		},
		{
			name:               "blocks_exceeds_limit",
			currentSegmentSize: 100,
			currentBlocksCount: 8,
			expectedRollover:   true,
			description:        "Should rollover when blocks count exceeds the limit",
		},
		{
			name:               "blocks_below_limit",
			currentSegmentSize: 100,
			currentBlocksCount: 3,
			expectedRollover:   false,
			description:        "Should not rollover when blocks count is below the limit",
		},
		{
			name:               "zero_blocks",
			currentSegmentSize: 100,
			currentBlocksCount: 0,
			expectedRollover:   false,
			description:        "Should not rollover when blocks count is zero",
		},
	}

	lastRolloverTimeMs := time.Now().UnixMilli() - 100 // recent time to avoid time-based rollover

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := policy.ShouldRollover(context.TODO(), tt.currentSegmentSize, tt.currentBlocksCount, lastRolloverTimeMs)
			if result != tt.expectedRollover {
				t.Errorf("%s: expected %v, got %v", tt.description, tt.expectedRollover, result)
			}
		})
	}
}

func TestNewDefaultRollingPolicy_DefaultValues(t *testing.T) {
	// Test default values when invalid parameters are provided
	policy := NewDefaultRollingPolicy(-1, -1, -1)

	// The policy should still work with default values
	// We can't directly access the private fields, but we can test behavior

	// Test with very large values that should trigger defaults
	currentSegmentSize := int64(1024 * 1024 * 100)                  // 100MB (should exceed 64MB default)
	currentBlocksCount := int64(2000)                               // should exceed 1000 default
	lastRolloverTimeMs := time.Now().UnixMilli() - (11 * 60 * 1000) // 11 minutes ago (should exceed 10min default)

	// Should rollover due to size (exceeding 64MB default)
	if !policy.ShouldRollover(context.TODO(), currentSegmentSize, int64(500), lastRolloverTimeMs) {
		t.Errorf("Expected rollover due to default size limit")
	}

	// Should rollover due to blocks count (exceeding 1000 default)
	if !policy.ShouldRollover(context.TODO(), int64(1024), currentBlocksCount, lastRolloverTimeMs) {
		t.Errorf("Expected rollover due to default blocks limit")
	}

	// Should rollover due to time (exceeding 10min default, with non-empty segment)
	if !policy.ShouldRollover(context.TODO(), int64(1024), int64(500), lastRolloverTimeMs) {
		t.Errorf("Expected rollover due to default time limit")
	}
}
