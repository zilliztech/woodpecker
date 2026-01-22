// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package codec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchBlock(t *testing.T) {
	tests := []struct {
		name        string
		blocks      []*IndexRecord
		entryId     int64
		expectedIdx int // Expected index in the blocks slice, -1 if not found
		expectError bool
	}{
		{
			name:        "empty list",
			blocks:      []*IndexRecord{},
			entryId:     100,
			expectedIdx: -1,
			expectError: false,
		},
		{
			name: "single block - entry in range",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
			},
			entryId:     150,
			expectedIdx: 0,
			expectError: false,
		},
		{
			name: "single block - entry at first boundary",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
			},
			entryId:     100,
			expectedIdx: 0,
			expectError: false,
		},
		{
			name: "single block - entry at last boundary",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
			},
			entryId:     200,
			expectedIdx: 0,
			expectError: false,
		},
		{
			name: "single block - entry before range",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
			},
			entryId:     50,
			expectedIdx: -1,
			expectError: false,
		},
		{
			name: "single block - entry after range",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
			},
			entryId:     250,
			expectedIdx: -1,
			expectError: false,
		},
		{
			name: "multiple blocks - found in first block",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 201, LastEntryID: 300},
				{BlockNumber: 2, FirstEntryID: 301, LastEntryID: 400},
			},
			entryId:     150,
			expectedIdx: 0,
			expectError: false,
		},
		{
			name: "multiple blocks - found in middle block",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 201, LastEntryID: 300},
				{BlockNumber: 2, FirstEntryID: 301, LastEntryID: 400},
			},
			entryId:     250,
			expectedIdx: 1,
			expectError: false,
		},
		{
			name: "multiple blocks - found in last block",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 201, LastEntryID: 300},
				{BlockNumber: 2, FirstEntryID: 301, LastEntryID: 400},
			},
			entryId:     350,
			expectedIdx: 2,
			expectError: false,
		},
		{
			name: "multiple blocks - entry before all blocks",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 201, LastEntryID: 300},
				{BlockNumber: 2, FirstEntryID: 301, LastEntryID: 400},
			},
			entryId:     50,
			expectedIdx: -1,
			expectError: false,
		},
		{
			name: "multiple blocks - entry after all blocks",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 201, LastEntryID: 300},
				{BlockNumber: 2, FirstEntryID: 301, LastEntryID: 400},
			},
			entryId:     500,
			expectedIdx: -1,
			expectError: false,
		},
		{
			name: "multiple blocks - entry in gap between blocks",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 250, LastEntryID: 300}, // Gap from 201-249
				{BlockNumber: 2, FirstEntryID: 350, LastEntryID: 400}, // Gap from 301-349
			},
			entryId:     225, // In the gap
			expectedIdx: -1,
			expectError: false,
		},
		{
			name: "blocks with overlapping ranges - should find any valid block",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 200},
				{BlockNumber: 1, FirstEntryID: 150, LastEntryID: 250}, // Overlaps with first block
				{BlockNumber: 2, FirstEntryID: 300, LastEntryID: 400}, // No overlap
			},
			entryId:     175, // Could match blocks 0 or 1
			expectedIdx: -2,  // Special value indicating we should validate differently
			expectError: false,
		},
		{
			name: "large number of blocks",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 99},
				{BlockNumber: 1, FirstEntryID: 100, LastEntryID: 199},
				{BlockNumber: 2, FirstEntryID: 200, LastEntryID: 299},
				{BlockNumber: 3, FirstEntryID: 300, LastEntryID: 399},
				{BlockNumber: 4, FirstEntryID: 400, LastEntryID: 499},
				{BlockNumber: 5, FirstEntryID: 500, LastEntryID: 599},
				{BlockNumber: 6, FirstEntryID: 600, LastEntryID: 699},
				{BlockNumber: 7, FirstEntryID: 700, LastEntryID: 799},
				{BlockNumber: 8, FirstEntryID: 800, LastEntryID: 899},
				{BlockNumber: 9, FirstEntryID: 900, LastEntryID: 999},
			},
			entryId:     555,
			expectedIdx: 5,
			expectError: false,
		},
		{
			name: "boundary case - exact match at the middle block boundaries",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 199},
				{BlockNumber: 1, FirstEntryID: 200, LastEntryID: 299},
				{BlockNumber: 2, FirstEntryID: 300, LastEntryID: 399},
			},
			entryId:     200, // Exact match at boundary
			expectedIdx: 1,
			expectError: false,
		},
		{
			name: "boundary case - exact match at the first block boundaries",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 199},
				{BlockNumber: 1, FirstEntryID: 200, LastEntryID: 299},
				{BlockNumber: 2, FirstEntryID: 300, LastEntryID: 399},
			},
			entryId:     100, // Exact match at boundary
			expectedIdx: 0,
			expectError: false,
		},
		{
			name: "boundary case - exact match at the first block boundaries",
			blocks: []*IndexRecord{
				{BlockNumber: 0, FirstEntryID: 100, LastEntryID: 199},
				{BlockNumber: 1, FirstEntryID: 200, LastEntryID: 299},
				{BlockNumber: 2, FirstEntryID: 300, LastEntryID: 399},
			},
			entryId:     399, // Exact match at boundary
			expectedIdx: 2,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SearchBlock(tt.blocks, tt.entryId)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.expectedIdx == -1 {
				// Should not find a matching block
				if result != nil {
					// If result is not nil, it should not contain the entryId
					assert.False(t,
						result.FirstEntryID <= tt.entryId && tt.entryId <= result.LastEntryID,
						"Found block should not contain the entryId when no match expected")
				}
			} else if tt.expectedIdx == -2 {
				// Special case for overlapping ranges - should find any valid block
				require.NotNil(t, result, "Expected to find a block but got nil")

				// Verify the found block actually contains the entryId
				assert.True(t,
					result.FirstEntryID <= tt.entryId && tt.entryId <= result.LastEntryID,
					"Found block should contain the entryId")

				// Additionally verify it's one of the expected valid blocks
				foundValidBlock := false
				for _, block := range tt.blocks {
					if block.FirstEntryID <= tt.entryId && tt.entryId <= block.LastEntryID {
						if block == result {
							foundValidBlock = true
							break
						}
					}
				}
				assert.True(t, foundValidBlock, "Found block should be one of the valid containing blocks")
			} else {
				// Should find the expected block
				require.NotNil(t, result, "Expected to find a block but got nil")
				expectedBlock := tt.blocks[tt.expectedIdx]
				assert.Equal(t, expectedBlock, result, "Found different block than expected")

				// Verify the found block actually contains the entryId
				assert.True(t,
					result.FirstEntryID <= tt.entryId && tt.entryId <= result.LastEntryID,
					"Found block should contain the entryId")
			}
		})
	}
}

func TestSearchBlock_PerformanceCharacteristics(t *testing.T) {
	// Test that the binary search algorithm works correctly with a large dataset
	const numBlocks = 10000
	blocks := make([]*IndexRecord, numBlocks)

	for i := 0; i < numBlocks; i++ {
		blocks[i] = &IndexRecord{
			BlockNumber:  int32(i),
			FirstEntryID: int64(i * 1000),
			LastEntryID:  int64(i*1000 + 999),
		}
	}

	// Test finding blocks at different positions
	testCases := []struct {
		name        string
		entryId     int64
		expectedIdx int
	}{
		{"first block", 500, 0},
		{"middle block", 5000500, 5000},
		{"last block", 9999500, 9999},
		{"boundary case", 5000000, 5000}, // Exact first entry of a block
		{"boundary case", 4999999, 4999}, // Exact last entry of a block
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := SearchBlock(blocks, tc.entryId)
			require.NoError(t, err)
			require.NotNil(t, result)

			expectedBlock := blocks[tc.expectedIdx]
			assert.Equal(t, expectedBlock, result)
			assert.True(t,
				result.FirstEntryID <= tc.entryId && tc.entryId <= result.LastEntryID,
				"Found block should contain the entryId")
		})
	}
}

func TestSearchBlock_EdgeCasesWithNegativeValues(t *testing.T) {
	blocks := []*IndexRecord{
		{BlockNumber: 0, FirstEntryID: -100, LastEntryID: -1},
		{BlockNumber: 1, FirstEntryID: 0, LastEntryID: 99},
		{BlockNumber: 2, FirstEntryID: 100, LastEntryID: 199},
	}

	tests := []struct {
		name        string
		entryId     int64
		expectedIdx int
	}{
		{"negative entry in range", -50, 0},
		{"zero entry", 0, 1},
		{"positive entry", 150, 2},
		{"negative entry out of range", -200, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SearchBlock(blocks, tt.entryId)
			require.NoError(t, err)

			if tt.expectedIdx == -1 {
				if result != nil {
					assert.False(t,
						result.FirstEntryID <= tt.entryId && tt.entryId <= result.LastEntryID,
						"Found block should not contain the entryId when no match expected")
				}
			} else {
				require.NotNil(t, result)
				expectedBlock := blocks[tt.expectedIdx]
				assert.Equal(t, expectedBlock, result)
			}
		})
	}
}

func TestSearchBlock_OnlyOneBlk(t *testing.T) {
	blocks := []*IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 0},
	}

	tests := []struct {
		name        string
		entryId     int64
		expectedIdx int
	}{
		{"zero entry", 0, 0},
		{"positive entry", 1, -1},
		{"negative entry out of range", -200, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SearchBlock(blocks, tt.entryId)
			require.NoError(t, err)

			if tt.expectedIdx == -1 {
				if result != nil {
					assert.False(t,
						result.FirstEntryID <= tt.entryId && tt.entryId <= result.LastEntryID,
						"Found block should not contain the entryId when no match expected")
				}
			} else {
				require.NotNil(t, result)
				expectedBlock := blocks[tt.expectedIdx]
				assert.Equal(t, expectedBlock, result)
			}
		})
	}
}
