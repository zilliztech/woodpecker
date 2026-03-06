// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package idempotent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateSnapshotFromWindow(t *testing.T) {
	config := DefaultDedupWindowConfig()
	window := NewDedupWindow(config)

	// Add some entries
	window.Put("id-1", 1, 100, EntryStatusCommitted)
	window.Put("id-2", 1, 101, EntryStatusCommitted)
	window.Put("id-3", 2, 50, EntryStatusCommitted)
	window.Put("pending", 2, 51, EntryStatusPending) // Should not be included

	snapshot := CreateSnapshotFromWindow(123, window, config)

	assert.Equal(t, int64(123), snapshot.LogId)
	assert.Len(t, snapshot.Entries, 3) // Only committed entries
	assert.Equal(t, config.WindowDuration, snapshot.Config.WindowDuration)
	assert.Equal(t, config.MaxKeys, snapshot.Config.MaxKeys)

	// Verify watermarks
	assert.Equal(t, int64(101), snapshot.SegmentWatermarks[1])
	assert.Equal(t, int64(51), snapshot.SegmentWatermarks[2])
}

func TestDedupSnapshotManager_toProto_fromProto(t *testing.T) {
	manager := &dedupSnapshotManagerImpl{
		logId:      123,
		bucketName: "test-bucket",
		basePath:   "logs/test",
	}

	original := &DedupSnapshot{
		LogId:             123,
		SnapshotTimestamp: time.Now(),
		Config: DedupWindowConfig{
			WindowDuration: 10 * time.Minute,
			MaxKeys:        100000,
		},
		SegmentWatermarks: map[int64]int64{
			1: 100,
			2: 200,
		},
		Entries: []*DedupEntry{
			{IdempotencyId: "id-1", SegmentId: 1, EntryId: 100, Status: EntryStatusCommitted, Timestamp: time.Now()},
			{IdempotencyId: "id-2", SegmentId: 2, EntryId: 200, Status: EntryStatusCommitted, Timestamp: time.Now()},
		},
	}

	// Convert to proto and back
	pbSnapshot := manager.toProto(original)
	restored := manager.fromProto(pbSnapshot)

	// Verify
	assert.Equal(t, original.LogId, restored.LogId)
	assert.Equal(t, original.Config.WindowDuration, restored.Config.WindowDuration)
	assert.Equal(t, original.Config.MaxKeys, restored.Config.MaxKeys)
	assert.Equal(t, len(original.SegmentWatermarks), len(restored.SegmentWatermarks))
	assert.Equal(t, len(original.Entries), len(restored.Entries))

	for segId, entryId := range original.SegmentWatermarks {
		assert.Equal(t, entryId, restored.SegmentWatermarks[segId])
	}
}

func TestDedupSnapshotManager_GetSnapshotPath(t *testing.T) {
	manager := &dedupSnapshotManagerImpl{
		logId:      123,
		bucketName: "test-bucket",
		basePath:   "logs/test",
	}

	path := manager.GetSnapshotPath()
	assert.Equal(t, "logs/test/dedup_snapshot/123", path)
}

func TestDedupSnapshotManager_getSnapshotObjectKey(t *testing.T) {
	manager := &dedupSnapshotManagerImpl{
		logId:      123,
		bucketName: "test-bucket",
		basePath:   "logs/test",
	}

	timestamp := time.Unix(0, 1234567890)
	key := manager.getSnapshotObjectKey(timestamp)
	assert.Equal(t, "logs/test/dedup_snapshot/123/snapshot_1234567890", key)
}
