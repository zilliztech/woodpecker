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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupWindow_BasicOperations(t *testing.T) {
	config := DedupWindowConfig{
		WindowDuration: 10 * time.Minute,
		MaxKeys:        1000,
	}
	window := NewDedupWindow(config)

	// Test Put and Get
	entry, isNew := window.Put("id-1", 1, 100, EntryStatusCommitted)
	assert.True(t, isNew)
	assert.Equal(t, "id-1", entry.IdempotencyId)
	assert.Equal(t, int64(1), entry.SegmentId)
	assert.Equal(t, int64(100), entry.EntryId)
	assert.Equal(t, EntryStatusCommitted, entry.Status)

	// Get existing entry
	retrieved := window.Get("id-1")
	require.NotNil(t, retrieved)
	assert.Equal(t, entry.IdempotencyId, retrieved.IdempotencyId)

	// Get non-existing entry
	missing := window.Get("non-existing")
	assert.Nil(t, missing)

	// Put duplicate should return existing entry
	entry2, isNew2 := window.Put("id-1", 2, 200, EntryStatusPending)
	assert.False(t, isNew2)
	assert.Equal(t, entry.SegmentId, entry2.SegmentId) // Should be original values
	assert.Equal(t, entry.EntryId, entry2.EntryId)
}

func TestDedupWindow_SegmentWatermarks(t *testing.T) {
	config := DefaultDedupWindowConfig()
	window := NewDedupWindow(config)

	// Add entries to different segments
	window.Put("id-1", 1, 100, EntryStatusCommitted)
	window.Put("id-2", 1, 200, EntryStatusCommitted)
	window.Put("id-3", 2, 50, EntryStatusCommitted)
	window.Put("id-4", 2, 150, EntryStatusCommitted)

	watermarks := window.GetSegmentWatermarks()
	assert.Equal(t, int64(200), watermarks[1])
	assert.Equal(t, int64(150), watermarks[2])
}

func TestDedupWindow_GetCommittedEntries(t *testing.T) {
	config := DefaultDedupWindowConfig()
	window := NewDedupWindow(config)

	// Add mix of committed and pending entries
	window.Put("committed-1", 1, 100, EntryStatusCommitted)
	window.Put("pending-1", 1, 101, EntryStatusPending)
	window.Put("committed-2", 1, 102, EntryStatusCommitted)

	committed := window.GetCommittedEntries()
	assert.Len(t, committed, 2)

	// Verify only committed entries are returned
	for _, entry := range committed {
		assert.Equal(t, EntryStatusCommitted, entry.Status)
	}
}

func TestDedupWindow_TimeBasedEviction(t *testing.T) {
	config := DedupWindowConfig{
		WindowDuration: 100 * time.Millisecond,
		MaxKeys:        1000,
	}
	window := NewDedupWindow(config)

	// Add an entry
	window.Put("id-1", 1, 100, EntryStatusCommitted)
	assert.Equal(t, 1, window.Len())

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Evict should remove expired entry
	evicted := window.Evict()
	assert.Equal(t, 1, evicted)
	assert.Equal(t, 0, window.Len())
}

func TestDedupWindow_PendingEntriesNotEvicted(t *testing.T) {
	config := DedupWindowConfig{
		WindowDuration: 100 * time.Millisecond,
		MaxKeys:        1000,
	}
	window := NewDedupWindow(config)

	// Add a pending entry
	window.Put("pending-1", 1, 100, EntryStatusPending)
	assert.Equal(t, 1, window.Len())

	// Wait for time-based expiration
	time.Sleep(150 * time.Millisecond)

	// Evict should NOT remove pending entry (even if expired)
	evicted := window.Evict()
	assert.Equal(t, 0, evicted)
	assert.Equal(t, 1, window.Len())
}

func TestDedupWindow_CapacityBasedEviction(t *testing.T) {
	config := DedupWindowConfig{
		WindowDuration: 10 * time.Minute,
		MaxKeys:        3,
	}
	window := NewDedupWindow(config)

	// Add entries up to capacity
	window.Put("id-1", 1, 1, EntryStatusCommitted)
	window.Put("id-2", 1, 2, EntryStatusCommitted)
	window.Put("id-3", 1, 3, EntryStatusCommitted)
	window.Put("id-4", 1, 4, EntryStatusCommitted)
	window.Put("id-5", 1, 5, EntryStatusCommitted)

	assert.Equal(t, 5, window.Len())

	// Evict should remove oldest entries to meet capacity
	evicted := window.Evict()
	assert.Equal(t, 2, evicted)
	assert.Equal(t, 3, window.Len())
}

func TestDedupWindow_LoadFromSnapshot(t *testing.T) {
	config := DefaultDedupWindowConfig()
	window := NewDedupWindow(config)

	// Pre-populate
	window.Put("existing", 1, 1, EntryStatusCommitted)

	// Load from snapshot
	entries := []*DedupEntry{
		{IdempotencyId: "snap-1", SegmentId: 2, EntryId: 100, Status: EntryStatusCommitted, Timestamp: time.Now()},
		{IdempotencyId: "snap-2", SegmentId: 2, EntryId: 101, Status: EntryStatusCommitted, Timestamp: time.Now()},
	}
	window.LoadFromSnapshot(entries)

	// Old entries should be gone
	assert.Nil(t, window.Get("existing"))

	// New entries should be present
	assert.NotNil(t, window.Get("snap-1"))
	assert.NotNil(t, window.Get("snap-2"))
	assert.Equal(t, 2, window.Len())
}

func TestDedupWindow_Clear(t *testing.T) {
	config := DefaultDedupWindowConfig()
	window := NewDedupWindow(config)

	window.Put("id-1", 1, 1, EntryStatusCommitted)
	window.Put("id-2", 1, 2, EntryStatusCommitted)
	assert.Equal(t, 2, window.Len())

	window.Clear()
	assert.Equal(t, 0, window.Len())
	assert.Nil(t, window.Get("id-1"))
}

func TestDedupWindow_Concurrent(t *testing.T) {
	config := DefaultDedupWindowConfig()
	window := NewDedupWindow(config)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idStr := string(rune('a' + id%26))
			window.Put(idStr, int64(id%10), int64(id), EntryStatusCommitted)
			window.Get(idStr)
		}(i)
	}
	wg.Wait()

	// Should not panic and should have entries
	assert.True(t, window.Len() > 0)
}
