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
	"container/list"
	"sync"
	"time"
)

// EntryStatus represents the status of a dedup entry.
type EntryStatus int

const (
	// EntryStatusPending indicates the entry is in-flight (not yet committed).
	EntryStatusPending EntryStatus = iota
	// EntryStatusCommitted indicates the entry has been successfully committed.
	EntryStatusCommitted
)

// DedupEntry represents a single entry in the dedup window.
type DedupEntry struct {
	IdempotencyId string
	SegmentId     int64
	EntryId       int64
	Status        EntryStatus
	Timestamp     time.Time
	// listElement is a pointer to the element in the LRU list for O(1) removal.
	listElement *list.Element
}

// DedupWindowConfig holds configuration for the dedup window.
type DedupWindowConfig struct {
	// WindowDuration is the maximum age of a dedup entry before eviction.
	WindowDuration time.Duration
	// MaxKeys is the maximum number of idempotencyIds to track.
	MaxKeys int
}

// DefaultDedupWindowConfig returns the default configuration.
func DefaultDedupWindowConfig() DedupWindowConfig {
	return DedupWindowConfig{
		WindowDuration: 10 * time.Minute,
		MaxKeys:        100000,
	}
}

// DedupWindow manages idempotencyId -> (segId, entryId, status) mappings.
// It provides bounded deduplication with time + capacity limits.
type DedupWindow interface {
	// Get returns the entry for the given idempotencyId, or nil if not found.
	Get(idempotencyId string) *DedupEntry

	// Put adds or updates an entry in the window.
	// If the idempotencyId already exists, this returns the existing entry without modification.
	// Returns the entry (existing or newly created) and a boolean indicating if it was newly created.
	Put(idempotencyId string, segmentId, entryId int64, status EntryStatus) (*DedupEntry, bool)

	// MarkCommitted marks an entry as committed.
	// Returns false if the entry doesn't exist.
	MarkCommitted(idempotencyId string) bool

	// Evict removes expired entries and enforces capacity limit.
	// Returns the number of entries evicted.
	Evict() int

	// Len returns the current number of entries in the window.
	Len() int

	// GetCommittedEntries returns all committed entries for snapshot persistence.
	GetCommittedEntries() []*DedupEntry

	// GetSegmentWatermarks returns the highest entryId for each segment.
	GetSegmentWatermarks() map[int64]int64

	// LoadFromSnapshot loads entries from a snapshot.
	// This replaces all existing entries.
	LoadFromSnapshot(entries []*DedupEntry)

	// Clear removes all entries from the window.
	Clear()
}

// dedupWindowImpl is the implementation of DedupWindow.
type dedupWindowImpl struct {
	mu     sync.RWMutex
	config DedupWindowConfig

	// entries maps idempotencyId to DedupEntry.
	entries map[string]*DedupEntry

	// lruList maintains entries in LRU order (oldest at front) for eviction.
	// Only committed entries are considered for eviction by age/capacity.
	lruList *list.List

	// segmentWatermarks tracks the highest entryId per segment.
	segmentWatermarks map[int64]int64
}

// NewDedupWindow creates a new DedupWindow with the given configuration.
func NewDedupWindow(config DedupWindowConfig) DedupWindow {
	return &dedupWindowImpl{
		config:            config,
		entries:           make(map[string]*DedupEntry),
		lruList:           list.New(),
		segmentWatermarks: make(map[int64]int64),
	}
}

// Get returns the entry for the given idempotencyId, or nil if not found.
func (w *dedupWindowImpl) Get(idempotencyId string) *DedupEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	entry, ok := w.entries[idempotencyId]
	if !ok {
		return nil
	}
	return entry
}

// Put adds or updates an entry in the window.
func (w *dedupWindowImpl) Put(idempotencyId string, segmentId, entryId int64, status EntryStatus) (*DedupEntry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// If entry already exists, return it without modification.
	if existing, ok := w.entries[idempotencyId]; ok {
		return existing, false
	}

	// Create new entry.
	entry := &DedupEntry{
		IdempotencyId: idempotencyId,
		SegmentId:     segmentId,
		EntryId:       entryId,
		Status:        status,
		Timestamp:     time.Now(),
	}

	// Add to LRU list.
	entry.listElement = w.lruList.PushBack(entry)

	// Add to map.
	w.entries[idempotencyId] = entry

	// Update segment watermark.
	if entryId > w.segmentWatermarks[segmentId] {
		w.segmentWatermarks[segmentId] = entryId
	}

	return entry, true
}

// MarkCommitted marks an entry as committed.
func (w *dedupWindowImpl) MarkCommitted(idempotencyId string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry, ok := w.entries[idempotencyId]
	if !ok {
		return false
	}

	entry.Status = EntryStatusCommitted
	return true
}

// Evict removes expired entries and enforces capacity limit.
func (w *dedupWindowImpl) Evict() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	evicted := 0
	now := time.Now()
	cutoff := now.Add(-w.config.WindowDuration)

	// First pass: evict expired committed entries.
	for e := w.lruList.Front(); e != nil; {
		entry := e.Value.(*DedupEntry)
		next := e.Next()

		// Only evict committed entries that have expired.
		if entry.Status == EntryStatusCommitted && entry.Timestamp.Before(cutoff) {
			w.removeEntry(entry)
			evicted++
		}

		e = next
	}

	// Second pass: evict oldest committed entries if still over capacity.
	for w.lruList.Len() > w.config.MaxKeys {
		// Find the oldest committed entry.
		for e := w.lruList.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*DedupEntry)
			if entry.Status == EntryStatusCommitted {
				w.removeEntry(entry)
				evicted++
				break
			}
		}
		// If no committed entries found, we can't evict more.
		// This means we have more pending entries than MaxKeys,
		// which is an edge case that shouldn't happen in normal operation.
		if evicted == 0 {
			break
		}
	}

	return evicted
}

// removeEntry removes an entry from both the map and the LRU list.
// Caller must hold the write lock.
func (w *dedupWindowImpl) removeEntry(entry *DedupEntry) {
	delete(w.entries, entry.IdempotencyId)
	if entry.listElement != nil {
		w.lruList.Remove(entry.listElement)
	}
}

// Len returns the current number of entries in the window.
func (w *dedupWindowImpl) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.entries)
}

// GetCommittedEntries returns all committed entries for snapshot persistence.
func (w *dedupWindowImpl) GetCommittedEntries() []*DedupEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var committed []*DedupEntry
	for _, entry := range w.entries {
		if entry.Status == EntryStatusCommitted {
			// Create a copy to avoid data race.
			entryCopy := &DedupEntry{
				IdempotencyId: entry.IdempotencyId,
				SegmentId:     entry.SegmentId,
				EntryId:       entry.EntryId,
				Status:        entry.Status,
				Timestamp:     entry.Timestamp,
			}
			committed = append(committed, entryCopy)
		}
	}
	return committed
}

// GetSegmentWatermarks returns the highest entryId for each segment.
func (w *dedupWindowImpl) GetSegmentWatermarks() map[int64]int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Create a copy.
	result := make(map[int64]int64, len(w.segmentWatermarks))
	for segId, entryId := range w.segmentWatermarks {
		result[segId] = entryId
	}
	return result
}

// LoadFromSnapshot loads entries from a snapshot.
func (w *dedupWindowImpl) LoadFromSnapshot(entries []*DedupEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Clear existing entries.
	w.entries = make(map[string]*DedupEntry)
	w.lruList = list.New()
	w.segmentWatermarks = make(map[int64]int64)

	// Load new entries.
	for _, entry := range entries {
		entryCopy := &DedupEntry{
			IdempotencyId: entry.IdempotencyId,
			SegmentId:     entry.SegmentId,
			EntryId:       entry.EntryId,
			Status:        entry.Status,
			Timestamp:     entry.Timestamp,
		}
		entryCopy.listElement = w.lruList.PushBack(entryCopy)
		w.entries[entry.IdempotencyId] = entryCopy

		// Update segment watermark.
		if entry.EntryId > w.segmentWatermarks[entry.SegmentId] {
			w.segmentWatermarks[entry.SegmentId] = entry.EntryId
		}
	}
}

// Clear removes all entries from the window.
func (w *dedupWindowImpl) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.entries = make(map[string]*DedupEntry)
	w.lruList = list.New()
	w.segmentWatermarks = make(map[int64]int64)
}
