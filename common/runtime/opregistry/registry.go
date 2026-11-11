package opregistry

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

// Registry is a bounded, thread-safe pool of in-flight OpRecords.
// When capacity is reached, the oldest entry is evicted (drop-oldest FIFO).
type Registry struct {
	mu       sync.RWMutex
	capacity int
	warnAge  time.Duration
	clock    clockwork.Clock

	// pool is the ordered list of active ops (oldest first).
	pool []*entry
	// index maps OpID → pool index for O(1) lookup.
	index map[string]int
	// handleMap maps handle → OpID for deregistration.
	handleMap map[uint64]string

	// Eviction counters (accessed under mu).
	evictedYoung int64
	evictedOld   int64

	// onEvict is called when an entry is evicted (called under lock — must be fast).
	onEvict func(age time.Duration, isOld bool)
	// onPoolChange is called when pool size changes (for Prometheus gauge).
	onPoolChange func(size int)
}

type entry struct {
	record OpRecord
	handle uint64
}

var handleCounter atomic.Uint64

// New creates a Registry with the given capacity and warn age threshold.
func New(capacity int, warnAge time.Duration) *Registry {
	return NewWithClock(capacity, warnAge, clockwork.NewRealClock())
}

// NewWithClock creates a Registry with a custom clock (for testing).
func NewWithClock(capacity int, warnAge time.Duration, clock clockwork.Clock) *Registry {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Registry{
		capacity:  capacity,
		warnAge:   warnAge,
		clock:     clock,
		pool:      make([]*entry, 0, capacity),
		index:     make(map[string]int, capacity),
		handleMap: make(map[uint64]string, capacity),
	}
}

// Register adds an OpRecord to the pool. Returns a handle for Deregister.
// If the pool is full, the oldest entry is evicted first.
func (r *Registry) Register(rec OpRecord) uint64 {
	handle := handleCounter.Add(1)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Evict oldest if at capacity.
	if len(r.pool) >= r.capacity {
		r.evictOldestLocked()
	}

	e := &entry{record: rec, handle: handle}
	r.index[rec.OpID] = len(r.pool)
	r.handleMap[handle] = rec.OpID
	r.pool = append(r.pool, e)
	r.notifyPoolChangeLocked()

	return handle
}

// Deregister removes an op by handle. Returns the elapsed duration.
func (r *Registry) Deregister(handle uint64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	opID, ok := r.handleMap[handle]
	if !ok {
		return 0
	}

	idx, ok := r.index[opID]
	if !ok {
		delete(r.handleMap, handle)
		return 0
	}

	elapsed := r.clock.Since(r.pool[idx].record.StartedAt)
	r.removeAtLocked(idx)
	delete(r.handleMap, handle)
	r.notifyPoolChangeLocked()

	return elapsed
}

// List returns all ops matching the filter, sorted by elapsed desc.
func (r *Registry) List(f Filter) []OpRecord {
	r.mu.RLock()
	now := r.clock.Now()
	// Collect matching records under read lock.
	matched := make([]OpRecord, 0, len(r.pool))
	for _, e := range r.pool {
		if f.Matches(e.record, now) {
			matched = append(matched, e.record)
		}
	}
	r.mu.RUnlock()

	// Sort by elapsed descending (longest first).
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].StartedAt.Before(matched[j].StartedAt)
	})

	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched
}

// Get returns a single op by OpID, or nil if not found.
func (r *Registry) Get(opID string) *OpRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	idx, ok := r.index[opID]
	if !ok {
		return nil
	}
	rec := r.pool[idx].record
	return &rec
}

// Stats returns current registry utilization and eviction counters.
func (r *Registry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return Stats{
		Capacity:     r.capacity,
		InUse:        len(r.pool),
		WarnAgeMS:    r.warnAge.Milliseconds(),
		EvictedTotal: r.evictedYoung + r.evictedOld,
		EvictedYoung: r.evictedYoung,
		EvictedOld:   r.evictedOld,
	}
}

// SetOnEvict sets the eviction callback (called under lock — must be fast).
func (r *Registry) SetOnEvict(fn func(age time.Duration, isOld bool)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onEvict = fn
}

// notifyPoolChangeLocked calls the pool change callback if set. Caller must hold mu.
func (r *Registry) notifyPoolChangeLocked() {
	if r.onPoolChange != nil {
		r.onPoolChange(len(r.pool))
	}
}

// evictOldestLocked removes pool[0] and signals young/old. Caller must hold mu.
func (r *Registry) evictOldestLocked() {
	if len(r.pool) == 0 {
		return
	}
	oldest := r.pool[0]
	age := r.clock.Since(oldest.record.StartedAt)
	isOld := age >= r.warnAge

	if isOld {
		r.evictedOld++
	} else {
		r.evictedYoung++
	}

	if r.onEvict != nil {
		r.onEvict(age, isOld)
	}

	delete(r.handleMap, oldest.handle)
	r.removeAtLocked(0)
}

// removeAtLocked removes the entry at idx preserving insertion order (FIFO).
// Caller must hold mu. Uses copy+shift to maintain oldest-first ordering
// which is required for correct drop-oldest eviction.
func (r *Registry) removeAtLocked(idx int) {
	opID := r.pool[idx].record.OpID
	delete(r.index, opID)

	copy(r.pool[idx:], r.pool[idx+1:])
	r.pool[len(r.pool)-1] = nil
	r.pool = r.pool[:len(r.pool)-1]

	// Rebuild index for shifted entries.
	for i := idx; i < len(r.pool); i++ {
		r.index[r.pool[i].record.OpID] = i
	}
}
