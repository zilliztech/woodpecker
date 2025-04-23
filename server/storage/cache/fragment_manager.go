package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
)

// FragmentManager is responsible for managing memory usage of fragments.
type FragmentManager interface {
	// GetMaxMemory returns the maximum memory limit.
	GetMaxMemory() int64
	// GetUsedMemory returns the current used memory.
	GetUsedMemory() int64
	// GetFragment returns a fragment from the manager.
	GetFragment(ctx context.Context, fragmentKey string) (storage.Fragment, bool)
	// AddFragment manually adds a fragment to the manager.
	AddFragment(ctx context.Context, fragment storage.Fragment) error
	// RemoveFragment manually removes a fragment from the manager.
	RemoveFragment(ctx context.Context, fragment storage.Fragment) error
	// EvictFragments evicts fragments to free up memory.
	EvictFragments() error
	// StartEvictionLoop starts a loop to periodically trigger evict fragments automatically .
	StartEvictionLoop(interval time.Duration) error
	// StopEvictionLoop stops the eviction loop.
	StopEvictionLoop() error
}

var (
	once      sync.Once
	instance  FragmentManager
	maxMemory int64 = 256_000_000
	interval  int   = 1_000
)

func GetInstance(maxMemoryBytes int64, intervalMs int) FragmentManager {
	once.Do(func() {
		maxMemory = maxMemoryBytes
		interval = intervalMs
		instance = newFragmentManager(maxMemory)
		go instance.StartEvictionLoop(time.Duration(interval) * time.Millisecond) // Run cleanup every 10 seconds
	})
	return instance
}

func GetCachedFragment(ctx context.Context, key string) (storage.Fragment, bool) {
	return GetInstance(maxMemory, interval).GetFragment(ctx, key)
}

func AddCacheFragment(ctx context.Context, fragment storage.Fragment) error {
	logger.Ctx(ctx).Debug("add cache fragment", zap.String("key", fragment.GetFragmentKey()), zap.Any("fragInst", fmt.Sprintf("%p", fragment)))
	return GetInstance(maxMemory, interval).AddFragment(ctx, fragment)
}

func RemoveCachedFragment(ctx context.Context, frag storage.Fragment) error {
	return GetInstance(maxMemory, interval).RemoveFragment(ctx, frag)
}

func newFragmentManager(maxMemory int64) FragmentManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &fragmentManagerImpl{
		maxMemory:  maxMemory,
		usedMemory: 0,

		cache: make(map[string]*CacheItem),
		order: list.New(),

		mutex:          sync.RWMutex{},
		evictionCtx:    ctx,
		evictionCancel: cancel,
	}
}

type CacheItem struct {
	fragment storage.Fragment
	element  *list.Element
}

var _ FragmentManager = (*fragmentManagerImpl)(nil)

type fragmentManagerImpl struct {
	maxMemory  int64
	usedMemory int64

	cache map[string]*CacheItem // key: fileId-fragmentId, value: fragment
	order *list.List            // LRU order, front is the least recently used fragment key element

	mutex          sync.RWMutex
	evictionCtx    context.Context
	evictionCancel context.CancelFunc
}

func (m *fragmentManagerImpl) GetMaxMemory() int64 {
	return m.maxMemory
}

func (m *fragmentManagerImpl) GetUsedMemory() int64 {
	return m.usedMemory
}

func (m *fragmentManagerImpl) GetFragment(ctx context.Context, fragmentKey string) (storage.Fragment, bool) {
	// First use read lock to check if the fragment exists in cache
	m.mutex.RLock()
	item, ok := m.cache[fragmentKey]
	if !ok {
		// Fragment not in cache, release read lock and return
		m.mutex.RUnlock()
		return nil, false
	}

	// Fragment exists in cache, get the fragment reference
	fragment := item.fragment
	m.mutex.RUnlock()

	// Then use write lock to update LRU order
	m.mutex.Lock()

	// Check again if the fragment is still in cache (may have been removed after releasing read lock)
	item, stillExists := m.cache[fragmentKey]
	if stillExists {
		m.order.MoveToFront(item.element)
	}
	m.mutex.Unlock()

	return fragment, true
}

func (m *fragmentManagerImpl) RemoveFragment(ctx context.Context, fragment storage.Fragment) error {
	key := fragment.GetFragmentKey()

	// First check if the fragment is in cache
	m.mutex.RLock()
	item, ok := m.cache[key]
	if !ok {
		// Fragment not in cache, release read lock and return error
		m.mutex.RUnlock()
		return werr.ErrFragmentNotFound
	}

	// Record information we need to save
	fragmentSize := calculateSize(item.fragment)
	m.mutex.RUnlock()

	// Acquire write lock to perform deletion
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Double check
	item, stillExists := m.cache[key]
	if !stillExists {
		return werr.ErrFragmentNotFound
	}

	// Perform deletion
	delete(m.cache, key)
	m.order.Remove(item.element)
	m.usedMemory -= fragmentSize
	item.fragment.Release()
	metrics.WpFragmentCacheBytesGauge.WithLabelValues("default").Sub(float64(fragmentSize))
	logger.Ctx(m.evictionCtx).Debug("remove fragment finish", zap.String("key", key), zap.Int64("fragmentSize", fragmentSize), zap.Int64("currentUsedMemory", m.usedMemory))

	return nil
}

func (m *fragmentManagerImpl) StopEvictionLoop() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.evictionCancel != nil {
		m.evictionCancel()
		m.evictionCtx = nil
		m.evictionCancel = nil
	}
	return nil
}

func (m *fragmentManagerImpl) AddFragment(ctx context.Context, fragment storage.Fragment) error {
	key := fragment.GetFragmentKey()

	// First check if fragment already exists to avoid duplicates
	m.mutex.RLock()
	_, exists := m.cache[key]
	m.mutex.RUnlock()

	if exists {
		// Already exists, no need to add
		return nil
	}

	// Perform add operation
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Double check to ensure it wasn't added by another thread while acquiring write lock
	if _, ok := m.cache[key]; ok {
		// Already exists, no need to add
		return nil
	}

	// Add to cache
	pushedElement := m.order.PushFront(key)
	m.cache[key] = &CacheItem{
		fragment: fragment,
		element:  pushedElement,
	}

	// Update memory usage statistics
	fragmentSize := calculateSize(fragment)
	m.usedMemory += fragmentSize
	metrics.WpFragmentCacheBytesGauge.WithLabelValues("default").Add(float64(fragmentSize))
	logger.Ctx(m.evictionCtx).Debug("add fragment finish", zap.String("key", key), zap.Int64("fragmentSize", fragmentSize), zap.Int64("currentUsedMemory", m.usedMemory))
	return nil
}

func (m *fragmentManagerImpl) EvictFragments() error {
	// First check if eviction is needed
	m.mutex.RLock()
	needEviction := m.usedMemory > m.maxMemory && m.order.Len() > 0
	m.mutex.RUnlock()

	if !needEviction {
		return nil
	}

	// Maximum number of fragments to evict in one call to prevent holding lock for too long
	maxEvictionCount := 10
	evictCount := 0

	for {
		if evictCount >= maxEvictionCount {
			// Reached maximum eviction count for a single call, return to allow other operations to proceed
			break
		}

		// Get the least recently used fragment
		var keyToEvict string
		var itemToEvict *CacheItem

		m.mutex.RLock()
		if m.order.Len() == 0 || m.usedMemory <= m.maxMemory {
			m.mutex.RUnlock()
			return nil
		}

		evictElement := m.order.Back()
		if evictElement != nil {
			keyToEvict = evictElement.Value.(string)
			if item, ok := m.cache[keyToEvict]; ok {
				itemToEvict = item
			}
		}
		m.mutex.RUnlock()

		if itemToEvict == nil {
			// No fragment found to evict
			return nil
		}

		// Acquire write lock to perform eviction
		m.mutex.Lock()
		// Verify this key is still in cache
		if item, ok := m.cache[keyToEvict]; ok {
			// Ensure this is still the last element
			if m.order.Back().Value.(string) == keyToEvict {
				delete(m.cache, keyToEvict)
				fragmentSize := calculateSize(item.fragment)
				m.usedMemory -= fragmentSize
				item.fragment.Release()
				metrics.WpFragmentCacheBytesGauge.WithLabelValues("default").Sub(float64(fragmentSize))
				m.order.Remove(m.order.Back())
				logger.Ctx(m.evictionCtx).Debug("evict fragment automatically",
					zap.String("key", keyToEvict),
					zap.String("fragInst", fmt.Sprintf("%p", item.fragment)),
					zap.Int64("fragmentSize", fragmentSize),
					zap.Int64("currentUsedMemory", m.usedMemory))
				evictCount++
			}
		}

		// Check if we've reached target memory usage
		if m.order.Len() == 0 || m.usedMemory <= m.maxMemory {
			m.mutex.Unlock()
			break
		}
		m.mutex.Unlock()
	}

	return nil
}

func (m *fragmentManagerImpl) StartEvictionLoop(interval time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	m.evictionCtx = ctx
	m.evictionCancel = cancel

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.EvictFragments()
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (m *fragmentManagerImpl) run(intervalMs int) error {
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := m.EvictFragments()
			if err != nil {
				logger.Ctx(m.evictionCtx).Error("Failed to evict fragments", zap.Error(err))
			}
		case <-m.evictionCtx.Done():
			return nil
		}
	}
}

func calculateSize(f storage.Fragment) int64 {
	return f.GetSize()
}
