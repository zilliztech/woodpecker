package cache

import (
	"container/list"
	"context"
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
	once     sync.Once
	instance FragmentManager
)

func GetInstance(maxMemoryBytes int64, intervalMs int) FragmentManager {
	once.Do(func() {
		instance = newFragmentManager(maxMemoryBytes)
		go instance.StartEvictionLoop(time.Duration(intervalMs) * time.Millisecond) // Run cleanup every 10 seconds
	})
	return instance
}

func GetCachedFragment(ctx context.Context, key string) (storage.Fragment, bool) {
	return GetInstance(1_000_000, 1000).GetFragment(ctx, key)
}

func AddCacheFragment(ctx context.Context, fragment storage.Fragment) error {
	return GetInstance(1_000_000, 1000).AddFragment(ctx, fragment)
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
	if item, ok := m.cache[fragmentKey]; ok {
		return item.fragment, true
	}
	return nil, false
}

func (m *fragmentManagerImpl) RemoveFragment(ctx context.Context, fragment storage.Fragment) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := fragment.GetFragmentKey()
	if item, ok := m.cache[key]; ok {
		delete(m.cache, key)
		m.order.Remove(item.element)
		fragmentSize := calculateSize(item.fragment)
		m.usedMemory -= fragmentSize
		item.fragment.Release()
		metrics.WpFragmentCacheBytesGauge.WithLabelValues(fragment.GetFragmentKey()).Sub(float64(fragmentSize))
		logger.Ctx(m.evictionCtx).Info("remove fragment finish", zap.String("key", key), zap.Int64("fragmentSize", fragmentSize), zap.Int64("currentUsedMemory", m.usedMemory))
		return nil
	}
	return werr.ErrFragmentNotFound
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := fragment.GetFragmentKey()

	// check if fragment already exists
	if _, ok := m.cache[key]; ok {
		// already exists
		return nil
	}

	// push to front of list
	pushedElement := m.order.PushFront(key)
	m.cache[key] = &CacheItem{
		fragment: fragment,
		element:  pushedElement,
	}

	// update used memory
	fragmentSize := calculateSize(fragment)
	m.usedMemory += fragmentSize
	metrics.WpFragmentCacheBytesGauge.WithLabelValues(fragment.GetFragmentKey()).Add(float64(fragmentSize))
	logger.Ctx(m.evictionCtx).Info("add fragment finish", zap.String("key", key), zap.Int64("fragmentSize", fragmentSize), zap.Int64("currentUsedMemory", m.usedMemory))
	return nil
}

func (m *fragmentManagerImpl) EvictFragments() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 实现LRU淘汰逻辑
	if m.order.Len() == 0 || m.usedMemory <= m.maxMemory {
		return nil
	}

	// 按最后访问时间排序
	evictElement := m.order.Back()
	if evictElement == nil {
		return nil
	}
	key := evictElement.Value.(string)
	if item, ok := m.cache[key]; ok {
		delete(m.cache, key)
		fragmentSize := calculateSize(item.fragment)
		m.usedMemory -= fragmentSize
		item.fragment.Release()
		metrics.WpFragmentCacheBytesGauge.WithLabelValues(item.fragment.GetFragmentKey()).Sub(float64(fragmentSize))
	}
	m.order.Remove(evictElement)
	logger.Ctx(m.evictionCtx).Info("evict fragment automatically", zap.String("key", key))
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
