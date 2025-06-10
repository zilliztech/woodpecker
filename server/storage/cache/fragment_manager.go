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

// Deprecated
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
	// StartEvictionLoop starts a loop to periodically trigger evict fragments automatically.
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

// Deprecated
func GetInstance(maxMemoryBytes int64, intervalMs int) FragmentManager {
	once.Do(func() {
		maxMemory = maxMemoryBytes
		interval = intervalMs
		instance = newFragmentManager(maxMemory)
		go func() {
			err := instance.StartEvictionLoop(time.Duration(interval) * time.Millisecond)
			if err != nil {
				logger.Ctx(context.Background()).Warn("start eviction loop error", zap.Error(err))
			}
		}()
	})
	return instance
}

// Deprecated
func GetCachedFragment(ctx context.Context, key string) (storage.Fragment, bool) {
	return GetInstance(maxMemory, interval).GetFragment(ctx, key)
}

// Deprecated
func AddCacheFragment(ctx context.Context, fragment storage.Fragment) error {
	logger.Ctx(ctx).Debug("add cache fragment", zap.String("key", fragment.GetFragmentKey()), zap.Any("fragInst", fmt.Sprintf("%p", fragment)))
	return GetInstance(maxMemory, interval).AddFragment(ctx, fragment)
}

// Deprecated
func RemoveCachedFragment(ctx context.Context, frag storage.Fragment) error {
	return GetInstance(maxMemory, interval).RemoveFragment(ctx, frag)
}

// Operation types for the fragment manager
type operationType int

const (
	opAdd operationType = iota
	opRemove
	opGet
	opEvict
	opGetStats
	opShutdown // Special operation to signal shutdown
)

// Request structure for operations
type fragmentRequest struct {
	op           operationType
	fragmentKey  string
	fragment     storage.Fragment
	responseChan chan interface{}
	ctx          context.Context
}

// Response for getStats operation
type statsResponse struct {
	maxMemory  int64
	usedMemory int64
}

type CacheItem struct {
	fragment storage.Fragment
	element  *list.Element
}

func newFragmentManager(maxMemory int64) FragmentManager {
	ctx, cancel := context.WithCancel(context.Background())
	fm := &fragmentManagerImpl{
		maxMemory:      maxMemory,
		usedMemory:     0,
		dataMemory:     0,
		cache:          make(map[string]*CacheItem),
		order:          list.New(),
		requestChan:    make(chan fragmentRequest, 100), // Buffer to prevent blocking
		evictionCtx:    ctx,
		evictionCancel: cancel,
		shutdownChan:   make(chan struct{}), // Channel to signal shutdown completion
	}

	// Start the processing goroutine
	go fm.processRequests()

	return fm
}

var _ FragmentManager = (*fragmentManagerImpl)(nil)

type fragmentManagerImpl struct {
	maxMemory      int64                 // Memory usage limitation
	usedMemory     int64                 // Pooling byte buff instances occupies memory
	dataMemory     int64                 // The actual fragment data occupies memory
	cache          map[string]*CacheItem // key: fileId-fragmentId, value: fragment
	order          *list.List            // LRU order, front is the least recently used fragment key element
	requestChan    chan fragmentRequest  // Channel for all operations
	evictionCtx    context.Context
	evictionCancel context.CancelFunc
	shutdownChan   chan struct{} // Channel to signal when shutdown is complete
	isShutdown     bool          // Flag to indicate if shutdown has been requested
}

// processRequests handles all operations on the fragment cache
func (m *fragmentManagerImpl) processRequests() {
	for req := range m.requestChan {
		// If we've received a shutdown request, only process the shutdown operation
		if m.isShutdown && req.op != opShutdown {
			if req.responseChan != nil {
				req.responseChan <- fmt.Errorf("fragment manager is shutting down")
			}
			continue
		}

		switch req.op {
		case opAdd:
			m.handleAddFragment(req)
		case opRemove:
			m.handleRemoveFragment(req)
		case opGet:
			m.handleGetFragment(req)
		case opEvict:
			m.handleEvictFragments(req)
		case opGetStats:
			m.handleGetStats(req)
		case opShutdown:
			// Release all fragments and clean up
			m.handleShutdown(req)
			// Signal that shutdown is complete
			close(m.shutdownChan)
			// Exit the goroutine
			return
		}
	}
}

// handleShutdown releases all fragments and cleans up resources
func (m *fragmentManagerImpl) handleShutdown(req fragmentRequest) {
	logger.Ctx(req.ctx).Debug("shutting down fragment manager",
		zap.Int("fragments_count", m.order.Len()),
		zap.Int64("used_memory", m.usedMemory))

	// Release all fragments to prevent memory leaks
	for key, item := range m.cache {
		fragmentSize, rawFragmentBufSize := calculateSize(item.fragment)

		// Reset metrics for this fragment before releasing
		logId := fmt.Sprintf("%d", item.fragment.GetLogId())
		segmentId := fmt.Sprintf("%d", item.fragment.GetSegmentId())
		metrics.WpFragmentManagerCachedFragmentsTotal.WithLabelValues(logId, segmentId).Dec()
		metrics.WpFragmentManagerDataCacheBytes.WithLabelValues(logId, segmentId).Sub(float64(fragmentSize))
		metrics.WpFragmentManagerBufferCacheBytes.WithLabelValues(logId, segmentId).Sub(float64(rawFragmentBufSize))

		m.dataMemory -= fragmentSize
		m.usedMemory -= rawFragmentBufSize
		item.fragment.Release(context.TODO())
		logger.Ctx(req.ctx).Debug("released fragment during shutdown",
			zap.String("key", key),
			zap.String("fragInst", fmt.Sprintf("%p", item.fragment)),
			zap.Int64("fragmentSize", fragmentSize),
			zap.Int64("rawFragmentBufSize", rawFragmentBufSize),
			zap.Int64("currentUsedMemory", m.usedMemory),
			zap.Int64("currentDataMemory", m.dataMemory))
	}

	// Clear the cache and order list
	m.cache = make(map[string]*CacheItem)
	m.order = list.New()
	m.usedMemory = 0

	// Signal completion
	if req.responseChan != nil {
		req.responseChan <- nil
	}
}

func (m *fragmentManagerImpl) handleAddFragment(req fragmentRequest) {
	fragment := req.fragment
	key := fragment.GetFragmentKey()

	// Check if fragment already exists
	if _, ok := m.cache[key]; ok {
		// Already exists, no need to add
		if req.responseChan != nil {
			req.responseChan <- nil // Signal success with nil error
		}
		return
	}

	// Add to cache
	pushedElement := m.order.PushFront(key)
	m.cache[key] = &CacheItem{
		fragment: fragment,
		element:  pushedElement,
	}

	// Update memory usage statistics
	fragmentSize, rawFragmentBufSize := calculateSize(fragment)
	m.dataMemory += fragmentSize
	m.usedMemory += rawFragmentBufSize

	// Update metrics for this fragment
	logId := fmt.Sprintf("%d", fragment.GetLogId())
	segmentId := fmt.Sprintf("%d", fragment.GetSegmentId())
	metrics.WpFragmentManagerCachedFragmentsTotal.WithLabelValues(logId, segmentId).Inc()
	metrics.WpFragmentManagerDataCacheBytes.WithLabelValues(logId, segmentId).Add(float64(fragmentSize))
	metrics.WpFragmentManagerBufferCacheBytes.WithLabelValues(logId, segmentId).Add(float64(rawFragmentBufSize))

	logger.Ctx(req.ctx).Debug("add fragment finish", zap.String("key", key),
		zap.String("fragInst", fmt.Sprintf("%p", fragment)),
		zap.Int64("fragmentSize", fragmentSize),
		zap.Int64("rawFragmentBufSize", rawFragmentBufSize),
		zap.Int64("currentUsedMemory", m.usedMemory),
		zap.Int64("currentDataMemory", m.dataMemory))

	if req.responseChan != nil {
		req.responseChan <- nil // Signal success with nil error
	}
}

func (m *fragmentManagerImpl) handleRemoveFragment(req fragmentRequest) {
	key := req.fragmentKey
	if req.fragment != nil {
		key = req.fragment.GetFragmentKey()
	}

	// Check if the fragment is in cache
	item, ok := m.cache[key]
	if !ok {
		if req.responseChan != nil {
			req.responseChan <- werr.ErrFragmentNotFound
		}
		return
	}

	// Record information we need to save
	fragmentSize, rawFragmentBufSize := calculateSize(item.fragment)

	// Update metrics before removing
	logId := fmt.Sprintf("%d", item.fragment.GetLogId())
	segmentId := fmt.Sprintf("%d", item.fragment.GetSegmentId())
	metrics.WpFragmentManagerCachedFragmentsTotal.WithLabelValues(logId, segmentId).Dec()
	metrics.WpFragmentManagerDataCacheBytes.WithLabelValues(logId, segmentId).Sub(float64(fragmentSize))
	metrics.WpFragmentManagerBufferCacheBytes.WithLabelValues(logId, segmentId).Sub(float64(rawFragmentBufSize))

	// Perform deletion
	delete(m.cache, key)
	m.order.Remove(item.element)
	m.dataMemory -= fragmentSize
	m.usedMemory -= rawFragmentBufSize
	item.fragment.Release(context.TODO())
	logger.Ctx(req.ctx).Debug("remove fragment finish",
		zap.String("key", key),
		zap.String("fragInst", fmt.Sprintf("%p", item.fragment)),
		zap.Int64("fragmentSize", fragmentSize),
		zap.Int64("rawFragmentBufSize", rawFragmentBufSize),
		zap.Int64("currentUsedMemory", m.usedMemory),
		zap.Int64("currentDataMemory", m.dataMemory))

	if req.responseChan != nil {
		req.responseChan <- nil // Signal success with nil error
	}
}

func (m *fragmentManagerImpl) handleGetFragment(req fragmentRequest) {
	item, ok := m.cache[req.fragmentKey]
	if !ok {
		req.responseChan <- nil // Fragment not found
		return
	}

	// Update LRU order
	m.order.MoveToFront(item.element)

	// Return the fragment
	req.responseChan <- item.fragment
}

func (m *fragmentManagerImpl) handleEvictFragments(req fragmentRequest) {
	// Check if eviction is needed
	if m.usedMemory <= m.maxMemory || m.order.Len() == 0 {
		if req.responseChan != nil {
			req.responseChan <- nil // No eviction needed
		}
		return
	}

	// Maximum number of fragments to evict in one call
	maxEvictionCount := 1000 // TODO should be configurable
	evictCount := 0

	for m.usedMemory > m.maxMemory && m.order.Len() > 0 && evictCount < maxEvictionCount {
		// Get the least recently used fragment
		evictElement := m.order.Back()
		if evictElement == nil {
			break // No more elements to evict
		}

		keyToEvict := evictElement.Value.(string)
		item, ok := m.cache[keyToEvict]
		if !ok {
			// Should not happen, but just in case
			m.order.Remove(evictElement)
			continue
		}

		// Update metrics before evicting
		fragmentSize, rawFragmentBufSize := calculateSize(item.fragment)
		logId := fmt.Sprintf("%d", item.fragment.GetLogId())
		segmentId := fmt.Sprintf("%d", item.fragment.GetSegmentId())
		metrics.WpFragmentManagerCachedFragmentsTotal.WithLabelValues(logId, segmentId).Dec()
		metrics.WpFragmentManagerDataCacheBytes.WithLabelValues(logId, segmentId).Sub(float64(fragmentSize))
		metrics.WpFragmentManagerBufferCacheBytes.WithLabelValues(logId, segmentId).Sub(float64(rawFragmentBufSize))

		// Perform eviction
		delete(m.cache, keyToEvict)
		m.dataMemory -= fragmentSize
		m.usedMemory -= rawFragmentBufSize
		item.fragment.Release(context.TODO())
		m.order.Remove(evictElement)
		logger.Ctx(req.ctx).Debug("evict fragment automatically",
			zap.String("key", keyToEvict),
			zap.String("fragInst", fmt.Sprintf("%p", item.fragment)),
			zap.Int64("fragmentSize", fragmentSize),
			zap.Int64("rawFragmentBufSize", rawFragmentBufSize),
			zap.Int64("currentUsedMemory", m.usedMemory),
			zap.Int64("currentDataMemory", m.dataMemory))
		evictCount++
	}

	logger.Ctx(req.ctx).Debug("one round of evict fragments complete",
		zap.Int("totalEvict", evictCount),
		zap.Int64("currentUsedMemory", m.usedMemory),
		zap.Int64("currentDataMemory", m.dataMemory))

	if req.responseChan != nil {
		req.responseChan <- nil // Signal success with nil error
	}
}

func (m *fragmentManagerImpl) handleGetStats(req fragmentRequest) {
	if req.responseChan != nil {
		req.responseChan <- statsResponse{
			maxMemory:  m.maxMemory,
			usedMemory: m.usedMemory,
		}
	}
}

// GetMaxMemory returns the maximum memory limit.
func (m *fragmentManagerImpl) GetMaxMemory() int64 {
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opGetStats,
		responseChan: responseChan,
		ctx:          context.Background(),
	}
	response := (<-responseChan).(statsResponse)
	return response.maxMemory
}

// GetUsedMemory returns the current used memory.
func (m *fragmentManagerImpl) GetUsedMemory() int64 {
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opGetStats,
		responseChan: responseChan,
		ctx:          context.Background(),
	}
	response := (<-responseChan).(statsResponse)
	return response.usedMemory
}

// GetFragment returns a fragment from the manager.
func (m *fragmentManagerImpl) GetFragment(ctx context.Context, fragmentKey string) (storage.Fragment, bool) {
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opGet,
		fragmentKey:  fragmentKey,
		responseChan: responseChan,
		ctx:          ctx,
	}
	response := <-responseChan
	if response == nil {
		return nil, false
	}

	// Check if we got an error (fragment manager is shutting down)
	if err, ok := response.(error); ok {
		logger.Ctx(ctx).Debug("get fragment failed", zap.String("key", fragmentKey), zap.Error(err))
		return nil, false
	}

	return response.(storage.Fragment), true
}

// AddFragment manually adds a fragment to the manager.
func (m *fragmentManagerImpl) AddFragment(ctx context.Context, fragment storage.Fragment) error {
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opAdd,
		fragment:     fragment,
		responseChan: responseChan,
		ctx:          ctx,
	}
	response := <-responseChan
	if response == nil {
		return nil
	}
	return response.(error)
}

// RemoveFragment manually removes a fragment from the manager.
func (m *fragmentManagerImpl) RemoveFragment(ctx context.Context, fragment storage.Fragment) error {
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opRemove,
		fragment:     fragment,
		responseChan: responseChan,
		ctx:          ctx,
	}
	response := <-responseChan
	if response == nil {
		return nil
	}
	return response.(error)
}

// EvictFragments evicts fragments to free up memory.
func (m *fragmentManagerImpl) EvictFragments() error {
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opEvict,
		responseChan: responseChan,
		ctx:          m.evictionCtx,
	}
	response := <-responseChan
	if response == nil {
		return nil
	}
	return response.(error)
}

// StartEvictionLoop starts a loop to periodically trigger evict fragments automatically.
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

// StopEvictionLoop stops the eviction loop and shuts down the fragment manager.
func (m *fragmentManagerImpl) StopEvictionLoop() error {
	if m.evictionCancel != nil {
		m.evictionCancel()
		m.evictionCtx = nil
		m.evictionCancel = nil
	}

	// Set the shutdown flag and send a shutdown operation
	m.isShutdown = true

	// Send shutdown request
	responseChan := make(chan interface{})
	m.requestChan <- fragmentRequest{
		op:           opShutdown,
		responseChan: responseChan,
		ctx:          context.Background(),
	}

	// Wait for the response or a timeout
	select {
	case err := <-responseChan:
		if err != nil {
			return err.(error)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("fragment manager shutdown timed out")
	}

	// Wait for the processing goroutine to complete
	select {
	case <-m.shutdownChan:
		// Shutdown completed successfully
	case <-time.After(5 * time.Second):
		return fmt.Errorf("fragment manager shutdown completion timed out")
	}

	// Close the request channel after all in-flight requests are processed
	close(m.requestChan)

	return nil
}

func calculateSize(f storage.Fragment) (int64, int64) {
	return f.GetSize(), f.GetRawBufSize()
}
