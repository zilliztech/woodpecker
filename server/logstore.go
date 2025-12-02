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

package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/net"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/processor"
)

const (
	LogStoreScopeName = "LogStore"
)

//go:generate mockery --dir=./server --name=LogStore --structname=LogStore --output=mocks/mocks_server --filename=mock_logstore.go --with-expecter=true  --outpkg=mocks_server
type LogStore interface {
	Start() error
	Stop() error
	SetAddress(address string)
	GetAddress() string
	SetEtcdClient(etcdCli *clientv3.Client)
	Register(ctx context.Context) error
	AddEntry(ctx context.Context, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error)
	GetBatchEntriesAdv(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error)
	FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error)
	CompleteSegment(ctx context.Context, logId int64, segmentId int64) (int64, error)
	CompactSegment(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error)
	GetSegmentBlockCount(ctx context.Context, logId int64, segmentId int64) (int64, error)
	CleanSegment(ctx context.Context, logId int64, segmentId int64, flag int) error
}

var _ LogStore = (*logStore)(nil)

type logStore struct {
	cfg           *config.Configuration
	ctx           context.Context
	cancel        context.CancelFunc
	etcdCli       *clientv3.Client
	storageClient storageclient.ObjectStorage
	address       string

	spMu              sync.RWMutex
	segmentProcessors map[int64]map[int64]processor.SegmentProcessor

	// Background cleanup goroutine management
	cleanupWg   sync.WaitGroup
	cleanupDone chan struct{}
	stopped     atomic.Bool
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, storageClient storageclient.ObjectStorage) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	logStore := &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		etcdCli:           etcdCli,
		storageClient:     storageClient,
		segmentProcessors: make(map[int64]map[int64]processor.SegmentProcessor),
		address:           net.GetIP(""),
		cleanupDone:       make(chan struct{}),
	}
	logStore.stopped.Store(true)

	logger.Ctx(ctx).Info("LogStore created successfully",
		zap.String("address", logStore.address))

	return logStore
}

func (l *logStore) Start() error {
	logger.Ctx(l.ctx).Info("Starting LogStore service",
		zap.String("address", l.address))

	err := l.Register(context.Background())
	if err != nil {
		logger.Ctx(l.ctx).Warn("Failed to start LogStore service - registration failed",
			zap.String("address", l.address),
			zap.Error(err))
		return err
	}

	// Start background cleanup goroutine
	l.startBackgroundCleanup()

	metrics.WpLogStoreRunningTotal.WithLabelValues("default").Inc()

	l.stopped.Store(false)
	logger.Ctx(l.ctx).Info("LogStore service started successfully",
		zap.String("address", l.address))

	return nil
}

func (l *logStore) Stop() error {
	logger.Ctx(l.ctx).Info("Stopping LogStore service - initiating shutdown",
		zap.String("address", l.address))

	l.cancel()

	if !l.stopped.CompareAndSwap(false, true) {
		logger.Ctx(l.ctx).Info("LogStore service is already stopped")
	}

	// Stop background cleanup goroutine and wait for it to finish
	l.stopBackgroundCleanup()

	// Clean up all segment processors
	l.spMu.Lock()
	defer l.spMu.Unlock()

	totalProcessors := 0
	for logId, processors := range l.segmentProcessors {
		totalProcessors += len(processors)
		for segmentId, processor := range processors {
			l.closeSegmentProcessorUnsafe(context.Background(), logId, segmentId, processor)
		}
	}

	// Clear the maps
	l.segmentProcessors = make(map[int64]map[int64]processor.SegmentProcessor)

	metrics.WpLogStoreRunningTotal.WithLabelValues("default").Dec()

	logger.Ctx(l.ctx).Info("LogStore service stopped successfully",
		zap.String("address", l.address),
		zap.Int("cleanedProcessors", totalProcessors))

	return nil
}

func (l *logStore) SetAddress(address string) {
	oldAddress := l.address
	l.address = address
	logger.Ctx(l.ctx).Info("LogStore address updated",
		zap.String("oldAddress", oldAddress),
		zap.String("newAddress", address))
}

func (l *logStore) GetAddress() string {
	return l.address
}

func (l *logStore) SetEtcdClient(etcdCli *clientv3.Client) {
	l.etcdCli = etcdCli
	logger.Ctx(l.ctx).Info("LogStore etcd client updated",
		zap.Bool("clientSet", etcdCli != nil))
}

func (l *logStore) Register(ctx context.Context) error {
	logger.Ctx(ctx).Info("Registering LogStore service to etcd",
		zap.String("address", l.address))

	// register this node to etcd and keep alive
	// TODO

	logger.Ctx(ctx).Info("LogStore service registration completed (TODO implementation)",
		zap.String("address", l.address))

	return nil
}

func (l *logStore) AddEntry(ctx context.Context, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "AddEntry")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId) // Using logId as logName for metrics

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, entry.SegId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "add_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "add_entry", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, err
	}
	entryId, err := segmentProcessor.AddEntry(ctx, entry, syncedResultCh)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "add_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "add_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "add_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "add_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return entryId, nil
}

func (l *logStore) getOrCreateSegmentProcessor(ctx context.Context, logId int64, segmentId int64) (processor.SegmentProcessor, error) {
	l.spMu.Lock()
	defer l.spMu.Unlock()

	segProcessors := make(map[int64]processor.SegmentProcessor)

	if processors, logExists := l.segmentProcessors[logId]; logExists {
		segProcessors = processors
	}

	if processor, segExists := segProcessors[segmentId]; segExists {
		return processor, nil
	}

	logger.Ctx(ctx).Info("Creating new segment processor",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	s := processor.NewSegmentProcessor(ctx, l.cfg, logId, segmentId, l.storageClient)
	segProcessors[segmentId] = s
	l.segmentProcessors[logId] = segProcessors

	// Update metrics for active segment processors
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(fmt.Sprintf("%d", logId)).Inc()

	logger.Ctx(ctx).Info("Segment processor created successfully",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	return s, nil
}

func (l *logStore) getExistsSegmentProcessor(logId int64, segmentId int64) processor.SegmentProcessor {
	l.spMu.Lock()
	defer l.spMu.Unlock()
	if processors, logExists := l.segmentProcessors[logId]; logExists {
		if processor, segExists := processors[segmentId]; segExists {
			return processor
		}
	}
	return nil
}

func (l *logStore) GetBatchEntriesAdv(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	if l.stopped.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetBatchEntriesAdv")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_batch_entries", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_batch_entries", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("maxEntries", maxEntries), zap.Error(err))
		return nil, err
	}
	batchData, err := segmentProcessor.ReadBatchEntriesAdv(ctx, fromEntryId, maxEntries, lastReadState)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_batch_entries", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_batch_entries", "error").Observe(float64(time.Since(start).Milliseconds()))
		if !werr.ErrEntryNotFound.Is(err) && !werr.ErrFileReaderEndOfFile.Is(err) {
			logger.Ctx(ctx).Warn("get batch entries failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("maxEntries", maxEntries), zap.Error(err))
		}
		return nil, err
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_batch_entries", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_batch_entries", "success").Observe(float64(time.Since(start).Milliseconds()))
	return batchData, nil
}

func (l *logStore) CompleteSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompleteSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "fence", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "fence", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	return segmentProcessor.Complete(ctx)
}

func (l *logStore) FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "FenceSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "fence", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "fence", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	lastEntryId, fenceErr := segmentProcessor.Fence(ctx)
	if fenceErr != nil {
		logger.Ctx(ctx).Info("fence segment skip", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(fenceErr))
		return -1, fenceErr
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "fence", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "fence", "success").Observe(float64(time.Since(start).Milliseconds()))
	return lastEntryId, nil
}

func (l *logStore) GetSegmentLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_segment_lac", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_segment_lac", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get segment LAC failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	lac, err := segmentProcessor.GetSegmentLastAddConfirmed(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_segment_lac", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_segment_lac", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get segment LAC failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_segment_lac", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_segment_lac", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return lac, err
}

func (l *logStore) GetSegmentBlockCount(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentBlockCount")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_segment_block_count", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_segment_block_count", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get segment block count failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}

	blockCount, err := segmentProcessor.GetBlocksCount(ctx)
	if err != nil {
		if werr.ErrSegmentProcessorNoWriter.Is(err) {
			// means there is no data yet
			return 0, nil
		}
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_segment_block_count", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_segment_block_count", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get segment block count failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "get_segment_block_count", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "get_segment_block_count", "success").Observe(float64(time.Since(start).Milliseconds()))
	return blockCount, nil
}

// CompactSegment merge all files in a segment into bigger files
func (l *logStore) CompactSegment(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	if l.stopped.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompactSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "compact_segment", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "compact_segment", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("compact segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}
	metadata, err := segmentProcessor.Compact(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "compact_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "compact_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("compact segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "compact_segment", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "compact_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	return metadata, nil
}

func (l *logStore) CleanSegment(ctx context.Context, logId int64, segmentId int64, flag int) error {
	if l.stopped.Load() {
		return werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CleanSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "clean_segment", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "clean_segment", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("clean segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int("flag", flag), zap.Error(err))
		return err
	}
	err = segmentProcessor.Clean(ctx, flag)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "clean_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "clean_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("clean segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int("flag", flag), zap.Error(err))
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "clean_segment", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "clean_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return err
}

// closeSegmentProcessorUnsafe closes a segment processor and updates metrics
// This method should be called while holding the spMu lock
func (l *logStore) closeSegmentProcessorUnsafe(ctx context.Context, logId int64, segmentId int64, processor processor.SegmentProcessor) {
	logger.Ctx(ctx).Info("Closing segment processor",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))

	// Close the processor
	if err := processor.Close(ctx); err != nil {
		logger.Ctx(ctx).Warn("failed to close segment processor",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
	} else {
		logger.Ctx(ctx).Info("Segment processor closed successfully",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId))
	}

	// Update metrics
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(fmt.Sprintf("%d", logId)).Dec()
}

// cleanupIdleSegmentProcessorsUnsafe removes segment processors that haven't been accessed for the specified duration
// This method should be called while holding the spMu lock
func (l *logStore) cleanupIdleSegmentProcessorsUnsafe(ctx context.Context, maxIdleTime time.Duration) {
	now := time.Now()
	var toRemove []struct {
		logId     int64
		segmentId int64
	}

	logger.Ctx(ctx).Info("Scanning for idle segment processors to cleanup",
		zap.Duration("maxIdleTime", maxIdleTime),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	// TODO: Consider using a unified method to check if the processor can be cleaned up, e.g., no writes and idle time exceeds threshold
	// The current cleanup strategy: for each log, except for the last segment processor, any processor with idle time exceeding the threshold can be cleaned up
	for logId, processors := range l.segmentProcessors {
		// Find the highest segment ID (most likely to be actively writing)
		var maxSegmentId int64 = -1
		for segmentId := range processors {
			if segmentId > maxSegmentId {
				maxSegmentId = segmentId
			}
		}

		// Check each segment for cleanup eligibility
		for segmentId, processor := range processors {
			// Always protect the highest segment ID (most likely writing)
			if segmentId == maxSegmentId {
				continue
			}

			// Get last access time from processor
			lastAccessTimeMs := processor.GetLastAccessTime()
			lastAccessTime := time.UnixMilli(lastAccessTimeMs)

			// Check if idle time exceeds threshold
			if now.Sub(lastAccessTime) > maxIdleTime {
				toRemove = append(toRemove, struct {
					logId     int64
					segmentId int64
				}{logId, segmentId})
			}
		}
	}

	// Perform cleanup
	for _, item := range toRemove {
		if processors, logExists := l.segmentProcessors[item.logId]; logExists {
			if processor, segExists := processors[item.segmentId]; segExists {
				// Close the processor
				l.closeSegmentProcessorUnsafe(ctx, item.logId, item.segmentId, processor)

				// Remove from maps
				delete(processors, item.segmentId)
				if len(processors) == 0 {
					delete(l.segmentProcessors, item.logId)
				} else {
					l.segmentProcessors[item.logId] = processors
				}

				logger.Ctx(ctx).Debug("cleaned up idle segment processor",
					zap.Int64("logId", item.logId),
					zap.Int64("segmentId", item.segmentId))
			}
		}
	}

	if len(toRemove) > 0 {
		logger.Ctx(ctx).Info("Idle segment processor cleanup completed",
			zap.Int("cleanedCount", len(toRemove)),
			zap.Int("remainingProcessors", l.getTotalProcessorCountUnsafe()))
	} else {
		logger.Ctx(ctx).Info("Idle segment processor cleanup completed - no processors cleaned",
			zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))
	}
}

// getTotalProcessorCountUnsafe returns the total number of segment processors
// This method should be called while holding the spMu lock
func (l *logStore) getTotalProcessorCountUnsafe() int {
	total := 0
	for _, processors := range l.segmentProcessors {
		total += len(processors)
	}
	return total
}

// RemoveSegmentProcessor removes a specific segment processor (e.g., after segment cleanup)
// Test Only
func (l *logStore) RemoveSegmentProcessor(ctx context.Context, logId int64, segmentId int64) {
	logger.Ctx(ctx).Info("Removing segment processor",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))

	l.spMu.Lock()
	defer l.spMu.Unlock()

	if processors, logExists := l.segmentProcessors[logId]; logExists {
		if processor, segExists := processors[segmentId]; segExists {
			// Close the processor
			l.closeSegmentProcessorUnsafe(ctx, logId, segmentId, processor)

			// Remove from maps
			delete(processors, segmentId)
			if len(processors) == 0 {
				delete(l.segmentProcessors, logId)
			} else {
				l.segmentProcessors[logId] = processors
			}

			logger.Ctx(ctx).Info("Segment processor removed successfully",
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId),
				zap.Int("remainingProcessors", l.getTotalProcessorCountUnsafe()))
		} else {
			logger.Ctx(ctx).Info("Segment processor not found for removal",
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId))
		}
	} else {
		logger.Ctx(ctx).Info("Log not found for segment processor removal",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId))
	}
}

// startBackgroundCleanup starts the background cleanup goroutine
func (l *logStore) startBackgroundCleanup() {
	logger.Ctx(l.ctx).Info("Starting background segment processor cleanup goroutine")

	l.cleanupWg.Add(1)
	go l.backgroundCleanupLoop()
}

// stopBackgroundCleanup stops the background cleanup goroutine and waits for it to finish
func (l *logStore) stopBackgroundCleanup() {
	logger.Ctx(l.ctx).Info("Stopping background segment processor cleanup goroutine")

	close(l.cleanupDone)
	l.cleanupWg.Wait()

	logger.Ctx(l.ctx).Info("Background segment processor cleanup goroutine stopped")
}

// backgroundCleanupLoop runs the background cleanup logic
func (l *logStore) backgroundCleanupLoop() {
	defer l.cleanupWg.Done()

	// Cleanup configuration
	const cleanupInterval = 1 * time.Minute // How often to check for cleanup
	const maxIdleTime = 5 * time.Minute     // How long a processor can be idle before cleanup

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	logger.Ctx(l.ctx).Info("Background cleanup goroutine started",
		zap.Duration("cleanupInterval", cleanupInterval),
		zap.Duration("maxIdleTime", maxIdleTime))

	for {
		select {
		case <-l.cleanupDone:
			logger.Ctx(l.ctx).Info("Background cleanup goroutine received shutdown signal")
			return
		case <-l.ctx.Done():
			logger.Ctx(l.ctx).Info("Background cleanup goroutine context cancelled")
			return
		case <-ticker.C:
			l.performBackgroundCleanup(maxIdleTime)
		}
	}
}

// performBackgroundCleanup performs the actual cleanup logic
func (l *logStore) performBackgroundCleanup(maxIdleTime time.Duration) {
	l.spMu.Lock()
	defer l.spMu.Unlock()

	totalProcessors := l.getTotalProcessorCountUnsafe()

	logger.Ctx(l.ctx).Info("Starting background cleanup cycle",
		zap.Int("totalProcessors", totalProcessors),
		zap.Duration("maxIdleTime", maxIdleTime))

	l.cleanupIdleSegmentProcessorsUnsafe(l.ctx, maxIdleTime)
}
