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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	AddEntry(ctx context.Context, bucketName string, rootPath string, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error)
	GetBatchEntriesAdv(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error)
	FenceSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error)
	CompleteSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) (int64, error)
	CompactSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error)
	GetSegmentBlockCount(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error)
	UpdateLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) error
	CleanSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, flag int) error
}

var _ LogStore = (*logStore)(nil)

type logStore struct {
	cfg           *config.Configuration
	ctx           context.Context
	cancel        context.CancelFunc
	storageClient storageclient.ObjectStorage
	address       string

	spMu              sync.RWMutex
	segmentProcessors map[string]map[int64]processor.SegmentProcessor // bucketName/rootPath/logId,segmentId -> segmentProcessor

	// Background cleanup goroutine management
	cleanupWg   sync.WaitGroup
	cleanupDone chan struct{}
	stopped     atomic.Bool
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, storageClient storageclient.ObjectStorage) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	logStore := &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		storageClient:     storageClient,
		segmentProcessors: make(map[string]map[int64]processor.SegmentProcessor),
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
		logger.Ctx(l.ctx).Debug("LogStore service is already stopped")
	}

	// Stop background cleanup goroutine and wait for it to finish
	l.stopBackgroundCleanup()

	// Clean up all segment processors with a timeout to avoid indefinite blocking
	l.spMu.Lock()
	defer l.spMu.Unlock()

	shutdownTimeout := time.Duration(l.cfg.Woodpecker.Logstore.ProcessorCleanupPolicy.ShutdownTimeout.Seconds()) * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	totalProcessors := 0
	for logKey, processors := range l.segmentProcessors {
		for segmentId, processor := range processors {
			totalProcessors += 1
			l.closeSegmentProcessorUnsafe(shutdownCtx, logKey, segmentId, processor)
		}
	}

	// Clear the maps
	l.segmentProcessors = make(map[string]map[int64]processor.SegmentProcessor)

	metrics.WpLogStoreRunningTotal.WithLabelValues("default").Dec()

	logger.Ctx(l.ctx).Info("LogStore service stopped successfully",
		zap.String("address", l.address),
		zap.Int("cleanedProcessors", totalProcessors))

	return nil
}

func (l *logStore) SetAddress(address string) {
	l.spMu.Lock()
	defer l.spMu.Unlock()
	oldAddress := l.address
	l.address = address
	logger.Ctx(l.ctx).Info("LogStore address updated",
		zap.String("oldAddress", oldAddress),
		zap.String("newAddress", address))
}

func (l *logStore) GetAddress() string {
	l.spMu.RLock()
	defer l.spMu.RUnlock()
	return l.address
}

func (l *logStore) AddEntry(ctx context.Context, bucketName string, rootPath string, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "AddEntry")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10) // Using logId as logName for metrics

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, entry.SegId)
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

func GetLogKey(bucketName string, rootPath string, logId int64) string {
	return fmt.Sprintf("%s/%s/%d", bucketName, rootPath, logId)
}

func (l *logStore) getOrCreateSegmentProcessor(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (processor.SegmentProcessor, error) {
	logKey := GetLogKey(bucketName, rootPath, logId)

	// Fast path: read lock to check if processor already exists
	l.spMu.RLock()
	if processors, logExists := l.segmentProcessors[logKey]; logExists {
		if segProcessor, segExists := processors[segmentId]; segExists {
			l.spMu.RUnlock()
			return segProcessor, nil
		}
	}
	l.spMu.RUnlock()

	// Slow path: write lock to create new processor
	l.spMu.Lock()
	defer l.spMu.Unlock()

	// Double-check after acquiring write lock
	if processors, logExists := l.segmentProcessors[logKey]; logExists {
		if segProcessor, segExists := processors[segmentId]; segExists {
			return segProcessor, nil
		}
	}

	logger.Ctx(ctx).Info("Creating new segment processor",
		zap.String("bucketName", bucketName),
		zap.String("rootPath", rootPath),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	s := processor.NewSegmentProcessor(ctx, l.cfg, bucketName, rootPath, logId, segmentId, l.storageClient)

	// Initialize log map if not exists
	if _, exists := l.segmentProcessors[logKey]; !exists {
		l.segmentProcessors[logKey] = make(map[int64]processor.SegmentProcessor)
	}
	l.segmentProcessors[logKey][segmentId] = s

	// Update metrics for active segment processors
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(strconv.FormatInt(logId, 10)).Inc()

	logger.Ctx(ctx).Info("Segment processor created successfully",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	return s, nil
}

// Test Only
func (l *logStore) getExistsSegmentProcessor(bucketName string, rootPath string, logId int64, segmentId int64) processor.SegmentProcessor {
	l.spMu.RLock()
	defer l.spMu.RUnlock()

	logKey := GetLogKey(bucketName, rootPath, logId)
	if processors, logExists := l.segmentProcessors[logKey]; logExists {
		if segProcessor, segExists := processors[segmentId]; segExists {
			return segProcessor
		}
	}
	return nil
}

func (l *logStore) GetBatchEntriesAdv(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	if l.stopped.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetBatchEntriesAdv")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
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

func (l *logStore) CompleteSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompleteSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)
	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "complete", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "complete", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("complete segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	return segmentProcessor.Complete(ctx, lac)
}

func (l *logStore) FenceSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "FenceSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "fence", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "fence", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("fence segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	lastEntryId, fenceErr := segmentProcessor.Fence(ctx)
	if fenceErr != nil {
		logger.Ctx(ctx).Debug("fence segment skip", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(fenceErr))
		return -1, fenceErr
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "fence", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "fence", "success").Observe(float64(time.Since(start).Milliseconds()))
	return lastEntryId, nil
}

func (l *logStore) GetSegmentLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
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

func (l *logStore) GetSegmentBlockCount(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentBlockCount")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
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
func (l *logStore) CompactSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	if l.stopped.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompactSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
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

func (l *logStore) CleanSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, flag int) error {
	if l.stopped.Load() {
		return werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CleanSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
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

func (l *logStore) UpdateLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) error {
	if l.stopped.Load() {
		return werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "UpdateLastAddConfirmed")
	defer sp.End()
	start := time.Now()
	logIdStr := strconv.FormatInt(logId, 10)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "update_lac", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "update_lac", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("update segment lac failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("lac", lac), zap.Error(err))
		return err
	}
	err = segmentProcessor.UpdateSegmentLastAddConfirmed(ctx, lac)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "update_lac", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "update_lac", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("update segment lac failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("lac", lac), zap.Error(err))
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, "update_lac", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, "update_lac", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return err
}

// closeSegmentProcessorUnsafe closes a segment processor and updates metrics
// This method should be called while holding the spMu lock
func (l *logStore) closeSegmentProcessorUnsafe(ctx context.Context, logKey string, segmentId int64, processor processor.SegmentProcessor) {
	logger.Ctx(ctx).Info("Closing segment processor",
		zap.String("logKey", logKey),
		zap.Int64("segmentId", segmentId))

	// Close the processor
	if err := processor.Close(ctx); err != nil {
		logger.Ctx(ctx).Warn("failed to close segment processor",
			zap.String("logKey", logKey),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
	} else {
		logger.Ctx(ctx).Info("Segment processor closed successfully",
			zap.String("logKey", logKey),
			zap.Int64("segmentId", segmentId))
	}

	// Update metrics
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(strconv.FormatInt(processor.GetLogId(), 10)).Dec()
}

// collectIdleSegmentProcessorsUnsafe collects idle segment processors and removes them from the map.
// The actual closing of processors should be done outside the lock by the caller.
// This method should be called while holding the spMu lock.
func (l *logStore) collectIdleSegmentProcessorsUnsafe(ctx context.Context, maxIdleTime time.Duration) []struct {
	logKey    string
	segmentId int64
	processor processor.SegmentProcessor
} {
	now := time.Now()
	var toRemove []struct {
		logKey    string
		segmentId int64
		processor processor.SegmentProcessor
	}

	logger.Ctx(ctx).Info("Scanning for idle segment processors to cleanup",
		zap.Duration("maxIdleTime", maxIdleTime),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	// The cleanup strategy: for each log, except for the segment with the highest segmentId,
	// any processor with idle time exceeding the threshold can be cleaned up
	for logKey, processors := range l.segmentProcessors {
		// Find the highest segment ID (most likely to be actively writing)
		var maxSegmentId int64 = -1
		for segmentId := range processors {
			if segmentId > maxSegmentId {
				maxSegmentId = segmentId
			}
		}

		// Check each segment for cleanup eligibility
		for segmentId, proc := range processors {
			// Always protect the highest segment ID (most likely writing)
			if segmentId == maxSegmentId {
				continue
			}

			// Get last access time from processor
			lastAccessTimeMs := proc.GetLastAccessTime()
			lastAccessTime := time.UnixMilli(lastAccessTimeMs)

			// Check if idle time exceeds threshold
			if now.Sub(lastAccessTime) > maxIdleTime {
				toRemove = append(toRemove, struct {
					logKey    string
					segmentId int64
					processor processor.SegmentProcessor
				}{logKey, segmentId, proc})
			}
		}
	}

	// Remove from maps under the lock
	for _, item := range toRemove {
		if processors, logExists := l.segmentProcessors[item.logKey]; logExists {
			delete(processors, item.segmentId)
			if len(processors) == 0 {
				delete(l.segmentProcessors, item.logKey)
			}
		}
	}

	if len(toRemove) > 0 {
		logger.Ctx(ctx).Info("Idle segment processors collected for cleanup",
			zap.Int("cleanedCount", len(toRemove)),
			zap.Int("remainingProcessors", l.getTotalProcessorCountUnsafe()))
	} else {
		logger.Ctx(ctx).Info("Idle segment processor cleanup completed - no processors to clean",
			zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))
	}

	return toRemove
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
func (l *logStore) RemoveSegmentProcessor(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) {
	logger.Ctx(ctx).Info("Removing segment processor",
		zap.String("bucketName", bucketName),
		zap.String("rootPath", rootPath),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))

	l.spMu.Lock()
	defer l.spMu.Unlock()

	logKey := GetLogKey(bucketName, rootPath, logId)

	if processors, logExists := l.segmentProcessors[logKey]; logExists {
		if processor, segExists := processors[segmentId]; segExists {
			// Close the processor
			l.closeSegmentProcessorUnsafe(ctx, logKey, segmentId, processor)

			// Remove from maps
			delete(processors, segmentId)
			if len(processors) == 0 {
				delete(l.segmentProcessors, logKey)
			}

			logger.Ctx(ctx).Info("Segment processor removed successfully",
				zap.String("logKey", logKey),
				zap.Int64("segmentId", segmentId),
				zap.Int("remainingProcessors", l.getTotalProcessorCountUnsafe()))
		} else {
			logger.Ctx(ctx).Info("Segment processor not found for removal",
				zap.String("logKey", logKey),
				zap.Int64("segmentId", segmentId))
		}
	} else {
		logger.Ctx(ctx).Info("Log not found for segment processor removal",
			zap.String("logKey", logKey),
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

	// Cleanup configuration from config
	cleanupInterval := time.Duration(l.cfg.Woodpecker.Logstore.ProcessorCleanupPolicy.CleanupInterval.Seconds()) * time.Second
	maxIdleTime := time.Duration(l.cfg.Woodpecker.Logstore.ProcessorCleanupPolicy.MaxIdleTime.Seconds()) * time.Second

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
	// Phase 1: Under lock, collect idle processors and remove from map
	l.spMu.Lock()

	totalProcessors := l.getTotalProcessorCountUnsafe()

	logger.Ctx(l.ctx).Info("Starting background cleanup cycle",
		zap.Int("totalProcessors", totalProcessors),
		zap.Duration("maxIdleTime", maxIdleTime))

	idleProcessors := l.collectIdleSegmentProcessorsUnsafe(l.ctx, maxIdleTime)
	l.spMu.Unlock()

	// Phase 2: Close processors outside the lock to avoid blocking I/O under lock
	for _, item := range idleProcessors {
		l.closeSegmentProcessorUnsafe(l.ctx, item.logKey, item.segmentId, item.processor)

		logger.Ctx(l.ctx).Info("cleaned up idle segment processor",
			zap.String("logKey", item.logKey),
			zap.Int64("segmentId", item.segmentId))
	}
}
