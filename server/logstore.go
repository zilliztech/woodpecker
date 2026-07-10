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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
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
	// AddEntryBatch buffers a run of consecutive entries (same segment) in one
	// call, amortizing the per-entry processor/writer/buffer overhead. Returns
	// the buffered id per entry (same order as input).
	AddEntryBatch(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, entries []*proto.LogEntry, resultChs []channel.ResultChannel) ([]int64, error)
	GetBatchEntriesAdv(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, fromEntryId int64, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error)
	FenceSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error)
	CompleteSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) (int64, error)
	CompactSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error)
	GetSegmentBlockCount(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error)
	UpdateLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) error
	CleanSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, flag int) error
	GetActiveProcessorCount() int
	RejectNewWrites()
	AllowNewWrites()
	HasLocalSegmentData() bool
	EvictLog(ctx context.Context, bucketName string, rootPath string, logId int64) error
	EvictInstance(ctx context.Context, bucketName string, rootPath string) error
}

var _ LogStore = (*logStore)(nil)

type logStore struct {
	cfg           *config.Configuration
	ctx           context.Context
	cancel        context.CancelFunc
	storageClient storageclient.ObjectStorage
	address       string
	syncScheduler *stagedstorage.SyncScheduler

	spMu              sync.RWMutex
	segmentProcessors map[string]map[int64]processor.SegmentProcessor // bucketName/rootPath/logId,segmentId -> segmentProcessor

	// Logs/instances marked for deletion: serving is rejected and processors are evicted.
	// Guarded by spMu (same lock as segmentProcessors).
	// Entries are only added here; they are pruned by the maintenance reclaim task (a later plan).
	deletingLogs      map[string]struct{} // logKey set
	deletingInstances map[string]struct{} // instanceKey set

	maintenance  *NodeMaintenanceManager
	stopped      atomic.Bool
	rejectWrites atomic.Bool // separate from stopped: only blocks new writes during decommission, not reads
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, storageClient storageclient.ObjectStorage) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	logStore := &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		storageClient:     storageClient,
		syncScheduler:     stagedstorage.NewSyncScheduler(runtime.NumCPU() * 2),
		segmentProcessors: make(map[string]map[int64]processor.SegmentProcessor),
		deletingLogs:      make(map[string]struct{}),
		deletingInstances: make(map[string]struct{}),
		address:           net.GetIP(""),
	}
	logStore.stopped.Store(true)
	logStore.maintenance = NewNodeMaintenanceManager(ctx)
	logStore.maintenance.Register(newIdleProcessorCleanupTask(logStore))
	logStore.maintenance.Register(newDeletedLogReclaimTask(logStore, cfg.Woodpecker.Logstore.MaintenanceStrategy.DeleteGracePeriod.Duration.Duration()))

	logger.Ctx(ctx).Info("LogStore created successfully",
		zap.String("address", logStore.address))

	return logStore
}

func (l *logStore) Start() error {
	logger.Ctx(l.ctx).Info("Starting LogStore service",
		zap.String("address", l.address))

	metrics.WpLogStoreRunningTotal.WithLabelValues(metrics.NodeID).Inc()

	if err := l.rebuildDeletingSetsFromMarkers(); err != nil {
		return err
	}

	l.maintenance.Start()
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

	// Stop maintenance manager and wait for it to finish
	l.maintenance.Stop()

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
			l.closeSegmentProcessor(shutdownCtx, logKey, segmentId, processor)
		}
	}

	if l.syncScheduler != nil {
		l.syncScheduler.Close()
	}

	// Clear the maps
	l.segmentProcessors = make(map[string]map[int64]processor.SegmentProcessor)

	metrics.WpLogStoreActiveLogs.Reset()
	metrics.WpLogStoreActiveSegments.Reset()
	metrics.WpLogStoreRunningTotal.WithLabelValues(metrics.NodeID).Dec()

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
	if l.stopped.Load() || l.rejectWrites.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "AddEntry")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.add_entry", nil, nil, metrics.WithInstance(bucketName, rootPath), metrics.WithLogSegment(logId, entry.SegId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, entry.SegId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, err
	}
	entryId, err := segmentProcessor.AddEntry(ctx, entry, syncedResultCh)
	if err != nil {
		status = "error"
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, err
	}
	return entryId, nil
}

func (l *logStore) AddEntryBatch(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, entries []*proto.LogEntry, resultChs []channel.ResultChannel) ([]int64, error) {
	if l.stopped.Load() || l.rejectWrites.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "AddEntryBatch")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.add_entry_batch", nil, nil, metrics.WithInstance(bucketName, rootPath), metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry_batch", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "add_entry_batch", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("add entry batch failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}
	ids, err := segmentProcessor.AddEntryBatch(ctx, entries, resultChs)
	if err != nil {
		status = "error"
		logger.Ctx(ctx).Warn("add entry batch failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return ids, err
	}
	return ids, nil
}

func GetLogKey(bucketName string, rootPath string, logId int64) string {
	return fmt.Sprintf("%s/%s/%d", bucketName, rootPath, logId)
}

// GetInstanceKey returns the key prefix shared by all logs under a bucket/rootPath instance.
func GetInstanceKey(bucketName string, rootPath string) string {
	return fmt.Sprintf("%s/%s", bucketName, rootPath)
}

func (l *logStore) getOrCreateSegmentProcessor(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (processor.SegmentProcessor, error) {
	logKey := GetLogKey(bucketName, rootPath, logId)
	ns := bucketName + "/" + rootPath

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

	// Reject serving a log/instance that is being deleted.
	if _, deleting := l.deletingLogs[logKey]; deleting {
		return nil, werr.ErrLogBeingDeleted
	}
	if _, deleting := l.deletingInstances[GetInstanceKey(bucketName, rootPath)]; deleting {
		return nil, werr.ErrLogBeingDeleted
	}

	logger.Ctx(ctx).Info("Creating new segment processor",
		zap.String("bucketName", bucketName),
		zap.String("rootPath", rootPath),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalProcessors", l.getTotalProcessorCountUnsafe()))

	s := processor.NewSegmentProcessor(ctx, l.cfg, bucketName, rootPath, logId, segmentId, l.storageClient, l.syncScheduler)

	// Initialize log map if not exists
	if _, exists := l.segmentProcessors[logKey]; !exists {
		l.segmentProcessors[logKey] = make(map[int64]processor.SegmentProcessor)
		metrics.WpLogStoreActiveLogs.WithLabelValues(metrics.NodeID, ns).Inc()
	}
	l.segmentProcessors[logKey][segmentId] = s
	metrics.WpLogStoreActiveSegments.WithLabelValues(metrics.NodeID, ns, strconv.FormatInt(logId, 10)).Inc()

	// Update metrics for active segment processors
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(metrics.NodeID, ns, strconv.FormatInt(logId, 10)).Inc()

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
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.get_batch_entries", nil, nil, metrics.WithInstance(bucketName, rootPath), metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "get_batch_entries", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "get_batch_entries", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("get entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("maxEntries", maxEntries), zap.Error(err))
		return nil, err
	}
	batchData, err := segmentProcessor.ReadBatchEntriesAdv(ctx, fromEntryId, maxEntries, lastReadState)
	if err != nil {
		if werr.ErrEntryNotFound.Is(err) || werr.ErrFileReaderEndOfFile.Is(err) {
			// Reaching the end of the log is a routine, healthy read outcome, not a failure.
			status = "end_of_file"
		} else {
			status = "error"
			logger.Ctx(ctx).Warn("get batch entries failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("maxEntries", maxEntries), zap.Error(err))
		}
		return nil, err
	}
	return batchData, nil
}

func (l *logStore) CompleteSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompleteSegment")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.complete", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "complete", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "complete", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
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
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.fence", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "fence", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "fence", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("fence segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	lastEntryId, fenceErr := segmentProcessor.Fence(ctx)
	if fenceErr != nil {
		status = "error"
		logger.Ctx(ctx).Debug("fence segment skip", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(fenceErr))
		return -1, fenceErr
	}
	return lastEntryId, nil
}

func (l *logStore) GetSegmentLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.get_segment_lac", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "get_segment_lac", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "get_segment_lac", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("get segment LAC failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	lac, err := segmentProcessor.GetSegmentLastAddConfirmed(ctx)
	if err != nil {
		status = "error"
		logger.Ctx(ctx).Warn("get segment LAC failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
	}
	return lac, err
}

func (l *logStore) GetSegmentBlockCount(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (int64, error) {
	if l.stopped.Load() {
		return -1, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentBlockCount")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.get_segment_block_count", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "get_segment_block_count", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "get_segment_block_count", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("get segment block count failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}

	blockCount, err := segmentProcessor.GetBlocksCount(ctx)
	if err != nil {
		if werr.ErrSegmentProcessorNoWriter.Is(err) {
			return 0, nil
		}
		status = "error"
		logger.Ctx(ctx).Warn("get segment block count failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	return blockCount, nil
}

// CompactSegment merge all files in a segment into bigger files
func (l *logStore) CompactSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	if l.stopped.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompactSegment")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.compact_segment", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "compact_segment", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "compact_segment", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("compact segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}
	metadata, err := segmentProcessor.Compact(ctx)
	if err != nil {
		status = "error"
		logger.Ctx(ctx).Warn("compact segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}
	return metadata, nil
}

func (l *logStore) CleanSegment(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, flag int) error {
	if l.stopped.Load() {
		return werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CleanSegment")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.clean_segment", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "clean_segment", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "clean_segment", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("clean segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int("flag", flag), zap.Error(err))
		return err
	}
	err = segmentProcessor.Clean(ctx, flag)
	if err != nil {
		status = "error"
		logger.Ctx(ctx).Warn("clean segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int("flag", flag), zap.Error(err))
	}
	return err
}

func (l *logStore) UpdateLastAddConfirmed(ctx context.Context, bucketName string, rootPath string, logId int64, segmentId int64, lac int64) error {
	if l.stopped.Load() {
		return werr.ErrLogStoreShutdown
	}
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "UpdateLastAddConfirmed")
	defer sp.End()
	logIdStr := strconv.FormatInt(logId, 10)
	ns := bucketName + "/" + rootPath
	op := metrics.StartOp("logstore.update_lac", nil, nil, metrics.WithLogSegment(logId, segmentId))
	status := "success"
	defer func() {
		op.End(status)
		elapsed := float64(time.Since(op.StartedAt()).Milliseconds())
		metrics.WpLogStoreOperationsTotal.WithLabelValues(metrics.NodeID, ns, logIdStr, "update_lac", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(metrics.NodeID, ns, logIdStr, "update_lac", status).Observe(elapsed)
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, bucketName, rootPath, logId, segmentId)
	if err != nil {
		status = "error_get_processor"
		logger.Ctx(ctx).Warn("update segment lac failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("lac", lac), zap.Error(err))
		return err
	}
	err = segmentProcessor.UpdateSegmentLastAddConfirmed(ctx, lac)
	if err != nil {
		status = "error"
		logger.Ctx(ctx).Warn("update segment lac failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("lac", lac), zap.Error(err))
	}
	return err
}

// GetActiveProcessorCount returns the total number of active segment processors.
func (l *logStore) GetActiveProcessorCount() int {
	l.spMu.RLock()
	defer l.spMu.RUnlock()
	return l.getTotalProcessorCountUnsafe()
}

// RejectNewWrites sets a flag to reject new write operations while allowing
// reads and segment completions to continue (for graceful decommission).
func (l *logStore) RejectNewWrites() {
	if !l.rejectWrites.Swap(true) {
		logger.Ctx(context.Background()).Info("log store now rejecting new writes (decommission)",
			zap.String("nodeID", metrics.NodeID))
	}
}

// AllowNewWrites clears the write-rejection flag set by RejectNewWrites,
// restoring normal write acceptance after a cancelled decommission.
func (l *logStore) AllowNewWrites() {
	if l.rejectWrites.Swap(false) {
		logger.Ctx(context.Background()).Info("log store accepting new writes again (decommission cancelled)",
			zap.String("nodeID", metrics.NodeID))
	}
}

// HasLocalSegmentData scans the data directory for any remaining segment data files (data.log).
// Returns true if at least one non-empty data.log file exists under the root path.
// This is used during decommission to determine if all segment data has been cleaned up.
func (l *logStore) HasLocalSegmentData() bool {
	rootPath := l.cfg.Woodpecker.Storage.RootPath
	if rootPath == "" {
		return false
	}
	found := false
	_ = filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			// Skip ONLY the top-level marker dir, not any user dir that happens to be
			// named ".deleted" deeper under rootPath.
			if path == filepath.Join(rootPath, deleteMarkerDir) {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() == "data.log" {
			info, statErr := d.Info()
			if statErr == nil && info.Size() > 0 {
				found = true
				return filepath.SkipAll // stop walking, we found data
			}
		}
		return nil
	})
	return found
}

func (l *logStore) EvictLog(ctx context.Context, bucketName string, rootPath string, logId int64) error {
	if root := l.cfg.Woodpecker.Storage.RootPath; root != "" {
		if err := writeDeleteMarker(ctx, root, deleteMarker{
			Bucket: bucketName, RootPath: rootPath, LogId: logId, DeletedAt: time.Now().Unix(),
		}); err != nil {
			return werr.ErrMarkDeleteFailed.WithCauseErr(err)
		}
	}
	logKey := GetLogKey(bucketName, rootPath, logId)
	ns := GetInstanceKey(bucketName, rootPath)
	l.spMu.Lock()
	l.deletingLogs[logKey] = struct{}{}
	procs := l.segmentProcessors[logKey]
	delete(l.segmentProcessors, logKey)
	if len(procs) > 0 {
		metrics.WpLogStoreActiveLogs.WithLabelValues(metrics.NodeID, ns).Dec()
	}
	l.spMu.Unlock()
	for segmentId, proc := range procs {
		l.closeSegmentProcessor(ctx, logKey, segmentId, proc)
	}
	logger.Ctx(ctx).Info("evicted log", zap.String("logKey", logKey), zap.Int("closedProcessors", len(procs)))
	return nil
}

func (l *logStore) EvictInstance(ctx context.Context, bucketName string, rootPath string) error {
	if root := l.cfg.Woodpecker.Storage.RootPath; root != "" {
		if err := writeDeleteMarker(ctx, root, deleteMarker{
			Bucket: bucketName, RootPath: rootPath, Instance: true, DeletedAt: time.Now().Unix(),
		}); err != nil {
			return werr.ErrMarkDeleteFailed.WithCauseErr(err)
		}
	}
	instanceKey := GetInstanceKey(bucketName, rootPath)
	prefix := instanceKey + "/"
	l.spMu.Lock()
	l.deletingInstances[instanceKey] = struct{}{}
	type item struct {
		logKey    string
		segmentId int64
		proc      processor.SegmentProcessor
	}
	var collected []item
	for logKey, procs := range l.segmentProcessors {
		if !strings.HasPrefix(logKey, prefix) {
			continue
		}
		for segmentId, proc := range procs {
			collected = append(collected, item{logKey, segmentId, proc})
		}
		delete(l.segmentProcessors, logKey)
		metrics.WpLogStoreActiveLogs.WithLabelValues(metrics.NodeID, instanceKey).Dec()
	}
	l.spMu.Unlock()
	for _, it := range collected {
		l.closeSegmentProcessor(ctx, it.logKey, it.segmentId, it.proc)
	}
	logger.Ctx(ctx).Info("evicted instance", zap.String("instanceKey", instanceKey), zap.Int("closedProcessors", len(collected)))
	return nil
}

// rebuildDeletingSetsFromMarkers repopulates deletingLogs/deletingInstances from on-disk
// markers. Called from Start() BEFORE stopped.Store(false) so a marked log is never served
// after a restart.
func (l *logStore) rebuildDeletingSetsFromMarkers() error {
	root := l.cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return nil
	}
	markers, err := scanDeleteMarkers(l.ctx, root)
	if err != nil {
		return err
	}
	l.spMu.Lock()
	defer l.spMu.Unlock()
	for _, m := range markers {
		if m.Instance {
			l.deletingInstances[GetInstanceKey(m.Bucket, m.RootPath)] = struct{}{}
		} else {
			l.deletingLogs[GetLogKey(m.Bucket, m.RootPath, m.LogId)] = struct{}{}
		}
	}
	logger.Ctx(l.ctx).Info("rebuilt deleting sets from markers", zap.Int("markerCount", len(markers)))
	return nil
}

// closeSegmentProcessor closes a segment processor and updates metrics
// This method does not require any lock to be held
func (l *logStore) closeSegmentProcessor(ctx context.Context, logKey string, segmentId int64, processor processor.SegmentProcessor) {
	ns := logKey[:strings.LastIndex(logKey, "/")]

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
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(metrics.NodeID, ns, strconv.FormatInt(processor.GetLogId(), 10)).Dec()
	metrics.WpLogStoreActiveSegments.WithLabelValues(metrics.NodeID, ns, strconv.FormatInt(processor.GetLogId(), 10)).Dec()
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
				metrics.WpLogStoreActiveLogs.WithLabelValues(metrics.NodeID, item.logKey[:strings.LastIndex(item.logKey, "/")]).Dec()
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
			l.closeSegmentProcessor(ctx, logKey, segmentId, processor)

			// Remove from maps
			delete(processors, segmentId)
			if len(processors) == 0 {
				delete(l.segmentProcessors, logKey)
				metrics.WpLogStoreActiveLogs.WithLabelValues(metrics.NodeID, bucketName+"/"+rootPath).Dec()
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
		l.closeSegmentProcessor(l.ctx, item.logKey, item.segmentId, item.processor)

		logger.Ctx(l.ctx).Info("cleaned up idle segment processor",
			zap.String("logKey", item.logKey),
			zap.Int64("segmentId", item.segmentId))
	}

	if l.syncScheduler != nil {
		metrics.WpSyncSchedulerScheduled.WithLabelValues(metrics.NodeID).Set(float64(l.syncScheduler.Scheduled()))
		metrics.WpSyncSchedulerRunning.WithLabelValues(metrics.NodeID).Set(float64(l.syncScheduler.Running()))
		metrics.WpSyncSchedulerWaiting.WithLabelValues(metrics.NodeID).Set(float64(l.syncScheduler.Waiting()))
		metrics.WpSyncSchedulerCapacity.WithLabelValues(metrics.NodeID).Set(float64(l.syncScheduler.Capacity()))
	}
}
