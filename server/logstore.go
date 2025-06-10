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
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
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
	SetAddress(string)
	GetAddress() string
	SetEtcdClient(*clientv3.Client)
	Register(context.Context) error
	// TODO use ResultChannel abstract instead of chan
	AddEntry(context.Context, int64, *processor.SegmentEntry, chan<- int64) (int64, error)
	GetEntry(context.Context, int64, int64, int64) (*processor.SegmentEntry, error)
	GetBatchEntries(context.Context, int64, int64, int64, int64) ([]*processor.SegmentEntry, error)
	FenceSegment(context.Context, int64, int64) (int64, error)
	IsSegmentFenced(context.Context, int64, int64) (bool, error)
	CompactSegment(context.Context, int64, int64) (*proto.SegmentMetadata, error)
	RecoverySegmentFromInProgress(context.Context, int64, int64) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(context.Context, int64, int64) (int64, error)
	CleanSegment(context.Context, int64, int64, int) error
}

var _ LogStore = (*logStore)(nil)

type logStore struct {
	cfg      *config.Configuration
	ctx      context.Context
	cancel   context.CancelFunc
	etcdCli  *clientv3.Client
	minioCli minioHandler.MinioHandler
	address  string

	spMu              sync.RWMutex
	segmentProcessors map[int64]map[int64]processor.SegmentProcessor
	// Track last access time for segment processors to enable cleanup
	segmentProcessorLastAccess map[int64]map[int64]time.Time
	// Track last cleanup time to avoid too frequent cleanup
	lastCleanupTime time.Time
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, minioCli minioHandler.MinioHandler) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	return &logStore{
		cfg:                        cfg,
		ctx:                        ctx,
		cancel:                     cancel,
		etcdCli:                    etcdCli,
		minioCli:                   minioCli,
		segmentProcessors:          make(map[int64]map[int64]processor.SegmentProcessor),
		segmentProcessorLastAccess: make(map[int64]map[int64]time.Time),
	}
}

func (l *logStore) Start() error {
	err := l.Register(context.Background())
	if err != nil {
		return err
	}
	metrics.WpLogStoreRunningTotal.WithLabelValues("default").Inc()
	return nil
}
func (l *logStore) Stop() error {
	l.cancel()

	// Clean up all segment processors
	l.spMu.Lock()
	defer l.spMu.Unlock()

	for logId, processors := range l.segmentProcessors {
		for segmentId, processor := range processors {
			l.closeSegmentProcessorUnsafe(context.Background(), logId, segmentId, processor)
		}
	}

	// Clear the maps
	l.segmentProcessors = make(map[int64]map[int64]processor.SegmentProcessor)
	l.segmentProcessorLastAccess = make(map[int64]map[int64]time.Time)

	metrics.WpLogStoreRunningTotal.WithLabelValues("default").Add(-1)
	return nil
}

func (l *logStore) SetAddress(address string) {
	l.address = address
}

func (l *logStore) GetAddress() string {
	return l.address
}

func (l *logStore) SetEtcdClient(etcdCli *clientv3.Client) {
	l.etcdCli = etcdCli
}

func (l *logStore) Register(ctx context.Context) error {
	// register this node to etcd and keep alive
	// TODO
	return nil
}

func (l *logStore) AddEntry(ctx context.Context, logId int64, entry *processor.SegmentEntry, syncedResultCh chan<- int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "AddEntry")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId) // Using logId as logName for metrics
	segIdStr := fmt.Sprintf("%d", entry.SegmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, entry.SegmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegmentId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, err
	}
	if segmentProcessor.IsFenced(ctx) {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "segment_fenced").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "segment_fenced").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Debug("add entry reject, segment is fenced", zap.Int64("logId", logId), zap.Int64("segId", entry.SegmentId), zap.Int64("entryId", entry.EntryId))
		return -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d is fenced, reject add entry:%d", logId, entry.SegmentId, entry.EntryId))
	}
	entryId, err := segmentProcessor.AddEntry(ctx, entry, syncedResultCh)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegmentId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, werr.ErrSegmentWriteException.WithCauseErr(err)
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return entryId, nil
}

func (l *logStore) getOrCreateSegmentProcessor(ctx context.Context, logId int64, segmentId int64) (processor.SegmentProcessor, error) {
	l.spMu.Lock()
	defer l.spMu.Unlock()

	// Try cleanup idle processors if conditions are met
	l.tryCleanupIdleSegmentProcessorsUnsafe(ctx)

	segProcessors := make(map[int64]processor.SegmentProcessor)
	segAccessTimes := make(map[int64]time.Time)

	if processors, logExists := l.segmentProcessors[logId]; logExists {
		segProcessors = processors
	}
	if accessTimes, logExists := l.segmentProcessorLastAccess[logId]; logExists {
		segAccessTimes = accessTimes
	}

	if processor, segExists := segProcessors[segmentId]; segExists {
		// Update access time
		segAccessTimes[segmentId] = time.Now()
		l.segmentProcessorLastAccess[logId] = segAccessTimes
		return processor, nil
	}

	s := processor.NewSegmentProcessor(ctx, l.cfg, logId, segmentId, l.minioCli)
	segProcessors[segmentId] = s
	segAccessTimes[segmentId] = time.Now()
	l.segmentProcessors[logId] = segProcessors
	l.segmentProcessorLastAccess[logId] = segAccessTimes

	// Update metrics for active segment processors
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(fmt.Sprintf("%d", logId)).Inc()

	return s, nil
}

func (l *logStore) getExistsSegmentProcessor(logId int64, segmentId int64) processor.SegmentProcessor {
	l.spMu.Lock()
	defer l.spMu.Unlock()
	if processors, logExists := l.segmentProcessors[logId]; logExists {
		if processor, segExists := processors[segmentId]; segExists {
			// Update access time
			if accessTimes, exists := l.segmentProcessorLastAccess[logId]; exists {
				accessTimes[segmentId] = time.Now()
				l.segmentProcessorLastAccess[logId] = accessTimes
			}
			return processor
		}
	}
	return nil
}

func (l *logStore) GetEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*processor.SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetEntry")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_entry", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	entry, err := segmentProcessor.ReadEntry(ctx, entryId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		if !werr.ErrEntryNotFound.Is(err) {
			logger.Ctx(ctx).Warn("get entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("entryId", entryId), zap.Error(err))
		}
		return nil, err
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return entry, nil
}

func (l *logStore) GetBatchEntries(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, size int64) ([]*processor.SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetBatchEntries")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_batch_entries", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_batch_entries", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get entry failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("size", size), zap.Error(err))
		return nil, err
	}
	entries, err := segmentProcessor.ReadBatchEntries(ctx, fromEntryId, size)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_batch_entries", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_batch_entries", "error").Observe(float64(time.Since(start).Milliseconds()))
		if !werr.ErrEntryNotFound.Is(err) {
			logger.Ctx(ctx).Warn("get batch entries failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("size", size), zap.Error(err))
		}
		return nil, err
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_batch_entries", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_batch_entries", "success").Observe(float64(time.Since(start).Milliseconds()))
	return entries, nil
}

func (l *logStore) FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "FenceSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	if processor := l.getExistsSegmentProcessor(logId, segmentId); processor != nil {
		lastEntryId, err := processor.SetFenced(ctx)
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "fence_segment", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "fence_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
		return lastEntryId, err
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "fence_segment", "segment_not_found").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "fence_segment", "segment_not_found").Observe(float64(time.Since(start).Milliseconds()))
	fenceErr := werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("processor of log:%d segment:%d not exists", logId, segmentId))
	logger.Ctx(ctx).Info("fence segment skip", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(fenceErr))
	return -1, fenceErr
}

func (l *logStore) IsSegmentFenced(ctx context.Context, logId int64, segmentId int64) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "IsSegmentFenced")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	if processor := l.getExistsSegmentProcessor(logId, segmentId); processor != nil {
		isFenced := processor.IsFenced(ctx)
		status := "not_fenced"
		if isFenced {
			status = "fenced"
		}
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced", status).Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced", status).Observe(float64(time.Since(start).Milliseconds()))
		return isFenced, nil
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced", "segment_not_found").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced", "segment_not_found").Observe(float64(time.Since(start).Milliseconds()))
	checkErr := werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d not exists", logId, segmentId))
	logger.Ctx(ctx).Info("check if segment fenced failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(checkErr))
	return false, checkErr
}

func (l *logStore) GetSegmentLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get segment LAC failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return -1, err
	}
	lac, err := segmentProcessor.GetSegmentLastAddConfirmed(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get segment LAC failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return lac, err
}

// CompactSegment merge all files in a segment into bigger files
func (l *logStore) CompactSegment(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CompactSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact_segment", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact_segment", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("compact segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}
	metadata, err := segmentProcessor.Compact(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("compact segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact_segment", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	return metadata, nil
}

// RecoverySegmentFromInProgress read logFiles to get meta info
func (l *logStore) RecoverySegmentFromInProgress(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "RecoverySegmentFromInProgress")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("recover segment in-progress failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}
	metadata, err := segmentProcessor.Recover(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("recover segment in-progress failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(err))
		return nil, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	return metadata, nil
}

func (l *logStore) CleanSegment(ctx context.Context, logId int64, segmentId int64, flag int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, LogStoreScopeName, "CleanSegment")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean_segment", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean_segment", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("clean segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int("flag", flag), zap.Error(err))
		return err
	}
	err = segmentProcessor.Clean(ctx, flag)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean_segment", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("clean segment failed", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Int("flag", flag), zap.Error(err))
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean_segment", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return err
}

// closeSegmentProcessorUnsafe closes a segment processor and updates metrics
// This method should be called while holding the spMu lock
func (l *logStore) closeSegmentProcessorUnsafe(ctx context.Context, logId int64, segmentId int64, processor processor.SegmentProcessor) {
	// Close the processor
	if err := processor.Close(ctx); err != nil {
		logger.Ctx(ctx).Warn("failed to close segment processor",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
	}

	// Update metrics
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(fmt.Sprintf("%d", logId)).Dec()

	logger.Ctx(ctx).Debug("closed segment processor",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))
}

// tryCleanupIdleSegmentProcessorsUnsafe attempts to clean up idle segment processors if conditions are met
// This method should be called while holding the spMu lock
func (l *logStore) tryCleanupIdleSegmentProcessorsUnsafe(ctx context.Context) {
	// Cleanup configuration - centralized for easy modification
	const cleanupInterval = 1 * time.Minute // How often to check for cleanup
	const maxIdleTime = 5 * time.Minute     // How long a processor can be idle before cleanup
	const minProcessorThreshold = 10        // Minimum processors before cleanup is considered

	now := time.Now()
	totalProcessors := 0
	for _, processors := range l.segmentProcessors {
		totalProcessors += len(processors)
	}

	if totalProcessors > minProcessorThreshold && now.Sub(l.lastCleanupTime) > cleanupInterval {
		l.cleanupIdleSegmentProcessorsUnsafe(ctx, maxIdleTime)
		l.lastCleanupTime = now
	}
}

// cleanupIdleSegmentProcessorsUnsafe removes segment processors that haven't been accessed for the specified duration
// This method should be called while holding the spMu lock
func (l *logStore) cleanupIdleSegmentProcessorsUnsafe(ctx context.Context, maxIdleTime time.Duration) {
	now := time.Now()
	var toRemove []struct {
		logId     int64
		segmentId int64
	}

	// For each log, collect all segment IDs and sort them to find the latest ones
	for logId, accessTimes := range l.segmentProcessorLastAccess {
		// Collect all segment IDs for this log
		var segmentIds []int64
		for segmentId := range accessTimes {
			segmentIds = append(segmentIds, segmentId)
		}

		// Sort segment IDs in descending order (largest first)
		sort.Slice(segmentIds, func(i, j int) bool {
			return segmentIds[i] > segmentIds[j]
		})

		// Determine which segments to protect (top 10 by segment ID)
		protectedSegments := make(map[int64]bool)
		protectCount := 10
		if len(segmentIds) < protectCount {
			protectCount = len(segmentIds)
		}
		for i := 0; i < protectCount; i++ {
			protectedSegments[segmentIds[i]] = true
		}

		// Check each segment for cleanup eligibility
		for segmentId, lastAccess := range accessTimes {
			// Skip if this segment is protected (one of the top 10 by ID)
			if protectedSegments[segmentId] {
				continue
			}

			// Check if idle time exceeds threshold
			if now.Sub(lastAccess) > maxIdleTime {
				toRemove = append(toRemove, struct {
					logId     int64
					segmentId int64
				}{logId, segmentId})
			}
		}
	}

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

				if accessTimes, exists := l.segmentProcessorLastAccess[item.logId]; exists {
					delete(accessTimes, item.segmentId)
					if len(accessTimes) == 0 {
						delete(l.segmentProcessorLastAccess, item.logId)
					} else {
						l.segmentProcessorLastAccess[item.logId] = accessTimes
					}
				}

				logger.Ctx(ctx).Debug("cleaned up idle segment processor",
					zap.Int64("logId", item.logId),
					zap.Int64("segmentId", item.segmentId))
			}
		}
	}

	if len(toRemove) > 0 {
		logger.Ctx(ctx).Info("cleaned up idle segment processors",
			zap.Int("cleanedCount", len(toRemove)),
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
func (l *logStore) RemoveSegmentProcessor(ctx context.Context, logId int64, segmentId int64) {
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

			if accessTimes, exists := l.segmentProcessorLastAccess[logId]; exists {
				delete(accessTimes, segmentId)
				if len(accessTimes) == 0 {
					delete(l.segmentProcessorLastAccess, logId)
				} else {
					l.segmentProcessorLastAccess[logId] = accessTimes
				}
			}

			logger.Ctx(ctx).Info("removed segment processor",
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId))
		}
	}
}
