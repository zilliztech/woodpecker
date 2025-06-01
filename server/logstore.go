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

//go:generate mockery --dir=./server --name=LogStore --structname=LogStore --output=mocks/mocks_server --filename=mock_logstore.go --with-expecter=true  --outpkg=mocks_server
type LogStore interface {
	Start() error
	Stop() error
	SetAddress(string)
	GetAddress() string
	SetEtcdClient(*clientv3.Client)
	Register(context.Context) error
	AddEntry(context.Context, int64, *processor.SegmentEntry) (int64, <-chan int64, error)
	GetEntry(context.Context, int64, int64, int64) (*processor.SegmentEntry, error)
	GetBatchEntries(context.Context, int64, int64, int64, int64) ([]*processor.SegmentEntry, error)
	FenceSegment(context.Context, int64, int64) error
	IsSegmentFenced(context.Context, int64, int64) (bool, error)
	CompactSegment(context.Context, int64, int64) (*proto.SegmentMetadata, error)
	RecoverySegmentFromInProgress(context.Context, int64, int64) (*proto.SegmentMetadata, error)
	RecoverySegmentFromInRecovery(context.Context, int64, int64) (*proto.SegmentMetadata, error)
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
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, minioCli minioHandler.MinioHandler) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	return &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		etcdCli:           etcdCli,
		minioCli:          minioCli,
		segmentProcessors: make(map[int64]map[int64]processor.SegmentProcessor),
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

func (l *logStore) AddEntry(ctx context.Context, logId int64, entry *processor.SegmentEntry) (int64, <-chan int64, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId) // Using logId as logName for metrics
	segIdStr := fmt.Sprintf("%d", entry.SegmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, entry.SegmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "error_get_processor").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegmentId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, nil, err
	}
	if segmentProcessor.IsFenced() {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "segment_fenced").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "segment_fenced").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Debug("add entry reject, segment is fenced", zap.Int64("logId", logId), zap.Int64("segId", entry.SegmentId), zap.Int64("entryId", entry.EntryId))
		return -1, nil, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d is fenced, reject add entry:%d", logId, entry.SegmentId, entry.EntryId))
	}
	entryId, syncedCh, err := segmentProcessor.AddEntry(ctx, entry)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("add entry failed", zap.Int64("logId", logId), zap.Int64("segId", entry.SegmentId), zap.Int64("entryId", entry.EntryId), zap.Error(err))
		return -1, nil, werr.ErrSegmentWriteException.WithCauseErr(err)
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return entryId, syncedCh, nil
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
	s := processor.NewSegmentProcessor(ctx, l.cfg, logId, segmentId, l.minioCli)
	segProcessors[segmentId] = s
	l.segmentProcessors[logId] = segProcessors

	// Update metrics for active segment processors
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(fmt.Sprintf("%d", logId)).Inc()

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

func (l *logStore) GetEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*processor.SegmentEntry, error) {
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

func (l *logStore) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	if processor := l.getExistsSegmentProcessor(logId, segmentId); processor != nil {
		processor.SetFenced()
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "fence_segment", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "fence_segment", "success").Observe(float64(time.Since(start).Milliseconds()))
		return nil
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "fence_segment", "segment_not_found").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "fence_segment", "segment_not_found").Observe(float64(time.Since(start).Milliseconds()))
	fenceErr := werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("processor of log:%d segment:%d not exists", logId, segmentId))
	logger.Ctx(ctx).Info("fence segment skip", zap.Int64("logId", logId), zap.Int64("segId", segmentId), zap.Error(fenceErr))
	return fenceErr
}

func (l *logStore) IsSegmentFenced(ctx context.Context, logId int64, segmentId int64) (bool, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	if processor := l.getExistsSegmentProcessor(logId, segmentId); processor != nil {
		isFenced := processor.IsFenced()
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

// RecoverySegmentFromInRecovery read logFiles to get meta info
func (l *logStore) RecoverySegmentFromInRecovery(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	// same as RecoverySegmentFromInProgress currently
	return l.RecoverySegmentFromInProgress(ctx, logId, segmentId)
}

func (l *logStore) CleanSegment(ctx context.Context, logId int64, segmentId int64, flag int) error {
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
