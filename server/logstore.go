package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/segment"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

//go:generate mockery --dir=./server --name=LogStore --structname=LogStore --output=mocks/mocks_server --filename=mock_logstore.go --with-expecter=true  --outpkg=mocks_server
type LogStore interface {
	Start() error
	Stop() error
	SetAddress(string)
	GetAddress() string
	SetEtcdClient(*clientv3.Client)
	Register(context.Context) error
	AddEntry(context.Context, int64, *segment.SegmentEntry) (int64, <-chan int64, error)
	GetEntry(context.Context, int64, int64, int64) (*segment.SegmentEntry, error)
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
	cfg             *config.Configuration
	ctx             context.Context
	cancel          context.CancelFunc
	etcdCli         *clientv3.Client
	minioCli        minioHandler.MinioHandler
	address         string
	fragmentManager cache.FragmentManager

	spMu              sync.RWMutex
	segmentProcessors map[int64]map[int64]segment.SegmentProcessor
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, minioCli minioHandler.MinioHandler) LogStore {
	ctx, cancel := context.WithCancel(ctx)
	fragmentMgr := cache.GetInstance(cfg.Woodpecker.Logstore.FragmentManager.MaxBytes, cfg.Woodpecker.Logstore.FragmentManager.MaxInterval)
	return &logStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		etcdCli:           etcdCli,
		minioCli:          minioCli,
		fragmentManager:   fragmentMgr,
		segmentProcessors: make(map[int64]map[int64]segment.SegmentProcessor),
	}
}

func (l *logStore) Start() error {
	err := l.Register(context.Background())
	if err != nil {
		return err
	}
	return nil
}
func (l *logStore) Stop() error {
	l.cancel()
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

func (l *logStore) AddEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, <-chan int64, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId) // Using logId as logName for metrics
	segIdStr := fmt.Sprintf("%d", entry.SegmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, entry.SegmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry").Observe(float64(time.Since(start).Milliseconds()))
		return -1, nil, err
	}
	if segmentProcessor.IsFenced() {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "segment_fenced").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry").Observe(float64(time.Since(start).Milliseconds()))
		return -1, nil, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d is fenced, reject add entry:%d", logId, entry.SegmentId, entry.EntryId))
	}
	entryId, syncedCh, err := segmentProcessor.AddEntry(ctx, entry)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry").Observe(float64(time.Since(start).Milliseconds()))
		return -1, nil, werr.ErrSegmentWriteException.WithCauseErr(err)
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry").Observe(float64(time.Since(start).Milliseconds()))
	return entryId, syncedCh, nil
}

func (l *logStore) getOrCreateSegmentProcessor(ctx context.Context, logId int64, segmentId int64) (segment.SegmentProcessor, error) {
	l.spMu.Lock()
	defer l.spMu.Unlock()
	segProcessors := make(map[int64]segment.SegmentProcessor)
	if processors, logExists := l.segmentProcessors[logId]; logExists {
		segProcessors = processors
	}
	if processor, segExists := segProcessors[segmentId]; segExists {
		return processor, nil
	}
	s := segment.NewSegmentProcessor(ctx, l.cfg, logId, segmentId, l.minioCli)
	segProcessors[segmentId] = s
	l.segmentProcessors[logId] = segProcessors

	// Update metrics for active segment processors
	metrics.WpLogStoreActiveSegmentProcessors.WithLabelValues(fmt.Sprintf("%d", logId)).Inc()

	return s, nil
}

func (l *logStore) getExistsSegmentProcessor(logId int64, segmentId int64) segment.SegmentProcessor {
	l.spMu.Lock()
	defer l.spMu.Unlock()
	if processors, logExists := l.segmentProcessors[logId]; logExists {
		if processor, segExists := processors[segmentId]; segExists {
			return processor
		}
	}
	return nil
}

func (l *logStore) GetEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*segment.SegmentEntry, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_entry", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_entry").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	entry, err := segmentProcessor.ReadEntry(ctx, entryId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_entry", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_entry").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_entry", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_entry").Observe(float64(time.Since(start).Milliseconds()))
	return entry, nil
}

func (l *logStore) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	if processor := l.getExistsSegmentProcessor(logId, segmentId); processor != nil {
		processor.SetFenced()
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "fence_segment", "success").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "fence_segment").Observe(float64(time.Since(start).Milliseconds()))
		return nil
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "fence_segment", "segment_not_found").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "fence_segment").Observe(float64(time.Since(start).Milliseconds()))
	return werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d not exists", logId, segmentId))
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
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced").Observe(float64(time.Since(start).Milliseconds()))
		return isFenced, nil
	}
	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced", "segment_not_found").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "is_segment_fenced").Observe(float64(time.Since(start).Milliseconds()))
	return false, werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d not exists", logId, segmentId))
}

func (l *logStore) GetSegmentLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segIdStr := fmt.Sprintf("%d", segmentId)

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "error_get_processor").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_segment_lac").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}
	lac, err := segmentProcessor.GetSegmentLastAddConfirmed(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "error").Inc()
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_segment_lac", "success").Inc()
	}
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_segment_lac").Observe(float64(time.Since(start).Milliseconds()))
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
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact_segment").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	metadata, err := segmentProcessor.Compact(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact_segment").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact_segment", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact_segment").Observe(float64(time.Since(start).Milliseconds()))
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
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "recovery_segment").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	metadata, err := segmentProcessor.Recover(ctx)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "error").Inc()
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "recovery_segment").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recovery_segment", "success").Inc()
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "recovery_segment").Observe(float64(time.Since(start).Milliseconds()))
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
		metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean_segment").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}
	err = segmentProcessor.Clean(ctx, flag)
	if err != nil {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean_segment", "error").Inc()
	} else {
		metrics.WpLogStoreOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean_segment", "success").Inc()
	}
	metrics.WpLogStoreOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean_segment").Observe(float64(time.Since(start).Milliseconds()))
	return err
}
