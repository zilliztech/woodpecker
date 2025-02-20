package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/segment"
)

type LogStore struct {
	sync.RWMutex
	cfg      *config.Configuration
	ctx      context.Context
	cancel   context.CancelFunc
	etcdCli  *clientv3.Client
	minioCli *minio.Client
	address  string

	segmentProcessors map[int64][]segment.SegmentProcessor
}

func NewLogStore(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, minioCli *minio.Client) *LogStore {
	ctx, cancel := context.WithCancel(ctx)
	return &LogStore{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		etcdCli:           etcdCli,
		minioCli:          minioCli,
		segmentProcessors: make(map[int64][]segment.SegmentProcessor),
	}
}

func (l *LogStore) Start() error {
	// TODO start service
	// register to etcd and keep alive
	registerErr := l.Register(context.Background())
	return registerErr
}
func (l *LogStore) Stop() error {
	l.cancel()
	return nil
}

func (l *LogStore) SetAddress(address string) {
	l.address = address
}

func (l *LogStore) GetAddress() string {
	return l.address
}

func (l *LogStore) SetEtcdClient(etcdCli *clientv3.Client) {
	l.etcdCli = etcdCli
}

func (l *LogStore) Register(ctx context.Context) error {
	// register this node to etcd and keep alive
	return nil
}

func (l *LogStore) AddEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, <-chan int64, error) {
	start := time.Now()
	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, entry.SegmentId)
	if err != nil {
		return -1, nil, err
	}
	if segmentProcessor.IsFenced() {
		return -1, nil, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d is fenced", logId, entry.SegmentId))
	}
	entryId, syncedCh, err := segmentProcessor.AddEntry(ctx, entry)
	if err != nil {
		return -1, nil, werr.ErrSegmentWriteException.WithCauseErr(err)
	}
	//log.Printf("LogStore addEntry call, log:%d, entry: %v", logId, entry)
	cost := time.Now().Sub(start)
	metrics.WpAppendReqLatency.WithLabelValues(fmt.Sprintf("%d", logId)).Observe(float64(cost.Milliseconds()))
	metrics.WpAppendBytes.WithLabelValues(fmt.Sprintf("%d", logId)).Observe(float64(len(entry.Data)))
	return entryId, syncedCh, nil
}

func (l *LogStore) getOrCreateSegmentProcessor(ctx context.Context, logId int64, segmentId int64) (segment.SegmentProcessor, error) {
	l.Lock()
	defer l.Unlock()
	if processors, ok := l.segmentProcessors[logId]; ok {
		for _, processor := range processors {
			if processor.GetSegmentId() == segmentId {
				return processor, nil
			}
		}
	}
	s := segment.NewSegmentProcessor(ctx, l.cfg, logId, segmentId, l.etcdCli, l.minioCli)
	l.segmentProcessors[logId] = append(l.segmentProcessors[logId], s)
	return s, nil
}

func (l *LogStore) GetEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) (*segment.SegmentEntry, error) {
	start := time.Now()
	metrics.WpReadEntriesGauge.WithLabelValues(fmt.Sprintf("%d", logId)).Inc()
	metrics.WpReadRequestsGauge.WithLabelValues(fmt.Sprintf("%d", logId)).Inc()
	defer func() {
		metrics.WpReadEntriesGauge.WithLabelValues(fmt.Sprintf("%d", logId)).Dec()
		metrics.WpReadRequestsGauge.WithLabelValues(fmt.Sprintf("%d", logId)).Dec()
	}()

	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, segmentId)
	if err != nil {
		return nil, err
	}
	entry, err := segmentProcessor.ReadEntry(ctx, entryId)
	if err != nil {
		return nil, err
	}
	cost := time.Now().Sub(start)
	// record success request latency
	metrics.WpReadReqLatency.WithLabelValues(fmt.Sprintf("%d", logId)).Observe(float64(cost.Milliseconds()))
	metrics.WpReadBytes.WithLabelValues(fmt.Sprintf("%d", logId)).Observe(float64(len(entry.Data)))
	return entry, nil
}

func (l *LogStore) FenceSegment(ctx context.Context, logId int64, segmentId int64) error {
	if processors, ok := l.segmentProcessors[logId]; ok {
		for _, processor := range processors {
			if processor.GetSegmentId() == segmentId {
				processor.SetFenced()
			}
		}
	}
	return nil
}

// CompactSegment merge all files in a segment into one file
func (l *LogStore) CompactSegment(ctx context.Context, logId int64, segmentId int64) error {
	// TODO compact segment
	return nil
}
