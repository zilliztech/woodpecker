package server

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/woodpecker/server/segment"
)

type LogStore struct {
	ctx      context.Context
	cancel   context.CancelFunc
	etcdCli  *clientv3.Client
	minioCli *minio.Client
	address  string

	segmentProcessors map[int64][]segment.SegmentProcessor
}

func NewLogStore(ctx context.Context, etcdCli *clientv3.Client, minioCli *minio.Client) *LogStore {
	ctx, cancel := context.WithCancel(ctx)
	return &LogStore{
		ctx:               ctx,
		cancel:            cancel,
		etcdCli:           etcdCli,
		minioCli:          minioCli,
		segmentProcessors: make(map[int64][]segment.SegmentProcessor),
	}
}

func (l *LogStore) Start() error {
	// TODO start service
	//

	// register to etcd and keep alive
	registerErr := l.Register(context.Background())
	return registerErr
}
func (l *LogStore) Stop() error {
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

func (l *LogStore) AddEntry(ctx context.Context, logId int64, entry *segment.SegmentEntry) (int64, error) {
	// 1. add to commitLog, which stores WAL for this node
	// TODO Get wal for this logId, and write entry to it. (zero disk mode skip this step)

	// 2. add to logfile, which stores entry data
	// Get logfile for this logId, and write entry to it, then return the entryId if success
	segmentProcessor, err := l.getOrCreateSegmentProcessor(ctx, logId, entry.SegmentId)
	if err != nil {
		return -1, err
	}
	if segmentProcessor.IsFenced() {
		return -1, errors.New(fmt.Sprintf("log:%d segment:%d is fenced", logId, entry.SegmentId))
	}
	entryId, err := segmentProcessor.AddEntry(ctx, entry)
	if err != nil {
		return -1, err
	}

	// 3. add to EntryBuffer
	// TODO add entry to EntryBuffer, trigger async flush if necessary

	log.Printf("LogStore addEntry call, log:%d, entry: %v", logId, entry)
	return entryId, nil
}

func (l *LogStore) getOrCreateSegmentProcessor(ctx context.Context, logId int64, segmentId int64) (segment.SegmentProcessor, error) {
	if processors, ok := l.segmentProcessors[logId]; ok {
		for _, processor := range processors {
			if processor.GetSegmentId() == segmentId {
				return processor, nil
			}
		}
	}
	s := segment.NewSegmentProcessor(ctx, logId, segmentId, l.etcdCli, l.minioCli)
	l.segmentProcessors[logId] = append(l.segmentProcessors[logId], s)
	return s, nil
}

func (l *LogStore) GetEntry(ctx context.Context, logId int64, segmentId int64, entryId int64) ([]byte, error) {
	return nil, nil
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
