package segment

import (
	"context"
	"fmt"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
	"log"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/server/storage"
)

// SegmentProcessor for segment processing in server side
type SegmentProcessor interface {
	GetLogId() int64
	GetSegmentId() int64
	AddEntry(context.Context, *SegmentEntry) (int64, <-chan int64, error)
	ReadEntry(context.Context, int64) (*SegmentEntry, error)
	IsFenced() bool
	SetFenced()
}

func NewSegmentProcessor(ctx context.Context, logId int64, segId int64, etcdCli *clientv3.Client, minioCli *minio.Client) SegmentProcessor {
	ctime := time.Now().UnixMilli()
	log.Printf("%d new segment processor with logId: %d, segId: %d", ctime, logId, segId)
	return &segmentProcessor{
		logId:       logId,
		segId:       segId,
		etcdCli:     etcdCli,
		minioClient: minioCli,
		fenced:      false,
		createTime:  ctime,
	}
}

var _ SegmentProcessor = (*segmentProcessor)(nil)

type segmentProcessor struct {
	sync.RWMutex
	logId       int64
	segId       int64
	etcdCli     *clientv3.Client
	minioClient *minio.Client

	currentLogFileId     int64
	currentLogFileWriter storage.LogFile
	fenced               bool

	createTime int64
}

func (s *segmentProcessor) GetLogId() int64 {
	return s.logId
}

func (s *segmentProcessor) GetSegmentId() int64 {
	return s.segId
}

func (s *segmentProcessor) IsFenced() bool {
	return s.fenced
}

func (s *segmentProcessor) SetFenced() {
	s.fenced = true
}

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry) (int64, <-chan int64, error) {
	// 1. add to commitLog, which stores WAL for this node
	// TODO Get wal for this logId, and write entry to it. (zero disk mode skip this step)

	// 2. add to logfile, which stores entry data
	// Get logfile for this logId, and write entry to it, then return the entryId if success
	logFileWriter, err := s.getOrCreateLogFileWriter(ctx)
	if err != nil {
		return -1, nil, err
	}

	//err = logFileWriter.Append(ctx, entry.Data)
	//if err != nil {
	//	return -1, err
	//}
	bufferedSeqNo, syncedCh, _ := logFileWriter.AppendAsync(ctx, entry.EntryId, entry.Data)
	if bufferedSeqNo == -1 {
		return -1, syncedCh, fmt.Errorf("failed to append to log file")
	}

	//// wait for log file to be synced
	//syncedSeqNo := <-syncedCh
	//if syncedSeqNo == -1 {
	//	return -1, fmt.Errorf("failed to append to log file")
	//}
	//
	//if syncedSeqNo != bufferedSeqNo {
	//	return -1, fmt.Errorf("failed to append to log file")
	//}

	// 3. add to EntryBuffer
	// TODO add entry to EntryBuffer, trigger async flush if necessary

	// 4. return syncedSeqNo, chan
	return bufferedSeqNo, syncedCh, nil
}

func (s *segmentProcessor) ReadEntry(ctx context.Context, entryId int64) (*SegmentEntry, error) {
	logFileReader, err := s.getOrCreateLogFileReader(ctx, entryId)
	if err != nil {
		return nil, err
	}
	r, err := logFileReader.NewReader(ctx, storage.ReaderOpt{
		StartSequenceNum: entryId,
		EndSequenceNum:   entryId + 1,
	})
	if err != nil {
		return nil, err
	}

	if !r.HasNext() {
		return nil, werr.ErrEntryNotFound
	}

	e, err := r.ReadNext()
	if err != nil {
		return nil, err
	}

	return &SegmentEntry{
		SegmentId: s.segId,
		EntryId:   e.EntryId,
		Data:      e.Values,
	}, nil
}

func (s *segmentProcessor) getOrCreateLogFileWriter(ctx context.Context) (storage.LogFile, error) {
	s.Lock()
	defer s.Unlock()
	if s.currentLogFileWriter == nil {
		// get logfile id from meta/storage
		s.currentLogFileId = 0
		s.currentLogFileWriter = objectstorage.NewLogFile(s.currentLogFileId, s.getSegmentKeyPrefix(), s.getInstanceBucket(), s.minioClient)
		log.Printf("createLogFileWriter with logId: %d, segId: %d", s.logId, s.segId)
	}
	return s.currentLogFileWriter, nil
}

func (s *segmentProcessor) getOrCreateLogFileReader(ctx context.Context, entryId int64) (storage.LogFile, error) {
	s.Lock()
	defer s.Unlock()
	// TODO get logFile Id according entryId
	currentLogFileId := int64(0)
	currentLogFileReader := objectstorage.NewROLogFile(currentLogFileId, s.getSegmentKeyPrefix(), s.getInstanceBucket(), s.minioClient)
	return currentLogFileReader, nil
}

// TODO move to common package for config
func (s *segmentProcessor) getInstanceBucket() string {
	return "woodpecker"
}

func (s *segmentProcessor) getSegmentKeyPrefix() string {
	return fmt.Sprintf("%d/%d", s.logId, s.segId)
}
