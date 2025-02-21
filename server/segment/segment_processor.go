package segment

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

// SegmentProcessor for segment processing in server side
type SegmentProcessor interface {
	GetLogId() int64
	GetSegmentId() int64
	AddEntry(context.Context, *SegmentEntry) (int64, <-chan int64, error)
	ReadEntry(context.Context, int64) (*SegmentEntry, error)
	IsFenced() bool
	SetFenced()
	Compact(ctx context.Context) (*proto.SegmentMetadata, error)
	Recover(ctx context.Context) (*proto.SegmentMetadata, error)
}

func NewSegmentProcessor(ctx context.Context, cfg *config.Configuration, logId int64, segId int64, etcdCli *clientv3.Client, minioCli *minio.Client) SegmentProcessor {
	ctime := time.Now().UnixMilli()
	logger.Ctx(ctx).Debug("new segment processor created", zap.Int64("ctime", ctime), zap.Int64("logId", logId), zap.Int64("segId", segId))
	return &segmentProcessor{
		cfg:         cfg,
		logId:       logId,
		segId:       segId,
		etcdCli:     etcdCli,
		minioClient: minioCli,
		createTime:  ctime,
	}
}

var _ SegmentProcessor = (*segmentProcessor)(nil)

type segmentProcessor struct {
	sync.RWMutex
	cfg         *config.Configuration
	logId       int64
	segId       int64
	etcdCli     *clientv3.Client
	minioClient *minio.Client

	currentLogFileId     int64
	currentLogFileWriter storage.LogFile
	fenced               atomic.Bool

	createTime int64
}

func (s *segmentProcessor) GetLogId() int64 {
	return s.logId
}

func (s *segmentProcessor) GetSegmentId() int64 {
	return s.segId
}

func (s *segmentProcessor) IsFenced() bool {
	return s.fenced.Load()
}

func (s *segmentProcessor) SetFenced() {
	s.fenced.Store(true)
	if s.currentLogFileWriter != nil {
		closeLogFileWriterErr := s.currentLogFileWriter.Close()
		if closeLogFileWriterErr != nil {
			logger.Ctx(context.TODO()).Error("close log file writer failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("logFileId", s.currentLogFileId), zap.Error(closeLogFileWriterErr))
		}
	}
}

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry) (int64, <-chan int64, error) {
	if s.IsFenced() {
		return -1, nil, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("append entry:%d failed, log:%d segment:%d is fenced", entry.EntryId, s.logId, s.segId))
	}
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
	bufferedSeqNo, syncedCh, err := logFileWriter.AppendAsync(ctx, entry.EntryId, entry.Data)
	if bufferedSeqNo == -1 {
		return -1, syncedCh, fmt.Errorf("failed to append to log file")
	} else if err != nil {
		return -1, syncedCh, err
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
		s.currentLogFileId = 0 // Currently, simplified support for one LogFile per Segment
		s.currentLogFileWriter = objectstorage.NewLogFile(
			s.currentLogFileId,
			s.getSegmentKeyPrefix(),
			s.getInstanceBucket(),
			s.minioClient,
			s.cfg)
	}
	return s.currentLogFileWriter, nil
}

func (s *segmentProcessor) getOrCreateLogFileReader(ctx context.Context, entryId int64) (storage.LogFile, error) {
	s.Lock()
	defer s.Unlock()
	// TODO get logFile Id according entryId
	currentLogFileId := int64(0)
	currentLogFileReader := objectstorage.NewROLogFile(
		currentLogFileId,
		s.getSegmentKeyPrefix(),
		s.getInstanceBucket(),
		s.minioClient)
	return currentLogFileReader, nil
}

func (s *segmentProcessor) getInstanceBucket() string {
	return s.cfg.Minio.BucketName
}

func (s *segmentProcessor) getSegmentKeyPrefix() string {
	return fmt.Sprintf("woodpecker/%d/%d", s.logId, s.segId)
}

func (s *segmentProcessor) Compact(ctx context.Context) (*proto.SegmentMetadata, error) {
	logFile, err := s.getOrCreateLogFileReader(ctx, 0)
	if err != nil {
		return nil, err
	}
	mergedFrags, mergedErr := logFile.Merge(ctx)
	if mergedErr != nil {
		return nil, mergedErr
	}
	lastMergedFrag := mergedFrags[len(mergedFrags)-1]
	totalSize := int64(0)
	mergedFragFirstEntryIds := make([]int32, 0)
	for _, frag := range mergedFrags {
		firstEntryId := frag.GetFirstEntryIdDirectly()
		mergedFragFirstEntryIds = append(mergedFragFirstEntryIds, int32(firstEntryId))
	}

	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Sealed,
		CompletionTime: lastMergedFrag.GetLastModified(),
		SealedTime:     time.Now().UnixMilli(),
		LastEntryId:    lastMergedFrag.GetLastEntryIdDirectly(),
		Size:           totalSize,
		Offset:         mergedFragFirstEntryIds,
	}, nil
}

func (s *segmentProcessor) Recover(ctx context.Context) (*proto.SegmentMetadata, error) {
	logFile, err := s.getOrCreateLogFileReader(ctx, 0)
	if err != nil {
		return nil, err
	}
	size, lastFragment, err := logFile.Load(ctx)
	if err != nil {
		return nil, err
	}
	if lastFragment == nil {
		return &proto.SegmentMetadata{
			State:          proto.SegmentState_Completed,
			CompletionTime: time.Now().UnixMilli(),
			LastEntryId:    -1,
			Size:           size,
		}, nil
	}

	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Completed,
		CompletionTime: lastFragment.GetLastModified(),
		LastEntryId:    lastFragment.GetLastEntryIdDirectly(),
		Size:           size,
	}, nil
}
