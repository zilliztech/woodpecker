package segment

import (
	"context"
	"errors"
	"fmt"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

// SegmentProcessor for segment processing in server side
//
//go:generate mockery --dir=./server/segment --name=SegmentProcessor --structname=SegmentProcessor --output=mocks/mocks_server/mocks_segment --filename=mock_segment_processor.go --with-expecter=true  --outpkg=mocks_segment
type SegmentProcessor interface {
	GetLogId() int64
	GetSegmentId() int64
	AddEntry(context.Context, *SegmentEntry) (int64, <-chan int64, error)
	ReadEntry(context.Context, int64) (*SegmentEntry, error)
	IsFenced() bool
	SetFenced()
	Compact(ctx context.Context) (*proto.SegmentMetadata, error)
	Recover(ctx context.Context) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(ctx context.Context) (int64, error)
}

func NewSegmentProcessor(ctx context.Context, cfg *config.Configuration, logId int64, segId int64, minioCli minioHandler.MinioHandler) SegmentProcessor {
	ctime := time.Now().UnixMilli()
	logger.Ctx(ctx).Debug("new segment processor created", zap.Int64("ctime", ctime), zap.Int64("logId", logId), zap.Int64("segId", segId))
	return &segmentProcessor{
		cfg:         cfg,
		logId:       logId,
		segId:       segId,
		minioClient: minioCli,
		createTime:  ctime,
	}
}

// NewSegmentProcessorWithLogFile TODO Test Only
func NewSegmentProcessorWithLogFile(ctx context.Context, cfg *config.Configuration, logId int64, segId int64, minioCli minioHandler.MinioHandler, currentLogFile storage.LogFile) SegmentProcessor {
	return &segmentProcessor{
		cfg:                  cfg,
		logId:                logId,
		segId:                segId,
		minioClient:          minioCli,
		createTime:           time.Now().UnixMilli(),
		currentLogFileWriter: currentLogFile,
		currentLogFileReader: currentLogFile,
	}
}

var _ SegmentProcessor = (*segmentProcessor)(nil)

type segmentProcessor struct {
	sync.RWMutex
	cfg         *config.Configuration
	logId       int64
	segId       int64
	minioClient minioHandler.MinioHandler

	createTime int64

	// for logFile writer
	currentLogFileId     int64
	currentLogFileWriter storage.LogFile
	fenced               atomic.Bool

	// for logFile reader
	currentLogFileReader storage.LogFile
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
	logger.Ctx(ctx).Debug("segment processor add entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entry.EntryId), zap.String("segmentProcessorInstance", fmt.Sprintf("%p", s)))
	if s.IsFenced() {
		return -1, nil, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("append entry:%d failed, log:%d segment:%d is fenced", entry.EntryId, s.logId, s.segId))
	}

	logFileWriter, err := s.getOrCreateLogFileWriter(ctx)
	if err != nil {
		return -1, nil, err
	}

	bufferedSeqNo, syncedCh, err := logFileWriter.AppendAsync(ctx, entry.EntryId, entry.Data)
	if bufferedSeqNo == -1 {
		return -1, syncedCh, fmt.Errorf("failed to append to log file")
	} else if err != nil {
		return -1, syncedCh, err
	}

	return bufferedSeqNo, syncedCh, nil
}

func (s *segmentProcessor) ReadEntry(ctx context.Context, entryId int64) (*SegmentEntry, error) {
	// TODO should cache reader for each open reader
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

func (s *segmentProcessor) GetSegmentLastAddConfirmed(ctx context.Context) (int64, error) {
	logFileReader, err := s.getOrCreateLogFileReader(ctx, -1)
	if err != nil {
		return -1, err
	}
	return logFileReader.GetLastEntryId()
}

func (s *segmentProcessor) getOrCreateLogFileWriter(ctx context.Context) (storage.LogFile, error) {
	s.Lock()
	defer s.Unlock()
	if s.currentLogFileWriter == nil {
		// get logfile id from meta/storage
		// Currently, simplified support for one logical LogFile per Segment
		s.currentLogFileId = 0
		if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
			// use local FileSystem or local FileSystem + minio-compatible
			writerFile, err := disk.NewDiskLogFile(s.currentLogFileId, path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()))
			s.currentLogFileWriter = writerFile
			logger.Ctx(ctx).Info("create DiskLogFile for write", zap.Int64("logFileId", s.currentLogFileId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()))
			return s.currentLogFileWriter, err
		} else {
			// use MinIO-compatible storage
			s.currentLogFileWriter = objectstorage.NewLogFile(
				s.currentLogFileId,
				s.getSegmentKeyPrefix(),
				s.getInstanceBucket(),
				s.minioClient,
				s.cfg)
		}
	}
	return s.currentLogFileWriter, nil
}

func (s *segmentProcessor) getOrCreateLogFileReader(ctx context.Context, entryId int64) (storage.LogFile, error) {
	s.Lock()
	defer s.Unlock()
	if s.currentLogFileReader == nil {
		// get logfile id from meta/storage
		// Currently, simplified support for one LogFile per Segment
		roLogFileId := int64(0)
		if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
			// use local FileSystem or local FileSystem + minio-compatible
			writerFile, err := disk.NewDiskLogFile(s.currentLogFileId, path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()), disk.WithDisableAutoSync())
			s.currentLogFileWriter = writerFile
			logger.Ctx(ctx).Info("create DiskLogFile for read", zap.Int64("logFileId", s.currentLogFileId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()))
			return s.currentLogFileWriter, err
		} else {
			s.currentLogFileReader = objectstorage.NewROLogFile(
				roLogFileId,
				s.getSegmentKeyPrefix(),
				s.getInstanceBucket(),
				s.minioClient)
		}
	}
	return s.currentLogFileReader, nil
}

func (s *segmentProcessor) getInstanceBucket() string {
	return s.cfg.Minio.BucketName
}

func (s *segmentProcessor) getSegmentKeyPrefix() string {
	return fmt.Sprintf("%s/%d/%d", s.cfg.Woodpecker.Meta.Prefix, s.logId, s.segId)
}

func (s *segmentProcessor) Compact(ctx context.Context) (*proto.SegmentMetadata, error) {
	logFile, err := s.getOrCreateLogFileReader(ctx, 0)
	if err != nil {
		return nil, err
	}
	mergedFrags, entryOffset, fragsOffset, mergedErr := logFile.Merge(ctx)
	if mergedErr != nil {
		return nil, mergedErr
	}
	if len(mergedFrags) == 0 {
		return nil, errors.New("no frags to merge")
	}
	lastMergedFrag := mergedFrags[len(mergedFrags)-1]
	totalSize := lastMergedFrag.GetSize()
	lastEntryIdOfAllMergedFrags, err := lastMergedFrag.GetLastEntryId()
	if err != nil {
		return nil, err
	}
	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Sealed,
		CompletionTime: lastMergedFrag.GetLastModified(),
		SealedTime:     time.Now().UnixMilli(),
		LastEntryId:    lastEntryIdOfAllMergedFrags,
		Size:           totalSize,
		EntryOffset:    entryOffset,
		FragmentOffset: fragsOffset,
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
	lastEntryId, err := lastFragment.GetLastEntryId()
	if err != nil {
		return nil, err
	}
	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Completed,
		CompletionTime: lastFragment.GetLastModified(),
		LastEntryId:    lastEntryId,
		Size:           size,
	}, nil
}
