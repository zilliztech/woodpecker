package segment

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
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
	Clean(ctx context.Context, flag int) error
}

func NewSegmentProcessor(ctx context.Context, cfg *config.Configuration, logId int64, segId int64, minioCli minioHandler.MinioHandler) SegmentProcessor {
	ctime := time.Now().UnixMilli()
	logger.Ctx(ctx).Debug("new segment processor created", zap.Int64("ctime", ctime), zap.Int64("logId", logId), zap.Int64("segId", segId))
	s := &segmentProcessor{
		cfg:         cfg,
		logId:       logId,
		segId:       segId,
		minioClient: minioCli,
		createTime:  ctime,
	}
	s.fenced.Store(false)
	return s
}

// NewSegmentProcessorWithLogFile TODO Test Only
func NewSegmentProcessorWithLogFile(ctx context.Context, cfg *config.Configuration, logId int64, segId int64, minioCli minioHandler.MinioHandler, currentLogFile storage.LogFile) SegmentProcessor {
	s := &segmentProcessor{
		cfg:                  cfg,
		logId:                logId,
		segId:                segId,
		minioClient:          minioCli,
		createTime:           time.Now().UnixMilli(),
		currentLogFileWriter: currentLogFile,
		currentLogFileReader: currentLogFile,
	}
	s.fenced.Store(false)
	return s
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
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	s.fenced.Store(true)
	if s.currentLogFileWriter != nil {
		closeLogFileWriterErr := s.currentLogFileWriter.Close()
		if closeLogFileWriterErr != nil {
			logger.Ctx(context.TODO()).Error("close log file writer failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("logFileId", s.currentLogFileId), zap.Error(closeLogFileWriterErr))
			metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "set_fenced", "close_error").Inc()
			metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "set_fenced", "close_error").Observe(float64(time.Since(start).Milliseconds()))
		} else {
			metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "set_fenced", "success").Inc()
			metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "set_fenced", "success").Observe(float64(time.Since(start).Milliseconds()))
		}
	} else {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "set_fenced", "success").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "set_fenced", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
}

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry) (int64, <-chan int64, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logger.Ctx(ctx).Debug("segment processor add entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entry.EntryId), zap.String("segmentProcessorInstance", fmt.Sprintf("%p", s)))
	if s.IsFenced() {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "fenced").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "fenced").Observe(float64(time.Since(start).Milliseconds()))
		return -1, nil, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("append entry:%d failed, log:%d segment:%d is fenced", entry.EntryId, s.logId, s.segId))
	}

	logFileWriter, err := s.getOrCreateLogFileWriter(ctx)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "writer_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "writer_error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, nil, err
	}

	bufferedSeqNo, syncedCh, err := logFileWriter.AppendAsync(ctx, entry.EntryId, entry.Data)
	if bufferedSeqNo == -1 {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "append_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "append_error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, syncedCh, fmt.Errorf("failed to append to log file")
	} else if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, syncedCh, err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return bufferedSeqNo, syncedCh, nil
}

func (s *segmentProcessor) ReadEntry(ctx context.Context, entryId int64) (*SegmentEntry, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logger.Ctx(ctx).Debug("segment processor read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId))
	logFileReader, err := s.getOrCreateLogFileReader(ctx, entryId)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	// TODO should cache reader for each open reader
	r, err := logFileReader.NewReader(ctx, storage.ReaderOpt{
		StartSequenceNum: entryId,
		EndSequenceNum:   entryId + 1,
	})
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "new_reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "new_reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	hasNext, err := r.HasNext()
	if err != nil {
		logger.Ctx(ctx).Warn("failed to check has next", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId), zap.Error(err))
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "has_next_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "has_next_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	if !hasNext {
		logger.Ctx(ctx).Debug("no entry found", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId))
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "not_found").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "not_found").Observe(float64(time.Since(start).Milliseconds()))
		return nil, werr.ErrEntryNotFound
	}

	e, err := r.ReadNext()
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId), zap.Error(err))
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "read_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "read_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return &SegmentEntry{
		SegmentId: s.segId,
		EntryId:   e.EntryId,
		Data:      e.Values,
	}, nil
}

func (s *segmentProcessor) GetSegmentLastAddConfirmed(ctx context.Context) (int64, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logFileReader, err := s.getOrCreateLogFileReader(ctx, -1)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}

	lastEntryId, err := logFileReader.GetLastEntryId()
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "success").Observe(float64(time.Since(start).Milliseconds()))
	return lastEntryId, nil
}

func (s *segmentProcessor) getOrCreateLogFileWriter(ctx context.Context) (storage.LogFile, error) {
	// First check with read lock to avoid data race
	s.RLock()
	if s.currentLogFileWriter != nil {
		writer := s.currentLogFileWriter
		s.RUnlock()
		return writer, nil
	}
	s.RUnlock()

	// Need to initialize, acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check after acquiring lock
	if s.currentLogFileWriter != nil {
		return s.currentLogFileWriter, nil
	}

	// Initialize writer
	// get logfile id from meta/storage
	// Currently, simplified support for one logical LogFile per Segment
	s.currentLogFileId = 0

	if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
		// use local FileSystem or local FileSystem + minio-compatible
		writerFile, err := disk.NewDiskLogFile(
			s.logId,
			s.segId,
			s.currentLogFileId,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()),
			disk.WithWriteFragmentSize(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxBytes),
			disk.WithWriteMaxBufferSize(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxFlushSize),
			disk.WithWriteMaxEntryPerFile(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxEntries),
			disk.WithWriteMaxIntervalMs(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxInterval))
		s.currentLogFileWriter = writerFile
		logger.Ctx(ctx).Info("create DiskLogFile for write", zap.Int64("logFileId", s.currentLogFileId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.String("logFileInst", fmt.Sprintf("%p", writerFile)))
		return s.currentLogFileWriter, err
	} else {
		// use MinIO-compatible storage
		s.currentLogFileWriter = objectstorage.NewLogFile(
			s.logId,
			s.segId,
			s.currentLogFileId,
			s.getSegmentKeyPrefix(),
			s.getInstanceBucket(),
			s.minioClient,
			s.cfg)
		logger.Ctx(ctx).Info("create LogFile for write", zap.Int64("logFileId", s.currentLogFileId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.String("logFileInst", fmt.Sprintf("%p", s.currentLogFileWriter)))
	}
	return s.currentLogFileWriter, nil
}

func (s *segmentProcessor) getOrCreateLogFileReader(ctx context.Context, entryId int64) (storage.LogFile, error) {
	// First check with read lock to avoid data race
	s.RLock()
	if s.currentLogFileReader != nil {
		reader := s.currentLogFileReader
		s.RUnlock()
		return reader, nil
	}
	s.RUnlock()

	// Need to initialize, acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check after acquiring lock
	if s.currentLogFileReader != nil {
		return s.currentLogFileReader, nil
	}

	// Initialize reader
	// get logfile id from meta/storage
	// Currently, simplified support for one LogFile per Segment
	s.currentLogFileId = 0

	if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
		// use local FileSystem or local FileSystem + minio-compatible
		readerFile, err := disk.NewRODiskLogFile(
			s.logId,
			s.segId,
			s.currentLogFileId,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()))
		s.currentLogFileReader = readerFile
		logger.Ctx(ctx).Info("create DiskLogFile for read", zap.Int64("logFileId", s.currentLogFileId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.Int64("entryId", entryId), zap.String("logFileInst", fmt.Sprintf("%p", readerFile)))
		return s.currentLogFileReader, err
	} else {
		s.currentLogFileReader = objectstorage.NewROLogFile(
			s.logId,
			s.segId,
			s.currentLogFileId,
			s.getSegmentKeyPrefix(),
			s.getInstanceBucket(),
			s.minioClient)
		logger.Ctx(ctx).Info("create LogFile for read", zap.Int64("logFileId", s.currentLogFileId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.String("logFileInst", fmt.Sprintf("%p", s.currentLogFileReader)))
	}
	return s.currentLogFileReader, nil
}

func (s *segmentProcessor) getInstanceBucket() string {
	return s.cfg.Minio.BucketName
}

func (s *segmentProcessor) getSegmentKeyPrefix() string {
	return fmt.Sprintf("%s/%d/%d", s.cfg.Minio.RootPath, s.logId, s.segId)
}

func (s *segmentProcessor) Compact(ctx context.Context) (*proto.SegmentMetadata, error) {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logFile, err := s.getOrCreateLogFileReader(ctx, 0)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	mergedFrags, entryOffset, fragsOffset, mergedErr := logFile.Merge(ctx)
	if mergedErr != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "merge_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "merge_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, mergedErr
	}

	if len(mergedFrags) == 0 {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "no_frags").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "no_frags").Observe(float64(time.Since(start).Milliseconds()))
		return nil, errors.New("no frags to merge")
	}

	lastMergedFrag := mergedFrags[len(mergedFrags)-1]
	totalSize := lastMergedFrag.GetSize()
	lastEntryIdOfAllMergedFrags, err := lastMergedFrag.GetLastEntryId()
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "get_last_entry_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "get_last_entry_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	logger.Ctx(ctx).Debug("Compact segment merge completed",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("mergedFrags", len(mergedFrags)))

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "success").Observe(float64(time.Since(start).Milliseconds()))
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
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logFile, err := s.getOrCreateLogFileReader(ctx, 0)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recover", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "recover", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	size, lastFragment, err := logFile.Load(ctx)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recover", "load_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "recover", "load_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	if lastFragment == nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recover", "no_fragments").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "recover", "no_fragments").Observe(float64(time.Since(start).Milliseconds()))
		return &proto.SegmentMetadata{
			State:          proto.SegmentState_Completed,
			CompletionTime: time.Now().UnixMilli(),
			LastEntryId:    -1,
			Size:           size,
		}, nil
	}

	lastEntryId, err := lastFragment.GetLastEntryId()
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recover", "get_last_entry_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "recover", "get_last_entry_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	logger.Ctx(ctx).Debug("recover segment load completed",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int64("lastEntryId", lastEntryId),
		zap.String("lastFrag", lastFragment.GetFragmentKey()))

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recover", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "recover", "success").Observe(float64(time.Since(start).Milliseconds()))
	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Completed,
		CompletionTime: lastFragment.GetLastModified(),
		LastEntryId:    lastEntryId,
		Size:           size,
	}, nil
}

func (s *segmentProcessor) Clean(ctx context.Context, flag int) error {
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logFile, err := s.getOrCreateLogFileReader(ctx, 0) // TODO use a writer or special instance for maintain
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	err = logFile.DeleteFragments(ctx, flag)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean", "delete_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean", "delete_error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}
