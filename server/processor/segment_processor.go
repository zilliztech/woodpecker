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

package processor

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

const (
	ProcessorScopeName = "SegmentProcessor"
)

// SegmentProcessor for segment processing in server side
//
//go:generate mockery --dir=./server/processor --name=SegmentProcessor --structname=SegmentProcessor --output=mocks/mocks_server/mocks_segment --filename=mock_segment_processor.go --with-expecter=true  --outpkg=mocks_segment
type SegmentProcessor interface {
	GetLogId() int64
	GetSegmentId() int64
	AddEntry(context.Context, *SegmentEntry, channel.ResultChannel) (int64, error)
	ReadBatchEntriesAdv(context.Context, int64, int64, *LastReadState) (*BatchData, error)
	Fence(ctx context.Context) (int64, error)
	Complete(ctx context.Context) (int64, error)
	Compact(ctx context.Context) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(ctx context.Context) (int64, error)
	GetBlocksCount(ctx context.Context) (int64, error)
	GetLastAccessTime() int64
	Clean(ctx context.Context, flag int) error
	Close(ctx context.Context) error
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
	s.lastAccessTime.Store(ctime)
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

	createTime     int64
	lastAccessTime atomic.Int64

	// for segment Impl
	currentSegmentImpl   storage.Segment
	currentSegmentWriter storage.Writer
	fenced               atomic.Bool // For fence state: true confirms it is fenced, while false requires verification by checking the storage backend for a fence flag file/object.
}

func (s *segmentProcessor) GetLogId() int64 {
	return s.logId
}

func (s *segmentProcessor) GetSegmentId() int64 {
	return s.segId
}

func (s *segmentProcessor) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Fenced")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor fence operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	// open a writer in recover mode if necessary
	writer, err := s.getOrCreateSegmentWriter(ctx, true)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segment reader for recovery",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Error(err))
		return -1, err
	}

	// fence
	_, fenceErr := writer.Fence(ctx)
	if fenceErr != nil {
		logger.Ctx(ctx).Warn("Failed to fence segment",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Error(fenceErr))
		return -1, fenceErr
	}

	// finalize
	lastEntryID, finalizeErr := writer.Finalize(ctx)
	if finalizeErr != nil {
		logger.Ctx(ctx).Warn("Failed to finalize segment when fencing",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Error(finalizeErr))
		return -1, finalizeErr
	}

	s.fenced.CompareAndSwap(false, true)
	logger.Ctx(ctx).Info("Segment processor fence operation completed successfully",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int64("lastEntryID", lastEntryID),
		zap.Duration("duration", time.Since(start)))
	return lastEntryID, nil
}

func (s *segmentProcessor) Complete(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Complete")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("segment processor call complete", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("segmentProcessorInstance", fmt.Sprintf("%p", s)))

	if s.fenced.Load() {
		// fast return if fenced
		return -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("log:%d segment:%d is fenced", s.logId, s.segId))
	}

	writer, err := s.getSegmentWriter(ctx)
	if err != nil {
		return -1, err
	}
	return writer.Finalize(ctx)
}

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "AddEntry")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("segment processor add entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entry.EntryId), zap.String("ch", fmt.Sprintf("%p", resultCh)), zap.String("inst", fmt.Sprintf("%p", s)))

	if s.fenced.Load() {
		// fast return if fenced
		return -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("append entry:%d failed, log:%d segment:%d is fenced", entry.EntryId, s.logId, s.segId))
	}

	writer, err := s.getOrCreateSegmentWriter(ctx, false)
	if err != nil {
		return -1, err
	}

	bufferedSeqNo, err := writer.WriteDataAsync(ctx, entry.EntryId, entry.Data, resultCh)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to append to log file", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(err))
		return -1, err
	} else if bufferedSeqNo == -1 {
		logger.Ctx(ctx).Warn("failed to append to log file", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId))
		return -1, fmt.Errorf("failed to append to log file")
	}

	return bufferedSeqNo, nil
}

func (s *segmentProcessor) ReadBatchEntriesAdv(ctx context.Context, fromEntryId int64, maxSize int64, lastReadState *LastReadState) (*BatchData, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "ReadBatchEntries")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("segment processor read batch entries", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("maxSize", maxSize))
	reader, err := s.getNewSegmentReaderAdv(ctx, lastReadState)
	if err != nil {
		return nil, err
	}
	defer reader.Close(ctx)

	// read batch entries
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: fromEntryId,
		EndEntryID:   0, // means no stop point
		BatchSize:    maxSize,
	})
	if err != nil {
		if werr.ErrEntryNotFound.Is(err) {
			logger.Ctx(ctx).Debug("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		} else if werr.ErrFileReaderEndOfFile.Is(err) {
			logger.Ctx(ctx).Info("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		} else {
			logger.Ctx(ctx).Warn("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		}
		return nil, err
	}
	// batch result
	result := make([]*SegmentEntry, 0, len(batch.Entries))
	for _, entry := range batch.Entries {
		segmentEntry := &SegmentEntry{
			SegmentId: s.segId,
			EntryId:   entry.EntryId,
			Data:      entry.Values,
		}
		result = append(result, segmentEntry)
	}

	batchData := &BatchData{
		Entries: result,
		ReadState: &LastReadState{
			SegmentId:   s.segId,
			Flags:       batch.LastBatchInfo.Flags,
			Version:     batch.LastBatchInfo.Version,
			LastBlockID: batch.LastBatchInfo.LastBlockInfo.BlockNumber,
			BlockOffset: batch.LastBatchInfo.LastBlockInfo.StartOffset,
			BlockSize:   batch.LastBatchInfo.LastBlockInfo.BlockSize,
		},
	}
	// return
	return batchData, nil
}

func (s *segmentProcessor) GetSegmentLastAddConfirmed(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	s.updateAccessTime()
	readerImpl, err := s.getNewSegmentReaderAdv(ctx, nil)
	if err != nil {
		return -1, err
	}
	defer readerImpl.Close(ctx)

	lastEntryId, err := readerImpl.GetLastEntryID(ctx)
	if err != nil {
		return -1, err
	}

	return lastEntryId, nil
}

func (s *segmentProcessor) GetBlocksCount(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "GetBlocksCount")
	defer sp.End()
	s.updateAccessTime()

	// get exists file writer
	writer, err := s.getSegmentWriter(ctx)
	if err != nil {
		return -1, err
	}

	return writer.GetBlockCount(ctx), nil
}

func (s *segmentProcessor) getOrCreateSegmentImpl(ctx context.Context) (storage.Segment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "getOrCreateSegmentImpl")
	defer sp.End()
	// First check with read lock to avoid data race
	s.RLock()
	if s.currentSegmentImpl != nil {
		s.RUnlock()
		return s.currentSegmentImpl, nil
	}
	s.RUnlock()

	// Need to initialize, acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check after acquiring lock
	if s.currentSegmentImpl != nil {
		return s.currentSegmentImpl, nil
	}

	// use local FileSystem or local FileSystem + minio-compatible
	if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
		s.currentSegmentImpl = disk.NewDiskSegmentImpl(
			ctx,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getLogBaseDir()),
			s.logId,
			s.segId,
			s.cfg)
		logger.Ctx(ctx).Info("create segment impl for local", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("logBaseDir", s.getLogBaseDir()), zap.String("inst", fmt.Sprintf("%p", s.currentSegmentImpl)))
		return s.currentSegmentImpl, nil
	} else {
		s.currentSegmentImpl = objectstorage.NewSegmentImpl(
			ctx,
			s.getInstanceBucket(),
			s.getLogBaseDir(),
			s.logId,
			s.segId,
			s.minioClient,
			s.cfg)
		logger.Ctx(ctx).Info("create segment impl for object", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("logBaseDir", s.getLogBaseDir()), zap.String("inst", fmt.Sprintf("%p", s.currentSegmentImpl)))
	}
	return s.currentSegmentImpl, nil
}

func (s *segmentProcessor) getNewSegmentReaderAdv(ctx context.Context, lastReadState *LastReadState) (storage.Reader, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "getNewSegmentReader")
	defer sp.End()
	var lastBlockInfo *storage.BatchInfo
	if lastReadState != nil && lastReadState.SegmentId == s.segId {
		lastBlockInfo = &storage.BatchInfo{
			Flags:   lastReadState.Flags,
			Version: lastReadState.Version,
			LastBlockInfo: &codec.IndexRecord{
				BlockNumber: lastReadState.LastBlockID,
				StartOffset: lastReadState.BlockOffset,
				BlockSize:   lastReadState.BlockSize,
			},
		}
	}

	//Initialize reader
	if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
		// use local FileSystem or local FileSystem + minio-compatible
		localReader, err := disk.NewLocalFileReaderAdv(
			ctx,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getLogBaseDir()),
			s.logId,
			s.segId,
			lastBlockInfo,
			s.cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize)
		logger.Ctx(ctx).Info("created segment local reader", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("logBaseDir", s.getLogBaseDir()), zap.String("inst", fmt.Sprintf("%p", localReader)))
		return localReader, err
	} else {
		minioReader, getReaderErr := objectstorage.NewMinioFileReaderAdv(
			ctx,
			s.getInstanceBucket(),
			s.getLogBaseDir(),
			s.logId,
			s.segId,
			s.minioClient,
			lastBlockInfo,
			s.cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize,
			s.cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads)
		if getReaderErr != nil {
			return nil, getReaderErr
		}
		logger.Ctx(ctx).Info("created segment reader", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("logBaseDir", s.getLogBaseDir()), zap.String("inst", fmt.Sprintf("%p", minioReader)))
		return minioReader, nil
	}
}

func (s *segmentProcessor) getSegmentWriter(ctx context.Context) (storage.Writer, error) {
	// First check with read lock to avoid data race
	s.RLock()
	defer s.RUnlock()
	if s.currentSegmentWriter != nil {
		writer := s.currentSegmentWriter
		return writer, nil
	}
	return nil, werr.ErrSegmentProcessorNoWriter.WithCauseErrMsg("current segment writer not exists")
}

func (s *segmentProcessor) getOrCreateSegmentWriter(ctx context.Context, recoverMode bool) (storage.Writer, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "getOrCreateSegmentWriter")
	defer sp.End()
	// First check with read lock to avoid data race
	s.RLock()
	if s.currentSegmentWriter != nil {
		writer := s.currentSegmentWriter
		s.RUnlock()
		return writer, nil
	}
	s.RUnlock()

	// Need to initialize, acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check after acquiring lock
	if s.currentSegmentWriter != nil {
		return s.currentSegmentWriter, nil
	}

	// Initialize writer
	if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
		// use local FileSystem or local FileSystem + minio-compatible
		writerFile, err := disk.NewLocalFileWriterWithMode(
			ctx,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getLogBaseDir()),
			s.logId,
			s.segId,
			s.cfg,
			recoverMode)
		s.currentSegmentWriter = writerFile
		logger.Ctx(ctx).Info("create segment local writer", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("logBaseDir", s.getLogBaseDir()), zap.String("inst", fmt.Sprintf("%p", writerFile)))
		return s.currentSegmentWriter, err
	} else {
		// use MinIO-compatible storage
		w, getWriterErr := objectstorage.NewMinioFileWriterWithMode(
			ctx,
			s.getInstanceBucket(),
			s.getLogBaseDir(),
			s.logId,
			s.segId,
			s.minioClient,
			s.cfg,
			recoverMode)
		if getWriterErr != nil {
			return nil, getWriterErr
		}
		s.currentSegmentWriter = w
		logger.Ctx(ctx).Info("create segment writer", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("logBaseDir", s.getLogBaseDir()), zap.String("inst", fmt.Sprintf("%p", s.currentSegmentWriter)))
	}
	return s.currentSegmentWriter, nil
}

func (s *segmentProcessor) getInstanceBucket() string {
	return s.cfg.Minio.BucketName
}

func (s *segmentProcessor) getLogBaseDir() string {
	return fmt.Sprintf("%s", s.cfg.Minio.RootPath)
}

func (s *segmentProcessor) Compact(ctx context.Context) (*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Compact")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor compact operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	writer, err := s.getOrCreateSegmentWriter(ctx, true)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segment reader for compaction",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Starting segment merge operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	segmentSizeAfterCompact, mergedErr := writer.Compact(ctx)
	if mergedErr != nil {
		logger.Ctx(ctx).Warn("Segment merge operation failed",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Duration("duration", time.Since(start)),
			zap.Error(mergedErr))
		return nil, mergedErr
	}

	compactionDuration := time.Since(start)
	logger.Ctx(ctx).Info("Segment processor compact operation completed successfully",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Duration("totalDuration", compactionDuration),
		zap.Int64("segmentSizeAfterCompact", segmentSizeAfterCompact),
		zap.String("finalState", "Sealed"))

	return &proto.SegmentMetadata{
		State:      proto.SegmentState_Sealed,
		SealedTime: time.Now().UnixMilli(),
		Size:       segmentSizeAfterCompact,
	}, nil
}

func (s *segmentProcessor) GetLastAccessTime() int64 {
	return s.lastAccessTime.Load()
}

func (s *segmentProcessor) Clean(ctx context.Context, flag int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Clean")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor clean operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("cleanupFlag", flag))

	impl, err := s.getOrCreateSegmentImpl(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segment reader for cleanup",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Int("cleanupFlag", flag),
			zap.Error(err))
		return err
	}

	logger.Ctx(ctx).Info("Starting fragment deletion operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("cleanupFlag", flag))

	deleteCount, err := impl.DeleteFileData(ctx, flag)
	if err != nil {
		logger.Ctx(ctx).Warn("Fragment deletion operation failed",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Int("cleanupFlag", flag),
			zap.Int("deleteCount", deleteCount),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return err
	}

	cleanupDuration := time.Since(start)
	logger.Ctx(ctx).Info("Segment processor clean operation completed successfully",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("cleanupFlag", flag),
		zap.Duration("totalDuration", cleanupDuration))

	return nil
}

func (s *segmentProcessor) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Close")
	defer sp.End()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor close operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))
	var writerErr error

	if s.currentSegmentWriter != nil {
		logger.Ctx(ctx).Info("Closing segment writer",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId))

		writerErr = s.currentSegmentWriter.Close(ctx)
		if writerErr != nil {
			logger.Ctx(ctx).Warn("close segment writer failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(writerErr))
		} else {
			logger.Ctx(ctx).Info("Successfully closed segment writer",
				zap.Int64("logId", s.logId),
				zap.Int64("segId", s.segId))
		}
	} else {
		logger.Ctx(ctx).Info("No segment writer to close",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId))
	}

	// Determine the final error to return
	closeDuration := time.Since(start)
	if writerErr != nil {
		logger.Ctx(ctx).Warn("Segment processor close operation completed with errors",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Duration("totalDuration", closeDuration),
			zap.Error(writerErr))
		return writerErr
	}

	logger.Ctx(ctx).Info("Segment processor close operation completed successfully",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Duration("totalDuration", closeDuration))
	return nil
}

// updateAccessTime updates the last access time to current time
func (s *segmentProcessor) updateAccessTime() {
	s.lastAccessTime.Store(time.Now().UnixMilli())
}
