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
	"errors"
	"fmt"
	"github.com/zilliztech/woodpecker/common/channel"
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
	ReadEntry(context.Context, int64) (*SegmentEntry, error)
	ReadBatchEntries(context.Context, int64, int64) ([]*SegmentEntry, error)
	IsFenced(ctx context.Context) bool
	SetFenced(ctx context.Context) (int64, error)
	Compact(ctx context.Context) (*proto.SegmentMetadata, error)
	Recover(ctx context.Context) (*proto.SegmentMetadata, error)
	GetSegmentLastAddConfirmed(ctx context.Context) (int64, error)
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

// NewSegmentProcessorWithLogFile TODO Test Only
func NewSegmentProcessorWithLogFile(ctx context.Context, cfg *config.Configuration, logId int64, segId int64, minioCli minioHandler.MinioHandler, currentLogFile storage.Segment) SegmentProcessor {
	s := &segmentProcessor{
		cfg:                  cfg,
		logId:                logId,
		segId:                segId,
		minioClient:          minioCli,
		createTime:           time.Now().UnixMilli(),
		currentSegmentWriter: currentLogFile,
		currentSegmentReader: currentLogFile,
	}
	s.lastAccessTime.Store(time.Now().UnixMilli())
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

	// for segment writer
	currentSegmentWriter storage.Segment
	fenced               atomic.Bool

	// for segment reader
	currentSegmentReader storage.Segment
}

func (s *segmentProcessor) GetLogId() int64 {
	return s.logId
}

func (s *segmentProcessor) GetSegmentId() int64 {
	return s.segId
}

func (s *segmentProcessor) IsFenced(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "IsFenced")
	defer sp.End()
	s.updateAccessTime()
	return s.fenced.Load()
}

func (s *segmentProcessor) SetFenced(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "SetFenced")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor fence operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	// for idempotent fence, the storage layer can only fence until it is successful
	s.fenced.Store(true)
	logger.Ctx(ctx).Info("Set segment processor fenced state",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	if s.currentSegmentWriter != nil {
		logger.Ctx(ctx).Info("Closing segment writer during fence operation",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId))

		// Close&sync segment writer
		closeSegmentWriterErr := s.currentSegmentWriter.Close(ctx)
		if closeSegmentWriterErr != nil {
			logger.Ctx(ctx).Warn("close log file writer failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(closeSegmentWriterErr))
			return -1, closeSegmentWriterErr
		}

		logger.Ctx(ctx).Info("Successfully closed segment writer, retrieving last entry ID",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId))

		lastEntryId, getLastEntryIdErr := s.currentSegmentWriter.GetLastEntryId(ctx)
		if getLastEntryIdErr != nil {
			logger.Ctx(ctx).Warn("get log file last entry id failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(getLastEntryIdErr))
			return -1, getLastEntryIdErr
		}

		logger.Ctx(ctx).Info("Segment processor fence operation completed successfully",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Int64("lastEntryId", lastEntryId),
			zap.Duration("duration", time.Since(start)))

		return lastEntryId, getLastEntryIdErr
	}

	logger.Ctx(ctx).Info("No segment writer found during fence operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Duration("duration", time.Since(start)))

	return -1, werr.ErrSegmentNoWritingFragment
}

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "AddEntry")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("segment processor add entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entry.EntryId), zap.String("segmentProcessorInstance", fmt.Sprintf("%p", s)))
	if s.IsFenced(ctx) {
		return -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("append entry:%d failed, log:%d segment:%d is fenced", entry.EntryId, s.logId, s.segId))
	}

	segmentWriter, err := s.getOrCreateSegmentWriter(ctx)
	if err != nil {
		return -1, err
	}

	bufferedSeqNo, err := segmentWriter.AppendAsync(ctx, entry.EntryId, entry.Data, resultCh)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to append to log file", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(err))
		return -1, err
	} else if bufferedSeqNo == -1 {
		logger.Ctx(ctx).Warn("failed to append to log file", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId))
		return -1, fmt.Errorf("failed to append to log file")
	}

	return bufferedSeqNo, nil
}

func (s *segmentProcessor) ReadEntry(ctx context.Context, entryId int64) (*SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "ReadEntry")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("segment processor read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId))
	segmentReader, err := s.getOrCreateSegmentReader(ctx, entryId)
	if err != nil {
		return nil, err
	}
	r, err := segmentReader.NewReader(ctx, storage.ReaderOpt{
		StartSequenceNum: entryId,
		EndSequenceNum:   entryId + 1,
	})
	if err != nil {
		return nil, err
	}

	hasNext, err := r.HasNext(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to check has next", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if !hasNext {
		logger.Ctx(ctx).Debug("no entry found", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId))
		return nil, werr.ErrEntryNotFound
	}

	e, err := r.ReadNext(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}

	return &SegmentEntry{
		SegmentId: s.segId,
		EntryId:   e.EntryId,
		Data:      e.Values,
	}, nil
}

func (s *segmentProcessor) ReadBatchEntries(ctx context.Context, fromEntryId int64, size int64) ([]*SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "ReadBatchEntries")
	defer sp.End()
	s.updateAccessTime()
	logger.Ctx(ctx).Debug("segment processor read batch entries", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("size", size))
	segmentReader, err := s.getOrCreateSegmentReader(ctx, fromEntryId)
	if err != nil {
		return nil, err
	}

	r, err := segmentReader.NewReader(ctx, storage.ReaderOpt{
		StartSequenceNum: fromEntryId,
		EndSequenceNum:   0, // means no stop point
	})
	if err != nil {
		return nil, err
	}

	hasNext, err := r.HasNext(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to check has next", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		return nil, err
	}
	if !hasNext {
		logger.Ctx(ctx).Debug("no batch entries found", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId))
		return nil, werr.ErrEntryNotFound
	}

	// read batch entries
	batchEntries, err := r.ReadNextBatch(ctx, size)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		return nil, err
	}
	// batch result
	result := make([]*SegmentEntry, 0, len(batchEntries))
	for _, entry := range batchEntries {
		segmentEntry := &SegmentEntry{
			SegmentId: s.segId,
			EntryId:   entry.EntryId,
			Data:      entry.Values,
		}
		result = append(result, segmentEntry)
	}
	// update metrics
	return result, nil
}

func (s *segmentProcessor) GetSegmentLastAddConfirmed(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	s.updateAccessTime()
	segmentReader, err := s.getOrCreateSegmentReader(ctx, -1)
	if err != nil {
		return -1, err
	}

	lastEntryId, err := segmentReader.GetLastEntryId(ctx)
	if err != nil {
		return -1, err
	}

	return lastEntryId, nil
}

func (s *segmentProcessor) getOrCreateSegmentWriter(ctx context.Context) (storage.Segment, error) {
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
		writerFile, err := disk.NewDiskSegmentImpl(
			ctx,
			s.logId,
			s.segId,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()),
			disk.WithWriteFragmentSize(s.cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes),
			disk.WithWriteMaxBufferSize(s.cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize),
			disk.WithWriteMaxEntryPerFile(s.cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries),
			disk.WithWriteMaxIntervalMs(s.cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage))
		s.currentSegmentWriter = writerFile
		logger.Ctx(ctx).Info("create DiskSegmentImpl for write", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.String("logFileInst", fmt.Sprintf("%p", writerFile)))
		return s.currentSegmentWriter, err
	} else {
		// use MinIO-compatible storage
		s.currentSegmentWriter = objectstorage.NewSegmentImpl(
			ctx,
			s.logId,
			s.segId,
			s.getSegmentKeyPrefix(),
			s.getInstanceBucket(),
			s.minioClient,
			s.cfg)
		logger.Ctx(ctx).Info("create SegmentImpl for write", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.String("logFileInst", fmt.Sprintf("%p", s.currentSegmentWriter)))
	}
	return s.currentSegmentWriter, nil
}

func (s *segmentProcessor) getOrCreateSegmentReader(ctx context.Context, entryId int64) (storage.Segment, error) {
	// First check with read lock to avoid data race
	s.RLock()
	if s.currentSegmentReader != nil {
		reader := s.currentSegmentReader
		s.RUnlock()
		return reader, nil
	}
	s.RUnlock()

	// Need to initialize, acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check after acquiring lock
	if s.currentSegmentReader != nil {
		return s.currentSegmentReader, nil
	}

	// Initialize reader
	if s.cfg.Woodpecker.Storage.IsStorageLocal() || s.cfg.Woodpecker.Storage.IsStorageService() {
		// use local FileSystem or local FileSystem + minio-compatible
		readerFile, err := disk.NewRODiskSegmentImpl(
			ctx,
			s.logId,
			s.segId,
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()),
			disk.WithReadFragmentSize(s.cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes),
			disk.WithReadBatchSize(s.cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize))
		s.currentSegmentReader = readerFile
		logger.Ctx(ctx).Info("create RODiskSegmentImpl for read", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.Int64("entryId", entryId), zap.String("logFileInst", fmt.Sprintf("%p", readerFile)))
		return s.currentSegmentReader, err
	} else {
		s.currentSegmentReader = objectstorage.NewROSegmentImpl(
			ctx,
			s.logId,
			s.segId,
			s.getSegmentKeyPrefix(),
			s.getInstanceBucket(),
			s.minioClient,
			s.cfg)
		logger.Ctx(ctx).Info("create ROSegmentImpl for read", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId), zap.String("SegmentKeyPrefix", s.getSegmentKeyPrefix()), zap.String("logFileInst", fmt.Sprintf("%p", s.currentSegmentReader)))
	}
	return s.currentSegmentReader, nil
}

func (s *segmentProcessor) getInstanceBucket() string {
	return s.cfg.Minio.BucketName
}

func (s *segmentProcessor) getSegmentKeyPrefix() string {
	return fmt.Sprintf("%s/%d/%d", s.cfg.Minio.RootPath, s.logId, s.segId)
}

func (s *segmentProcessor) Compact(ctx context.Context) (*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Compact")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor compact operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	segmentReader, err := s.getOrCreateSegmentReader(ctx, 0)
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

	mergedFrags, entryOffset, fragsOffset, mergedErr := segmentReader.Merge(ctx)
	if mergedErr != nil {
		logger.Ctx(ctx).Warn("Segment merge operation failed",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Duration("duration", time.Since(start)),
			zap.Error(mergedErr))
		return nil, mergedErr
	}

	if len(mergedFrags) == 0 {
		logger.Ctx(ctx).Info("No fragments found to merge during compaction",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Duration("duration", time.Since(start)))
		return nil, errors.New("no frags to merge")
	}

	logger.Ctx(ctx).Info("Segment merge completed, processing merged fragments",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("mergedFragmentCount", len(mergedFrags)),
		zap.Int("entryOffsetCount", len(entryOffset)),
		zap.Int("fragmentOffsetCount", len(fragsOffset)))

	lastMergedFrag := mergedFrags[len(mergedFrags)-1]
	lastEntryIdOfAllMergedFrags, err := lastMergedFrag.GetLastEntryId(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get last entry ID from merged fragments",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Error(err))
		return nil, err
	}

	totalSize := int64(0)
	for _, frag := range mergedFrags {
		totalSize += frag.GetSize()
	}

	logger.Ctx(ctx).Info("Compact segment merge completed",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("mergedFrags", len(mergedFrags)),
		zap.Int64("lastEntryId", lastEntryIdOfAllMergedFrags),
		zap.Int64("totalSize", totalSize))

	compactionDuration := time.Since(start)
	logger.Ctx(ctx).Info("Segment processor compact operation completed successfully",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Duration("totalDuration", compactionDuration),
		zap.Int64("finalLastEntryId", lastEntryIdOfAllMergedFrags),
		zap.Int64("finalSize", totalSize),
		zap.String("finalState", "Sealed"))

	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Sealed,
		CompletionTime: lastMergedFrag.GetLastModified(ctx),
		SealedTime:     time.Now().UnixMilli(),
		LastEntryId:    lastEntryIdOfAllMergedFrags,
		Size:           totalSize,
		EntryOffset:    entryOffset,
		FragmentOffset: fragsOffset,
	}, nil
}

func (s *segmentProcessor) GetLastAccessTime() int64 {
	return s.lastAccessTime.Load()
}

func (s *segmentProcessor) Recover(ctx context.Context) (*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Recover")
	defer sp.End()
	s.updateAccessTime()
	start := time.Now()
	logger.Ctx(ctx).Info("Starting segment processor recovery operation",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	segmentReader, err := s.getOrCreateSegmentReader(ctx, 0)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segment reader for recovery",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Starting segment load operation for recovery",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId))

	size, lastFragment, err := segmentReader.Load(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Segment load operation failed during recovery",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil, err
	}

	if lastFragment == nil {
		logger.Ctx(ctx).Info("No fragments found during recovery, creating empty segment metadata",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Int64("segmentSize", size),
			zap.Duration("duration", time.Since(start)))
		return &proto.SegmentMetadata{
			State:          proto.SegmentState_Completed,
			CompletionTime: time.Now().UnixMilli(),
			LastEntryId:    -1,
			Size:           size,
		}, nil
	}

	logger.Ctx(ctx).Info("Found last fragment, retrieving last entry ID",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.String("lastFragmentKey", lastFragment.GetFragmentKey()),
		zap.Int64("segmentSize", size))

	lastEntryId, err := lastFragment.GetLastEntryId(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get last entry ID from last fragment during recovery",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.String("lastFragmentKey", lastFragment.GetFragmentKey()),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("recover segment load completed",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int64("lastEntryId", lastEntryId),
		zap.String("lastFrag", lastFragment.GetFragmentKey()))

	recoveryDuration := time.Since(start)
	logger.Ctx(ctx).Info("Segment processor recovery operation completed successfully",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Duration("totalDuration", recoveryDuration),
		zap.Int64("finalLastEntryId", lastEntryId),
		zap.Int64("finalSize", size),
		zap.String("finalState", "Completed"),
		zap.Int64("completionTime", lastFragment.GetLastModified(ctx)))

	return &proto.SegmentMetadata{
		State:          proto.SegmentState_Completed,
		CompletionTime: lastFragment.GetLastModified(ctx),
		LastEntryId:    lastEntryId,
		Size:           size,
	}, nil
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

	segmentReader, err := s.getOrCreateSegmentReader(ctx, 0) // TODO use a writer or special instance for maintain
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

	err = segmentReader.DeleteFragments(ctx, flag)
	if err != nil {
		logger.Ctx(ctx).Warn("Fragment deletion operation failed",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Int("cleanupFlag", flag),
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

	var writerErr, readerErr error

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

	if s.currentSegmentReader != nil {
		logger.Ctx(ctx).Info("Closing segment reader",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId))

		readerErr = s.currentSegmentReader.Close(ctx)
		if readerErr != nil {
			logger.Ctx(ctx).Warn("close segment reader failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(readerErr))
		} else {
			logger.Ctx(ctx).Info("Successfully closed segment reader",
				zap.Int64("logId", s.logId),
				zap.Int64("segId", s.segId))
		}
	} else {
		logger.Ctx(ctx).Info("No segment reader to close",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId))
	}

	// Determine the final error to return
	var finalErr error
	if writerErr != nil {
		finalErr = writerErr
	} else if readerErr != nil {
		finalErr = readerErr
	}

	closeDuration := time.Since(start)
	if finalErr != nil {
		logger.Ctx(ctx).Warn("Segment processor close operation completed with errors",
			zap.Int64("logId", s.logId),
			zap.Int64("segId", s.segId),
			zap.Duration("totalDuration", closeDuration),
			zap.Error(finalErr))
		return finalErr
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
