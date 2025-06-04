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

const (
	ProcessorScopeName = "WoodpeckerSegmentProcessor"
)

// SegmentProcessor for segment processing in server side
//
//go:generate mockery --dir=./server/processor --name=SegmentProcessor --structname=SegmentProcessor --output=mocks/mocks_server/mocks_segment --filename=mock_segment_processor.go --with-expecter=true  --outpkg=mocks_segment
type SegmentProcessor interface {
	GetLogId() int64
	GetSegmentId() int64
	AddEntry(context.Context, *SegmentEntry, chan<- int64) (int64, error)
	ReadEntry(context.Context, int64) (*SegmentEntry, error)
	ReadBatchEntries(context.Context, int64, int64) ([]*SegmentEntry, error)
	IsFenced(ctx context.Context) bool
	SetFenced(ctx context.Context)
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
	return s.fenced.Load()
}

func (s *segmentProcessor) SetFenced(ctx context.Context) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "SetFenced")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	s.fenced.Store(true)
	if s.currentSegmentWriter != nil {
		closeSegmentWriterErr := s.currentSegmentWriter.Close(ctx)
		if closeSegmentWriterErr != nil {
			logger.Ctx(context.TODO()).Error("close log file writer failed", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Error(closeSegmentWriterErr))
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

func (s *segmentProcessor) AddEntry(ctx context.Context, entry *SegmentEntry, resultCh chan<- int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "AddEntry")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logger.Ctx(ctx).Debug("segment processor add entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entry.EntryId), zap.String("segmentProcessorInstance", fmt.Sprintf("%p", s)))
	if s.IsFenced(ctx) {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "fenced").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "fenced").Observe(float64(time.Since(start).Milliseconds()))
		return -1, werr.ErrSegmentFenced.WithCauseErrMsg(fmt.Sprintf("append entry:%d failed, log:%d segment:%d is fenced", entry.EntryId, s.logId, s.segId))
	}

	segmentWriter, err := s.getOrCreateSegmentWriter(ctx)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "writer_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "writer_error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}

	bufferedSeqNo, err := segmentWriter.AppendAsync(ctx, entry.EntryId, entry.Data, resultCh)
	if bufferedSeqNo == -1 {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "append_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "append_error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, fmt.Errorf("failed to append to log file")
	} else if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "add_entry", "success").Observe(float64(time.Since(start).Milliseconds()))
	return bufferedSeqNo, nil
}

func (s *segmentProcessor) ReadEntry(ctx context.Context, entryId int64) (*SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "ReadEntry")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logger.Ctx(ctx).Debug("segment processor read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("entryId", entryId))
	segmentReader, err := s.getOrCreateSegmentReader(ctx, entryId)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	// TODO should cache reader for each open reader
	r, err := segmentReader.NewReader(ctx, storage.ReaderOpt{
		StartSequenceNum: entryId,
		EndSequenceNum:   entryId + 1,
	})
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_entry", "new_reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_entry", "new_reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	hasNext, err := r.HasNext(ctx)
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

	e, err := r.ReadNext(ctx)
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

func (s *segmentProcessor) ReadBatchEntries(ctx context.Context, fromEntryId int64, size int64) ([]*SegmentEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "ReadBatchEntries")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	logger.Ctx(ctx).Debug("segment processor read batch entries", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Int64("size", size))
	segmentReader, err := s.getOrCreateSegmentReader(ctx, fromEntryId)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	r, err := segmentReader.NewReader(ctx, storage.ReaderOpt{
		StartSequenceNum: fromEntryId,
		EndSequenceNum:   0, // means no stop point
	})
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "new_reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "new_reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	hasNext, err := r.HasNext(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to check has next", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "has_next_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "has_next_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	if !hasNext {
		logger.Ctx(ctx).Debug("no batch entries found", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId))
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "not_found").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "not_found").Observe(float64(time.Since(start).Milliseconds()))
		return nil, werr.ErrEntryNotFound
	}

	// read batch entries
	batchEntries, err := r.ReadNextBatch(ctx, size)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read entry", zap.Int64("logId", s.logId), zap.Int64("segId", s.segId), zap.Int64("fromEntryId", fromEntryId), zap.Error(err))
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "read_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "read_error").Observe(float64(time.Since(start).Milliseconds()))
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
	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "read_batch_entries", "success").Observe(float64(time.Since(start).Milliseconds()))
	return result, nil
}

func (s *segmentProcessor) GetSegmentLastAddConfirmed(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "GetSegmentLastAddConfirmed")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	segmentReader, err := s.getOrCreateSegmentReader(ctx, -1)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}

	lastEntryId, err := segmentReader.GetLastEntryId(ctx)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "error").Observe(float64(time.Since(start).Milliseconds()))
		return -1, err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "get_last_confirmed", "success").Observe(float64(time.Since(start).Milliseconds()))
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
			disk.WithWriteFragmentSize(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxBytes),
			disk.WithWriteMaxBufferSize(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxFlushSize),
			disk.WithWriteMaxEntryPerFile(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxEntries),
			disk.WithWriteMaxIntervalMs(s.cfg.Woodpecker.Logstore.LogFileSyncPolicy.MaxInterval))
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
			path.Join(s.cfg.Woodpecker.Storage.RootPath, s.getSegmentKeyPrefix()))
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
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	segmentReader, err := s.getOrCreateSegmentReader(ctx, 0)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	mergedFrags, entryOffset, fragsOffset, mergedErr := segmentReader.Merge(ctx)
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
	lastEntryIdOfAllMergedFrags, err := lastMergedFrag.GetLastEntryId(ctx)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "get_last_entry_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "get_last_entry_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	totalSize := int64(0)
	for _, frag := range mergedFrags {
		totalSize += frag.GetSize()
	}

	logger.Ctx(ctx).Debug("Compact segment merge completed",
		zap.Int64("logId", s.logId),
		zap.Int64("segId", s.segId),
		zap.Int("mergedFrags", len(mergedFrags)),
		zap.Int64("lastEntryId", lastEntryIdOfAllMergedFrags),
		zap.Int64("totalSize", totalSize))

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "compact", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "compact", "success").Observe(float64(time.Since(start).Milliseconds()))
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

func (s *segmentProcessor) Recover(ctx context.Context) (*proto.SegmentMetadata, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Recover")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	segmentReader, err := s.getOrCreateSegmentReader(ctx, 0)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "recover", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "recover", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}

	size, lastFragment, err := segmentReader.Load(ctx)
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

	lastEntryId, err := lastFragment.GetLastEntryId(ctx)
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
		CompletionTime: lastFragment.GetLastModified(ctx),
		LastEntryId:    lastEntryId,
		Size:           size,
	}, nil
}

func (s *segmentProcessor) Clean(ctx context.Context, flag int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, ProcessorScopeName, "Clean")
	defer sp.End()
	start := time.Now()
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segId)

	segmentReader, err := s.getOrCreateSegmentReader(ctx, 0) // TODO use a writer or special instance for maintain
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean", "reader_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean", "reader_error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	err = segmentReader.DeleteFragments(ctx, flag)
	if err != nil {
		metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean", "delete_error").Inc()
		metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean", "delete_error").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	metrics.WpSegmentProcessorOperationsTotal.WithLabelValues(logIdStr, segIdStr, "clean", "success").Inc()
	metrics.WpSegmentProcessorOperationLatency.WithLabelValues(logIdStr, segIdStr, "clean", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}
