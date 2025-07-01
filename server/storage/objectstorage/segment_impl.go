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

package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage/legacy"
)

const (
	SegmentScopeName = "Segment"
)

var _ storage.Segment = (*SegmentImpl)(nil)

// SegmentImpl is used to write data to object storage as a logical segment
type SegmentImpl struct {
	mu                sync.Mutex
	lastSyncTimestamp atomic.Int64
	client            minioHandler.MinioHandler
	segmentPrefixKey  string // The prefix key for the segment to which this Segment belongs
	bucket            string // The bucket name
	logId             int64
	segmentId         int64
	logIdStr          string // for metrics label only
	segmentIdStr      string // for metrics label only

	// write buffer
	buffer           atomic.Pointer[cache.SequentialBuffer] // Write buffer
	maxBufferSize    int64                                  // Max buffer size to sync buffer to object storage
	maxBufferEntries int64                                  // Maximum number of entries per buffer
	maxIntervalMs    int                                    // Max interval to sync buffer to object storage
	syncPolicyConfig *config.SegmentSyncPolicyConfig
	fileClose        chan struct{} // Close signal
	closeOnce        sync.Once
	closed           atomic.Bool

	// written info
	firstEntryID   atomic.Int64 // The first entryId of this Segment which already written to object storage
	lastEntryID    atomic.Int64 // The last entryId of this Segment which already written to object storage
	lastFragmentID atomic.Int64 // The last fragmentId of this Segment which already written to object storage

	// sync task pool
	fastSyncTriggerSize             int64 // The size of min buffer to trigger fast sync
	pool                            *conc.Pool[*flushResult]
	syncMu                          sync.Mutex
	storageWritable                 atomic.Bool  // Indicates whether the segment is writable
	flushingBufferSize              atomic.Int64 // The size of pending flush, it must be less than maxBufferSize
	flushingTaskList                chan *flushTask
	lastSubmittedFlushingFragmentID atomic.Int64

	// Object storage lock for segment exclusivity
	lockObjectKey string // Key for the lock object in object storage
	// For fence state: true confirms it is fenced, while false requires verification by checking the storage for a fence flag object.
	fenced atomic.Bool
}

// NewSegmentImpl is used to create a new Segment, which is used to write data to object storage
func NewSegmentImpl(ctx context.Context, logId int64, segId int64, segmentPrefixKey string, bucket string, objectCli minioHandler.MinioHandler, cfg *config.Configuration) storage.Segment {
	logger.Ctx(ctx).Debug("new SegmentImpl created", zap.String("segmentPrefixKey", segmentPrefixKey))
	syncPolicyConfig := &cfg.Woodpecker.Logstore.SegmentSyncPolicy
	maxBufferEntries := int64(syncPolicyConfig.MaxEntries)
	newBuffer := cache.NewSequentialBuffer(logId, segId, 0, maxBufferEntries)
	objFile := &SegmentImpl{
		logId:            logId,
		segmentId:        segId,
		logIdStr:         fmt.Sprintf("%d", logId),
		segmentIdStr:     fmt.Sprintf("%d", segId),
		client:           objectCli,
		segmentPrefixKey: segmentPrefixKey,
		bucket:           bucket,

		maxBufferSize:    syncPolicyConfig.MaxBytes,
		maxBufferEntries: maxBufferEntries,
		maxIntervalMs:    syncPolicyConfig.MaxInterval,
		syncPolicyConfig: syncPolicyConfig,
		fileClose:        make(chan struct{}, 1),

		fastSyncTriggerSize: syncPolicyConfig.MaxFlushSize, // set sync trigger size equal to maxFlushSize(single fragment max size) to make pipeline flush soon
		pool:                conc.NewPool[*flushResult](cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads, conc.WithPreAlloc(true)),

		flushingTaskList: make(chan *flushTask, cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads),
	}
	objFile.buffer.Store(newBuffer)
	objFile.firstEntryID.Store(-1)
	objFile.lastEntryID.Store(-1)
	objFile.lastFragmentID.Store(-1)
	objFile.lastSubmittedFlushingFragmentID.Store(-1)
	objFile.closed.Store(false)
	objFile.storageWritable.Store(true)
	objFile.flushingBufferSize.Store(0)
	objFile.fenced.Store(false)

	// Create segment lock object
	if err := objFile.createSegmentLock(ctx); err != nil {
		logger.Ctx(ctx).Error("Failed to create segment lock",
			zap.String("segmentPrefixKey", segmentPrefixKey),
			zap.Error(err))
		return nil // Return nil to indicate creation failure
	}

	go objFile.run()
	go objFile.ack()
	return objFile
}

// Like OS file fsync dirty pageCache periodically, objectStoreFile will sync buffer to object storage periodically
func (f *SegmentImpl) run() {
	// time ticker
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()
	f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	logIdStr := fmt.Sprintf("%d", f.logId)
	metrics.WpFileWriters.WithLabelValues(logIdStr).Inc()
	for {
		select {
		case <-ticker.C:
			if time.Now().UnixMilli()-f.lastSyncTimestamp.Load() < int64(f.maxIntervalMs) {
				continue
			}
			// Check if closed
			if f.closed.Load() {
				return
			}
			ctx, sp := logger.NewIntentCtx(SegmentScopeName, fmt.Sprintf("run_%d_%d", f.logId, f.segmentId))
			err := f.Sync(context.Background())
			if err != nil {
				logger.Ctx(ctx).Warn("sync error",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Error(err))
			}
			sp.End()
			ticker.Reset(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
		case <-f.fileClose:
			logger.Ctx(context.TODO()).Debug("close SegmentImpl", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
			metrics.WpFileWriters.WithLabelValues(logIdStr).Dec()
			return
		}
	}
}

func (f *SegmentImpl) ack() {
	var firstFlushErrTask *flushTask
	for task := range f.flushingTaskList {
		if task.flushFuture.OK() {
			if firstFlushErrTask != nil {
				// flush success, but there is a flush error task before
				logger.Ctx(context.TODO()).Info("flush success but error exists before, trigger fast flush fail",
					zap.String("firstFlushErrFragment", firstFlushErrTask.flushFuture.Value().target.GetFragmentKey()),
					zap.String("flushSuccessFragment", task.flushFuture.Value().target.GetFragmentKey()))
				f.fastFlushFailUnsafe(context.TODO(), task.flushData, firstFlushErrTask.flushFuture.Value().err)
			} else {
				// update flush state
				result := task.flushFuture.Value()
				flushedFirst, _ := result.target.GetFirstEntryId(context.TODO()) // always no error, because it's just created
				flushedLast, _ := result.target.GetLastEntryId(context.TODO())   // always no error, because it's just created
				flushedFragId := result.target.GetFragmentId()
				if flushedLast >= 0 {
					f.lastEntryID.Store(flushedLast)
					f.lastFragmentID.Store(flushedFragId)
					if f.firstEntryID.Load() == -1 {
						// Initialize firstEntryId on first successful flush
						// This should always be 0 for the initial flush
						f.firstEntryID.Store(flushedFirst)
					}
				}

				logger.Ctx(context.TODO()).Info("flush success, fast success ack",
					zap.String("flushSuccessFragment", task.flushFuture.Value().target.GetFragmentKey()))
				// flush success ack
				f.fastFlushSuccessUnsafe(context.TODO(), task.flushData)
			}
		} else {
			// flush fail, trigger mark storage not writable
			if firstFlushErrTask == nil {
				// after many retry flush fail, mark storage not writable
				firstFlushErrTask = task
				f.storageWritable.Store(false)
			}
			if werr.ErrSegmentFenced.Is(task.flushFuture.Err()) {
				// when put object fail cause by fence object exists, mark storage not writable
				f.fenced.Store(true)
			}
			logger.Ctx(context.TODO()).Info("flush error first encountered, trigger fast flush fail",
				zap.String("firstFlushErrFragment", firstFlushErrTask.flushFuture.Value().target.GetFragmentKey()))
			f.fastFlushFailUnsafe(context.TODO(), task.flushData, task.flushFuture.Value().err)
		}
		f.flushingBufferSize.Add(-task.flushFuture.Value().fragmentSize)
	}
}

func (f *SegmentImpl) GetId() int64 {
	return f.segmentId
}

func (f *SegmentImpl) AppendAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "AppendAsync")
	defer sp.End()
	if f.closed.Load() {
		// quick fail and return a close Err, which indicate than it is also not retriable
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment writer is closed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrLogFileClosed
	}
	if f.fenced.Load() {
		// quick fail and return a fenced Err
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment is fenced", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrSegmentFenced
	}
	if !f.storageWritable.Load() {
		// quick fail and return a Storage Err, which indicate that it is also not retriable
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment storage not writable", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrStorageNotWritable
	}

	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))

	// trigger sync by max buffer entries num
	currentBuffer := f.buffer.Load()
	pendingAppendId := currentBuffer.ExpectedNextEntryId.Load() + 1
	if pendingAppendId >= currentBuffer.FirstEntryId+currentBuffer.MaxEntries {
		logger.Ctx(ctx).Debug("buffer full, trigger flush",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("pendingAppendId", pendingAppendId),
			zap.Int64("bufferFirstId", currentBuffer.FirstEntryId),
			zap.Int64("bufferLastId", currentBuffer.FirstEntryId+currentBuffer.MaxEntries))
		err := f.Sync(ctx)
		if err != nil {
			// sync does not success
			logger.Ctx(ctx).Warn("AppendAsync: found buffer full, but sync failed before append", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(err))
			return entryId, err
		}
	}

	f.mu.Lock()
	if entryId <= f.lastEntryID.Load() {
		// If entryId is less than or equal to lastEntryID, it indicates that the entry has already been written to object storage. Return immediately.
		logger.Ctx(ctx).Debug("AppendAsync: skipping write, entryId is not greater than lastEntryID, already stored", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int64("lastEntryID", f.lastEntryID.Load()))
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, entryId, resultCh, entryId, nil)
		f.mu.Unlock()
		return entryId, nil
	}

	currentBuffer = f.buffer.Load()
	// write buffer with notification channel
	id, err := currentBuffer.WriteEntryWithNotify(entryId, data, resultCh)
	if err != nil {
		// write to buffer failed
		f.mu.Unlock()
		return id, err
	}
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	f.mu.Unlock()

	// trigger sync by max buffer entries bytes size
	sequentialReadyDataSize := currentBuffer.SequentialReadyDataSize.Load()
	dataSize := currentBuffer.DataSize.Load()
	if sequentialReadyDataSize >= f.fastSyncTriggerSize || dataSize >= f.maxBufferSize {
		logger.Ctx(ctx).Debug("reach max buffer size, trigger flush", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("bufferSize", dataSize), zap.Int64("sequentialReadyDataSize", sequentialReadyDataSize), zap.Int64("fastSyncTriggerSize", f.fastSyncTriggerSize), zap.Int64("maxSize", f.maxBufferSize))
		syncErr := f.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger flush failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("bufferSize", dataSize), zap.Int64("sequentialReadyDataSize", sequentialReadyDataSize), zap.Int64("fastSyncTriggerSize", f.fastSyncTriggerSize), zap.Int64("maxSize", f.maxBufferSize), zap.Error(syncErr))
		}
	}

	return id, nil
}

// Deprecated: use AppendAsync instead
func (f *SegmentImpl) Append(ctx context.Context, data []byte) error {
	panic("not support sync append, it's too slow")
}

func (f *SegmentImpl) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	return nil, werr.ErrNotSupport.WithCauseErrMsg("SegmentImpl writer support write only, cannot create reader")
}

// LastFragmentId returns the last fragmentId of the log file.
func (f *SegmentImpl) LastFragmentId() int64 {
	return f.lastFragmentID.Load()
}

func (f *SegmentImpl) getFirstEntryId() int64 {
	return f.firstEntryID.Load()
}

func (f *SegmentImpl) GetLastEntryId(ctx context.Context) (int64, error) {
	return f.lastEntryID.Load(), nil
}

// flushTask is the task for flush.
type flushTask struct {
	flushData             []*cache.BufferEntry
	flushDataFirstEntryId int64
	flushFuture           *conc.Future[*flushResult]
}

// flushResult is the result of flush operation
type flushResult struct {
	target       storage.Fragment
	err          error
	fragmentSize int64 // Size of the fragment for tracking flushing buffer size
}

func (f *SegmentImpl) waitIfFlushingBufferSizeExceededUnsafe(ctx context.Context) error {
	// Check if current flushing buffer size exceeds the maximum allowed buffer size
	for {
		currentFlushingSize := f.flushingBufferSize.Load()
		if currentFlushingSize < f.maxBufferSize {
			// Safe to proceed, flushing buffer size is within limits
			logger.Ctx(ctx).Debug("Flushing buffer size check passed",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("currentFlushingSize", currentFlushingSize),
				zap.Int64("maxBufferSize", f.maxBufferSize))
			return nil
		}

		// Flushing buffer size exceeded, need to wait
		logger.Ctx(ctx).Debug("Flushing buffer size exceeded, waiting for space",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("currentFlushingSize", currentFlushingSize),
			zap.Int64("maxBufferSize", f.maxBufferSize))

		// Wait for a short period before checking again
		select {
		case <-ctx.Done():
			// Context cancelled, return immediately
			logger.Ctx(ctx).Warn("Context cancelled while waiting for flushing buffer space",
				zap.String("segmentPrefixKey", f.segmentPrefixKey))
			return ctx.Err()
		case <-f.fileClose:
			// Segment is being closed, return immediately
			logger.Ctx(ctx).Debug("Segment close signal received while waiting for flushing buffer space",
				zap.String("segmentPrefixKey", f.segmentPrefixKey))
			return werr.ErrLogFileClosed
		case <-time.After(10 * time.Millisecond):
			// Continue checking after a short delay
			continue
		}
	}
}

// Sync Implement sync logic, e.g., flush to persistent storage
func (f *SegmentImpl) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Sync")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	f.syncMu.Lock() // ensure only one sync operation is running at a time
	defer f.syncMu.Unlock()
	defer func() {
		f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	}()

	if !f.storageWritable.Load() {
		logger.Ctx(ctx).Warn("Call Sync, but storage is not writable, quick fail all append requests", zap.String("segmentPrefixKey", f.segmentPrefixKey))
		return f.quickSyncFailUnsafe(ctx, werr.ErrStorageNotWritable)
	}

	// roll buff with lock
	f.mu.Lock()
	currentBuffer, toFlushData, toFlushDataFirstEntryId, err := f.rollBufferUnsafe(ctx)
	f.mu.Unlock()
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesRangeData failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}
	if len(toFlushData) == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	// submit async flush task
	flushResultFutures := f.submitFragmentFlushTaskUnsafe(ctx, currentBuffer, toFlushData, toFlushDataFirstEntryId)
	logger.Ctx(ctx).Debug("Sync submitted flush tasks",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int("fragments", len(flushResultFutures)),
		zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId),
		zap.Int("toFlushEntries", len(toFlushData)),
		zap.Int64("restDataFirstEntryId", currentBuffer.GetExpectedNextEntryId()),
		zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))

	return nil
}

// Get data that is sequentially ready to be flushed
// For example, in a sequence like 1,2,x,3,x,5,6, "1,2" is ready, while "x,3,x,5,6" still needs to wait for missing entries to arrive before it can be flushed
// Therefore, the toFlush data is "1,2", and the remaining data stays in the buffer for further append operations
func (f *SegmentImpl) rollBufferUnsafe(ctx context.Context) (*cache.SequentialBuffer, []*cache.BufferEntry, int64, error) {
	startTime := time.Now()

	// wait available buffer size
	waitBuffErr := f.waitIfFlushingBufferSizeExceededUnsafe(ctx)
	if waitBuffErr != nil {
		return nil, nil, -1, waitBuffErr
	}

	// get current buffer
	currentBuffer := f.buffer.Load()

	logger.Ctx(ctx).Debug("start roll buffer", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))

	// check if there are any entries to be written
	entryCount := len(currentBuffer.Entries)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return currentBuffer, make([]*cache.BufferEntry, 0), -1, nil
	}
	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	// get flush point to flush
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return currentBuffer, make([]*cache.BufferEntry, 0), -1, nil
	}
	// get flush data
	toFlushData, err := currentBuffer.ReadEntriesRange(currentBuffer.FirstEntryId, expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesRangeData failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return currentBuffer, nil, -1, err
	}
	toFlushDataFirstEntryId := currentBuffer.FirstEntryId

	// roll new buffer with rest data
	restData, err := currentBuffer.ReadEntriesToLast(expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesToLastData failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(err))
		return currentBuffer, nil, -1, err
	}
	restDataFirstEntryId := expectedNextEntryId
	newBuffer := cache.NewSequentialBufferWithData(f.logId, f.segmentId, restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries), restData)
	f.buffer.Store(newBuffer)
	logger.Ctx(ctx).Debug("start roll buffer", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId), zap.Int("count", len(toFlushData)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)), zap.String("newBufInst", fmt.Sprintf("%p", newBuffer)))
	return currentBuffer, toFlushData, toFlushDataFirstEntryId, nil
}

func (f *SegmentImpl) fastFlushFailUnsafe(ctx context.Context, fragmentData []*cache.BufferEntry, resultErr error) {
	for _, item := range fragmentData {
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, item.EntryId, item.NotifyChan, -1, resultErr)
	}
}

func (f *SegmentImpl) fastFlushSuccessUnsafe(ctx context.Context, fragmentData []*cache.BufferEntry) {
	for _, item := range fragmentData {
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, item.EntryId, item.NotifyChan, item.EntryId, nil)
	}
}

func (f *SegmentImpl) submitFragmentFlushTaskUnsafe(ctx context.Context, currentBuffer *cache.SequentialBuffer, toFlushData []*cache.BufferEntry, toFlushDataFirstEntryId int64) []*conc.Future[*flushResult] {
	fragmentDataList, fragmentFirstEntryIdList, fragmentSizeList := f.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	flushResultFutures := make([]*conc.Future[*flushResult], 0, len(toFlushData))

	var waitBuffErr error
	for i, fragmentData := range fragmentDataList {
		fragId := f.lastSubmittedFlushingFragmentID.Add(1) // fragment id
		fragmentFirstEntryId := fragmentFirstEntryIdList[i]
		part := fragmentData
		fragmentSize := fragmentSizeList[i] // Capture fragment size for the closure

		if waitBuffErr != nil {
			// if error exist, fast fail subsequent parts
			f.fastFlushFailUnsafe(ctx, fragmentData, waitBuffErr)
			logger.Ctx(ctx).Warn("fast fail the flush task before submit",
				zap.Int64("logId", f.logId),
				zap.Int64("segmentId", f.segmentId),
				zap.Int64("fragId", fragId),
				zap.Int64("fragFirstEntryId", fragmentFirstEntryId),
				zap.Int("count", len(fragmentData)),
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Error(waitBuffErr))
		}

		// first wait available space
		waitErr := f.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		if waitErr != nil {
			// sync interrupted, fast fail and notify all pending append entries
			f.fastFlushFailUnsafe(ctx, fragmentData, waitErr)
			logger.Ctx(ctx).Warn("fast fail the flush task before submit",
				zap.Int64("logId", f.logId),
				zap.Int64("segmentId", f.segmentId),
				zap.Int64("fragId", fragId),
				zap.Int64("fragFirstEntryId", fragmentFirstEntryId),
				zap.Int("count", len(fragmentData)),
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Error(waitErr))
			waitBuffErr = waitErr
			continue
		}

		// try to submit flush task
		resultFuture := f.pool.Submit(func() (*flushResult, error) {
			logger.Ctx(ctx).Debug("start flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Int("count", len(fragmentData)), zap.Int64("fragmentSize", fragmentSize))
			key := getFragmentObjectKey(f.segmentPrefixKey, fragId)
			fragment := legacy.NewLegacyFragmentObject(ctx, f.client, f.bucket, f.logId, f.segmentId, fragId, key, part, fragmentFirstEntryId, true, false, true)
			flushErr := retry.Do(ctx,
				func() error {
					return fragment.Flush(ctx)
				},
				retry.Attempts(uint(f.syncPolicyConfig.MaxFlushRetries)),
				retry.Sleep(100*time.Millisecond),
				retry.MaxSleepTime(time.Duration(f.syncPolicyConfig.RetryInterval)*time.Millisecond),
				retry.RetryErr(func(err error) bool {
					// if it is not fenced error, retry
					return !werr.ErrSegmentFenced.Is(err)
				}),
			)
			if flushErr != nil {
				logger.Ctx(ctx).Warn("flush part of buffer as fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Error(flushErr))
			}
			// release fragment immediately
			releaseErr := fragment.Release(ctx)
			if releaseErr != nil {
				logger.Ctx(ctx).Warn("release fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Error(releaseErr))
			}
			result := &flushResult{
				target:       fragment,
				err:          flushErr,
				fragmentSize: fragmentSize,
			}
			logger.Ctx(ctx).Debug("complete flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Int("count", len(fragmentData)), zap.Int64("fragmentSize", fragmentSize))
			return result, flushErr
		})

		// update submit flushing size
		submitFlushingSize := fragmentSizeList[i]
		f.flushingBufferSize.Add(submitFlushingSize)
		f.flushingTaskList <- &flushTask{
			flushData:             part,
			flushDataFirstEntryId: fragmentFirstEntryId,
			flushFuture:           resultFuture,
		}
		flushResultFutures = append(flushResultFutures, resultFuture)
	}

	logger.Ctx(ctx).Debug("submitted fragment flush tasks", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int("fragments", len(fragmentDataList)), zap.Int("submitted", len(flushResultFutures)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	return flushResultFutures
}

func (f *SegmentImpl) awaitSuccessfulFlushes(ctx context.Context, flushResultFutures []*conc.Future[*flushResult]) ([]storage.Fragment, int64, int64, int64) {
	logger.Ctx(ctx).Debug("wait for all parts of buffer to be flushed", zap.String("segmentPrefixKey", f.segmentPrefixKey))
	firstFLushErr := conc.BlockOnAll(flushResultFutures...)
	if firstFLushErr != nil {
		logger.Ctx(ctx).Warn("all parts of buffer flush task finish, but found some fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(firstFLushErr))
	} else {
		logger.Ctx(ctx).Info("all parts of buffer have been flushed success", zap.String("segmentPrefixKey", f.segmentPrefixKey))
	}
	successFrags := make([]storage.Fragment, 0)
	flushedFirstEntryId := int64(-1)   // first entry id of the first fragment that was successfully flushed in sequence
	flushedLastEntryId := int64(-1)    // last entry id of the last fragment that was successfully flushed in sequence
	flushedLastFragmentId := int64(-1) // last fragment id that was successfully flushed in sequence
	for _, singleFlushResult := range flushResultFutures {
		r := singleFlushResult.Value()

		// Decrease flushing buffer size regardless of success or failure
		f.flushingBufferSize.Add(-r.fragmentSize)

		first, _ := r.target.GetFirstEntryId(ctx)
		last, _ := r.target.GetLastEntryId(ctx)
		fragId := r.target.GetFragmentId()
		if r.err != nil {
			logger.Ctx(ctx).Warn("flush fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Int64("firstEntryId", first), zap.Int64("lastEntryId", last))
			// Can only succeed sequentially without holes
			break
		} else {
			logger.Ctx(ctx).Debug("flush fragment success", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Int64("firstEntryId", first), zap.Int64("lastEntryId", last))
			successFrags = append(successFrags, r.target)
			flushedLastEntryId = last
			flushedLastFragmentId = fragId
			if flushedFirstEntryId == -1 {
				flushedFirstEntryId = first
			}
		}
	}
	return successFrags, flushedFirstEntryId, flushedLastEntryId, flushedLastFragmentId
}

func (f *SegmentImpl) quickSyncFailUnsafe(ctx context.Context, resultErr error) error {
	logger.Ctx(ctx).Warn("Call Sync, but storage is not writable, quick fail all append requests", zap.String("segmentPrefixKey", f.segmentPrefixKey))
	currentBuffer := f.buffer.Load()
	currentBuffer.NotifyAllPendingEntries(ctx, -1, resultErr)
	currentBuffer.Reset(ctx)
	return errors.New("storage is not writable")
}

func (f *SegmentImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Close")
	defer sp.End()
	if !f.closed.CompareAndSwap(false, true) { // mark close, and there will be no more add and sync in the future
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return nil
	}
	logger.Ctx(ctx).Info("run: received close signal,trigger sync before close ", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
	err := f.Sync(context.Background()) // manual sync all pending append operation
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
	}
	// Release segment lock
	if err := f.releaseSegmentLock(ctx); err != nil {
		logger.Ctx(ctx).Warn("Failed to release segment lock during close",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
	}

	// close file
	f.closeOnce.Do(func() {
		f.fileClose <- struct{}{}
		close(f.fileClose)
		close(f.flushingTaskList)
	})
	return nil
}

func (f *SegmentImpl) IsFenced(ctx context.Context) (bool, error) {
	return f.fenced.Load(), nil
}

func (f *SegmentImpl) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Fence")
	defer sp.End()

	// If already fenced, return idempotently
	if f.fenced.Load() {
		logger.Ctx(ctx).Debug("Segment already fenced, returning idempotently",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logId", f.logId),
			zap.Int64("segmentId", f.segmentId))
		return f.lastEntryID.Load(), nil
	}

	var lastEntryId int64
	var fenceObjectKey string

	// Use retry.Do to handle the fence object creation with retries
	err := retry.Do(ctx,
		func() error {
			// Get current last fragment ID and create fence object key
			currentLastFragmentId := f.lastFragmentID.Load()
			fenceFragmentId := currentLastFragmentId + 1
			fenceObjectKey = getFragmentObjectKey(f.segmentPrefixKey, fenceFragmentId)

			// Try to create fence object
			_, putErr := f.client.PutFencedObject(ctx, f.bucket, fenceObjectKey)
			if putErr != nil {
				if werr.ErrFragmentAlreadyExists.Is(putErr) {
					// Fragment already exists, this might be normal during concurrent operations
					logger.Ctx(ctx).Debug("Fence object already exists, retrying with next fragment ID",
						zap.String("segmentPrefixKey", f.segmentPrefixKey),
						zap.String("fenceObjectKey", fenceObjectKey),
						zap.Int64("fenceFragmentId", fenceFragmentId))
					return putErr // This will trigger a retry
				}
				// Other errors are not retryable
				logger.Ctx(ctx).Error("Failed to create fence object",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.String("fenceObjectKey", fenceObjectKey),
					zap.Error(putErr))
				return putErr
			}

			// Successfully created fence object
			lastEntryId = f.lastEntryID.Load()
			logger.Ctx(ctx).Info("Successfully created fence object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("fenceObjectKey", fenceObjectKey),
				zap.Int64("fenceFragmentId", fenceFragmentId),
				zap.Int64("lastEntryId", lastEntryId))
			return nil
		},
		retry.Attempts(5),                 // Retry up to 5 times
		retry.Sleep(100*time.Millisecond), // Initial sleep between retries
		retry.MaxSleepTime(1*time.Second), // Max sleep time between retries
		retry.RetryErr(func(err error) bool {
			// Only retry on fragment already exists error
			return werr.ErrFragmentAlreadyExists.Is(err)
		}),
	)

	if err != nil {
		logger.Ctx(ctx).Error("Failed to create fence object after retries",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.String("fenceObjectKey", fenceObjectKey),
			zap.Error(err))
		return -1, fmt.Errorf("failed to create fence object %s: %w", fenceObjectKey, err)
	}

	// Mark as fenced
	f.fenced.Store(true)

	logger.Ctx(ctx).Info("Successfully fenced segment",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.String("fenceObjectKey", fenceObjectKey),
		zap.Int64("logId", f.logId),
		zap.Int64("segmentId", f.segmentId),
		zap.Int64("lastEntryId", lastEntryId))

	return lastEntryId, nil
}

func (f *SegmentImpl) prepareMultiFragmentDataIfNecessary(toFlushData []*cache.BufferEntry, toFlushDataFirstEntryId int64) ([][]*cache.BufferEntry, []int64, []int64) {
	if len(toFlushData) == 0 {
		return nil, nil, nil
	}

	maxPartitionSize := f.syncPolicyConfig.MaxFlushSize

	// First pass: calculate partition boundaries without copying data
	type partitionRange struct {
		start int
		end   int
	}

	ranges := make([]partitionRange, 0)
	sizeList := make([]int64, 0)
	currentStart := 0
	currentSize := int64(0)

	for i, item := range toFlushData {
		entrySize := int64(len(item.Data))
		currentSize += entrySize
		// Check if adding this entry would exceed the max partition size
		if currentSize >= maxPartitionSize {
			// Close current partition
			ranges = append(ranges, partitionRange{start: currentStart, end: i + 1})
			sizeList = append(sizeList, currentSize)
			currentStart = i + 1
			currentSize = 0
		}
	}

	// Add the last partition
	if currentStart < len(toFlushData) && currentSize > 0 {
		ranges = append(ranges, partitionRange{start: currentStart, end: len(toFlushData)})
		sizeList = append(sizeList, currentSize) // Add the size for the last partition
	}

	// Second pass: create partitions using slice references (no copying)
	partitions := make([][]*cache.BufferEntry, len(ranges))
	partitionFirstEntryIds := make([]int64, len(ranges))

	offset := toFlushDataFirstEntryId
	for i, r := range ranges {
		// Use slice reference instead of copying
		partitions[i] = toFlushData[r.start:r.end]
		partitionFirstEntryIds[i] = offset
		offset += int64(r.end - r.start)
	}

	return partitions, partitionFirstEntryIds, sizeList
}

func (f *SegmentImpl) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	return nil, nil, nil, werr.ErrNotSupport.WithCauseErrMsg("not support SegmentImpl writer to merge currently")
}

func (f *SegmentImpl) Load(ctx context.Context) (int64, storage.Fragment, error) {
	return -1, nil, werr.ErrNotSupport.WithCauseErrMsg("not support SegmentImpl writer to load currently")
}

func (f *SegmentImpl) DeleteFragments(ctx context.Context, flag int) error {
	return werr.ErrNotSupport.WithCauseErrMsg("not support SegmentImpl writer to delete fragments currently")
}

var _ storage.Segment = (*ROSegmentImpl)(nil)

// ROSegmentImpl is used to read data from object storage as a logical segment file
type ROSegmentImpl struct {
	mu sync.RWMutex

	compactPolicyConfig *config.SegmentCompactionPolicy
	client              minioHandler.MinioHandler
	segmentPrefixKey    string // The prefix key for the segment to which this Segment belongs
	bucket              string // The bucket name

	logId           int64
	segmentId       int64
	fragments       []storage.Fragment // Segment cached fragments in order
	mergedFragments []storage.Fragment // Segment cached merged fragments in order

	// For fence state: true confirms it is fenced, while false requires verification by checking the storage for a fence flag object.
	fenced atomic.Bool
}

// NewROSegmentImpl is used to read only segment
func NewROSegmentImpl(ctx context.Context, logId int64, segId int64, segmentPrefixKey string, bucket string, objectCli minioHandler.MinioHandler, cfg *config.Configuration) storage.Segment {
	objFile := &ROSegmentImpl{
		logId:               logId,
		segmentId:           segId,
		compactPolicyConfig: &cfg.Woodpecker.Logstore.SegmentCompactionPolicy,
		client:              objectCli,
		segmentPrefixKey:    segmentPrefixKey,
		bucket:              bucket,
		fragments:           make([]storage.Fragment, 0),
	}
	objFile.fenced.Store(false)
	existsFragments, existsMergedFragments, err := objFile.prefetchAllFragmentInfosOnce(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("prefetch fragment infos failed when create Read-only SegmentImpl",
			zap.String("segmentPrefixKey", segmentPrefixKey),
			zap.Error(err))
	} else {
		logger.Ctx(ctx).Debug("prefetch all fragment infos finish",
			zap.String("segmentPrefixKey", segmentPrefixKey),
			zap.Int("fragments", existsFragments),
			zap.Int("mergedFragments", existsMergedFragments))
	}
	return objFile
}

func (f *ROSegmentImpl) GetId() int64 {
	return f.segmentId
}

func (f *ROSegmentImpl) AppendAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error) {
	return entryId, werr.ErrNotSupport.WithCauseErrMsg("read only SegmentImpl reader cannot support append")
}

// Deprecated: use AppendAsync instead
func (f *ROSegmentImpl) Append(ctx context.Context, data []byte) error {
	return werr.ErrNotSupport.WithCauseErrMsg("read only SegmentImpl reader cannot support append")
}

// get the fragment for the entryId
func (f *ROSegmentImpl) getFragment(ctx context.Context, entryId int64) (storage.Fragment, error) {
	logger.Ctx(ctx).Debug("get fragment for entryId", zap.Int64("entryId", entryId))

	// find from merged fragments first
	foundMergedFrag, err := f.findMergedFragment(entryId)
	if err != nil {
		return nil, err
	}
	if foundMergedFrag != nil {
		return foundMergedFrag, nil
	}

	// find from normal fragments
	foundFrag, err := f.findFragment(entryId)
	if err != nil {
		logger.Ctx(ctx).Warn("get fragment from cache failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if foundFrag != nil {
		logger.Ctx(ctx).Debug("get fragment from cache for entryId completed", zap.Int64("entryId", entryId), zap.Int64("fragmentId", foundFrag.GetFragmentId()))
		return foundFrag, nil
	}

	// try to fetch new fragments if exists
	existsNewFragment, fetchedLastFragment, err := f.prefetchFragmentInfos(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("prefetch fragment info failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if !existsNewFragment {
		// means get no fragment for this entryId
		return nil, nil
	}

	fetchedLastEntryId, err := legacy.GetLastEntryIdWithoutDataLoadedIfPossible(ctx, fetchedLastFragment)
	if err != nil {
		logger.Ctx(ctx).Warn("get fragment lastEntryId failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.String("fragmentKey", fetchedLastFragment.GetFragmentKey()), zap.Error(err))
		return nil, err
	}
	if entryId > fetchedLastEntryId {
		// fast return, no fragment for this entryId
		return nil, nil
	}
	if entryId == fetchedLastEntryId {
		// fast return this fragment
		return fetchedLastFragment, nil
	}

	// find again
	foundFrag, err = f.findFragment(entryId)
	if err != nil {
		return nil, err
	}
	if foundFrag != nil {
		logger.Ctx(ctx).Debug("get fragment from cache for entryId", zap.Int64("entryId", entryId), zap.Int64("fragmentId", foundFrag.GetFragmentId()))
		return foundFrag, nil
	}

	// means get no fragment for this entryId
	return nil, nil
}

// findFragment finds the exists cache fragments for the entryId
func (f *ROSegmentImpl) findFragment(entryId int64) (storage.Fragment, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return legacy.SearchFragment(entryId, f.fragments)
}

func (f *ROSegmentImpl) findMergedFragment(entryId int64) (storage.Fragment, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.mergedFragments) == 0 {
		return nil, nil
	}
	return legacy.SearchFragment(entryId, f.mergedFragments)
}

// objectExists checks if an object exists in the MinIO bucket
func (f *ROSegmentImpl) objectExists(ctx context.Context, objectKey string) (bool, error) {
	info, err := f.client.StatObject(ctx, f.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil && minioHandler.IsObjectNotExists(err) {
		return false, nil
	}
	if minioHandler.IsFencedObject(info) {
		// it means the object is fenced out, no more fragment data
		logger.Ctx(ctx).Debug("object is fenced out", zap.String("objectKey", objectKey))
		f.fenced.Store(true)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *ROSegmentImpl) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "NewReader")
	defer sp.End()
	opt.BatchSize = -1 // TODO currently only support auto batch size
	reader := NewEntryReader(opt, f)
	return reader, nil
}

// LastFragmentId returns the last fragmentId of the log file.
func (f *ROSegmentImpl) LastFragmentId() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.fragments) == 0 {
		return -1
	}
	return f.fragments[len(f.fragments)-1].GetFragmentId()
}

func (f *ROSegmentImpl) GetLastEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "GetLastEntryId")
	defer sp.End()
	// prefetch fragmentInfos if any new fragment created
	_, lastFragment, err := f.prefetchFragmentInfos(ctx)
	if err != nil {
		logger.Ctx(ctx).Debug("get last entryId failed when fetch the last fragment, treating as empty segment",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
		// For empty segments, return -1 as the last entry ID
		return -1, nil
	}

	// If no fragments exist, return -1
	if lastFragment == nil {
		logger.Ctx(ctx).Debug("no fragments exist, returning -1 as last entry ID",
			zap.String("segmentPrefixKey", f.segmentPrefixKey))
		return -1, nil
	}

	lastEntryId, err := legacy.GetLastEntryIdWithoutDataLoadedIfPossible(ctx, lastFragment)
	if err != nil {
		logger.Ctx(ctx).Warn("get last entryId failed",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
		return -1, err
	}
	logger.Ctx(ctx).Debug("get last entryId finish",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("lastFragId", lastFragment.GetFragmentId()),
		zap.Int64("lastEntryId", lastEntryId))
	return lastEntryId, nil
}

func (f *ROSegmentImpl) Sync(ctx context.Context) error {
	return werr.ErrNotSupport.WithCauseErrMsg("ROSegmentImpl not support sync")
}

func (f *ROSegmentImpl) Close(ctx context.Context) error {
	return nil
}

// Start by listing all once
func (f *ROSegmentImpl) prefetchAllFragmentInfosOnce(ctx context.Context) (int, int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "prefetchAllFragmentInfosOnce")
	defer sp.End()
	listPrefix := fmt.Sprintf("%s/", f.segmentPrefixKey)
	objectCh := f.client.ListObjects(ctx, f.bucket, listPrefix, false, minio.ListObjectsOptions{})
	existsFragments := make([]storage.Fragment, 0, 32)
	existsMergedFragments := make([]storage.Fragment, 0, 32)
	for objInfo := range objectCh {
		if objInfo.Err != nil {
			logger.Ctx(ctx).Warn("Error listing objects",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Error(objInfo.Err))
			return 0, 0, objInfo.Err
		}
		if !strings.HasSuffix(objInfo.Key, ".frag") {
			continue
		}

		if minioHandler.IsFencedObject(objInfo) {
			// it means the object is fenced out, no more fragment data
			logger.Ctx(ctx).Info("object is fenced out", zap.String("objectKey", objInfo.Key))
			f.fenced.Store(true)
			break
		}

		fragmentId, isMerged, parseErr := parseFragmentFilename(objInfo.Key)
		if parseErr != nil {
			logger.Ctx(ctx).Warn("Error parsing fragment filename",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key),
				zap.Error(parseErr))
			return 0, 0, parseErr
		}
		logger.Ctx(ctx).Info("Found fragment object",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.String("objectKey", objInfo.Key),
			zap.Int64("objectSize", objInfo.Size),
			zap.Int64("fragmentId", fragmentId),
			zap.Bool("isMerged", isMerged))

		frag := legacy.NewLegacyFragmentObject(ctx, f.client, f.bucket, f.logId, f.segmentId, fragmentId, objInfo.Key, nil, -1, false, true, false)
		if isMerged {
			existsMergedFragments = append(existsMergedFragments, frag)
		} else {
			existsFragments = append(existsFragments, frag)
		}
	}
	// ensure no hole in list
	sort.Slice(existsFragments, func(i, j int) bool {
		return existsFragments[i].GetFragmentId() < existsFragments[j].GetFragmentId()
	})
	existsFragmentExpectedFragId := int64(0)
	for i := 0; i < len(existsFragments); i++ {
		if existsFragments[i].GetFragmentId() != existsFragmentExpectedFragId {
			logger.Ctx(ctx).Debug("Found fragment hole",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("fragmentId", existsFragments[i].GetFragmentId()),
				zap.Int64("expectedFragmentId", existsFragmentExpectedFragId))
			existsFragments = existsFragments[:i]
			break
		}
		existsFragmentExpectedFragId += 1
	}

	// ensure no hole in list
	sort.Slice(existsMergedFragments, func(i, j int) bool {
		return existsMergedFragments[i].GetFragmentId() < existsMergedFragments[j].GetFragmentId()
	})
	existsMergedFragmentExpectedFragId := int64(0)
	for i := 0; i < len(existsMergedFragments); i++ {
		if existsMergedFragments[i].GetFragmentId() != existsMergedFragmentExpectedFragId {
			logger.Ctx(ctx).Debug("Found fragment hole",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("fragmentId", existsMergedFragments[i].GetFragmentId()),
				zap.Int64("expectedFragmentId", existsMergedFragmentExpectedFragId))
			existsMergedFragments = existsMergedFragments[:i]
			break
		}
		existsMergedFragmentExpectedFragId += 1
	}

	f.fragments = existsFragments
	f.mergedFragments = existsMergedFragments
	return len(existsFragments), len(existsMergedFragments), nil
}

// incrementally fetch new fragments as they come in
func (f *ROSegmentImpl) prefetchFragmentInfos(ctx context.Context) (bool, storage.Fragment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "prefetchFragmentInfos")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	var fetchedLastFragment storage.Fragment = nil

	fragId := int64(0)
	if len(f.fragments) > 0 {
		lastFrag := f.fragments[len(f.fragments)-1]
		fragId = int64(lastFrag.GetFragmentId()) + 1
		fetchedLastFragment = lastFrag
	}
	existsNewFragment := false
	for {
		fragKey := getFragmentObjectKey(f.segmentPrefixKey, fragId)

		// check if the fragment exists in object storage
		exists, err := f.objectExists(context.Background(), fragKey)
		if err != nil {
			// indicates that the prefetching of fragments has completed.
			//fmt.Println("object storage read fragment err: ", err)
			return existsNewFragment, nil, err
		}

		if exists {
			fragment := legacy.NewLegacyFragmentObject(ctx, f.client, f.bucket, f.logId, f.segmentId, fragId, fragKey, nil, -1, false, true, false)
			fetchedLastFragment = fragment
			f.fragments = append(f.fragments, fragment)
			existsNewFragment = true
			fragId++
			logger.Ctx(ctx).Info("prefetch fragment info", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("lastFragId", fragId-1))
		} else {
			// no more fragment exists, cause the id sequence is broken
			break
		}
	}
	logger.Ctx(ctx).Debug("prefetch fragment infos", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int("fragments", len(f.fragments)), zap.Int64("lastFragId", fragId-1))
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "prefetch_fragment_infos", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "prefetch_fragment_infos", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return existsNewFragment, fetchedLastFragment, nil
}

func (f *ROSegmentImpl) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Merge")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)

	fileMaxSize := f.compactPolicyConfig.MaxBytes
	singleFragmentMaxSize := int64(float64(fileMaxSize) * 0.6) // Merging large files is not very beneficial, TODO should be configurable
	mergedFrags := make([]storage.Fragment, 0)
	mergedFragId := int64(0)
	entryOffset := make([]int32, 0)
	fragmentIdOffset := make([]int32, 0)
	if len(f.fragments) == 0 {
		return mergedFrags, entryOffset, fragmentIdOffset, nil
	}

	totalMergeSize := int64(0)
	pendingMergeSize := int64(0)
	pendingMergeFrags := make([]storage.Fragment, 0)
	// load all fragment in memory
	for _, frag := range f.fragments {
		fragSize, loadFragSizeErr := frag.LoadSizeStateOnly(ctx)
		if loadFragSizeErr != nil {
			return nil, nil, nil, loadFragSizeErr
		}
		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += fragSize
		if pendingMergeSize >= fileMaxSize || fragSize >= singleFragmentMaxSize {
			// merge immediately
			mergedFrag, mergeErr := legacy.MergeFragmentsAndReleaseAfterCompletedPro(ctx, f.client, f.bucket, getMergedFragmentObjectKey(f.segmentPrefixKey, mergedFragId), mergedFragId, pendingMergeFrags, pendingMergeSize, true)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			mergedFragFirstEntryId, getFirstEntryIdErr := legacy.GetFirstEntryIdWithoutDataLoadedIfPossible(ctx, mergedFrag)
			if getFirstEntryIdErr != nil {
				return nil, nil, nil, getFirstEntryIdErr
			}
			entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].GetFragmentId()))
			pendingMergeFrags = make([]storage.Fragment, 0)
			totalMergeSize += pendingMergeSize
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := legacy.MergeFragmentsAndReleaseAfterCompletedPro(ctx, f.client, f.bucket, getMergedFragmentObjectKey(f.segmentPrefixKey, mergedFragId), mergedFragId, pendingMergeFrags, pendingMergeSize, true)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		mergedFragFirstEntryId, getFirstEntryIdErr := legacy.GetFirstEntryIdWithoutDataLoadedIfPossible(ctx, mergedFrag)
		if getFirstEntryIdErr != nil {
			return nil, nil, nil, getFirstEntryIdErr
		}
		entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].GetFragmentId()))
		pendingMergeFrags = make([]storage.Fragment, 0)
		totalMergeSize += pendingMergeSize
		pendingMergeSize = 0
	}

	lastMergedFragment := mergedFrags[len(mergedFrags)-1]
	lastMergedFragmentLastEntryId, err := legacy.GetLastEntryIdWithoutDataLoadedIfPossible(ctx, lastMergedFragment)
	if err != nil {
		return nil, nil, nil, err
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, "merge", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "merge", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	metrics.WpFileCompactLatency.WithLabelValues(logId).Observe(float64(time.Since(startTime).Milliseconds()))
	metrics.WpFileCompactBytesWritten.WithLabelValues(logId).Add(float64(totalMergeSize))
	logger.Ctx(ctx).Info("merge fragments finish", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int("mergedFrags", len(mergedFrags)), zap.Int64("lastMergedFragmentLastEntryId", lastMergedFragmentLastEntryId), zap.Int("fragments", len(f.fragments)), zap.Int64("totalMergeSize", totalMergeSize), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

// Load here only need to load the last fragment data, only used in recover to load fragments info
func (f *ROSegmentImpl) Load(ctx context.Context) (int64, storage.Fragment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Load")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	if len(f.fragments) == 0 {
		return 0, nil, nil
	}
	totalSize := int64(0)
	for _, frag := range f.fragments {
		// load from object storage
		objSize, loadFragSizeErr := frag.LoadSizeStateOnly(ctx)
		if loadFragSizeErr != nil {
			return 0, nil, loadFragSizeErr
		}
		totalSize += objSize
	}

	lastFragment := f.fragments[len(f.fragments)-1]
	lastEntryId, err := legacy.GetLastEntryIdWithoutDataLoadedIfPossible(ctx, lastFragment)
	if err != nil {
		return -1, nil, err
	}
	fragId := lastFragment.GetFragmentId()
	logger.Ctx(ctx).Info("Load fragments", zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int("fragments", len(f.fragments)),
		zap.Int64("totalSize", totalSize),
		zap.Int64("lastFragId", fragId),
		zap.Int64("lastEntryId", lastEntryId),
	)
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "load", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "load", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return totalSize, lastFragment, nil
}

func (f *ROSegmentImpl) DeleteFragments(ctx context.Context, flag int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFragments")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)

	logger.Ctx(ctx).Info("Starting to delete fragments",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int("flag", flag))

	// List all fragment objects in object storage
	listPrefix := fmt.Sprintf("%s/", f.segmentPrefixKey)
	objectCh := f.client.ListObjects(ctx, f.bucket, listPrefix, false, minio.ListObjectsOptions{})

	var deleteErrors []error
	var normalFragmentCount int = 0
	var mergedFragmentCount int = 0

	// Iterate through all found objects
	for objInfo := range objectCh {
		if objInfo.Err != nil {
			logger.Ctx(ctx).Warn("Error listing objects",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Error(objInfo.Err))
			deleteErrors = append(deleteErrors, objInfo.Err)
			continue
		}

		// Only process fragment files
		if !strings.HasSuffix(objInfo.Key, ".frag") {
			continue
		}

		// Delete object
		err := f.client.RemoveObject(ctx, f.bucket, objInfo.Key, minio.RemoveObjectOptions{})
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to delete fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key),
				zap.Error(err))
			deleteErrors = append(deleteErrors, err)
			continue
		}

		// Count deleted fragment types
		if strings.Contains(objInfo.Key, "/m_") {
			logger.Ctx(ctx).Info("Successfully deleted merged fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key))
			mergedFragmentCount++
		} else {
			logger.Ctx(ctx).Info("Successfully deleted normal fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key))
			normalFragmentCount++
		}
	}

	// Clean up internal state
	f.fragments = make([]storage.Fragment, 0)
	f.mergedFragments = make([]storage.Fragment, 0)

	// Update metrics
	if len(deleteErrors) > 0 {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_fragments", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_fragments", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_fragments", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	}

	logger.Ctx(ctx).Info("Completed fragment deletion",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int("normalFragmentCount", normalFragmentCount),
		zap.Int("mergedFragmentCount", mergedFragmentCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return fmt.Errorf("failed to delete %d fragment objects", len(deleteErrors))
	}

	return nil
}

func (f *ROSegmentImpl) IsFenced(ctx context.Context) (bool, error) {
	// If already fenced in memory, return true immediately
	if f.fenced.Load() {
		return true, nil
	}

	// Try to prefetch fragment infos to check for fence objects
	// This might fail for empty segments, which is normal
	_, _, err := f.prefetchFragmentInfos(ctx)
	if err != nil {
		// For empty segments or when objects don't exist, this is not necessarily an error
		// Just return the current fence state
		logger.Ctx(ctx).Debug("Failed to prefetch fragment infos for fence check, treating as not fenced",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
		return f.fenced.Load(), nil
	}

	return f.fenced.Load(), nil
}

func (f *ROSegmentImpl) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Fence")
	defer sp.End()

	// If already fenced, return idempotently
	if f.fenced.Load() {
		logger.Ctx(ctx).Debug("ROSegment already fenced, returning idempotently",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logId", f.logId),
			zap.Int64("segmentId", f.segmentId))
		// Get last entry ID for read-only segment
		lastEntryId, err := f.GetLastEntryId(ctx)
		if err != nil {
			return -1, err
		}
		return lastEntryId, nil
	}

	var lastEntryId int64
	var fenceObjectKey string

	// Use retry.Do to handle the fence object creation with retries
	err := retry.Do(ctx,
		func() error {
			// Fetch fragments to get the latest fragment information
			// For empty segments, this might fail, which is normal
			_, lastFragment, prefetchErr := f.prefetchFragmentInfos(ctx)
			if prefetchErr != nil {
				logger.Ctx(ctx).Debug("Failed to prefetch fragment infos during fence, treating as empty segment",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Error(prefetchErr))
				// Treat as empty segment, lastFragment will be nil
				lastFragment = nil
			}

			// Calculate fence fragment ID: last fragment ID + 1
			var fenceFragmentId int64
			if lastFragment != nil {
				fenceFragmentId = lastFragment.GetFragmentId() + 1
			} else {
				// No fragments exist, start with fragment ID 0
				fenceFragmentId = 0
			}

			fenceObjectKey = getFragmentObjectKey(f.segmentPrefixKey, fenceFragmentId)

			// Try to create fence object
			_, putErr := f.client.PutFencedObject(ctx, f.bucket, fenceObjectKey)
			if putErr != nil {
				if werr.ErrFragmentAlreadyExists.Is(putErr) {
					// Fragment already exists, this might be normal during concurrent operations
					logger.Ctx(ctx).Debug("Fence object already exists, retrying with updated fragment info",
						zap.String("segmentPrefixKey", f.segmentPrefixKey),
						zap.String("fenceObjectKey", fenceObjectKey),
						zap.Int64("fenceFragmentId", fenceFragmentId))
					return putErr // This will trigger a retry
				}
				// Other errors are not retryable
				logger.Ctx(ctx).Error("Failed to create fence object",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.String("fenceObjectKey", fenceObjectKey),
					zap.Error(putErr))
				return putErr
			}

			// Successfully created fence object, get last entry ID
			if lastFragment != nil {
				var getLastEntryIdErr error
				lastEntryId, getLastEntryIdErr = legacy.GetLastEntryIdWithoutDataLoadedIfPossible(ctx, lastFragment)
				if getLastEntryIdErr != nil {
					logger.Ctx(ctx).Warn("Failed to get last entry ID from last fragment",
						zap.String("segmentPrefixKey", f.segmentPrefixKey),
						zap.String("lastFragmentKey", lastFragment.GetFragmentKey()),
						zap.Error(getLastEntryIdErr))
					return getLastEntryIdErr
				}
			} else {
				// No fragments exist, last entry ID is -1
				lastEntryId = -1
			}

			logger.Ctx(ctx).Info("Successfully created fence object for ROSegment",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("fenceObjectKey", fenceObjectKey),
				zap.Int64("fenceFragmentId", fenceFragmentId),
				zap.Int64("lastEntryId", lastEntryId))
			return nil
		},
		retry.Attempts(5),                 // Retry up to 5 times
		retry.Sleep(500*time.Millisecond), // Initial sleep between retries (longer for object storage)
		retry.MaxSleepTime(2*time.Second), // Max sleep time between retries
		retry.RetryErr(func(err error) bool {
			// Only retry on fragment already exists error
			return werr.ErrFragmentAlreadyExists.Is(err)
		}),
	)

	if err != nil {
		logger.Ctx(ctx).Error("Failed to create fence object after retries",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.String("fenceObjectKey", fenceObjectKey),
			zap.Error(err))
		return -1, fmt.Errorf("failed to create fence object %s: %w", fenceObjectKey, err)
	}

	// Mark as fenced
	f.fenced.Store(true)

	logger.Ctx(ctx).Info("Successfully fenced ROSegment",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.String("fenceObjectKey", fenceObjectKey),
		zap.Int64("logId", f.logId),
		zap.Int64("segmentId", f.segmentId),
		zap.Int64("lastEntryId", lastEntryId))

	return lastEntryId, nil
}

// utils for fragment object key
func getFragmentObjectKey(segmentPrefixKey string, fragmentId int64) string {
	return fmt.Sprintf("%s/%d.frag", segmentPrefixKey, fragmentId)
}

// utils for merged fragment object key
func getMergedFragmentObjectKey(segmentPrefixKey string, mergedFragmentId int64) string {
	return fmt.Sprintf("%s/m_%d.frag", segmentPrefixKey, mergedFragmentId)
}

// utils to parse object key
func parseFragmentFilename(key string) (id int64, isMerge bool, err error) {
	filename := filepath.Base(key)
	name := strings.TrimSuffix(filename, ".frag")
	if strings.HasPrefix(name, "m_") {
		isMerge = true
		idStr := strings.TrimPrefix(name, "m_")
		id, err = strconv.ParseInt(idStr, 10, 64)
		return id, isMerge, err
	}
	isMerge = false
	id, err = strconv.ParseInt(name, 10, 64)
	return id, isMerge, err
}

// createSegmentLock creates a lock object in object storage for segment exclusivity
func (f *SegmentImpl) createSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "createSegmentLock")
	defer sp.End()

	// Create lock object key
	f.lockObjectKey = fmt.Sprintf("%s/segment_%d_%d.lock", f.segmentPrefixKey, f.logId, f.segmentId)

	// Create lock object with segment information
	lockInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nhostname=%s\n",
		f.logId, f.segmentId, os.Getpid(), time.Now().Unix(), getHostname())

	// Use PutObjectIfNoneMatch to atomically create lock object
	_, err := f.client.PutObjectIfNoneMatch(ctx, f.bucket, f.lockObjectKey,
		strings.NewReader(lockInfo), int64(len(lockInfo)))
	if err != nil {
		if werr.ErrFragmentAlreadyExists.Is(err) {
			logger.Ctx(ctx).Error("Lock object already exists - segment is already locked by another process",
				zap.String("lockObjectKey", f.lockObjectKey))
			return fmt.Errorf("segment is already locked by another process: %s", f.lockObjectKey)
		}
		logger.Ctx(ctx).Error("Failed to create lock object",
			zap.String("lockObjectKey", f.lockObjectKey),
			zap.Error(err))
		return fmt.Errorf("failed to create lock object %s: %w", f.lockObjectKey, err)
	}

	logger.Ctx(ctx).Info("Successfully created segment lock object",
		zap.String("lockObjectKey", f.lockObjectKey),
		zap.Int64("logId", f.logId),
		zap.Int64("segmentId", f.segmentId))

	return nil
}

// releaseSegmentLock removes the lock object from object storage
func (f *SegmentImpl) releaseSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "releaseSegmentLock")
	defer sp.End()

	if f.lockObjectKey == "" {
		logger.Ctx(ctx).Debug("No lock object to release")
		return nil
	}

	// Remove the lock object
	err := f.client.RemoveObject(ctx, f.bucket, f.lockObjectKey, minio.RemoveObjectOptions{})
	if err != nil {
		// Check if object doesn't exist (already removed)
		if minioHandler.IsObjectNotExists(err) {
			logger.Ctx(ctx).Info("Lock object already removed",
				zap.String("lockObjectKey", f.lockObjectKey))
		} else {
			logger.Ctx(ctx).Warn("Failed to remove lock object",
				zap.String("lockObjectKey", f.lockObjectKey),
				zap.Error(err))
			return err
		}
	} else {
		logger.Ctx(ctx).Info("Successfully removed segment lock object",
			zap.String("lockObjectKey", f.lockObjectKey),
			zap.Int64("logId", f.logId),
			zap.Int64("segmentId", f.segmentId))
	}

	f.lockObjectKey = ""
	return nil
}

// getHostname returns the hostname for lock identification
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}
