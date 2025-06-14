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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
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
		client:           objectCli,
		segmentPrefixKey: segmentPrefixKey,
		bucket:           bucket,

		maxBufferSize:    syncPolicyConfig.MaxBytes,
		maxBufferEntries: maxBufferEntries,
		maxIntervalMs:    syncPolicyConfig.MaxInterval,
		syncPolicyConfig: syncPolicyConfig,
		fileClose:        make(chan struct{}, 1),
	}
	objFile.buffer.Store(newBuffer)
	objFile.firstEntryID.Store(-1)
	objFile.lastEntryID.Store(-1)
	objFile.lastFragmentID.Store(-1)
	objFile.closed.Store(false)
	go objFile.run()
	return objFile
}

// Like OS file fsync dirty pageCache periodically, objectStoreFile will sync buffer to object storage periodically
func (f *SegmentImpl) run() {
	// time ticker
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()
	f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	logIdStr := fmt.Sprintf("%d", f.logId)
	segIdStr := fmt.Sprintf("%d", f.segmentId)
	metrics.WpFileWriters.WithLabelValues(logIdStr, segIdStr).Inc()
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
			metrics.WpFileWriters.WithLabelValues(logIdStr, segIdStr).Dec()
			return
		}
	}
}

func (f *SegmentImpl) GetId() int64 {
	return f.segmentId
}

func (f *SegmentImpl) AppendAsync(ctx context.Context, entryId int64, data []byte, resultCh chan<- int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "AppendAsync")
	defer sp.End()
	if f.closed.Load() {
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, file closed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrLogFileClosed
	}
	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))

	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segmentId := fmt.Sprintf("%d", f.segmentId)

	// trigger sync by max buffer entries num
	currentBuffer := f.buffer.Load()
	pendingAppendId := currentBuffer.ExpectedNextEntryId.Load() + 1
	if pendingAppendId >= int64(currentBuffer.FirstEntryId+currentBuffer.MaxEntries) {
		logger.Ctx(ctx).Debug("buffer full, trigger flush",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("pendingAppendId", pendingAppendId),
			zap.Int64("bufferFirstId", currentBuffer.FirstEntryId),
			zap.Int64("bufferLastId", currentBuffer.FirstEntryId+currentBuffer.MaxEntries))
		err := f.Sync(ctx)
		if err != nil {
			// sync does not success
			metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "error").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			return entryId, err
		}
	}

	f.mu.Lock()
	if entryId <= f.lastEntryID.Load() {
		// If entryId is less than or equal to lastEntryID, it indicates that the entry has already been written to object storage. Return immediately.
		logger.Ctx(ctx).Debug("AppendAsync: skipping write, entryId is not greater than lastEntryID, already stored", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int64("lastEntryID", f.lastEntryID.Load()))
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, entryId, resultCh, entryId)
		f.mu.Unlock()
		return entryId, nil
	}

	currentBuffer = f.buffer.Load()
	// write buffer with notification channel
	id, err := currentBuffer.WriteEntryWithNotify(entryId, data, resultCh)
	if err != nil {
		// write to buffer failed
		f.mu.Unlock()
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return id, err
	}
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	f.mu.Unlock()

	// trigger sync by max buffer entries bytes size
	dataSize := currentBuffer.DataSize.Load()
	if dataSize >= f.maxBufferSize {
		logger.Ctx(ctx).Debug("reach max buffer size, trigger flush", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("bufferSize", dataSize), zap.Int64("maxSize", f.maxBufferSize))
		syncErr := f.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger flush failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("bufferSize", dataSize), zap.Int64("maxSize", f.maxBufferSize), zap.Error(syncErr))
		}
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "success").Observe(float64(time.Since(startTime).Milliseconds()))

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

// flushResult is the result of flush operation
type flushResult struct {
	target *FragmentObject
	err    error
}

func (f *SegmentImpl) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Sync")
	defer sp.End()
	// Implement sync logic, e.g., flush to persistent storage
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	}()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segmentId := fmt.Sprintf("%d", f.segmentId)

	currentBuffer := f.buffer.Load()

	entryCount := len(currentBuffer.Entries)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()

	// get flush point to flush
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	toFlushData, err := currentBuffer.ReadEntriesRange(currentBuffer.FirstEntryId, expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRangeData failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}
	toFlushDataFirstEntryId := currentBuffer.FirstEntryId

	fragmentDataList, fragmentFirstEntryIdList := f.prepareMultiFragmentDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	concurrentCh := make(chan int, f.syncPolicyConfig.MaxFlushThreads)
	flushResultCh := make(chan *flushResult, len(fragmentDataList))
	var concurrentWg sync.WaitGroup
	logger.Ctx(ctx).Debug("get flush partitions finish", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int("fragments", len(fragmentDataList)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	lastFragmentId := f.LastFragmentId()
	for i, fragmentData := range fragmentDataList {
		concurrentWg.Add(1)
		go func() {
			concurrentCh <- i                       // take one flush goroutine to start
			fragId := lastFragmentId + 1 + int64(i) // fragment id
			logger.Ctx(ctx).Debug("start flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId))
			key := getFragmentObjectKey(f.segmentPrefixKey, fragId)
			part := fragmentData
			fragment := NewFragmentObject(ctx, f.client, f.bucket, f.logId, f.segmentId, fragId, key, part, fragmentFirstEntryIdList[i], true, false, true)
			flushErr := retry.Do(ctx,
				func() error {
					return fragment.Flush(ctx)
				},
				retry.Attempts(uint(f.syncPolicyConfig.MaxFlushRetries)),
				retry.Sleep(time.Duration(f.syncPolicyConfig.RetryInterval)*time.Millisecond),
			)
			if flushErr != nil {
				logger.Ctx(ctx).Warn("flush part of buffer as fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Error(flushErr))
			}
			// release fragment immediately
			releaseErr := fragment.Release(ctx)
			if releaseErr != nil {
				logger.Ctx(ctx).Warn("release fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId), zap.Error(releaseErr))
			}
			flushResultCh <- &flushResult{
				target: fragment,
				err:    flushErr,
			}
			concurrentWg.Done() // finish a flush goroutine
			<-concurrentCh      // release a flush goroutine
			logger.Ctx(ctx).Debug("complete flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("fragId", fragId))
		}()
	}

	logger.Ctx(ctx).Debug("wait for all parts of buffer to be flushed", zap.String("segmentPrefixKey", f.segmentPrefixKey))
	concurrentWg.Wait() // Wait for all tasks to complete
	close(flushResultCh)
	logger.Ctx(ctx).Debug("all parts of buffer have been flushed", zap.String("segmentPrefixKey", f.segmentPrefixKey))
	resultFrags := make([]*flushResult, 0)
	for r := range flushResultCh {
		resultFrags = append(resultFrags, r)
	}
	sort.Slice(resultFrags, func(i, j int) bool {
		return resultFrags[i].target.GetFragmentId() < resultFrags[j].target.GetFragmentId()
	})
	successFrags := make([]*FragmentObject, 0)
	flushedLastEntryId := int64(-1) // last entry id of the last fragment that was successfully flushed in sequence
	flushedLastFragmentId := int64(-1)
	for _, r := range resultFrags {
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
		}
	}
	// Update the first and last entry IDs after successful flush
	if flushedLastEntryId >= 0 {
		f.lastEntryID.Store(flushedLastEntryId)
		f.lastFragmentID.Store(flushedLastFragmentId)
		if f.firstEntryID.Load() == -1 {
			// Initialize firstEntryId on first successful flush
			// This should always be 0 for the initial flush
			f.firstEntryID.Store(toFlushDataFirstEntryId)
		}
	}

	// callback to notify all waiting append request channels
	if len(successFrags) == len(resultFrags) {

		restData, err := currentBuffer.ReadEntriesToLast(expectedNextEntryId)
		if err != nil {
			logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLastData failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Error(err))
			return err
		}
		if len(restData) == 0 {
			fmt.Sprintf("")
		}
		restDataFirstEntryId := expectedNextEntryId
		newBuffer := cache.NewSequentialBufferWithData(f.logId, f.segmentId, restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries), restData)
		f.buffer.Store(newBuffer)

		// Notify all successfully flushed entries
		currentBuffer.NotifyEntriesInRange(ctx, toFlushDataFirstEntryId, expectedNextEntryId, flushedLastEntryId)

		logger.Ctx(ctx).Debug("Sync to object storage success",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int("fragments", len(fragmentDataList)),
			zap.Int64("successFirstEntryId", toFlushDataFirstEntryId),
			zap.Int64("restDataFirstEntryId", restDataFirstEntryId),
			zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)),
			zap.String("newBufInst", fmt.Sprintf("%p", newBuffer)))
	} else if len(successFrags) > 0 {
		lastSuccessFrag := successFrags[len(successFrags)-1]
		last, _ := lastSuccessFrag.GetLastEntryId(ctx)
		restDataFirstEntryId := last + 1

		// Notify successful entries with success and failed entries with error
		currentBuffer.NotifyEntriesInRange(ctx, toFlushDataFirstEntryId, restDataFirstEntryId, last)
		currentBuffer.NotifyEntriesInRange(ctx, restDataFirstEntryId, expectedNextEntryId, -1)

		// new a empty buffer
		newBuffer := cache.NewSequentialBuffer(f.logId, f.segmentId, restDataFirstEntryId, f.maxBufferEntries)
		f.buffer.Store(newBuffer)
		logger.Ctx(ctx).Debug("Sync to object storage partial success",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int("fragments", len(fragmentDataList)),
			zap.Int64("successFirstEntryId", toFlushDataFirstEntryId),
			zap.Int64("restDataFirstEntryId", restDataFirstEntryId),
			zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)),
			zap.String("newBufInst", fmt.Sprintf("%p", newBuffer)))
	} else {
		// no flush success, notify all entries with error
		currentBuffer.NotifyAllPendingEntries(ctx, -1)
		// reset buffer as empty
		currentBuffer.Reset(ctx)
		logger.Ctx(ctx).Debug("Sync to object storage all failed,reset buffer",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int("fragments", len(fragmentDataList)),
			zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "sync", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "sync", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (f *SegmentImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Close")
	defer sp.End()
	if !f.closed.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return nil
	}
	logger.Ctx(ctx).Info("run: received close signal,trigger sync before close ", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
	err := f.Sync(context.Background())
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
	}
	// close file
	f.closeOnce.Do(func() {
		f.fileClose <- struct{}{}
		close(f.fileClose)
	})
	return nil
}

func (f *SegmentImpl) prepareMultiFragmentDataIfNecessary(toFlushData []*cache.BufferEntry, toFlushDataFirstEntryId int64) ([][]*cache.BufferEntry, []int64) {
	if len(toFlushData) == 0 {
		return nil, nil
	}

	maxPartitionSize := f.syncPolicyConfig.MaxFlushSize

	// First pass: calculate partition boundaries without copying data
	type partitionRange struct {
		start int
		end   int
	}

	var ranges []partitionRange
	currentStart := 0
	currentSize := int64(0)

	for i, item := range toFlushData {
		entrySize := int64(len(item.Data))

		// Check if adding this entry would exceed the max partition size
		if currentSize+entrySize > maxPartitionSize && currentSize > 0 {
			// Close current partition
			ranges = append(ranges, partitionRange{start: currentStart, end: i})
			currentStart = i
			currentSize = 0
		}

		currentSize += entrySize
	}

	// Add the last partition
	if currentStart < len(toFlushData) {
		ranges = append(ranges, partitionRange{start: currentStart, end: len(toFlushData)})
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

	return partitions, partitionFirstEntryIds
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
	fragments       []*FragmentObject // Segment cached fragments in order
	mergedFragments []*FragmentObject // Segment cached merged fragments in order
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
		fragments:           make([]*FragmentObject, 0),
	}
	existsFragments, existsMergedFragments, err := objFile.prefetchAllFragmentInfosOnce(context.TODO())
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

func (f *ROSegmentImpl) AppendAsync(ctx context.Context, entryId int64, data []byte, resultCh chan<- int64) (int64, error) {
	return entryId, werr.ErrNotSupport.WithCauseErrMsg("read only SegmentImpl reader cannot support append")
}

// Deprecated: use AppendAsync instead
func (f *ROSegmentImpl) Append(ctx context.Context, data []byte) error {
	return werr.ErrNotSupport.WithCauseErrMsg("read only SegmentImpl reader cannot support append")
}

// get the fragment for the entryId
func (f *ROSegmentImpl) getFragment(ctx context.Context, entryId int64) (*FragmentObject, error) {
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
	existsNewFragment, fetchedLastFragment, err := f.prefetchFragmentInfos(context.TODO())
	if err != nil {
		logger.Ctx(ctx).Warn("prefetch fragment info failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if !existsNewFragment {
		// means get no fragment for this entryId
		return nil, nil
	}

	fetchedLastEntryId, err := getLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), fetchedLastFragment)
	if err != nil {
		logger.Ctx(ctx).Warn("get fragment lastEntryId failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.String("fragmentKey", fetchedLastFragment.fragmentKey), zap.Error(err))
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
func (f *ROSegmentImpl) findFragment(entryId int64) (*FragmentObject, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return searchFragment(entryId, f.fragments)
}

func (f *ROSegmentImpl) findMergedFragment(entryId int64) (*FragmentObject, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.mergedFragments) == 0 {
		return nil, nil
	}
	return searchFragment(entryId, f.mergedFragments)
}

func searchFragment(entryId int64, list []*FragmentObject) (*FragmentObject, error) {
	low, high := 0, len(list)-1
	var candidate *FragmentObject

	for low <= high {
		mid := (low + high) / 2
		frag := list[mid]

		first, err := getFirstEntryIdWithoutDataLoadedIfPossible(context.TODO(), frag)
		if err != nil {
			return nil, err
		}

		if first > entryId {
			high = mid - 1
		} else {
			last, err := getLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), frag)
			if err != nil {
				return nil, err
			}
			if last >= entryId {
				candidate = frag
				return candidate, nil
			} else {
				low = mid + 1
			}
		}
	}
	return candidate, nil
}

// objectExists checks if an object exists in the MinIO bucket
func (f *ROSegmentImpl) objectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := f.client.StatObject(ctx, f.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *ROSegmentImpl) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "NewReader")
	defer sp.End()
	reader := NewLogFileReader(opt, f)
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
	_, lastFragment, err := f.prefetchFragmentInfos(context.TODO())
	if err != nil {
		logger.Ctx(ctx).Warn("get last entryId failed when fetch the last fragment",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Error(err))
		return -1, err
	}
	lastEntryId, err := getLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), lastFragment)
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
	existsFragments := make([]*FragmentObject, 0, 32)
	existsMergedFragments := make([]*FragmentObject, 0, 32)
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
		fragmentId, isMerged, parseErr := parseFragmentFilename(objInfo.Key)
		if parseErr != nil {
			logger.Ctx(ctx).Warn("Error parsing fragment filename",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key),
				zap.Error(parseErr))
			return 0, 0, parseErr
		}
		logger.Ctx(ctx).Debug("Found fragment object",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.String("objectKey", objInfo.Key),
			zap.Int64("objectSize", objInfo.Size),
			zap.Int64("fragmentId", fragmentId),
			zap.Bool("isMerged", isMerged))

		frag := NewFragmentObject(ctx, f.client, f.bucket, f.logId, f.segmentId, fragmentId, objInfo.Key, nil, -1, false, true, false)
		if isMerged {
			existsMergedFragments = append(existsMergedFragments, frag)
		} else {
			existsFragments = append(existsFragments, frag)
		}
	}
	// ensure no hole in list
	sort.Slice(existsFragments, func(i, j int) bool {
		return existsFragments[i].fragmentId < existsFragments[j].fragmentId
	})
	existsFragmentExpectedFragId := int64(0)
	for i := 0; i < len(existsFragments); i++ {
		if existsFragments[i].fragmentId != existsFragmentExpectedFragId {
			logger.Ctx(ctx).Debug("Found fragment hole",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("fragmentId", existsFragments[i].fragmentId),
				zap.Int64("expectedFragmentId", existsFragmentExpectedFragId))
			existsFragments = existsFragments[:i]
			break
		}
		existsFragmentExpectedFragId += 1
	}

	// ensure no hole in list
	sort.Slice(existsMergedFragments, func(i, j int) bool {
		return existsMergedFragments[i].fragmentId < existsMergedFragments[j].fragmentId
	})
	existsMergedFragmentExpectedFragId := int64(0)
	for i := 0; i < len(existsMergedFragments); i++ {
		if existsMergedFragments[i].fragmentId != existsMergedFragmentExpectedFragId {
			logger.Ctx(ctx).Debug("Found fragment hole",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("fragmentId", existsMergedFragments[i].fragmentId),
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
func (f *ROSegmentImpl) prefetchFragmentInfos(ctx context.Context) (bool, *FragmentObject, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "prefetchFragmentInfos")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segmentId := fmt.Sprintf("%d", f.segmentId)
	var fetchedLastFragment *FragmentObject = nil

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
			fragment := NewFragmentObject(ctx, f.client, f.bucket, f.logId, f.segmentId, fragId, fragKey, nil, -1, false, true, false)
			fetchedLastFragment = fragment
			f.fragments = append(f.fragments, fragment)
			existsNewFragment = true
			fragId++
			logger.Ctx(ctx).Debug("prefetch fragment info", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("lastFragId", fragId-1))
		} else {
			// no more fragment exists, cause the id sequence is broken
			break
		}
	}
	logger.Ctx(ctx).Debug("prefetch fragment infos", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int("fragments", len(f.fragments)), zap.Int64("lastFragId", fragId-1))
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "prefetch_fragment_infos", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "prefetch_fragment_infos", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return existsNewFragment, fetchedLastFragment, nil
}

func (f *ROSegmentImpl) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Merge")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segmentId := fmt.Sprintf("%d", f.segmentId)

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
	pendingMergeFrags := make([]*FragmentObject, 0)
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
			mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompletedPro(ctx, getMergedFragmentObjectKey(f.segmentPrefixKey, mergedFragId), mergedFragId, pendingMergeFrags, pendingMergeSize, true)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			mergedFragFirstEntryId, getFirstEntryIdErr := getFirstEntryIdWithoutDataLoadedIfPossible(ctx, mergedFrag)
			if getFirstEntryIdErr != nil {
				return nil, nil, nil, getFirstEntryIdErr
			}
			entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].GetFragmentId()))
			pendingMergeFrags = make([]*FragmentObject, 0)
			totalMergeSize += pendingMergeSize
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompletedPro(ctx, getMergedFragmentObjectKey(f.segmentPrefixKey, mergedFragId), mergedFragId, pendingMergeFrags, pendingMergeSize, true)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		mergedFragFirstEntryId, getFirstEntryIdErr := getFirstEntryIdWithoutDataLoadedIfPossible(ctx, mergedFrag)
		if getFirstEntryIdErr != nil {
			return nil, nil, nil, getFirstEntryIdErr
		}
		entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].GetFragmentId()))
		pendingMergeFrags = make([]*FragmentObject, 0)
		totalMergeSize += pendingMergeSize
		pendingMergeSize = 0
	}

	lastMergedFragment := mergedFrags[len(mergedFrags)-1]
	lastMergedFragmentLastEntryId, err := getLastEntryIdWithoutDataLoadedIfPossible(ctx, lastMergedFragment.(*FragmentObject))
	if err != nil {
		return nil, nil, nil, err
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "merge", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "merge", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	metrics.WpFileCompactLatency.WithLabelValues(logId, segmentId).Observe(float64(time.Since(startTime).Milliseconds()))
	metrics.WpFileCompactBytesWritten.WithLabelValues(logId, segmentId).Add(float64(totalMergeSize))
	logger.Ctx(ctx).Info("merge fragments finish", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int("mergedFrags", len(mergedFrags)), zap.Int64("lastMergedFragmentLastEntryId", lastMergedFragmentLastEntryId), zap.Int("fragments", len(f.fragments)), zap.Int64("totalMergeSize", totalMergeSize), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

func mergeFragmentsAndReleaseAfterCompletedPro(ctx context.Context, mergedFragKey string, mergeFragId int64, fragments []*FragmentObject, pendingMergeSize int64, releaseImmediately bool) (*FragmentObject, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "mergeFragmentsAndReleaseAfterCompleted")
	defer sp.End()
	// Check args
	if len(fragments) == 0 {
		return nil, errors.New("no fragments to merge")
	}

	// Fast merge by rename
	if len(fragments) == 1 {
		// no need to merge here, just rename
		return fastMergeSingleFragment(ctx, mergedFragKey, mergeFragId, fragments[0])
	}

	// Merge them one by one to reduce memory usage
	startTime := time.Now()
	dataBuff := make([]byte, 0, pendingMergeSize)
	indexBuff := make([]byte, 0, 1024)

	mergeTarget := &FragmentObject{
		client:       fragments[0].client,
		bucket:       fragments[0].bucket,
		fragmentId:   mergeFragId,
		fragmentKey:  mergedFragKey,
		entriesData:  dataBuff,
		indexes:      indexBuff,
		firstEntryId: -1,
		lastEntryId:  -1,
		dataUploaded: false,
		dataLoaded:   false,
		infoFetched:  true,
	}
	expectedEntryId := int64(-1)
	fragIds := make([]int64, 0)
	for _, candidateFrag := range fragments {
		fragFirstEntryId, err := getFirstEntryIdWithoutDataLoadedIfPossible(ctx, candidateFrag)
		if err != nil {
			return nil, err
		}
		fragLastEntryId, err := getLastEntryIdWithoutDataLoadedIfPossible(ctx, candidateFrag)
		if err != nil {
			return nil, err
		}
		// check the order of entries
		if expectedEntryId == -1 {
			// the first segment
			mergeTarget.firstEntryId = fragFirstEntryId
			expectedEntryId = fragLastEntryId + 1
		} else {
			if expectedEntryId != fragFirstEntryId {
				logger.Ctx(ctx).Warn("fragments are not in order", zap.String("fragmentKey", candidateFrag.fragmentKey), zap.Int64("expectedEntryId", expectedEntryId), zap.Int64("fragFirstEntryId", fragFirstEntryId))
				return nil, errors.New("fragments are not in order")
			}
			expectedEntryId = fragLastEntryId + 1
			mergeTarget.lastEntryId = fragLastEntryId
		}
		// load candidate fragment data
		loadCandidateFragmentDataErr := candidateFrag.Load(ctx)
		if loadCandidateFragmentDataErr != nil {
			logger.Ctx(ctx).Warn("failed to load fragment", zap.String("fragmentKey", candidateFrag.fragmentKey), zap.Error(loadCandidateFragmentDataErr))
			return nil, loadCandidateFragmentDataErr
		}
		// append fragment data to merge target
		baseOffset := len(mergeTarget.entriesData)
		mergeOneFragmentErr := candidateFrag.AppendToMergeTarget(ctx, mergeTarget, int64(baseOffset))
		candidateFrag.Release(ctx) // release candidate fragment data immediately
		if mergeOneFragmentErr != nil {
			logger.Ctx(ctx).Warn("failed to merge fragment", zap.String("fragmentKey", candidateFrag.fragmentKey), zap.Error(mergeOneFragmentErr))
			return nil, mergeOneFragmentErr
		}
		fragIds = append(fragIds, candidateFrag.GetFragmentId())
	}

	// set data cache ready
	mergeTarget.dataLoaded = true
	mergeTarget.size = int64(len(mergeTarget.entriesData) + len(mergeTarget.indexes))
	mergeTarget.rawBufSize = int64(cap(mergeTarget.entriesData) + cap(mergeTarget.indexes))

	if mergeTarget.firstEntryId == -1 {
		logger.Ctx(ctx).Warn("fragment not loaded", zap.String("fragKey", mergeTarget.fragmentKey))
	}
	// upload the mergedFragment
	flushErr := mergeTarget.Flush(ctx)
	if flushErr != nil {
		return nil, flushErr
	}
	// set flag
	mergeTarget.dataUploaded = true
	mergeTarget.infoFetched = true

	if releaseImmediately {
		// release immediately
		mergeTarget.entriesData = nil
		mergeTarget.indexes = nil
		mergeTarget.dataLoaded = false
	}
	logger.Ctx(ctx).Info("merge fragments and release after completed", zap.String("mergedFragKey", mergeTarget.fragmentKey), zap.Int64("mergeFragId", mergeFragId), zap.Int64s("fragmentIds", fragIds), zap.Int64("size", mergeTarget.size), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return mergeTarget, nil
}

func fastMergeSingleFragment(ctx context.Context, mergedFragKey string, mergeFragId int64, fragment *FragmentObject) (*FragmentObject, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "fastMergeSingleFragment")
	defer sp.End()
	startTime := time.Now()
	// merge
	mergedFrag := &FragmentObject{
		client:       fragment.client,
		bucket:       fragment.bucket,
		fragmentId:   mergeFragId,
		fragmentKey:  mergedFragKey,
		entriesData:  make([]byte, 0),
		indexes:      make([]byte, 0),
		firstEntryId: -1,
		lastEntryId:  -1,
		dataUploaded: false,
		dataLoaded:   false,
		infoFetched:  false,
	}

	// fast rename
	uploadInfo, uploadErr := fragment.client.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: mergedFrag.bucket,
			Object: mergedFrag.fragmentKey,
		}, minio.CopySrcOptions{
			Bucket: fragment.bucket,
			Object: fragment.fragmentKey,
		})
	if uploadErr != nil {
		return nil, uploadErr
	}

	// set data cache ready
	mergedFrag.size = uploadInfo.Size
	mergedFrag.rawBufSize = uploadInfo.Size
	mergedFrag.dataLoaded = false
	mergedFrag.dataUploaded = true
	mergedFrag.infoFetched = false

	logger.Ctx(ctx).Info("fast merge single fragment completed", zap.String("mergedFragKey", mergedFrag.fragmentKey), zap.Int64("mergeFragId", mergeFragId), zap.Int64("fragmentId", fragment.fragmentId), zap.Int64("size", mergedFrag.size), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return mergedFrag, nil
}

// Load here only need to load the last fragment data, only used in recover to load fragments info
func (f *ROSegmentImpl) Load(ctx context.Context) (int64, storage.Fragment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Load")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segmentId := fmt.Sprintf("%d", f.segmentId)
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
	lastEntryId, err := getLastEntryIdWithoutDataLoadedIfPossible(ctx, lastFragment)
	if err != nil {
		return -1, nil, err
	}
	fragId := lastFragment.GetFragmentId()
	logger.Ctx(ctx).Debug("Load fragments", zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int("fragments", len(f.fragments)),
		zap.Int64("totalSize", totalSize),
		zap.Int64("lastFragId", fragId),
		zap.Int64("lastEntryId", lastEntryId),
	)
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "load", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "load", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return totalSize, lastFragment, nil
}

func (f *ROSegmentImpl) DeleteFragments(ctx context.Context, flag int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFragments")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segmentId := fmt.Sprintf("%d", f.segmentId)

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
			logger.Ctx(ctx).Debug("Successfully deleted merged fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key))
			mergedFragmentCount++
		} else {
			logger.Ctx(ctx).Debug("Successfully deleted normal fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.String("objectKey", objInfo.Key))
			normalFragmentCount++
		}
	}

	// Clean up internal state
	f.fragments = make([]*FragmentObject, 0)
	f.mergedFragments = make([]*FragmentObject, 0)

	// Update metrics
	if len(deleteErrors) > 0 {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "delete_fragments", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "delete_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "delete_fragments", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "delete_fragments", "success").Observe(float64(time.Since(startTime).Milliseconds()))
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

// NewLogFileReader creates a new LogFileReader instance.
func NewLogFileReader(opt storage.ReaderOpt, objectFile *ROSegmentImpl) storage.Reader {
	return &logFileReader{
		opt:                opt,
		logfile:            objectFile,
		pendingReadEntryId: opt.StartSequenceNum,
	}
}

var _ storage.Reader = (*logFileReader)(nil)

type logFileReader struct {
	ctx     context.Context
	opt     storage.ReaderOpt
	logfile *ROSegmentImpl

	pendingReadEntryId int64
	currentFragment    *FragmentObject
}

func (o *logFileReader) HasNext(ctx context.Context) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "HasNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", o.logfile.logId)
	segmentId := fmt.Sprintf("%d", o.logfile.segmentId)
	if o.pendingReadEntryId >= int64(o.opt.EndSequenceNum) && o.opt.EndSequenceNum > 0 {
		// reach the end of range
		return false, nil
	}
	f, err := o.logfile.getFragment(ctx, o.pendingReadEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get fragment",
			zap.String("segmentPrefixKey", o.logfile.segmentPrefixKey),
			zap.Int64("pendingReadEntryId", o.pendingReadEntryId),
			zap.Error(err))
		return false, err
	}
	if f == nil {
		// no more fragment
		return false, nil
	}
	//
	o.currentFragment = f
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "has_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "has_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return true, nil
}

func (o *logFileReader) ReadNext(ctx context.Context) (*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", o.logfile.logId)
	segmentId := fmt.Sprintf("%d", o.logfile.segmentId)
	if o.currentFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := o.currentFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer o.currentFragment.Release(ctx)
	entryValue, err := o.currentFragment.GetEntry(ctx, o.pendingReadEntryId)
	if err != nil {
		return nil, err
	}
	entry := &proto.LogEntry{
		EntryId: o.pendingReadEntryId,
		Values:  entryValue,
	}
	o.pendingReadEntryId++
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "read_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "read_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entry, nil
}

// ReadNextBatch reads next batch of entries from the log file.
// size = -1 means auto batch,which will read all entries of a fragment for a time,
// otherwise it's the number of entries to read.
func (o *logFileReader) ReadNextBatch(ctx context.Context, size int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNextBatch")
	defer sp.End()
	if size != -1 {
		// TODO add batch size limit.
		return nil, werr.ErrNotSupport.WithCauseErrMsg("custom batch size not supported currently")
	}

	startTime := time.Now()
	logId := fmt.Sprintf("%d", o.logfile.logId)
	segmentId := fmt.Sprintf("%d", o.logfile.segmentId)
	if o.currentFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := o.currentFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer o.currentFragment.Release(ctx)
	entries := make([]*proto.LogEntry, 0, 32)
	for {
		entryValue, err := o.currentFragment.GetEntry(ctx, o.pendingReadEntryId)
		if err != nil {
			if werr.ErrEntryNotFound.Is(err) {
				break
			}
			return nil, err
		}
		entry := &proto.LogEntry{
			EntryId: o.pendingReadEntryId,
			Values:  entryValue,
		}
		entries = append(entries, entry)
		o.pendingReadEntryId++
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "read_batch_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "read_batch_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entries, nil
}

func (o *logFileReader) Close() error {
	// NO OP
	return nil
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

func getLastEntryIdWithoutDataLoadedIfPossible(ctx context.Context, fragment *FragmentObject) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "getLastEntryIdWithoutDataLoadedIfPossible")
	defer sp.End()
	lastEntryId, err := fragment.GetLastEntryId(ctx)
	if werr.ErrFragmentInfoNotFetched.Is(err) {
		loadErr := fragment.Load(ctx)
		if loadErr != nil {
			return -1, loadErr
		}
		defer fragment.Release(ctx)
		lastEntryId, err = fragment.GetLastEntryId(ctx)
		if err != nil {
			return -1, err
		}
	}
	return lastEntryId, nil
}

func getFirstEntryIdWithoutDataLoadedIfPossible(ctx context.Context, fragment *FragmentObject) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "getFirstEntryIdWithoutDataLoadedIfPossible")
	defer sp.End()
	firstEntryId, err := fragment.GetFirstEntryId(ctx)
	if werr.ErrFragmentInfoNotFetched.Is(err) {
		loadErr := fragment.Load(ctx)
		if loadErr != nil {
			return -1, loadErr
		}
		defer fragment.Release(ctx)
		firstEntryId, err = fragment.GetFirstEntryId(ctx)
		if err != nil {
			return -1, err
		}
	}
	return firstEntryId, nil
}
