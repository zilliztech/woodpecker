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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
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
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var (
	SegmentWriterScope = "MinioFileWriter"
)

var _ storage.Writer = (*MinioFileWriter)(nil)

// MinioFileWriter implements a logical file writer for MinIO object storage
// Each flush operation creates a part of the segment file
type MinioFileWriter struct {
	mu             sync.Mutex
	client         minioHandler.MinioHandler
	segmentFileKey string // The prefix key for the segment to which this Segment belongs
	bucket         string // The bucket name
	logId          int64
	segmentId      int64
	logIdStr       string // for metrics label only
	segmentIdStr   string // for metrics label only

	// configuration
	maxBufferSize    int64 // Max buffer size to sync buffer to object storage
	maxBufferEntries int64 // Maximum number of entries per buffer
	maxIntervalMs    int   // Max interval to sync buffer to object storage
	syncPolicyConfig *config.SegmentSyncPolicyConfig

	// write buffer
	buffer            atomic.Pointer[cache.SequentialBuffer] // Write buffer
	lastSyncTimestamp atomic.Int64

	// written info
	firstEntryID     atomic.Int64 // The first entryId of this Segment which already written to object storage
	lastEntryID      atomic.Int64 // The last entryId of this Segment which already written to object storage
	lastPartID       atomic.Int64 // The last blockId of this Segment which already written to object storage
	blockIndexes     []*codec.IndexRecord
	footerRecord     *codec.FooterRecord // exists if the segment is finalized
	headerWritten    atomic.Bool         // Ensure a header record is written before writing data
	lastModifiedTime int64               // lastModifiedTime

	// async upload blocks task pool
	syncMu                        sync.Mutex
	pool                          *conc.Pool[*blockUploadResult]
	fastSyncTriggerSize           int64        // The size of min buffer to trigger fast sync
	storageWritable               atomic.Bool  // Indicates whether the segment is writable
	flushingBufferSize            atomic.Int64 // The size of pending flush, it must be less than maxBufferSize
	flushingTaskList              chan *blockUploadTask
	lastSubmittedUploadingPartID  atomic.Int64
	lastSubmittedUploadingEntryID atomic.Int64
	allUploadingTaskDone          atomic.Bool

	// writing state
	fileClose     chan struct{} // Close signal
	closed        atomic.Bool
	recovered     atomic.Bool
	fenced        atomic.Bool // For fence state: true confirms it is fenced, while false requires verification by checking the storage for a fence flag object.
	lockObjectKey string      // Segment lock object key
}

// NewMinioFileWriter is used to create a new Segment File Writer, which is used to write data to object storage
func NewMinioFileWriter(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, objectCli minioHandler.MinioHandler, cfg *config.Configuration) (storage.Writer, error) {
	return NewMinioFileWriterWithMode(ctx, bucket, baseDir, logId, segId, objectCli, cfg, false)
}

func NewMinioFileWriterWithMode(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, objectCli minioHandler.MinioHandler, cfg *config.Configuration, recoveryMode bool) (storage.Writer, error) {
	segmentFileKey := getSegmentFileKey(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("new SegmentImpl created", zap.String("segmentFileKey", segmentFileKey))
	syncPolicyConfig := &cfg.Woodpecker.Logstore.SegmentSyncPolicy
	maxBufferEntries := int64(syncPolicyConfig.MaxEntries)
	segmentFileWriter := &MinioFileWriter{
		logId:          logId,
		segmentId:      segId,
		logIdStr:       fmt.Sprintf("%d", logId),
		segmentIdStr:   fmt.Sprintf("%d", segId),
		client:         objectCli,
		segmentFileKey: segmentFileKey,
		bucket:         bucket,

		maxBufferSize:    syncPolicyConfig.MaxBytes,
		maxBufferEntries: maxBufferEntries,
		maxIntervalMs:    syncPolicyConfig.MaxInterval,
		syncPolicyConfig: syncPolicyConfig,
		fileClose:        make(chan struct{}, 1),

		fastSyncTriggerSize: syncPolicyConfig.MaxFlushSize, // set sync trigger size equal to maxFlushSize(single block max size) to make pipeline flush soon
		pool:                conc.NewPool[*blockUploadResult](cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads, conc.WithPreAlloc(true)),

		flushingTaskList: make(chan *blockUploadTask, cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads),
	}
	segmentFileWriter.firstEntryID.Store(-1)
	segmentFileWriter.lastEntryID.Store(-1)
	segmentFileWriter.lastPartID.Store(-1)
	segmentFileWriter.lastSubmittedUploadingPartID.Store(-1)
	segmentFileWriter.lastSubmittedUploadingEntryID.Store(-1)
	segmentFileWriter.closed.Store(false)
	segmentFileWriter.storageWritable.Store(true)
	segmentFileWriter.allUploadingTaskDone.Store(false)
	segmentFileWriter.flushingBufferSize.Store(0)
	segmentFileWriter.recovered.Store(false)
	segmentFileWriter.fenced.Store(false)
	segmentFileWriter.headerWritten.Store(false)
	segmentFileWriter.lastSyncTimestamp.Store(time.Now().UnixMilli())

	if recoveryMode {
		// Try to recover existing state from MinIO before creating lock
		if err := segmentFileWriter.recoverFromStorage(ctx); err != nil {
			logger.Ctx(ctx).Warn("Failed to recover from storage, starting fresh",
				zap.String("segmentFileKey", segmentFileKey),
				zap.Error(err))
			return nil, err
		}
		lastEntryID := segmentFileWriter.GetLastEntryId(ctx)
		if lastEntryID != -1 {
			newBuffer := cache.NewSequentialBuffer(logId, segId, lastEntryID+1, maxBufferEntries)
			segmentFileWriter.buffer.Store(newBuffer)
		} else {
			// If no existing data found, start from entry ID 0
			newBuffer := cache.NewSequentialBuffer(logId, segId, 0, maxBufferEntries)
			segmentFileWriter.buffer.Store(newBuffer)
		}
	} else {
		newBuffer := cache.NewSequentialBuffer(logId, segId, 0, maxBufferEntries)
		segmentFileWriter.buffer.Store(newBuffer)

		// Create segment file writer lock object
		if err := segmentFileWriter.createSegmentLock(ctx); err != nil {
			logger.Ctx(ctx).Warn("Failed to create segment lock",
				zap.String("segmentFileKey", segmentFileKey),
				zap.Error(err))
			return nil, err // Return nil to indicate creation failure
		}
	}

	go segmentFileWriter.run()
	go segmentFileWriter.ack()
	return segmentFileWriter, nil
}

// recoverFromStorage attempts to recover the writer state from existing objects in MinIO
func (f *MinioFileWriter) recoverFromStorage(ctx context.Context) error {
	logger.Ctx(ctx).Info("attempting to recover writer state from storage",
		zap.String("segmentFileKey", f.segmentFileKey))

	footerPartKey := getFooterPartKey(f.segmentFileKey)
	footerPartObjInfo, err := f.client.StatObject(ctx, f.bucket, footerPartKey, minio.StatObjectOptions{})
	if err != nil && minioHandler.IsObjectNotExists(err) {
		return f.recoverFromFullListing(ctx)
	}
	return f.recoverFromFooter(ctx, footerPartKey, footerPartObjInfo)
}

func (f *MinioFileWriter) recoverFromFooter(ctx context.Context, footerKey string, footerPartObjInfo minio.ObjectInfo) error {
	// Read the entire footer.blk file
	footerObj, err := f.client.GetObject(ctx, f.bucket, footerKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	f.lastModifiedTime = footerPartObjInfo.LastModified.UnixMilli()

	footerBlkData, err := minioHandler.ReadObjectFull(ctx, footerObj, footerPartObjInfo.Size)
	if err != nil {
		return err
	}

	logger.Ctx(ctx).Debug("read entire footer.blk",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("footerBlkSize", footerPartObjInfo.Size),
		zap.Int("footerBlkDataLength", len(footerBlkData)))

	// Parse footer record from the end of the file
	if len(footerBlkData) < codec.RecordHeaderSize+codec.FooterRecordSize {
		return fmt.Errorf("footer.blk too small: %d bytes", len(footerBlkData))
	}

	footerRecordStart := len(footerBlkData) - codec.RecordHeaderSize - codec.FooterRecordSize
	footerRecordData := footerBlkData[footerRecordStart:]

	footerRecord, err := codec.DecodeRecord(footerRecordData)
	if err != nil {
		return fmt.Errorf("failed to decode footer record: %w", err)
	}

	if footerRecord.Type() != codec.FooterRecordType {
		return fmt.Errorf("expected footer record, got type %d", footerRecord.Type())
	}

	f.footerRecord = footerRecord.(*codec.FooterRecord)

	// If footer exists, the segment is finalized and should not be writable
	f.storageWritable.Store(false)

	// Parse index records sequentially from the beginning of the file
	indexData := footerBlkData[:footerRecordStart]

	logger.Ctx(ctx).Debug("parsing index records sequentially",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexDataLength", len(indexData)),
		zap.Int32("expectedTotalBlocks", f.footerRecord.TotalBlocks))

	// Parse all index records sequentially
	offset := 0
	var firstEntryID int64 = -1
	var lastEntryID int64 = -1
	var maxPartID int64 = -1

	for offset < len(indexData) {
		if offset+codec.RecordHeaderSize > len(indexData) {
			break // Not enough data for a complete record header
		}

		record, err := codec.DecodeRecord(indexData[offset:])
		if err != nil {
			logger.Ctx(ctx).Warn("failed to decode index record",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int("offset", offset),
				zap.Error(err))
			break
		}

		if record.Type() != codec.IndexRecordType {
			logger.Ctx(ctx).Warn("unexpected record type in index section",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int("offset", offset),
				zap.Uint8("recordType", record.Type()))
			break
		}

		indexRecord := record.(*codec.IndexRecord)
		f.blockIndexes = append(f.blockIndexes, indexRecord)

		// Update state tracking
		if firstEntryID == -1 || indexRecord.FirstEntryID < firstEntryID {
			firstEntryID = indexRecord.FirstEntryID
		}
		if indexRecord.LastEntryID > lastEntryID {
			lastEntryID = indexRecord.LastEntryID
		}
		if int64(indexRecord.BlockNumber) > maxPartID {
			maxPartID = int64(indexRecord.BlockNumber)
		}

		// Move to next record (header + payload)
		recordSize := codec.RecordHeaderSize + 36 // IndexRecord payload size
		offset += recordSize

		logger.Ctx(ctx).Debug("parsed index record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int32("blockNumber", indexRecord.BlockNumber),
			zap.Int64("startOffset", indexRecord.StartOffset),
			zap.Int64("firstEntryID", indexRecord.FirstEntryID),
			zap.Int64("lastEntryID", indexRecord.LastEntryID))
	}

	// Update writer state
	if firstEntryID != -1 {
		f.firstEntryID.Store(firstEntryID)
	}
	if lastEntryID != -1 {
		f.lastEntryID.Store(lastEntryID)
	}
	if maxPartID != -1 {
		f.lastPartID.Store(maxPartID)
		f.lastSubmittedUploadingPartID.Store(maxPartID)
		f.lastSubmittedUploadingEntryID.Store(lastEntryID)
	}

	f.recovered.Store(true)
	logger.Ctx(ctx).Info("successfully parsed footer and index records",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexRecordCount", len(f.blockIndexes)),
		zap.Int32("expectedTotalBlocks", f.footerRecord.TotalBlocks),
		zap.Int64("recoveredFirstEntryID", firstEntryID),
		zap.Int64("recoveredLastEntryID", lastEntryID),
		zap.Int64("recoveredLastPartID", maxPartID))
	return nil
}

func (f *MinioFileWriter) recoverFromFullListing(ctx context.Context) error {

	// List all objects with the segment prefix
	objectCh := f.client.ListObjects(ctx, f.bucket, f.segmentFileKey, true, minio.ListObjectsOptions{})

	var dataObjects []string
	var maxPartID int64 = -1

	lastModifiedTime := int64(0)
	for object := range objectCh {
		if object.Err != nil {
			logger.Ctx(ctx).Warn("error listing objects during recovery",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Error(object.Err))
			continue
		}

		// Skip lock files
		if strings.HasSuffix(object.Key, ".lock") {
			continue
		}

		// Parse object key to get part ID
		if strings.HasSuffix(object.Key, ".blk") {
			if object.LastModified.UnixMilli() > lastModifiedTime {
				lastModifiedTime = object.LastModified.UnixMilli()
			}
			partID, isMerge, err := parseFilePartName(object.Key)
			if err != nil {
				logger.Ctx(ctx).Warn("failed to parse object key during recovery",
					zap.String("objectKey", object.Key),
					zap.Error(err))
				continue
			}

			if !isMerge {
				dataObjects = append(dataObjects, object.Key)
				if partID > maxPartID {
					maxPartID = partID
				}
			}
		}
	}

	f.lastModifiedTime = lastModifiedTime
	if len(dataObjects) == 0 {
		logger.Ctx(ctx).Info("no existing data objects found, starting fresh",
			zap.String("segmentFileKey", f.segmentFileKey))
		return nil
	}

	logger.Ctx(ctx).Info("found existing data objects during recovery",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("objectCount", len(dataObjects)),
		zap.Int64("maxPartID", maxPartID))

	// Sort objects by part ID to process them in order
	sort.Strings(dataObjects)

	// Try to read each data object to rebuild block indexes
	var firstEntryID int64 = -1
	var lastEntryID int64 = -1

	for _, objectKey := range dataObjects {
		partID, _, err := parseFilePartName(objectKey)
		if err != nil {
			continue
		}

		// TODO only read blocks last record
		// Read the object to get block information
		obj, err := f.client.GetObject(ctx, f.bucket, objectKey, minio.GetObjectOptions{})
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read object during recovery",
				zap.String("objectKey", objectKey),
				zap.Error(err))
			continue
		}

		data, err := io.ReadAll(obj)
		obj.Close()
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read object data during recovery",
				zap.String("objectKey", objectKey),
				zap.Error(err))
			continue
		}

		// Parse the data to find BlockLastRecord
		logger.Ctx(ctx).Info("attempting to parse block last record during recovery",
			zap.String("objectKey", objectKey),
			zap.Int("dataSize", len(data)))

		blockLastRecord, err := f.parseBlockLastRecord(ctx, data)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to parse block last record during recovery",
				zap.String("objectKey", objectKey),
				zap.Int("dataSize", len(data)),
				zap.Error(err))
			continue
		}

		// Create index record for this block
		indexRecord := &codec.IndexRecord{
			BlockNumber:       int32(partID),
			StartOffset:       partID,
			FirstRecordOffset: 0,
			FirstEntryID:      blockLastRecord.FirstEntryID,
			LastEntryID:       blockLastRecord.LastEntryID,
		}

		f.blockIndexes = append(f.blockIndexes, indexRecord)

		if firstEntryID == -1 || blockLastRecord.FirstEntryID < firstEntryID {
			firstEntryID = blockLastRecord.FirstEntryID
		}
		if blockLastRecord.LastEntryID > lastEntryID {
			lastEntryID = blockLastRecord.LastEntryID
		}

		logger.Ctx(ctx).Debug("recovered block during recovery",
			zap.String("objectKey", objectKey),
			zap.Int64("partID", partID),
			zap.Int64("firstEntryID", blockLastRecord.FirstEntryID),
			zap.Int64("lastEntryID", blockLastRecord.LastEntryID))
	}

	// Update writer state
	if firstEntryID != -1 {
		f.firstEntryID.Store(firstEntryID)
	}
	if lastEntryID != -1 {
		f.lastEntryID.Store(lastEntryID)
	}
	if maxPartID != -1 {
		f.lastPartID.Store(maxPartID)
		f.lastSubmittedUploadingPartID.Store(maxPartID)
		f.lastSubmittedUploadingEntryID.Store(lastEntryID)
	}
	logger.Ctx(ctx).Info("successfully recovered writer state from storage",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("blockIndexCount", len(f.blockIndexes)),
		zap.Int64("firstEntryID", firstEntryID),
		zap.Int64("lastEntryID", lastEntryID),
		zap.Int64("lastPartID", maxPartID))

	f.recovered.Store(true)
	return nil
}

// parseBlockLastRecord extracts the BlockLastRecord from the end of block data
func (f *MinioFileWriter) parseBlockLastRecord(ctx context.Context, data []byte) (*codec.BlockLastRecord, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	logger.Ctx(ctx).Info("parsing block data for BlockLastRecord",
		zap.Int("dataSize", len(data)))

	// Parse all records in the data to find the BlockLastRecord
	// The BlockLastRecord should be the last record in the block
	records, err := codec.DecodeRecordList(data)
	if err != nil {
		logger.Ctx(ctx).Info("failed to decode records",
			zap.Error(err),
			zap.Int("dataSize", len(data)))
		return nil, fmt.Errorf("failed to decode records: %w", err)
	}

	logger.Ctx(ctx).Info("decoded records from block data",
		zap.Int("recordCount", len(records)))

	// Check if the first record is HeaderRecord (to determine if header was written)
	if len(records) > 0 {
		if _, ok := records[0].(*codec.HeaderRecord); ok {
			logger.Ctx(ctx).Info("found HeaderRecord in existing block, marking header as written")
			f.headerWritten.Store(true)
		}
	}

	// Look for BlockLastRecord from the end
	for i := len(records) - 1; i >= 0; i-- {
		logger.Ctx(ctx).Info("checking record type",
			zap.Int("recordIndex", i),
			zap.String("recordType", fmt.Sprintf("%T", records[i])))

		if blockLastRecord, ok := records[i].(*codec.BlockLastRecord); ok {
			logger.Ctx(ctx).Info("found BlockLastRecord",
				zap.Int64("firstEntryID", blockLastRecord.FirstEntryID),
				zap.Int64("lastEntryID", blockLastRecord.LastEntryID))
			return blockLastRecord, nil
		}
	}

	return nil, errors.New("BlockLastRecord not found")
}

// Like OS file fsync dirty pageCache periodically, objectStoreFile will sync buffer to object storage periodically
func (f *MinioFileWriter) run() {
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
			ctx, sp := logger.NewIntentCtx(SegmentWriterScope, fmt.Sprintf("run_%d_%d", f.logId, f.segmentId))
			err := f.Sync(context.Background())
			if err != nil {
				logger.Ctx(ctx).Warn("sync error",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Error(err))
			}
			sp.End()
			ticker.Reset(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
		case <-f.fileClose:
			logger.Ctx(context.TODO()).Debug("close SegmentImpl", zap.String("segmentFileKey", f.segmentFileKey), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
			metrics.WpFileWriters.WithLabelValues(logIdStr).Dec()
			return
		}
	}
}

func (f *MinioFileWriter) ack() {
	var firstUploadErrTask *blockUploadTask
	for task := range f.flushingTaskList {
		if task.flushData == nil {
			logger.Ctx(context.TODO()).Debug("received termination signal, marking all upload tasks as done",
				zap.String("segmentFileKey", f.segmentFileKey))
			f.allUploadingTaskDone.Store(true)
			break
		}
		if task.flushFuture.OK() {
			if firstUploadErrTask != nil {
				// flush success, but there is a flush error task before
				logger.Ctx(context.TODO()).Info("flush success but error exists before, trigger fast flush fail",
					zap.String("firstFlushErrPart", firstUploadErrTask.flushFuture.Value().part.PartKey),
					zap.String("flushSuccessPart", task.flushFuture.Value().part.PartKey))
				f.fastFlushFailUnsafe(context.TODO(), task.flushData, firstUploadErrTask.flushFuture.Value().err)
			} else {
				// update flush state
				result := task.flushFuture.Value()
				flushedFirst := result.part.FirstEntryID // always no error, because it's just created
				flushedLast := result.part.LastEntryID   // always no error, because it's just created
				flushedPartID := result.part.PartID
				if flushedLast >= 0 {
					f.lastEntryID.Store(flushedLast)
					f.lastPartID.Store(flushedPartID)
					if f.firstEntryID.Load() == -1 {
						// Initialize firstEntryId on first successful flush
						// This should always be 0 for the initial flush
						f.firstEntryID.Store(flushedFirst)
					}
				}

				logger.Ctx(context.TODO()).Info("flush success, fast success ack", zap.String("flushSuccessPart", task.flushFuture.Value().part.PartKey))
				// flush success ack
				f.fastFlushSuccessUnsafe(context.TODO(), task.flushFuture.Value().part, task.flushData)
				f.lastModifiedTime = time.Now().UnixMilli()
			}
		} else {
			// flush fail, trigger mark storage not writable
			if firstUploadErrTask == nil {
				// after many retry flush fail, mark storage not writable
				firstUploadErrTask = task
				f.storageWritable.Store(false)
			}
			if werr.ErrSegmentFenced.Is(task.flushFuture.Err()) {
				// when put object fail cause by fence object exists, mark storage not writable
				f.fenced.Store(true)
			}
			logger.Ctx(context.TODO()).Info("flush error first encountered, trigger fast flush fail",
				zap.String("firstFlushErrBlock", firstUploadErrTask.flushFuture.Value().part.PartKey))
			f.fastFlushFailUnsafe(context.TODO(), task.flushData, task.flushFuture.Value().err)
		}
		f.flushingBufferSize.Add(-task.flushFuture.Value().part.Size)
	}
}

func (f *MinioFileWriter) GetId() int64 {
	return f.segmentId
}

func (f *MinioFileWriter) WriteDataAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "AppendAsync")
	defer sp.End()

	// Validate that data is not empty
	if len(data) == 0 {
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, data cannot be empty", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrEmptyPayload
	}

	if f.closed.Load() {
		// quick fail and return a close Err, which indicate than it is also not retriable
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment writer is closed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrLogFileClosed
	}
	if f.fenced.Load() {
		// quick fail and return a fenced Err
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment is fenced", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrSegmentFenced
	}
	if !f.storageWritable.Load() {
		// quick fail and return a Storage Err, which indicate that it is also not retriable
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment storage not writable", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrStorageNotWritable
	}

	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))

	// trigger sync by max buffer entries num
	currentBuffer := f.buffer.Load()
	pendingAppendId := currentBuffer.ExpectedNextEntryId.Load() + 1
	if pendingAppendId >= currentBuffer.FirstEntryId+currentBuffer.MaxEntries {
		logger.Ctx(ctx).Debug("buffer full, trigger flush",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("pendingAppendId", pendingAppendId),
			zap.Int64("bufferFirstId", currentBuffer.FirstEntryId),
			zap.Int64("bufferLastId", currentBuffer.FirstEntryId+currentBuffer.MaxEntries))
		err := f.Sync(ctx)
		if err != nil {
			// sync does not success
			logger.Ctx(ctx).Warn("AppendAsync: found buffer full, but sync failed before append", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err))
			return entryId, err
		}
	}

	f.mu.Lock()
	if entryId <= f.lastEntryID.Load() {
		// If entryId is less than or equal to lastEntryID, it indicates that the entry has already been written to object storage. Return immediately.
		logger.Ctx(ctx).Debug("AppendAsync: skipping write, entryId is not greater than lastEntryID, already stored", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int64("lastEntryID", f.lastEntryID.Load()))
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, entryId, resultCh, entryId, nil)
		f.mu.Unlock()
		return entryId, nil
	}
	if entryId <= f.lastSubmittedUploadingEntryID.Load() {
		// If entryId is less than or equal to lastSubmittedUploadingEntryID, it indicates that the entry has already been submitted for upload. Return immediately.
		logger.Ctx(ctx).Debug("AppendAsync: skipping write, entryId is not greater than lastSubmittedUploadingEntryID, already submitted for upload", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId))
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
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	f.mu.Unlock()

	// trigger sync by max buffer entries bytes size
	sequentialReadyDataSize := currentBuffer.SequentialReadyDataSize.Load()
	dataSize := currentBuffer.DataSize.Load()
	if sequentialReadyDataSize >= f.fastSyncTriggerSize || dataSize >= f.maxBufferSize {
		logger.Ctx(ctx).Debug("reach max buffer size, trigger flush", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("bufferSize", dataSize), zap.Int64("sequentialReadyDataSize", sequentialReadyDataSize), zap.Int64("fastSyncTriggerSize", f.fastSyncTriggerSize), zap.Int64("maxSize", f.maxBufferSize))
		syncErr := f.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger flush failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("bufferSize", dataSize), zap.Int64("sequentialReadyDataSize", sequentialReadyDataSize), zap.Int64("fastSyncTriggerSize", f.fastSyncTriggerSize), zap.Int64("maxSize", f.maxBufferSize), zap.Error(syncErr))
		}
	}

	return id, nil
}

func (f *MinioFileWriter) GetFirstEntryId(ctx context.Context) int64 {
	return f.firstEntryID.Load()
}

func (f *MinioFileWriter) GetLastEntryId(ctx context.Context) int64 {
	return f.lastEntryID.Load()
}

func (f *MinioFileWriter) waitIfFlushingBufferSizeExceededUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "waitIfFlushingBufferSizeExceededUnsafe")
	defer sp.End()
	// Check if current flushing buffer size exceeds the maximum allowed buffer size
	for {
		currentFlushingSize := f.flushingBufferSize.Load()
		if currentFlushingSize < f.maxBufferSize {
			// Safe to proceed, flushing buffer size is within limits
			logger.Ctx(ctx).Debug("Flushing buffer size check passed",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("currentFlushingSize", currentFlushingSize),
				zap.Int64("maxBufferSize", f.maxBufferSize))
			return nil
		}

		// Flushing buffer size exceeded, need to wait
		logger.Ctx(ctx).Debug("Flushing buffer size exceeded, waiting for space",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("currentFlushingSize", currentFlushingSize),
			zap.Int64("maxBufferSize", f.maxBufferSize))

		// Wait for a short period before checking again
		select {
		case <-ctx.Done():
			// Context cancelled, return immediately
			logger.Ctx(ctx).Warn("Context cancelled while waiting for flushing buffer space",
				zap.String("segmentFileKey", f.segmentFileKey))
			return ctx.Err()
		case <-f.fileClose:
			// Segment is being closed, return immediately
			logger.Ctx(ctx).Debug("Segment close signal received while waiting for flushing buffer space",
				zap.String("segmentFileKey", f.segmentFileKey))
			return werr.ErrLogFileClosed
		case <-time.After(10 * time.Millisecond):
			// Continue checking after a short delay
			continue
		}
	}
}

func (f *MinioFileWriter) Compact(ctx context.Context) ([]int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Compact")
	defer sp.End()

	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Ctx(ctx).Info("starting segment compaction",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("currentBlockCount", len(f.blockIndexes)))

	// Check if segment is already compacted
	if f.footerRecord != nil && codec.IsCompacted(f.footerRecord.Flags) {
		logger.Ctx(ctx).Info("segment is already compacted, skipping",
			zap.String("segmentFileKey", f.segmentFileKey))
		return []int64{}, nil
	}

	// Ensure segment is finalized before compaction
	if f.footerRecord == nil {
		logger.Ctx(ctx).Warn("segment must be finalized before compaction",
			zap.String("segmentFileKey", f.segmentFileKey))
		return nil, fmt.Errorf("segment must be finalized before compaction")
	}

	// Get target block size for compaction (use maxFlushSize as target)
	targetBlockSize := f.syncPolicyConfig.MaxFlushSize
	if targetBlockSize <= 0 {
		targetBlockSize = 2 * 1024 * 1024 // Default 2MB
	}

	// Stream merge and upload blocks
	newBlockIndexes, uploadedBlockIDs, err := f.streamMergeAndUploadBlocks(ctx, targetBlockSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stream merge and upload blocks",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(err))
		return nil, fmt.Errorf("failed to stream merge and upload blocks: %w", err)
	}

	if len(newBlockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks to compact",
			zap.String("segmentFileKey", f.segmentFileKey))
		return []int64{}, nil
	}

	// Create new footer with compacted flag
	newFooter := &codec.FooterRecord{
		TotalBlocks:  int32(len(newBlockIndexes)),
		TotalRecords: f.footerRecord.TotalRecords,
		IndexOffset:  0,
		IndexLength:  uint32(len(newBlockIndexes) * (codec.RecordHeaderSize + 36)), // IndexRecord size
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(f.footerRecord.Flags), // Set compacted flag=1 (bit 0)
	}

	// Serialize new footer and indexes
	footerData := f.serializeCompactedFooterAndIndexes(ctx, newBlockIndexes, newFooter)

	// Upload new footer
	footerKey := getFooterPartKey(f.segmentFileKey)
	_, putErr := f.client.PutObject(ctx, f.bucket, footerKey, bytes.NewReader(footerData), int64(len(footerData)), minio.PutObjectOptions{})
	if putErr != nil {
		logger.Ctx(ctx).Warn("failed to upload compacted footer",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(putErr))
		return nil, fmt.Errorf("failed to upload compacted footer: %w", putErr)
	}

	// Update internal state
	originalBlockCount := len(f.blockIndexes)
	f.blockIndexes = newBlockIndexes
	f.footerRecord = newFooter

	logger.Ctx(ctx).Info("successfully compacted segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("originalBlockCount", originalBlockCount),
		zap.Int("compactedBlockCount", len(newBlockIndexes)),
		zap.Int("uploadedBlocks", len(uploadedBlockIDs)))

	return uploadedBlockIDs, nil
}

// Sync Implement sync logic, e.g., flush to persistent storage
func (f *MinioFileWriter) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Sync")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	f.syncMu.Lock() // ensure only one sync operation is running at a time
	defer f.syncMu.Unlock()
	defer func() {
		f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	}()

	if !f.storageWritable.Load() {
		logger.Ctx(ctx).Warn("Call Sync, but storage is not writable, quick fail all append requests", zap.String("segmentFileKey", f.segmentFileKey))
		return f.quickSyncFailUnsafe(ctx, werr.ErrStorageNotWritable)
	}

	// roll buff with lock
	f.mu.Lock()
	currentBuffer, toFlushData, toFlushDataFirstEntryId, err := f.rollBufferUnsafe(ctx)
	f.mu.Unlock()
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesRangeData failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}
	if len(toFlushData) == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentFileKey", f.segmentFileKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	// submit async flush task
	flushResultFutures := f.submitBlockFlushTaskUnsafe(ctx, currentBuffer, toFlushData, toFlushDataFirstEntryId)
	logger.Ctx(ctx).Debug("Sync submitted flush tasks",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("blocks", len(flushResultFutures)),
		zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId),
		zap.Int("toFlushEntries", len(toFlushData)),
		zap.Int64("restDataFirstEntryId", currentBuffer.GetExpectedNextEntryId()),
		zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))

	return nil
}

// Get data that is sequentially ready to be flushed
// For example, in a sequence like 1,2,x,3,x,5,6, "1,2" is ready, while "x,3,x,5,6" still needs to wait for missing entries to arrive before it can be flushed
// Therefore, the toFlush data is "1,2", and the remaining data stays in the buffer for further append operations
func (f *MinioFileWriter) rollBufferUnsafe(ctx context.Context) (*cache.SequentialBuffer, []*cache.BufferEntry, int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "rollBufferUnsafe")
	defer sp.End()
	startTime := time.Now()

	// wait available buffer size
	waitBuffErr := f.waitIfFlushingBufferSizeExceededUnsafe(ctx)
	if waitBuffErr != nil {
		return nil, nil, -1, waitBuffErr
	}

	// get current buffer
	currentBuffer := f.buffer.Load()

	logger.Ctx(ctx).Debug("start roll buffer", zap.String("segmentFileKey", f.segmentFileKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))

	// check if there are any entries to be written
	entryCount := len(currentBuffer.Entries)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentFileKey", f.segmentFileKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return currentBuffer, make([]*cache.BufferEntry, 0), -1, nil
	}
	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	// get flush point to flush
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentFileKey", f.segmentFileKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return currentBuffer, make([]*cache.BufferEntry, 0), -1, nil
	}
	// get flush data
	toFlushData, err := currentBuffer.ReadEntriesRange(currentBuffer.FirstEntryId, expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesRangeData failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return currentBuffer, nil, -1, err
	}
	toFlushDataFirstEntryId := currentBuffer.FirstEntryId

	// roll new buffer with rest data
	restData, err := currentBuffer.ReadEntriesToLast(expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesToLastData failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err))
		return currentBuffer, nil, -1, err
	}
	restDataFirstEntryId := expectedNextEntryId
	newBuffer := cache.NewSequentialBufferWithData(f.logId, f.segmentId, restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries), restData)
	f.buffer.Store(newBuffer)
	logger.Ctx(ctx).Debug("start roll buffer", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId), zap.Int("count", len(toFlushData)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)), zap.String("newBufInst", fmt.Sprintf("%p", newBuffer)))
	return currentBuffer, toFlushData, toFlushDataFirstEntryId, nil
}

func (f *MinioFileWriter) fastFlushFailUnsafe(ctx context.Context, blockData []*cache.BufferEntry, resultErr error) {
	for _, item := range blockData {
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, item.EntryId, item.NotifyChan, -1, resultErr)
	}
}

func (f *MinioFileWriter) fastFlushSuccessUnsafe(ctx context.Context, partInfo *PartInfo, blockData []*cache.BufferEntry) {
	// Calculate cumulative offset from previous blocks
	var cumulativeOffset int64 = 0
	for _, indexRecord := range f.blockIndexes {
		// Use the actual size from PartInfo instead of just incrementing by 1
		cumulativeOffset += indexRecord.StartOffset
	}

	f.blockIndexes = append(f.blockIndexes, &codec.IndexRecord{
		BlockNumber:       int32(partInfo.PartID),
		StartOffset:       partInfo.PartID, // Use PartID as StartOffset (block number in MinIO)
		FirstRecordOffset: 0,               // First record starts at offset 0 within this block
		FirstEntryID:      partInfo.FirstEntryID,
		LastEntryID:       partInfo.LastEntryID,
	})
	for _, item := range blockData {
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, item.EntryId, item.NotifyChan, item.EntryId, nil)
	}
}

func (f *MinioFileWriter) submitBlockFlushTaskUnsafe(ctx context.Context, currentBuffer *cache.SequentialBuffer, toFlushData []*cache.BufferEntry, toFlushDataFirstEntryId int64) []*conc.Future[*blockUploadResult] {
	blockDataList, blockFirstEntryIdList, blockSizeList := f.prepareMultiBlockDataIfNecessary(toFlushData, toFlushDataFirstEntryId)
	flushResultFutures := make([]*conc.Future[*blockUploadResult], 0, len(toFlushData))

	var waitBuffErr error
	for i, blockData := range blockDataList {
		blockId := f.lastSubmittedUploadingPartID.Add(1) // block id
		blockFirstEntryId := blockFirstEntryIdList[i]
		partData := blockData
		blockLastEntryId := blockFirstEntryId + int64(len(partData)) - 1
		f.lastSubmittedUploadingEntryID.Store(blockLastEntryId)
		blockSize := blockSizeList[i] // Capture block size for the closure

		if waitBuffErr != nil {
			// if error exist, fast fail subsequent parts
			f.fastFlushFailUnsafe(ctx, partData, waitBuffErr)
			logger.Ctx(ctx).Warn("fast fail the flush task before submit",
				zap.Int64("logId", f.logId),
				zap.Int64("segmentId", f.segmentId),
				zap.Int64("blockId", blockId),
				zap.Int64("blockFirstEntryId", blockFirstEntryId),
				zap.Int("count", len(partData)),
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Error(waitBuffErr))
		}

		// first wait available space
		waitErr := f.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		if waitErr != nil {
			// sync interrupted, fast fail and notify all pending append entries
			f.fastFlushFailUnsafe(ctx, partData, waitErr)
			logger.Ctx(ctx).Warn("fast fail the flush task before submit",
				zap.Int64("logId", f.logId),
				zap.Int64("segmentId", f.segmentId),
				zap.Int64("blockId", blockId),
				zap.Int64("blockFirstEntryId", blockFirstEntryId),
				zap.Int("count", len(partData)),
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Error(waitErr))
			waitBuffErr = waitErr
			continue
		}

		// try to submit flush task
		resultFuture := f.pool.Submit(func() (*blockUploadResult, error) {
			logger.Ctx(ctx).Debug("start flush part of buffer as block", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Int("count", len(partData)), zap.Int64("blockSize", blockSize))
			partKey := getPartKey(f.segmentFileKey, blockId)
			partRawData := f.serialize(partData)
			actualDataSize := int64(len(partRawData))
			logger.Ctx(ctx).Debug("serialized block data", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Int64("originalBlockSize", blockSize), zap.Int64("actualDataSize", actualDataSize))
			flushErr := retry.Do(ctx,
				func() error {
					_, putErr := f.client.PutObjectIfNoneMatch(ctx, f.bucket, partKey, bytes.NewReader(partRawData), actualDataSize)
					return putErr
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
				logger.Ctx(ctx).Warn("flush part of buffer as block failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Error(flushErr))
			}
			result := &blockUploadResult{
				part: &PartInfo{
					FirstEntryID: blockFirstEntryId,
					LastEntryID:  blockFirstEntryId + int64(len(partData)) - 1,
					PartKey:      partKey,
					PartID:       blockId,
					Size:         actualDataSize,
				},
				err: flushErr,
			}
			logger.Ctx(ctx).Debug("complete flush part of buffer as block", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Int("count", len(partData)), zap.Int64("blockSize", blockSize))
			return result, flushErr
		})

		// update submit flushing size
		submitFlushingSize := blockSizeList[i]
		f.flushingBufferSize.Add(submitFlushingSize)
		f.flushingTaskList <- &blockUploadTask{
			flushData:             partData,
			flushDataFirstEntryId: blockFirstEntryId,
			flushFuture:           resultFuture,
		}
		flushResultFutures = append(flushResultFutures, resultFuture)
	}

	logger.Ctx(ctx).Debug("submitted block flush tasks", zap.String("segmentFileKey", f.segmentFileKey), zap.Int("blocks", len(blockDataList)), zap.Int("submitted", len(flushResultFutures)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	return flushResultFutures
}

func (f *MinioFileWriter) awaitAllFlushTasks(ctx context.Context) error {
	logger.Ctx(ctx).Info("wait for all parts of buffer to be flushed", zap.String("segmentFileKey", f.segmentFileKey))

	// First, wait for all upload tasks to complete
	maxWaitTime := 10 * time.Second
	startTime := time.Now()

	for {
		runningTasks := f.pool.Running()

		logger.Ctx(ctx).Info("checking upload task status",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int("runningTasks", runningTasks),
			zap.Duration("elapsed", time.Since(startTime)))

		// If no tasks are running, all uploads are done
		if runningTasks == 0 {
			logger.Ctx(ctx).Info("all upload tasks completed", zap.String("segmentFileKey", f.segmentFileKey))
			break
		}

		// Check timeout
		if time.Since(startTime) > maxWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for upload tasks to complete",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int("runningTasks", runningTasks),
				zap.Duration("elapsed", time.Since(startTime)))
			return errors.New("timeout waiting for upload tasks to complete")
		}

		// Short sleep to avoid busy waiting
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}

	// Now wait for the ack goroutine to process all completed tasks
	// This ensures that blockIndexes are properly populated
	logger.Ctx(ctx).Info("waiting for ack goroutine to process completed tasks", zap.String("segmentFileKey", f.segmentFileKey))

	// Reset the flag before sending the termination signal
	f.allUploadingTaskDone.Store(false)

	// Send termination signal to ack goroutine
	select {
	case f.flushingTaskList <- &blockUploadTask{
		flushData:             nil,
		flushDataFirstEntryId: 0,
		flushFuture:           nil,
	}:
		logger.Ctx(ctx).Info("termination signal sent to ack goroutine", zap.String("segmentFileKey", f.segmentFileKey))
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
		return errors.New("timeout sending termination signal")
	}

	// Wait for ack goroutine to set the done flag
	ackWaitTime := 5 * time.Second
	ackStartTime := time.Now()

	for {
		if f.allUploadingTaskDone.Load() {
			logger.Ctx(ctx).Info("ack goroutine completed processing all tasks", zap.String("segmentFileKey", f.segmentFileKey))
			return nil
		}

		// Check timeout
		if time.Since(ackStartTime) > ackWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for ack goroutine to complete",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Bool("allUploadingTaskDone", f.allUploadingTaskDone.Load()),
				zap.Duration("elapsed", time.Since(ackStartTime)))
			return errors.New("timeout waiting for ack goroutine to complete")
		}

		// Short sleep to avoid busy waiting
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
			continue
		}
	}
}

func (f *MinioFileWriter) quickSyncFailUnsafe(ctx context.Context, resultErr error) error {
	logger.Ctx(ctx).Warn("Call Sync, but storage is not writable, quick fail all append requests", zap.String("segmentFileKey", f.segmentFileKey))
	currentBuffer := f.buffer.Load()
	currentBuffer.NotifyAllPendingEntries(ctx, -1, resultErr)
	currentBuffer.Reset(ctx)
	return errors.New("storage is not writable")
}

func (f *MinioFileWriter) Finalize(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Finalize")
	defer sp.End()
	if !f.closed.CompareAndSwap(false, true) { // mark close, and there will be no more add and sync in the future
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrSegmentClosed
	}
	err := f.Sync(context.Background()) // manual sync all pending append operation
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(err))
	}
	// wait all flush
	waitErr := f.awaitAllFlushTasks(ctx)
	if waitErr != nil {
		logger.Ctx(ctx).Warn("wait flush error before close",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(waitErr))
		return -1, waitErr
	}

	// finalize with footer.blk name
	partKey := getFooterPartKey(f.segmentFileKey)
	logger.Ctx(ctx).Info("finalizing segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("partKey", partKey),
		zap.Int("blockIndexesCount", len(f.blockIndexes)),
		zap.Int64("lastPartID", f.lastPartID.Load()))

	partRawData, footer := serializeFooterAndIndexes(ctx, f.blockIndexes)
	logger.Ctx(ctx).Info("serialized footer and indexes",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("serializedDataSize", len(partRawData)))

	_, putErr := f.client.PutObjectIfNoneMatch(ctx, f.bucket, partKey, bytes.NewReader(partRawData), int64(len(partRawData)))
	if putErr != nil {
		logger.Ctx(ctx).Warn("failed to put finalization object",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("partKey", partKey),
			zap.Error(putErr))
		return -1, fmt.Errorf("failed to put object: %w", putErr)
	}
	f.footerRecord = footer
	logger.Ctx(ctx).Info("successfully finalized segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("partKey", partKey))
	return f.GetLastEntryId(ctx), nil
}

func (f *MinioFileWriter) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Close")
	defer sp.End()
	if !f.closed.CompareAndSwap(false, true) { // mark close, and there will be no more add and sync in the future
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return nil
	}
	logger.Ctx(ctx).Info("run: received close signal,trigger sync before close ", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
	err := f.Sync(context.Background()) // manual sync all pending append operation
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(err))
	}

	// wait all flush
	waitErr := f.awaitAllFlushTasks(ctx)
	if waitErr != nil {
		logger.Ctx(ctx).Warn("wait flush error before close",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(waitErr))
		return waitErr
	}

	// Release segment lock
	if err := f.releaseSegmentLock(ctx); err != nil {
		logger.Ctx(ctx).Warn("Failed to release segment lock during close",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(err))
	}

	// close file
	f.fileClose <- struct{}{}
	close(f.fileClose)
	close(f.flushingTaskList)
	return nil
}

func (f *MinioFileWriter) IsFenced(ctx context.Context) (bool, error) {
	return f.fenced.Load(), nil
}

func (f *MinioFileWriter) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Fence")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()

	// If already fenced, fast return
	if f.fenced.Load() {
		return f.lastEntryID.Load(), nil
	}

	// if already completed, no heed to fence,just return
	if f.footerRecord != nil {
		return f.lastEntryID.Load(), nil
	}

	var fenceObjectKey string
	firstFenceAttampt := true

	// Use retry.Do to handle the fence object creation with retries
	err := retry.Do(ctx,
		func() error {
			// recover to find incremental new blocks
			if !firstFenceAttampt {
				recoverErr := f.recoverFromStorage(ctx)
				if recoverErr != nil {
					logger.Ctx(ctx).Warn("Failed to recover from storage during fence",
						zap.String("segmentFileKey", f.segmentFileKey),
						zap.Error(recoverErr))
					return recoverErr
				}
			}
			firstFenceAttampt = false

			// get last block ID
			blocks := f.blockIndexes
			lastBlockID := int64(-1)
			lastEntryID := int64(-1)
			if len(blocks) > 0 {
				lastBlockID = int64(blocks[len(blocks)-1].BlockNumber)
				lastEntryID = blocks[len(blocks)-1].LastEntryID
			}
			// Get current last block ID and create fence object key
			fenceBlockId := lastBlockID + 1
			fenceObjectKey = getPartKey(f.segmentFileKey, fenceBlockId)

			// Try to create fence object
			_, putErr := f.client.PutFencedObject(ctx, f.bucket, fenceObjectKey)
			if putErr != nil {
				if werr.ErrFragmentAlreadyExists.Is(putErr) {
					// Block already exists, this might be normal during concurrent operations
					logger.Ctx(ctx).Debug("Fence object already exists, retrying with next block ID",
						zap.String("segmentFileKey", f.segmentFileKey),
						zap.String("fenceObjectKey", fenceObjectKey),
						zap.Int64("fenceBlockId", fenceBlockId))
					return putErr // This will trigger a retry
				}
				// Other errors are not retryable
				logger.Ctx(ctx).Warn("Failed to create fence object",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Int64("fenceBlockId", fenceBlockId),
					zap.Error(putErr))
				return putErr
			}

			// Successfully created fence object
			logger.Ctx(ctx).Info("Successfully created fence object",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("fenceObjectKey", fenceObjectKey),
				zap.Int64("fenceBlockId", fenceBlockId),
				zap.Int64("lastEntryId", lastEntryID))
			return nil
		},
		retry.Attempts(5),                 // Retry up to 5 times
		retry.Sleep(100*time.Millisecond), // Initial sleep between retries
		retry.MaxSleepTime(1*time.Second), // Max sleep time between retries
		retry.RetryErr(func(err error) bool {
			// Only retry on block already exists error
			return werr.ErrFragmentAlreadyExists.Is(err)
		}),
	)

	if err != nil {
		logger.Ctx(ctx).Warn("Failed to create fence object after retries",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("fenceObjectKey", fenceObjectKey),
			zap.Error(err))
		return -1, fmt.Errorf("failed to create fence object %s: %w", fenceObjectKey, err)
	}

	// Mark as fenced
	f.fenced.Store(true)

	logger.Ctx(ctx).Info("Successfully fenced segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("lastBlockID", f.lastPartID.Load()),
		zap.Int64("lastEntryId", f.lastEntryID.Load()))
	return f.lastEntryID.Load(), nil
}

func (f *MinioFileWriter) Recover(ctx context.Context) (int64, int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Recover")
	defer sp.End()
	if f.recovered.Load() {
		return f.lastEntryID.Load(), f.lastModifiedTime, nil
	}

	recoverErr := f.recoverFromStorage(ctx)
	if recoverErr != nil {
		return -1, -1, recoverErr
	}

	return f.lastEntryID.Load(), f.lastModifiedTime, nil
}

func (f *MinioFileWriter) prepareMultiBlockDataIfNecessary(toFlushData []*cache.BufferEntry, toFlushDataFirstEntryId int64) ([][]*cache.BufferEntry, []int64, []int64) {
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

// createSegmentLock creates a lock object in object storage for segment exclusivity
func (f *MinioFileWriter) createSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "createSegmentLock")
	defer sp.End()

	// Create lock object key
	f.lockObjectKey = getSegmentLockKey(f.segmentFileKey)

	// Create lock object with segment information
	lockInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nhostname=%s\n",
		f.logId, f.segmentId, os.Getpid(), time.Now().Unix(), getHostname())

	// Use PutObjectIfNoneMatch to atomically create lock object
	_, err := f.client.PutObjectIfNoneMatch(ctx, f.bucket, f.lockObjectKey,
		strings.NewReader(lockInfo), int64(len(lockInfo)))
	if err != nil {
		if werr.ErrFragmentAlreadyExists.Is(err) {
			logger.Ctx(ctx).Warn("Lock object already exists - segment is already locked by another process",
				zap.String("lockObjectKey", f.lockObjectKey))
			return fmt.Errorf("segment is already locked by another process: %s", f.lockObjectKey)
		}
		logger.Ctx(ctx).Warn("Failed to create lock object",
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
func (f *MinioFileWriter) releaseSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "releaseSegmentLock")
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

// serialize serializes entries with optional HeaderRecord for the first block
func (f *MinioFileWriter) serialize(entries []*cache.BufferEntry) []byte {
	if len(entries) == 0 {
		return []byte{}
	}

	serializedData := make([]byte, 0)

	// Add HeaderRecord only for the first block (when headerWritten is false)
	if !f.headerWritten.Load() {
		firstEntryID := entries[0].EntryId
		headerRecord := &codec.HeaderRecord{
			Version:      codec.FormatVersion,
			Flags:        0,
			FirstEntryID: firstEntryID,
		}
		encodedHeaderRecord := codec.EncodeRecord(headerRecord)
		serializedData = append(serializedData, encodedHeaderRecord...)

		// Mark header as written
		f.headerWritten.Store(true)
	}

	// Serialize all data records
	for _, entry := range entries {
		dataRecord, _ := codec.ParseData(entry.Data)
		encodedRecord := codec.EncodeRecord(dataRecord)
		serializedData = append(serializedData, encodedRecord...)
	}

	// Add BlockLastRecord at the end of the block
	firstEntryID := entries[0].EntryId
	lastEntryID := entries[len(entries)-1].EntryId

	blockLastRecord := &codec.BlockLastRecord{
		FirstEntryID: firstEntryID,
		LastEntryID:  lastEntryID,
	}

	encodedBlockLastRecord := codec.EncodeRecord(blockLastRecord)
	serializedData = append(serializedData, encodedBlockLastRecord...)

	return serializedData
}

// streamMergeAndUploadBlocks streams through blocks, merges them and uploads immediately
func (f *MinioFileWriter) streamMergeAndUploadBlocks(ctx context.Context, targetBlockSize int64) ([]*codec.IndexRecord, []int64, error) {
	if len(f.blockIndexes) == 0 {
		return nil, nil, nil
	}

	// Sort blocks by block number to ensure correct order
	sort.Slice(f.blockIndexes, func(i, j int) bool {
		return f.blockIndexes[i].BlockNumber < f.blockIndexes[j].BlockNumber
	})

	var newBlockIndexes []*codec.IndexRecord
	var uploadedBlockIDs []int64
	var currentMergedBlock []byte
	var currentMergedSize int64
	var currentEntryID int64
	var mergedBlockID int64 = 0
	var isFirstMergedBlock = true

	// Initialize current entry ID
	if f.firstEntryID.Load() != -1 {
		currentEntryID = f.firstEntryID.Load()
	} else {
		currentEntryID = 0
	}

	// Helper function to upload current merged block
	uploadCurrentBlock := func() error {
		if len(currentMergedBlock) == 0 {
			return nil
		}

		blockIndex, blockID, err := f.uploadSingleMergedBlock(ctx, currentMergedBlock, mergedBlockID, currentEntryID, isFirstMergedBlock)
		if err != nil {
			return err
		}

		newBlockIndexes = append(newBlockIndexes, blockIndex)
		uploadedBlockIDs = append(uploadedBlockIDs, blockID)

		// Update for next block
		currentEntryID = blockIndex.LastEntryID + 1
		mergedBlockID++
		isFirstMergedBlock = false
		currentMergedBlock = nil
		currentMergedSize = 0

		return nil
	}

	// Process each original block
	for _, blockIndex := range f.blockIndexes {
		// Read the block data
		blockKey := getPartKey(f.segmentFileKey, int64(blockIndex.BlockNumber))
		blockData, err := f.readBlockData(ctx, blockKey)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block data",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", blockKey),
				zap.Error(err))
			return nil, nil, fmt.Errorf("failed to read block %d: %w", blockIndex.BlockNumber, err)
		}

		// Extract only data records (skip header and block last records)
		dataRecords, err := f.extractDataRecords(blockData)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to extract data records",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", blockKey),
				zap.Error(err))
			return nil, nil, fmt.Errorf("failed to extract data records from block %d: %w", blockIndex.BlockNumber, err)
		}

		// Check if adding this block would exceed target size
		if currentMergedSize+int64(len(dataRecords)) > targetBlockSize && len(currentMergedBlock) > 0 {
			// Upload current merged block before continuing
			if err := uploadCurrentBlock(); err != nil {
				return nil, nil, fmt.Errorf("failed to upload merged block %d: %w", mergedBlockID, err)
			}
		}

		// Add data records to current merged block
		currentMergedBlock = append(currentMergedBlock, dataRecords...)
		currentMergedSize += int64(len(dataRecords))

		logger.Ctx(ctx).Debug("processed block for streaming merge",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int32("blockNumber", blockIndex.BlockNumber),
			zap.Int("dataRecordsSize", len(dataRecords)),
			zap.Int64("currentMergedSize", currentMergedSize))
	}

	// Upload the last merged block if it has data
	if len(currentMergedBlock) > 0 {
		if err := uploadCurrentBlock(); err != nil {
			return nil, nil, fmt.Errorf("failed to upload final merged block %d: %w", mergedBlockID, err)
		}
	}

	logger.Ctx(ctx).Info("completed streaming merge and upload",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("originalBlockCount", len(f.blockIndexes)),
		zap.Int("mergedBlockCount", len(newBlockIndexes)))

	return newBlockIndexes, uploadedBlockIDs, nil
}

// readBlockData reads the complete data of a block
func (f *MinioFileWriter) readBlockData(ctx context.Context, blockKey string) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "readBlockData")
	defer sp.End()
	obj, err := f.client.GetObject(ctx, f.bucket, blockKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", blockKey, err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data %s: %w", blockKey, err)
	}

	return data, nil
}

// extractDataRecords extracts only data records from a block, skipping header and block last records
func (f *MinioFileWriter) extractDataRecords(blockData []byte) ([]byte, error) {
	records, err := codec.DecodeRecordList(blockData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode records: %w", err)
	}

	var dataRecords []byte
	for _, record := range records {
		if record.Type() == codec.DataRecordType {
			encodedRecord := codec.EncodeRecord(record)
			dataRecords = append(dataRecords, encodedRecord...)
		}
	}

	return dataRecords, nil
}

// uploadSingleMergedBlock uploads a single merged block with m_ prefix
func (f *MinioFileWriter) uploadSingleMergedBlock(ctx context.Context, mergedBlockData []byte, mergedBlockID int64, firstEntryID int64, isFirstBlock bool) (*codec.IndexRecord, int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "uploadSingleMergedBlock")
	defer sp.End()
	// Add header record at the beginning if this is the first block
	var completeBlockData []byte
	if isFirstBlock {
		headerRecord := &codec.HeaderRecord{
			Version:      codec.FormatVersion,
			Flags:        0,
			FirstEntryID: firstEntryID,
		}
		encodedHeader := codec.EncodeRecord(headerRecord)
		completeBlockData = append(completeBlockData, encodedHeader...)
	}

	// Add the merged data records
	completeBlockData = append(completeBlockData, mergedBlockData...)

	// Count data records to calculate entry range
	records, err := codec.DecodeRecordList(mergedBlockData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode merged block data: %w", err)
	}

	dataRecordCount := 0
	for _, record := range records {
		if record.Type() == codec.DataRecordType {
			dataRecordCount++
		}
	}

	blockFirstEntryID := firstEntryID
	blockLastEntryID := firstEntryID + int64(dataRecordCount) - 1

	// Add block last record
	blockLastRecord := &codec.BlockLastRecord{
		FirstEntryID: blockFirstEntryID,
		LastEntryID:  blockLastEntryID,
	}
	encodedBlockLast := codec.EncodeRecord(blockLastRecord)
	completeBlockData = append(completeBlockData, encodedBlockLast...)

	// Upload merged block with m_ prefix (use PutObject for idempotent overwrites)
	mergedBlockKey := getMergedPartKey(f.segmentFileKey, mergedBlockID)
	_, putErr := f.client.PutObject(ctx, f.bucket, mergedBlockKey,
		bytes.NewReader(completeBlockData), int64(len(completeBlockData)), minio.PutObjectOptions{})
	if putErr != nil {
		logger.Ctx(ctx).Warn("failed to upload merged block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("mergedBlockKey", mergedBlockKey),
			zap.Error(putErr))
		return nil, 0, fmt.Errorf("failed to upload merged block %d: %w", mergedBlockID, putErr)
	}

	// Create index record for the merged block
	indexRecord := &codec.IndexRecord{
		BlockNumber:       int32(mergedBlockID),
		StartOffset:       mergedBlockID,
		FirstRecordOffset: 0,
		FirstEntryID:      blockFirstEntryID,
		LastEntryID:       blockLastEntryID,
	}

	logger.Ctx(ctx).Info("uploaded merged block",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("mergedBlockKey", mergedBlockKey),
		zap.Int64("blockFirstEntryID", blockFirstEntryID),
		zap.Int64("blockLastEntryID", blockLastEntryID),
		zap.Int("blockSize", len(completeBlockData)))

	return indexRecord, mergedBlockID, nil
}

// serializeCompactedFooterAndIndexes serializes the footer and indexes for compacted segment
func (f *MinioFileWriter) serializeCompactedFooterAndIndexes(ctx context.Context, blockIndexes []*codec.IndexRecord, footer *codec.FooterRecord) []byte {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "serializeCompactedFooterAndIndexes")
	defer sp.End()
	serializedData := make([]byte, 0)

	// Serialize all block index records
	for _, record := range blockIndexes {
		encodedRecord := codec.EncodeRecord(record)
		serializedData = append(serializedData, encodedRecord...)
	}

	// Serialize footer record
	encodedFooter := codec.EncodeRecord(footer)
	serializedData = append(serializedData, encodedFooter...)

	logger.Ctx(ctx).Debug("serialized compacted footer and indexes",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("blockCount", len(blockIndexes)),
		zap.Int("totalSize", len(serializedData)))

	return serializedData
}

// PartInfo is the information of a part
type PartInfo struct {
	FirstEntryID int64
	LastEntryID  int64
	PartKey      string
	PartID       int64
	Size         int64
}

// blockUploadTask is the task for flush.
type blockUploadTask struct {
	flushData             []*cache.BufferEntry
	flushDataFirstEntryId int64
	flushFuture           *conc.Future[*blockUploadResult]
}

// blockUploadResult is the result of flush operation
type blockUploadResult struct {
	part *PartInfo
	err  error
}

// utils for Part key
func getPartKey(segmentFileKey string, partID int64) string {
	return fmt.Sprintf("%s/%d.blk", segmentFileKey, partID)
}

// utils for merged part key
func getMergedPartKey(segmentFileKey string, mergedPartID int64) string {
	return fmt.Sprintf("%s/m_%d.blk", segmentFileKey, mergedPartID)
}

func getSegmentFileKey(baseDir string, logId int64, segmentId int64) string {
	return fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)
}

func getSegmentLockKey(segmentFileKey string) string {
	return fmt.Sprintf("%s/write.lock", segmentFileKey)
}

func getFooterPartKey(segmentFileKey string) string {
	return fmt.Sprintf("%s/footer.blk", segmentFileKey)
}

// utils to parse object key
func parseFilePartName(key string) (id int64, isMerge bool, err error) {
	filename := filepath.Base(key)
	name := strings.TrimSuffix(filename, ".blk")
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

// getHostname returns the hostname for lock identification
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func serializeFooterAndIndexes(ctx context.Context, blocks []*codec.IndexRecord) ([]byte, *codec.FooterRecord) {
	serializedData := make([]byte, 0)
	// Serialize all block index records
	for _, record := range blocks {
		encodedRecord := codec.EncodeRecord(record)
		serializedData = append(serializedData, encodedRecord...)
	}
	indexLength := len(serializedData)

	// Verify that indexLength is a multiple of 36 (IndexRecord size)
	expectedRecordSize := codec.RecordHeaderSize + 36 // 9 + 36 = 45 bytes per IndexRecord
	if indexLength%(expectedRecordSize) != 0 {
		logger.Ctx(ctx).Warn("Index length is not a multiple of expected record size",
			zap.Int("indexLength", indexLength),
			zap.Int("expectedRecordSize", expectedRecordSize),
			zap.Int("recordCount", len(blocks)))
	}

	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blocks)),
		TotalRecords: uint32(len(blocks)),
		IndexOffset:  0,
		IndexLength:  uint32(indexLength),
		Version:      codec.FormatVersion,
		Flags:        0,
	}

	encodedFooterRecord := codec.EncodeRecord(footer)
	serializedData = append(serializedData, encodedFooterRecord...)
	return serializedData, footer
}
