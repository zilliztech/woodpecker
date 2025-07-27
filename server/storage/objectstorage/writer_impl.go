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
	"hash/crc32"
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
// Each flush operation creates a block of the segment file
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
	lastBlockID      atomic.Int64 // The last blockId of this Segment which already written to object storage
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
	lastSubmittedUploadingBlockID atomic.Int64
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
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "NewWriter")
	defer sp.End()
	segmentFileKey := getSegmentFileKey(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("creating new minio file writer", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId))
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
	segmentFileWriter.lastBlockID.Store(-1)
	segmentFileWriter.lastSubmittedUploadingBlockID.Store(-1)
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
		if err := segmentFileWriter.recoverFromStorageUnsafe(ctx); err != nil {
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
	logger.Ctx(ctx).Info("create new minio file writer finish", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId))
	return segmentFileWriter, nil
}

// recoverFromStorage attempts to recover the writer state from existing objects in MinIO
func (f *MinioFileWriter) recoverFromStorageUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "recoverFromStorage")
	defer sp.End()
	logger.Ctx(ctx).Info("attempting to recover writer state from storage",
		zap.String("segmentFileKey", f.segmentFileKey))

	footerBlockKey := getFooterBlockKey(f.segmentFileKey)
	footerBlockObjInfo, err := f.client.StatObject(ctx, f.bucket, footerBlockKey, minio.StatObjectOptions{})
	if err != nil && minioHandler.IsObjectNotExists(err) {
		return f.recoverFromFullListing(ctx)
	}
	return f.recoverFromFooter(ctx, footerBlockKey, footerBlockObjInfo)
}

func (f *MinioFileWriter) recoverFromFooter(ctx context.Context, footerBlockKey string, footerBlockObjInfo minio.ObjectInfo) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "recoverFromFooter")
	defer sp.End()
	startTime := time.Now()
	// Read the entire footer.blk file
	footerObj, err := f.client.GetObject(ctx, f.bucket, footerBlockKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer footerObj.Close()
	f.lastModifiedTime = footerBlockObjInfo.LastModified.UnixMilli()

	footerBlkData, err := minioHandler.ReadObjectFull(ctx, footerObj, footerBlockObjInfo.Size)
	if err != nil {
		return err
	}

	logger.Ctx(ctx).Debug("read entire footer.blk",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("footerBlkSize", footerBlockObjInfo.Size),
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
	var maxBlockID int64 = -1

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
		if int64(indexRecord.BlockNumber) > maxBlockID {
			maxBlockID = int64(indexRecord.BlockNumber)
		}

		// Move to next record (header + payload)
		recordSize := codec.RecordHeaderSize + codec.IndexRecordSize // IndexRecord payload size
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
	if maxBlockID != -1 {
		f.lastBlockID.Store(maxBlockID)
		f.lastSubmittedUploadingBlockID.Store(maxBlockID)
		f.lastSubmittedUploadingEntryID.Store(lastEntryID)
	}

	f.recovered.Store(true)
	logger.Ctx(ctx).Info("successfully parsed footer and index records",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexRecordCount", len(f.blockIndexes)),
		zap.Int32("expectedTotalBlocks", f.footerRecord.TotalBlocks),
		zap.Int64("recoveredFirstEntryID", firstEntryID),
		zap.Int64("recoveredLastEntryID", lastEntryID),
		zap.Int64("recoveredLastBlockID", maxBlockID))
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "recover_footer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "recover_footer", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

func (f *MinioFileWriter) recoverFromFullListing(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "recoverFromFullListing")
	defer sp.End()
	startTime := time.Now()
	logger.Ctx(ctx).Info("recovering from storage by sequentially reading blocks",
		zap.String("segmentFileKey", f.segmentFileKey))

	// Try to read each data object to rebuild block indexes
	var firstEntryID int64 = -1
	var lastEntryID int64 = -1
	var tempIndexRecords []*codec.IndexRecord
	lastModifiedTime := int64(0)

	// fence state recover
	fenceBlockId := int64(-1)

	// Sequentially try to read blocks starting from ID 0
	// Block sequence must be continuous without gaps - if any block is missing, stop recovery
	blockID := int64(0)
	for {
		blockKey := getBlockKey(f.segmentFileKey, blockID)

		// Try to get object info first to check if it exists and get last modified time
		objInfo, stateErr := f.client.StatObject(ctx, f.bucket, blockKey, minio.StatObjectOptions{})
		if stateErr != nil {
			if minioHandler.IsObjectNotExists(stateErr) {
				// Block doesn't exist, we've reached the end of continuous sequence
				logger.Ctx(ctx).Debug("block not found, stopping recovery as sequence must be continuous",
					zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockID", blockID), zap.String("blockKey", blockKey))
				break
			}
			// Other errors (network issues, permissions, etc.) should also stop recovery
			logger.Ctx(ctx).Warn("failed to stat object during recovery, please retry later",
				zap.String("blockKey", blockKey), zap.Error(stateErr))
			return stateErr
		}
		if minioHandler.IsFencedObject(objInfo) {
			logger.Ctx(ctx).Warn("object is fenced, stopping recovery",
				zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("fenceBlockID", blockID), zap.String("blockKey", blockKey))
			fenceBlockId = blockID
			break
		}

		// Update last modified time
		if objInfo.LastModified.UnixMilli() > lastModifiedTime {
			lastModifiedTime = objInfo.LastModified.UnixMilli()
		}

		// Parse the data to find blockHeaderRecord
		logger.Ctx(ctx).Debug("attempting to parse block header record during recovery",
			zap.String("blockKey", blockKey),
			zap.Int64("dataSize", objInfo.Size))

		blockHeaderRecord, parseBlockHeaderErr := f.parseBlockHeaderRecord(ctx, blockID, blockKey)
		if parseBlockHeaderErr != nil {
			logger.Ctx(ctx).Warn("failed to parse block header record during recovery, please retry later",
				zap.String("blockKey", blockKey),
				zap.Error(parseBlockHeaderErr))
			return parseBlockHeaderErr
		}

		// Create index record for this block
		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(blockID),
			StartOffset:  blockID,
			BlockSize:    uint32(objInfo.Size), // Use object size as block size
			FirstEntryID: blockHeaderRecord.FirstEntryID,
			LastEntryID:  blockHeaderRecord.LastEntryID,
		}

		tempIndexRecords = append(tempIndexRecords, indexRecord)

		if firstEntryID == -1 || blockHeaderRecord.FirstEntryID < firstEntryID {
			firstEntryID = blockHeaderRecord.FirstEntryID
		}
		if blockHeaderRecord.LastEntryID > lastEntryID {
			lastEntryID = blockHeaderRecord.LastEntryID
		}

		logger.Ctx(ctx).Debug("recovered block during recovery",
			zap.String("blockKey", blockKey),
			zap.Int64("blockID", blockID),
			zap.Int32("readBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Int64("firstEntryID", blockHeaderRecord.FirstEntryID),
			zap.Int64("lastEntryID", blockHeaderRecord.LastEntryID))

		blockID++
	}

	f.lastModifiedTime = lastModifiedTime
	if fenceBlockId > -1 {
		logger.Ctx(ctx).Info("fence block found, fence state recovered",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("fenceBlockId", fenceBlockId))
		f.fenced.Store(true)
	}

	if len(tempIndexRecords) == 0 {
		logger.Ctx(ctx).Info("no existing data objects found, recover completed",
			zap.String("segmentFileKey", f.segmentFileKey))
		f.recovered.Store(true)
		metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "recover_raw", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "recover_raw", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return nil
	}

	// Index records are already in correct order since we read them sequentially
	f.blockIndexes = tempIndexRecords

	// Update writer state
	if firstEntryID != -1 {
		f.firstEntryID.Store(firstEntryID)
	}
	if lastEntryID != -1 {
		f.lastEntryID.Store(lastEntryID)
	}

	// Calculate maxBlockID from the recovered blocks
	var maxBlockID int64 = -1
	if len(tempIndexRecords) > 0 {
		// Since blocks are sequential, the last block has the maximum ID
		maxBlockID = int64(tempIndexRecords[len(tempIndexRecords)-1].BlockNumber)
		f.lastBlockID.Store(maxBlockID)
		f.lastSubmittedUploadingBlockID.Store(maxBlockID)
		f.lastSubmittedUploadingEntryID.Store(lastEntryID)
	}

	logger.Ctx(ctx).Info("successfully recovered writer state from storage",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("blockIndexCount", len(f.blockIndexes)),
		zap.Int64("firstEntryID", firstEntryID),
		zap.Int64("lastEntryID", lastEntryID),
		zap.Int64("lastBlockID", maxBlockID))

	f.recovered.Store(true)
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "recover_raw", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "recover_raw", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// parseBlockHeaderRecord extracts the BlockHeaderRecord from the end of block data
func (f *MinioFileWriter) parseBlockHeaderRecord(ctx context.Context, blockID int64, blockKey string) (*codec.BlockHeaderRecord, error) {
	// For the first block (block 0), we need to read more data to account for the HeaderRecord
	// For other blocks, we only need to read the BlockHeaderRecord
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)

	// Check if this is the first block (block 0)
	if blockID == 0 {
		// First block has HeaderRecord + BlockHeaderRecord, so we need to read more
		readSize = int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	}

	// get block header record from the beginning of the block
	getBlockHeaderRecordOpt := minio.GetObjectOptions{}
	setOptErr := getBlockHeaderRecordOpt.SetRange(0, readSize-1)
	if setOptErr != nil {
		logger.Ctx(ctx).Warn("Error setting range for block header record",
			zap.String("blockKey", blockKey),
			zap.Error(setOptErr))
		return nil, setOptErr
	}
	headerRecordObj, getErr := f.client.GetObject(ctx, f.bucket, blockKey, getBlockHeaderRecordOpt)
	if getErr != nil {
		logger.Ctx(ctx).Warn("Error getting block header record",
			zap.String("blockKey", blockKey),
			zap.Error(getErr))
		return nil, getErr
	}
	defer headerRecordObj.Close()
	data, readErr := io.ReadAll(headerRecordObj)
	if readErr != nil {
		logger.Ctx(ctx).Warn("failed to read object data during prefetch, please retry later",
			zap.String("blockKey", blockKey),
			zap.Error(readErr))
		return nil, readErr
	}

	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	logger.Ctx(ctx).Info("parsing block data for BlockHeaderRecord",
		zap.Int("dataSize", len(data)))

	// Parse all records in the data to find the BlockHeaderRecord
	// The BlockHeaderRecord should be at the beginning of the block
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

	// Look for BlockHeaderRecord from the beginning (after optional HeaderRecord)
	for i := 0; i < len(records); i++ {
		logger.Ctx(ctx).Info("checking record type",
			zap.Int("recordIndex", i),
			zap.String("recordType", fmt.Sprintf("%T", records[i])))

		if blockHeaderRecord, ok := records[i].(*codec.BlockHeaderRecord); ok {
			logger.Ctx(ctx).Info("found BlockHeaderRecord",
				zap.Int32("blockNumber", blockHeaderRecord.BlockNumber),
				zap.Int64("firstEntryID", blockHeaderRecord.FirstEntryID),
				zap.Int64("lastEntryID", blockHeaderRecord.LastEntryID))
			return blockHeaderRecord, nil
		}
	}

	return nil, errors.New("BlockHeaderRecord not found")
}

// Like OS file fsync dirty pageCache periodically, objectStoreFile will sync buffer to object storage periodically
func (f *MinioFileWriter) run() {
	// time ticker
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()
	f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	logIdStr := fmt.Sprintf("%d", f.logId)
	metrics.WpFileWriters.WithLabelValues(logIdStr).Inc()
	logger.Ctx(context.TODO()).Debug("writer start", zap.String("segmentFileKey", f.segmentFileKey), zap.Int("maxIntervalMs", f.maxIntervalMs), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
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
			ctx, sp := logger.NewIntentCtx(SegmentWriterScope, "run_sync")
			err := f.Sync(ctx)
			if err != nil {
				logger.Ctx(ctx).Warn("sync error",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Error(err))
			}
			sp.End()
			ticker.Reset(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
		case <-f.fileClose:
			logger.Ctx(context.TODO()).Debug("close segment file writer", zap.String("segmentFileKey", f.segmentFileKey), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
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
					zap.String("firstFlushErrBlock", firstUploadErrTask.flushFuture.Value().block.BlockKey),
					zap.String("flushSuccessBlock", task.flushFuture.Value().block.BlockKey))
				f.fastFlushFailUnsafe(context.TODO(), task.flushData, firstUploadErrTask.flushFuture.Value().err)
			} else {
				// update flush state
				result := task.flushFuture.Value()
				flushedFirst := result.block.FirstEntryID // always no error, because it's just created
				flushedLast := result.block.LastEntryID   // always no error, because it's just created
				flushedBlockID := result.block.BlockID
				if flushedLast >= 0 {
					f.lastEntryID.Store(flushedLast)
					f.lastBlockID.Store(flushedBlockID)
					if f.firstEntryID.Load() == -1 {
						// Initialize firstEntryId on first successful flush
						// This should always be 0 for the initial flush
						f.firstEntryID.Store(flushedFirst)
					}
				}

				logger.Ctx(context.TODO()).Info("flush success, fast success ack",
					zap.String("block", task.flushFuture.Value().block.BlockKey),
					zap.Int64("firstEntryID", task.flushFuture.Value().block.FirstEntryID),
					zap.Int64("lastEntryID", task.flushFuture.Value().block.LastEntryID),
					zap.Int64("blockSize", task.flushFuture.Value().block.Size))
				// flush success ack
				f.fastFlushSuccessUnsafe(context.TODO(), task.flushFuture.Value().block, task.flushData)
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
			logger.Ctx(context.TODO()).Info("flush error encountered, trigger fast flush fail",
				zap.String("block", task.flushFuture.Value().block.BlockKey),
				zap.Int64("firstEntryID", task.flushFuture.Value().block.FirstEntryID),
				zap.Int64("lastEntryID", task.flushFuture.Value().block.LastEntryID),
				zap.String("firstFlushErrBlock", firstUploadErrTask.flushFuture.Value().block.BlockKey))
			f.fastFlushFailUnsafe(context.TODO(), task.flushData, task.flushFuture.Value().err)
		}
		f.flushingBufferSize.Add(-task.flushFuture.Value().block.Size)
	}
}

func (f *MinioFileWriter) GetId() int64 {
	return f.segmentId
}

func (f *MinioFileWriter) WriteDataAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "AppendAsync")
	defer sp.End()
	startTime := time.Now()

	// Validate that data is not empty
	if len(data) == 0 {
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, data cannot be empty", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrEmptyPayload
	}

	if f.closed.Load() {
		// quick fail and return a close Err, which indicate than it is also not retriable
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, segment writer is closed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrFileWriterAlreadyClosed
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
		sp.AddEvent("wait sync", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
		if err != nil {
			// sync does not success
			logger.Ctx(ctx).Warn("AppendAsync: found buffer full, but sync failed before append", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err))
			return entryId, err
		}
	}

	f.mu.Lock()
	sp.AddEvent("wait lock", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
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
	startTime := time.Now()
	logger.Ctx(ctx).Debug("waitIfFlushingBufferSizeExceededUnsafe: checking flushing buffer size", zap.String("segmentFileKey", f.segmentFileKey))
	// Check if current flushing buffer size exceeds the maximum allowed buffer size
	for {
		currentFlushingSize := f.flushingBufferSize.Load()
		if currentFlushingSize < f.maxBufferSize {
			// Safe to proceed, flushing buffer size is within limits
			logger.Ctx(ctx).Debug("Flushing buffer size check passed",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("currentFlushingSize", currentFlushingSize),
				zap.Int64("maxBufferSize", f.maxBufferSize),
				zap.Int64("elapsedTime", time.Since(startTime).Milliseconds()))
			return nil
		}

		// Flushing buffer size exceeded, need to wait
		logger.Ctx(ctx).Debug("Flushing buffer size exceeded, waiting for space",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("currentFlushingSize", currentFlushingSize),
			zap.Int64("maxBufferSize", f.maxBufferSize),
			zap.Int64("elapsedTime", time.Since(startTime).Milliseconds()))

		// Wait for a short period before checking again
		select {
		case <-ctx.Done():
			// Context cancelled, return immediately
			logger.Ctx(ctx).Warn("Context cancelled while waiting for flushing buffer space",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("elapsedTime", time.Since(startTime).Milliseconds()))
			return ctx.Err()
		case <-f.fileClose:
			// Segment is being closed, return immediately
			logger.Ctx(ctx).Debug("Segment close signal received while waiting for flushing buffer space",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("elapsedTime", time.Since(startTime).Milliseconds()))
			return werr.ErrFileWriterAlreadyClosed
		case <-time.After(10 * time.Millisecond):
			// Continue checking after a short delay
			continue
		}
	}
}

func (f *MinioFileWriter) Compact(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Compact")
	defer sp.End()
	startTime := time.Now()

	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Ctx(ctx).Info("starting segment compaction",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("currentBlockCount", len(f.blockIndexes)))

	// Check if segment is already compacted
	if f.footerRecord != nil && codec.IsCompacted(f.footerRecord.Flags) {
		logger.Ctx(ctx).Info("segment is already compacted, skipping",
			zap.String("segmentFileKey", f.segmentFileKey))
		return int64(f.footerRecord.TotalSize), nil
	}

	// Ensure segment is finalized before compaction
	if f.footerRecord == nil {
		logger.Ctx(ctx).Warn("segment must be finalized before compaction",
			zap.String("segmentFileKey", f.segmentFileKey))
		return -1, fmt.Errorf("segment must be finalized before compaction")
	}

	// Get target block size for compaction (use maxFlushSize as target)
	targetBlockSize := f.syncPolicyConfig.MaxFlushSize
	if targetBlockSize <= 0 {
		targetBlockSize = 2 * 1024 * 1024 // Default 2MB
	}

	// Stream merge and upload blocks
	newBlockIndexes, fileSizeAfterCompact, err := f.streamMergeAndUploadBlocks(ctx, targetBlockSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stream merge and upload blocks",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(err))
		return -1, fmt.Errorf("failed to stream merge and upload blocks: %w", err)
	}

	if len(newBlockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks to compact",
			zap.String("segmentFileKey", f.segmentFileKey))
		return -1, nil
	}

	// Create new footer with compacted flag
	newFooter := &codec.FooterRecord{
		TotalBlocks:  int32(len(newBlockIndexes)),
		TotalRecords: f.footerRecord.TotalRecords,
		TotalSize:    uint64(fileSizeAfterCompact),
		IndexOffset:  0,
		IndexLength:  uint32(len(newBlockIndexes) * (codec.RecordHeaderSize + codec.IndexRecordSize)), // IndexRecord size
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(f.footerRecord.Flags), // Set compacted flag=1 (bit 0)
	}

	// Serialize new footer and indexes
	footerData := f.serializeCompactedFooterAndIndexes(ctx, newBlockIndexes, newFooter)

	// Upload new footer
	footerKey := getFooterBlockKey(f.segmentFileKey)
	_, putErr := f.client.PutObject(ctx, f.bucket, footerKey, bytes.NewReader(footerData), int64(len(footerData)), minio.PutObjectOptions{})
	if putErr != nil {
		logger.Ctx(ctx).Warn("failed to upload compacted footer",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Error(putErr))
		return -1, fmt.Errorf("failed to upload compacted footer: %w", putErr)
	}

	// Update internal state
	originalBlockCount := len(f.blockIndexes)
	f.blockIndexes = newBlockIndexes
	f.footerRecord = newFooter

	logger.Ctx(ctx).Info("successfully compacted segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("originalBlockCount", originalBlockCount),
		zap.Int("compactedBlockCount", len(newBlockIndexes)),
		zap.Int64("fileSizeAfterCompact", fileSizeAfterCompact))
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "compact", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "compact", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return fileSizeAfterCompact, nil
}

// Sync Implement sync logic, e.g., flush to persistent storage
func (f *MinioFileWriter) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Sync")
	defer sp.End()
	startTime := time.Now()
	f.syncMu.Lock() // ensure only one sync operation is running at a time
	defer f.syncMu.Unlock()
	defer func() {
		f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	}()
	syncLockWaitFinishTime := time.Now()
	sp.AddEvent("wait syncMu lock", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))

	if !f.storageWritable.Load() {
		logger.Ctx(ctx).Warn("Call Sync, but storage is not writable, quick fail all append requests", zap.String("segmentFileKey", f.segmentFileKey))
		return f.quickSyncFailUnsafe(ctx, werr.ErrStorageNotWritable)
	}

	// roll buff with lock
	f.mu.Lock()
	lockWaitFinishTime := time.Now()
	sp.AddEvent("wait lock", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	currentBuffer, toFlushData, toFlushDataFirstEntryId, err := f.rollBufferUnsafe(ctx)
	sp.AddEvent("wait rollBuff", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	f.mu.Unlock()
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesRangeData failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return err
	}
	if len(toFlushData) == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentFileKey", f.segmentFileKey), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	logger.Ctx(ctx).Debug("Sync start to submit flush tasks",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("syncLockWait", time.Since(syncLockWaitFinishTime).Milliseconds()),
		zap.Int64("writeLockWait", time.Since(lockWaitFinishTime).Milliseconds()),
	)
	// submit async flush task
	flushResultFutures := f.submitBlockFlushTaskUnsafe(ctx, currentBuffer, toFlushData, toFlushDataFirstEntryId)
	sp.AddEvent("submit task", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))
	logger.Ctx(ctx).Debug("Sync submitted flush tasks",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("blocks", len(flushResultFutures)),
		zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId),
		zap.Int("toFlushEntries", len(toFlushData)),
		zap.Int64("restDataFirstEntryId", currentBuffer.GetExpectedNextEntryId()),
		zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "sync", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "sync", "success").Observe(float64(time.Since(startTime).Milliseconds()))

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
	sp.AddEvent("wait available flushing buff quota", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))

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
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "rollBuffer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "rollBuffer", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return currentBuffer, toFlushData, toFlushDataFirstEntryId, nil
}

func (f *MinioFileWriter) fastFlushFailUnsafe(ctx context.Context, blockData []*cache.BufferEntry, resultErr error) {
	for _, item := range blockData {
		cache.NotifyPendingEntryDirectly(ctx, f.logId, f.segmentId, item.EntryId, item.NotifyChan, -1, resultErr)
	}
}

func (f *MinioFileWriter) fastFlushSuccessUnsafe(ctx context.Context, blockInfo *BlockInfo, blockData []*cache.BufferEntry) {
	// Calculate cumulative offset from previous blocks
	var cumulativeOffset int64 = 0
	for _, indexRecord := range f.blockIndexes {
		// Use the actual size from BlockInfo instead of just incrementing by 1
		cumulativeOffset += indexRecord.StartOffset
	}

	f.blockIndexes = append(f.blockIndexes, &codec.IndexRecord{
		BlockNumber:  int32(blockInfo.BlockID),
		StartOffset:  blockInfo.BlockID,
		BlockSize:    uint32(blockInfo.Size), // Use block size from BlockInfo
		FirstEntryID: blockInfo.FirstEntryID,
		LastEntryID:  blockInfo.LastEntryID,
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
		blockId := f.lastSubmittedUploadingBlockID.Add(1) // block id
		blockFirstEntryId := blockFirstEntryIdList[i]
		blockDataBuff := blockData
		blockLastEntryId := blockFirstEntryId + int64(len(blockDataBuff)) - 1
		f.lastSubmittedUploadingEntryID.Store(blockLastEntryId)
		blockSize := blockSizeList[i] // Capture block size for the closure

		if waitBuffErr != nil {
			// if error exist, fast fail subsequent blocks
			f.fastFlushFailUnsafe(ctx, blockDataBuff, waitBuffErr)
			logger.Ctx(ctx).Warn("fast fail the flush task before submit",
				zap.Int64("logId", f.logId),
				zap.Int64("segmentId", f.segmentId),
				zap.Int64("blockId", blockId),
				zap.Int64("blockFirstEntryId", blockFirstEntryId),
				zap.Int("count", len(blockDataBuff)),
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Error(waitBuffErr))
		}

		// first wait available space
		waitErr := f.waitIfFlushingBufferSizeExceededUnsafe(ctx)
		if waitErr != nil {
			// sync interrupted, fast fail and notify all pending append entries
			f.fastFlushFailUnsafe(ctx, blockDataBuff, waitErr)
			logger.Ctx(ctx).Warn("fast fail the flush task before submit",
				zap.Int64("logId", f.logId),
				zap.Int64("segmentId", f.segmentId),
				zap.Int64("blockId", blockId),
				zap.Int64("blockFirstEntryId", blockFirstEntryId),
				zap.Int("count", len(blockDataBuff)),
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Error(waitErr))
			waitBuffErr = waitErr
			continue
		}

		// try to submit flush task
		resultFuture := f.pool.Submit(func() (*blockUploadResult, error) {
			flushTaskStart := time.Now()
			logger.Ctx(ctx).Debug("start flush one block", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Int("count", len(blockDataBuff)), zap.Int64("blockSize", blockSize))
			blockKey := getBlockKey(f.segmentFileKey, blockId)
			blockRawData := f.serialize(blockId, blockDataBuff)
			actualDataSize := int64(len(blockRawData))
			logger.Ctx(ctx).Debug("serialized block data", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Int64("originalBlockSize", blockSize), zap.Int64("actualDataSize", actualDataSize))
			flushErr := retry.Do(ctx,
				func() error {
					_, putErr := f.client.PutObjectIfNoneMatch(ctx, f.bucket, blockKey, bytes.NewReader(blockRawData), actualDataSize)
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
				logger.Ctx(ctx).Warn("flush one block failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Error(flushErr))
			}
			result := &blockUploadResult{
				block: &BlockInfo{
					FirstEntryID: blockFirstEntryId,
					LastEntryID:  blockFirstEntryId + int64(len(blockDataBuff)) - 1,
					BlockKey:     blockKey,
					BlockID:      blockId,
					Size:         actualDataSize,
				},
				err: flushErr,
			}
			logger.Ctx(ctx).Debug("complete flush one block", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockId", blockId), zap.Int("count", len(blockDataBuff)), zap.Int64("blockSize", blockSize))
			metrics.WpFileFlushBytesWritten.WithLabelValues(f.logIdStr).Add(float64(actualDataSize))
			metrics.WpFileFlushLatency.WithLabelValues(f.logIdStr).Observe(float64(time.Since(flushTaskStart).Milliseconds()))
			return result, flushErr
		})

		// update submit flushing size
		submitFlushingSize := blockSizeList[i]
		f.flushingBufferSize.Add(submitFlushingSize)
		f.flushingTaskList <- &blockUploadTask{
			flushData:             blockDataBuff,
			flushDataFirstEntryId: blockFirstEntryId,
			flushFuture:           resultFuture,
		}
		flushResultFutures = append(flushResultFutures, resultFuture)
	}

	logger.Ctx(ctx).Debug("submitted block flush tasks", zap.String("segmentFileKey", f.segmentFileKey), zap.Int("blocks", len(blockDataList)), zap.Int("submitted", len(flushResultFutures)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	return flushResultFutures
}

func (f *MinioFileWriter) awaitAllFlushTasks(ctx context.Context) error {
	logger.Ctx(ctx).Info("wait for all blocks to be flushed", zap.String("segmentFileKey", f.segmentFileKey))

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
	startTime := time.Now()
	if !f.closed.CompareAndSwap(false, true) { // mark close, and there will be no more add and sync in the future
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("SegmentImplInst", fmt.Sprintf("%p", f)))
		return -1, werr.ErrFileWriterAlreadyClosed
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
	footerBlockKey := getFooterBlockKey(f.segmentFileKey)
	logger.Ctx(ctx).Info("finalizing segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("footerBlockKey", footerBlockKey),
		zap.Int("blockIndexesCount", len(f.blockIndexes)),
		zap.Int64("lastBlockID", f.lastBlockID.Load()))

	footerBlockRawData, footer := serializeFooterAndIndexes(ctx, f.blockIndexes)
	logger.Ctx(ctx).Info("serialized footer and indexes",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("footerBlockRawData", len(footerBlockRawData)))

	_, putErr := f.client.PutObjectIfNoneMatch(ctx, f.bucket, footerBlockKey, bytes.NewReader(footerBlockRawData), int64(len(footerBlockRawData)))
	if putErr != nil {
		logger.Ctx(ctx).Warn("failed to put finalization object",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("footerBlockKey", footerBlockKey),
			zap.Error(putErr))
		return -1, fmt.Errorf("failed to put object: %w", putErr)
	}
	f.footerRecord = footer
	logger.Ctx(ctx).Info("successfully finalized segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("footerBlockKey", footerBlockKey))
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "finalize", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "finalize", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return f.GetLastEntryId(ctx), nil
}

func (f *MinioFileWriter) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Close")
	defer sp.End()
	startTime := time.Now()
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
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "close", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "close", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (f *MinioFileWriter) IsFenced(ctx context.Context) (bool, error) {
	return f.fenced.Load(), nil
}

func (f *MinioFileWriter) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentWriterScope, "Fence")
	defer sp.End()
	startTime := time.Now()
	logger.Ctx(ctx).Info("start to fence segment", zap.String("segmentFileKey", f.segmentFileKey))
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

	var fenceBlockKey string
	firstFenceAttempt := true

	// Use retry.Do to handle the fence object creation with retries
	err := retry.Do(ctx,
		func() error {
			// recover to find incremental new blocks
			if !firstFenceAttempt {
				recoverErr := f.recoverFromStorageUnsafe(ctx)
				if recoverErr != nil {
					logger.Ctx(ctx).Warn("Failed to recover from storage during fence",
						zap.String("segmentFileKey", f.segmentFileKey),
						zap.Error(recoverErr))
					return recoverErr
				}
			}
			firstFenceAttempt = false

			// get last block ID
			blocks := f.blockIndexes
			lastBlockID := int64(-1)
			if len(blocks) > 0 {
				lastBlockID = int64(blocks[len(blocks)-1].BlockNumber)
			}
			// Get current last block ID and create fence object key
			fenceBlockId := lastBlockID + 1
			fenceBlockKey = getBlockKey(f.segmentFileKey, fenceBlockId)

			// Try to create fence object
			_, putErr := f.client.PutFencedObject(ctx, f.bucket, fenceBlockKey)
			if putErr != nil {
				if werr.ErrObjectAlreadyExists.Is(putErr) {
					// Block already exists, this might be normal during concurrent operations
					logger.Ctx(ctx).Debug("Fence object already exists, retrying with next block ID",
						zap.String("segmentFileKey", f.segmentFileKey),
						zap.String("fenceBlockKey", fenceBlockKey),
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
				zap.String("fenceBlockKey", fenceBlockKey),
				zap.Int64("fenceBlockId", fenceBlockId))
			return nil
		},
		retry.Attempts(5),                 // Retry up to 5 times
		retry.Sleep(100*time.Millisecond), // Initial sleep between retries
		retry.MaxSleepTime(1*time.Second), // Max sleep time between retries
		retry.RetryErr(func(err error) bool {
			// Only retry on block already exists error
			return werr.ErrObjectAlreadyExists.Is(err)
		}),
	)

	if err != nil {
		logger.Ctx(ctx).Warn("Failed to create fence object after retries",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("fenceBlockKey", fenceBlockKey),
			zap.Error(err))
		return -1, fmt.Errorf("failed to create fence block %s: %w", fenceBlockKey, err)
	}

	// Mark as fenced
	f.fenced.Store(true)

	logger.Ctx(ctx).Info("Successfully fenced segment",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("lastBlockID", f.lastBlockID.Load()),
		zap.Int64("lastEntryId", f.lastEntryID.Load()))

	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "fence", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "fence", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return f.lastEntryID.Load(), nil
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
		if werr.ErrObjectAlreadyExists.Is(err) {
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
func (f *MinioFileWriter) serialize(blockId int64, entries []*cache.BufferEntry) []byte {
	if len(entries) == 0 {
		return []byte{}
	}

	serializedData := make([]byte, 0)

	// Add HeaderRecord only for the first block
	if blockId == 0 {
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

	// First, serialize all data records to calculate block length and CRC
	var blockDataBuffer []byte
	for _, entry := range entries {
		dataRecord, _ := codec.ParseData(entry.Data)
		encodedRecord := codec.EncodeRecord(dataRecord)
		blockDataBuffer = append(blockDataBuffer, encodedRecord...)
	}

	// Calculate block length and CRC
	blockLength := uint32(len(blockDataBuffer))
	blockCrc := crc32.ChecksumIEEE(blockDataBuffer)

	// Add BlockHeaderRecord at the start of the block with calculated values
	firstEntryID := entries[0].EntryId
	lastEntryID := entries[len(entries)-1].EntryId
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  int32(blockId),
		FirstEntryID: firstEntryID,
		LastEntryID:  lastEntryID,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}
	encodedBlockHeaderRecord := codec.EncodeRecord(blockHeaderRecord)
	serializedData = append(serializedData, encodedBlockHeaderRecord...)

	// Append the pre-serialized data records
	serializedData = append(serializedData, blockDataBuffer...)

	return serializedData
}

// streamMergeAndUploadBlocks streams through blocks, merges them and uploads immediately
func (f *MinioFileWriter) streamMergeAndUploadBlocks(ctx context.Context, targetBlockSize int64) ([]*codec.IndexRecord, int64, error) {
	if len(f.blockIndexes) == 0 {
		return nil, -1, nil
	}

	// Sort blocks by block number to ensure correct order
	sort.Slice(f.blockIndexes, func(i, j int) bool {
		return f.blockIndexes[i].BlockNumber < f.blockIndexes[j].BlockNumber
	})

	var newBlockIndexes []*codec.IndexRecord
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

	// Track file size after compact
	fileSizeAfterCompact := int64(0)

	// Helper function to upload current merged block
	uploadCurrentBlock := func() error {
		uploadCompactedBlockStartTime := time.Now()
		if len(currentMergedBlock) == 0 {
			return nil
		}

		blockIndex, blockSizeAfterCompact, err := f.uploadSingleMergedBlock(ctx, currentMergedBlock, mergedBlockID, currentEntryID, isFirstMergedBlock)
		if err != nil {
			return err
		}

		// update block compact metrics
		metrics.WpFileCompactLatency.WithLabelValues(f.logIdStr).Observe(float64(time.Since(uploadCompactedBlockStartTime).Milliseconds()))
		metrics.WpFileCompactBytesWritten.WithLabelValues(f.logIdStr).Add(float64(blockSizeAfterCompact))

		newBlockIndexes = append(newBlockIndexes, blockIndex)
		fileSizeAfterCompact += blockSizeAfterCompact

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
		blockKey := getBlockKey(f.segmentFileKey, int64(blockIndex.BlockNumber))
		blockData, err := f.readBlockData(ctx, blockKey)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block data",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", blockKey),
				zap.Error(err))
			return nil, -1, fmt.Errorf("failed to read block %d: %w", blockIndex.BlockNumber, err)
		}

		// Extract only data records (skip header and block last records)
		dataRecords, err := f.extractDataRecords(blockData)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to extract data records",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", blockKey),
				zap.Error(err))
			return nil, -1, fmt.Errorf("failed to extract data records from block %d: %w", blockIndex.BlockNumber, err)
		}

		// Add data records to current merged block first
		currentMergedBlock = append(currentMergedBlock, dataRecords...)
		currentMergedSize += int64(len(dataRecords))

		// Check if current merged block exceeds target size after adding data
		if currentMergedSize >= targetBlockSize {
			// Upload current merged block since it has reached the target size
			if err := uploadCurrentBlock(); err != nil {
				return nil, -1, fmt.Errorf("failed to upload merged block %d: %w", mergedBlockID, err)
			}
		}

		logger.Ctx(ctx).Debug("processed block for streaming merge",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int32("blockNumber", blockIndex.BlockNumber),
			zap.Int("dataRecordsSize", len(dataRecords)),
			zap.Int64("currentMergedSize", currentMergedSize))
	}

	// Upload the last merged block if it has data
	if len(currentMergedBlock) > 0 {
		if err := uploadCurrentBlock(); err != nil {
			return nil, -1, fmt.Errorf("failed to upload final merged block %d: %w", mergedBlockID, err)
		}
	}

	logger.Ctx(ctx).Info("completed streaming merge and upload",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("originalBlockCount", len(f.blockIndexes)),
		zap.Int("mergedBlockCount", len(newBlockIndexes)),
		zap.Int64("fileSizeAfterCompact", fileSizeAfterCompact))

	return newBlockIndexes, fileSizeAfterCompact, nil
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

// extractDataRecords extracts only data records from a block, skipping header and block header records
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
	// Count data records to calculate entry range
	records, err := codec.DecodeRecordList(mergedBlockData)
	if err != nil {
		return nil, -1, fmt.Errorf("failed to decode merged block data: %w", err)
	}

	dataRecordCount := 0
	for _, record := range records {
		if record.Type() == codec.DataRecordType {
			dataRecordCount++
		}
	}

	blockFirstEntryID := firstEntryID
	blockLastEntryID := firstEntryID + int64(dataRecordCount) - 1

	// Calculate block length and CRC for the merged block data
	blockLength := uint32(len(mergedBlockData))
	blockCrc := crc32.ChecksumIEEE(mergedBlockData)

	// Add block header record with calculated values
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  int32(mergedBlockID),
		FirstEntryID: blockFirstEntryID,
		LastEntryID:  blockLastEntryID,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}
	encodedBlockHeader := codec.EncodeRecord(blockHeaderRecord)

	// Build the complete block data
	var completeBlockData []byte
	if isFirstBlock {
		// For first block: header + blockHeader + data
		headerRecord := &codec.HeaderRecord{
			Version:      codec.FormatVersion,
			Flags:        codec.SetCompacted(f.footerRecord.Flags),
			FirstEntryID: firstEntryID,
		}
		encodedHeader := codec.EncodeRecord(headerRecord)
		completeBlockData = append(completeBlockData, encodedHeader...)
		completeBlockData = append(completeBlockData, encodedBlockHeader...)
		completeBlockData = append(completeBlockData, mergedBlockData...)
	} else {
		// For other blocks: blockHeader + data
		completeBlockData = append(completeBlockData, encodedBlockHeader...)
		completeBlockData = append(completeBlockData, mergedBlockData...)
	}

	// Upload merged block with m_ prefix (use PutObject for idempotent overwrites)
	mergedBlockKey := getMergedBlockKey(f.segmentFileKey, mergedBlockID)
	_, putErr := f.client.PutObject(ctx, f.bucket, mergedBlockKey,
		bytes.NewReader(completeBlockData), int64(len(completeBlockData)), minio.PutObjectOptions{})
	if putErr != nil {
		logger.Ctx(ctx).Warn("failed to upload merged block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("mergedBlockKey", mergedBlockKey),
			zap.Error(putErr))
		return nil, -1, fmt.Errorf("failed to upload merged block %d: %w", mergedBlockID, putErr)
	}

	// Create index record for the merged block
	indexRecord := &codec.IndexRecord{
		BlockNumber:  int32(mergedBlockID),
		StartOffset:  mergedBlockID,
		BlockSize:    uint32(len(completeBlockData)), // Use actual block data size
		FirstEntryID: blockFirstEntryID,
		LastEntryID:  blockLastEntryID,
	}

	logger.Ctx(ctx).Info("uploaded merged block",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("mergedBlockKey", mergedBlockKey),
		zap.Int64("blockFirstEntryID", blockFirstEntryID),
		zap.Int64("blockLastEntryID", blockLastEntryID),
		zap.Int("blockSize", len(completeBlockData)))

	return indexRecord, int64(len(completeBlockData)), nil
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

// BlockInfo is the information of a Block
type BlockInfo struct {
	FirstEntryID int64
	LastEntryID  int64
	BlockKey     string
	BlockID      int64
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
	block *BlockInfo
	err   error
}

// utils for block key
func getBlockKey(segmentFileKey string, blockID int64) string {
	return fmt.Sprintf("%s/%d.blk", segmentFileKey, blockID)
}

// utils for merged block key
func getMergedBlockKey(segmentFileKey string, mergedBlockID int64) string {
	return fmt.Sprintf("%s/m_%d.blk", segmentFileKey, mergedBlockID)
}

func getSegmentFileKey(baseDir string, logId int64, segmentId int64) string {
	return fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)
}

func getSegmentLockKey(segmentFileKey string) string {
	return fmt.Sprintf("%s/write.lock", segmentFileKey)
}

func getFooterBlockKey(segmentFileKey string) string {
	return fmt.Sprintf("%s/footer.blk", segmentFileKey)
}

// utils to parse object key
func parseBlockIdFromBlockKey(key string) (id int64, isMerge bool, err error) {
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
	totalSize := int64(0)
	// Serialize all block index records
	for _, record := range blocks {
		encodedRecord := codec.EncodeRecord(record)
		serializedData = append(serializedData, encodedRecord...)
		totalSize += int64(record.BlockSize)
	}
	indexLength := len(serializedData)

	// Verify that indexLength is a multiple of IndexRecordSize (IndexRecord size)
	expectedRecordSize := codec.RecordHeaderSize + codec.IndexRecordSize // 9 + 40 = 49 bytes per IndexRecord
	if indexLength%(expectedRecordSize) != 0 {
		logger.Ctx(ctx).Warn("Index length is not a multiple of expected record size",
			zap.Int("indexLength", indexLength),
			zap.Int("expectedRecordSize", expectedRecordSize),
			zap.Int("recordCount", len(blocks)))
	}

	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blocks)),
		TotalRecords: uint32(len(blocks)),
		TotalSize:    uint64(totalSize),
		IndexOffset:  0,
		IndexLength:  uint32(indexLength),
		Version:      codec.FormatVersion,
		Flags:        0,
	}

	encodedFooterRecord := codec.EncodeRecord(footer)
	serializedData = append(serializedData, encodedFooterRecord...)
	return serializedData, footer
}
