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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var (
	SegmentReaderScope = "MinioFileReader"
)

var _ storage.Reader = (*MinioFileReader)(nil)

// MinioFileReader implements AbstractFileReader for MinIO object storage.
// Deprecated use MinoFileReaderAdv instead
type MinioFileReader struct {
	client         minioHandler.MinioHandler
	bucket         string
	segmentFileKey string
	logId          int64
	segmentId      int64
	logIdStr       string // for metrics only

	blocks      []*codec.IndexRecord
	footer      *codec.FooterRecord
	isCompacted atomic.Bool // if the segment is compacted

	isCompleted atomic.Bool
	isFenced    atomic.Bool
	closed      atomic.Bool
}

// NewMinioFileReader creates a new MinIO reader
func NewMinioFileReader(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, client minioHandler.MinioHandler) (*MinioFileReader, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewMinioFileReader")
	defer sp.End()
	segmentFileKey := getSegmentFileKey(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("creating new minio file reader", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId))
	// Get object size
	reader := &MinioFileReader{
		logId:          logId,
		segmentId:      segId,
		logIdStr:       fmt.Sprintf("%d", logId),
		client:         client,
		bucket:         bucket,
		segmentFileKey: segmentFileKey,

		blocks: make([]*codec.IndexRecord, 0),
		footer: nil,
	}
	reader.isCompacted.Store(false)
	reader.isCompleted.Store(false)
	reader.isFenced.Store(false)
	reader.closed.Store(false)
	err := reader.tryReadFooterAndIndex(ctx)
	if err != nil {
		return nil, err
	}
	// If completed, no need to prefetch from data blocks
	if reader.isCompleted.Load() {
		logger.Ctx(ctx).Debug("create new minio file reader finish for completed segment", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId))
		return reader, nil
	}
	if !reader.isCompleted.Load() && !reader.isFenced.Load() {
		// if uncompleted, try fetch all block infos
		existsBlocks, err := reader.prefetchAllBlockInfo(ctx)
		if err != nil {
			logger.Ctx(ctx).Warn("prefetch block infos failed when create Read-only SegmentImpl",
				zap.String("segmentFileKey", segmentFileKey),
				zap.Error(err))
			return nil, err
		}
		logger.Ctx(ctx).Debug("prefetch all block infos finish",
			zap.String("segmentFileKey", segmentFileKey),
			zap.Int("blocks", existsBlocks))
	}
	logger.Ctx(ctx).Debug("create new minio file reader finish", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId))
	return reader, nil
}

// Start by listing all once
func (f *MinioFileReader) prefetchAllBlockInfo(ctx context.Context) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "prefetchAllBlockInfo")
	defer sp.End()
	startTime := time.Now()
	existsBlocks := make([]*codec.IndexRecord, 0, 32)

	var firstEntryID int64 = -1
	var lastEntryID int64 = -1
	// fence blockId if fence block exists
	fenceBlockId := int64(-1)
	// Sequentially try to read blocks starting from ID 0
	// Block sequence must be continuous without gaps - if any block is missing, stop fetching
	blockID := int64(0)
	for {
		blockKey := getBlockKey(f.segmentFileKey, blockID)

		// Try to get object info first to check if it exists and get last modified time
		objInfo, stateErr := f.client.StatObject(ctx, f.bucket, blockKey, minio.StatObjectOptions{})
		if stateErr != nil {
			if minioHandler.IsObjectNotExists(stateErr) {
				// Block doesn't exist, we've reached the end of continuous sequence
				logger.Ctx(ctx).Debug("block not found, stopping prefetch as sequence must be continuous",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Int64("blockID", blockID),
					zap.String("blockKey", blockKey))
				break
			}
			// Other errors (network issues, permissions, etc.) should also stop fetching
			return -1, stateErr
		}
		if minioHandler.IsFencedObject(objInfo) {
			logger.Ctx(ctx).Warn("object is fenced, stopping prefetch",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("fenceBlockID", blockID),
				zap.String("blockKey", blockKey))
			fenceBlockId = blockID
			f.isFenced.Store(true)
			break
		}

		logger.Ctx(ctx).Debug("Found segment file block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", objInfo.Key),
			zap.Int64("blockSize", objInfo.Size),
			zap.Int64("blockID", blockID))

		// get block header record
		blockHeaderRecord, getErr := f.getBlockHeaderRecord(ctx, blockID, objInfo.Key)
		if getErr != nil && werr.ErrSegmentFenced.Is(getErr) {
			continue
		}
		if getErr != nil {
			return 0, getErr
		}

		indexRecord := &codec.IndexRecord{
			BlockNumber:       int32(blockID),
			StartOffset:       blockID,
			FirstRecordOffset: 0,
			BlockSize:         uint32(objInfo.Size), // Use object size as block size
			FirstEntryID:      blockHeaderRecord.FirstEntryID,
			LastEntryID:       blockHeaderRecord.LastEntryID,
		}

		existsBlocks = append(existsBlocks, indexRecord)
		if firstEntryID == -1 || blockHeaderRecord.FirstEntryID < firstEntryID {
			firstEntryID = blockHeaderRecord.FirstEntryID
		}
		if blockHeaderRecord.LastEntryID > lastEntryID {
			lastEntryID = blockHeaderRecord.LastEntryID
		}

		logger.Ctx(ctx).Debug("extracted block info during prefetch All blocks info",
			zap.String("blockKey", blockKey),
			zap.Int64("blockID", blockID),
			zap.Int64("firstEntryID", blockHeaderRecord.FirstEntryID),
			zap.Int64("lastEntryID", blockHeaderRecord.LastEntryID))

		// move to next blockID
		blockID++
	}

	f.blocks = existsBlocks
	logger.Ctx(ctx).Info("successfully prefetch all blocks info from storage",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("blockIndexCount", len(f.blocks)),
		zap.Int64("firstEntryID", firstEntryID),
		zap.Int64("lastEntryID", lastEntryID),
		zap.Int64("fenceBlockId", fenceBlockId))
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "loadAll", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "loadAll", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return len(existsBlocks), nil
}

func (f *MinioFileReader) tryReadFooterAndIndex(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryReadFooterAndIndex")
	defer sp.End()
	// Check if footer.blk exists
	footerKey := getFooterBlockKey(f.segmentFileKey)
	statInfo, err := f.client.StatObject(ctx, f.bucket, footerKey, minio.StatObjectOptions{})
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			f.isCompleted.Store(false)
			return nil
		}
		return err
	}

	// Read the entire footer.blk file
	footerObj, err := f.client.GetObject(ctx, f.bucket, footerKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer footerObj.Close()

	footerBlkData, err := minioHandler.ReadObjectFull(ctx, footerObj, statInfo.Size)
	if err != nil {
		return err
	}

	logger.Ctx(ctx).Debug("read entire footer.blk",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("footerBlkSize", statInfo.Size),
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

	f.footer = footerRecord.(*codec.FooterRecord)
	f.isFenced.Store(true)
	f.isCompleted.Store(true)
	f.isCompacted.Store(codec.IsCompacted(f.footer.Flags))

	// Parse index records sequentially from the beginning of the file
	indexData := footerBlkData[:footerRecordStart]

	logger.Ctx(ctx).Debug("parsing index records sequentially",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexDataLength", len(indexData)),
		zap.Int32("expectedTotalBlocks", f.footer.TotalBlocks))

	// Parse all index records sequentially
	offset := 0
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
		f.blocks = append(f.blocks, indexRecord)

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

	logger.Ctx(ctx).Info("successfully parsed footer and index records",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexRecordCount", len(f.blocks)),
		zap.Int32("expectedTotalBlocks", f.footer.TotalBlocks))

	return nil
}

// incrementally fetch new blocks as they come in
func (f *MinioFileReader) prefetchIncrementalBlockInfo(ctx context.Context) (bool, *codec.IndexRecord, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "prefetchIncrementalBlockInfo")
	defer sp.End()
	startTime := time.Now()
	var fetchedLastBlock *codec.IndexRecord

	blockID := int64(0)
	if len(f.blocks) > 0 {
		lastFrag := f.blocks[len(f.blocks)-1]
		blockID = int64(lastFrag.BlockNumber) + 1
		fetchedLastBlock = lastFrag
	}
	existsNewBlock := false
	for {
		blockKey := getBlockKey(f.segmentFileKey, blockID)

		// check if the block exists in object storage
		blockObjInfo, err := f.client.StatObject(ctx, f.bucket, blockKey, minio.StatObjectOptions{})
		if err != nil && minioHandler.IsObjectNotExists(err) {
			break
		}
		if err != nil {
			// indicates that the prefetching of blocks has completed.
			//fmt.Println("object storage read block err: ", err)
			return existsNewBlock, nil, err
		}
		if minioHandler.IsFencedObject(blockObjInfo) {
			logger.Ctx(ctx).Warn("object is fenced, stopping recovery",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("fenceBlockID", blockID),
				zap.String("blockKey", blockKey))
			f.isFenced.Store(true)
			break
		}

		blockHeaderRecord, getErr := f.getBlockHeaderRecord(ctx, blockID, blockKey)
		if getErr != nil {
			return existsNewBlock, nil, getErr
		}

		fetchedLastBlock = &codec.IndexRecord{
			BlockNumber:       int32(blockID),
			StartOffset:       blockID,
			FirstRecordOffset: 0,
			BlockSize:         uint32(blockObjInfo.Size),
			FirstEntryID:      blockHeaderRecord.FirstEntryID,
			LastEntryID:       blockHeaderRecord.LastEntryID,
		}
		f.blocks = append(f.blocks, fetchedLastBlock)
		existsNewBlock = true
		logger.Ctx(ctx).Info("prefetch block info", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("lastBlockID", blockID))
		blockID++
	}

	logger.Ctx(ctx).Debug("prefetch block infos", zap.String("segmentFileKey", f.segmentFileKey), zap.Int("blocks", len(f.blocks)), zap.Int64("lastBlockID", blockID-1))
	metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "loadIncr", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "loadIncr", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return existsNewBlock, fetchedLastBlock, nil
}

func (f *MinioFileReader) getBlockHeaderRecord(ctx context.Context, blockID int64, blockKey string) (*codec.BlockHeaderRecord, error) {
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
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(setOptErr))
		return nil, setOptErr
	}
	headerRecordObj, getErr := f.client.GetObject(ctx, f.bucket, blockKey, getBlockHeaderRecordOpt)
	if getErr != nil {
		logger.Ctx(ctx).Warn("Error getting block header record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(getErr))
		return nil, getErr
	}
	defer headerRecordObj.Close()

	data, err := minioHandler.ReadObjectFull(ctx, headerRecordObj, readSize)
	if err != nil {
		logger.Ctx(ctx).Warn("Error reading block header record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(err))
		return nil, err
	}
	// check if it is a fence object
	if int64(len(data)) != readSize {
		logger.Ctx(ctx).Warn("Error getting block header record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Int64("requestReadSize", readSize),
			zap.Int("actualReadSize", len(data)))
		return nil, werr.ErrFileReaderInvalidRecord.WithCauseErrMsg("not enough size for block header record")
	}

	// Parse records to find the BlockHeaderRecord
	records, err := codec.DecodeRecordList(data)
	if err != nil {
		logger.Ctx(ctx).Warn("Error decoding records",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(err))
		return nil, err
	}

	// Look for BlockHeaderRecord (skip HeaderRecord if present)
	for _, record := range records {
		if record.Type() == codec.BlockHeaderRecordType {
			blockHeaderRecord := record.(*codec.BlockHeaderRecord)
			return blockHeaderRecord, nil
		}
	}

	typeErr := fmt.Errorf("BlockHeaderRecord not found in block")
	logger.Ctx(ctx).Warn("Error finding block header record",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.String("blockKey", blockKey),
		zap.Int("records", len(records)),
		zap.Error(typeErr))
	return nil, typeErr
}

// get the Block for the entryId
func (f *MinioFileReader) getBlock(ctx context.Context, entryId int64) (*codec.IndexRecord, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "getBlock")
	defer sp.End()
	logger.Ctx(ctx).Debug("get block for entryId", zap.Int64("entryId", entryId))

	// find from normal block
	foundFrag, err := f.findBlock(entryId)
	if err != nil {
		logger.Ctx(ctx).Warn("get block from cache failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if foundFrag != nil {
		logger.Ctx(ctx).Debug("get block from cache for entryId completed", zap.Int64("entryId", entryId), zap.Int32("blockID", foundFrag.BlockNumber))
		return foundFrag, nil
	}

	if f.isCompleted.Load() || f.isFenced.Load() {
		// means the prefetching of blocks has not completed.
		return nil, nil
	}

	// try to fetch new blocks if exists
	existsNewBlock, _, err := f.prefetchIncrementalBlockInfo(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("prefetch block info failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if !existsNewBlock {
		// means get no block for this entryId
		return nil, nil
	}

	// find again
	foundFrag, err = f.findBlock(entryId)
	if err != nil {
		return nil, err
	}
	if foundFrag != nil {
		logger.Ctx(ctx).Debug("get block from cache for entryId", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", entryId), zap.Int32("blockID", foundFrag.BlockNumber))
		return foundFrag, nil
	}

	// means get no block for this entryId
	return nil, nil
}

// findBlock finds the exists cache blocks for the entryId
func (f *MinioFileReader) findBlock(entryId int64) (*codec.IndexRecord, error) {
	return SearchBlock(f.blocks, entryId)
}

func (f *MinioFileReader) GetLastEntryID(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "GetLastEntryID")
	defer sp.End()
	if !f.isCompleted.Load() {
		_, lastBlockInfo, err := f.prefetchIncrementalBlockInfo(ctx)
		if err != nil {
			return -1, err
		}
		if lastBlockInfo == nil {
			return -1, werr.ErrFileReaderNoBlockFound
		}
		return lastBlockInfo.LastEntryID, nil
	}
	if len(f.blocks) > 0 {
		return f.blocks[len(f.blocks)-1].LastEntryID, nil
	}

	logger.Ctx(ctx).Debug("no blocks exist, returning -1 as last entry ID",
		zap.String("segmentFileKey", f.segmentFileKey))
	return -1, nil
}

func (f *MinioFileReader) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt) (*storage.Batch, error) {
	// use Adv Reader instead
	return nil, werr.ErrOperationNotSupported.WithCauseErrMsg("not support advance read")
}

func (f *MinioFileReader) ReadNextBatch(ctx context.Context, opt storage.ReaderOpt) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatch")
	defer sp.End()
	startTime := time.Now()

	logger.Ctx(ctx).Debug("ReadNextBatch called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("batchSize", opt.BatchSize),
		zap.Bool("isCompleted", f.isCompleted.Load()),
		zap.Bool("isFenced", f.isFenced.Load()))

	// For incomplete files, try to scan for new blocks if we don't have enough data
	if !f.isCompleted.Load() && !f.isFenced.Load() {
		if err := f.ensureSufficientBlocks(ctx, opt.StartEntryID, opt.BatchSize); err != nil {
			logger.Ctx(ctx).Warn("failed to scan for new blocks", zap.Error(err))
			// Continue with existing blocks even if scan fails
		}
	}

	// Get all available blocks
	allBlocks := make([]*codec.IndexRecord, len(f.blocks))
	copy(allBlocks, f.blocks)

	if len(allBlocks) == 0 {
		// if no blocks and the segment is compacted or fenced, return an EOF
		if f.isCompacted.Load() || f.isFenced.Load() {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no data blocks")
		}
		// otherwise return entry Not found, since there is no data, reader should retry later
		return nil, werr.ErrEntryNotFound
	}

	// Find the starting block that contains the start sequence number
	startBlockIndex := -1
	for i, block := range allBlocks {
		logger.Ctx(ctx).Debug("checking block for start sequence",
			zap.Int("blockIndex", i),
			zap.Int32("blockNumber", block.BlockNumber),
			zap.Int64("blockFirstEntryID", block.FirstEntryID),
			zap.Int64("blockLastEntryID", block.LastEntryID),
			zap.Int64("startEntryID", opt.StartEntryID))

		if block.FirstEntryID <= opt.StartEntryID && opt.StartEntryID <= block.LastEntryID {
			startBlockIndex = i
			logger.Ctx(ctx).Debug("found matching block",
				zap.Int("startBlockIndex", startBlockIndex),
				zap.Int32("blockNumber", block.BlockNumber))
			break
		}
	}

	if startBlockIndex == -1 {
		logger.Ctx(ctx).Warn("no block found for start sequence number",
			zap.Int64("startEntryID", opt.StartEntryID),
			zap.Int("totalBlocks", len(allBlocks)))
		// if no block contains reading point and the segment is compacted or fenced, return an EOF
		if f.isCompacted.Load() || f.isFenced.Load() {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg(fmt.Sprintf("no data contains %d", opt.StartEntryID))
		}
		// otherwise return entry Not found, since there is no data, reader should retry later
		return nil, werr.ErrEntryNotFound
	}

	if opt.BatchSize == -1 {
		// Auto batch mode: return all data records from the single block containing the start sequence number
		logger.Ctx(ctx).Debug("using single block mode")
		res, readErr := f.readSingleBlock(ctx, allBlocks[startBlockIndex], opt.StartEntryID)
		if readErr == nil {
			metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "readBatch", "success").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "readBatch", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		}
		return res, readErr
	} else {
		// Specified batch size mode: read across multiple blocks if necessary to get the requested number of entries
		logger.Ctx(ctx).Debug("using multiple blocks mode",
			zap.Int("startBlockIndex", startBlockIndex),
			zap.Int64("batchSize", opt.BatchSize))
		res, readErr := f.readMultipleBlocks(ctx, allBlocks, startBlockIndex, opt.StartEntryID, opt.BatchSize)
		if readErr == nil {
			metrics.WpFileOperationsTotal.WithLabelValues(f.logIdStr, "readBatch", "success").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(f.logIdStr, "readBatch", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		}
		return res, readErr
	}
}

// ensureSufficientBlocks ensures we have scanned enough blocks to satisfy the read request
// This method checks if we need to scan for new blocks in incomplete files
func (f *MinioFileReader) ensureSufficientBlocks(ctx context.Context, startSequenceNum int64, batchSize int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ensureSufficientBlocks")
	defer sp.End()
	// Check if we already have the starting block
	hasStartingBlock := false
	var lastAvailableEntryID int64 = -1

	for _, block := range f.blocks {
		if block.FirstEntryID <= startSequenceNum && startSequenceNum <= block.LastEntryID {
			hasStartingBlock = true
		}
		if block.LastEntryID > lastAvailableEntryID {
			lastAvailableEntryID = block.LastEntryID
		}
	}

	// If we don't have the starting block, definitely need to scan
	needToScan := !hasStartingBlock

	// If we have the starting block but need more data for the batch size
	if hasStartingBlock && batchSize > 0 {
		requiredLastEntryID := startSequenceNum + batchSize - 1
		if lastAvailableEntryID < requiredLastEntryID {
			needToScan = true
			logger.Ctx(ctx).Debug("need more blocks for batch size",
				zap.Int64("startSequenceNum", startSequenceNum),
				zap.Int64("batchSize", batchSize),
				zap.Int64("requiredLastEntryID", requiredLastEntryID),
				zap.Int64("lastAvailableEntryID", lastAvailableEntryID))
		}
	}

	if needToScan {
		logger.Ctx(ctx).Debug("scanning for new blocks to satisfy read request",
			zap.Int64("startSequenceNum", startSequenceNum),
			zap.Int64("batchSize", batchSize),
			zap.Bool("hasStartingBlock", hasStartingBlock),
			zap.Int64("lastAvailableEntryID", lastAvailableEntryID))

		_, _, err := f.prefetchIncrementalBlockInfo(ctx)
		return err
	}

	logger.Ctx(ctx).Debug("sufficient blocks available, no scan needed",
		zap.Int64("startSequenceNum", startSequenceNum),
		zap.Int64("batchSize", batchSize),
		zap.Int64("lastAvailableEntryID", lastAvailableEntryID))

	return nil
}

func (f *MinioFileReader) getBlockObjectKey(blockNumber int64) string {
	if f.isCompacted.Load() {
		return getMergedBlockKey(f.segmentFileKey, blockNumber)
	} else {
		return getBlockKey(f.segmentFileKey, blockNumber)
	}
}

// readSingleBlock reads all data records from a single block starting from the specified entry ID
func (f *MinioFileReader) readSingleBlock(ctx context.Context, blockInfo *codec.IndexRecord, startEntryID int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readSingleBlock")
	defer sp.End()
	startTime := time.Now()
	blockObjKey := f.getBlockObjectKey(int64(blockInfo.BlockNumber))

	// Get object info to determine the actual size
	objInfo, statErr := f.client.StatObject(ctx, f.bucket, blockObjKey, minio.StatObjectOptions{})
	if statErr != nil {
		logger.Ctx(ctx).Warn("Failed to get block object info",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
			zap.Error(statErr))
		return nil, statErr
	}

	blockObj, getErr := f.client.GetObject(ctx, f.bucket, blockObjKey, minio.GetObjectOptions{})
	if getErr != nil {
		logger.Ctx(ctx).Warn("Failed to get block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
			zap.Error(getErr))
		return nil, getErr
	}
	defer blockObj.Close()

	blockData, err := minioHandler.ReadObjectFull(ctx, blockObj, objInfo.Size)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to read block data",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
			zap.Error(err))
		return nil, err
	}

	records, decodeErr := codec.DecodeRecordList(blockData)
	if decodeErr != nil {
		logger.Ctx(ctx).Warn("Failed to decode block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
			zap.Error(decodeErr))
		return nil, decodeErr
	}

	// Find BlockHeaderRecord and verify data integrity
	var blockHeaderRecord *codec.BlockHeaderRecord
	for _, record := range records {
		if record.Type() == codec.BlockHeaderRecordType {
			blockHeaderRecord = record.(*codec.BlockHeaderRecord)
			break
		}
	}

	// If we found a BlockHeaderRecord, verify the block data integrity
	if blockHeaderRecord != nil {
		// Extract the data records part from blockData
		// For the first block (blockNumber == 0), there might be a HeaderRecord before BlockHeaderRecord
		// For other blocks, only BlockHeaderRecord exists
		var dataStartOffset int
		// First block: HeaderRecord -> BlockHeaderRecord -> DataRecords
		// Check if the first record is HeaderRecord
		if blockInfo.BlockNumber == 0 {
			// Skip HeaderRecord + BlockHeaderRecord
			dataStartOffset = codec.RecordHeaderSize + codec.HeaderRecordSize + codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		} else {
			// Only BlockHeaderRecord exists
			dataStartOffset = codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		}

		if len(blockData) > dataStartOffset {
			dataRecordsBuffer := blockData[dataStartOffset:]
			// Verify block data integrity
			if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, dataRecordsBuffer); err != nil {
				logger.Ctx(ctx).Warn("block data integrity verification failed",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
					zap.Error(err))
				return nil, fmt.Errorf("block data integrity verification failed: %w", err)
			}

			logger.Ctx(ctx).Debug("block data integrity verified successfully",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
				zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
		}

		entries := make([]*proto.LogEntry, 0, 32)
		currentEntryID := blockInfo.FirstEntryID

		readBytes := 0
		// Parse all data records in the block
		for j := 0; j < len(records); j++ {
			if records[j].Type() != codec.DataRecordType {
				continue
			}
			// Only include entries from the start sequence number onwards
			if currentEntryID >= startEntryID {
				r := records[j].(*codec.DataRecord)
				entry := &proto.LogEntry{
					EntryId: currentEntryID,
					Values:  r.Payload,
				}
				entries = append(entries, entry)
				readBytes += len(r.Payload)
			}
			currentEntryID++
		}

		if len(entries) == 0 {
			// return entry not found yet, wp client retry later
			logger.Ctx(ctx).Debug("read single block completed, no entry extracted",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Int64("startEntryID", startEntryID),
				zap.Int("entriesReturned", len(entries)))
			return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
		} else {
			logger.Ctx(ctx).Debug("read single block completed",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Int64("startEntryID", startEntryID),
				zap.Int("entriesReturned", len(entries)))
		}
		metrics.WpFileReadBatchBytes.WithLabelValues(f.logIdStr).Add(float64(readBytes))
		metrics.WpFileReadBatchLatency.WithLabelValues(f.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

		return entries, nil
	}

	// means get no block for this entryId
	return nil, nil
}

// readMultipleBlocks reads across multiple blocks to get the specified number of entries
func (f *MinioFileReader) readMultipleBlocks(ctx context.Context, allBlocks []*codec.IndexRecord, startBlockIndex int, startSequenceNum int64, batchSize int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readMultipleBlocks")
	defer sp.End()
	startTime := time.Now()
	entries := make([]*proto.LogEntry, 0, batchSize)
	entriesCollected := int64(0)
	readBytes := 0

	for i := startBlockIndex; i < len(allBlocks) && entriesCollected < batchSize; i++ {
		blockInfo := allBlocks[i]
		blockObjKey := f.getBlockObjectKey(int64(blockInfo.BlockNumber))

		// Get object info to determine the actual size
		objInfo, statErr := f.client.StatObject(ctx, f.bucket, blockObjKey, minio.StatObjectOptions{})
		if statErr != nil {
			logger.Ctx(ctx).Warn("Failed to get block object info",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Error(statErr))
			break // Stop reading on error, return what we have so far
		}

		blockObj, getErr := f.client.GetObject(ctx, f.bucket, blockObjKey, minio.GetObjectOptions{})
		if getErr != nil {
			logger.Ctx(ctx).Warn("Failed to get block",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Error(getErr))
			break // Stop reading on error, return what we have so far
		}

		blockData, err := minioHandler.ReadObjectFull(ctx, blockObj, objInfo.Size)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to read block data",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Error(err))
			blockObj.Close()
			break // Stop reading on error, return what we have so far
		}
		blockObj.Close()

		records, decodeErr := codec.DecodeRecordList(blockData)
		if decodeErr != nil {
			logger.Ctx(ctx).Warn("Failed to decode block",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
				zap.Error(decodeErr))
			break // Stop reading on error, return what we have so far
		}

		// Find BlockHeaderRecord and verify data integrity
		var blockHeaderRecord *codec.BlockHeaderRecord
		for _, record := range records {
			if record.Type() == codec.BlockHeaderRecordType {
				blockHeaderRecord = record.(*codec.BlockHeaderRecord)
				break
			}
		}

		// If we found a BlockHeaderRecord, verify the block data integrity
		if blockHeaderRecord != nil {
			// Extract the data records part from blockData
			// For the first block (blockNumber == 0), there might be a HeaderRecord before BlockHeaderRecord
			// For other blocks, only BlockHeaderRecord exists
			var dataStartOffset int
			// First block: HeaderRecord -> BlockHeaderRecord -> DataRecords
			// Check if the first record is HeaderRecord
			if blockInfo.BlockNumber == 0 {
				// Skip HeaderRecord + BlockHeaderRecord
				dataStartOffset = codec.RecordHeaderSize + codec.HeaderRecordSize + codec.RecordHeaderSize + codec.BlockHeaderRecordSize
			} else {
				// Only BlockHeaderRecord exists
				dataStartOffset = codec.RecordHeaderSize + codec.BlockHeaderRecordSize
			}

			if len(blockData) > dataStartOffset {
				dataRecordsBuffer := blockData[dataStartOffset:]
				// Verify block data integrity
				if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, dataRecordsBuffer); err != nil {
					logger.Ctx(ctx).Warn("block data integrity verification failed",
						zap.String("segmentFileKey", f.segmentFileKey),
						zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
						zap.Error(err))
					break // Stop reading on error, return what we have so far
				}

				logger.Ctx(ctx).Debug("block data integrity verified successfully",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
					zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
					zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
			}

			currentEntryID := blockInfo.FirstEntryID
			// Parse all data records in the block
			for j := 0; j < len(records) && entriesCollected < batchSize; j++ {
				if records[j].Type() != codec.DataRecordType {
					continue
				}
				// Only include entries from the start sequence number onwards
				if currentEntryID >= startSequenceNum {
					r := records[j].(*codec.DataRecord)
					entry := &proto.LogEntry{
						EntryId: currentEntryID,
						Values:  r.Payload,
					}
					entries = append(entries, entry)
					entriesCollected++
					readBytes += len(r.Payload)
				}
				currentEntryID++
			}
		}
	}

	if len(entries) == 0 {
		logger.Ctx(ctx).Debug("read multiple blocks completed, no entry extracted",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("startSequenceNum", startSequenceNum),
			zap.Int64("requestedBatchSize", batchSize),
			zap.Int("entriesReturned", len(entries)))
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("read multiple blocks completed",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("startSequenceNum", startSequenceNum),
			zap.Int64("requestedBatchSize", batchSize),
			zap.Int("entriesReturned", len(entries)))
	}
	metrics.WpFileReadBatchBytes.WithLabelValues(f.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(f.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	return entries, nil
}

func (f *MinioFileReader) GetBlockIndexes() []*codec.IndexRecord {
	// prefetch if not completed
	return f.blocks
}

func (f *MinioFileReader) GetFooter() *codec.FooterRecord {
	return f.footer
}

func (f *MinioFileReader) GetTotalRecords() uint32 {
	if f.footer != nil {
		return f.footer.TotalRecords
	}
	return 0
}

func (f *MinioFileReader) GetTotalBlocks() int32 {
	if f.footer != nil {
		return f.footer.TotalBlocks
	}
	return 0
}

func (f *MinioFileReader) Close(ctx context.Context) error {
	if !f.closed.CompareAndSwap(false, true) {
		return errors.New("already close")
	}
	return nil
}

func SearchBlock(list []*codec.IndexRecord, entryId int64) (*codec.IndexRecord, error) {
	low, high := 0, len(list)-1
	var candidate *codec.IndexRecord

	for low <= high {
		mid := (low + high) / 2
		block := list[mid]

		firstEntryID := block.FirstEntryID
		if firstEntryID > entryId {
			high = mid - 1
		} else {
			lastEntryID := block.LastEntryID
			if lastEntryID >= entryId {
				candidate = block
				return candidate, nil
			} else {
				low = mid + 1
			}
		}
	}
	return candidate, nil
}
