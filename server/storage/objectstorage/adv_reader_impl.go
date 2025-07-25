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

var _ storage.Reader = (*MinioFileReaderAdv)(nil)

// MinioFileReaderAdv is an implementation of AbstractFileReader, offering enhanced options for advanced batch reading features.
type MinioFileReaderAdv struct {
	client         minioHandler.MinioHandler
	bucket         string
	segmentFileKey string
	logId          int64
	segmentId      int64
	logIdStr       string // for metrics only

	// adv reading options
	advOpt *storage.BatchInfo

	// When no advanced options are provided, Prefetch block information from exists the footer
	blocks      []*codec.IndexRecord
	footer      *codec.FooterRecord
	isCompacted atomic.Bool // if the segment is compacted
	isCompleted atomic.Bool
	isFenced    atomic.Bool
	flags       uint16
	version     uint16

	// close state
	closed atomic.Bool
}

// NewMinioFileReaderAdv creates a new MinIO reader
func NewMinioFileReaderAdv(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, client minioHandler.MinioHandler,
	advOpt *storage.BatchInfo) (*MinioFileReaderAdv, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewMinioFileReaderAdv")
	defer sp.End()
	segmentFileKey := getSegmentFileKey(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("creating new minio file reader", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId), zap.Any("advOpt", advOpt))
	// Get object size
	reader := &MinioFileReaderAdv{
		logId:          logId,
		segmentId:      segId,
		logIdStr:       fmt.Sprintf("%d", logId),
		client:         client,
		bucket:         bucket,
		segmentFileKey: segmentFileKey,

		advOpt: advOpt,

		blocks:  make([]*codec.IndexRecord, 0),
		footer:  nil,
		flags:   0,
		version: codec.FormatVersion,
	}
	reader.closed.Store(false)
	reader.isCompacted.Store(false)
	reader.isCompleted.Store(false)
	reader.isFenced.Store(false)

	if advOpt != nil {
		isCompacted := codec.IsCompacted(advOpt.Flags)
		reader.version = advOpt.Version
		reader.flags = advOpt.Flags
		reader.isCompacted.Store(isCompacted)
	} else {
		// try read footer and extract block index infos
		err := reader.tryReadFooterAndIndex(ctx)
		if err != nil {
			return nil, err
		}
	}
	logger.Ctx(ctx).Debug("create new minio file readerAdv finish", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId), zap.Any("advOpt", advOpt))
	return reader, nil
}

func (f *MinioFileReaderAdv) tryReadFooterAndIndex(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryReadFooterAndIndex")
	defer sp.End()
	logger.Ctx(ctx).Debug("try to read footer and block indexes", zap.String("segmentFileKey", f.segmentFileKey))

	// Check if footer.blk exists
	footerKey := getFooterBlockKey(f.segmentFileKey)
	statInfo, err := f.client.StatObject(ctx, f.bucket, footerKey, minio.StatObjectOptions{})
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			// no footer blk yet
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
	f.isCompacted.Store(codec.IsCompacted(f.footer.Flags))
	f.isCompleted.Store(true)
	f.version = f.footer.Version
	f.flags = f.footer.Flags

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
func (f *MinioFileReaderAdv) prefetchIncrementalBlockInfo(ctx context.Context) (bool, *codec.IndexRecord, error) {
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
			BlockNumber:  int32(blockID),
			StartOffset:  blockID,
			BlockSize:    uint32(blockObjInfo.Size),
			FirstEntryID: blockHeaderRecord.FirstEntryID,
			LastEntryID:  blockHeaderRecord.LastEntryID,
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

func (f *MinioFileReaderAdv) getBlockHeaderRecord(ctx context.Context, blockID int64, blockKey string) (*codec.BlockHeaderRecord, error) {
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

func (f *MinioFileReaderAdv) GetLastEntryID(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "GetLastEntryID")
	defer sp.End()

	// Try to fetch new blocks incrementally to get the latest entry ID
	_, lastBlockInfo, err := f.prefetchIncrementalBlockInfo(ctx)
	if err != nil {
		return -1, err
	}
	if lastBlockInfo == nil {
		if len(f.blocks) > 0 {
			return f.blocks[len(f.blocks)-1].LastEntryID, nil
		}
		return -1, werr.ErrFileReaderNoBlockFound
	}
	return lastBlockInfo.LastEntryID, nil
}

func (f *MinioFileReaderAdv) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt) (*storage.Batch, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatchAdv")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatchAdv called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("batchSize", opt.BatchSize),
		zap.Bool("isCompacted", f.isCompacted.Load()),
		zap.Bool("footerExists", f.footer != nil),
		zap.Any("opt", opt),
		zap.Any("advOpt", f.advOpt))

	startBlockID := int64(0)
	if f.advOpt != nil {
		// When we have advOpt, start from the block after the last read block
		startBlockID = int64(f.advOpt.LastBlockInfo.BlockNumber + 1)
	} else if f.footer != nil {
		// When we have footer, find the block containing the start entry ID
		foundStartBlock, err := codec.SearchBlock(f.blocks, opt.StartEntryID)
		if err != nil {
			logger.Ctx(ctx).Warn("search block failed", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("entryId", opt.StartEntryID), zap.Error(err))
			return nil, err
		}
		if foundStartBlock != nil {
			logger.Ctx(ctx).Debug("found block for entryId", zap.Int64("entryId", opt.StartEntryID), zap.Int32("blockID", foundStartBlock.BlockNumber))
			startBlockID = int64(foundStartBlock.BlockNumber)
		}
		// No block found for entryId in this completed segment
		if foundStartBlock == nil {
			logger.Ctx(ctx).Debug("no more entries to read",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("startEntryId", opt.StartEntryID))
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
	}

	return f.readDataBlocks(ctx, opt, startBlockID)
}

func (f *MinioFileReaderAdv) readDataBlocks(ctx context.Context, opt storage.ReaderOpt, startBlockID int64) (*storage.Batch, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

	logger.Ctx(ctx).Debug("read data blocks start", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("startEntryId", opt.StartEntryID), zap.Int64("requestedBatchSize", opt.BatchSize))

	// Soft limitation
	maxEntries := opt.BatchSize // soft count limit: Read the minimum number of blocks exceeding the count limit
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := 16 * 1024 * 1024 // soft bytes limit: Read the minimum number of blocks exceeding the bytes limit

	// read data result
	entries := make([]*proto.LogEntry, 0, maxEntries)
	var lastBlockInfo *codec.IndexRecord

	// read stats
	entriesCollected := int64(0)
	readBytes := 0

	// extract data from blocks
	for i := startBlockID; readBytes < maxBytes && entriesCollected < maxEntries; i++ {
		currentBlockID := i
		blockObjKey := f.getBlockObjectKey(currentBlockID)
		logger.Ctx(ctx).Debug("try to read block", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockNumber", currentBlockID))

		// Get object info to determine the actual size
		objInfo, statErr := f.client.StatObject(ctx, f.bucket, blockObjKey, minio.StatObjectOptions{})
		if statErr != nil {
			if minioHandler.IsObjectNotExists(statErr) {
				// Block doesn't exist, we've reached the end
				logger.Ctx(ctx).Debug("block not found, reached end of data",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Int64("blockNumber", currentBlockID))
				break
			}
			logger.Ctx(ctx).Warn("Failed to get block object info",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(statErr))
			break // Stop reading on error, return what we have so far
		}

		if minioHandler.IsFencedObject(objInfo) {
			logger.Ctx(ctx).Warn("object is fenced, stop reading",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("fenceBlockID", currentBlockID),
				zap.String("blockKey", blockObjKey))
			f.isFenced.Store(true)
			break
		}

		blockObj, getErr := f.client.GetObject(ctx, f.bucket, blockObjKey, minio.GetObjectOptions{})
		if getErr != nil {
			logger.Ctx(ctx).Warn("Failed to get block",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(getErr))
			break // Stop reading on error, return what we have so far
		}

		blockData, err := minioHandler.ReadObjectFull(ctx, blockObj, objInfo.Size)
		blockObj.Close() // Always close the object
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to read block data",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			break // Stop reading on error, return what we have so far
		}

		records, decodeErr := codec.DecodeRecordList(blockData)
		if decodeErr != nil {
			logger.Ctx(ctx).Warn("Failed to decode block",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(decodeErr))
			break // Stop reading on error, return what we have so far
		}
		logger.Ctx(ctx).Debug("decoded data from block", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("blockNumber", currentBlockID), zap.Int("records", len(records)))

		// Find BlockHeaderRecord and verify data integrity
		var blockHeaderRecord *codec.BlockHeaderRecord
		for _, record := range records {
			if record.Type() == codec.HeaderRecordType {
				// if no footer/no advOpt, it may start from 0, and header record will be read to init version&flags
				fileHeaderRecord := record.(*codec.HeaderRecord)
				f.version = fileHeaderRecord.Version
				f.flags = fileHeaderRecord.Flags
				continue
			}
			if record.Type() == codec.BlockHeaderRecordType {
				blockHeaderRecord = record.(*codec.BlockHeaderRecord)
				break
			}
		}
		if blockHeaderRecord == nil {
			logger.Ctx(ctx).Warn("block header record not found, stop reading",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID))
			break
		}

		// Verify the block data integrity
		verifyBlockErr := f.verifyBlockDataIntegrity(ctx, blockHeaderRecord, currentBlockID, blockData)
		if verifyBlockErr != nil {
			logger.Ctx(ctx).Warn("verify block data integrity failed, stop reading",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID),
				zap.Int32("readBlockNumber", blockHeaderRecord.BlockNumber),
				zap.Error(verifyBlockErr))
			break // Stop reading on data integrity error, return what we have so far
		}

		currentEntryID := blockHeaderRecord.FirstEntryID
		// Parse all data records in the block even if it exceeds the maxSize or maxBytes, because one block should be read once
		for j := 0; j < len(records); j++ {
			if records[j].Type() != codec.DataRecordType {
				continue
			}
			// Only include entries from the start sequence number onwards
			if opt.StartEntryID <= currentEntryID {
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

		logger.Ctx(ctx).Debug("Extracted data from block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", currentBlockID),
			zap.Int32("readBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Int64("collectedEntries", entriesCollected),
			zap.Int("collectedBytes", readBytes))

		lastBlockInfo = &codec.IndexRecord{
			BlockNumber:  int32(currentBlockID),
			StartOffset:  currentBlockID,
			BlockSize:    uint32(objInfo.Size),
			FirstEntryID: blockHeaderRecord.FirstEntryID,
			LastEntryID:  blockHeaderRecord.LastEntryID,
		}
	}

	if len(entries) == 0 {
		logger.Ctx(ctx).Debug("no entry extracted",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("requestedBatchSize", opt.BatchSize),
			zap.Int("entriesReturned", len(entries)))
		if f.isCompleted.Load() || f.isFenced.Load() || f.isCompacted.Load() {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
		if f.footer != nil || f.isFooterExists(ctx) {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("read data blocks completed",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("requestedBatchSize", opt.BatchSize),
			zap.Int("entriesReturned", len(entries)),
			zap.Any("lastBlockInfo", lastBlockInfo))
	}

	metrics.WpFileReadBatchBytes.WithLabelValues(f.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(f.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	// Create batch with proper error handling for nil lastBlockInfo
	var batchInfo *storage.BatchInfo
	if lastBlockInfo != nil {
		batchInfo = &storage.BatchInfo{
			Flags:         f.flags,
			Version:       f.version,
			LastBlockInfo: lastBlockInfo,
		}
	}

	batch := &storage.Batch{
		LastBatchInfo: batchInfo,
		Entries:       entries,
	}
	return batch, nil
}

func (f *MinioFileReaderAdv) isFooterExists(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryReadFooterAndIndex")
	defer sp.End()
	logger.Ctx(ctx).Debug("try to read footer and block indexes", zap.String("segmentFileKey", f.segmentFileKey))

	// Check if footer.blk exists
	footerKey := getFooterBlockKey(f.segmentFileKey)
	statInfo, err := f.client.StatObject(ctx, f.bucket, footerKey, minio.StatObjectOptions{})
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			// no footer blk yet
			return false
		}
		logger.Ctx(ctx).Warn("failed to stat footer.blk", zap.String("segmentFileKey", f.segmentFileKey), zap.Error(err))
		return false
	}
	logger.Ctx(ctx).Debug("found footer.blk", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("size", statInfo.Size))
	return true
}

func (f *MinioFileReaderAdv) verifyBlockDataIntegrity(ctx context.Context, blockHeaderRecord *codec.BlockHeaderRecord, currentBlockID int64, blockData []byte) error {
	// Extract the data records part from blockData
	// For the first block (blockNumber == 0), there might be a HeaderRecord before BlockHeaderRecord
	// For other blocks, only BlockHeaderRecord exists
	var dataStartOffset int
	// First block: HeaderRecord -> BlockHeaderRecord -> DataRecords
	// Check if the first record is HeaderRecord
	if currentBlockID == 0 {
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
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			return err // Stop reading on error, return what we have so far
		}

		logger.Ctx(ctx).Debug("block data integrity verified successfully",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", currentBlockID),
			zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
			zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
	}
	return nil
}

func (f *MinioFileReaderAdv) getBlockObjectKey(blockNumber int64) string {
	if f.isCompacted.Load() {
		return getMergedBlockKey(f.segmentFileKey, blockNumber)
	} else {
		return getBlockKey(f.segmentFileKey, blockNumber)
	}
}

func (f *MinioFileReaderAdv) GetBlockIndexes() []*codec.IndexRecord {
	// prefetch if not completed
	return f.blocks
}

func (f *MinioFileReaderAdv) GetFooter() *codec.FooterRecord {
	return f.footer
}

func (f *MinioFileReaderAdv) GetTotalRecords() uint32 {
	if f.footer != nil {
		return f.footer.TotalRecords
	}
	return 0
}

func (f *MinioFileReaderAdv) GetTotalBlocks() int32 {
	if f.footer != nil {
		return f.footer.TotalBlocks
	}
	return 0
}

func (f *MinioFileReaderAdv) Close(ctx context.Context) error {
	if !f.closed.CompareAndSwap(false, true) {
		return errors.New("already close")
	}
	return nil
}
