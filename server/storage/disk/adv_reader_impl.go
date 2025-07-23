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

package disk

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var _ storage.Reader = (*LocalFileReaderAdv)(nil)

// LocalFileReaderAdv implements AbstractFileReader for local filesystem storage
type LocalFileReaderAdv struct {
	logId    int64
	segId    int64
	logIdStr string // for metrics only
	filePath string
	file     *os.File
	size     int64

	// adv reading options
	advOpt *storage.BatchInfo

	// When no advanced options are provided, Prefetch block information from exists the footer
	footer           *codec.FooterRecord
	blockIndexes     []*codec.IndexRecord
	flags            uint16
	version          uint16
	closed           atomic.Bool
	isIncompleteFile bool // Whether this file is incomplete (no footer)

}

// NewLocalFileReaderAdv creates a new local filesystem reader
func NewLocalFileReaderAdv(ctx context.Context, baseDir string, logId int64, segId int64, advOpt *storage.BatchInfo) (*LocalFileReaderAdv, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewLocalFileReader")
	defer sp.End()

	logger.Ctx(ctx).Debug("creating new local file reader",
		zap.String("baseDir", baseDir),
		zap.Int64("logId", logId),
		zap.Int64("segId", segId),
		zap.Any("advOpt", advOpt))

	segmentDir := getSegmentDir(baseDir, logId, segId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	filePath := getSegmentFilePath(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("attempting to open file for reading",
		zap.String("filePath", filePath))

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, return entry not found
			logger.Ctx(ctx).Debug("file does not exist, returning ErrEntryNotFound",
				zap.String("filePath", filePath))
			return nil, werr.ErrEntryNotFound
		}
		logger.Ctx(ctx).Warn("failed to open file for reading",
			zap.String("filePath", filePath),
			zap.Error(err))
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		logger.Ctx(ctx).Warn("failed to stat file",
			zap.String("filePath", filePath),
			zap.Error(err))
		return nil, fmt.Errorf("stat file: %w", err)
	}

	logger.Ctx(ctx).Debug("file opened successfully",
		zap.String("filePath", filePath),
		zap.Int64("fileSize", stat.Size()))

	reader := &LocalFileReaderAdv{
		logId:    logId,
		segId:    segId,
		logIdStr: fmt.Sprintf("%d", logId),
		filePath: filePath,
		file:     file,
		size:     stat.Size(),
		advOpt:   advOpt,
	}
	reader.closed.Store(false)
	reader.isIncompleteFile = true

	if advOpt != nil {
		reader.flags = advOpt.Flags
		reader.version = advOpt.Version
	} else {
		// Try to parse footer and indexes
		if err := reader.tryParseFooterAndIndexesIfExists(ctx); err != nil {
			file.Close()
			reader.closed.Store(true)
			logger.Ctx(ctx).Warn("failed to parse footer and indexes",
				zap.String("filePath", filePath),
				zap.Error(err))
			return nil, fmt.Errorf("try parse footer and indexes: %w", err)
		}
	}

	logger.Ctx(ctx).Debug("local file readerAdv created successfully",
		zap.String("filePath", filePath),
		zap.Int64("fileSize", reader.size),
		zap.Any("advOpt", reader.advOpt),
		zap.Bool("isIncompleteFile", reader.isIncompleteFile),
		zap.Int("blockIndexesCount", len(reader.blockIndexes)))
	return reader, nil
}

func (r *LocalFileReaderAdv) tryParseFooterAndIndexesIfExists(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseFooterAndIndexesIfExists")
	defer sp.End()
	// Check if file has minimum size for a footer
	if r.size < codec.RecordHeaderSize+codec.FooterRecordSize {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", r.size))
		r.isIncompleteFile = true
		return nil
	}

	// Try to read and parse footer from the end of the file
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	footerData, err := r.readAt(ctx, r.size-footerRecordSize, int(footerRecordSize))
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile = true
		return werr.ErrEntryNotFound.WithCauseErrMsg("read file failed") // client would retry later
	}

	// Try to decode footer record
	footerRecord, err := codec.DecodeRecord(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to decode footer record, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile = true
		return nil
	}

	// Check if it's actually a footer record
	if footerRecord.Type() != codec.FooterRecordType {
		logger.Ctx(ctx).Debug("no valid footer found, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Uint8("recordType", footerRecord.Type()),
			zap.Uint8("expectedType", codec.FooterRecordType))
		r.isIncompleteFile = true
		return nil
	}

	// Successfully found footer, parse using footer method
	r.footer = footerRecord.(*codec.FooterRecord)
	r.isIncompleteFile = false
	r.version = r.footer.Version
	r.flags = r.footer.Flags
	logger.Ctx(ctx).Debug("found footer, parsing index records",
		zap.String("filePath", r.filePath),
		zap.Int32("totalBlocks", r.footer.TotalBlocks))

	// Parse index records - they are located before the footer
	if r.footer.TotalBlocks > 0 {
		if err := r.parseIndexRecords(ctx); err != nil {
			logger.Ctx(ctx).Debug("failed to decode index records, performing raw scan",
				zap.String("filePath", r.filePath),
				zap.Error(err))
			r.isIncompleteFile = true
			r.footer = nil // reset footer since we can't parse indexes
			return nil
		}
	}
	return nil
}

// parseIndexRecords parses all index records from the file
func (r *LocalFileReaderAdv) parseIndexRecords(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "parseIndexRecords")
	defer sp.End()
	// Calculate where index records start
	// Index records are located before the footer record
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	indexRecordSize := int64(codec.RecordHeaderSize + codec.IndexRecordSize) // IndexRecord payload size
	totalIndexSize := int64(r.footer.TotalBlocks) * indexRecordSize

	indexStartOffset := r.size - footerRecordSize - totalIndexSize

	// Read all index data
	indexData, err := r.readAt(ctx, indexStartOffset, int(totalIndexSize))
	if err != nil {
		return fmt.Errorf("read index data: %w", err)
	}

	// Parse index records sequentially
	r.blockIndexes = make([]*codec.IndexRecord, 0, r.footer.TotalBlocks)
	offset := 0

	for i := int32(0); i < r.footer.TotalBlocks; i++ {
		if offset+codec.RecordHeaderSize+codec.IndexRecordSize > len(indexData) {
			return fmt.Errorf("incomplete index record at offset %d", offset)
		}

		recordData := indexData[offset : offset+codec.RecordHeaderSize+codec.IndexRecordSize]
		record, err := codec.DecodeRecord(recordData)
		if err != nil {
			return fmt.Errorf("decode index record %d: %w", i, err)
		}

		if record.Type() != codec.IndexRecordType {
			return fmt.Errorf("expected index record type %d, got %d at record %d", codec.IndexRecordType, record.Type(), i)
		}

		indexRecord := record.(*codec.IndexRecord)
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		offset += codec.RecordHeaderSize + codec.IndexRecordSize
	}

	// Sort index records by block number to ensure correct order
	sort.Slice(r.blockIndexes, func(i, j int) bool {
		return r.blockIndexes[i].BlockNumber < r.blockIndexes[j].BlockNumber
	})

	return nil
}

// scanForNewBlocks scans for new blocks that may have been written since last scan
// This is used for incomplete files to detect newly written data
func (r *LocalFileReaderAdv) scanForAllBlockInfo(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "scanForAllBlockInfo")
	defer sp.End()
	startTime := time.Now()

	// Get current file stat
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file for scan: %w", err)
	}

	currentFileSize := stat.Size()
	if currentFileSize <= codec.RecordHeaderSize+codec.HeaderRecordSize+codec.RecordHeaderSize+codec.BlockHeaderRecordSize {
		// No data to scan
		logger.Ctx(ctx).Debug("no data to scan",
			zap.String("filePath", r.filePath),
			zap.Int64("currentFileSize", currentFileSize))
		return nil
	}

	logger.Ctx(ctx).Debug("scanning for block index only",
		zap.String("filePath", r.filePath),
		zap.Int64("currentFileSize", currentFileSize))

	// Start scanning from the beginning of the file
	currentOffset := int64(0)

	// First, read and skip the file header record if it exists
	// Try to read HeaderRecord first
	headerRecordSize := codec.RecordHeaderSize + codec.HeaderRecordSize
	headerData, readHeaderErr := r.readAt(ctx, currentOffset, headerRecordSize)
	if readHeaderErr != nil {
		logger.Ctx(ctx).Warn("read file header failed",
			zap.String("filePath", r.filePath),
			zap.Error(readHeaderErr))
		return readHeaderErr
	}

	headerRecord, decodeHeaderErr := codec.DecodeRecord(headerData)
	if decodeHeaderErr != nil {
		logger.Ctx(ctx).Warn("decode file header failed",
			zap.String("filePath", r.filePath),
			zap.Error(decodeHeaderErr))
		return decodeHeaderErr
	}

	if headerRecord.Type() == codec.HeaderRecordType {
		// Found and decoded HeaderRecord successfully
		fileHeaderRecord := headerRecord.(*codec.HeaderRecord)
		r.version = fileHeaderRecord.Version
		r.flags = fileHeaderRecord.Flags
		currentOffset += int64(headerRecordSize)
		logger.Ctx(ctx).Debug("found file header record",
			zap.String("filePath", r.filePath),
			zap.Int64("headerOffset", int64(0)),
			zap.Uint16("version", r.version),
			zap.Uint16("flags", r.flags),
			zap.Int64("firstEntryID", fileHeaderRecord.FirstEntryID))
	} else {
		// No valid header found, start from beginning
		logger.Ctx(ctx).Warn("Error decoding header record",
			zap.String("filePath", r.filePath),
			zap.Error(decodeHeaderErr))
		return werr.ErrFileReaderInvalidRecord.WithCauseErrMsg("invalid header record type")
	}

	// Now scan for block header
	blockNumber := int32(0)
	tmpBlockIndexes := make([]*codec.IndexRecord, 0)
	for currentOffset < currentFileSize {
		// Try to read BlockHeaderRecord
		blockHeaderRecordSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		if currentOffset+int64(blockHeaderRecordSize) > currentFileSize {
			// Not enough data for a complete BlockHeaderRecord
			logger.Ctx(ctx).Debug("not enough data for block header record",
				zap.String("filePath", r.filePath),
				zap.Int64("currentOffset", currentOffset),
				zap.Int64("remainingSize", currentFileSize-currentOffset),
				zap.Int("requiredSize", blockHeaderRecordSize))
			break
		}

		blockHeaderData, err := r.readAt(ctx, currentOffset, blockHeaderRecordSize)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block header data",
				zap.String("filePath", r.filePath),
				zap.Int64("currentOffset", currentOffset),
				zap.Error(err))
			break
		}

		blockHeaderRecord, err := codec.DecodeRecord(blockHeaderData)
		if err != nil || blockHeaderRecord.Type() != codec.BlockHeaderRecordType {
			logger.Ctx(ctx).Debug("invalid block header record, stopping scan",
				zap.String("filePath", r.filePath),
				zap.Int64("currentOffset", currentOffset),
				zap.Error(err))
			break
		}

		blockHeader := blockHeaderRecord.(*codec.BlockHeaderRecord)
		blockStartOffset := currentOffset

		// Read the block data to verify CRC
		blockDataLength := int(blockHeader.BlockLength)
		blockDataOffset := currentOffset + int64(blockHeaderRecordSize)

		if blockDataOffset+int64(blockDataLength) > currentFileSize {
			// Not enough data for the complete block
			logger.Ctx(ctx).Debug("not enough data for complete block",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", blockNumber),
				zap.Int64("blockDataOffset", blockDataOffset),
				zap.Int("blockDataLength", blockDataLength),
				zap.Int64("remainingSize", currentFileSize-blockDataOffset))
			break
		}

		blockData, err := r.readAt(ctx, blockDataOffset, blockDataLength)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block data for verification",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", blockNumber),
				zap.Int64("blockDataOffset", blockDataOffset),
				zap.Error(err))
			break
		}

		// Verify block data integrity using CRC
		if err := codec.VerifyBlockDataIntegrity(blockHeader, blockData); err != nil {
			logger.Ctx(ctx).Warn("block CRC verification failed, skipping block",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", blockNumber),
				zap.Uint32("expectedCrc", blockHeader.BlockCrc),
				zap.Error(err))
			currentOffset = blockDataOffset + int64(blockDataLength)
			blockNumber++
			break
		}

		// CRC verification passed, create index record
		totalBlockSize := uint32(blockHeaderRecordSize + blockDataLength)
		indexRecord := &codec.IndexRecord{
			BlockNumber:       blockNumber,
			StartOffset:       blockStartOffset,
			FirstRecordOffset: 0,
			BlockSize:         totalBlockSize,
			FirstEntryID:      blockHeader.FirstEntryID,
			LastEntryID:       blockHeader.LastEntryID,
		}

		tmpBlockIndexes = append(tmpBlockIndexes, indexRecord)

		logger.Ctx(ctx).Debug("successfully verified and added block",
			zap.String("filePath", r.filePath),
			zap.Int32("blockNumber", blockNumber),
			zap.Int64("startOffset", blockStartOffset),
			zap.Uint32("blockSize", totalBlockSize),
			zap.Int64("firstEntryID", blockHeader.FirstEntryID),
			zap.Int64("lastEntryID", blockHeader.LastEntryID),
			zap.Uint32("blockCrc", blockHeader.BlockCrc))

		// Move to the next block
		blockNumber++
		currentOffset = blockDataOffset + int64(blockDataLength)
	}

	// Update file size
	r.size = currentFileSize
	r.blockIndexes = tmpBlockIndexes

	logger.Ctx(ctx).Info("completed block info scan",
		zap.String("filePath", r.filePath),
		zap.Int("totalBlocks", len(r.blockIndexes)),
		zap.Int64("fileSize", r.size),
		zap.Int64("scannedOffset", currentOffset))

	metrics.WpFileOperationsTotal.WithLabelValues(r.logIdStr, "loadAll", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(r.logIdStr, "loadAll", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// readAt reads data from the file at the specified offset
func (r *LocalFileReaderAdv) readAt(ctx context.Context, offset int64, length int) ([]byte, error) {
	logger.Ctx(ctx).Debug("reading data from file",
		zap.Int64("offset", offset),
		zap.Int("length", length),
		zap.Int64("fileSize", r.size),
		zap.String("filePath", r.filePath))

	// Seek to offset
	if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
		logger.Ctx(ctx).Warn("seek offset to start failed",
			zap.String("filePath", r.filePath),
			zap.Int64("offset", offset),
			zap.Int("requestedLength", length),
			zap.Error(err))
		return nil, err
	}

	// Read data
	data := make([]byte, length)
	n, err := io.ReadFull(r.file, data)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read data from file",
			zap.String("filePath", r.filePath),
			zap.Int64("offset", offset),
			zap.Int("requestedLength", length),
			zap.Int("actualRead", n),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("data read successfully",
		zap.String("filePath", r.filePath),
		zap.Int64("offset", offset),
		zap.Int("requestedLength", length),
		zap.Int("actualRead", n))

	return data[:n], nil
}

// GetLastEntryID returns the last entry ID in the file
func (r *LocalFileReaderAdv) GetLastEntryID(ctx context.Context) (int64, error) {
	if r.closed.Load() {
		return -1, werr.ErrFileReaderAlreadyClosed
	}

	// If we have a footer, return the last entry ID from it
	if r.footer != nil && len(r.blockIndexes) > 0 {
		lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
		return lastBlock.LastEntryID, nil
	}

	// If the file is incomplete, try to scan for new blocks to get the latest entry ID
	if r.isIncompleteFile {
		if err := r.scanForAllBlockInfo(ctx); err != nil {
			return -1, err
		}
	}

	// Check if we have any blocks after scanning
	if len(r.blockIndexes) == 0 {
		return -1, werr.ErrFileReaderNoBlockFound
	}

	// Return the last entry ID from the last block
	lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
	return lastBlock.LastEntryID, nil
}

func (r *LocalFileReaderAdv) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt) (*storage.Batch, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatchAdv")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatchAdv called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("batchSize", opt.BatchSize),
		zap.Bool("isIncompleteFile", r.isIncompleteFile),
		zap.Bool("footerExists", r.footer != nil),
		zap.Any("opt", opt),
		zap.Any("advOpt", r.advOpt))

	if r.closed.Load() {
		return nil, werr.ErrFileReaderAlreadyClosed
	}

	startBlockID := int64(0)
	startBlockOffset := int64(0)
	if r.advOpt != nil {
		// Scenario 1: Subsequent reads with advanced options
		// When we have advOpt, start from the block after the last read block
		startBlockID = int64(r.advOpt.LastBlockInfo.BlockNumber + 1)
		startBlockOffset = r.advOpt.LastBlockInfo.StartOffset + int64(r.advOpt.LastBlockInfo.BlockSize)
		logger.Ctx(ctx).Debug("using advOpt mode",
			zap.Int64("lastBlockNumber", int64(r.advOpt.LastBlockInfo.BlockNumber)),
			zap.Int64("startBlockID", startBlockID),
			zap.Int64("startBlockOffset", startBlockOffset))
	} else if r.footer != nil {
		// Scenario 2: First read with footer (completed file)
		// When we have footer, find the block containing the start entry ID
		foundStartBlock, err := r.searchBlock(r.blockIndexes, opt.StartEntryID)
		if err != nil {
			logger.Ctx(ctx).Warn("search block failed", zap.String("filePath", r.filePath), zap.Int64("entryId", opt.StartEntryID), zap.Error(err))
			return nil, err
		}
		if foundStartBlock != nil {
			logger.Ctx(ctx).Debug("found block for entryId", zap.Int64("entryId", opt.StartEntryID), zap.Int32("blockID", foundStartBlock.BlockNumber))
			startBlockID = int64(foundStartBlock.BlockNumber)
			startBlockOffset = foundStartBlock.StartOffset
		}
		// No block found for entryId in this completed file
		if foundStartBlock == nil {
			logger.Ctx(ctx).Debug("no more entries to read",
				zap.String("filePath", r.filePath),
				zap.Int64("startEntryId", opt.StartEntryID))
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
	}

	return r.readDataBlocks(ctx, opt, startBlockID, startBlockOffset)
}

func (r *LocalFileReaderAdv) readDataBlocks(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startBlockOffset int64) (*storage.Batch, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

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
	startOffset := startBlockOffset
	entriesCollected := int64(0)
	readBytes := 0

	// extract data from blocks
	for i := startBlockID; readBytes < maxBytes && entriesCollected < maxEntries; i++ {
		currentBlockID := i

		headersLen := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		if currentBlockID == 0 && startOffset == 0 {
			// if we start from the beginning, we need to read the file header to init version&flags
			headersLen = codec.RecordHeaderSize + codec.HeaderRecordSize + codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		}

		// Read header Record
		headersData, readErr := r.readAt(ctx, startOffset, headersLen)
		if readErr != nil {
			logger.Ctx(ctx).Warn("Failed to read block header",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(readErr))
			break
		}
		headerRecords, decodeErr := codec.DecodeRecordList(headersData)
		if decodeErr != nil {
			logger.Ctx(ctx).Warn("Failed to decode block",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(decodeErr))
			break
		}
		// Find BlockHeaderRecord and verify data integrity
		var blockHeaderRecord *codec.BlockHeaderRecord
		for _, record := range headerRecords {
			if record.Type() == codec.HeaderRecordType {
				// if no footer/no advOpt, it may start from 0, and header record will be read to init version&flags
				fileHeaderRecord := record.(*codec.HeaderRecord)
				r.version = fileHeaderRecord.Version
				r.flags = fileHeaderRecord.Flags
				continue
			}
			if record.Type() == codec.BlockHeaderRecordType {
				blockHeaderRecord = record.(*codec.BlockHeaderRecord)
				break
			}
		}
		if blockHeaderRecord == nil {
			logger.Ctx(ctx).Warn("block header record not found, stop reading",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID))
			break
		}
		blockDataLen := int(blockHeaderRecord.BlockLength)

		// Read the block data
		blockData, err := r.readAt(ctx, startOffset+int64(headersLen), blockDataLen)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to read block data",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			break // Stop reading on error, return what we have so far
		}
		// Verify the block data integrity
		if err := r.verifyBlockDataIntegrity(ctx, blockHeaderRecord, currentBlockID, blockData); err != nil {
			logger.Ctx(ctx).Warn("verify block data integrity failed, stop reading",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			break // Stop reading on data integrity error, return what we have so far
		}

		// decode block data to extract data records
		records, decodeErr := codec.DecodeRecordList(blockData)
		if decodeErr != nil {
			logger.Ctx(ctx).Warn("Failed to decode block",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(decodeErr))
			break // Stop reading on error, return what we have so far
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
			zap.String("filePath", r.filePath),
			zap.Int64("blockNumber", currentBlockID),
			zap.Int64("collectedEntries", entriesCollected),
			zap.Int("collectedBytes", readBytes))

		lastBlockInfo = &codec.IndexRecord{
			BlockNumber:       int32(currentBlockID),
			StartOffset:       startOffset,
			FirstRecordOffset: 0,
			BlockSize:         uint32(headersLen + blockDataLen),
			FirstEntryID:      blockHeaderRecord.FirstEntryID,
			LastEntryID:       blockHeaderRecord.LastEntryID,
		}

		// Move to the next block
		startOffset += int64(headersLen + blockDataLen)
	}

	if len(entries) == 0 {
		logger.Ctx(ctx).Debug("no entry extracted",
			zap.String("filePath", r.filePath),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("requestedBatchSize", opt.BatchSize),
			zap.Int("entriesReturned", len(entries)))
		if !r.isIncompleteFile || r.footer != nil {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
		if r.isFooterExists(ctx) {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("read data blocks completed",
			zap.String("filePath", r.filePath),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("requestedBatchSize", opt.BatchSize),
			zap.Int("entriesReturned", len(entries)),
			zap.Any("lastBlockInfo", lastBlockInfo))
	}

	metrics.WpFileReadBatchBytes.WithLabelValues(r.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(r.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	// Create batch with proper error handling for nil lastBlockInfo
	var batchInfo *storage.BatchInfo
	if lastBlockInfo != nil {
		batchInfo = &storage.BatchInfo{
			Flags:         r.flags,
			Version:       r.version,
			LastBlockInfo: lastBlockInfo,
		}
	}

	batch := &storage.Batch{
		LastBatchInfo: batchInfo,
		Entries:       entries,
	}
	return batch, nil
}

func (r *LocalFileReaderAdv) isFooterExists(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "isFooterExists")
	defer sp.End()
	// Check if file has minimum size for a footer
	if r.size < codec.RecordHeaderSize+codec.FooterRecordSize {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", r.size))
		return false
	}

	// Try to read and parse footer from the end of the file
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	footerData, err := r.readAt(ctx, r.size-footerRecordSize, int(footerRecordSize))
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return false
	}

	// Try to decode footer record
	footerRecord, err := codec.DecodeRecord(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to decode footer record, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return false
	}

	// Check if it's actually a footer record
	if footerRecord.Type() != codec.FooterRecordType {
		logger.Ctx(ctx).Debug("no valid footer found, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Uint8("recordType", footerRecord.Type()),
			zap.Uint8("expectedType", codec.FooterRecordType))
		return false
	}

	// footer exists
	return true
}

func (r *LocalFileReaderAdv) verifyBlockDataIntegrity(ctx context.Context, blockHeaderRecord *codec.BlockHeaderRecord, currentBlockID int64, dataRecordsBuffer []byte) error {
	// Verify block data integrity
	if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, dataRecordsBuffer); err != nil {
		logger.Ctx(ctx).Warn("block data integrity verification failed",
			zap.String("filePath", r.filePath),
			zap.Int64("blockNumber", currentBlockID),
			zap.Error(err))
		return err // Stop reading on error, return what we have so far
	}

	logger.Ctx(ctx).Debug("block data integrity verified successfully",
		zap.String("filePath", r.filePath),
		zap.Int64("blockNumber", currentBlockID),
		zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
		zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
	return nil
}

func (r *LocalFileReaderAdv) searchBlock(list []*codec.IndexRecord, entryId int64) (*codec.IndexRecord, error) {
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

// ReadNextBatch reads the next batch of entries
func (r *LocalFileReaderAdv) ReadNextBatch(ctx context.Context, opt storage.ReaderOpt) ([]*proto.LogEntry, error) {
	return nil, werr.ErrOperationNotSupported.WithCauseErrMsg("not support simple read, use ReadNextBatchAdv instead")
}

// GetBlockIndexes returns all block indexes
func (r *LocalFileReaderAdv) GetBlockIndexes() []*codec.IndexRecord {
	return r.blockIndexes
}

// GetFooter returns the footer record
func (r *LocalFileReaderAdv) GetFooter() *codec.FooterRecord {
	return r.footer
}

// GetTotalRecords returns the total number of records
func (r *LocalFileReaderAdv) GetTotalRecords() uint32 {
	if r.footer != nil {
		return r.footer.TotalRecords
	}
	return 0
}

// GetTotalBlocks returns the total number of blocks
func (r *LocalFileReaderAdv) GetTotalBlocks() int32 {
	if r.footer != nil {
		return r.footer.TotalBlocks
	}
	return 0
}

// Close closes the reader
func (r *LocalFileReaderAdv) Close(ctx context.Context) error {
	if !r.closed.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug("reader already closed", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId))
		return nil
	}

	if r.file != nil {
		if err := r.file.Close(); err != nil {
			return fmt.Errorf("close file: %w", err)
		}
		r.file = nil
	}

	return nil
}
