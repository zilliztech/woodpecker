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
	"sync"
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

var (
	SegmentReaderScope = "LocalFileReader"
)

var _ storage.Reader = (*LocalFileReaderAdv)(nil)

// LocalFileReaderAdv implements AbstractFileReader for local filesystem storage
type LocalFileReaderAdv struct {
	logId    int64
	segId    int64
	logIdStr string // for metrics only
	filePath string

	// read config
	batchMaxSize int64

	// file access
	mu   sync.RWMutex
	file *os.File

	// runtime file state
	flags            atomic.Uint32 // stores uint16 value
	version          atomic.Uint32 // stores uint16 value
	footer           *codec.FooterRecord
	blockIndexes     []*codec.IndexRecord
	isIncompleteFile atomic.Bool // Whether this file is incomplete (no footer)
	closed           atomic.Bool
}

// NewLocalFileReaderAdv creates a new local filesystem reader
func NewLocalFileReaderAdv(ctx context.Context, baseDir string, logId int64, segId int64, batchMaxSize int64) (*LocalFileReaderAdv, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewLocalFileReader")
	defer sp.End()

	logger.Ctx(ctx).Debug("creating new local file reader",
		zap.String("baseDir", baseDir),
		zap.Int64("logId", logId),
		zap.Int64("segId", segId))

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

	currentSize := stat.Size()
	reader := &LocalFileReaderAdv{
		logId:    logId,
		segId:    segId,
		logIdStr: fmt.Sprintf("%d", logId),
		filePath: filePath,
		file:     file,

		batchMaxSize: batchMaxSize,
	}
	reader.flags.Store(0)
	reader.version.Store(codec.FormatVersion)
	reader.closed.Store(false)

	// try to check footer when opening
	reader.isIncompleteFile.Store(true)
	parseFooterErr := reader.tryParseFooterAndIndexesIfExists(ctx)
	if parseFooterErr != nil {
		file.Close()
		reader.closed.Store(true)
		logger.Ctx(ctx).Warn("failed to parse footer and indexes",
			zap.String("filePath", filePath),
			zap.Error(err))
		return nil, fmt.Errorf("try parse footer and indexes: %w", err)
	}

	logger.Ctx(ctx).Debug("local file readerAdv created successfully",
		zap.String("filePath", filePath),
		zap.Int64("currentFileSize", currentSize),
		zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()),
		zap.Int("blockIndexesCount", len(reader.blockIndexes)))
	return reader, nil
}

func (r *LocalFileReaderAdv) tryParseFooterAndIndexesIfExists(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseFooterAndIndexesIfExists")
	defer sp.End()
	logger.Ctx(ctx).Debug("try to read footer and block indexes", zap.String("filePath", r.filePath))

	// check if already exists
	if !r.isIncompleteFile.Load() {
		// already parse an exists footer
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// double check if already exists
	if !r.isIncompleteFile.Load() {
		// already parse an exists footer
		return nil
	}

	stat, err := r.file.Stat()
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stat file",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return fmt.Errorf("stat file: %w", err)
	}
	currentFileSize := stat.Size()

	// Check if file has minimum size for a footer
	if currentFileSize < codec.RecordHeaderSize+codec.FooterRecordSize {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", currentFileSize))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Try to read and parse footer from the end of the file
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	footerData, err := r.readAtUnsafe(ctx, currentFileSize-footerRecordSize, int(footerRecordSize))
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile.Store(true)
		return werr.ErrEntryNotFound.WithCauseErrMsg("read file failed") // client would retry later
	}

	// Try to decode footer record
	footerRecord, err := codec.DecodeRecord(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to decode footer record, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Check if it's actually a footer record
	if footerRecord.Type() != codec.FooterRecordType {
		logger.Ctx(ctx).Debug("no valid footer found, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Uint8("recordType", footerRecord.Type()),
			zap.Uint8("expectedType", codec.FooterRecordType))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Successfully found footer, parse using footer method
	r.footer = footerRecord.(*codec.FooterRecord)
	r.isIncompleteFile.Store(false)
	r.version.Store(uint32(r.footer.Version))
	r.flags.Store(uint32(r.footer.Flags))
	logger.Ctx(ctx).Debug("found footer, parsing index records",
		zap.String("filePath", r.filePath),
		zap.Int32("totalBlocks", r.footer.TotalBlocks))

	// Parse index records - they are located before the footer
	if r.footer.TotalBlocks > 0 {
		if err := r.parseIndexRecordsUnsafe(ctx, currentFileSize); err != nil {
			logger.Ctx(ctx).Debug("failed to decode index records, performing raw scan",
				zap.String("filePath", r.filePath),
				zap.Error(err))
			r.isIncompleteFile.Store(true)
			r.footer = nil // reset footer since we can't parse indexes
			return nil
		}
	}
	return nil
}

// parseIndexRecords parses all index records from the file
func (r *LocalFileReaderAdv) parseIndexRecordsUnsafe(ctx context.Context, currentFileSize int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "parseIndexRecords")
	defer sp.End()
	// Calculate where index records start
	// Index records are located before the footer record
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	indexRecordSize := int64(codec.RecordHeaderSize + codec.IndexRecordSize) // IndexRecord payload size
	totalIndexSize := int64(r.footer.TotalBlocks) * indexRecordSize

	indexStartOffset := currentFileSize - footerRecordSize - totalIndexSize

	if indexStartOffset < 0 {
		return fmt.Errorf("invalid index start offset: %d", indexStartOffset)
	}

	// Read all index data
	indexData, err := r.readAtUnsafe(ctx, indexStartOffset, int(totalIndexSize))
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

func (r *LocalFileReaderAdv) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt, lastReadBatchInfo *storage.BatchInfo) (*storage.Batch, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatchAdv")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatchAdv called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("batchSize", opt.BatchSize),
		zap.Bool("isIncompleteFile", r.isIncompleteFile.Load()),
		zap.Bool("footerExists", r.footer != nil),
		zap.Any("opt", opt),
		zap.Any("lastReadBatchInfo", lastReadBatchInfo))

	if r.closed.Load() {
		return nil, werr.ErrFileReaderAlreadyClosed
	}

	// set adv options
	if lastReadBatchInfo != nil {
		r.flags.Store(uint32(lastReadBatchInfo.Flags))
		r.version.Store(uint32(lastReadBatchInfo.Version))
	} else {
		// Try to parse footer and indexes
		if err := r.tryParseFooterAndIndexesIfExists(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to parse footer and indexes",
				zap.String("filePath", r.filePath),
				zap.Error(err))
			return nil, fmt.Errorf("try parse footer and indexes: %w", err)
		}
	}

	// hold read lock
	r.mu.RLock()
	defer r.mu.RUnlock()

	// start read batch from a certain point - need to protect blockIndexes read
	startBlockID := int64(0)
	startBlockOffset := int64(0)
	if lastReadBatchInfo != nil {
		// Scenario 1: Subsequent reads with advanced options
		// When we have lastReadBatchInfo, start from the block after the last read block
		startBlockID = int64(lastReadBatchInfo.LastBlockInfo.BlockNumber + 1)
		startBlockOffset = lastReadBatchInfo.LastBlockInfo.StartOffset + int64(lastReadBatchInfo.LastBlockInfo.BlockSize)
		logger.Ctx(ctx).Debug("using advOpt mode",
			zap.Int64("lastBlockNumber", int64(lastReadBatchInfo.LastBlockInfo.BlockNumber)),
			zap.Int64("startBlockID", startBlockID),
			zap.Int64("startBlockOffset", startBlockOffset))
	} else if !r.isIncompleteFile.Load() {
		// Scenario 2: First read with footer (completed file)
		// When we have footer, find the block containing the start entry ID
		foundStartBlock, err := codec.SearchBlock(r.blockIndexes, opt.StartEntryID)
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

	return r.readDataBlocksUnsafe(ctx, opt, startBlockID, startBlockOffset)
}

// readAt reads data from the file at the specified offset
func (r *LocalFileReaderAdv) readAtUnsafe(ctx context.Context, offset int64, length int) ([]byte, error) {
	logger.Ctx(ctx).Debug("reading data from file",
		zap.Int64("offset", offset),
		zap.Int("length", length),
		zap.String("filePath", r.filePath))

	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid read parameters: offset=%d, length=%d", offset, length)
	}

	// Read data
	data := make([]byte, length)
	n, err := r.file.ReadAt(data, offset)
	if err != nil {
		// if EOF, maybe the file is at the beginning or EOF, otherwise it maybe in error state
		if err != io.EOF {
			logger.Ctx(ctx).Warn("failed to read data from file", zap.String("filePath", r.filePath), zap.Int64("offset", offset), zap.Int("requestedLength", length), zap.Int("actualRead", n), zap.Error(err))
		}
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

	// First try to read without write lock
	r.mu.RLock()
	if r.footer != nil && len(r.blockIndexes) > 0 {
		lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
		lastEntryID := lastBlock.LastEntryID
		r.mu.RUnlock()
		return lastEntryID, nil
	}
	r.mu.RUnlock()

	// If the file is incomplete, try to scan for new blocks to get the latest entry ID
	if r.isIncompleteFile.Load() {
		r.mu.Lock() // Need write lock for scanning
		defer r.mu.Unlock()

		// Double-check after acquiring write lock
		if r.footer != nil && len(r.blockIndexes) > 0 {
			lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
			return lastBlock.LastEntryID, nil
		}

		if err := r.scanForAllBlockInfoUnsafe(ctx); err != nil {
			return -1, err
		}

		// Check if we have any blocks after scanning
		if len(r.blockIndexes) == 0 {
			return -1, werr.ErrFileReaderNoBlockFound
		}

		// Return the last entry ID from the last block
		lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
		return lastBlock.LastEntryID, nil
	}

	return -1, werr.ErrFileReaderNoBlockFound
}

// scanForNewBlocks scans for new blocks that may have been written since last scan
// This is used for incomplete files to detect newly written data
func (r *LocalFileReaderAdv) scanForAllBlockInfoUnsafe(ctx context.Context) error {
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
	headerData, readHeaderErr := r.readAtUnsafe(ctx, currentOffset, headerRecordSize)
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
		r.version.Store(uint32(fileHeaderRecord.Version))
		r.flags.Store(uint32(fileHeaderRecord.Flags))
		currentOffset += int64(headerRecordSize)
		logger.Ctx(ctx).Debug("found file header record",
			zap.String("filePath", r.filePath),
			zap.Int64("headerOffset", int64(0)),
			zap.Uint16("version", uint16(r.version.Load())),
			zap.Uint16("flags", uint16(r.flags.Load())),
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

		blockHeaderData, err := r.readAtUnsafe(ctx, currentOffset, blockHeaderRecordSize)
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
				zap.Int32("readBlockNumber", blockHeader.BlockNumber),
				zap.Int64("blockDataOffset", blockDataOffset),
				zap.Int("blockDataLength", blockDataLength),
				zap.Int64("remainingSize", currentFileSize-blockDataOffset))
			break
		}

		blockData, err := r.readAtUnsafe(ctx, blockDataOffset, blockDataLength)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block data for verification",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", blockNumber),
				zap.Int32("readBlockNumber", blockHeader.BlockNumber),
				zap.Int64("blockDataOffset", blockDataOffset),
				zap.Error(err))
			break
		}

		// Verify block data integrity using CRC
		if err := codec.VerifyBlockDataIntegrity(blockHeader, blockData); err != nil {
			logger.Ctx(ctx).Warn("block CRC verification failed, skipping block",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", blockNumber),
				zap.Int32("readBlockNumber", blockHeader.BlockNumber),
				zap.Uint32("expectedCrc", blockHeader.BlockCrc),
				zap.Error(err))
			currentOffset = blockDataOffset + int64(blockDataLength)
			blockNumber++
			break
		}

		// CRC verification passed, create index record
		totalBlockSize := uint32(blockHeaderRecordSize + blockDataLength)
		indexRecord := &codec.IndexRecord{
			BlockNumber:  blockNumber,
			StartOffset:  blockStartOffset,
			BlockSize:    totalBlockSize,
			FirstEntryID: blockHeader.FirstEntryID,
			LastEntryID:  blockHeader.LastEntryID,
		}

		tmpBlockIndexes = append(tmpBlockIndexes, indexRecord)

		logger.Ctx(ctx).Debug("successfully verified and added block",
			zap.String("filePath", r.filePath),
			zap.Int32("blockNumber", blockNumber),
			zap.Int64("startOffset", blockStartOffset),
			zap.Uint32("blockSize", totalBlockSize),
			zap.Int32("readBlockNumber", blockHeader.BlockNumber),
			zap.Int64("firstEntryID", blockHeader.FirstEntryID),
			zap.Int64("lastEntryID", blockHeader.LastEntryID),
			zap.Uint32("blockCrc", blockHeader.BlockCrc))

		// Move to the next block
		blockNumber++
		currentOffset = blockDataOffset + int64(blockDataLength)
	}

	// Update file size
	r.blockIndexes = tmpBlockIndexes

	logger.Ctx(ctx).Info("completed block info scan",
		zap.String("filePath", r.filePath),
		zap.Int("totalBlocks", len(r.blockIndexes)),
		zap.Int64("fileSize", currentFileSize),
		zap.Int64("scannedOffset", currentOffset))

	metrics.WpFileOperationsTotal.WithLabelValues(r.logIdStr, "loadAll", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(r.logIdStr, "loadAll", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

func (r *LocalFileReaderAdv) readDataBlocksUnsafe(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startBlockOffset int64) (*storage.Batch, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

	// Soft limitation
	maxEntries := opt.BatchSize // soft count limit: Read the minimum number of blocks exceeding the count limit
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := r.batchMaxSize // soft bytes limit: Read the minimum number of blocks exceeding the bytes limit

	// read data result
	entries := make([]*proto.LogEntry, 0, maxEntries)
	var lastBlockInfo *codec.IndexRecord

	// read stats
	startOffset := startBlockOffset
	entriesCollected := int64(0)
	readBytes := int64(0)
	hasDataReadError := false

	// extract data from blocks
	for i := startBlockID; readBytes < maxBytes && entriesCollected < maxEntries; i++ {
		currentBlockID := i

		headersLen := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		if currentBlockID == 0 && startOffset == 0 {
			// if we start from the beginning, we need to read the file header to init version&flags
			headersLen = codec.RecordHeaderSize + codec.HeaderRecordSize + codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		}

		// Read header Record
		headersData, readErr := r.readAtUnsafe(ctx, startOffset, headersLen)
		if readErr != nil {
			// if EOF, maybe the file is at the beginning or EOF, otherwise it maybe in error state
			if io.EOF == readErr {
				logger.Ctx(ctx).Info("no block header to read currently, retry later", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID))
			} else {
				logger.Ctx(ctx).Warn("Failed to read block header", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID), zap.Error(readErr))
			}
			hasDataReadError = true
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
				r.version.Store(uint32(fileHeaderRecord.Version))
				r.flags.Store(uint32(fileHeaderRecord.Flags))
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
		blockData, err := r.readAtUnsafe(ctx, startOffset+int64(headersLen), blockDataLen)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to read block data",
				zap.String("filePath", r.filePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			hasDataReadError = true
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
				readBytes += int64(len(r.Payload))
			}
			currentEntryID++
		}

		logger.Ctx(ctx).Debug("Extracted data from block",
			zap.String("filePath", r.filePath),
			zap.Int64("blockNumber", currentBlockID),
			zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Int64("blockFirstEntryID", blockHeaderRecord.FirstEntryID),
			zap.Int64("blockLastEntryID", blockHeaderRecord.LastEntryID),
			zap.Int64("lastCollectedEntryID", currentEntryID),
			zap.Int64("totalCollectedEntries", entriesCollected),
			zap.Int64("totalCollectedBytes", readBytes))

		lastBlockInfo = &codec.IndexRecord{
			BlockNumber:  int32(currentBlockID),
			StartOffset:  startOffset,
			BlockSize:    uint32(headersLen + blockDataLen),
			FirstEntryID: blockHeaderRecord.FirstEntryID,
			LastEntryID:  blockHeaderRecord.LastEntryID,
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
		if !hasDataReadError {
			// only read without dataReadError, determine whether it is an EOF
			if !r.isIncompleteFile.Load() || r.footer != nil {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
			if r.isFooterExistsUnsafe(ctx) {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
		}
		// return entryNotFound to let read caller retry later
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
			Flags:         uint16(r.flags.Load()),
			Version:       uint16(r.version.Load()),
			LastBlockInfo: lastBlockInfo,
		}
	}

	batch := &storage.Batch{
		LastBatchInfo: batchInfo,
		Entries:       entries,
	}
	return batch, nil
}

func (r *LocalFileReaderAdv) isFooterExistsUnsafe(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "isFooterExists")
	defer sp.End()

	// update size
	stat, err := r.file.Stat()
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stat file",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return false
	}
	currentSize := stat.Size()

	// Check if file has minimum size for a footer
	if currentSize < codec.RecordHeaderSize+codec.FooterRecordSize {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", currentSize))
		return false
	}

	// Try to read and parse footer from the end of the file
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	footerData, err := r.readAtUnsafe(ctx, currentSize-footerRecordSize, int(footerRecordSize))
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
			zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Error(err))
		return err // Stop reading on error, return what we have so far
	}

	logger.Ctx(ctx).Debug("block data integrity verified successfully",
		zap.String("filePath", r.filePath),
		zap.Int64("blockNumber", currentBlockID),
		zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber),
		zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
		zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
	return nil
}

// Close closes the reader
func (r *LocalFileReaderAdv) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "Close")
	defer sp.End()

	r.mu.Lock()
	defer r.mu.Unlock()

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

// GetBlockIndexes Test Only
func (r *LocalFileReaderAdv) GetBlockIndexes() []*codec.IndexRecord {
	return r.blockIndexes
}

// GetFooter Test Only
func (r *LocalFileReaderAdv) GetFooter() *codec.FooterRecord {
	return r.footer
}

// GetTotalRecords Test Only
func (r *LocalFileReaderAdv) GetTotalRecords() uint32 {
	if r.footer != nil {
		return r.footer.TotalRecords
	}
	return 0
}

// GetTotalBlocks Test Only
func (r *LocalFileReaderAdv) GetTotalBlocks() int32 {
	if r.footer != nil {
		return r.footer.TotalBlocks
	}
	return 0
}
