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

// =========================================================================================
// LEGACY MODE SUPPORT FUNCTIONS (for single-file data.log format)
// =========================================================================================
// This file contains backward compatibility code for the old data.log single-file format.
// These functions are only used when perBlockMode is false (auto-detected during constructor).
//
// LEGACY-ONLY METHODS (ordered by call hierarchy):
//   - tryParseFooterAndIndexesIfExists: parses footer from legacy file (called by constructor)
//   - parseIndexRecordsUnsafe: parses index records from legacy footer
//   - readAtUnsafe: reads data from the single data.log file at specified offset
//   - scanForAllBlockInfoUnsafe: scans legacy file to build block index
//   - readDataBlocksUnsafe: reads data blocks from legacy single file
//   - verifyBlockDataIntegrity: wrapper for block verification in legacy mode
//
// BRANCHING POINTS (in reader_impl.go):
//   - Constructor: detects mode and branches to per-block vs legacy mode
//   - ReadNextBatchAdv: calls readNextBatchPerBlockMode or readDataBlocksUnsafe
//   - GetLastEntryID: calls loadBlockFilesUnsafe or scanForAllBlockInfoUnsafe
//   - isFooterExistsUnsafe: checks footer.blk or parses legacy footer
//
// IMPORTANT: After branching, the legacy code path calls these legacy-specific methods
// and does NOT share member methods with the per-block mode code. Only utility functions
// (like path helpers and codec functions) are shared.
// =========================================================================================

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// tryParseFooterAndIndexesIfExists is a LEGACY-ONLY method for parsing footer from data.log
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
		logger.Ctx(ctx).Warn("failed to stat file", zap.String("filePath", r.filePath), zap.Error(err))
		return fmt.Errorf("stat file: %w", err)
	}
	currentFileSize := stat.Size()

	// Check if file has minimum size for a footer (use V5 footer size as minimum)
	minFooterSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5
	if currentFileSize < int64(minFooterSize) {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, performing raw scan", zap.String("filePath", r.filePath), zap.Int64("fileSize", currentFileSize))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Try to read and parse footer from the end of the file using compatibility parsing
	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData, err := r.readAtUnsafe(ctx, currentFileSize-int64(maxFooterSize), maxFooterSize)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, performing raw scan", zap.String("filePath", r.filePath), zap.Error(err))
		r.isIncompleteFile.Store(true)
		return werr.ErrEntryNotFound.WithCauseErrMsg("read file failed") // client would retry later
	}

	// Try to parse footer with compatibility parsing
	footerRecord, err := codec.ParseFooterFromBytes(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to parse footer record, performing raw scan", zap.String("filePath", r.filePath), zap.Error(err))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Successfully found footer, parse using footer method
	r.footer = footerRecord
	r.isIncompleteFile.Store(false)
	r.version.Store(uint32(r.footer.Version))
	r.flags.Store(uint32(r.footer.Flags))
	logger.Ctx(ctx).Debug("found footer, parsing index records", zap.String("filePath", r.filePath), zap.Int32("totalBlocks", r.footer.TotalBlocks))

	// Parse index records - they are located before the footer
	if r.footer.TotalBlocks > 0 {
		if err := r.parseIndexRecordsUnsafe(ctx, currentFileSize); err != nil {
			logger.Ctx(ctx).Debug("failed to decode index records, performing raw scan", zap.String("filePath", r.filePath), zap.Error(err))
			r.isIncompleteFile.Store(true)
			r.footer = nil // reset footer since we can't parse indexes
			return nil
		}
	}
	return nil
}

// parseIndexRecordsUnsafe is a LEGACY-ONLY method that parses all index records from the file
func (r *LocalFileReaderAdv) parseIndexRecordsUnsafe(ctx context.Context, currentFileSize int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "parseIndexRecords")
	defer sp.End()
	// Calculate where index records start
	// Index records are located before the footer record
	footerRecordSize := int64(codec.RecordHeaderSize + codec.GetFooterRecordSize(r.footer.Version))
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

// readAtUnsafe is a LEGACY-ONLY method that reads data from the single data.log file
func (r *LocalFileReaderAdv) readAtUnsafe(ctx context.Context, offset int64, length int) ([]byte, error) {
	logger.Ctx(ctx).Debug("reading data from file", zap.Int64("offset", offset), zap.Int("length", length), zap.String("filePath", r.filePath))

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

	logger.Ctx(ctx).Debug("data read successfully", zap.String("filePath", r.filePath),
		zap.Int64("offset", offset), zap.Int("requestedLength", length), zap.Int("actualRead", n))

	return data[:n], nil
}

// scanForAllBlockInfoUnsafe is a LEGACY-ONLY method that scans the single data.log file
// to build block index. This is used for incomplete legacy files to detect newly written data.
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
		logger.Ctx(ctx).Debug("no data to scan", zap.String("filePath", r.filePath), zap.Int64("currentFileSize", currentFileSize))
		return nil
	}

	logger.Ctx(ctx).Debug("scanning for block index only", zap.String("filePath", r.filePath), zap.Int64("currentFileSize", currentFileSize))

	// Start scanning from the beginning of the file
	currentOffset := int64(0)

	// First, read and skip the file header record if it exists
	// Try to read HeaderRecord first
	headerRecordSize := codec.RecordHeaderSize + codec.HeaderRecordSize
	headerData, readHeaderErr := r.readAtUnsafe(ctx, currentOffset, headerRecordSize)
	if readHeaderErr != nil {
		logger.Ctx(ctx).Warn("read file header failed", zap.String("filePath", r.filePath), zap.Error(readHeaderErr))
		return readHeaderErr
	}

	headerRecord, decodeHeaderErr := codec.DecodeRecord(headerData)
	if decodeHeaderErr != nil {
		logger.Ctx(ctx).Warn("decode file header failed", zap.String("filePath", r.filePath), zap.Error(decodeHeaderErr))
		return decodeHeaderErr
	}

	if headerRecord.Type() == codec.HeaderRecordType {
		// Found and decoded HeaderRecord successfully
		fileHeaderRecord := headerRecord.(*codec.HeaderRecord)
		r.version.Store(uint32(fileHeaderRecord.Version))
		r.flags.Store(uint32(fileHeaderRecord.Flags))
		currentOffset += int64(headerRecordSize)
		logger.Ctx(ctx).Debug("found file header record", zap.String("filePath", r.filePath), zap.Int64("headerOffset", int64(0)),
			zap.Uint16("version", uint16(r.version.Load())), zap.Uint16("flags", uint16(r.flags.Load())), zap.Int64("firstEntryID", fileHeaderRecord.FirstEntryID))
	} else {
		// No valid header found, start from beginning
		logger.Ctx(ctx).Warn("Error decoding header record", zap.String("filePath", r.filePath), zap.Error(decodeHeaderErr))
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
			logger.Ctx(ctx).Debug("not enough data for block header record", zap.String("filePath", r.filePath),
				zap.Int64("currentOffset", currentOffset), zap.Int64("remainingSize", currentFileSize-currentOffset), zap.Int("requiredSize", blockHeaderRecordSize))
			break
		}

		blockHeaderData, err := r.readAtUnsafe(ctx, currentOffset, blockHeaderRecordSize)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block header data", zap.String("filePath", r.filePath), zap.Int64("currentOffset", currentOffset), zap.Error(err))
			break
		}

		blockHeaderRecord, err := codec.DecodeRecord(blockHeaderData)
		if err != nil || blockHeaderRecord.Type() != codec.BlockHeaderRecordType {
			logger.Ctx(ctx).Debug("invalid block header record, stopping scan", zap.String("filePath", r.filePath), zap.Int64("currentOffset", currentOffset), zap.Error(err))
			break
		}

		blockHeader := blockHeaderRecord.(*codec.BlockHeaderRecord)
		blockStartOffset := currentOffset

		// Read the block data to verify CRC
		blockDataLength := int(blockHeader.BlockLength)
		blockDataOffset := currentOffset + int64(blockHeaderRecordSize)

		if blockDataOffset+int64(blockDataLength) > currentFileSize {
			// Not enough data for the complete block
			logger.Ctx(ctx).Debug("not enough data for complete block", zap.String("filePath", r.filePath), zap.Int32("blockNumber", blockNumber),
				zap.Int32("readBlockNumber", blockHeader.BlockNumber), zap.Int64("blockDataOffset", blockDataOffset), zap.Int("blockDataLength", blockDataLength), zap.Int64("remainingSize", currentFileSize-blockDataOffset))
			break
		}

		blockData, err := r.readAtUnsafe(ctx, blockDataOffset, blockDataLength)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block data for verification", zap.String("filePath", r.filePath), zap.Int32("blockNumber", blockNumber),
				zap.Int32("readBlockNumber", blockHeader.BlockNumber), zap.Int64("blockDataOffset", blockDataOffset), zap.Error(err))
			break
		}

		// Verify block data integrity using CRC
		if err := codec.VerifyBlockDataIntegrity(blockHeader, blockData); err != nil {
			logger.Ctx(ctx).Warn("block CRC verification failed, skipping block", zap.String("filePath", r.filePath), zap.Int32("blockNumber", blockNumber),
				zap.Int32("readBlockNumber", blockHeader.BlockNumber), zap.Uint32("expectedCrc", blockHeader.BlockCrc), zap.Error(err))
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

		logger.Ctx(ctx).Debug("successfully verified and added block", zap.String("filePath", r.filePath), zap.Int32("blockNumber", blockNumber),
			zap.Int64("startOffset", blockStartOffset), zap.Uint32("blockSize", totalBlockSize), zap.Int32("readBlockNumber", blockHeader.BlockNumber),
			zap.Int64("firstEntryID", blockHeader.FirstEntryID), zap.Int64("lastEntryID", blockHeader.LastEntryID), zap.Uint32("blockCrc", blockHeader.BlockCrc))

		// Move to the next block
		blockNumber++
		currentOffset = blockDataOffset + int64(blockDataLength)
	}

	// Update file size
	r.blockIndexes = tmpBlockIndexes

	logger.Ctx(ctx).Info("completed block info scan", zap.String("filePath", r.filePath),
		zap.Int("totalBlocks", len(r.blockIndexes)), zap.Int64("fileSize", currentFileSize), zap.Int64("scannedOffset", currentOffset))

	metrics.WpFileOperationsTotal.WithLabelValues(r.logIdStr, "loadAll", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(r.logIdStr, "loadAll", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// readDataBlocksUnsafe is a LEGACY-ONLY method that reads data blocks from the single data.log file.
// It is called by ReadNextBatchAdv when perBlockMode is false.
func (r *LocalFileReaderAdv) readDataBlocksUnsafe(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startBlockOffset int64) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

	// Soft limitation
	maxEntries := opt.MaxBatchEntries // soft count limit: Read the minimum number of blocks exceeding the count limit
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := r.maxBatchSize // soft bytes limit: Read the minimum number of blocks exceeding the bytes limit

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
			logger.Ctx(ctx).Warn("Failed to decode block", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID), zap.Error(decodeErr))
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
			logger.Ctx(ctx).Info("block header record not ready, stop reading", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID))
			break
		}
		blockDataLen := int(blockHeaderRecord.BlockLength)

		// Read the block data
		blockData, err := r.readAtUnsafe(ctx, startOffset+int64(headersLen), blockDataLen)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to read block data", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID), zap.Error(err))
			hasDataReadError = true
			break // Stop reading on error, return what we have so far
		}
		// Verify the block data integrity
		if err := r.verifyBlockDataIntegrity(ctx, blockHeaderRecord, currentBlockID, blockData); err != nil {
			logger.Ctx(ctx).Warn("verify block data integrity failed, stop reading", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID), zap.Error(err))
			break // Stop reading on data integrity error, return what we have so far
		}

		// decode block data to extract data records
		records, decodeErr := codec.DecodeRecordList(blockData)
		if decodeErr != nil {
			logger.Ctx(ctx).Warn("Failed to decode block", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID), zap.Error(decodeErr))
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
				dr := records[j].(*codec.DataRecord)
				entry := &proto.LogEntry{
					SegId:   r.segId,
					EntryId: currentEntryID,
					Values:  dr.Payload,
				}
				entries = append(entries, entry)
				entriesCollected++
				readBytes += int64(len(dr.Payload))
			}
			currentEntryID++
		}

		logger.Ctx(ctx).Debug("Extracted data from block", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID),
			zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber), zap.Int64("blockFirstEntryID", blockHeaderRecord.FirstEntryID),
			zap.Int64("blockLastEntryID", blockHeaderRecord.LastEntryID), zap.Int64("lastCollectedEntryID", currentEntryID),
			zap.Int64("totalCollectedEntries", entriesCollected), zap.Int64("totalCollectedBytes", readBytes))

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
		logger.Ctx(ctx).Debug("no entry extracted", zap.String("filePath", r.filePath),
			zap.Int64("startEntryId", opt.StartEntryID), zap.Int64("maxBatchEntries", opt.MaxBatchEntries), zap.Int("entriesReturned", len(entries)))
		if !hasDataReadError {
			// only read without dataReadError, determine whether it is an EOF
			if !r.isIncompleteFile.Load() || r.footer != nil {
				return nil, werr.ErrFileReaderEndOfFile
			}
			if r.isFooterExistsUnsafe(ctx) {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
		}
		// return entryNotFound to let read caller retry later
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("read data blocks completed", zap.String("filePath", r.filePath), zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("maxBatchEntries", opt.MaxBatchEntries), zap.Int("entriesReturned", len(entries)), zap.Any("lastBlockInfo", lastBlockInfo))
	}

	metrics.WpFileReadBatchBytes.WithLabelValues(r.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(r.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	// Create batch with proper error handling for nil lastBlockInfo
	var lastReadState *proto.LastReadState
	if lastBlockInfo != nil {
		lastReadState = &proto.LastReadState{
			SegmentId:   r.segId,
			Flags:       r.flags.Load(),
			Version:     r.version.Load(),
			LastBlockId: lastBlockInfo.BlockNumber,
			BlockOffset: lastBlockInfo.StartOffset,
			BlockSize:   lastBlockInfo.BlockSize,
		}
	}

	batch := &proto.BatchReadResult{
		Entries:       entries,
		LastReadState: lastReadState,
	}
	return batch, nil
}

// verifyBlockDataIntegrity is a LEGACY-ONLY method wrapper for block verification.
// It is called by readDataBlocksUnsafe to verify block CRC in legacy mode.
func (r *LocalFileReaderAdv) verifyBlockDataIntegrity(ctx context.Context, blockHeaderRecord *codec.BlockHeaderRecord, currentBlockID int64, dataRecordsBuffer []byte) error {
	// Verify block data integrity
	if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, dataRecordsBuffer); err != nil {
		logger.Ctx(ctx).Warn("block data integrity verification failed", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID),
			zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber), zap.Error(err))
		return err // Stop reading on error, return what we have so far
	}

	logger.Ctx(ctx).Debug("block data integrity verified successfully", zap.String("filePath", r.filePath), zap.Int64("blockNumber", currentBlockID),
		zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber), zap.Uint32("blockLength", blockHeaderRecord.BlockLength), zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
	return nil
}
