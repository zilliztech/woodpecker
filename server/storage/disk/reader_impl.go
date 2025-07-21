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

var (
	SegmentReaderScope = "LocalReader"
)

var _ storage.Reader = (*LocalFileReader)(nil)

// LocalFileReader implements AbstractFileReader for local filesystem storage
type LocalFileReader struct {
	logId        int64
	segId        int64
	logIdStr     string // for metrics only
	filePath     string
	file         *os.File
	size         int64
	footer       *codec.FooterRecord
	blockIndexes []*codec.IndexRecord
	closed       atomic.Bool

	// Fields for dynamic scanning of incomplete files
	lastScannedOffset int64 // Last offset we scanned to (for incomplete files)
	isIncompleteFile  bool  // Whether this file is incomplete (no footer)
}

// NewLocalFileReader creates a new local filesystem reader
func NewLocalFileReader(ctx context.Context, baseDir string, logId int64, segId int64) (*LocalFileReader, error) {
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

	reader := &LocalFileReader{
		logId:    logId,
		segId:    segId,
		logIdStr: fmt.Sprintf("%d", logId),
		filePath: filePath,
		file:     file,
		size:     stat.Size(),
	}
	reader.closed.Store(false)

	// Try to parse footer and indexes
	if err := reader.tryParseFooterAndIndexesUnsafe(ctx); err != nil {
		file.Close()
		reader.closed.Store(true)
		logger.Ctx(ctx).Warn("failed to parse footer and indexes",
			zap.String("filePath", filePath),
			zap.Error(err))
		return nil, fmt.Errorf("try parse footer and indexes: %w", err)
	}

	logger.Ctx(ctx).Debug("local file reader created successfully",
		zap.String("filePath", filePath),
		zap.Int64("fileSize", reader.size),
		zap.Bool("isIncompleteFile", reader.isIncompleteFile),
		zap.Int("blockIndexesCount", len(reader.blockIndexes)))

	return reader, nil
}

// tryParseFooterAndIndexes attempts to parse footer and index records
// If footer exists (finalized file), parse using footer for fast access
// If no footer exists (file still being written), perform full scan to build block indexes
func (r *LocalFileReader) tryParseFooterAndIndexesUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseFooterAndIndexes")
	defer sp.End()
	// Check if file has minimum size for a footer
	if r.size < codec.RecordHeaderSize+codec.FooterRecordSize {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, performing full scan",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", r.size))
		r.isIncompleteFile = true
		return r.scanFileForBlocksUnsafe(ctx)
	}

	// Try to read and parse footer from the end of the file
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	footerData, err := r.readAt(ctx, r.size-footerRecordSize, int(footerRecordSize))
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, performing full scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile = true
		return r.scanFileForBlocksUnsafe(ctx)
	}

	// Try to decode footer record
	footerRecord, err := codec.DecodeRecord(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to decode footer record, performing full scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile = true
		return r.scanFileForBlocksUnsafe(ctx)
	}

	// Check if it's actually a footer record
	if footerRecord.Type() != codec.FooterRecordType {
		logger.Ctx(ctx).Debug("no valid footer found, performing full scan",
			zap.String("filePath", r.filePath),
			zap.Uint8("recordType", footerRecord.Type()),
			zap.Uint8("expectedType", codec.FooterRecordType))
		r.isIncompleteFile = true
		return r.scanFileForBlocksUnsafe(ctx)
	}

	// Successfully found footer, parse using footer method
	r.footer = footerRecord.(*codec.FooterRecord)
	r.isIncompleteFile = false
	logger.Ctx(ctx).Debug("found footer, parsing index records",
		zap.String("filePath", r.filePath),
		zap.Int32("totalBlocks", r.footer.TotalBlocks))

	// Parse index records - they are located before the footer
	if r.footer.TotalBlocks > 0 {
		if err := r.parseIndexRecords(ctx); err != nil {
			return fmt.Errorf("parse index records: %w", err)
		}
	}

	return nil
}

// parseIndexRecords parses all index records from the file
func (r *LocalFileReader) parseIndexRecords(ctx context.Context) error {
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

// scanFileForBlocks performs a full scan of the file to build block indexes
// This is used when the file doesn't have a footer (still being written)
func (r *LocalFileReader) scanFileForBlocksUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "scanFileForBlocks")
	defer sp.End()
	startTime := time.Now()
	logger.Ctx(ctx).Info("starting full file scan to build block indexes",
		zap.String("filePath", r.filePath),
		zap.Int64("fileSize", r.size))

	if r.size == 0 {
		// Empty file
		r.blockIndexes = make([]*codec.IndexRecord, 0)
		return nil
	}

	// Read the entire file content
	fileData, err := r.readAt(ctx, 0, int(r.size))
	if err != nil {
		return fmt.Errorf("read entire file for scan: %w", err)
	}

	// Try to decode all records from the file
	records, err := codec.DecodeRecordList(fileData)
	if err != nil {
		// If decoding fails, the file might be corrupted or incomplete
		// Try to recover by scanning for valid record boundaries
		logger.Ctx(ctx).Warn("failed to decode all records, attempting partial recovery",
			zap.String("filePath", r.filePath),
			zap.Int("partialRecords", len(records)),
			zap.Error(err))
	}

	logger.Ctx(ctx).Debug("decoded records from incomplete file",
		zap.String("filePath", r.filePath),
		zap.Int("totalRecords", len(records)))

	// Process records to build block indexes
	// File structure: HeaderRecord -> BlockHeaderRecord -> DataRecord1 -> DataRecord2 -> BlockHeaderRecord -> DataRecord3 -> ...
	r.blockIndexes = make([]*codec.IndexRecord, 0)

	var currentBlockNumber int32 = 0
	var currentBlockStart int64 = 0
	var currentBlockFirstEntryID int64 = -1
	var currentBlockLastEntryID int64 = -1
	var currentEntryID int64 = 0
	var currentOffset int64 = 0
	var inBlock bool = false
	var fileHasHeader bool = false

	for _, record := range records {
		// Calculate record size for offset tracking
		var recordSize int64
		switch record.Type() {
		case codec.HeaderRecordType:
			recordSize = codec.RecordHeaderSize + 16 // Version(2) + Flags(2) + FirstEntryID(8) + Magic(4)
			fileHasHeader = true

			logger.Ctx(ctx).Debug("found file header",
				zap.String("filePath", r.filePath),
				zap.Int64("headerOffset", currentOffset))

		case codec.DataRecordType:
			dataRecord := record.(*codec.DataRecord)
			recordSize = int64(codec.RecordHeaderSize + len(dataRecord.Payload))

			// If we're not in a block yet, this DataRecord starts a new block
			// (This handles old format files or files without BlockHeaderRecord)
			if !inBlock {
				currentBlockStart = currentOffset
				currentBlockFirstEntryID = currentEntryID
				inBlock = true

				logger.Ctx(ctx).Debug("found block start from DataRecord (legacy format)",
					zap.String("filePath", r.filePath),
					zap.Int32("blockNumber", currentBlockNumber),
					zap.Int64("startOffset", currentBlockStart),
					zap.Int64("firstEntryID", currentEntryID))
			}

			// Track the last entry ID in current block
			currentBlockLastEntryID = currentEntryID
			currentEntryID++

		case codec.BlockHeaderRecordType:
			recordSize = codec.RecordHeaderSize + codec.BlockHeaderRecordSize
			blockHeaderRecord := record.(*codec.BlockHeaderRecord)

			// BlockHeaderRecord is now at the beginning of a block
			// If we were already in a block, finalize the previous block first
			if inBlock {
				// Finalize the previous block
				indexRecord := &codec.IndexRecord{
					BlockNumber:       currentBlockNumber,
					StartOffset:       currentBlockStart,
					FirstRecordOffset: 0,
					BlockSize:         uint32(int64(currentOffset) - currentBlockStart), // Calculate block size
					FirstEntryID:      currentBlockFirstEntryID,
					LastEntryID:       currentBlockLastEntryID,
				}
				r.blockIndexes = append(r.blockIndexes, indexRecord)

				logger.Ctx(ctx).Debug("finalized previous block during scan",
					zap.String("filePath", r.filePath),
					zap.Int32("blockNumber", currentBlockNumber),
					zap.Int64("startOffset", currentBlockStart),
					zap.Int64("firstEntryID", currentBlockFirstEntryID),
					zap.Int64("lastEntryID", currentBlockLastEntryID))

				currentBlockNumber++
			}

			// Start new block
			currentBlockStart = currentOffset
			currentBlockFirstEntryID = blockHeaderRecord.FirstEntryID
			currentBlockLastEntryID = blockHeaderRecord.LastEntryID
			inBlock = true

			logger.Ctx(ctx).Debug("found new block header during scan",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", currentBlockNumber),
				zap.Int64("startOffset", currentBlockStart),
				zap.Int64("firstEntryID", currentBlockFirstEntryID),
				zap.Int64("lastEntryID", currentBlockLastEntryID))

		case codec.IndexRecordType:
			recordSize = codec.RecordHeaderSize + codec.IndexRecordSize // IndexRecord payload size
		case codec.FooterRecordType:
			recordSize = codec.RecordHeaderSize + codec.FooterRecordSize
		default:
			// Unknown record type, skip
			recordSize = codec.RecordHeaderSize
		}

		currentOffset += recordSize
	}

	// Handle case where file ends without completing the current block (incomplete block still being written)
	if inBlock && currentBlockFirstEntryID != -1 {
		indexRecord := &codec.IndexRecord{
			BlockNumber:       currentBlockNumber,
			StartOffset:       currentBlockStart,
			FirstRecordOffset: 0,
			BlockSize:         uint32(int64(currentOffset) - currentBlockStart), // Calculate block size
			FirstEntryID:      currentBlockFirstEntryID,
			LastEntryID:       currentBlockLastEntryID,
		}
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		logger.Ctx(ctx).Debug("found incomplete block at end of file during scan",
			zap.String("filePath", r.filePath),
			zap.Int32("blockNumber", currentBlockNumber),
			zap.Int64("startOffset", currentBlockStart),
			zap.Int64("endOffset", currentOffset),
			zap.Int64("firstEntryID", currentBlockFirstEntryID),
			zap.Int64("lastEntryID", currentBlockLastEntryID))
	}

	// If no blocks were found but we have a header, create a single block for the entire file
	if len(r.blockIndexes) == 0 && fileHasHeader {
		// Calculate the start offset after the header
		headerSize := int64(codec.RecordHeaderSize + 16)
		if currentEntryID > 0 {
			// We have some data records after the header
			// StartOffset should be 0 to include the HeaderRecord in the block
			// This ensures that when reading the block, we process all records including the header
			indexRecord := &codec.IndexRecord{
				BlockNumber:       0,
				StartOffset:       0,              // Start from beginning to include HeaderRecord
				FirstRecordOffset: headerSize,     // First data record offset after header
				BlockSize:         uint32(r.size), // Total file size as block size
				FirstEntryID:      0,
				LastEntryID:       currentEntryID - 1,
			}
			r.blockIndexes = append(r.blockIndexes, indexRecord)

			logger.Ctx(ctx).Debug("created single block for headerless data",
				zap.String("filePath", r.filePath),
				zap.Int64("startOffset", int64(0)),
				zap.Int64("firstRecordOffset", headerSize),
				zap.Int64("firstEntryID", int64(0)),
				zap.Int64("lastEntryID", currentEntryID-1))
		}
	}

	// Sort blocks by block number to ensure correct order
	sort.Slice(r.blockIndexes, func(i, j int) bool {
		return r.blockIndexes[i].BlockNumber < r.blockIndexes[j].BlockNumber
	})

	// Set the last scanned offset to current file size for incomplete files
	r.lastScannedOffset = r.size

	logger.Ctx(ctx).Info("completed full file scan",
		zap.String("filePath", r.filePath),
		zap.Int("blocksFound", len(r.blockIndexes)),
		zap.Int64("lastScannedOffset", r.lastScannedOffset))

	metrics.WpFileOperationsTotal.WithLabelValues(r.logIdStr, "loadAll", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(r.logIdStr, "loadAll", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// scanForNewBlocks scans for new blocks that may have been written since last scan
// This is used for incomplete files to detect newly written data
func (r *LocalFileReader) scanForNewBlocks(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "scanForNewBlocks")
	defer sp.End()
	startTime := time.Now()
	// Get current file size
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file for new blocks scan: %w", err)
	}

	currentSize := stat.Size()
	if currentSize <= r.lastScannedOffset {
		// No new data since last scan
		logger.Ctx(ctx).Debug("no new data since last scan",
			zap.String("filePath", r.filePath),
			zap.Int64("currentSize", currentSize),
			zap.Int64("lastScannedOffset", r.lastScannedOffset))
		return nil
	}

	logger.Ctx(ctx).Debug("scanning for new blocks",
		zap.String("filePath", r.filePath),
		zap.Int64("lastScannedOffset", r.lastScannedOffset),
		zap.Int64("currentSize", currentSize),
		zap.Int64("newDataSize", currentSize-r.lastScannedOffset))

	// Read only the new data since last scan
	newDataSize := currentSize - r.lastScannedOffset
	newData, err := r.readAt(ctx, r.lastScannedOffset, int(newDataSize))
	if err != nil {
		return fmt.Errorf("read new data for scan: %w", err)
	}

	// Try to decode records from the new data
	records, err := codec.DecodeRecordList(newData)
	if err != nil {
		// If decoding fails, some records might be incomplete
		// This is expected for files still being written
		logger.Ctx(ctx).Debug("failed to decode some new records (expected for incomplete files)",
			zap.String("filePath", r.filePath),
			zap.Error(err))

		// Update the file size and last scanned offset anyway
		r.size = currentSize
		r.lastScannedOffset = currentSize
		return nil
	}

	logger.Ctx(ctx).Debug("decoded new records",
		zap.String("filePath", r.filePath),
		zap.Int("newRecordCount", len(records)))

	// Process new records to find new blocks
	var currentBlockNumber int32
	if len(r.blockIndexes) > 0 {
		currentBlockNumber = r.blockIndexes[len(r.blockIndexes)-1].BlockNumber + 1
	}

	var currentBlockStart int64 = r.lastScannedOffset
	var currentBlockFirstEntryID int64 = -1
	var currentBlockLastEntryID int64 = -1
	var currentEntryID int64 = 0
	var currentOffset int64 = r.lastScannedOffset
	var inBlock bool = false

	// Calculate the starting entry ID based on existing blocks
	if len(r.blockIndexes) > 0 {
		lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
		currentEntryID = lastBlock.LastEntryID + 1
	}

	for _, record := range records {
		// Calculate record size for offset tracking
		var recordSize int64
		switch record.Type() {
		case codec.HeaderRecordType:
			recordSize = codec.RecordHeaderSize + 16
			// HeaderRecord should not appear in the middle of file
			logger.Ctx(ctx).Warn("unexpected HeaderRecord in new data scan")

		case codec.DataRecordType:
			dataRecord := record.(*codec.DataRecord)
			recordSize = int64(codec.RecordHeaderSize + len(dataRecord.Payload))

			// If we're not in a block yet, this DataRecord starts a new block
			// (This handles old format files or files without BlockHeaderRecord)
			if !inBlock {
				currentBlockStart = currentOffset
				currentBlockFirstEntryID = currentEntryID
				inBlock = true

				logger.Ctx(ctx).Debug("found new block start from DataRecord",
					zap.String("filePath", r.filePath),
					zap.Int32("blockNumber", currentBlockNumber),
					zap.Int64("startOffset", currentBlockStart),
					zap.Int64("firstEntryID", currentEntryID))
			}

			// Track the last entry ID in current block
			currentBlockLastEntryID = currentEntryID
			currentEntryID++

		case codec.BlockHeaderRecordType:
			recordSize = codec.RecordHeaderSize + codec.BlockHeaderRecordSize
			blockHeaderRecord := record.(*codec.BlockHeaderRecord)

			// BlockHeaderRecord is now at the beginning of a block
			// If we were already in a block, finalize the previous block first
			if inBlock {
				// Finalize the previous block
				indexRecord := &codec.IndexRecord{
					BlockNumber:       currentBlockNumber,
					StartOffset:       currentBlockStart,
					FirstRecordOffset: 0,
					BlockSize:         uint32(currentOffset - currentBlockStart), // Calculate block size
					FirstEntryID:      currentBlockFirstEntryID,
					LastEntryID:       currentBlockLastEntryID,
				}
				r.blockIndexes = append(r.blockIndexes, indexRecord)

				logger.Ctx(ctx).Debug("finalized previous block in new data scan",
					zap.String("filePath", r.filePath),
					zap.Int32("blockNumber", currentBlockNumber),
					zap.Int64("startOffset", currentBlockStart),
					zap.Int64("firstEntryID", currentBlockFirstEntryID),
					zap.Int64("lastEntryID", currentBlockLastEntryID))

				currentBlockNumber++
			}

			// Start new block
			currentBlockStart = currentOffset
			currentBlockFirstEntryID = blockHeaderRecord.FirstEntryID
			currentBlockLastEntryID = blockHeaderRecord.LastEntryID
			inBlock = true

			logger.Ctx(ctx).Debug("found new block header in new data scan",
				zap.String("filePath", r.filePath),
				zap.Int32("blockNumber", currentBlockNumber),
				zap.Int64("startOffset", currentBlockStart),
				zap.Int64("firstEntryID", currentBlockFirstEntryID),
				zap.Int64("lastEntryID", currentBlockLastEntryID))

		case codec.IndexRecordType:
			recordSize = codec.RecordHeaderSize + codec.IndexRecordSize // IndexRecord payload size
		case codec.FooterRecordType:
			recordSize = codec.RecordHeaderSize + codec.FooterRecordSize
		default:
			recordSize = codec.RecordHeaderSize
		}

		currentOffset += recordSize
	}

	// Handle case where new data ends with incomplete block
	if inBlock && currentBlockFirstEntryID != -1 {
		indexRecord := &codec.IndexRecord{
			BlockNumber:       currentBlockNumber,
			StartOffset:       currentBlockStart,
			FirstRecordOffset: 0,
			BlockSize:         uint32(int64(currentOffset) - currentBlockStart), // Calculate block size
			FirstEntryID:      currentBlockFirstEntryID,
			LastEntryID:       currentBlockLastEntryID,
		}
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		logger.Ctx(ctx).Debug("found new incomplete block at end",
			zap.String("filePath", r.filePath),
			zap.Int32("blockNumber", currentBlockNumber),
			zap.Int64("startOffset", currentBlockStart),
			zap.Int64("endOffset", currentOffset),
			zap.Int64("firstEntryID", currentBlockFirstEntryID),
			zap.Int64("lastEntryID", currentBlockLastEntryID))
	}

	// Update file size and last scanned offset
	r.size = currentSize
	r.lastScannedOffset = currentSize

	logger.Ctx(ctx).Debug("completed new blocks scan",
		zap.String("filePath", r.filePath),
		zap.Int("totalBlocks", len(r.blockIndexes)),
		zap.Int64("newSize", r.size),
		zap.Int64("lastScannedOffset", r.lastScannedOffset))
	metrics.WpFileOperationsTotal.WithLabelValues(r.logIdStr, "loadIncr", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(r.logIdStr, "loadIncr", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// readAt reads data from the file at the specified offset
func (r *LocalFileReader) readAt(ctx context.Context, offset int64, length int) ([]byte, error) {
	logger.Ctx(ctx).Debug("reading data from file",
		zap.Int64("offset", offset),
		zap.Int("length", length),
		zap.Int64("fileSize", r.size),
		zap.String("filePath", r.filePath))

	// Seek to offset
	if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", offset, err)
	}

	// Read data
	data := make([]byte, length)
	n, err := io.ReadFull(r.file, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		logger.Ctx(ctx).Warn("failed to read data from file",
			zap.Int64("offset", offset),
			zap.Int("requestedLength", length),
			zap.Int("actualRead", n),
			zap.Error(err))
		return nil, fmt.Errorf("read data: %w", err)
	}

	logger.Ctx(ctx).Debug("data read successfully",
		zap.Int64("offset", offset),
		zap.Int("requestedLength", length),
		zap.Int("actualRead", n))

	return data[:n], nil
}

// GetLastEntryID returns the last entry ID in the file
func (r *LocalFileReader) GetLastEntryID(ctx context.Context) (int64, error) {
	if r.closed.Load() {
		return -1, werr.ErrFileReaderAlreadyClosed
	}

	if len(r.blockIndexes) == 0 {
		return -1, werr.ErrEntryNotFound
	}

	// Return the last entry ID from the last block
	lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
	return lastBlock.LastEntryID, nil
}

// ReadNextBatch reads the next batch of entries
func (r *LocalFileReader) ReadNextBatch(ctx context.Context, opt storage.ReaderOpt) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatch")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatch called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("batchSize", opt.BatchSize),
		zap.Int("blockCount", len(r.blockIndexes)),
		zap.Bool("isIncompleteFile", r.isIncompleteFile))

	if r.closed.Load() {
		return nil, werr.ErrFileReaderAlreadyClosed
	}

	// For incomplete files, try to scan for new blocks if we don't have enough data
	if r.isIncompleteFile {
		if err := r.ensureSufficientBlocks(ctx, opt.StartEntryID, opt.BatchSize); err != nil {
			logger.Ctx(ctx).Warn("failed to scan for new blocks", zap.Error(err))
			// Continue with existing blocks even if scan fails
		}
	}

	if len(r.blockIndexes) == 0 {
		// if no blocks and the segment is compacted or fenced, return an EOF
		if !r.isIncompleteFile {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no data blocks")
		}
		// otherwise return entry Not found, since there is no data, reader should retry later
		return nil, werr.ErrEntryNotFound
	}

	// Find the starting block that contains the start sequence number
	startBlockIndex := -1
	for i, block := range r.blockIndexes {
		logger.Ctx(ctx).Debug("checking block for start sequence",
			zap.Int("blockIndex", i),
			zap.Int32("blockNumber", block.BlockNumber),
			zap.Int64("blockFirstEntryID", block.FirstEntryID),
			zap.Int64("blockLastEntryID", block.LastEntryID),
			zap.Int64("readStartEntryID", opt.StartEntryID))

		if block.FirstEntryID <= opt.StartEntryID && opt.StartEntryID <= block.LastEntryID {
			startBlockIndex = i
			logger.Ctx(ctx).Debug("found matching block",
				zap.Int64("logId", r.logId),
				zap.Int64("segId", r.segId),
				zap.Int("startBlockIndex", startBlockIndex),
				zap.Int32("blockNumber", block.BlockNumber),
				zap.Int64("blockFirstEntryID", block.FirstEntryID),
				zap.Int64("blockLastEntryID", block.LastEntryID),
				zap.Any("readStartEntryID", opt.StartEntryID))
			break
		}
	}

	if startBlockIndex == -1 {
		logger.Ctx(ctx).Warn("no block found for start sequence number",
			zap.Int64("startEntryID", opt.StartEntryID),
			zap.Int("totalBlocks", len(r.blockIndexes)))
		// if no blocks and the segment is compacted or fenced, return an EOF
		if !r.isIncompleteFile {
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg(fmt.Sprintf("no block contains %d", opt.StartEntryID))
		}
		// otherwise return entry Not found, since there is no data, reader should retry later
		return nil, werr.ErrEntryNotFound
	}

	// if completed file or auto batch size=-1, default read 100 entries as a batch
	if r.isIncompleteFile && opt.BatchSize == -1 {
		// Auto batch mode: return all data records from the single block containing the start sequence number
		logger.Ctx(ctx).Debug("using single block mode")
		return r.readSingleBlock(ctx, r.blockIndexes[startBlockIndex], opt.StartEntryID)
	} else {
		// Specified batch size mode: read across multiple blocks if necessary to get the requested number of entries
		if opt.BatchSize == -1 {
			opt.BatchSize = 100
		}
		logger.Ctx(ctx).Debug("using multiple blocks mode",
			zap.Int("startBlockIndex", startBlockIndex),
			zap.Int64("batchSize", opt.BatchSize))
		return r.readMultipleBlocks(ctx, r.blockIndexes, startBlockIndex, opt.StartEntryID, opt.BatchSize)
	}
}

// ensureSufficientBlocks ensures we have scanned enough blocks to satisfy the read request
// This method checks if we need to scan for new blocks in incomplete files
func (r *LocalFileReader) ensureSufficientBlocks(ctx context.Context, startEntryID int64, batchSize int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ensureSufficientBlocks")
	defer sp.End()

	// Check if we already have the starting block
	hasStartingBlock := false
	var lastAvailableEntryID int64 = -1

	for _, block := range r.blockIndexes {
		if block.FirstEntryID <= startEntryID && startEntryID <= block.LastEntryID {
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
		requiredLastEntryID := startEntryID + batchSize - 1
		if lastAvailableEntryID < requiredLastEntryID {
			needToScan = true
			logger.Ctx(ctx).Debug("need more blocks for batch size",
				zap.Int64("startEntryID", startEntryID),
				zap.Int64("batchSize", batchSize),
				zap.Int64("requiredLastEntryID", requiredLastEntryID),
				zap.Int64("lastAvailableEntryID", lastAvailableEntryID))
		}
	}

	if needToScan {
		logger.Ctx(ctx).Debug("scanning for new blocks to satisfy read request",
			zap.Int64("startEntryID", startEntryID),
			zap.Int64("batchSize", batchSize),
			zap.Bool("hasStartingBlock", hasStartingBlock),
			zap.Int64("lastAvailableEntryID", lastAvailableEntryID))

		return r.scanForNewBlocks(ctx)
	}

	logger.Ctx(ctx).Debug("sufficient blocks available, no scan needed",
		zap.Int64("startEntryID", startEntryID),
		zap.Int64("batchSize", batchSize),
		zap.Int64("lastAvailableEntryID", lastAvailableEntryID))

	return nil
}

// readSingleBlock reads all data records from a single block starting from the specified entry ID
func (r *LocalFileReader) readSingleBlock(ctx context.Context, blockInfo *codec.IndexRecord, startEntryID int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readSingleBlock")
	defer sp.End()
	startTime := time.Now()
	// Get blockSize
	blockSize := int64(blockInfo.BlockSize)

	logger.Ctx(ctx).Debug("reading single block",
		zap.Int64("logId", r.logId),
		zap.Int64("segId", r.segId),
		zap.Int32("blockNumber", blockInfo.BlockNumber),
		zap.Int64("startOffset", blockInfo.StartOffset),
		zap.Int64("blockFirstEntryID", blockInfo.FirstEntryID),
		zap.Int64("blockLastEntryID", blockInfo.LastEntryID),
		zap.Int64("blockSize", blockSize),
		zap.Int64("startEntryID", startEntryID))

	if blockSize <= 0 {
		logger.Ctx(ctx).Debug("block size is zero or negative, returning empty entries",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int64("blockSize", blockSize))
		return []*proto.LogEntry{}, nil
	}

	// Read the block data
	blockData, err := r.readAt(ctx, blockInfo.StartOffset, int(blockSize))
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read block data",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int32("blockNumber", blockInfo.BlockNumber),
			zap.Int64("startOffset", blockInfo.StartOffset),
			zap.Int64("blockSize", blockSize),
			zap.Error(err))
		return nil, fmt.Errorf("read block data: %w", err)
	}

	// Decode all records from the block
	records, err := codec.DecodeRecordList(blockData)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to decode block records",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int32("blockNumber", blockInfo.BlockNumber),
			zap.Int("blockDataSize", len(blockData)),
			zap.Error(err))
		return nil, fmt.Errorf("decode block records: %w", err)
	}

	logger.Ctx(ctx).Debug("decoded block records",
		zap.Int64("logId", r.logId),
		zap.Int64("segId", r.segId),
		zap.Int32("blockNumber", blockInfo.BlockNumber),
		zap.Int("recordCount", len(records)))

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
		// Extract the data records part from blockData (skip BlockHeaderRecord)
		// BlockHeaderRecord is at the beginning, so data starts after it
		blockHeaderSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
		if len(blockData) > blockHeaderSize {
			dataRecordsBuffer := blockData[blockHeaderSize:]
			// Verify block data integrity
			if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, dataRecordsBuffer); err != nil {
				logger.Ctx(ctx).Warn("block data integrity verification failed",
					zap.Int64("logId", r.logId),
					zap.Int64("segId", r.segId),
					zap.Int32("blockNumber", blockInfo.BlockNumber),
					zap.Error(err))
				// only
				return nil, werr.ErrEntryNotFound.WithCauseErrMsg("reading block is incomplete")
			} else {
				logger.Ctx(ctx).Debug("block data integrity verified successfully",
					zap.Int64("logId", r.logId),
					zap.Int64("segId", r.segId),
					zap.Int32("blockNumber", blockInfo.BlockNumber),
					zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
					zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
			}
		} else {
			// block is not complated, stop reading next records
			logger.Ctx(ctx).Warn("block data integrity verification failed, it is incomplete",
				zap.Int64("logId", r.logId),
				zap.Int64("segId", r.segId),
				zap.Int32("blockNumber", blockInfo.BlockNumber))
			return nil, werr.ErrEntryNotFound.WithCauseErrMsg("reading block is incomplete")
		}
	}

	// Extract data records and convert to LogEntry
	entries := make([]*proto.LogEntry, 0)
	currentEntryID := blockInfo.FirstEntryID

	// Check if this block starts with a HeaderRecord
	// If StartOffset is 0 and FirstRecordOffset > 0, then this block includes a HeaderRecord
	skipHeaderRecord := blockInfo.StartOffset == 0 && blockInfo.FirstRecordOffset > 0
	readBytes := 0

	for _, record := range records {
		if record.Type() == codec.HeaderRecordType && skipHeaderRecord {
			// Skip the HeaderRecord when processing entries
			logger.Ctx(ctx).Debug("skipping HeaderRecord in block",
				zap.Int64("logId", r.logId),
				zap.Int64("segId", r.segId),
				zap.Int32("blockNumber", blockInfo.BlockNumber))
			continue
		}

		if record.Type() == codec.BlockHeaderRecordType && skipHeaderRecord {
			// Skip the BlockHeaderRecord when processing entries
			logger.Ctx(ctx).Debug("skipping BlockHeaderRecord in block",
				zap.Int64("logId", r.logId),
				zap.Int64("segId", r.segId),
				zap.Int32("blockNumber", blockInfo.BlockNumber))
			continue
		}

		if record.Type() == codec.DataRecordType {
			// Only include entries starting from the specified sequence number
			if currentEntryID >= startEntryID {
				dataRecord := record.(*codec.DataRecord)
				entry := &proto.LogEntry{
					EntryId: currentEntryID,
					Values:  dataRecord.Payload,
				}
				entries = append(entries, entry)
				readBytes += len(dataRecord.Payload)
			}
			currentEntryID++
		}
	}

	if len(entries) == 0 {
		// return entry not found yet, wp client retry later
		logger.Ctx(ctx).Debug("no record extracted from the ongoing block",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int32("blockNumber", blockInfo.BlockNumber),
			zap.Int64("blockFirstEntryID", blockInfo.FirstEntryID),
			zap.Int64("blockLastEntryID", blockInfo.LastEntryID),
			zap.Int64("startEntryID", startEntryID))
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record has been written to the ongoing block")
	} else {
		logger.Ctx(ctx).Debug("extracted entries from single block",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int32("blockNumber", blockInfo.BlockNumber),
			zap.Int64("blockFirstEntryID", blockInfo.FirstEntryID),
			zap.Int64("blockLastEntryID", blockInfo.LastEntryID),
			zap.Int64("startEntryID", startEntryID),
			zap.Int("readEntryCount", len(entries)))
	}
	metrics.WpFileReadBatchBytes.WithLabelValues(r.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(r.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	return entries, nil
}

// readMultipleBlocks reads across multiple blocks to get the specified number of entries
func (r *LocalFileReader) readMultipleBlocks(ctx context.Context, allBlocks []*codec.IndexRecord, startBlockIndex int, startEntryID int64, batchSize int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readMultipleBlocks")
	defer sp.End()
	startTime := time.Now()
	logger.Ctx(ctx).Debug("readMultipleBlocks started",
		zap.Int64("logId", r.logId),
		zap.Int64("segId", r.segId),
		zap.Int("startBlockIndex", startBlockIndex),
		zap.Int64("startEntryID", startEntryID),
		zap.Int64("batchSize", batchSize),
		zap.Int("totalBlocks", len(allBlocks)))

	entries := make([]*proto.LogEntry, 0, batchSize)
	entriesCollected := int64(0)
	readBytes := 0

	for i := startBlockIndex; i < len(allBlocks) && entriesCollected < batchSize; i++ {
		blkIdx := i
		blockInfo := allBlocks[blkIdx]

		logger.Ctx(ctx).Debug("processing block in readMultipleBlocks",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int("blockIndex", blkIdx),
			zap.Int32("blockNumber", blockInfo.BlockNumber),
			zap.Int64("blockStartOffset", blockInfo.StartOffset),
			zap.Uint32("blockSize", blockInfo.BlockSize),
			zap.Int64("blockFirstEntryID", blockInfo.FirstEntryID),
			zap.Int64("blockLastEntryID", blockInfo.LastEntryID))

		// Get blockSize
		blockSize := int64(blockInfo.BlockSize)

		logger.Ctx(ctx).Debug("reading block data",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int("blockIndex", blkIdx),
			zap.Int64("startOffset", blockInfo.StartOffset),
			zap.Int64("blockSize", blockSize))

		if blockSize <= 0 {
			logger.Ctx(ctx).Debug("skipping block with zero size",
				zap.Int64("logId", r.logId),
				zap.Int64("segId", r.segId),
				zap.Int("blockIndex", blkIdx),
				zap.Int64("blockSize", blockSize))
			continue
		}

		// Read the block data
		blockData, err := r.readAt(ctx, blockInfo.StartOffset, int(blockSize))
		if err != nil {
			return nil, fmt.Errorf("read block data: %w", err)
		}

		// Decode all records from the block
		records, err := codec.DecodeRecordList(blockData)
		if err != nil {
			return nil, fmt.Errorf("decode block records: %w", err)
		}

		logger.Ctx(ctx).Debug("decoded records from block",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int("blockIndex", blkIdx),
			zap.Int64("blockSize", blockSize),
			zap.Int("readBlockSize", len(blockData)),
			zap.Int("recordCount", len(records)))

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
			// Extract the data records part from blockData (skip BlockHeaderRecord)
			// BlockHeaderRecord is at the beginning, so data starts after it
			blockHeaderSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
			if len(blockData) > blockHeaderSize {
				dataRecordsBuffer := blockData[blockHeaderSize:]
				// Verify block data integrity
				if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, dataRecordsBuffer); err != nil {
					logger.Ctx(ctx).Warn("block data integrity verification failed",
						zap.Int64("logId", r.logId),
						zap.Int64("segId", r.segId),
						zap.Int("blockIndex", blkIdx),
						zap.Int32("blockNumber", blockInfo.BlockNumber),
						zap.Error(err))
					// block is incomplete, stop reading next records
					break
				} else {
					logger.Ctx(ctx).Debug("block data integrity verified successfully",
						zap.Int64("logId", r.logId),
						zap.Int64("segId", r.segId),
						zap.Int("blockIndex", blkIdx),
						zap.Int32("blockNumber", blockInfo.BlockNumber),
						zap.Uint32("blockLength", blockHeaderRecord.BlockLength),
						zap.Uint32("blockCrc", blockHeaderRecord.BlockCrc))
				}
			} else {
				// block is not completed, stop reading next records
				logger.Ctx(ctx).Warn("block data integrity verification failed, it is incomplete",
					zap.Int64("logId", r.logId),
					zap.Int64("segId", r.segId),
					zap.Int("blockIndex", blkIdx),
					zap.Int32("blockNumber", blockInfo.BlockNumber))
				break
			}
		}

		// Process records in this block
		currentEntryID := blockInfo.FirstEntryID

		// Check if this block starts with a HeaderRecord
		// If StartOffset is 0 and FirstRecordOffset > 0, then this block includes a HeaderRecord
		skipHeaderRecord := blockInfo.StartOffset == 0 && blockInfo.FirstRecordOffset > 0

		for recordIndex, record := range records {
			logger.Ctx(ctx).Debug("processing record",
				zap.Int64("logId", r.logId),
				zap.Int64("segId", r.segId),
				zap.Int("blockIndex", blkIdx),
				zap.Int("recordIndex", recordIndex),
				zap.Uint8("recordType", record.Type()),
				zap.Int64("currentEntryID", currentEntryID),
				zap.Int64("startEntryID", startEntryID),
				zap.Int64("entriesCollected", entriesCollected),
				zap.Int64("batchSize", batchSize))

			if record.Type() == codec.HeaderRecordType && skipHeaderRecord {
				// Skip the HeaderRecord when processing entries
				logger.Ctx(ctx).Debug("skipping HeaderRecord in block",
					zap.Int64("logId", r.logId),
					zap.Int64("segId", r.segId),
					zap.Int("blockIndex", blkIdx),
					zap.Int32("blockNumber", blockInfo.BlockNumber))
				continue
			}

			if record.Type() == codec.DataRecordType {
				// Check if this entry should be included
				if currentEntryID >= startEntryID && entriesCollected < batchSize {
					dataRecord := record.(*codec.DataRecord)
					entry := &proto.LogEntry{
						EntryId: currentEntryID,
						Values:  dataRecord.Payload,
					}
					entries = append(entries, entry)
					entriesCollected++
					readBytes += len(dataRecord.Payload)

					logger.Ctx(ctx).Debug("added entry to result",
						zap.Int64("logId", r.logId),
						zap.Int64("segId", r.segId),
						zap.Int64("entryId", currentEntryID),
						zap.Int("blockIndex", blkIdx),
						zap.Int("payloadSize", len(dataRecord.Payload)),
						zap.Int64("entriesCollected", entriesCollected))
				} else {
					logger.Ctx(ctx).Debug("not add entry to result",
						zap.Int64("logId", r.logId),
						zap.Int64("segId", r.segId),
						zap.Int("blockIndex", blkIdx),
						zap.Int64("entryId", currentEntryID),
						zap.Int64("entriesCollected", entriesCollected),
						zap.Int64("entriesCollected", entriesCollected))
				}
				currentEntryID++
			} else {
				logger.Ctx(ctx).Debug("skipping non-data record or batch size reached",
					zap.Int64("logId", r.logId),
					zap.Int64("segId", r.segId),
					zap.Int("blockIndex", blkIdx),
					zap.Uint8("recordType", record.Type()),
					zap.Int64("entriesCollected", entriesCollected),
					zap.Int64("batchSize", batchSize))
			}

			// Stop processing if we've collected enough entries
			if entriesCollected >= batchSize {
				break
			}
		}

		logger.Ctx(ctx).Debug("finished processing block",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int("blockIndex", blkIdx),
			zap.Int64("entriesCollected", entriesCollected))

		// Stop processing blocks if we've collected enough entries
		if entriesCollected >= batchSize {
			break
		}
	}

	if entriesCollected == 0 {
		// return entry not found yet, wp client retry later
		logger.Ctx(ctx).Debug("no record extracted from multi blocks",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int64("startEntryID", startEntryID),
			zap.Int64("totalEntriesCollected", entriesCollected),
			zap.Int64("entriesCollected", entriesCollected))
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("extracted entries from multi blocks",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int64("startEntryID", startEntryID),
			zap.Int64("totalEntriesCollected", entriesCollected),
			zap.Int64("entriesCollected", entriesCollected))
	}
	metrics.WpFileReadBatchBytes.WithLabelValues(r.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(r.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	return entries, nil
}

// GetBlockIndexes returns all block indexes
func (r *LocalFileReader) GetBlockIndexes() []*codec.IndexRecord {
	return r.blockIndexes
}

// GetFooter returns the footer record
func (r *LocalFileReader) GetFooter() *codec.FooterRecord {
	return r.footer
}

// GetTotalRecords returns the total number of records
func (r *LocalFileReader) GetTotalRecords() uint32 {
	if r.footer != nil {
		return r.footer.TotalRecords
	}
	return 0
}

// GetTotalBlocks returns the total number of blocks
func (r *LocalFileReader) GetTotalBlocks() int32 {
	if r.footer != nil {
		return r.footer.TotalBlocks
	}
	return 0
}

// Close closes the reader
func (r *LocalFileReader) Close(ctx context.Context) error {
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
