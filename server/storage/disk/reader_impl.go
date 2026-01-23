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
	"os"
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
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

var (
	SegmentReaderScope = "LocalFileReader"
)

var _ storage.Reader = (*LocalFileReaderAdv)(nil)

// LocalFileReaderAdv implements AbstractFileReader for local filesystem storage
type LocalFileReaderAdv struct {
	// Core fields (used by both per-block and legacy modes)
	logId        int64
	segId        int64
	logIdStr     string // for metrics only
	baseDir      string // base directory for per-block files
	maxBatchSize int64
	mu           sync.RWMutex

	// Legacy mode flag (backward compatibility)
	legacyMode atomic.Bool // true if using legacy data.log single-file format

	// Compaction state
	isCompacted bool // true if segment uses merged blocks (m_N.blk)

	// Runtime file state (shared)
	flags            atomic.Uint32 // stores uint16 value
	version          atomic.Uint32 // stores uint16 value
	footer           *codec.FooterRecord
	blockIndexes     []*codec.IndexRecord
	isIncompleteFile atomic.Bool // Whether this file is incomplete (no footer)
	closed           atomic.Bool

	// ==================== Legacy Mode Fields (backward compatibility) ====================
	// These fields are only used when reading from old data.log single-file format.
	// When legacyMode is false (per-block mode), these fields are nil/unused.
	filePath string   // legacy single-file path (used only in legacy mode)
	file     *os.File // legacy single-file handle (nil in per-block mode)
}

// NewLocalFileReaderAdv creates a new local filesystem reader
func NewLocalFileReaderAdv(ctx context.Context, baseDir string, logId int64, segId int64, maxBatchSize int64) (*LocalFileReaderAdv, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewLocalFileReader")
	defer sp.End()

	logger.Ctx(ctx).Debug("creating new local file reader", zap.String("baseDir", baseDir),
		zap.Int64("logId", logId), zap.Int64("segId", segId), zap.Int64("maxBatchSize", maxBatchSize))

	segmentDir := getSegmentDir(baseDir, logId, segId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	reader := &LocalFileReaderAdv{
		logId:        logId,
		segId:        segId,
		logIdStr:     fmt.Sprintf("%d", logId),
		baseDir:      baseDir,
		filePath:     getSegmentFilePath(baseDir, logId, segId),
		maxBatchSize: maxBatchSize,
	}
	reader.flags.Store(0)
	reader.version.Store(codec.FormatVersion)
	reader.closed.Store(false)
	reader.isIncompleteFile.Store(true)

	// Priority 1: Try to load footer.blk first
	// If footer exists, it contains all block indexes and segment metadata (works for both empty and non-empty completed segments)
	if err := reader.tryLoadLocalFooter(ctx); err == nil {
		reader.legacyMode.Store(false)
		logger.Ctx(ctx).Debug("local file readerAdv created successfully (footer loaded)",
			zap.String("segmentDir", segmentDir),
			zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()),
			zap.Int("blockIndexesCount", len(reader.blockIndexes)))
		return reader, nil
	}

	// Priority 2: If footer doesn't exist, scan block files sequentially (0.blk, 1.blk, 2.blk...)
	// This is for incomplete segments that are still being written
	if err := reader.scanLocalBlockFiles(ctx); err == nil {
		reader.legacyMode.Store(false)
		logger.Ctx(ctx).Debug("local file readerAdv created successfully (block scan mode)",
			zap.String("segmentDir", segmentDir),
			zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()),
			zap.Int("blockIndexesCount", len(reader.blockIndexes)))
		return reader, nil
	}

	// Priority 3: Check if legacy file exists
	filePath := reader.filePath
	legacyFileExists := false
	if _, statErr := os.Stat(filePath); statErr == nil {
		legacyFileExists = true
	}

	// If no per-block files, no footer, AND no legacy file, use per-block mode with empty blockIndexes
	// This allows reader to be created before any data is written (empty segment)
	// Error will occur when trying to read (ErrEntryNotFound in ReadNextBatchAdv)
	if !legacyFileExists {
		reader.legacyMode.Store(false)
		reader.blockIndexes = make([]*codec.IndexRecord, 0)
		logger.Ctx(ctx).Debug("local file readerAdv created successfully (per-block mode, empty segment)", zap.String("segmentDir", segmentDir),
			zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()), zap.Int("blockIndexesCount", 0))
		return reader, nil
	}

	// Legacy single-file mode (legacy file exists)
	reader.legacyMode.Store(true)
	logger.Ctx(ctx).Debug("attempting to open legacy single file for reading", zap.String("filePath", filePath))

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to open file for reading", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		logger.Ctx(ctx).Warn("failed to stat file", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("stat file: %w", err)
	}

	logger.Ctx(ctx).Debug("file opened successfully", zap.String("filePath", filePath), zap.Int64("fileSize", stat.Size()))

	currentSize := stat.Size()
	reader.file = file

	// try to check footer when opening
	parseFooterErr := reader.tryParseFooterAndIndexesIfExists(ctx)
	if parseFooterErr != nil {
		file.Close()
		reader.closed.Store(true)
		logger.Ctx(ctx).Warn("failed to parse footer and indexes", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("try parse footer and indexes: %w", err)
	}

	logger.Ctx(ctx).Debug("local file readerAdv created successfully (legacy mode)", zap.String("filePath", filePath),
		zap.Int64("currentFileSize", currentSize), zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()), zap.Int("blockIndexesCount", len(reader.blockIndexes)))
	return reader, nil
}

// listBlockFiles lists all completed block files in the segment directory
// tryLoadLocalFooter attempts to load footer.blk from local disk
// If successful, it loads all block indexes from the footer data
// Returns nil on success, error if footer doesn't exist or can't be parsed
func (r *LocalFileReaderAdv) tryLoadLocalFooter(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryLoadLocalFooter")
	defer sp.End()

	footerPath := serde.GetFooterBlockPath(r.baseDir, r.logId, r.segId)
	footerData, err := os.ReadFile(footerPath)
	if err != nil {
		return fmt.Errorf("read footer file: %w", err)
	}

	// Parse footer record and block indexes using serde package
	footerAndIndexes, err := serde.DeserializeFooterAndIndexes(footerData)
	if err != nil {
		// Try parsing just footer (for backwards compatibility or empty segments with no indexes)
		footerRecord, parseErr := codec.ParseFooterFromBytes(footerData)
		if parseErr != nil {
			return fmt.Errorf("parse footer: %w", err)
		}
		r.footer = footerRecord
		r.blockIndexes = make([]*codec.IndexRecord, 0)
	} else {
		r.footer = footerAndIndexes.Footer
		r.blockIndexes = footerAndIndexes.Indexes
	}

	r.isIncompleteFile.Store(false)
	r.version.Store(uint32(r.footer.Version))
	r.flags.Store(uint32(r.footer.Flags))

	// Check if segment is compacted from footer flags
	if codec.IsCompacted(r.footer.Flags) {
		r.isCompacted = true
	}

	logger.Ctx(ctx).Debug("loaded footer from footer.blk",
		zap.Int32("totalBlocks", r.footer.TotalBlocks),
		zap.Int64("lac", r.footer.LAC),
		zap.Int("blockIndexesCount", len(r.blockIndexes)),
		zap.Bool("isCompacted", r.isCompacted))

	return nil
}

// scanLocalBlockFiles scans block files sequentially starting from 0.blk
// It builds block indexes by reading each block file header
// Scanning stops when: block file doesn't exist, or encounter a directory (fence)
// Also handles merged blocks (m_N.blk) for compacted segments
// Returns nil on success (even if no blocks found)
// Returns error only if there's a critical failure
func (r *LocalFileReaderAdv) scanLocalBlockFiles(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "scanLocalBlockFiles")
	defer sp.End()

	segmentDir := getSegmentDir(r.baseDir, r.logId, r.segId)
	r.blockIndexes = make([]*codec.IndexRecord, 0)
	var currentOffset int64 = 0

	// First check if we have merged blocks (compacted segment)
	// By checking if m_0.blk exists
	mergedBlockPath := serde.GetMergedBlockFilePath(r.baseDir, r.logId, r.segId, 0)
	if _, err := os.Stat(mergedBlockPath); err == nil {
		r.isCompacted = true
	}

	// Scan blocks sequentially starting from 0
	for blockId := int64(0); ; blockId++ {
		var blockPath string
		if r.isCompacted {
			blockPath = serde.GetMergedBlockFilePath(r.baseDir, r.logId, r.segId, blockId)
		} else {
			blockPath = getBlockFilePath(r.baseDir, r.logId, r.segId, blockId)
		}

		// Check if this is a fence directory (directory with .blk suffix)
		info, err := os.Stat(blockPath)
		if err != nil {
			if os.IsNotExist(err) {
				// Block doesn't exist, we've reached the end of data blocks
				logger.Ctx(ctx).Debug("block file not found, stopping scan",
					zap.Int64("blockId", blockId),
					zap.String("blockPath", blockPath))
				break
			}
			// Other error, log and stop
			logger.Ctx(ctx).Warn("failed to stat block file",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath),
				zap.Error(err))
			break
		}

		// Check if it's a fence directory
		if info.IsDir() {
			// This is a fence directory, no more data blocks after this
			logger.Ctx(ctx).Debug("encountered fence directory, stopping scan",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath))
			break
		}

		// Read and parse block file
		blockData, err := os.ReadFile(blockPath)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block file",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath),
				zap.Error(err))
			break
		}

		// Parse block header to get entry range
		if len(blockData) < codec.RecordHeaderSize+codec.BlockHeaderRecordSize {
			logger.Ctx(ctx).Warn("block file too small",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath),
				zap.Int("size", len(blockData)))
			break
		}

		headerData := blockData[:codec.RecordHeaderSize+codec.BlockHeaderRecordSize]
		record, err := codec.DecodeRecord(headerData)
		if err != nil || record.Type() != codec.BlockHeaderRecordType {
			logger.Ctx(ctx).Warn("failed to decode block header",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath),
				zap.Error(err))
			break
		}

		blockHeader := record.(*codec.BlockHeaderRecord)

		// Verify block data integrity
		blockDataContent := blockData[codec.RecordHeaderSize+codec.BlockHeaderRecordSize:]
		if err := codec.VerifyBlockDataIntegrity(blockHeader, blockDataContent); err != nil {
			logger.Ctx(ctx).Warn("block data integrity verification failed",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath),
				zap.Error(err))
			break
		}

		// Create index record
		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(blockId),
			StartOffset:  currentOffset, // Virtual offset for per-block files
			BlockSize:    uint32(len(blockData)),
			FirstEntryID: blockHeader.FirstEntryID,
			LastEntryID:  blockHeader.LastEntryID,
		}
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		currentOffset += int64(len(blockData))

		logger.Ctx(ctx).Debug("loaded block index from scan",
			zap.Int64("blockId", blockId),
			zap.Int64("firstEntryId", blockHeader.FirstEntryID),
			zap.Int64("lastEntryId", blockHeader.LastEntryID),
			zap.Bool("isCompacted", r.isCompacted))
	}

	// If we found any blocks, this is a valid incomplete segment
	if len(r.blockIndexes) > 0 {
		r.version.Store(codec.FormatVersion)
		logger.Ctx(ctx).Debug("block scan completed",
			zap.String("segmentDir", segmentDir),
			zap.Int("blockCount", len(r.blockIndexes)),
			zap.Bool("isCompacted", r.isCompacted))
		return nil
	}

	// No blocks found - return error to trigger legacy file fallback
	return fmt.Errorf("no local block files found")
}

func (r *LocalFileReaderAdv) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt, lastReadBatchInfo *proto.LastReadState) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatchAdv")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatchAdv called", zap.Int64("startEntryID", opt.StartEntryID), zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
		zap.Bool("isIncompleteFile", r.isIncompleteFile.Load()), zap.Bool("legacyMode", r.legacyMode.Load()), zap.Any("opt", opt), zap.Any("lastReadBatchInfo", lastReadBatchInfo))

	if r.closed.Load() {
		return nil, werr.ErrFileReaderAlreadyClosed
	}

	// Per-block file mode (not legacy)
	if !r.legacyMode.Load() {
		return r.readNextBatchPerBlockMode(ctx, opt, lastReadBatchInfo)
	}

	// Legacy single-file mode
	// set adv options
	if lastReadBatchInfo != nil {
		r.flags.Store(uint32(lastReadBatchInfo.Flags))
		r.version.Store(uint32(lastReadBatchInfo.Version))
	} else {
		// Try to parse footer and indexes
		if err := r.tryParseFooterAndIndexesIfExists(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to parse footer and indexes", zap.String("filePath", r.filePath), zap.Error(err))
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
		startBlockID = int64(lastReadBatchInfo.LastBlockId + 1)
		startBlockOffset = lastReadBatchInfo.BlockOffset + int64(lastReadBatchInfo.BlockSize)
		logger.Ctx(ctx).Debug("using advOpt mode", zap.Int64("lastBlockNumber", int64(lastReadBatchInfo.LastBlockId)),
			zap.Int64("startBlockID", startBlockID), zap.Int64("startBlockOffset", startBlockOffset))
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
			logger.Ctx(ctx).Debug("no more entries to read", zap.String("filePath", r.filePath), zap.Int64("startEntryId", opt.StartEntryID))
			return nil, werr.ErrFileReaderEndOfFile
		}
	}

	return r.readDataBlocksUnsafe(ctx, opt, startBlockID, startBlockOffset)
}

// readNextBatchPerBlockMode reads entries from per-block files
func (r *LocalFileReaderAdv) readNextBatchPerBlockMode(ctx context.Context, opt storage.ReaderOpt, lastReadBatchInfo *proto.LastReadState) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readNextBatchPerBlockMode")
	defer sp.End()
	startTime := time.Now()

	// Refresh block list if incomplete (rescan to get latest blocks)
	if r.isIncompleteFile.Load() {
		r.mu.Lock()
		if err := r.scanLocalBlockFiles(ctx); err != nil {
			// Scanning failed, but continue with existing block indexes
			logger.Ctx(ctx).Debug("rescan block files failed, continuing with existing indexes", zap.Error(err))
		}
		r.mu.Unlock()
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Determine starting block
	startBlockID := int64(0)
	if lastReadBatchInfo != nil {
		r.flags.Store(uint32(lastReadBatchInfo.Flags))
		r.version.Store(uint32(lastReadBatchInfo.Version))
		startBlockID = int64(lastReadBatchInfo.LastBlockId + 1)
	} else if !r.isIncompleteFile.Load() && len(r.blockIndexes) > 0 {
		// Find the block containing the start entry ID
		foundStartBlock, err := codec.SearchBlock(r.blockIndexes, opt.StartEntryID)
		if err != nil {
			return nil, err
		}
		if foundStartBlock == nil {
			return nil, werr.ErrFileReaderEndOfFile
		}
		startBlockID = int64(foundStartBlock.BlockNumber)
	}

	// Read blocks
	maxEntries := opt.MaxBatchEntries
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := r.maxBatchSize

	entries := make([]*proto.LogEntry, 0, maxEntries)
	var lastBlockInfo *codec.IndexRecord
	entriesCollected := int64(0)
	readBytes := int64(0)
	hasDataReadError := false

	// Find starting index in blockIndexes
	startIdx := 0
	for idx, bi := range r.blockIndexes {
		if int64(bi.BlockNumber) >= startBlockID {
			startIdx = idx
			break
		}
	}

	for idx := startIdx; idx < len(r.blockIndexes) && readBytes < maxBytes && entriesCollected < maxEntries; idx++ {
		blockIndex := r.blockIndexes[idx]
		blockId := int64(blockIndex.BlockNumber)

		// Read block file - use merged block path if segment is compacted
		var blockPath string
		if r.isCompacted {
			blockPath = serde.GetMergedBlockFilePath(r.baseDir, r.logId, r.segId, blockId)
		} else {
			blockPath = getBlockFilePath(r.baseDir, r.logId, r.segId, blockId)
		}
		blockData, err := os.ReadFile(blockPath)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block file", zap.String("blockPath", blockPath), zap.Error(err))
			hasDataReadError = true
			break
		}

		// Parse block header
		if len(blockData) < codec.RecordHeaderSize+codec.BlockHeaderRecordSize {
			logger.Ctx(ctx).Warn("block file too small", zap.String("blockPath", blockPath))
			break
		}

		headerData := blockData[:codec.RecordHeaderSize+codec.BlockHeaderRecordSize]
		record, err := codec.DecodeRecord(headerData)
		if err != nil || record.Type() != codec.BlockHeaderRecordType {
			logger.Ctx(ctx).Warn("failed to decode block header", zap.String("blockPath", blockPath), zap.Error(err))
			break
		}

		blockHeader := record.(*codec.BlockHeaderRecord)
		blockDataContent := blockData[codec.RecordHeaderSize+codec.BlockHeaderRecordSize:]

		// Verify block integrity
		if err := codec.VerifyBlockDataIntegrity(blockHeader, blockDataContent); err != nil {
			logger.Ctx(ctx).Warn("block integrity verification failed", zap.String("blockPath", blockPath), zap.Error(err))
			break
		}

		// Decode data records
		records, err := codec.DecodeRecordList(blockDataContent)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to decode block data", zap.String("blockPath", blockPath), zap.Error(err))
			break
		}

		currentEntryID := blockHeader.FirstEntryID
		for _, rec := range records {
			if rec.Type() != codec.DataRecordType {
				continue
			}
			if opt.StartEntryID <= currentEntryID {
				dr := rec.(*codec.DataRecord)
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

		lastBlockInfo = &codec.IndexRecord{
			BlockNumber:  int32(blockId),
			StartOffset:  blockIndex.StartOffset,
			BlockSize:    uint32(len(blockData)),
			FirstEntryID: blockHeader.FirstEntryID,
			LastEntryID:  blockHeader.LastEntryID,
		}

		logger.Ctx(ctx).Debug("read block file", zap.Int64("blockId", blockId),
			zap.Int64("entriesCollected", entriesCollected), zap.Int64("readBytes", readBytes))
	}

	if len(entries) == 0 {
		if !hasDataReadError {
			if !r.isIncompleteFile.Load() || r.footer != nil {
				return nil, werr.ErrFileReaderEndOfFile
			}
			// Check if footer exists now
			footerPath := getFooterBlockPath(r.baseDir, r.logId, r.segId)
			if _, err := os.Stat(footerPath); err == nil {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
		}
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	}

	metrics.WpFileReadBatchBytes.WithLabelValues(r.logIdStr).Add(float64(readBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(r.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

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

	return &proto.BatchReadResult{
		Entries:       entries,
		LastReadState: lastReadState,
	}, nil
}

// =========================================================================================
// PUBLIC API METHODS WITH BRANCHING
// =========================================================================================
// NOTE: Legacy mode support functions have been moved to reader_impl_legacy.go for better
// code organization. See that file for backward compatibility with data.log format.
// =========================================================================================

// GetLastEntryID returns the last entry ID in the file.
// BRANCHING POINT: Uses legacyMode.Load() to call either:
//   - loadBlockFilesUnsafe (per-block mode, legacyMode=false)
//   - scanForAllBlockInfoUnsafe (legacy mode, legacyMode=true)
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

		// Branch based on mode
		if r.legacyMode.Load() {
			// Legacy mode: scan single file
			if err := r.scanForAllBlockInfoUnsafe(ctx); err != nil {
				return -1, err
			}
		} else {
			// Per-block mode: rescan block files
			if err := r.scanLocalBlockFiles(ctx); err != nil {
				// Scanning failed, continue with existing indexes
				logger.Ctx(ctx).Debug("rescan block files failed in GetLastEntryID", zap.Error(err))
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

	return -1, werr.ErrFileReaderNoBlockFound
}

// isFooterExistsUnsafe checks if the segment has been finalized (has footer).
// BRANCHING POINT: Uses legacyMode.Load() to either:
//   - Check footer.blk file exists (per-block mode, legacyMode=false)
//   - Read and parse footer from data.log (legacy mode, legacyMode=true, calls readAtUnsafe from reader_impl_legacy.go)
func (r *LocalFileReaderAdv) isFooterExistsUnsafe(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "isFooterExists")
	defer sp.End()

	// Per-block mode: check for footer.blk file
	if !r.legacyMode.Load() {
		footerPath := getFooterBlockPath(r.baseDir, r.logId, r.segId)
		if _, err := os.Stat(footerPath); err == nil {
			return true
		}
		return false
	}

	// Legacy single-file mode (implementation calls readAtUnsafe from reader_impl_legacy.go)
	if r.file == nil {
		return false
	}

	// update size
	stat, err := r.file.Stat()
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stat file", zap.String("filePath", r.filePath), zap.Error(err))
		return false
	}
	currentSize := stat.Size()

	// Check if file has minimum size for a footer (use V5 footer size as minimum)
	minFooterSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5
	if currentSize < int64(minFooterSize) {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, no footer exists yet", zap.String("filePath", r.filePath), zap.Int64("fileSize", currentSize))
		return false
	}

	// Try to read and parse footer from the end of the file using compatibility parsing
	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData, err := r.readAtUnsafe(ctx, currentSize-int64(maxFooterSize), maxFooterSize)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, no footer exists yet", zap.String("filePath", r.filePath), zap.Error(err))
		return false
	}

	// Try to parse footer with compatibility parsing
	_, err = codec.ParseFooterFromBytes(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to parse footer record, no footer exists yet", zap.String("filePath", r.filePath), zap.Error(err))
		return false
	}

	// footer exists
	return true
}

// =========================================================================================
// PUBLIC API METHODS AND TEST HELPERS
// =========================================================================================

// Close closes the reader and releases resources (works for both modes)
func (r *LocalFileReaderAdv) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "Close")
	defer sp.End()
	logger.Ctx(ctx).Info("Closing segment reader", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId))

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
	logger.Ctx(ctx).Info("segment reader closed", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId))
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

func (r *LocalFileReaderAdv) UpdateLastAddConfirmed(ctx context.Context, lac int64) error {
	// NO-OP
	return nil
}
