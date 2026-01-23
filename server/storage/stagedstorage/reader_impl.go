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

package stagedstorage

import (
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

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var (
	SegmentReaderScope = "StagedFileReader"
)

var _ storage.Reader = (*StagedFileReaderAdv)(nil)

// StagedFileReaderAdv implements staged storage reader for local disk and object storage.
// Supports LAC-based reading and compacted object access.
// Uses per-block file format ({blockId}.blk) for local storage.
type StagedFileReaderAdv struct {
	logId    int64
	segId    int64
	logIdStr string // for metrics only
	baseDir  string // base directory for per-block files

	// object access, only used for compacted object access
	bucket          string
	rootPath        string
	storageCli      objectstorage.ObjectStorage
	pool            *conc.Pool[*BlockReadResult]
	maxFetchThreads int
	maxBatchSize    int64

	// file access
	mu sync.RWMutex

	// runtime file state
	flags            atomic.Uint32 // stores uint16 value
	version          atomic.Uint32 // stores uint16 value
	footer           *codec.FooterRecord
	blockIndexes     []*codec.IndexRecord
	isIncompleteFile atomic.Bool // Whether this file is incomplete (no footer)
	isCompacted      atomic.Bool // Whether this segment is compacted (data in minio)
	closed           atomic.Bool

	lacMu            sync.RWMutex
	lastAddConfirmed atomic.Int64 // only for runtime segment reads; reduces multi-node requests compared to quorum read
}

// NewStagedFileReaderAdv creates a new staged file reader
func NewStagedFileReaderAdv(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration) (*StagedFileReaderAdv, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewStagedFileReader")
	defer sp.End()

	maxBatchSize := cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize.Int64()
	maxFetchThreads := cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads
	logger.Ctx(ctx).Debug("creating new staged file reader",
		zap.String("localBaseDir", localBaseDir), zap.Int64("logId", logId), zap.Int64("segId", segId),
		zap.String("rootPath", rootPath), zap.Int("maxFetchThreads", maxFetchThreads), zap.Int64("maxBatchSize", maxBatchSize))

	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Create base reader
	reader := &StagedFileReaderAdv{
		logId:           logId,
		segId:           segId,
		logIdStr:        fmt.Sprintf("%d", logId),
		baseDir:         localBaseDir,
		maxBatchSize:    maxBatchSize,
		bucket:          bucket,
		rootPath:        rootPath,
		maxFetchThreads: maxFetchThreads,
		pool:            conc.NewPool[*BlockReadResult](maxFetchThreads),
		storageCli:      storageCli,
	}
	reader.flags.Store(0)
	reader.version.Store(codec.FormatVersion)
	reader.closed.Store(false)
	reader.lastAddConfirmed.Store(-1)
	reader.isIncompleteFile.Store(true)

	// Check for per-block files (local storage)
	blockFiles, perBlockErr := reader.listBlockFiles(ctx)
	if perBlockErr == nil && len(blockFiles) > 0 {
		logger.Ctx(ctx).Debug("detected per-block files", zap.String("segmentDir", segmentDir), zap.Int("blockFileCount", len(blockFiles)))

		// Load block indexes from per-block files
		if err := reader.loadBlockFilesUnsafe(ctx, blockFiles); err != nil {
			logger.Ctx(ctx).Warn("failed to load block files", zap.String("segmentDir", segmentDir), zap.Error(err))
			return nil, fmt.Errorf("load block files: %w", err)
		}

		logger.Ctx(ctx).Debug("staged file reader created successfully",
			zap.String("segmentDir", segmentDir), zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()), zap.Int("blockIndexesCount", len(reader.blockIndexes)))
		return reader, nil
	}

	// No local block files found, try to check if compacted data exists in minio
	if err := reader.tryParseMinioFooterUnsafe(ctx); err != nil {
		logger.Ctx(ctx).Debug("no local or compacted data found", zap.String("segmentDir", segmentDir), zap.Error(err))
		return nil, werr.ErrEntryNotFound
	}

	logger.Ctx(ctx).Debug("staged file reader created successfully (compacted mode)",
		zap.String("segmentDir", segmentDir), zap.Bool("isCompacted", reader.isCompacted.Load()), zap.Int("blockIndexesCount", len(reader.blockIndexes)))
	return reader, nil
}

// listBlockFiles lists all completed block files in the segment directory
func (r *StagedFileReaderAdv) listBlockFiles(ctx context.Context) ([]int64, error) {
	segmentDir := getSegmentDir(r.baseDir, r.logId, r.segId)
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var blockIds []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Only consider completed block files (n.blk), not inflight or incomplete ones
		if len(name) > 4 && name[len(name)-4:] == ".blk" && !strings.Contains(name, ".blk.") && name != "footer.blk" {
			// Parse block ID from filename
			blockIdStr := name[:len(name)-4]
			blockId, err := strconv.ParseInt(blockIdStr, 10, 64)
			if err != nil {
				logger.Ctx(ctx).Debug("skipping non-numeric block file", zap.String("filename", name))
				continue
			}
			blockIds = append(blockIds, blockId)
		}
	}

	// Sort block IDs
	sort.Slice(blockIds, func(i, j int) bool {
		return blockIds[i] < blockIds[j]
	})

	return blockIds, nil
}

// loadBlockFilesUnsafe loads block indexes from per-block files
func (r *StagedFileReaderAdv) loadBlockFilesUnsafe(ctx context.Context, blockIds []int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "loadBlockFiles")
	defer sp.End()

	// First, try to load footer if exists
	// Note: Do NOT read footer.blk.inflight - it may be incomplete (crashed during write)
	// If footer.blk doesn't exist, we fall back to scanning individual block files
	footerPath := getLocalFooterBlockPath(r.baseDir, r.logId, r.segId)
	if footerData, err := os.ReadFile(footerPath); err == nil {
		// Parse footer record
		footerRecord, err := codec.ParseFooterFromBytes(footerData)
		if err == nil {
			r.footer = footerRecord
			r.isIncompleteFile.Store(false)
			r.version.Store(uint32(footerRecord.Version))
			r.flags.Store(uint32(footerRecord.Flags))
			logger.Ctx(ctx).Debug("loaded footer from footer.blk", zap.Int32("totalBlocks", footerRecord.TotalBlocks))
		}
	}

	// Load block indexes by reading block headers from each block file
	r.blockIndexes = make([]*codec.IndexRecord, 0, len(blockIds))

	for _, blockId := range blockIds {
		blockPath := getBlockFilePath(r.baseDir, r.logId, r.segId, blockId)
		blockData, err := os.ReadFile(blockPath)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block file", zap.String("blockPath", blockPath), zap.Error(err))
			continue
		}

		// Parse block header to get entry range
		if len(blockData) < codec.RecordHeaderSize+codec.BlockHeaderRecordSize {
			logger.Ctx(ctx).Warn("block file too small", zap.String("blockPath", blockPath), zap.Int("size", len(blockData)))
			continue
		}

		headerData := blockData[:codec.RecordHeaderSize+codec.BlockHeaderRecordSize]
		record, err := codec.DecodeRecord(headerData)
		if err != nil || record.Type() != codec.BlockHeaderRecordType {
			logger.Ctx(ctx).Warn("failed to decode block header", zap.String("blockPath", blockPath), zap.Error(err))
			continue
		}

		blockHeader := record.(*codec.BlockHeaderRecord)

		// Create index record
		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(blockId),
			StartOffset:  blockId, // Virtual offset
			BlockSize:    uint32(len(blockData)),
			FirstEntryID: blockHeader.FirstEntryID,
			LastEntryID:  blockHeader.LastEntryID,
		}
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		logger.Ctx(ctx).Debug("loaded block index",
			zap.Int64("blockId", blockId),
			zap.Int64("firstEntryId", blockHeader.FirstEntryID),
			zap.Int64("lastEntryId", blockHeader.LastEntryID))
	}

	// Update version if not set from footer
	if len(r.blockIndexes) > 0 && r.footer == nil {
		r.version.Store(codec.FormatVersion)
	}

	return nil
}

func (r *StagedFileReaderAdv) tryParseFooterAndIndexesIfExists(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseFooterAndIndexesIfExists")
	defer sp.End()
	logger.Ctx(ctx).Debug("try to read footer and block indexes")

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

	// Try to read footer from minio (compacted data)
	if err := r.tryParseMinioFooterUnsafe(ctx); err != nil {
		logger.Ctx(ctx).Debug("failed to parse minio footer", zap.Error(err))
		return nil // Not an error - data might still be in local per-block files
	}

	logger.Ctx(ctx).Info("successfully parsed compacted footer from minio", zap.Bool("isCompacted", r.isCompacted.Load()))
	return nil
}

// tryParseMinioFooterUnsafe attempts to parse footer from minio object storage
func (r *StagedFileReaderAdv) tryParseMinioFooterUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseMinioFooterUnsafe")
	defer sp.End()

	if r.storageCli == nil {
		return fmt.Errorf("storage client not available")
	}

	// Check if compacted footer exists in minio
	footerKey := r.getFooterBlockKey()

	logger.Ctx(ctx).Debug("checking for compacted footer in minio", zap.String("bucket", r.bucket), zap.String("footerKey", footerKey))

	// Check if footer exists
	objSize, _, err := r.storageCli.StatObject(ctx, r.bucket, footerKey)
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			logger.Ctx(ctx).Debug("no compacted footer found in minio", zap.String("footerKey", footerKey))
			return fmt.Errorf("no footer in minio")
		}
		logger.Ctx(ctx).Warn("failed to stat footer object in minio", zap.String("footerKey", footerKey), zap.Error(err))
		return fmt.Errorf("failed to stat footer object: %w", err)
	}

	// Read the entire footer file
	reader, err := r.storageCli.GetObject(ctx, r.bucket, footerKey, 0, objSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to get footer object from minio", zap.String("footerKey", footerKey), zap.Error(err))
		return fmt.Errorf("failed to get footer object: %w", err)
	}
	defer reader.Close()

	footerData := make([]byte, objSize)
	_, err = io.ReadFull(reader, footerData)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read footer data from minio", zap.String("footerKey", footerKey), zap.Error(err))
		return fmt.Errorf("failed to read footer data: %w", err)
	}

	// Parse footer and indexes from minio data
	if err := r.parseMinioFooterDataUnsafe(ctx, footerData); err != nil {
		logger.Ctx(ctx).Warn("failed to parse minio footer data", zap.String("footerKey", footerKey), zap.Error(err))
		return fmt.Errorf("failed to parse minio footer data: %w", err)
	}

	// Mark as compacted
	r.isCompacted.Store(true)
	r.isIncompleteFile.Store(false)

	logger.Ctx(ctx).Info("successfully parsed compacted footer from minio",
		zap.String("footerKey", footerKey), zap.Int32("totalBlocks", r.footer.TotalBlocks), zap.Bool("isCompacted", codec.IsCompacted(r.footer.Flags)))

	return nil
}

// parseMinioFooterDataUnsafe parses footer and indexes from minio data
func (r *StagedFileReaderAdv) parseMinioFooterDataUnsafe(ctx context.Context, footerData []byte) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "parseMinioFooterDataUnsafe")
	defer sp.End()

	// The footer data contains: [IndexRecord1][IndexRecord2]...[IndexRecordN][FooterRecord]
	// We need to parse from the end backwards using compatibility parsing

	maxFooterSize := codec.GetMaxFooterReadSize()
	if len(footerData) < maxFooterSize {
		return fmt.Errorf("footer data too small: %d bytes", len(footerData))
	}

	// Use the last maxFooterSize bytes for compatibility parsing
	footerBytes := footerData[len(footerData)-maxFooterSize:]

	footer, err := codec.ParseFooterFromBytes(footerBytes)
	if err != nil {
		return fmt.Errorf("failed to parse footer record: %w", err)
	}
	r.footer = footer
	r.version.Store(uint32(footer.Version))
	r.flags.Store(uint32(footer.Flags))

	// Parse index records
	if footer.TotalBlocks > 0 {
		// Calculate the actual footer size from the parsed footer
		actualFooterSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(footer.Version)
		footerStart := len(footerData) - actualFooterSize

		indexDataSize := int(footer.IndexLength)
		if indexDataSize > footerStart {
			return fmt.Errorf("index data size %d exceeds available data %d", indexDataSize, footerStart)
		}

		indexStart := footerStart - indexDataSize
		indexData := footerData[indexStart:footerStart]

		if err := r.parseIndexDataUnsafe(ctx, indexData, int(footer.TotalBlocks)); err != nil {
			return fmt.Errorf("failed to parse index data: %w", err)
		}
	}

	return nil
}

// parseIndexDataUnsafe parses index records from raw data
func (r *StagedFileReaderAdv) parseIndexDataUnsafe(ctx context.Context, indexData []byte, expectedBlocks int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "parseIndexDataUnsafe")
	defer sp.End()

	r.blockIndexes = make([]*codec.IndexRecord, 0, expectedBlocks)

	offset := 0
	for offset < len(indexData) && len(r.blockIndexes) < expectedBlocks {
		if offset+codec.RecordHeaderSize > len(indexData) {
			break
		}

		record, err := codec.DecodeRecord(indexData[offset:])
		if err != nil {
			return fmt.Errorf("failed to decode index record at offset %d: %w", offset, err)
		}

		if record.Type() != codec.IndexRecordType {
			return fmt.Errorf("unexpected record type %d at offset %d", record.Type(), offset)
		}

		indexRecord := record.(*codec.IndexRecord)
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		// Move to next record
		offset += codec.RecordHeaderSize + codec.IndexRecordSize
	}

	if len(r.blockIndexes) != expectedBlocks {
		return fmt.Errorf("parsed %d blocks, expected %d", len(r.blockIndexes), expectedBlocks)
	}

	// Sort blocks by block number to ensure correct order
	sort.Slice(r.blockIndexes, func(i, j int) bool {
		return r.blockIndexes[i].BlockNumber < r.blockIndexes[j].BlockNumber
	})

	logger.Ctx(ctx).Debug("successfully parsed index records from minio data",
		zap.Int("blockCount", len(r.blockIndexes)))

	return nil
}

// getFooterBlockKey generates the object key for the footer block
func (r *StagedFileReaderAdv) getFooterBlockKey() string {
	return fmt.Sprintf("%s/%d/%d/footer.blk", r.rootPath, r.logId, r.segId)
}

// getCompactedBlockKey generates the object key for a compacted block
func (r *StagedFileReaderAdv) getCompactedBlockKey(blockID int64) string {
	return fmt.Sprintf("%s/%d/%d/m_%d.blk", r.rootPath, r.logId, r.segId, blockID)
}

// ReadNextBatchAdv reads the next batch of entries from the file according to read opt
func (r *StagedFileReaderAdv) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt, lastReadBatchInfo *proto.LastReadState) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatchAdv")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatchAdv called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
		zap.Bool("isIncompleteFile", r.isIncompleteFile.Load()),
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
				zap.String("baseDir", r.baseDir), zap.Int64("logId", r.logId), zap.Int64("segId", r.segId), zap.Error(err))
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

		// TODO: Add a flag to indicate if the block has been fully read. Without this flag,
		// the LAC mechanism may encounter situations where only part of a block is read.
		// The unified simple solution is to continue scanning from this block.
		// For further optimization, add another flag to indicate whether this block
		// was completely read in the last operation. If fully read, we can proceed directly
		// to the next block; otherwise, we need to start scanning from this block again.
		//startBlockID = int64(lastReadBatchInfo.LastBlockId + 1)
		//startBlockOffset = lastReadBatchInfo.BlockOffset + int64(lastReadBatchInfo.BlockSize)
		startBlockID = int64(lastReadBatchInfo.LastBlockId)
		startBlockOffset = lastReadBatchInfo.BlockOffset
		logger.Ctx(ctx).Debug("using advOpt mode",
			zap.Int64("lastBlockNumber", int64(lastReadBatchInfo.LastBlockId)), zap.Int64("startBlockID", startBlockID), zap.Int64("startBlockOffset", startBlockOffset))
	} else if !r.isIncompleteFile.Load() {
		// Scenario 2: First read with footer (completed file)
		// When we have footer, find the block containing the start entry ID
		foundStartBlock, err := codec.SearchBlock(r.blockIndexes, opt.StartEntryID)
		if err != nil {
			logger.Ctx(ctx).Warn("search block failed", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId), zap.Int64("entryId", opt.StartEntryID), zap.Error(err))
			return nil, err
		}
		if foundStartBlock != nil {
			logger.Ctx(ctx).Debug("found block for entryId", zap.Int64("entryId", opt.StartEntryID), zap.Int32("blockID", foundStartBlock.BlockNumber))
			startBlockID = int64(foundStartBlock.BlockNumber)
			startBlockOffset = foundStartBlock.StartOffset
		}

		// No block found for entryId in this completed file
		if foundStartBlock == nil {
			if opt.StartEntryID <= r.footer.LAC {
				logger.Ctx(ctx).Debug("some data less than footer's LAC but can't read here now, try to read from other nodes",
					zap.Int64("lac", r.footer.LAC), zap.Int64("logId", r.logId), zap.Int64("segId", r.segId), zap.Int64("startEntryId", opt.StartEntryID))
				return nil, werr.ErrEntryNotFound.WithCauseErrMsg("some data less than footer's LAC but can't read here now, try to read from other nodes")
			}
			logger.Ctx(ctx).Debug("no more entries to read", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId), zap.Int64("startEntryId", opt.StartEntryID))
			return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
		}
	}

	return r.readDataBlocksUnsafe(ctx, opt, startBlockID, startBlockOffset)
}

// GetLastEntryID returns the last entry ID in the file
func (r *StagedFileReaderAdv) GetLastEntryID(ctx context.Context) (int64, error) {
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

	// If the file is incomplete, try to reload block files to get the latest entry ID
	if r.isIncompleteFile.Load() {
		r.mu.Lock() // Need write lock for reloading
		defer r.mu.Unlock()

		// Double-check after acquiring write lock
		if r.footer != nil && len(r.blockIndexes) > 0 {
			lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
			return lastBlock.LastEntryID, nil
		}

		// Reload block files to get latest state
		blockFiles, err := r.listBlockFiles(ctx)
		if err != nil {
			return -1, err
		}
		if len(blockFiles) == 0 {
			return -1, werr.ErrFileReaderNoBlockFound
		}
		if err := r.loadBlockFilesUnsafe(ctx, blockFiles); err != nil {
			return -1, err
		}

		// Check if we have any blocks after reloading
		if len(r.blockIndexes) == 0 {
			return -1, werr.ErrFileReaderNoBlockFound
		}

		// Return the last entry ID from the last block
		lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
		return lastBlock.LastEntryID, nil
	}

	return -1, werr.ErrFileReaderNoBlockFound
}

func (r *StagedFileReaderAdv) readDataBlocksUnsafe(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startBlockOffset int64) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

	// Check if this is a compacted segment - if so, read from minio
	if r.isCompacted.Load() {
		logger.Ctx(ctx).Debug("reading compacted data from minio", zap.String("baseDir", r.baseDir), zap.Int64("startBlockID", startBlockID))
		return r.readCompactedDataFromMinio(ctx, opt, startBlockID, startBlockOffset)
	}

	// Read from per-block files (default local mode)
	logger.Ctx(ctx).Debug("reading data from per-block files", zap.String("baseDir", r.baseDir), zap.Int64("startBlockID", startBlockID))
	return r.readDataBlocksFromPerBlockFilesUnsafe(ctx, opt, startBlockID, startTime)
}

// readDataBlocksFromPerBlockFilesUnsafe reads data blocks from per-block files
func (r *StagedFileReaderAdv) readDataBlocksFromPerBlockFilesUnsafe(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startTime time.Time) (*proto.BatchReadResult, error) {
	// Soft limitation
	maxEntries := opt.MaxBatchEntries
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := r.maxBatchSize

	// read data result
	entries := make([]*proto.LogEntry, 0, maxEntries)
	var lastBlockInfo *codec.IndexRecord

	// get current lac
	currentLAC := r.lastAddConfirmed.Load()
	lastReadEntryID := int64(-1)
	entriesCollected := int64(0)
	readBytes := int64(0)
	hasDataReadError := false

	segmentDir := getSegmentDir(r.baseDir, r.logId, r.segId)

	// extract data from blocks
	for i := startBlockID; readBytes < maxBytes && entriesCollected < maxEntries; i++ {
		currentLAC = r.lastAddConfirmed.Load()
		if r.footer != nil && r.footer.LAC > currentLAC {
			currentLAC = r.footer.LAC
		}
		currentBlockID := i

		// Get block file path
		blockFilePath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", currentBlockID))

		// Read entire block file
		blockData, err := os.ReadFile(blockFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Ctx(ctx).Debug("block file not found, stopping read", zap.String("blockFilePath", blockFilePath), zap.Int64("blockNumber", currentBlockID))
			} else {
				logger.Ctx(ctx).Warn("failed to read block file",
					zap.String("blockFilePath", blockFilePath), zap.Int64("blockNumber", currentBlockID), zap.Error(err))
				hasDataReadError = true
			}
			break
		}

		// Parse block header
		if len(blockData) < codec.RecordHeaderSize+codec.BlockHeaderRecordSize {
			logger.Ctx(ctx).Warn("block file too small",
				zap.String("blockFilePath", blockFilePath), zap.Int64("blockNumber", currentBlockID), zap.Int("blockSize", len(blockData)))
			break
		}

		headerData := blockData[:codec.RecordHeaderSize+codec.BlockHeaderRecordSize]
		record, err := codec.DecodeRecord(headerData)
		if err != nil || record.Type() != codec.BlockHeaderRecordType {
			logger.Ctx(ctx).Warn("failed to decode block header",
				zap.String("blockFilePath", blockFilePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			break
		}

		blockHeaderRecord := record.(*codec.BlockHeaderRecord)

		// Verify block data integrity
		blockContent := blockData[codec.RecordHeaderSize+codec.BlockHeaderRecordSize:]
		if err := codec.VerifyBlockDataIntegrity(blockHeaderRecord, blockContent); err != nil {
			logger.Ctx(ctx).Warn("block integrity check failed",
				zap.String("blockFilePath", blockFilePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(err))
			break
		}

		// decode block data to extract data records
		records, decodeErr := codec.DecodeRecordList(blockContent)
		if decodeErr != nil {
			logger.Ctx(ctx).Warn("failed to decode block records",
				zap.String("blockFilePath", blockFilePath),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(decodeErr))
			break
		}

		currentEntryID := blockHeaderRecord.FirstEntryID
		hasDataCollectedFromThisBlock := false
		reachLac := false

		for j := 0; j < len(records); j++ {
			if records[j].Type() != codec.DataRecordType {
				continue
			}
			if currentEntryID > currentLAC {
				reachLac = true
				break
			}
			if opt.StartEntryID <= currentEntryID && currentEntryID <= currentLAC {
				dr := records[j].(*codec.DataRecord)
				entry := &proto.LogEntry{
					SegId:   r.segId,
					EntryId: currentEntryID,
					Values:  dr.Payload,
				}
				entries = append(entries, entry)
				entriesCollected++
				readBytes += int64(len(dr.Payload))
				lastReadEntryID = currentEntryID
				hasDataCollectedFromThisBlock = true
			}
			currentEntryID++
		}

		logger.Ctx(ctx).Debug("extracted data from per-block file",
			zap.String("blockFilePath", blockFilePath),
			zap.Int64("blockNumber", currentBlockID),
			zap.Int64("blockFirstEntryID", blockHeaderRecord.FirstEntryID),
			zap.Int64("blockLastEntryID", blockHeaderRecord.LastEntryID),
			zap.Int64("totalCollectedEntries", entriesCollected),
			zap.Bool("hasDataCollectedFromThisBlock", hasDataCollectedFromThisBlock),
			zap.Bool("reachLac", reachLac),
			zap.Int64("lac", currentLAC))

		if hasDataCollectedFromThisBlock {
			lastBlockInfo = &codec.IndexRecord{
				BlockNumber:  int32(currentBlockID),
				StartOffset:  0, // Not meaningful for per-block files
				BlockSize:    uint32(len(blockData)),
				FirstEntryID: blockHeaderRecord.FirstEntryID,
				LastEntryID:  blockHeaderRecord.LastEntryID,
			}
		}

		if reachLac {
			break
		}
	}

	if len(entries) == 0 {
		logger.Ctx(ctx).Debug("no entry extracted from per-block files",
			zap.String("baseDir", r.baseDir),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("lac", currentLAC),
			zap.Int64("lastReadEntryID", lastReadEntryID))
		if !hasDataReadError {
			if r.isIncompleteFile.Load() && r.footer == nil && (opt.StartEntryID <= currentLAC || currentLAC == -1) {
				return nil, werr.ErrEntryNotFound.WithCauseErrMsg("some data less than LAC but can't read here now, retry later or try to read from other nodes")
			}
			if r.footer != nil && opt.StartEntryID <= r.footer.LAC {
				return nil, werr.ErrEntryNotFound.WithCauseErrMsg("some data less than footer's LAC but can't read here now, retry later or try to read from other nodes")
			}
			if !r.isIncompleteFile.Load() || r.footer != nil {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
		}
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("read data from per-block files completed",
			zap.String("baseDir", r.baseDir),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int("entriesReturned", len(entries)),
			zap.Any("lastBlockInfo", lastBlockInfo))
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

	batch := &proto.BatchReadResult{
		Entries:       entries,
		LastReadState: lastReadState,
	}
	return batch, nil
}

// Close closes the reader
func (r *StagedFileReaderAdv) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "Close")
	defer sp.End()
	logger.Ctx(ctx).Info("Closing segment reader", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId))

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.closed.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug("reader already closed", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId))
		return nil
	}

	if r.pool != nil {
		r.pool.Release()
	}
	logger.Ctx(ctx).Info("segment reader closed", zap.Int64("logId", r.logId), zap.Int64("segId", r.segId))
	return nil
}

// GetBlockIndexes Test Only
func (r *StagedFileReaderAdv) GetBlockIndexes() []*codec.IndexRecord {
	return r.blockIndexes
}

// GetFooter Test Only
func (r *StagedFileReaderAdv) GetFooter() *codec.FooterRecord {
	return r.footer
}

// GetTotalRecords Test Only
func (r *StagedFileReaderAdv) GetTotalRecords() uint32 {
	if r.footer != nil {
		return r.footer.TotalRecords
	}
	return 0
}

// GetTotalBlocks Test Only
func (r *StagedFileReaderAdv) GetTotalBlocks() int32 {
	if r.footer != nil {
		return r.footer.TotalBlocks
	}
	return 0
}

// readCompactedDataFromMinio reads data from minio for compacted segments using concurrent block fetching
func (r *StagedFileReaderAdv) readCompactedDataFromMinio(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startBlockOffset int64) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readCompactedDataFromMinio")
	defer sp.End()
	startTime := time.Now()

	// Soft limitation
	maxEntries := opt.MaxBatchEntries
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := r.maxBatchSize

	// Find the starting block
	startBlockIndex := -1
	for i, blockIndex := range r.blockIndexes {
		if blockIndex.BlockNumber >= int32(startBlockID) {
			startBlockIndex = i
			break
		}
	}

	if startBlockIndex == -1 {
		logger.Ctx(ctx).Debug("no blocks found starting from blockID",
			zap.Int64("startBlockID", startBlockID))
		return &proto.BatchReadResult{
			Entries:       make([]*proto.LogEntry, 0),
			LastReadState: nil,
		}, nil
	}

	// Determine which blocks to read based on limits
	blocksToRead := r.determineBlocksToRead(startBlockIndex, maxEntries, maxBytes)
	if len(blocksToRead) == 0 {
		logger.Ctx(ctx).Debug("no blocks to read",
			zap.Int64("startBlockID", startBlockID))
		return &proto.BatchReadResult{
			Entries:       make([]*proto.LogEntry, 0),
			LastReadState: nil,
		}, nil
	}

	logger.Ctx(ctx).Debug("starting concurrent block reading",
		zap.Int("totalBlocks", len(blocksToRead)),
		zap.Int64("maxEntries", maxEntries),
		zap.Int64("maxBytes", maxBytes))

	// Submit concurrent block reading tasks
	futures := make([]*conc.Future[*BlockReadResult], len(blocksToRead))
	for i, blockToRead := range blocksToRead {
		blockIndex := r.blockIndexes[startBlockIndex+i]
		// Create local copies to avoid closure capture issues
		localBlockToRead := blockToRead
		localBlockIndex := blockIndex
		localStartEntryID := opt.StartEntryID
		futures[i] = r.pool.Submit(func() (*BlockReadResult, error) {
			result := r.readAndExtractBlockConcurrently(ctx, localBlockToRead, localBlockIndex, localStartEntryID)
			return result, result.err
		})
	}

	// Collect results sequentially to maintain block order
	entries := make([]*proto.LogEntry, 0, maxEntries)
	var lastBlockInfo *codec.IndexRecord
	entriesCollected := int64(0)
	readBytes := int64(0)
	concurrentReadTime := time.Now()

	for i, future := range futures {
		result, err := future.Await()
		if err != nil {
			logger.Ctx(ctx).Warn("failed to await block reading task",
				zap.Int("blockIndex", i),
				zap.Error(err))
			// The data to be read must be sequential. If one block cannot be read here, it will be aborted.
			// Let subsequent retries continue at this breakpoint
			break
		}

		// Add entries from this block
		// Note: read the entire block at once to avoid having one block pulled multiple times
		for _, entry := range result.entries {
			entries = append(entries, entry)
			entriesCollected++
			readBytes += int64(len(entry.Values))
		}

		lastBlockInfo = result.blockInfo

		// Check if we've reached the limits
		if entriesCollected >= maxEntries || readBytes >= maxBytes {
			logger.Ctx(ctx).Debug("reached read limits, stopping",
				zap.Int64("entriesCollected", entriesCollected),
				zap.Int64("readBytes", readBytes),
				zap.Int("processedBlocks", i+1))
			break
		}
	}

	if entriesCollected == 0 {
		// No desired data found in current segment and the entire segment has been scanned.
		// Return EOF to let client proceed to next segment.
		logger.Ctx(ctx).Debug("no more entries to read",
			zap.Int64("logId", r.logId),
			zap.Int64("segId", r.segId),
			zap.Int64("startEntryId", opt.StartEntryID))
		return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
	}

	// Create last read state from last block info
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

	result := &proto.BatchReadResult{
		Entries:       entries,
		LastReadState: lastReadState,
	}

	logger.Ctx(ctx).Info("completed concurrent reading compacted data from minio",
		zap.Int("entriesCount", len(entries)),
		zap.Int64("readBytes", readBytes),
		zap.Int("totalBlocksProcessed", len(blocksToRead)),
		zap.Int64("concurrentReadMs", time.Since(concurrentReadTime).Milliseconds()),
		zap.Int64("totalCostMs", time.Since(startTime).Milliseconds()))

	return result, nil
}

// determineBlocksToRead determines which blocks should be read based on limits
func (r *StagedFileReaderAdv) determineBlocksToRead(startBlockIndex int, maxEntries int64, maxBytes int64) []*BlockToRead {
	blocksToRead := make([]*BlockToRead, 0)
	estimatedEntries := int64(0)
	estimatedBytes := int64(0)

	for i := startBlockIndex; i < len(r.blockIndexes); i++ {
		blockIndex := r.blockIndexes[i]

		// Estimate entries and bytes for this block
		// Use rough estimation: assume average entries per block and average entry size
		estimatedEntriesInBlock := blockIndex.LastEntryID - blockIndex.FirstEntryID + 1
		estimatedBytesInBlock := int64(blockIndex.BlockSize) // rough estimation

		// Check if adding this block would exceed limits
		if estimatedEntries+estimatedEntriesInBlock > maxEntries || estimatedBytes+estimatedBytesInBlock > maxBytes {
			if len(blocksToRead) == 0 {
				// Always read at least one block
				blockToRead := &BlockToRead{
					blockID: int64(blockIndex.BlockNumber),
					objKey:  r.getCompactedBlockKey(int64(blockIndex.BlockNumber)),
					size:    int64(blockIndex.BlockSize),
				}
				blocksToRead = append(blocksToRead, blockToRead)
			}
			break
		}

		blockToRead := &BlockToRead{
			blockID: int64(blockIndex.BlockNumber),
			objKey:  r.getCompactedBlockKey(int64(blockIndex.BlockNumber)),
			size:    int64(blockIndex.BlockSize),
		}
		blocksToRead = append(blocksToRead, blockToRead)

		estimatedEntries += estimatedEntriesInBlock
		estimatedBytes += estimatedBytesInBlock
	}

	return blocksToRead
}

// readAndExtractBlockConcurrently reads a block from minio and extracts entries concurrently
func (r *StagedFileReaderAdv) readAndExtractBlockConcurrently(ctx context.Context, blockToRead *BlockToRead, blockIndex *codec.IndexRecord, startEntryID int64) *BlockReadResult {
	result := &BlockReadResult{
		blockID:   blockToRead.blockID,
		size:      blockToRead.size,
		blockInfo: blockIndex,
	}

	// Read block data from minio
	blockData, err := r.readBlockFromMinioByKey(ctx, blockToRead.objKey, blockToRead.size)
	if err != nil {
		result.err = fmt.Errorf("failed to read block %d from minio: %w", blockToRead.blockID, err)
		return result
	}

	// Extract entries from block data
	entries, readBytes, err := r.extractEntriesFromBlockDataWithBytes(ctx, blockData, blockIndex, startEntryID)
	if err != nil {
		result.err = fmt.Errorf("failed to extract entries from block %d: %w", blockToRead.blockID, err)
		return result
	}

	result.entries = entries
	result.readBytes = readBytes
	return result
}

// readBlockFromMinioByKey reads a block from minio using the object key
func (r *StagedFileReaderAdv) readBlockFromMinioByKey(ctx context.Context, objKey string, objSize int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readBlockFromMinioByKey")
	defer sp.End()

	logger.Ctx(ctx).Debug("reading block from minio by key",
		zap.String("bucket", r.bucket),
		zap.String("objKey", objKey))

	// Read the block object
	reader, err := r.storageCli.GetObject(ctx, r.bucket, objKey, 0, objSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get block object: %w", err)
	}
	defer reader.Close()

	// Read all data
	blockData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read block data: %w", err)
	}

	logger.Ctx(ctx).Debug("successfully read block from minio by key",
		zap.String("objKey", objKey),
		zap.Int("dataSize", len(blockData)))

	return blockData, nil
}

// extractEntriesFromBlockDataWithBytes extracts log entries and returns read bytes count
func (r *StagedFileReaderAdv) extractEntriesFromBlockDataWithBytes(ctx context.Context, blockData []byte, blockIndex *codec.IndexRecord, startEntryID int64) ([]*proto.LogEntry, int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "extractEntriesFromBlockDataWithBytes")
	defer sp.End()

	// Decode all records from block data
	records, err := codec.DecodeRecordList(blockData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode block records: %w", err)
	}

	var entries []*proto.LogEntry
	currentEntryID := blockIndex.FirstEntryID
	readBytes := 0

	lac := r.footer.LAC
	// Extract data records and convert to log entries
	for _, record := range records {
		if record.Type() != codec.DataRecordType {
			continue
		}
		if startEntryID <= currentEntryID && currentEntryID <= lac {
			dr := record.(*codec.DataRecord)
			entry := &proto.LogEntry{
				SegId:   r.segId,
				EntryId: currentEntryID,
				Values:  dr.Payload,
			}
			entries = append(entries, entry)
			readBytes += len(dr.Payload)
		}
		currentEntryID++
	}

	logger.Ctx(ctx).Debug("extracted entries from block data with bytes",
		zap.Int32("blockNumber", blockIndex.BlockNumber),
		zap.Int("entriesCount", len(entries)),
		zap.Int64("firstEntryID", blockIndex.FirstEntryID),
		zap.Int64("lastEntryID", blockIndex.LastEntryID),
		zap.Int64("lac", lac),
		zap.Int("readBytes", readBytes))

	return entries, readBytes, nil
}

// extractEntriesFromBlockData extracts log entries from block data (legacy method, kept for compatibility)
func (r *StagedFileReaderAdv) extractEntriesFromBlockData(ctx context.Context, blockData []byte, blockIndex *codec.IndexRecord, startEntryID int64) ([]*proto.LogEntry, error) {
	entries, _, err := r.extractEntriesFromBlockDataWithBytes(ctx, blockData, blockIndex, startEntryID)
	return entries, err
}

func (r *StagedFileReaderAdv) UpdateLastAddConfirmed(ctx context.Context, lac int64) error {
	if lac <= r.lastAddConfirmed.Load() {
		// skip
		return nil
	}
	r.lacMu.Lock()
	defer r.lacMu.Unlock()
	if lac <= r.lastAddConfirmed.Load() {
		// skip
		return nil
	}
	r.lastAddConfirmed.Store(lac)
	return nil
}

// BlockToRead represents a block that needs to be read
type BlockToRead struct {
	blockID int64
	objKey  string
	size    int64
}

// BlockReadResult represents the result of reading a single block
type BlockReadResult struct {
	blockID   int64
	size      int64
	err       error
	blockInfo *codec.IndexRecord
	entries   []*proto.LogEntry
	readBytes int
}
