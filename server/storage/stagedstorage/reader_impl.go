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
	"sort"
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
// TODO: refactor to reuse code with MinioFileReaderAdv & LocalFileReaderAdv
type StagedFileReaderAdv struct {
	logId    int64
	segId    int64
	logIdStr string // for metrics only
	filePath string

	// object access, only used for compacted object access
	bucket          string
	rootPath        string
	storageCli      objectstorage.ObjectStorage
	pool            *conc.Pool[*BlockReadResult]
	maxFetchThreads int
	maxBatchSize    int64

	// file access
	mu   sync.RWMutex
	file *os.File

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
		zap.String("localBaseDir", localBaseDir),
		zap.Int64("logId", logId),
		zap.Int64("segId", segId),
		zap.String("rootPath", rootPath),
		zap.Int("maxFetchThreads", maxFetchThreads),
		zap.Int64("maxBatchSize", maxBatchSize))

	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
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
	reader := &StagedFileReaderAdv{
		logId:           logId,
		segId:           segId,
		logIdStr:        fmt.Sprintf("%d", logId),
		filePath:        filePath,
		maxBatchSize:    maxBatchSize,
		file:            file,
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

	logger.Ctx(ctx).Debug("staged file reader created successfully",
		zap.String("filePath", filePath),
		zap.Int64("currentFileSize", currentSize),
		zap.Bool("isIncompleteFile", reader.isIncompleteFile.Load()),
		zap.Int("blockIndexesCount", len(reader.blockIndexes)))
	return reader, nil
}

func (r *StagedFileReaderAdv) tryParseFooterAndIndexesIfExists(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseFooterAndIndexesIfExists")
	defer sp.End()
	logger.Ctx(ctx).Debug("try to read footer and block indexes with priority", zap.String("filePath", r.filePath))

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

	// Priority 1: Try to read footer from minio (compacted data)
	if err := r.tryParseMinioFooterUnsafe(ctx); err != nil {
		logger.Ctx(ctx).Debug("failed to parse minio footer, trying local file",
			zap.String("filePath", r.filePath),
			zap.Error(err))
	} else if !r.isIncompleteFile.Load() {
		// Successfully parsed minio footer
		logger.Ctx(ctx).Info("successfully parsed compacted footer from minio",
			zap.String("filePath", r.filePath),
			zap.Bool("isCompacted", r.isCompacted.Load()))
		return nil
	}

	// Priority 2: Try to read footer from staged local file
	return r.tryParseLocalFooterUnsafe(ctx)
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

	logger.Ctx(ctx).Debug("checking for compacted footer in minio",
		zap.String("bucket", r.bucket),
		zap.String("footerKey", footerKey))

	// Check if footer exists
	objSize, _, err := r.storageCli.StatObject(ctx, r.bucket, footerKey)
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			logger.Ctx(ctx).Debug("no compacted footer found in minio",
				zap.String("footerKey", footerKey))
			return fmt.Errorf("no footer in minio")
		}
		logger.Ctx(ctx).Warn("failed to stat footer object in minio",
			zap.String("footerKey", footerKey),
			zap.Error(err))
		return fmt.Errorf("failed to stat footer object: %w", err)
	}

	// Read the entire footer file
	reader, err := r.storageCli.GetObject(ctx, r.bucket, footerKey, 0, objSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to get footer object from minio",
			zap.String("footerKey", footerKey),
			zap.Error(err))
		return fmt.Errorf("failed to get footer object: %w", err)
	}
	defer reader.Close()

	footerData := make([]byte, objSize)
	_, err = io.ReadFull(reader, footerData)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read footer data from minio",
			zap.String("footerKey", footerKey),
			zap.Error(err))
		return fmt.Errorf("failed to read footer data: %w", err)
	}

	// Parse footer and indexes from minio data
	if err := r.parseMinioFooterDataUnsafe(ctx, footerData); err != nil {
		logger.Ctx(ctx).Warn("failed to parse minio footer data",
			zap.String("footerKey", footerKey),
			zap.Error(err))
		return fmt.Errorf("failed to parse minio footer data: %w", err)
	}

	// Mark as compacted
	r.isCompacted.Store(true)
	r.isIncompleteFile.Store(false)

	logger.Ctx(ctx).Info("successfully parsed compacted footer from minio",
		zap.String("footerKey", footerKey),
		zap.Int32("totalBlocks", r.footer.TotalBlocks),
		zap.Bool("isCompacted", codec.IsCompacted(r.footer.Flags)))

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

// tryParseLocalFooterUnsafe attempts to parse footer from staged local file (fallback)
func (r *StagedFileReaderAdv) tryParseLocalFooterUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryParseLocalFooterUnsafe")
	defer sp.End()

	logger.Ctx(ctx).Debug("trying to parse footer from staged local file", zap.String("filePath", r.filePath))

	stat, err := r.file.Stat()
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stat staged local file",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return fmt.Errorf("stat file: %w", err)
	}
	currentFileSize := stat.Size()

	// Check if file has minimum size for a footer (use V5 footer size as minimum)
	minFooterSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5
	if currentFileSize < int64(minFooterSize) {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("staged local file too small for footer, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", currentFileSize))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Try to read and parse footer from the end of the file using compatibility parsing
	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData, err := r.readAtUnsafe(ctx, currentFileSize-int64(maxFooterSize), maxFooterSize)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data from staged local file, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile.Store(true)
		return werr.ErrEntryNotFound.WithCauseErrMsg("read file failed") // client would retry later
	}

	// Try to parse footer with compatibility parsing
	footerRecord, err := codec.ParseFooterFromBytes(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to parse footer record from staged local file, performing raw scan",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		r.isIncompleteFile.Store(true)
		return nil
	}

	// Successfully found footer, parse using footer method
	r.footer = footerRecord
	r.isIncompleteFile.Store(false)
	r.isCompacted.Store(false) // Staged local file is not compacted
	r.version.Store(uint32(r.footer.Version))
	r.flags.Store(uint32(r.footer.Flags))

	logger.Ctx(ctx).Debug("found footer in staged local file, parsing index records",
		zap.String("filePath", r.filePath),
		zap.Int32("totalBlocks", r.footer.TotalBlocks))

	// Parse index records - they are located before the footer
	if r.footer.TotalBlocks > 0 {
		if err := r.parseIndexRecordsUnsafe(ctx, currentFileSize); err != nil {
			logger.Ctx(ctx).Debug("failed to decode index records from staged local file, performing raw scan",
				zap.String("filePath", r.filePath),
				zap.Error(err))
			r.isIncompleteFile.Store(true)
			r.footer = nil // reset footer since we can't parse indexes
			return nil
		}
	}

	logger.Ctx(ctx).Info("successfully parsed footer from staged local file",
		zap.String("filePath", r.filePath),
		zap.Int32("totalBlocks", r.footer.TotalBlocks),
		zap.Bool("isCompacted", false))

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

// parseIndexRecords parses all index records from the file
func (r *StagedFileReaderAdv) parseIndexRecordsUnsafe(ctx context.Context, currentFileSize int64) error {
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
			zap.Int64("lastBlockNumber", int64(lastReadBatchInfo.LastBlockId)),
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
func (r *StagedFileReaderAdv) readAtUnsafe(ctx context.Context, offset int64, length int) ([]byte, error) {
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
func (r *StagedFileReaderAdv) scanForAllBlockInfoUnsafe(ctx context.Context) error {
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

func (r *StagedFileReaderAdv) readDataBlocksUnsafe(ctx context.Context, opt storage.ReaderOpt, startBlockID int64, startBlockOffset int64) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

	// Check if this is a compacted segment - if so, read from minio
	if r.isCompacted.Load() {
		logger.Ctx(ctx).Debug("reading compacted data from minio",
			zap.String("filePath", r.filePath),
			zap.Int64("startBlockID", startBlockID))
		return r.readCompactedDataFromMinio(ctx, opt, startBlockID, startBlockOffset)
	}

	logger.Ctx(ctx).Debug("reading data from staged local file",
		zap.String("filePath", r.filePath),
		zap.Int64("startBlockID", startBlockID))

	// Soft limitation
	maxEntries := opt.MaxBatchEntries // soft count limit: Read the minimum number of blocks exceeding the count limit
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := r.maxBatchSize // soft bytes limit: Read the minimum number of blocks exceeding the bytes limit

	// read data result
	entries := make([]*proto.LogEntry, 0, maxEntries)
	var lastBlockInfo *codec.IndexRecord

	// get current lac
	currentLAC := r.lastAddConfirmed.Load()
	lastReadEntryID := opt.StartEntryID
	// read stats
	startOffset := startBlockOffset
	entriesCollected := int64(0)
	readBytes := int64(0)
	hasDataReadError := false

	// extract data from blocks
	for i := startBlockID; readBytes < maxBytes && entriesCollected < maxEntries; i++ {
		currentLAC = r.lastAddConfirmed.Load() // Obtain the lac before each block starts reading, more aggressive realtime read
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
		hasDataCollectedFromThisBlock := false
		reachLac := false
		// Parse all data records in the block even if it exceeds the maxSize or maxBytes, because one block should be read once
		for j := 0; j < len(records); j++ {
			if records[j].Type() != codec.DataRecordType {
				continue
			}
			if currentEntryID > currentLAC {
				reachLac = true
				break
			}
			// Only include entries from the start sequence number onwards
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

		logger.Ctx(ctx).Debug("Extracted data from block",
			zap.String("segmentPath", r.filePath),
			zap.Int64("blockNumber", currentBlockID),
			zap.Int32("recordBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Int64("blockFirstEntryID", blockHeaderRecord.FirstEntryID),
			zap.Int64("blockLastEntryID", blockHeaderRecord.LastEntryID),
			zap.Int64("currentEntryID", currentEntryID),
			zap.Int64("totalCollectedEntries", entriesCollected),
			zap.Int64("totalCollectedBytes", readBytes),
			zap.Bool("hasDataCollectedFromThisBlock", hasDataCollectedFromThisBlock),
			zap.Bool("reachLac", reachLac),
			zap.Int64("lac", currentLAC))

		if hasDataCollectedFromThisBlock {
			// only if collected data from this block, update lastBlockInfo of last read block
			lastBlockInfo = &codec.IndexRecord{
				BlockNumber:  int32(currentBlockID),
				StartOffset:  startOffset,
				BlockSize:    uint32(headersLen + blockDataLen),
				FirstEntryID: blockHeaderRecord.FirstEntryID,
				LastEntryID:  blockHeaderRecord.LastEntryID,
			}
		}

		if reachLac {
			// reach the LAC, stop this batch scan
			break
		}

		// Move to the next block
		startOffset += int64(headersLen + blockDataLen)
	}

	if len(entries) == 0 {
		logger.Ctx(ctx).Debug("no entry extracted",
			zap.String("filePath", r.filePath),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
			zap.Int64("lac", currentLAC),
			zap.Int64("lastReadEntryID", lastReadEntryID),
			zap.Int("entriesReturned", len(entries)))
		if !hasDataReadError {
			// only read without dataReadError, determine whether it is an EOF
			if !r.isIncompleteFile.Load() || r.footer != nil {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
			if r.isFooterExistsUnsafe(ctx) {
				return nil, werr.ErrFileReaderEndOfFile.WithCauseErrMsg("no more data")
			}
			if lastReadEntryID < currentLAC {
				// means maybe some data not LAC, should retry to read other nodes
				return nil, werr.ErrInvalidLACAlignment.WithCauseErrMsg("some data not LAC,try to read other nodes")
			}
		}
		// return entryNotFound to let read caller retry later
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	} else {
		logger.Ctx(ctx).Debug("read data blocks completed",
			zap.String("filePath", r.filePath),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
			zap.Int("entriesReturned", len(entries)),
			zap.Any("lastBlockInfo", lastBlockInfo))
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

// isFooterExistsUnsafe determines whether a valid footer record exists in the local file
func (r *StagedFileReaderAdv) isFooterExistsUnsafe(ctx context.Context) bool {
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

	// Check if file has minimum size for a footer (use V5 footer size as minimum)
	minFooterSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5
	if currentSize < int64(minFooterSize) {
		// File is too small to contain footer, might be empty or still being written
		logger.Ctx(ctx).Debug("file too small for footer, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Int64("fileSize", currentSize))
		return false
	}

	// Try to read and parse footer from the end of the file using compatibility parsing
	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData, err := r.readAtUnsafe(ctx, currentSize-int64(maxFooterSize), maxFooterSize)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to read footer data, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return false
	}

	// Try to parse footer with compatibility parsing
	_, err = codec.ParseFooterFromBytes(footerData)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to parse footer record, no footer exists yet",
			zap.String("filePath", r.filePath),
			zap.Error(err))
		return false
	}

	// footer exists
	return true
}

func (r *StagedFileReaderAdv) verifyBlockDataIntegrity(ctx context.Context, blockHeaderRecord *codec.BlockHeaderRecord, currentBlockID int64, dataRecordsBuffer []byte) error {
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

	if r.file != nil {
		if err := r.file.Close(); err != nil {
			return fmt.Errorf("close file: %w", err)
		}
		r.file = nil
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
