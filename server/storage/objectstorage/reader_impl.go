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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/conc"
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
	bucket         string
	segmentFileKey string
	logId          int64
	segmentId      int64
	logIdStr       string // for metrics only

	// file access
	mu     sync.RWMutex
	client minioHandler.MinioHandler

	// When no advanced options are provided, Prefetch block information from exists the footer
	blocks      []*codec.IndexRecord
	footer      *codec.FooterRecord
	isCompacted atomic.Bool // if the segment is compacted
	isCompleted atomic.Bool
	isFenced    atomic.Bool
	flags       atomic.Uint32 // stores uint16 value
	version     atomic.Uint32 // stores uint16 value

	// thread pool for concurrent block reading
	pool         *conc.Pool[*BlockReadResult]
	maxBatchSize int64

	// close state
	closed atomic.Bool
}

// NewMinioFileReaderAdv creates a new MinIO reader
func NewMinioFileReaderAdv(ctx context.Context, bucket string, baseDir string, logId int64, segId int64, client minioHandler.MinioHandler, maxBatchSize int64, maxFetchThreads int) (*MinioFileReaderAdv, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "NewMinioFileReaderAdv")
	defer sp.End()
	segmentFileKey := getSegmentFileKey(baseDir, logId, segId)
	logger.Ctx(ctx).Debug("creating new minio file reader", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId), zap.Int64("maxBatchSize", maxBatchSize), zap.Int("maxFetchThreads", maxFetchThreads))
	// Get object size
	reader := &MinioFileReaderAdv{
		logId:          logId,
		segmentId:      segId,
		logIdStr:       fmt.Sprintf("%d", logId),
		client:         client,
		bucket:         bucket,
		segmentFileKey: segmentFileKey,

		blocks: make([]*codec.IndexRecord, 0),
		footer: nil,

		maxBatchSize: maxBatchSize,
		pool:         conc.NewPool[*BlockReadResult](maxFetchThreads),
	}
	reader.flags.Store(0)
	reader.version.Store(codec.FormatVersion)
	reader.closed.Store(false)
	reader.isCompacted.Store(false)
	reader.isCompleted.Store(false)
	reader.isFenced.Store(false)

	// try parse footer if exists
	readFooterErr := reader.tryReadFooterAndIndex(ctx)
	if readFooterErr != nil {
		logger.Ctx(ctx).Warn("create new minio file readerAdv failed", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId))
		return nil, readFooterErr
	}

	logger.Ctx(ctx).Info("create new minio file readerAdv finish", zap.String("segmentFileKey", segmentFileKey), zap.Int64("logId", logId), zap.Int64("segId", segId), zap.Int64("maxBatchSize", maxBatchSize), zap.Int("maxFetchThreads", maxFetchThreads))
	return reader, nil
}

// FooterAndIndex represents the parsed footer and index data
type FooterAndIndex struct {
	footer *codec.FooterRecord
	blocks []*codec.IndexRecord
}

// readFooterAndIndexUnsafe reads and parses footer and index records without acquiring locks
func (f *MinioFileReaderAdv) readFooterAndIndexUnsafe(ctx context.Context) (*FooterAndIndex, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readFooterAndIndexUnsafe")
	defer sp.End()
	logger.Ctx(ctx).Debug("reading footer and block indexes", zap.String("segmentFileKey", f.segmentFileKey))

	// Check if footer.blk exists
	footerKey := getFooterBlockKey(f.segmentFileKey)
	statInfo, err := f.client.StatObject(ctx, f.bucket, footerKey, minio.StatObjectOptions{})
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			// no footer blk yet
			return nil, nil
		}
		return nil, err
	}

	// Read the entire footer.blk file
	footerObj, err := f.client.GetObject(ctx, f.bucket, footerKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer footerObj.Close()

	footerBlkData, err := minioHandler.ReadObjectFull(ctx, footerObj, statInfo.Size)
	if err != nil {
		return nil, err
	}

	logger.Ctx(ctx).Debug("read entire footer.blk",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("footerBlkSize", statInfo.Size),
		zap.Int("footerBlkDataLength", len(footerBlkData)))

	// Parse footer record from the end of the file
	if len(footerBlkData) < codec.RecordHeaderSize+codec.FooterRecordSize {
		return nil, fmt.Errorf("footer.blk too small: %d bytes", len(footerBlkData))
	}

	footerRecordStart := len(footerBlkData) - codec.RecordHeaderSize - codec.FooterRecordSize
	footerRecordData := footerBlkData[footerRecordStart:]

	footerRecord, err := codec.DecodeRecord(footerRecordData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode footer record: %w", err)
	}

	if footerRecord.Type() != codec.FooterRecordType {
		return nil, fmt.Errorf("expected footer record, got type %d", footerRecord.Type())
	}

	fr := footerRecord.(*codec.FooterRecord)

	// Parse index records sequentially from the beginning of the file
	indexData := footerBlkData[:footerRecordStart]
	refreshBlocks := make([]*codec.IndexRecord, 0, fr.TotalBlocks)

	logger.Ctx(ctx).Debug("parsing index records sequentially",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexDataLength", len(indexData)),
		zap.Int32("expectedTotalBlocks", fr.TotalBlocks))

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
		refreshBlocks = append(refreshBlocks, indexRecord)

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

	result := &FooterAndIndex{
		footer: fr,
		blocks: refreshBlocks,
	}

	logger.Ctx(ctx).Info("successfully parsed footer and index records",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int("indexRecordCount", len(refreshBlocks)),
		zap.Int32("expectedTotalBlocks", fr.TotalBlocks))

	return result, nil
}

func (f *MinioFileReaderAdv) tryReadFooterAndIndex(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryReadFooterAndIndex")
	defer sp.End()
	logger.Ctx(ctx).Debug("try to read footer and block indexes", zap.String("segmentFileKey", f.segmentFileKey))

	if f.isCompleted.Load() {
		// already parse a footer
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isCompleted.Load() {
		// double check if already parse a footer
		return nil
	}

	footerAndIndex, err := f.readFooterAndIndexUnsafe(ctx)
	if err != nil {
		return err
	}

	if footerAndIndex == nil {
		// no footer yet
		return nil
	}

	// Update internal state
	f.footer = footerAndIndex.footer
	f.blocks = footerAndIndex.blocks
	f.isCompacted.Store(codec.IsCompacted(f.footer.Flags))
	f.isCompleted.Store(true)
	f.version.Store(uint32(f.footer.Version))
	f.flags.Store(uint32(f.footer.Flags))

	return nil
}

// incrementally fetch new blocks as they come in
func (f *MinioFileReaderAdv) prefetchIncrementalBlockInfoUnsafe(ctx context.Context) (bool, *codec.IndexRecord, error) {
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
	_, lastBlockInfo, err := f.prefetchIncrementalBlockInfoUnsafe(ctx)
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

func (f *MinioFileReaderAdv) ReadNextBatchAdv(ctx context.Context, opt storage.ReaderOpt, lastReadBatchInfo *proto.LastReadState) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatchAdv")
	defer sp.End()
	logger.Ctx(ctx).Debug("ReadNextBatchAdv called",
		zap.Int64("startEntryID", opt.StartEntryID),
		zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
		zap.Bool("isCompacted", f.isCompacted.Load()),
		zap.Any("opt", opt),
		zap.Any("lastReadBatchInfo", lastReadBatchInfo))

	lastReadState := lastReadBatchInfo
	// apply adv options if possible
	if lastReadState != nil {
		isCompactedLast := codec.IsCompacted(uint16(lastReadState.Flags))
		isCompactedNow := codec.IsCompacted(uint16(f.flags.Load()))
		if isCompactedLast != isCompactedNow && isCompactedNow {
			logger.Ctx(ctx).Info("adv options reset due to segment state changed after last read",
				zap.Int64("logId", f.logId),
				zap.Int64("segId", f.segmentId),
				zap.Int64("startEntryID", opt.StartEntryID),
				zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
				zap.Bool("isCompactedLast", isCompactedLast),
				zap.Bool("isCompactedNow", isCompactedNow))
			// reset lastReadState, will try to read from merged blocks
			lastReadState = nil
		}
	} else {
		// try read footer and extract block index infos
		err := f.tryReadFooterAndIndex(ctx)
		if err != nil {
			return nil, err
		}
	}

	// read lock
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed.Load() {
		logger.Ctx(ctx).Info("ReadNextBatchAdv closed", zap.Int64("logId", f.logId), zap.Int64("segId", f.segmentId), zap.Int64("startEntryID", opt.StartEntryID), zap.Int64("maxBatchEntries", opt.MaxBatchEntries))
		return nil, werr.ErrFileReaderAlreadyClosed
	}

	// start to read batch from a certain point
	startBlockID := int64(0)
	if lastReadState != nil {
		// When we have lastReadBatchInfo, start from the block after the last read block
		startBlockID = int64(lastReadState.LastBlockId + 1)
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
			return nil, werr.ErrFileReaderEndOfFile
		}
	}

	return f.readDataBlocksUnsafe(ctx, opt, startBlockID)
}

func (f *MinioFileReaderAdv) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "Close")
	defer sp.End()
	logger.Ctx(ctx).Info("Closing segment reader", zap.Int64("logId", f.logId), zap.Int64("segId", f.segmentId))

	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.closed.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Debug("reader already closed", zap.Int64("logId", f.logId), zap.Int64("segId", f.segmentId))
		return nil
	}

	if f.pool != nil {
		f.pool.Release()
	}
	logger.Ctx(ctx).Info("segment reader closed", zap.Int64("logId", f.logId), zap.Int64("segId", f.segmentId))
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

func (f *MinioFileReaderAdv) readDataBlocksUnsafe(ctx context.Context, opt storage.ReaderOpt, startBlockID int64) (*proto.BatchReadResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readDataBlocks")
	defer sp.End()
	startTime := time.Now()

	logger.Ctx(ctx).Debug("read data blocks start", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("startBlockID", startBlockID), zap.Int64("startEntryId", opt.StartEntryID), zap.Int64("maxBatchEntries", opt.MaxBatchEntries))

	maxEntries := opt.MaxBatchEntries // soft count limit: Read the minimum number of blocks exceeding the count limit
	if maxEntries <= 0 {
		maxEntries = 100
	}
	maxBytes := f.maxBatchSize // default 16MB limit per batch
	currentBlockID := startBlockID

	// Final collected results
	allEntries := make([]*proto.LogEntry, 0)
	var lastBlockInfo *codec.IndexRecord
	totalReadBytes := 0
	totalCollectedSize := int64(0)
	hasDataReadError := false

	// Keep reading batches until we have data or reach end
	for {
		// 1. Read one batch of blocks (up to maxBytes)
		batchFutures, nextBlockID, batchRawSize, hasMoreBlocks, readBlockBatchErr := f.readBlockBatchUnsafe(ctx, currentBlockID, maxBytes, opt.StartEntryID)

		if readBlockBatchErr != nil {
			hasDataReadError = true
		}

		if len(batchFutures) == 0 {
			// No blocks to read
			break
		}

		logger.Ctx(ctx).Debug("submitted batch reading tasks",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("currentBlockID", currentBlockID),
			zap.Int("batchTasks", len(batchFutures)),
			zap.Int64("batchRawSize", batchRawSize),
			zap.Bool("hasDataReadError", hasDataReadError),
			zap.Bool("hasMoreBlocks", hasMoreBlocks))

		// 2. Collect and process batch results
		batchResults := make([]*BlockReadResult, 0, len(batchFutures))
		for _, future := range batchFutures {
			result := future.Value()
			batchResults = append(batchResults, result)
		}

		// 3. Sort batch results by blockID to maintain order
		sort.Slice(batchResults, func(i, j int) bool {
			return batchResults[i].blockID < batchResults[j].blockID
		})

		// 4. Process batch results and collect entries
		batchEntries := make([]*proto.LogEntry, 0)
		batchReadBytes := 0

		for _, result := range batchResults {
			if result.err != nil {
				logger.Ctx(ctx).Warn("Failed to process block",
					zap.String("segmentFileKey", f.segmentFileKey),
					zap.Int64("blockNumber", result.blockID),
					zap.Error(result.err))
				hasDataReadError = true
				break
			}

			batchEntries = append(batchEntries, result.entries...)
			batchReadBytes += result.readBytes
			if result.blockInfo != nil {
				lastBlockInfo = result.blockInfo
			}
		}

		// Accumulate batch results
		allEntries = append(allEntries, batchEntries...)
		totalReadBytes += batchReadBytes
		totalCollectedSize += int64(batchReadBytes)

		logger.Ctx(ctx).Debug("processed batch",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int("batchEntries", len(batchEntries)),
			zap.Int("batchReadBytes", batchReadBytes),
			zap.Int("totalEntries", len(allEntries)),
			zap.Int64("totalCollectedSize", totalCollectedSize))

		// Move to next batch
		currentBlockID = nextBlockID

		// Stop conditions:
		// 1. We have collected enough data (>= maxBytes)
		// 2. We have collected some data and no more blocks available
		// 3. We reached the end of available blocks
		// 4. We encounter expected read error, data incomplete verification error / network/service unavailable, etc.
		if totalCollectedSize >= int64(maxBytes) {
			logger.Ctx(ctx).Debug("reached max collected size limit",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("totalCollectedSize", totalCollectedSize),
				zap.Int64("maxBytes", int64(maxBytes)))
			break
		}

		// If we have some data and approaching batch size limit, we can also stop
		if len(allEntries) > 0 && int64(len(allEntries)) >= maxEntries {
			logger.Ctx(ctx).Debug("reached requested batch size",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int("totalEntries", len(allEntries)),
				zap.Int64("maxBatchEntries", maxEntries))
			break
		}

		// Stop if there was an error in this batch
		if hasDataReadError {
			break
		}

		// Stop if no more blocks to read
		if !hasMoreBlocks {
			logger.Ctx(ctx).Debug("no more blocks available",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int("totalEntries", len(allEntries)))
			break
		}
	}

	sp.AddEvent("all_batches_completed", trace.WithAttributes(attribute.Int64("elapsedTime", time.Since(startTime).Milliseconds())))

	// Check final results
	if len(allEntries) == 0 {
		logger.Ctx(ctx).Debug("no entry extracted after reading all available blocks",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("startEntryId", opt.StartEntryID),
			zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
			zap.Int("entriesReturned", len(allEntries)))

		// If no data read error (or error is compaction-related), determine if it is EOF
		if !hasDataReadError {
			// only if no data read error, determine if it is EOF
			if f.isCompleted.Load() || f.isFenced.Load() || f.isCompacted.Load() {
				return nil, werr.ErrFileReaderEndOfFile
			}
			if f.footer != nil || f.isFooterExists(ctx) {
				return nil, werr.ErrFileReaderEndOfFile
			}
		}
		// otherwise return EntryNotFound to let reader caller retry later
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg("no record extract")
	}

	logger.Ctx(ctx).Debug("read data blocks completed",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("startEntryId", opt.StartEntryID),
		zap.Int64("maxBatchEntries", opt.MaxBatchEntries),
		zap.Int("entriesReturned", len(allEntries)),
		zap.Int("totalReadBytes", totalReadBytes),
		zap.Int64("totalCollectedSize", totalCollectedSize),
		zap.Any("lastBlockInfo", lastBlockInfo))

	metrics.WpFileReadBatchBytes.WithLabelValues(f.logIdStr).Add(float64(totalReadBytes))
	metrics.WpFileReadBatchLatency.WithLabelValues(f.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	// Create batch with proper error handling for nil lastBlockInfo
	var lastReadState *proto.LastReadState
	if lastBlockInfo != nil {
		lastReadState = &proto.LastReadState{
			SegmentId:   f.segmentId,
			Flags:       f.flags.Load(),
			Version:     f.version.Load(),
			LastBlockId: lastBlockInfo.BlockNumber,
			BlockOffset: lastBlockInfo.StartOffset,
			BlockSize:   lastBlockInfo.BlockSize,
		}
	}

	batch := &proto.BatchReadResult{
		Entries:       allEntries,
		LastReadState: lastReadState,
	}
	return batch, nil
}

// fetchAndProcessBlock fetches and processes a single block
func (f *MinioFileReaderAdv) fetchAndProcessBlock(ctx context.Context, block BlockToRead, startEntryID int64) *BlockReadResult {
	result := &BlockReadResult{
		blockID: block.blockID,
		size:    block.size,
		err:     nil,
	}

	// Get the object
	blockObj, getErr := f.client.GetObject(ctx, f.bucket, block.objKey, minio.GetObjectOptions{})
	if getErr != nil {
		// Check if block not found - use centralized handling
		if minioHandler.IsObjectNotExists(getErr) {
			handleErr := f.handleBlockNotFoundByCompaction(ctx, block.blockID, "GetObject")
			if handleErr != nil {
				result.err = handleErr
				return result
			}
			// If handleErr is nil, block legitimately doesn't exist - fall through to return original error
		}
		result.err = getErr
		return result
	}

	// Read the full object data
	blockData, err := minioHandler.ReadObjectFull(ctx, blockObj, block.size)
	blockObj.Close() // release immediately after reading
	if err != nil {
		result.err = err
		return result
	}

	// Process the block data
	entries, readBytes, blockInfo, processErr := f.processBlockData(ctx, block.blockID, blockData, startEntryID)
	if processErr != nil {
		result.err = processErr
		return result
	}

	result.entries = entries
	result.readBytes = readBytes
	result.blockInfo = blockInfo

	return result
}

// processBlockData decodes block data and extracts entries
func (f *MinioFileReaderAdv) processBlockData(ctx context.Context, blockID int64, blockData []byte, startEntryID int64) ([]*proto.LogEntry, int, *codec.IndexRecord, error) {
	// Decode the block data
	records, decodeErr := codec.DecodeRecordList(blockData)
	if decodeErr != nil {
		logger.Ctx(ctx).Warn("Failed to decode block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", blockID),
			zap.Error(decodeErr))
		return nil, 0, nil, decodeErr
	}

	// Find BlockHeaderRecord and verify data integrity
	var blockHeaderRecord *codec.BlockHeaderRecord
	for _, record := range records {
		if record.Type() == codec.HeaderRecordType {
			// if no footer/no advOpt, it may start from 0, and header record will be read to init version&flags
			fileHeaderRecord := record.(*codec.HeaderRecord)
			f.version.Store(uint32(fileHeaderRecord.Version))
			f.flags.Store(uint32(fileHeaderRecord.Flags))
			continue
		}
		if record.Type() == codec.BlockHeaderRecordType {
			blockHeaderRecord = record.(*codec.BlockHeaderRecord)
			break
		}
	}
	if blockHeaderRecord == nil {
		err := fmt.Errorf("block header record not found")
		logger.Ctx(ctx).Warn("block header record not found",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", blockID))
		return nil, 0, nil, err
	}

	// Verify the block data integrity
	verifyBlockErr := f.verifyBlockDataIntegrity(ctx, blockHeaderRecord, blockID, blockData)
	if verifyBlockErr != nil {
		logger.Ctx(ctx).Warn("verify block data integrity failed",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", blockID),
			zap.Int32("readBlockNumber", blockHeaderRecord.BlockNumber),
			zap.Error(verifyBlockErr))
		return nil, 0, nil, verifyBlockErr
	}

	// Extract entries from this block
	entries := make([]*proto.LogEntry, 0)
	readBytes := 0
	currentEntryID := blockHeaderRecord.FirstEntryID

	// Parse all data records in the block
	for j := 0; j < len(records); j++ {
		if records[j].Type() != codec.DataRecordType {
			continue
		}
		// Only include entries from the start sequence number onwards
		if startEntryID <= currentEntryID {
			dr := records[j].(*codec.DataRecord)
			entry := &proto.LogEntry{
				SegId:   f.segmentId,
				EntryId: currentEntryID,
				Values:  dr.Payload,
			}
			entries = append(entries, entry)
			readBytes += len(dr.Payload)
		}
		currentEntryID++
	}

	blockInfo := &codec.IndexRecord{
		BlockNumber:  int32(blockID),
		StartOffset:  blockID,
		BlockSize:    uint32(len(blockData)),
		FirstEntryID: blockHeaderRecord.FirstEntryID,
		LastEntryID:  blockHeaderRecord.LastEntryID,
	}

	logger.Ctx(ctx).Debug("processed block data",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("blockNumber", blockID),
		zap.Int32("readBlockNumber", blockHeaderRecord.BlockNumber),
		zap.Int("extractedEntries", len(entries)),
		zap.Int("readBytes", readBytes))

	return entries, readBytes, blockInfo, nil
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

// readBlockBatch reads a single batch of blocks up to maxBytes raw size
// Returns: futures, nextBlockID, totalRawSize, hasMoreBlocks
func (f *MinioFileReaderAdv) readBlockBatchUnsafe(ctx context.Context, startBlockID int64, maxBytes int64, startEntryID int64) ([]*conc.Future[*BlockReadResult], int64, int64, bool, error) {
	futures := make([]*conc.Future[*BlockReadResult], 0)
	currentBlockID := startBlockID
	totalRawSize := int64(0)
	hasMoreBlocks := true
	var readBlockBatchErr error

	// Read blocks until we reach maxBytes raw size
	for totalRawSize < maxBytes {
		blockObjKey := f.getBlockObjectKey(currentBlockID)

		// StatObject to get size (very fast: 1-5ms)
		objInfo, statErr := f.client.StatObject(ctx, f.bucket, blockObjKey, minio.StatObjectOptions{})
		if statErr != nil {
			if minioHandler.IsObjectNotExists(statErr) {
				// Use centralized block-not-found handling
				handleErr := f.handleBlockNotFoundByCompaction(ctx, currentBlockID, "StatObject")
				if handleErr == nil {
					// Block legitimately doesn't exist - normal EOF
					logger.Ctx(ctx).Debug("block not found, no more data",
						zap.String("segmentFileKey", f.segmentFileKey),
						zap.Int64("blockNumber", currentBlockID))
					hasMoreBlocks = false
					break
				}
				readBlockBatchErr = handleErr
				break
			}
			logger.Ctx(ctx).Warn("Failed to stat block object",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("blockNumber", currentBlockID),
				zap.Error(statErr))
			readBlockBatchErr = statErr
			break
		}

		if minioHandler.IsFencedObject(objInfo) {
			logger.Ctx(ctx).Warn("object is fenced, stop reading",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int64("fenceBlockID", currentBlockID),
				zap.String("blockKey", blockObjKey))
			f.isFenced.Store(true)
			hasMoreBlocks = false
			break
		}

		// Submit task immediately
		blockInfo := BlockToRead{
			blockID: currentBlockID,
			objKey:  blockObjKey,
			size:    objInfo.Size,
		}

		future := f.pool.Submit(func() (*BlockReadResult, error) {
			return f.fetchAndProcessBlock(ctx, blockInfo, startEntryID), nil
		})

		futures = append(futures, future)
		totalRawSize += objInfo.Size
		currentBlockID++

		logger.Ctx(ctx).Debug("submitted reading task for block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockObjKey),
			zap.Bool("compacted", f.isCompacted.Load()),
			zap.Int64("blockNumber", currentBlockID-1),
			zap.Int64("blockSize", objInfo.Size),
			zap.Int64("totalRawSize", totalRawSize))
	}

	logger.Ctx(ctx).Debug("readBlockBatch completed",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("startBlockID", startBlockID),
		zap.Int64("nextBlockID", currentBlockID),
		zap.Int("batchSize", len(futures)),
		zap.Int64("totalRawSize", totalRawSize),
		zap.Bool("hasMoreBlocks", hasMoreBlocks),
		zap.Bool("hasReadBlockBatchErr", readBlockBatchErr == nil))

	return futures, currentBlockID, totalRawSize, hasMoreBlocks, readBlockBatchErr
}

// GetBlockIndexes Test only
func (f *MinioFileReaderAdv) GetBlockIndexes() []*codec.IndexRecord {
	// prefetch if not completed
	return f.blocks
}

// GetFooter Test only
func (f *MinioFileReaderAdv) GetFooter() *codec.FooterRecord {
	return f.footer
}

// GetTotalRecords Test only
func (f *MinioFileReaderAdv) GetTotalRecords() uint32 {
	if f.footer != nil {
		return f.footer.TotalRecords
	}
	return 0
}

// GetTotalBlocks Test only
func (f *MinioFileReaderAdv) GetTotalBlocks() int32 {
	if f.footer != nil {
		return f.footer.TotalBlocks
	}
	return 0
}

// shouldBlockExist checks if a block should exist according to current segment state
func (f *MinioFileReaderAdv) shouldBlockExist(blockID int64) bool {
	// For completed/compacted segments, check against footer's total blocks
	if f.footer != nil {
		return blockID < int64(f.footer.TotalBlocks)
	}
	// For active segments, assume block might exist (let retry determine final state)
	return true
}

// handleBlockNotFoundByCompaction centralizes block-not-found handling with compaction detection.
// It determines if a block's absence is due to compaction during read operations and returns
// appropriate errors or nil for legitimate cases.
func (f *MinioFileReaderAdv) handleBlockNotFoundByCompaction(ctx context.Context, blockID int64, operation string) error {
	// If block shouldn't exist according to current state, it's normal EOF
	if !f.shouldBlockExist(blockID) {
		return nil
	}

	// If already compacted, no need to check again
	if f.isCompacted.Load() {
		return fmt.Errorf("block %d not found during %s", blockID, operation)
	}

	logger.Ctx(ctx).Debug("Block not found, checking for compaction",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("blockNumber", blockID),
		zap.String("operation", operation))

	// Read footer to check if segment was compacted
	footerAndIndex, err := f.readFooterAndIndexUnsafe(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to read footer when block not found",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("blockNumber", blockID),
			zap.String("operation", operation),
			zap.Error(err))
		return err
	}

	// If compaction detected, trigger async state update and return specific error
	if footerAndIndex != nil && codec.IsCompacted(footerAndIndex.footer.Flags) {
		logger.Ctx(ctx).Info("Detected segment compaction during read",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.Int64("deletedBlockNumber", blockID),
			zap.String("operation", operation))

		go f.asyncUpdateReaderState(ctx, footerAndIndex)
		return werr.ErrFileReaderBlockDeletedByCompaction.WithCauseErrMsg(
			fmt.Sprintf("block %d deleted by compaction during %s", blockID, operation))
	}

	// Block not found but no compaction detected
	return nil
}

// asyncUpdateReaderState updates reader state asynchronously to avoid blocking read operations
func (f *MinioFileReaderAdv) asyncUpdateReaderState(ctx context.Context, footerAndIndex *FooterAndIndex) {
	logger.Ctx(ctx).Debug("Starting async update of reader state",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Bool("isCompacted", codec.IsCompacted(footerAndIndex.footer.Flags)))

	// Acquire lock for state update
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check if already updated by another goroutine
	if f.isCompacted.Load() {
		logger.Ctx(ctx).Debug("Reader state already updated by another goroutine",
			zap.String("segmentFileKey", f.segmentFileKey))
		return
	}

	// Update internal state
	f.footer = footerAndIndex.footer
	f.blocks = footerAndIndex.blocks
	f.isCompacted.Store(codec.IsCompacted(f.footer.Flags))
	f.isCompleted.Store(true)
	f.version.Store(uint32(f.footer.Version))
	f.flags.Store(uint32(f.footer.Flags))

	logger.Ctx(ctx).Info("Reader state updated asynchronously",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Bool("isCompacted", f.isCompacted.Load()),
		zap.Int("totalBlocks", len(f.blocks)))
}
