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
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"

	"github.com/zilliztech/woodpecker/common/logger"
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

// MinioFileReader implements AbstractFileReader for MinIO object storage
type MinioFileReader struct {
	mu             sync.RWMutex
	client         minioHandler.MinioHandler
	bucket         string
	segmentFileKey string

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
	// Get object size
	reader := &MinioFileReader{
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
		return reader, nil
	}
	if !reader.isCompleted.Load() && !reader.isFenced.Load() {
		// if uncompleted, try fetch all block infos
		existsBlocks, err := reader.prefetchAllBlockInfoOnce(ctx)
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
	return reader, nil
}

// Start by listing all once
func (f *MinioFileReader) prefetchAllBlockInfoOnce(ctx context.Context) (int, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "prefetchAllBlockInfoOnce")
	defer sp.End()
	listPrefix := fmt.Sprintf("%s/", f.segmentFileKey)
	if f.isCompacted.Load() {
		listPrefix = fmt.Sprintf("%s/m_", f.segmentFileKey)
	}
	objectCh := f.client.ListObjects(ctx, f.bucket, listPrefix, false, minio.ListObjectsOptions{
		Recursive: f.isCompacted.Load(), // only list compacted merged blocks if
	})
	existsBlocks := make([]*codec.IndexRecord, 0, 32)
	for objInfo := range objectCh {
		if objInfo.Err != nil {
			logger.Ctx(ctx).Warn("Error listing objects",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Error(objInfo.Err))
			return 0, objInfo.Err
		}
		if !strings.HasSuffix(objInfo.Key, ".blk") {
			continue
		}

		// Skip footer object
		if strings.HasSuffix(objInfo.Key, "/footer.blk") {
			logger.Ctx(ctx).Debug("Skipping footer object",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", objInfo.Key))
			continue
		}

		blockId, isMerged, parseErr := parseFilePartName(objInfo.Key)
		if parseErr != nil {
			logger.Ctx(ctx).Warn("Error parsing segment file block id from block key",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", objInfo.Key),
				zap.Error(parseErr))
			return 0, parseErr
		}
		logger.Ctx(ctx).Debug("Found segment file block",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", objInfo.Key),
			zap.Int64("blockSize", objInfo.Size),
			zap.Int64("blockID", blockId),
			zap.Bool("isMerged", isMerged))

		// get block last record
		blockLastRecord, getErr := f.getBlockLastRecord(ctx, objInfo.Key)
		if getErr != nil && werr.ErrSegmentFenced.Is(getErr) {
			continue
		}
		if getErr != nil {
			return 0, getErr
		}

		if isMerged == f.isCompacted.Load() {
			// if compacted, only merged blocks we need
			// if not compacted, not merged blocks we need
			existsBlocks = append(existsBlocks, &codec.IndexRecord{
				BlockNumber:       int32(blockId),
				StartOffset:       blockId,
				FirstRecordOffset: 0,
				FirstEntryID:      blockLastRecord.FirstEntryID,
				LastEntryID:       blockLastRecord.LastEntryID,
			})
		}
	}
	// ensure no hole in list - only include continuous blocks from 0
	sort.Slice(existsBlocks, func(i, j int) bool {
		return existsBlocks[i].BlockNumber < existsBlocks[j].BlockNumber
	})

	// Only keep continuous blocks starting from 0
	continuousBlocks := make([]*codec.IndexRecord, 0, len(existsBlocks))
	expectedBlockId := int32(0)

	for _, block := range existsBlocks {
		if block.BlockNumber == expectedBlockId {
			continuousBlocks = append(continuousBlocks, block)
			expectedBlockId++
			logger.Ctx(ctx).Debug("Added continuous block",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int32("blockID", block.BlockNumber),
				zap.Int64("firstEntryID", block.FirstEntryID),
				zap.Int64("lastEntryID", block.LastEntryID))
		} else {
			logger.Ctx(ctx).Debug("Found block hole, stopping at continuous sequence",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.Int32("blockID", block.BlockNumber),
				zap.Int32("expectedBlockId", expectedBlockId))
			break
		}
	}

	existsBlocks = continuousBlocks

	f.blocks = existsBlocks
	return len(existsBlocks), nil
}

func (f *MinioFileReader) tryReadFooterAndIndex(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "tryReadFooterAndIndex")
	defer sp.End()
	// Check if footer.blk exists
	footerKey := getFooterPartKey(f.segmentFileKey)
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
		recordSize := codec.RecordHeaderSize + 36 // IndexRecord payload size
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
	f.mu.Lock()
	defer f.mu.Unlock()
	var fetchedLastBlock *codec.IndexRecord

	blockID := int64(0)
	if len(f.blocks) > 0 {
		lastFrag := f.blocks[len(f.blocks)-1]
		blockID = int64(lastFrag.BlockNumber) + 1
		fetchedLastBlock = lastFrag
	}
	existsNewBlock := false
	for {
		blockKey := getPartKey(f.segmentFileKey, blockID)

		// check if the block exists in object storage
		_, err := f.client.StatObject(ctx, f.bucket, blockKey, minio.StatObjectOptions{})
		if err != nil && minioHandler.IsObjectNotExists(err) {
			break
		}
		if err != nil {
			// indicates that the prefetching of blocks has completed.
			//fmt.Println("object storage read block err: ", err)
			return existsNewBlock, nil, err
		}

		blockLastRecord, getErr := f.getBlockLastRecord(ctx, blockKey)
		if getErr != nil {
			return existsNewBlock, nil, getErr
		}

		fetchedLastBlock = &codec.IndexRecord{
			BlockNumber:       int32(blockID),
			StartOffset:       blockID,
			FirstRecordOffset: 0,
			FirstEntryID:      blockLastRecord.FirstEntryID,
			LastEntryID:       blockLastRecord.LastEntryID,
		}
		f.blocks = append(f.blocks, fetchedLastBlock)
		existsNewBlock = true
		logger.Ctx(ctx).Info("prefetch block info", zap.String("segmentFileKey", f.segmentFileKey), zap.Int64("lastBlockID", blockID))
		blockID++
	}

	logger.Ctx(ctx).Debug("prefetch block infos", zap.String("segmentFileKey", f.segmentFileKey), zap.Int("blocks", len(f.blocks)), zap.Int64("lastBlockID", blockID-1))
	return existsNewBlock, fetchedLastBlock, nil
}

func (f *MinioFileReader) getBlockLastRecord(ctx context.Context, blockKey string) (*codec.BlockLastRecord, error) {
	// get block last record
	getBlockLastRecordOpt := minio.GetObjectOptions{}
	setOptErr := getBlockLastRecordOpt.SetRange(0, -codec.RecordHeaderSize-codec.BlockLastRecordSize)
	if setOptErr != nil {
		logger.Ctx(ctx).Warn("Error setting range for block last record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(setOptErr))
		return nil, setOptErr
	}
	lastRecordObj, getErr := f.client.GetObject(ctx, f.bucket, blockKey, getBlockLastRecordOpt)
	if getErr != nil {
		logger.Ctx(ctx).Warn("Error getting block last record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(getErr))
		return nil, getErr
	}

	data, err := minioHandler.ReadObjectFull(ctx, lastRecordObj, codec.RecordHeaderSize+codec.BlockLastRecordSize)
	if err != nil {
		logger.Ctx(ctx).Warn("Error reading block last record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(err))
		return nil, err
	}
	// check if it is a fence object
	if len(data) != codec.RecordHeaderSize+codec.BlockLastRecordSize {
		objStat, stateErr := lastRecordObj.Stat()
		if stateErr != nil {
			logger.Ctx(ctx).Warn("Error getting block last record",
				zap.String("segmentFileKey", f.segmentFileKey),
				zap.String("blockKey", blockKey),
				zap.Error(stateErr))
			return nil, stateErr
		}
		if minioHandler.IsFencedObject(objStat) {
			// it means the object is fenced out, no more blocks data
			logger.Ctx(ctx).Info("segment file is fenced out", zap.String("segmentFileKey", f.segmentFileKey), zap.String("fencedBlockKey", objStat.Key))
			f.isFenced.Store(true)
			return nil, werr.ErrSegmentFenced
		}
	}

	lastRecord, err := codec.DecodeRecord(data)
	if err != nil {
		logger.Ctx(ctx).Warn("Error decoding block last record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(err))
		return nil, err
	}
	if lastRecord.Type() != codec.BlockLastRecordType {
		logger.Ctx(ctx).Warn("Error decoding block last record",
			zap.String("segmentFileKey", f.segmentFileKey),
			zap.String("blockKey", blockKey),
			zap.Error(err))
		return nil, err
	}
	blockLastRecord := lastRecord.(*codec.BlockLastRecord)
	return blockLastRecord, nil
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
	f.mu.RLock()
	defer f.mu.RUnlock()
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
			return -1, werr.ErrNoBlockFound
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

func (f *MinioFileReader) ReadNextBatch(ctx context.Context, opt storage.ReaderOpt) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "ReadNextBatch")
	defer sp.End()

	logger.Ctx(ctx).Debug("ReadNextBatch called",
		zap.Int64("startSequenceNum", opt.StartSequenceNum),
		zap.Int64("batchSize", opt.BatchSize),
		zap.Bool("isCompleted", f.isCompleted.Load()),
		zap.Bool("isFenced", f.isFenced.Load()))

	// For incomplete files, try to scan for new blocks if we don't have enough data
	if !f.isCompleted.Load() && !f.isFenced.Load() {
		if err := f.ensureSufficientBlocks(ctx, opt.StartSequenceNum, opt.BatchSize); err != nil {
			logger.Ctx(ctx).Warn("failed to scan for new blocks", zap.Error(err))
			// Continue with existing blocks even if scan fails
		}
	}

	// Get all available blocks
	f.mu.RLock()
	allBlocks := make([]*codec.IndexRecord, len(f.blocks))
	copy(allBlocks, f.blocks)
	f.mu.RUnlock()

	if len(allBlocks) == 0 {
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
			zap.Int64("startSequenceNum", opt.StartSequenceNum))

		if block.FirstEntryID <= opt.StartSequenceNum && opt.StartSequenceNum <= block.LastEntryID {
			startBlockIndex = i
			logger.Ctx(ctx).Debug("found matching block",
				zap.Int("startBlockIndex", startBlockIndex),
				zap.Int32("blockNumber", block.BlockNumber))
			break
		}
	}

	if startBlockIndex == -1 {
		logger.Ctx(ctx).Warn("no block found for start sequence number",
			zap.Int64("startSequenceNum", opt.StartSequenceNum),
			zap.Int("totalBlocks", len(allBlocks)))
		return nil, werr.ErrEntryNotFound
	}

	if opt.BatchSize == -1 {
		// Auto batch mode: return all data records from the single block containing the start sequence number
		logger.Ctx(ctx).Debug("using single block mode")
		return f.readSingleBlock(ctx, allBlocks[startBlockIndex], opt.StartSequenceNum)
	} else {
		// Specified batch size mode: read across multiple blocks if necessary to get the requested number of entries
		logger.Ctx(ctx).Debug("using multiple blocks mode",
			zap.Int("startBlockIndex", startBlockIndex),
			zap.Int64("batchSize", opt.BatchSize))
		return f.readMultipleBlocks(ctx, allBlocks, startBlockIndex, opt.StartSequenceNum, opt.BatchSize)
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

	f.mu.RLock()
	for _, block := range f.blocks {
		if block.FirstEntryID <= startSequenceNum && startSequenceNum <= block.LastEntryID {
			hasStartingBlock = true
		}
		if block.LastEntryID > lastAvailableEntryID {
			lastAvailableEntryID = block.LastEntryID
		}
	}
	f.mu.RUnlock()

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
		return getMergedPartKey(f.segmentFileKey, blockNumber)
	} else {
		return getPartKey(f.segmentFileKey, blockNumber)
	}
}

// readSingleBlock reads all data records from a single block starting from the specified entry ID
func (f *MinioFileReader) readSingleBlock(ctx context.Context, blockInfo *codec.IndexRecord, startSequenceNum int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readSingleBlock")
	defer sp.End()
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

	entries := make([]*proto.LogEntry, 0, 32)
	currentEntryID := blockInfo.FirstEntryID

	// Parse all data records in the block
	for j := 0; j < len(records); j++ {
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
		}
		currentEntryID++
	}

	logger.Ctx(ctx).Debug("read single block completed",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("blockNumber", int64(blockInfo.BlockNumber)),
		zap.Int64("startSequenceNum", startSequenceNum),
		zap.Int("entriesReturned", len(entries)))

	return entries, nil
}

// readMultipleBlocks reads across multiple blocks to get the specified number of entries
func (f *MinioFileReader) readMultipleBlocks(ctx context.Context, allBlocks []*codec.IndexRecord, startBlockIndex int, startSequenceNum int64, batchSize int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentReaderScope, "readMultipleBlocks")
	defer sp.End()
	entries := make([]*proto.LogEntry, 0, batchSize)
	entriesCollected := int64(0)

	for i := startBlockIndex; i < len(allBlocks) && entriesCollected < batchSize; i++ {
		blockInfo := allBlocks[i]
		blockObjKey := getPartKey(f.segmentFileKey, int64(blockInfo.BlockNumber))

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
			}
			currentEntryID++
		}
	}

	logger.Ctx(ctx).Debug("read multiple blocks completed",
		zap.String("segmentFileKey", f.segmentFileKey),
		zap.Int64("startSequenceNum", startSequenceNum),
		zap.Int64("requestedBatchSize", batchSize),
		zap.Int("entriesReturned", len(entries)))

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
