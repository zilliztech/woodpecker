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
// These functions are only used when:
// 1. Recovery detects an existing data.log file (no per-block files exist)
// 2. A legacy segment needs to be finalized
// 3. A legacy segment needs to be fenced
//
// BRANCHING POINTS (in writer_impl.go):
// - recoverFromBlockFilesUnsafe(): branches to recoverFromLegacyFileUnsafe() if data.log exists
// - Finalize(): branches to finalizeLegacyFileUnsafe() if legacyMode is true
// - Fence(): branches to fenceLegacyModeUnsafe() if legacyMode is true
//
// IMPORTANT: After branching, the legacy code path is completely separate and does NOT
// call any per-block mode methods. Only utility functions (path helpers, codec) are shared.
// =========================================================================================

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// recoverFromLegacyFileUnsafe recovers state from legacy data.log file format (backward compatibility)
func (w *LocalFileWriter) recoverFromLegacyFileUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverFromLegacyFile")
	defer sp.End()
	startTime := time.Now()

	// Check if file exists
	stat, err := os.Stat(w.segmentFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat file: %w", err)
	}

	if stat.Size() == 0 {
		return nil
	}

	// Open file for reading
	file, err := os.Open(w.segmentFilePath)
	if err != nil {
		return fmt.Errorf("open file for reading: %w", err)
	}
	defer file.Close()

	w.writtenBytes = stat.Size()
	w.lastModifiedTime = stat.ModTime()
	w.legacyMode.Store(true)

	// Try to parse the file to determine its state
	// First, check if it has a complete footer (finalized file)
	maxFooterSize := codec.GetMaxFooterReadSize()
	if w.writtenBytes >= int64(maxFooterSize) {
		footerData := make([]byte, maxFooterSize)
		_, err := file.ReadAt(footerData, w.writtenBytes-int64(len(footerData)))
		if err == nil {
			footerRecord, err := codec.ParseFooterFromBytes(footerData)
			if err == nil {
				// File is already finalized
				w.finalized.Store(true)
				if err := w.recoverBlocksFromLegacyFooterUnsafe(ctx, file, footerRecord); err != nil {
					return fmt.Errorf("recover from legacy footer: %w", err)
				}
				w.recovered.Store(true)
				logger.Ctx(ctx).Info("recovered from legacy finalized file", zap.String("segmentFilePath", w.segmentFilePath), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
				return nil
			}
		}
	}

	// File is incomplete, try to recover blocks from whole file scan
	if err := w.recoverBlocksFromLegacyFullScanUnsafe(ctx, file); err != nil {
		return fmt.Errorf("recover blocks: %w", err)
	}

	w.recovered.Store(true)
	logger.Ctx(ctx).Info("recovered from legacy incomplete file", zap.String("segmentFilePath", w.segmentFilePath), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return nil
}

// recoverBlocksFromLegacyFooterUnsafe recovers blocks from a finalized legacy file using footer
func (w *LocalFileWriter) recoverBlocksFromLegacyFooterUnsafe(ctx context.Context, file *os.File, footerRecord *codec.FooterRecord) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverBlocksFromLegacyFooter")
	defer sp.End()

	logger.Ctx(ctx).Info("recovering blocks from legacy footer", zap.String("segmentFilePath", w.segmentFilePath),
		zap.Uint64("indexOffset", footerRecord.IndexOffset), zap.Uint32("indexLength", footerRecord.IndexLength), zap.Int32("totalBlocks", footerRecord.TotalBlocks))

	if footerRecord.IndexOffset == 0 || footerRecord.IndexLength == 0 {
		return fmt.Errorf("invalid footer record: IndexOffset=%d, IndexLength=%d",
			footerRecord.IndexOffset, footerRecord.IndexLength)
	}

	// Read index section from file
	indexData := make([]byte, footerRecord.IndexLength)
	_, err := file.ReadAt(indexData, int64(footerRecord.IndexOffset))
	if err != nil {
		return fmt.Errorf("failed to read index section: %w", err)
	}

	// Parse index records
	offset := 0
	blockIndexes := make([]*codec.IndexRecord, 0, footerRecord.TotalBlocks)

	for offset < len(indexData) {
		if offset+codec.RecordHeaderSize > len(indexData) {
			break
		}

		record, err := codec.DecodeRecord(indexData[offset:])
		if err != nil {
			return fmt.Errorf("failed to decode index record at offset %d: %w", offset, err)
		}

		if record.Type() != codec.IndexRecordType {
			return fmt.Errorf("expected index record type %d, got %d at offset %d",
				codec.IndexRecordType, record.Type(), offset)
		}

		indexRecord := record.(*codec.IndexRecord)
		blockIndexes = append(blockIndexes, indexRecord)

		offset += codec.RecordHeaderSize + codec.IndexRecordSize
	}

	// Update writer state
	w.blockIndexes = blockIndexes

	if len(blockIndexes) > 0 {
		firstBlock := blockIndexes[0]
		lastBlock := blockIndexes[len(blockIndexes)-1]

		w.firstEntryID.Store(firstBlock.FirstEntryID)
		w.lastEntryID.Store(lastBlock.LastEntryID)
		w.currentBlockNumber.Store(int64(len(blockIndexes)))
		w.lastSubmittedFlushingBlockID.Store(int64(len(blockIndexes)) - 1)
		w.lastSubmittedFlushingEntryID.Store(lastBlock.LastEntryID)
	}

	return nil
}

// recoverBlocksFromLegacyFullScanUnsafe recovers blocks from an incomplete legacy file via full scan
func (w *LocalFileWriter) recoverBlocksFromLegacyFullScanUnsafe(ctx context.Context, file *os.File) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverBlocksFromLegacyFullScan")
	defer sp.End()
	startTime := time.Now()

	// Read entire file content
	data := make([]byte, w.writtenBytes)
	_, err := file.ReadAt(data, 0)
	if err != nil {
		return fmt.Errorf("read file content: %w", err)
	}

	// Parse records sequentially
	offset := 0
	currentBlockStart := int64(0)
	currentBlockNumber := int64(0)
	var headerFound bool
	currentEntryId := int64(0)
	var currentBlockFirstEntryID int64 = -1
	var currentBlockLastEntryID int64 = -1
	var inBlock bool = false

	for offset < len(data) {
		if offset+codec.RecordHeaderSize > len(data) {
			break
		}

		record, err := codec.DecodeRecord(data[offset:])
		if err != nil {
			break
		}

		recordSize := codec.RecordHeaderSize
		switch record.Type() {
		case codec.HeaderRecordType:
			headerFound = true
			w.headerWritten.Store(true)
			recordSize += 16 // HeaderRecord size

			headerRecord := record.(*codec.HeaderRecord)
			currentEntryId = headerRecord.FirstEntryID
			if w.firstEntryID.Load() == -1 {
				w.firstEntryID.Store(currentEntryId)
			}

		case codec.DataRecordType:
			dataRecord := record.(*codec.DataRecord)
			recordSize += len(dataRecord.Payload)

			if !inBlock {
				currentBlockStart = int64(offset)
				currentBlockFirstEntryID = currentEntryId
				inBlock = true
			}

			currentBlockLastEntryID = currentEntryId
			w.lastEntryID.Store(currentEntryId)
			currentEntryId++

		case codec.BlockHeaderRecordType:
			blockHeaderRecord := record.(*codec.BlockHeaderRecord)
			recordSize += codec.BlockHeaderRecordSize

			if inBlock {
				indexRecord := &codec.IndexRecord{
					BlockNumber:  int32(currentBlockNumber),
					StartOffset:  currentBlockStart,
					BlockSize:    uint32(int64(offset) - currentBlockStart),
					FirstEntryID: currentBlockFirstEntryID,
					LastEntryID:  currentBlockLastEntryID,
				}
				w.blockIndexes = append(w.blockIndexes, indexRecord)
				currentBlockNumber++
			}

			currentBlockStart = int64(offset)
			if currentBlockNumber != int64(blockHeaderRecord.BlockNumber) {
				currentBlockNumber = int64(blockHeaderRecord.BlockNumber)
			}
			currentBlockFirstEntryID = blockHeaderRecord.FirstEntryID
			currentBlockLastEntryID = blockHeaderRecord.LastEntryID
			inBlock = true

		default:
			break
		}

		offset += recordSize
	}

	// Handle last block
	if inBlock {
		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(currentBlockNumber),
			StartOffset:  currentBlockStart,
			BlockSize:    uint32(int64(offset) - currentBlockStart),
			FirstEntryID: currentBlockFirstEntryID,
			LastEntryID:  currentBlockLastEntryID,
		}
		w.blockIndexes = append(w.blockIndexes, indexRecord)
		currentBlockNumber++
	}

	w.currentBlockNumber.Store(currentBlockNumber)
	w.lastSubmittedFlushingBlockID.Store(currentBlockNumber - 1)
	if w.lastEntryID.Load() != -1 {
		w.lastSubmittedFlushingEntryID.Store(w.lastEntryID.Load())
	}

	if !headerFound {
		w.headerWritten.Store(false)
		w.writtenBytes = 0
		w.blockIndexes = w.blockIndexes[:0]
		w.firstEntryID.Store(-1)
		w.lastEntryID.Store(-1)
		w.currentBlockNumber.Store(0)
	}

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "recover_legacy", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "recover_legacy", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// IsLegacyMode returns true if writer recovered from legacy data.log format
func (w *LocalFileWriter) IsLegacyMode() bool {
	return w.legacyMode.Load()
}

// finalizeLegacyFileUnsafe finalizes a legacy data.log file by appending index and footer
func (w *LocalFileWriter) finalizeLegacyFileUnsafe(ctx context.Context, blockIndexes []*codec.IndexRecord, totalBlockSize uint64, lastEntryID int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "finalizeLegacyFile")
	defer sp.End()

	// Open file for appending
	file, err := os.OpenFile(w.segmentFilePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open legacy file for append: %w", err)
	}
	defer file.Close()

	// Get current file position (end of data, start of index)
	currentPos, err := file.Seek(0, 2) // Seek to end
	if err != nil {
		return fmt.Errorf("seek to end: %w", err)
	}
	indexOffset := uint64(currentPos)

	// Build and write index records
	var indexBuffer bytes.Buffer
	for _, indexRecord := range blockIndexes {
		encodedIndex := codec.EncodeRecord(indexRecord)
		indexBuffer.Write(encodedIndex)
	}
	indexLength := uint32(indexBuffer.Len())

	if _, err := file.Write(indexBuffer.Bytes()); err != nil {
		return fmt.Errorf("write index records: %w", err)
	}

	// Build and write footer record
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blockIndexes)),
		TotalRecords: uint32(len(blockIndexes)),
		TotalSize:    totalBlockSize + uint64(indexLength),
		IndexOffset:  indexOffset,
		IndexLength:  indexLength,
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          lastEntryID,
	}
	encodedFooter := codec.EncodeRecord(footer)

	if _, err := file.Write(encodedFooter); err != nil {
		return fmt.Errorf("write footer record: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync legacy file: %w", err)
	}

	logger.Ctx(ctx).Info("finalized legacy file", zap.String("segmentFilePath", w.segmentFilePath), zap.Uint64("indexOffset", indexOffset),
		zap.Uint32("indexLength", indexLength), zap.Int("blockCount", len(blockIndexes)), zap.Int64("lastEntryID", lastEntryID))

	return nil
}

// fenceLegacyModeUnsafe creates a fence flag file for legacy mode
func (w *LocalFileWriter) fenceLegacyModeUnsafe(ctx context.Context, startTime time.Time) (int64, error) {
	// Get fence flag file path
	fenceFlagPath := getFenceFlagPath(w.baseDir, w.logId, w.segmentId)

	// Create fence flag file
	fenceFile, err := os.Create(fenceFlagPath)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to create fence flag file", zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(err))
		return w.GetLastEntryId(ctx), fmt.Errorf("failed to create fence flag file %s: %w", fenceFlagPath, err)
	}

	// Write fence information to the file
	fenceInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nreason=manual_fence\ntype=async_writer\nmode=legacy\n",
		w.logId, w.segmentId, os.Getpid(), time.Now().Unix())

	_, writeErr := fenceFile.WriteString(fenceInfo)
	fenceFile.Close()

	if writeErr != nil {
		logger.Ctx(ctx).Warn("Failed to write fence info to file, but file created successfully", zap.String("fenceFlagPath", fenceFlagPath), zap.Error(writeErr))
	}

	// Mark as fenced and stop accepting writes
	w.fenced.Store(true)

	// wait if necessary
	_ = waitForFenceCheckIntervalIfLockExists(ctx, w.baseDir, w.logId, w.segmentId, fenceFlagPath)

	lastEntryId := w.GetLastEntryId(ctx)
	logger.Ctx(ctx).Info("Successfully created fence flag file (legacy mode)", zap.String("fenceFlagPath", fenceFlagPath),
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("lastEntryId", lastEntryId))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "fence_legacy", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "fence_legacy", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return lastEntryId, nil
}
