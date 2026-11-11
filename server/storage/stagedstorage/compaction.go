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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func (w *StagedFileWriter) compactionParallelUploads() int {
	if n := w.compactPolicyConfig.MaxParallelUploads; n > 0 {
		return n
	}
	return 4
}

func (w *StagedFileWriter) compactionTargetBytes() int64 {
	if n := w.compactPolicyConfig.MaxBytes.Int64(); n > 0 {
		return n
	}
	return 2 * 1024 * 1024
}

// CompactionMemoryEstimate bounds the streaming scan plus the output buffers held by
// parallel uploads and the builder waiting to submit. It needs no file I/O. A source
// block bounds the largest record, including a record larger than the output target.
// The factor of two allows for slice growth while appending records. Small segments
// are charged for their actual input, rather than a full pool of target-sized blocks.
func (w *StagedFileWriter) CompactionMemoryEstimate(expectedLastEntryId int64) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	var total, largest int64
	for _, block := range w.blockIndexes {
		if block.FirstEntryID > expectedLastEntryId {
			break
		}
		n := int64(block.BlockSize)
		total += n
		largest = max(largest, n)
		if block.LastEntryID >= expectedLastEntryId {
			break
		}
	}
	if total == 0 {
		return 0
	}
	builders := int64(w.compactionParallelUploads() + 1)
	output := min(total, builders*max(w.compactionTargetBytes(), largest))
	headers := min(builders, expectedLastEntryId+1) * int64(compactedPrefixSize(0))
	return 2 * (largest + output + headers)
}

func compactedPrefixSize(blockID int64) int {
	size := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
	if blockID == 0 {
		size += codec.RecordHeaderSize + codec.HeaderRecordSize
	}
	return size
}

// scanCompactionRecords validates the local blocks and yields their original encoded
// DataRecords in entry order. Source block headers and flush boundaries never reach
// the output builder. Only one source block is resident at a time.
func (w *StagedFileWriter) scanCompactionRecords(ctx context.Context, expected int64, visit func(int64, []byte) error) error {
	file, err := os.Open(w.segmentFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	next := int64(0)
	var source []byte
	for _, block := range w.blockIndexes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if next > expected {
			break
		}
		if block.FirstEntryID != next || block.LastEntryID < block.FirstEntryID {
			return fmt.Errorf("non-contiguous compaction input: expected entry %d, block range [%d,%d]", next, block.FirstEntryID, block.LastEntryID)
		}
		if cap(source) < int(block.BlockSize) {
			source = make([]byte, block.BlockSize)
		} else {
			source = source[:block.BlockSize]
		}
		if _, err := file.ReadAt(source, block.StartOffset); err != nil {
			return fmt.Errorf("read compaction block %d: %w", block.BlockNumber, err)
		}
		records, _, err := w.extractCompactedDataRecords(block, source, expected)
		if err != nil {
			return err
		}
		for len(records) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			// extractCompactedDataRecords already validated lengths, CRCs and types.
			n := codec.RecordHeaderSize + int(binary.LittleEndian.Uint32(records[5:9]))
			if err := visit(next, records[:n]); err != nil {
				return err
			}
			next++
			records = records[n:]
		}
	}
	if next != expected+1 {
		return fmt.Errorf("incomplete compaction input: next entry %d, expected %d", next, expected+1)
	}
	return nil
}

// readLocalFileAndUploadToMinio deterministically reblocks the confirmed DataRecord
// stream. MaxBytes counts encoded DataRecord bytes, excluding the fixed output
// headers. An exact fit stays in the block; an oversized record occupies one block.
// Neither source block boundaries nor upload timing may affect these decisions.
func (w *StagedFileWriter) readLocalFileAndUploadToMinio(ctx context.Context, targetBytes, expected int64) ([]*codec.IndexRecord, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uploads, uploadCtx := errgroup.WithContext(ctx)
	uploads.SetLimit(w.compactionParallelUploads())

	var indexes []*codec.IndexRecord
	var output []byte
	var firstEntry, lastEntry, totalSize int64

	flush := func() error {
		if len(output) == 0 {
			return nil
		}
		if err := uploadCtx.Err(); err != nil {
			return err
		}
		id := int64(len(indexes))
		prefix := compactedPrefixSize(id)
		data := output
		records := data[prefix:]
		flags := codec.SetCompacted(w.recoveredFooter.Flags)
		offset := 0
		if id == 0 {
			header := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: flags, FirstEntryID: firstEntry}
			offset += copy(data, codec.EncodeRecord(header))
		}
		header := &codec.BlockHeaderRecord{
			BlockNumber: int32(id), FirstEntryID: firstEntry, LastEntryID: lastEntry,
			BlockLength: uint32(len(records)), BlockCrc: crc32.ChecksumIEEE(records),
		}
		offset += copy(data[offset:], codec.EncodeRecord(header))
		if offset != prefix {
			return fmt.Errorf("compacted block %d header width mismatch: reserved %d, encoded %d", id, prefix, offset)
		}
		indexes = append(indexes, &codec.IndexRecord{
			BlockNumber: int32(id), StartOffset: 0, BlockSize: uint32(len(data)),
			FirstEntryID: firstEntry, LastEntryID: lastEntry,
		})
		totalSize += int64(len(data))
		// Go blocks when the upload limit is reached. The builder never queues an
		// unbounded number of block buffers, and Wait drains all uploads on errors.
		uploads.Go(func() error {
			if err := uploadCtx.Err(); err != nil {
				return err
			}
			key := w.getCompactedBlockKey(id)
			start := time.Now()
			if err := w.storageCli.PutObject(uploadCtx, w.bucket, key, bytes.NewReader(data), int64(len(data)), w.logNs, w.logIdStr); err != nil {
				return fmt.Errorf("upload merged block %d: %w", id, err)
			}
			metrics.WpFileCompactBytesWritten.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).Add(float64(len(data)))
			metrics.WpFileCompactLatency.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).Observe(float64(time.Since(start).Milliseconds()))
			metrics.WpObjectStorageStoredBytes.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).Add(float64(len(data)))
			metrics.WpObjectStorageStoredObjects.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).Inc()
			return nil
		})
		output = nil
		return nil
	}

	scanErr := w.scanCompactionRecords(uploadCtx, expected, func(entryID int64, record []byte) error {
		if len(output) > 0 && int64(len(output)-compactedPrefixSize(int64(len(indexes)))+len(record)) > targetBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if output == nil {
			firstEntry = entryID
			output = make([]byte, compactedPrefixSize(int64(len(indexes))))
		}
		output = append(output, record...)
		lastEntry = entryID
		return nil
	})
	if scanErr == nil {
		scanErr = flush()
	}
	if scanErr != nil {
		cancel()
	}
	uploadErr := uploads.Wait()
	if scanErr != nil && !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
		return nil, 0, scanErr
	}
	if uploadErr != nil {
		return nil, 0, uploadErr
	}
	if scanErr != nil {
		return nil, 0, scanErr
	}
	return indexes, totalSize, nil
}
