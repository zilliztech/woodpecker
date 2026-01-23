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

package serde

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// =============================================================================
// Fence Block Constants
// =============================================================================

const (
	// FenceBlockFirstEntryID is the special FirstEntryID value used to identify fence blocks
	FenceBlockFirstEntryID int64 = math.MinInt64
	// FenceBlockLastEntryID is the special LastEntryID value used to identify fence blocks
	FenceBlockLastEntryID int64 = math.MinInt64
)

// =============================================================================
// Block Entry - Common data structure for block entries
// =============================================================================

// BlockEntry represents a single entry in a block, with its data payload.
type BlockEntry struct {
	EntryId int64
	Data    []byte
}

// =============================================================================
// Block Serialization
// =============================================================================

// SerializeBlockData serializes a list of entries into raw block data (DataRecords only).
// Returns the serialized data and CRC32 checksum.
func SerializeBlockData(entries []*BlockEntry) ([]byte, uint32) {
	if len(entries) == 0 {
		return []byte{}, 0
	}

	var buffer bytes.Buffer
	for _, entry := range entries {
		dataRecord := &codec.DataRecord{
			Payload: entry.Data,
		}
		encodedRecord := codec.EncodeRecord(dataRecord)
		buffer.Write(encodedRecord)
	}

	blockData := buffer.Bytes()
	blockCrc := crc32.ChecksumIEEE(blockData)
	return blockData, blockCrc
}

// SerializeBlockWithHeader creates a complete block with BlockHeaderRecord followed by data.
// The blockData should be the output of SerializeBlockData.
// If includeFileHeader is true, a HeaderRecord is prepended (for the first block).
func SerializeBlockWithHeader(blockId int64, firstEntryId int64, lastEntryId int64,
	blockData []byte, blockCrc uint32, includeFileHeader bool) []byte {

	var result bytes.Buffer

	// Add HeaderRecord only for the first block
	if includeFileHeader {
		headerRecord := &codec.HeaderRecord{
			Version:      codec.FormatVersion,
			Flags:        0,
			FirstEntryID: firstEntryId,
		}
		encodedHeader := codec.EncodeRecord(headerRecord)
		result.Write(encodedHeader)
	}

	// Add BlockHeaderRecord
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  int32(blockId),
		FirstEntryID: firstEntryId,
		LastEntryID:  lastEntryId,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     blockCrc,
	}
	encodedBlockHeader := codec.EncodeRecord(blockHeaderRecord)
	result.Write(encodedBlockHeader)

	// Add data records
	result.Write(blockData)

	return result.Bytes()
}

// SerializeBlock is a convenience function that serializes entries into a complete block.
// If includeFileHeader is true, a HeaderRecord is prepended (for the first block).
func SerializeBlock(blockId int64, entries []*BlockEntry, includeFileHeader bool) []byte {
	if len(entries) == 0 {
		return []byte{}
	}

	firstEntryId := entries[0].EntryId
	lastEntryId := entries[len(entries)-1].EntryId

	blockData, blockCrc := SerializeBlockData(entries)
	return SerializeBlockWithHeader(blockId, firstEntryId, lastEntryId, blockData, blockCrc, includeFileHeader)
}

// =============================================================================
// Block Header Parsing
// =============================================================================

// ParseBlockHeader parses the BlockHeaderRecord from the beginning of block data.
// If the data starts with a HeaderRecord (first block), it skips over it.
// Returns the BlockHeaderRecord and the offset where data records begin.
func ParseBlockHeader(data []byte) (*codec.BlockHeaderRecord, int, error) {
	if len(data) < codec.RecordHeaderSize {
		return nil, 0, fmt.Errorf("data too short: %d bytes, need at least %d", len(data), codec.RecordHeaderSize)
	}

	offset := 0

	// Try to decode the first record
	record, err := codec.DecodeRecord(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode first record: %w", err)
	}

	// If it's a HeaderRecord, skip it and read the next record
	if record.Type() == codec.HeaderRecordType {
		offset += codec.RecordHeaderSize + codec.HeaderRecordSize
		if len(data) < offset+codec.RecordHeaderSize {
			return nil, 0, fmt.Errorf("data too short after HeaderRecord: %d bytes", len(data))
		}
		record, err = codec.DecodeRecord(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode record after HeaderRecord: %w", err)
		}
	}

	// Now we should have a BlockHeaderRecord
	if record.Type() != codec.BlockHeaderRecordType {
		return nil, 0, fmt.Errorf("expected BlockHeaderRecord, got record type %d", record.Type())
	}

	blockHeader := record.(*codec.BlockHeaderRecord)
	dataOffset := offset + codec.RecordHeaderSize + codec.BlockHeaderRecordSize

	return blockHeader, dataOffset, nil
}

// =============================================================================
// Block Integrity Verification
// =============================================================================

// VerifyBlockIntegrity verifies the integrity of block data using the BlockHeaderRecord.
// blockData should be the raw data records (after the block header).
func VerifyBlockIntegrity(header *codec.BlockHeaderRecord, blockData []byte) error {
	// Verify block length
	if uint32(len(blockData)) != header.BlockLength {
		return fmt.Errorf("block length mismatch: expected %d, got %d", header.BlockLength, len(blockData))
	}

	// Verify CRC
	calculatedCrc := crc32.ChecksumIEEE(blockData)
	if calculatedCrc != header.BlockCrc {
		return fmt.Errorf("block CRC mismatch: expected %d, got %d", header.BlockCrc, calculatedCrc)
	}

	return nil
}

// VerifyBlockFromData parses and verifies a complete block (header + data).
func VerifyBlockFromData(data []byte) (*codec.BlockHeaderRecord, error) {
	header, dataOffset, err := ParseBlockHeader(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block header: %w", err)
	}

	blockData := data[dataOffset:]
	if err := VerifyBlockIntegrity(header, blockData); err != nil {
		return nil, fmt.Errorf("block integrity verification failed: %w", err)
	}

	return header, nil
}

// =============================================================================
// Fence Block Detection
// =============================================================================

// IsFenceBlock checks if the given block header represents a fence block.
// Fence blocks use special sentinel values to mark the end of a segment.
func IsFenceBlock(header *codec.BlockHeaderRecord) bool {
	return header.FirstEntryID == FenceBlockFirstEntryID && header.LastEntryID == FenceBlockLastEntryID
}

// CreateFenceBlockHeader creates a BlockHeaderRecord for a fence block.
func CreateFenceBlockHeader(blockId int64) *codec.BlockHeaderRecord {
	return &codec.BlockHeaderRecord{
		BlockNumber:  int32(blockId),
		FirstEntryID: FenceBlockFirstEntryID,
		LastEntryID:  FenceBlockLastEntryID,
		BlockLength:  0,
		BlockCrc:     0,
	}
}

// SerializeFenceBlock creates a serialized fence block.
func SerializeFenceBlock(blockId int64) []byte {
	fenceHeader := CreateFenceBlockHeader(blockId)
	return codec.EncodeRecord(fenceHeader)
}

// =============================================================================
// Block Index Creation
// =============================================================================

// CreateIndexRecord creates an IndexRecord from a BlockHeaderRecord.
func CreateIndexRecord(header *codec.BlockHeaderRecord, startOffset int64) *codec.IndexRecord {
	return &codec.IndexRecord{
		BlockNumber:  header.BlockNumber,
		StartOffset:  startOffset,
		BlockSize:    header.BlockLength,
		FirstEntryID: header.FirstEntryID,
		LastEntryID:  header.LastEntryID,
	}
}

// CreateIndexRecordFromBlock creates an IndexRecord from block metadata.
func CreateIndexRecordFromBlock(blockId int64, startOffset int64, blockSize uint32,
	firstEntryId int64, lastEntryId int64) *codec.IndexRecord {
	return &codec.IndexRecord{
		BlockNumber:  int32(blockId),
		StartOffset:  startOffset,
		BlockSize:    blockSize,
		FirstEntryID: firstEntryId,
		LastEntryID:  lastEntryId,
	}
}

// =============================================================================
// Data Record Extraction
// =============================================================================

// ExtractDataRecords extracts only the data records from block data, skipping headers.
// Returns the raw encoded data records.
func ExtractDataRecords(blockData []byte) ([]byte, error) {
	records, err := codec.DecodeRecordList(blockData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode records: %w", err)
	}

	var dataRecords bytes.Buffer
	for _, record := range records {
		if record.Type() == codec.DataRecordType {
			encodedRecord := codec.EncodeRecord(record)
			dataRecords.Write(encodedRecord)
		}
	}

	return dataRecords.Bytes(), nil
}

// ExtractEntries extracts entries from block data, returning them with their entry IDs.
func ExtractEntries(blockData []byte, firstEntryId int64) ([]*BlockEntry, error) {
	records, err := codec.DecodeRecordList(blockData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode records: %w", err)
	}

	var entries []*BlockEntry
	currentEntryId := firstEntryId

	for _, record := range records {
		if record.Type() == codec.DataRecordType {
			dataRecord := record.(*codec.DataRecord)
			entries = append(entries, &BlockEntry{
				EntryId: currentEntryId,
				Data:    dataRecord.Payload,
			})
			currentEntryId++
		}
	}

	return entries, nil
}

// ParseBlockEntries parses a complete block file and extracts all entries.
// The block data should include the BlockHeaderRecord followed by DataRecords.
// Optionally may include a HeaderRecord at the beginning (for the first block).
func ParseBlockEntries(data []byte) ([]*BlockEntry, error) {
	// Parse the block header to get firstEntryId
	header, dataOffset, err := ParseBlockHeader(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block header: %w", err)
	}

	// Verify block integrity
	blockData := data[dataOffset:]
	if err := VerifyBlockIntegrity(header, blockData); err != nil {
		return nil, fmt.Errorf("block integrity verification failed: %w", err)
	}

	// Extract entries from the data records portion
	return ExtractEntries(blockData, header.FirstEntryID)
}
