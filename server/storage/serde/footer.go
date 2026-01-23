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

	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// =============================================================================
// Footer File Structure
// =============================================================================
//
// The footer.blk file contains:
// 1. Index records (one per block, sequential)
// 2. Footer record (at the end)
//
// Layout:
// +-------------------+
// | IndexRecord 0     |
// +-------------------+
// | IndexRecord 1     |
// +-------------------+
// | ...               |
// +-------------------+
// | IndexRecord N-1   |
// +-------------------+
// | FooterRecord      |
// +-------------------+

// =============================================================================
// Footer and Index Result Structures
// =============================================================================

// FooterAndIndexes holds the parsed footer and index records.
type FooterAndIndexes struct {
	Footer  *codec.FooterRecord
	Indexes []*codec.IndexRecord
}

// =============================================================================
// Footer Serialization
// =============================================================================

// SerializeFooterAndIndexes serializes the index records and footer into a single buffer.
// Returns the serialized data and the footer record.
func SerializeFooterAndIndexes(blockIndexes []*codec.IndexRecord, lac int64) ([]byte, *codec.FooterRecord) {
	var buffer bytes.Buffer
	totalSize := uint64(0)
	lastEntryID := int64(-1)

	// Serialize all block index records
	for _, record := range blockIndexes {
		encodedRecord := codec.EncodeRecord(record)
		buffer.Write(encodedRecord)
		totalSize += uint64(record.BlockSize)
		if record.LastEntryID > lastEntryID {
			lastEntryID = record.LastEntryID
		}
	}
	indexLength := uint32(buffer.Len())

	// Use provided LAC if valid, otherwise use lastEntryID from blocks
	finalLAC := lac
	if finalLAC < 0 {
		finalLAC = lastEntryID
	}

	// Create footer record
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blockIndexes)),
		TotalRecords: uint32(len(blockIndexes)),
		TotalSize:    totalSize,
		IndexOffset:  0, // For per-block format, index offset is 0 (starts at beginning of footer.blk)
		IndexLength:  indexLength,
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          finalLAC,
	}

	// Serialize footer record
	encodedFooter := codec.EncodeRecord(footer)
	buffer.Write(encodedFooter)

	return buffer.Bytes(), footer
}

// SerializeFooterAndIndexesWithFlags is like SerializeFooterAndIndexes but allows setting flags.
func SerializeFooterAndIndexesWithFlags(blockIndexes []*codec.IndexRecord, lac int64, flags uint16) ([]byte, *codec.FooterRecord) {
	var buffer bytes.Buffer
	totalSize := uint64(0)
	lastEntryID := int64(-1)

	// Serialize all block index records
	for _, record := range blockIndexes {
		encodedRecord := codec.EncodeRecord(record)
		buffer.Write(encodedRecord)
		totalSize += uint64(record.BlockSize)
		if record.LastEntryID > lastEntryID {
			lastEntryID = record.LastEntryID
		}
	}
	indexLength := uint32(buffer.Len())

	// Use provided LAC if valid, otherwise use lastEntryID from blocks
	finalLAC := lac
	if finalLAC < 0 {
		finalLAC = lastEntryID
	}

	// Create footer record with flags
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blockIndexes)),
		TotalRecords: uint32(len(blockIndexes)),
		TotalSize:    totalSize,
		IndexOffset:  0,
		IndexLength:  indexLength,
		Version:      codec.FormatVersion,
		Flags:        flags,
		LAC:          finalLAC,
	}

	// Serialize footer record
	encodedFooter := codec.EncodeRecord(footer)
	buffer.Write(encodedFooter)

	return buffer.Bytes(), footer
}

// =============================================================================
// Footer Deserialization
// =============================================================================

// DeserializeFooterAndIndexes parses the footer.blk file content.
// Returns the footer record and all index records.
func DeserializeFooterAndIndexes(data []byte) (*FooterAndIndexes, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	// First, parse footer from the end
	maxFooterSize := codec.GetMaxFooterReadSize()
	if len(data) < maxFooterSize {
		maxFooterSize = len(data)
	}

	footerData := data[len(data)-maxFooterSize:]
	footer, err := codec.ParseFooterFromBytes(footerData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse footer: %w", err)
	}

	// Calculate where index data ends (before footer)
	actualFooterSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(footer.Version)
	indexEndOffset := len(data) - actualFooterSize

	// Parse index records from the beginning up to the footer
	indexData := data[:indexEndOffset]
	indexes, err := ParseIndexRecords(indexData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse index records: %w", err)
	}

	return &FooterAndIndexes{
		Footer:  footer,
		Indexes: indexes,
	}, nil
}

// ParseIndexRecords parses sequential index records from data.
func ParseIndexRecords(data []byte) ([]*codec.IndexRecord, error) {
	var indexes []*codec.IndexRecord
	offset := 0

	for offset < len(data) {
		if offset+codec.RecordHeaderSize > len(data) {
			break // Not enough data for a complete record header
		}

		record, err := codec.DecodeRecord(data[offset:])
		if err != nil {
			return indexes, fmt.Errorf("failed to decode record at offset %d: %w", offset, err)
		}

		if record.Type() != codec.IndexRecordType {
			// Reached non-index record, stop parsing
			break
		}

		indexRecord := record.(*codec.IndexRecord)
		indexes = append(indexes, indexRecord)

		// Move to next record
		offset += codec.RecordHeaderSize + codec.IndexRecordSize
	}

	return indexes, nil
}

// ParseFooterFromEnd parses only the footer record from the end of data.
func ParseFooterFromEnd(data []byte) (*codec.FooterRecord, error) {
	maxFooterSize := codec.GetMaxFooterReadSize()
	if len(data) < maxFooterSize {
		if len(data) < codec.RecordHeaderSize+codec.FooterRecordSizeV5 {
			return nil, fmt.Errorf("data too small: %d bytes, need at least %d",
				len(data), codec.RecordHeaderSize+codec.FooterRecordSizeV5)
		}
		maxFooterSize = len(data)
	}

	footerData := data[len(data)-maxFooterSize:]
	return codec.ParseFooterFromBytes(footerData)
}

// =============================================================================
// Footer Validation
// =============================================================================

// ValidateFooterAndIndexes validates that the footer matches the index records.
func ValidateFooterAndIndexes(footer *codec.FooterRecord, indexes []*codec.IndexRecord) error {
	if int32(len(indexes)) != footer.TotalBlocks {
		return fmt.Errorf("block count mismatch: footer says %d, got %d indexes",
			footer.TotalBlocks, len(indexes))
	}

	// Verify index records are sequential
	for i := 0; i < len(indexes)-1; i++ {
		if indexes[i].BlockNumber+1 != indexes[i+1].BlockNumber {
			return fmt.Errorf("non-sequential block numbers: %d followed by %d",
				indexes[i].BlockNumber, indexes[i+1].BlockNumber)
		}
	}

	return nil
}

// =============================================================================
// Legacy Footer Support (for data.log format)
// =============================================================================

// SerializeLegacyFooterAndIndexes serializes footer and indexes for legacy data.log format.
// In legacy format, index records are written at a specific offset in the file.
func SerializeLegacyFooterAndIndexes(blockIndexes []*codec.IndexRecord, indexOffset uint64, lac int64) ([]byte, *codec.FooterRecord) {
	var buffer bytes.Buffer
	totalSize := uint64(0)

	// Serialize all block index records
	for _, record := range blockIndexes {
		encodedRecord := codec.EncodeRecord(record)
		buffer.Write(encodedRecord)
		totalSize += uint64(record.BlockSize)
	}
	indexLength := uint32(buffer.Len())

	// Create footer record with legacy index offset
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blockIndexes)),
		TotalRecords: uint32(len(blockIndexes)),
		TotalSize:    totalSize + uint64(indexLength),
		IndexOffset:  indexOffset,
		IndexLength:  indexLength,
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          lac,
	}

	// Serialize footer record
	encodedFooter := codec.EncodeRecord(footer)
	buffer.Write(encodedFooter)

	return buffer.Bytes(), footer
}

// =============================================================================
// Index Record Utilities
// =============================================================================

// GetLastEntryIdFromIndexes returns the last entry ID from a list of index records.
func GetLastEntryIdFromIndexes(indexes []*codec.IndexRecord) int64 {
	if len(indexes) == 0 {
		return -1
	}
	return indexes[len(indexes)-1].LastEntryID
}

// GetFirstEntryIdFromIndexes returns the first entry ID from a list of index records.
func GetFirstEntryIdFromIndexes(indexes []*codec.IndexRecord) int64 {
	if len(indexes) == 0 {
		return -1
	}
	return indexes[0].FirstEntryID
}

// GetTotalBlockSizeFromIndexes calculates total block size from index records.
func GetTotalBlockSizeFromIndexes(indexes []*codec.IndexRecord) uint64 {
	var total uint64
	for _, idx := range indexes {
		total += uint64(idx.BlockSize)
	}
	return total
}

// FindBlockContainingEntry finds the index record that contains the given entry ID.
// Returns the index record and its position, or nil and -1 if not found.
func FindBlockContainingEntry(indexes []*codec.IndexRecord, entryId int64) (*codec.IndexRecord, int) {
	for i, idx := range indexes {
		if entryId >= idx.FirstEntryID && entryId <= idx.LastEntryID {
			return idx, i
		}
	}
	return nil, -1
}

// FindStartBlockIndex finds the first block that might contain entries >= startEntryId.
// Returns the index position, or len(indexes) if no such block exists.
func FindStartBlockIndex(indexes []*codec.IndexRecord, startEntryId int64) int {
	for i, idx := range indexes {
		if idx.LastEntryID >= startEntryId {
			return i
		}
	}
	return len(indexes)
}
