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

package codec

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &HeaderRecord{
		Version:      FormatVersion,
		Flags:        0x1234,
		FirstEntryID: 1000,
	}

	// Test encoding
	encoded := EncodeRecord(original)
	assert.NotEmpty(t, encoded)

	// Test decoding
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)

	// Verify type and content
	headerRecord, ok := decoded.(*HeaderRecord)
	require.True(t, ok)
	assert.Equal(t, original.Version, headerRecord.Version)
	assert.Equal(t, original.Flags, headerRecord.Flags)
	assert.Equal(t, original.FirstEntryID, headerRecord.FirstEntryID)
}

func TestDataRecord_EncodeDecodeRoundTrip(t *testing.T) {
	testCases := []struct {
		name    string
		payload []byte
	}{
		{"empty payload", []byte{}},
		{"small payload", []byte("hello")},
		{"large payload", bytes.Repeat([]byte("test"), 1000)},
		{"binary payload", []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := &DataRecord{
				Payload: tc.payload,
			}

			// Test encoding
			encoded := EncodeRecord(original)
			assert.NotEmpty(t, encoded)

			// Test decoding
			decoded, err := DecodeRecord(encoded)
			require.NoError(t, err)

			// Verify type and content
			dataRecord, ok := decoded.(*DataRecord)
			require.True(t, ok)
			assert.Equal(t, original.Payload, dataRecord.Payload)
		})
	}
}

func TestIndexRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &IndexRecord{
		BlockNumber:  10,
		StartOffset:  1024,
		BlockSize:    2048576, // 2MB block size
		FirstEntryID: 500,
		LastEntryID:  600,
	}

	// Test encoding
	encoded := EncodeRecord(original)
	assert.NotEmpty(t, encoded)

	// Test decoding
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)

	// Verify type and content
	indexRecord, ok := decoded.(*IndexRecord)
	require.True(t, ok)
	assert.Equal(t, original.BlockNumber, indexRecord.BlockNumber)
	assert.Equal(t, original.StartOffset, indexRecord.StartOffset)
	assert.Equal(t, original.BlockSize, indexRecord.BlockSize)
	assert.Equal(t, original.FirstEntryID, indexRecord.FirstEntryID)
	assert.Equal(t, original.LastEntryID, indexRecord.LastEntryID)
}

func TestBlockHeaderRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &BlockHeaderRecord{
		BlockNumber:  42,
		FirstEntryID: 1000,
		LastEntryID:  2000,
		BlockLength:  4096,
		BlockCrc:     0x12345678,
	}

	// Test encoding
	encoded := EncodeRecord(original)
	assert.NotEmpty(t, encoded)

	// Test decoding
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)

	// Verify type and content
	blockLastRecord, ok := decoded.(*BlockHeaderRecord)
	require.True(t, ok)
	assert.Equal(t, original.BlockNumber, blockLastRecord.BlockNumber)
	assert.Equal(t, original.FirstEntryID, blockLastRecord.FirstEntryID)
	assert.Equal(t, original.LastEntryID, blockLastRecord.LastEntryID)
	assert.Equal(t, original.BlockLength, blockLastRecord.BlockLength)
	assert.Equal(t, original.BlockCrc, blockLastRecord.BlockCrc)
}

func TestFooterRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &FooterRecord{
		TotalBlocks:  100,
		TotalRecords: 5000,
		IndexOffset:  10240,
		IndexLength:  512,
		TotalSize:    2048576, // 2MB file size
		Version:      FormatVersion,
		Flags:        0x5678,
		LAC:          12345, // Set LAC for V6
	}

	// Test encoding
	encoded := EncodeRecord(original)
	assert.NotEmpty(t, encoded)

	// Test decoding
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)

	// Verify type and content
	footerRecord, ok := decoded.(*FooterRecord)
	require.True(t, ok)
	assert.Equal(t, original.TotalBlocks, footerRecord.TotalBlocks)
	assert.Equal(t, original.TotalRecords, footerRecord.TotalRecords)
	assert.Equal(t, original.IndexOffset, footerRecord.IndexOffset)
	assert.Equal(t, original.IndexLength, footerRecord.IndexLength)
	assert.Equal(t, original.TotalSize, footerRecord.TotalSize)
	assert.Equal(t, original.Version, footerRecord.Version)
	assert.Equal(t, original.Flags, footerRecord.Flags)
	assert.Equal(t, original.LAC, footerRecord.LAC)
}

func TestDecodeRecordList_MultipleRecords(t *testing.T) {
	// Create multiple records
	records := []Record{
		&HeaderRecord{Version: FormatVersion, Flags: 0x1234, FirstEntryID: 1000},
		&DataRecord{Payload: []byte("hello world")},
		&IndexRecord{BlockNumber: 1, StartOffset: 100, BlockSize: 2048576, FirstEntryID: 1000, LastEntryID: 1010},
		&BlockHeaderRecord{BlockNumber: 1, FirstEntryID: 1000, LastEntryID: 1010, BlockLength: 2048, BlockCrc: 0xABCDEF01},
		&FooterRecord{TotalBlocks: 1, TotalRecords: 2, TotalSize: 2048, IndexOffset: 200, IndexLength: 44, Version: FormatVersion, Flags: 0x5678, LAC: 12345},
	}

	// Encode all records into a single buffer
	var buf bytes.Buffer
	for _, record := range records {
		encoded := EncodeRecord(record)
		buf.Write(encoded)
	}

	// Decode all records
	decodedRecords, err := DecodeRecordList(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, len(records), len(decodedRecords))

	// Verify each record
	for i, decoded := range decodedRecords {
		switch original := records[i].(type) {
		case *HeaderRecord:
			headerRecord, ok := decoded.(*HeaderRecord)
			require.True(t, ok)
			assert.Equal(t, original.Version, headerRecord.Version)
			assert.Equal(t, original.Flags, headerRecord.Flags)
			assert.Equal(t, original.FirstEntryID, headerRecord.FirstEntryID)
		case *DataRecord:
			dataRecord, ok := decoded.(*DataRecord)
			require.True(t, ok)
			assert.Equal(t, original.Payload, dataRecord.Payload)
		case *IndexRecord:
			indexRecord, ok := decoded.(*IndexRecord)
			require.True(t, ok)
			assert.Equal(t, original.BlockNumber, indexRecord.BlockNumber)
			assert.Equal(t, original.StartOffset, indexRecord.StartOffset)
			assert.Equal(t, original.BlockSize, indexRecord.BlockSize)
			assert.Equal(t, original.FirstEntryID, indexRecord.FirstEntryID)
			assert.Equal(t, original.LastEntryID, indexRecord.LastEntryID)
		case *BlockHeaderRecord:
			blockLastRecord, ok := decoded.(*BlockHeaderRecord)
			require.True(t, ok)
			assert.Equal(t, original.BlockNumber, blockLastRecord.BlockNumber)
			assert.Equal(t, original.FirstEntryID, blockLastRecord.FirstEntryID)
			assert.Equal(t, original.LastEntryID, blockLastRecord.LastEntryID)
			assert.Equal(t, original.BlockLength, blockLastRecord.BlockLength)
			assert.Equal(t, original.BlockCrc, blockLastRecord.BlockCrc)
		case *FooterRecord:
			footerRecord, ok := decoded.(*FooterRecord)
			require.True(t, ok)
			assert.Equal(t, original.TotalBlocks, footerRecord.TotalBlocks)
			assert.Equal(t, original.TotalRecords, footerRecord.TotalRecords)
			assert.Equal(t, original.IndexOffset, footerRecord.IndexOffset)
			assert.Equal(t, original.IndexLength, footerRecord.IndexLength)
			assert.Equal(t, original.Version, footerRecord.Version)
			assert.Equal(t, original.Flags, footerRecord.Flags)
			if original.Version >= 6 {
				assert.Equal(t, original.LAC, footerRecord.LAC)
			}
		}
	}
}

func TestDecodeRecordList_EmptyBuffer(t *testing.T) {
	records, err := DecodeRecordList([]byte{})
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestDecodeRecordList_IncompleteBuffer(t *testing.T) {
	// Create multiple records and concatenate them
	records := []Record{
		&DataRecord{Payload: []byte("first record")},
		&DataRecord{Payload: []byte("second record")},
		&DataRecord{Payload: []byte("third record")},
	}

	var buf bytes.Buffer
	for _, record := range records {
		encoded := EncodeRecord(record)
		buf.Write(encoded)
	}

	// Test with truncated buffer that cuts off the last record
	fullBuffer := buf.Bytes()

	// Truncate to remove the last 10 bytes (making the last record incomplete)
	truncatedBuffer := fullBuffer[:len(fullBuffer)-10]

	decodedRecords, err := DecodeRecordList(truncatedBuffer)
	require.NoError(t, err)

	// Should return the first two complete records (the third is incomplete)
	assert.Equal(t, 2, len(decodedRecords))

	// Verify the first two records are correctly parsed
	for i := 0; i < 2; i++ {
		dataRecord, ok := decodedRecords[i].(*DataRecord)
		require.True(t, ok)
		originalData := records[i].(*DataRecord)
		assert.Equal(t, originalData.Payload, dataRecord.Payload)
	}
}

func TestDecodeRecordList_CorruptedRecord(t *testing.T) {
	// Create multiple records and concatenate them
	records := []Record{
		&DataRecord{Payload: []byte("first record")},
		&DataRecord{Payload: []byte("second record")},
		&DataRecord{Payload: []byte("third record")},
	}

	var buf bytes.Buffer
	for _, record := range records {
		encoded := EncodeRecord(record)
		buf.Write(encoded)
	}

	// Corrupt the third record by changing its CRC
	fullBuffer := buf.Bytes()

	// Find the start of the third record and corrupt its CRC
	// First two records should be parsed successfully
	firstRecordSize := len(EncodeRecord(records[0]))
	secondRecordSize := len(EncodeRecord(records[1]))
	thirdRecordStart := firstRecordSize + secondRecordSize

	// Corrupt the CRC of the third record
	corruptedBuffer := make([]byte, len(fullBuffer))
	copy(corruptedBuffer, fullBuffer)
	corruptedBuffer[thirdRecordStart] = 0xFF // Corrupt first byte of CRC

	decodedRecords, err := DecodeRecordList(corruptedBuffer)
	require.NoError(t, err)

	// Should return the first two complete records (the third has corrupted CRC)
	assert.Equal(t, 2, len(decodedRecords))

	// Verify the first two records are correctly parsed
	for i := 0; i < 2; i++ {
		dataRecord, ok := decodedRecords[i].(*DataRecord)
		require.True(t, ok)
		originalData := records[i].(*DataRecord)
		assert.Equal(t, originalData.Payload, dataRecord.Payload)
	}
}

func TestDecodeRecord_CorruptedCRC(t *testing.T) {
	original := &DataRecord{Payload: []byte("hello world")}
	encoded := EncodeRecord(original)

	// Corrupt the CRC
	encoded[0] = 0xFF

	_, err := DecodeRecord(encoded)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CRC32 mismatch")
}

func TestDecodeRecord_InvalidLength(t *testing.T) {
	testCases := []struct {
		name string
		buf  []byte
	}{
		{"too short for header", []byte{0x01, 0x02}},
		{"header only", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRecord(tc.buf)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "buffer too short")
		})
	}
}

func TestParseBlockIndexList(t *testing.T) {
	// Create multiple index records
	indexRecords := []*IndexRecord{
		{BlockNumber: 1, StartOffset: 100, BlockSize: 2048576, FirstEntryID: 1000, LastEntryID: 1010},
		{BlockNumber: 2, StartOffset: 200, BlockSize: 2048576, FirstEntryID: 1011, LastEntryID: 1020},
		{BlockNumber: 3, StartOffset: 300, BlockSize: 2048576, FirstEntryID: 1021, LastEntryID: 1030},
	}

	// Encode all index records
	var buf bytes.Buffer
	for _, record := range indexRecords {
		encoded := EncodeRecord(record)
		// Extract payload (skip record header)
		payload := encoded[RecordHeaderSize:]
		buf.Write(payload)
	}

	// Parse the index list
	parsed, err := ParseBlockIndexList(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, len(indexRecords), len(parsed))

	// Verify each index record
	for i, parsedRecord := range parsed {
		original := indexRecords[i]
		assert.Equal(t, original.BlockNumber, parsedRecord.BlockNumber)
		assert.Equal(t, original.StartOffset, parsedRecord.StartOffset)
		assert.Equal(t, original.BlockSize, parsedRecord.BlockSize)
		assert.Equal(t, original.FirstEntryID, parsedRecord.FirstEntryID)
		assert.Equal(t, original.LastEntryID, parsedRecord.LastEntryID)
	}
}

func TestParseBlockIndexList_EmptyBuffer(t *testing.T) {
	parsed, err := ParseBlockIndexList([]byte{})
	require.NoError(t, err)
	assert.Empty(t, parsed)
}

func TestParseBlockIndexList_IncompleteBuffer(t *testing.T) {
	// Create incomplete buffer (less than 36 bytes)
	buf := make([]byte, 35)

	_, err := ParseBlockIndexList(buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid index payload length")
}

func TestRecordTypes(t *testing.T) {
	testCases := []struct {
		name       string
		record     Record
		recordType byte
	}{
		{"HeaderRecord", &HeaderRecord{}, HeaderRecordType},
		{"DataRecord", &DataRecord{}, DataRecordType},
		{"IndexRecord", &IndexRecord{}, IndexRecordType},
		{"BlockHeaderRecord", &BlockHeaderRecord{}, BlockHeaderRecordType},
		{"FooterRecord", &FooterRecord{}, FooterRecordType},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.recordType, tc.record.Type())
		})
	}
}

func TestEdgeCases(t *testing.T) {
	t.Run("MaxValues", func(t *testing.T) {
		// Test with maximum values
		indexRecord := &IndexRecord{
			BlockNumber:  2147483647,          // max int32
			StartOffset:  9223372036854775807, // max int64
			BlockSize:    4294967295,          // max uint32
			FirstEntryID: 9223372036854775807,
			LastEntryID:  9223372036854775807,
		}

		encoded := EncodeRecord(indexRecord)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedIndex, ok := decoded.(*IndexRecord)
		require.True(t, ok)
		assert.Equal(t, indexRecord.BlockNumber, decodedIndex.BlockNumber)
		assert.Equal(t, indexRecord.StartOffset, decodedIndex.StartOffset)
		assert.Equal(t, indexRecord.BlockSize, decodedIndex.BlockSize)
		assert.Equal(t, indexRecord.FirstEntryID, decodedIndex.FirstEntryID)
		assert.Equal(t, indexRecord.LastEntryID, decodedIndex.LastEntryID)
	})

	t.Run("MinValues", func(t *testing.T) {
		// Test with minimum values
		indexRecord := &IndexRecord{
			BlockNumber:  -2147483648,          // min int32
			StartOffset:  -9223372036854775808, // min int64
			BlockSize:    0,                    // min uint32
			FirstEntryID: -9223372036854775808,
			LastEntryID:  -9223372036854775808,
		}

		encoded := EncodeRecord(indexRecord)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedIndex, ok := decoded.(*IndexRecord)
		require.True(t, ok)
		assert.Equal(t, indexRecord.BlockNumber, decodedIndex.BlockNumber)
		assert.Equal(t, indexRecord.StartOffset, decodedIndex.StartOffset)
		assert.Equal(t, indexRecord.BlockSize, decodedIndex.BlockSize)
		assert.Equal(t, indexRecord.FirstEntryID, decodedIndex.FirstEntryID)
		assert.Equal(t, indexRecord.LastEntryID, decodedIndex.LastEntryID)
	})
}

func TestMagicValidation(t *testing.T) {
	t.Run("InvalidHeaderMagic", func(t *testing.T) {
		// Create a payload with invalid magic
		payload := make([]byte, 16)
		payload[12] = 0xFF // Invalid magic

		_, err := ParseHeader(payload)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid format version")
	})

	t.Run("InvalidFooterMagic", func(t *testing.T) {
		// Create a payload with invalid magic
		payload := make([]byte, 36)
		payload[32] = 0xFF // Invalid magic

		_, err := ParseFooter(payload)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid format version")
	})
}

// TestIsCompacted tests the IsCompacted function
func TestIsCompacted(t *testing.T) {
	testCases := []struct {
		name     string
		flags    uint16
		expected bool
	}{
		{"not compacted - zero flags", 0x0000, false},
		{"not compacted - even flags", 0x0002, false},
		{"not compacted - large even flags", 0xFFFE, false},
		{"compacted - bit 0 set", 0x0001, true},
		{"compacted - bit 0 set with other bits", 0x0003, true},
		{"compacted - bit 0 set with many bits", 0x0005, true},
		{"compacted - all bits set", 0xFFFF, true},
		{"compacted - only bit 0 set", 0x0001, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsCompacted(tc.flags)
			assert.Equal(t, tc.expected, result, "IsCompacted(0x%04X) should return %v", tc.flags, tc.expected)
		})
	}
}

// TestSetCompacted tests the SetCompacted function
func TestSetCompacted(t *testing.T) {
	testCases := []struct {
		name     string
		flags    uint16
		expected uint16
	}{
		{"set on zero flags", 0x0000, 0x0001},
		{"set on already compacted", 0x0001, 0x0001},
		{"set on flags with other bits", 0x0002, 0x0003},
		{"set on flags with many bits", 0x0004, 0x0005},
		{"set on all bits except bit 0", 0xFFFE, 0xFFFF},
		{"set on all bits set", 0xFFFF, 0xFFFF},
		{"set on random flags", 0x1234, 0x1235},
		{"set on another random flags", 0x5678, 0x5679},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := SetCompacted(tc.flags)
			assert.Equal(t, tc.expected, result, "SetCompacted(0x%04X) should return 0x%04X", tc.flags, tc.expected)

			// Verify that the result is always compacted
			assert.True(t, IsCompacted(result), "SetCompacted result should always be compacted")
		})
	}
}

// TestCompactedFlagIntegration tests the integration of IsCompacted and SetCompacted
func TestCompactedFlagIntegration(t *testing.T) {
	t.Run("FooterRecord_CompactedFlag", func(t *testing.T) {
		// Test with FooterRecord to ensure it works in real scenarios
		original := &FooterRecord{
			TotalBlocks:  100,
			TotalRecords: 5000,
			TotalSize:    2048576,
			IndexOffset:  10240,
			IndexLength:  512,
			Version:      FormatVersion,
			Flags:        0x1234, // Not compacted initially
		}

		// Verify initially not compacted
		assert.False(t, IsCompacted(original.Flags))

		// Set compacted flag
		original.Flags = SetCompacted(original.Flags)
		assert.True(t, IsCompacted(original.Flags))

		// Test encode/decode round trip with compacted flag
		encoded := EncodeRecord(original)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		footerRecord, ok := decoded.(*FooterRecord)
		require.True(t, ok)
		assert.True(t, IsCompacted(footerRecord.Flags))
		assert.Equal(t, original.Flags, footerRecord.Flags)
	})

	t.Run("HeaderRecord_CompactedFlag", func(t *testing.T) {
		// Test with HeaderRecord as well
		original := &HeaderRecord{
			Version:      FormatVersion,
			Flags:        0x5678, // Not compacted initially
			FirstEntryID: 1000,
		}

		// Verify initially not compacted
		assert.False(t, IsCompacted(original.Flags))

		// Set compacted flag
		original.Flags = SetCompacted(original.Flags)
		assert.True(t, IsCompacted(original.Flags))

		// Test encode/decode round trip with compacted flag
		encoded := EncodeRecord(original)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		headerRecord, ok := decoded.(*HeaderRecord)
		require.True(t, ok)
		assert.True(t, IsCompacted(headerRecord.Flags))
		assert.Equal(t, original.Flags, headerRecord.Flags)
	})
}

// TestCompactedFlagBitManipulation tests edge cases of bit manipulation
func TestCompactedFlagBitManipulation(t *testing.T) {
	t.Run("BitOperations", func(t *testing.T) {
		// Test that SetCompacted only affects bit 0
		flags := uint16(0x1234)
		compactedFlags := SetCompacted(flags)

		// Check that only bit 0 is different
		assert.Equal(t, flags|1, compactedFlags)

		// Check that all other bits remain unchanged
		assert.Equal(t, flags&0xFFFE, compactedFlags&0xFFFE)
	})

	t.Run("IdempotentOperation", func(t *testing.T) {
		// Test that SetCompacted is idempotent
		flags := uint16(0x1234)
		compactedOnce := SetCompacted(flags)
		compactedTwice := SetCompacted(compactedOnce)

		assert.Equal(t, compactedOnce, compactedTwice)
		assert.True(t, IsCompacted(compactedOnce))
		assert.True(t, IsCompacted(compactedTwice))
	})

	t.Run("BitBoundaries", func(t *testing.T) {
		// Test with boundary values
		testValues := []uint16{0x0000, 0x0001, 0x7FFF, 0x8000, 0xFFFF}

		for _, val := range testValues {
			compacted := SetCompacted(val)
			assert.True(t, IsCompacted(compacted), "SetCompacted(0x%04X) should result in compacted flag", val)
			assert.Equal(t, val|1, compacted, "SetCompacted(0x%04X) should equal 0x%04X", val, val|1)
		}
	})
}

func BenchmarkEncodeRecord(b *testing.B) {
	testCases := []struct {
		name   string
		record Record
	}{
		{"HeaderRecord", &HeaderRecord{Version: 1, Flags: 0x1234, FirstEntryID: 1000}},
		{"DataRecord_Small", &DataRecord{Payload: []byte("hello world")}},
		{"DataRecord_Large", &DataRecord{Payload: bytes.Repeat([]byte("test"), 1000)}},
		{"IndexRecord", &IndexRecord{BlockNumber: 10, StartOffset: 1024, BlockSize: 2048576, FirstEntryID: 500, LastEntryID: 600}},
		{"BlockHeaderRecord", &BlockHeaderRecord{BlockNumber: 1, FirstEntryID: 1000, LastEntryID: 2000, BlockLength: 4096, BlockCrc: 0x12345678}},
		{"FooterRecord", &FooterRecord{TotalBlocks: 100, TotalRecords: 5000, TotalSize: 2048576, IndexOffset: 10240, IndexLength: 512, Version: FormatVersion, Flags: 0x5678, LAC: 12345}},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = EncodeRecord(tc.record)
			}
		})
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	testCases := []struct {
		name   string
		record Record
	}{
		{"HeaderRecord", &HeaderRecord{Version: 1, Flags: 0x1234, FirstEntryID: 1000}},
		{"DataRecord_Small", &DataRecord{Payload: []byte("hello world")}},
		{"DataRecord_Large", &DataRecord{Payload: bytes.Repeat([]byte("test"), 1000)}},
		{"IndexRecord", &IndexRecord{BlockNumber: 10, StartOffset: 1024, BlockSize: 2048576, FirstEntryID: 500, LastEntryID: 600}},
		{"BlockHeaderRecord", &BlockHeaderRecord{BlockNumber: 1, FirstEntryID: 1000, LastEntryID: 2000, BlockLength: 4096, BlockCrc: 0x12345678}},
		{"FooterRecord", &FooterRecord{TotalBlocks: 100, TotalRecords: 5000, TotalSize: 2048576, IndexOffset: 10240, IndexLength: 512, Version: FormatVersion, Flags: 0x5678, LAC: 12345}},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			encoded := EncodeRecord(tc.record)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeRecord(encoded)
			}
		})
	}
}

func BenchmarkDecodeRecordList(b *testing.B) {
	// Create a buffer with multiple records
	records := []Record{
		&HeaderRecord{Version: 1, Flags: 0x1234, FirstEntryID: 1000},
		&DataRecord{Payload: []byte("hello world")},
		&IndexRecord{BlockNumber: 1, StartOffset: 100, BlockSize: 2048576, FirstEntryID: 1000, LastEntryID: 1010},
		&BlockHeaderRecord{BlockNumber: 1, FirstEntryID: 1000, LastEntryID: 1010, BlockLength: 2048, BlockCrc: 0xABCDEF01},
		&FooterRecord{TotalBlocks: 1, TotalRecords: 2, IndexOffset: 200, IndexLength: 44, Version: FormatVersion, Flags: 0x5678, LAC: 12345},
	}

	var buf bytes.Buffer
	for _, record := range records {
		encoded := EncodeRecord(record)
		buf.Write(encoded)
	}

	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeRecordList(data)
	}
}

func BenchmarkParseBlockIndexList(b *testing.B) {
	// Create multiple index records
	indexRecords := make([]*IndexRecord, 100)
	for i := 0; i < 100; i++ {
		indexRecords[i] = &IndexRecord{
			BlockNumber:  int32(i),
			StartOffset:  int64(i * 1024),
			BlockSize:    uint32(2048576 + i*1024), // Varying block sizes
			FirstEntryID: int64(i * 1000),
			LastEntryID:  int64(i*1000 + 999),
		}
	}

	// Encode all index records
	var buf bytes.Buffer
	for _, record := range indexRecords {
		encoded := EncodeRecord(record)
		// Extract payload (skip record header)
		payload := encoded[RecordHeaderSize:]
		buf.Write(payload)
	}

	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseBlockIndexList(data)
	}
}

func TestVerifyBlockDataIntegrity(t *testing.T) {
	// Create test data
	testData := []byte("test block data content")
	blockLength := uint32(len(testData))
	blockCrc := crc32.ChecksumIEEE(testData)

	t.Run("ValidBlockData", func(t *testing.T) {
		blockHeaderRecord := &BlockHeaderRecord{
			BlockNumber:  1,
			FirstEntryID: 1,
			LastEntryID:  10,
			BlockLength:  blockLength,
			BlockCrc:     blockCrc,
		}

		err := VerifyBlockDataIntegrity(blockHeaderRecord, testData)
		assert.NoError(t, err)
	})

	t.Run("InvalidBlockLength", func(t *testing.T) {
		blockHeaderRecord := &BlockHeaderRecord{
			BlockNumber:  1,
			FirstEntryID: 1,
			LastEntryID:  10,
			BlockLength:  blockLength + 1, // Wrong length
			BlockCrc:     blockCrc,
		}

		err := VerifyBlockDataIntegrity(blockHeaderRecord, testData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "block length mismatch")
	})

	t.Run("InvalidBlockCrc", func(t *testing.T) {
		blockHeaderRecord := &BlockHeaderRecord{
			BlockNumber:  1,
			FirstEntryID: 1,
			LastEntryID:  10,
			BlockLength:  blockLength,
			BlockCrc:     blockCrc + 1, // Wrong CRC
		}

		err := VerifyBlockDataIntegrity(blockHeaderRecord, testData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "block CRC mismatch")
	})

	t.Run("EmptyBlockData", func(t *testing.T) {
		emptyData := []byte{}
		blockHeaderRecord := &BlockHeaderRecord{
			BlockNumber:  1,
			FirstEntryID: 1,
			LastEntryID:  1,
			BlockLength:  0,
			BlockCrc:     crc32.ChecksumIEEE(emptyData),
		}

		err := VerifyBlockDataIntegrity(blockHeaderRecord, emptyData)
		assert.NoError(t, err)
	})
}

func TestBlockHeaderRecordWithIntegrity(t *testing.T) {
	// Test the complete flow: create block data, calculate CRC, encode/decode, verify
	originalData := [][]byte{
		[]byte("first record data"),
		[]byte("second record data"),
		[]byte("third record data"),
	}

	// Create data records and serialize them
	var blockDataBuffer []byte
	for _, data := range originalData {
		dataRecord := &DataRecord{Payload: data}
		encodedRecord := EncodeRecord(dataRecord)
		blockDataBuffer = append(blockDataBuffer, encodedRecord...)
	}

	// Calculate block length and CRC
	blockLength := uint32(len(blockDataBuffer))
	blockCrc := crc32.ChecksumIEEE(blockDataBuffer)

	// Create block header record
	blockHeaderRecord := &BlockHeaderRecord{
		BlockNumber:  1,
		FirstEntryID: 100,
		LastEntryID:  102,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}

	// Encode and decode the block header record
	encoded := EncodeRecord(blockHeaderRecord)
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)

	decodedBlockHeader := decoded.(*BlockHeaderRecord)
	assert.Equal(t, blockHeaderRecord.BlockNumber, decodedBlockHeader.BlockNumber)
	assert.Equal(t, blockHeaderRecord.FirstEntryID, decodedBlockHeader.FirstEntryID)
	assert.Equal(t, blockHeaderRecord.LastEntryID, decodedBlockHeader.LastEntryID)
	assert.Equal(t, blockHeaderRecord.BlockLength, decodedBlockHeader.BlockLength)
	assert.Equal(t, blockHeaderRecord.BlockCrc, decodedBlockHeader.BlockCrc)

	// Verify block data integrity
	err = VerifyBlockDataIntegrity(decodedBlockHeader, blockDataBuffer)
	assert.NoError(t, err)

	// Test with corrupted data
	corruptedData := make([]byte, len(blockDataBuffer))
	copy(corruptedData, blockDataBuffer)
	if len(corruptedData) > 0 {
		corruptedData[0] ^= 0xFF // Corrupt first byte
	}

	err = VerifyBlockDataIntegrity(decodedBlockHeader, corruptedData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block CRC mismatch")
}

// TestGetMaxFooterReadSize tests the GetMaxFooterReadSize function
func TestGetMaxFooterReadSize(t *testing.T) {
	maxSize := GetMaxFooterReadSize()
	expectedSize := RecordHeaderSize + FooterRecordSizeV6
	assert.Equal(t, expectedSize, maxSize)
	assert.Equal(t, 53, maxSize) // 9 + 44 = 53
}

// TestFooterV5Compatibility tests compatibility with V5 footer format
func TestFooterV5Compatibility(t *testing.T) {
	t.Run("CreateAndParseV5Footer", func(t *testing.T) {
		// Create a V5 footer
		footerV5 := &FooterRecord{
			TotalBlocks:  10,
			TotalRecords: 100,
			TotalSize:    1048576,
			IndexOffset:  1000000,
			IndexLength:  512,
			Version:      5, // V5
			Flags:        FooterFlagCompacted,
			LAC:          -1, // Not used in V5
		}

		// Encode V5 footer
		encoded := EncodeRecord(footerV5)
		assert.Equal(t, RecordHeaderSize+FooterRecordSizeV5, len(encoded))

		// Decode V5 footer
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedFooter := decoded.(*FooterRecord)
		assert.Equal(t, footerV5.TotalBlocks, decodedFooter.TotalBlocks)
		assert.Equal(t, footerV5.TotalRecords, decodedFooter.TotalRecords)
		assert.Equal(t, footerV5.TotalSize, decodedFooter.TotalSize)
		assert.Equal(t, footerV5.IndexOffset, decodedFooter.IndexOffset)
		assert.Equal(t, footerV5.IndexLength, decodedFooter.IndexLength)
		assert.Equal(t, uint16(5), decodedFooter.Version)
		assert.Equal(t, FooterFlagCompacted, decodedFooter.Flags)
		assert.False(t, decodedFooter.HasLAC())
		assert.Equal(t, int64(-1), decodedFooter.LAC)
	})

	t.Run("ParseV5FooterWithParseFooterFromBytes", func(t *testing.T) {
		// Create a V5 footer manually
		footerV5 := &FooterRecord{
			TotalBlocks:  5,
			TotalRecords: 50,
			TotalSize:    524288,
			IndexOffset:  500000,
			IndexLength:  256,
			Version:      5,
			Flags:        0,
		}

		encoded := EncodeRecord(footerV5)

		// Parse using ParseFooterFromBytes
		parsed, err := ParseFooterFromBytes(encoded)
		require.NoError(t, err)

		assert.Equal(t, footerV5.TotalBlocks, parsed.TotalBlocks)
		assert.Equal(t, uint16(5), parsed.Version)
		assert.False(t, parsed.HasLAC())
		assert.Equal(t, int64(-1), parsed.LAC)
	})
}

// TestFooterV6Compatibility tests the V6 footer format with LAC support
func TestFooterV6Compatibility(t *testing.T) {
	t.Run("CreateAndParseV6FooterWithLAC", func(t *testing.T) {
		// Create a V6 footer with LAC
		footerV6 := &FooterRecord{
			TotalBlocks:  20,
			TotalRecords: 200,
			TotalSize:    2097152,
			IndexOffset:  2000000,
			IndexLength:  1024,
			Version:      6, // V6
			Flags:        FooterFlagCompacted,
			LAC:          12345,
		}

		// Encode V6 footer
		encoded := EncodeRecord(footerV6)
		assert.Equal(t, RecordHeaderSize+FooterRecordSizeV6, len(encoded))

		// Decode V6 footer
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedFooter := decoded.(*FooterRecord)
		assert.Equal(t, footerV6.TotalBlocks, decodedFooter.TotalBlocks)
		assert.Equal(t, footerV6.TotalRecords, decodedFooter.TotalRecords)
		assert.Equal(t, footerV6.TotalSize, decodedFooter.TotalSize)
		assert.Equal(t, footerV6.IndexOffset, decodedFooter.IndexOffset)
		assert.Equal(t, footerV6.IndexLength, decodedFooter.IndexLength)
		assert.Equal(t, uint16(6), decodedFooter.Version)
		assert.Equal(t, FooterFlagCompacted, decodedFooter.Flags)
		assert.True(t, decodedFooter.HasLAC())
		assert.Equal(t, int64(12345), decodedFooter.LAC)
	})

	t.Run("CreateAndParseV6FooterWithoutLAC", func(t *testing.T) {
		// Create a V6 footer without valid LAC value
		footerV6 := &FooterRecord{
			TotalBlocks:  15,
			TotalRecords: 150,
			TotalSize:    1572864,
			IndexOffset:  1500000,
			IndexLength:  768,
			Version:      6,
			Flags:        FooterFlagCompacted,
			LAC:          -1, // -1 indicates no valid LAC
		}

		// Encode and decode
		encoded := EncodeRecord(footerV6)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedFooter := decoded.(*FooterRecord)
		assert.Equal(t, uint16(6), decodedFooter.Version)
		assert.False(t, decodedFooter.HasLAC())
		// LAC is physically present in V6 format but logically invalid when < 0
		assert.Equal(t, int64(-1), decodedFooter.LAC)
	})

	t.Run("ParseV6FooterWithParseFooterFromBytes", func(t *testing.T) {
		// Create a V6 footer with LAC
		footerV6 := &FooterRecord{
			TotalBlocks:  25,
			TotalRecords: 250,
			TotalSize:    2621440,
			IndexOffset:  2500000,
			IndexLength:  1280,
			Version:      6,
			Flags:        0,
			LAC:          67890,
		}

		encoded := EncodeRecord(footerV6)

		// Parse using ParseFooterFromBytes
		parsed, err := ParseFooterFromBytes(encoded)
		require.NoError(t, err)

		assert.Equal(t, footerV6.TotalBlocks, parsed.TotalBlocks)
		assert.Equal(t, uint16(6), parsed.Version)
		assert.True(t, parsed.HasLAC())
		assert.Equal(t, int64(67890), parsed.LAC)
	})
}

// TestParseFooterFromBytes tests the main compatibility parsing function
func TestParseFooterFromBytes(t *testing.T) {
	t.Run("ParseBothV5AndV6FromBuffer", func(t *testing.T) {
		// Create both V5 and V6 footers
		footerV5 := &FooterRecord{
			TotalBlocks:  3,
			TotalRecords: 30,
			TotalSize:    327680,
			IndexOffset:  300000,
			IndexLength:  128,
			Version:      5,
			Flags:        0,
		}

		footerV6 := &FooterRecord{
			TotalBlocks:  6,
			TotalRecords: 60,
			TotalSize:    655360,
			IndexOffset:  600000,
			IndexLength:  256,
			Version:      6,
			Flags:        0,
			LAC:          99999,
		}

		// Test V5
		encodedV5 := EncodeRecord(footerV5)
		parsedV5, err := ParseFooterFromBytes(encodedV5)
		require.NoError(t, err)
		assert.Equal(t, uint16(5), parsedV5.Version)
		assert.False(t, parsedV5.HasLAC())

		// Test V6
		encodedV6 := EncodeRecord(footerV6)
		parsedV6, err := ParseFooterFromBytes(encodedV6)
		require.NoError(t, err)
		assert.Equal(t, uint16(6), parsedV6.Version)
		assert.True(t, parsedV6.HasLAC())
		assert.Equal(t, int64(99999), parsedV6.LAC)
	})

	t.Run("ParseFromLargerBuffer", func(t *testing.T) {
		// Test parsing when footer is at the end of a larger buffer
		footer := &FooterRecord{
			TotalBlocks:  8,
			TotalRecords: 80,
			TotalSize:    819200,
			IndexOffset:  800000,
			IndexLength:  512,
			Version:      6,
			Flags:        0,
			LAC:          123456,
		}

		// Create a larger buffer with some data at the beginning
		prefixData := make([]byte, 1000)
		for i := range prefixData {
			prefixData[i] = byte(i % 256)
		}

		encodedFooter := EncodeRecord(footer)
		largerBuffer := append(prefixData, encodedFooter...)

		// Parse should find the footer at the end
		parsed, err := ParseFooterFromBytes(largerBuffer)
		require.NoError(t, err)

		assert.Equal(t, footer.TotalBlocks, parsed.TotalBlocks)
		assert.Equal(t, uint16(6), parsed.Version)
		assert.True(t, parsed.HasLAC())
		assert.Equal(t, int64(123456), parsed.LAC)
	})

	t.Run("InvalidFooterData", func(t *testing.T) {
		// Test with invalid data
		invalidData := []byte{0x01, 0x02, 0x03}
		_, err := ParseFooterFromBytes(invalidData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no valid footer found")
	})

	t.Run("EmptyBuffer", func(t *testing.T) {
		_, err := ParseFooterFromBytes([]byte{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no valid footer found")
	})
}

// TestTryParseFooterSize tests the helper function for parsing footer with specific size
func TestTryParseFooterSize(t *testing.T) {
	t.Run("ValidV5Footer", func(t *testing.T) {
		footer := &FooterRecord{
			TotalBlocks:  2,
			TotalRecords: 20,
			TotalSize:    204800,
			IndexOffset:  200000,
			IndexLength:  64,
			Version:      5,
			Flags:        0,
		}

		encoded := EncodeRecord(footer)
		parsed, err := tryParseFooterSize(encoded, FooterRecordSizeV5)
		require.NoError(t, err)

		assert.Equal(t, footer.TotalBlocks, parsed.TotalBlocks)
		assert.Equal(t, uint16(5), parsed.Version)
	})

	t.Run("ValidV6Footer", func(t *testing.T) {
		footer := &FooterRecord{
			TotalBlocks:  4,
			TotalRecords: 40,
			TotalSize:    409600,
			IndexOffset:  400000,
			IndexLength:  128,
			Version:      6,
			Flags:        0,
			LAC:          77777,
		}

		encoded := EncodeRecord(footer)
		parsed, err := tryParseFooterSize(encoded, FooterRecordSizeV6)
		require.NoError(t, err)

		assert.Equal(t, footer.TotalBlocks, parsed.TotalBlocks)
		assert.Equal(t, uint16(6), parsed.Version)
		assert.True(t, parsed.HasLAC())
		assert.Equal(t, int64(77777), parsed.LAC)
	})

	t.Run("InsufficientData", func(t *testing.T) {
		data := make([]byte, 10) // Too small
		_, err := tryParseFooterSize(data, FooterRecordSizeV6)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("NotFooterRecord", func(t *testing.T) {
		// Create a header record instead
		header := &HeaderRecord{
			Version:      6,
			Flags:        0,
			FirstEntryID: 1,
		}
		encoded := EncodeRecord(header)

		_, err := tryParseFooterSize(encoded, HeaderRecordSize)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a footer record")
	})
}

// TestFooterCompatibilityIntegration tests the complete integration flow
func TestFooterCompatibilityIntegration(t *testing.T) {
	t.Run("ExternalAPIUsage", func(t *testing.T) {
		// Simulate external caller usage pattern

		// Step 1: Get the maximum footer read size
		maxSize := GetMaxFooterReadSize()
		assert.Equal(t, 53, maxSize) // 9 + 44

		// Step 2: Create test data that represents file content ending with a footer
		footer := &FooterRecord{
			TotalBlocks:  50,
			TotalRecords: 500,
			TotalSize:    51200000,
			IndexOffset:  50000000,
			IndexLength:  2048,
			Version:      6,
			Flags:        FooterFlagCompacted,
			LAC:          999999,
		}

		// Create a file-like buffer with some content and footer at the end
		fileContent := make([]byte, 10000)
		for i := range fileContent {
			fileContent[i] = byte(i % 256)
		}

		encodedFooter := EncodeRecord(footer)
		fileContent = append(fileContent, encodedFooter...)

		// Step 3: Read the last maxSize bytes (simulating what external caller would do)
		var readBuffer []byte
		if len(fileContent) < maxSize {
			readBuffer = fileContent
		} else {
			readBuffer = fileContent[len(fileContent)-maxSize:]
		}

		// Step 4: Parse footer from the read buffer
		parsedFooter, err := ParseFooterFromBytes(readBuffer)
		require.NoError(t, err)

		// Verify the parsed footer matches original
		assert.Equal(t, footer.TotalBlocks, parsedFooter.TotalBlocks)
		assert.Equal(t, footer.TotalRecords, parsedFooter.TotalRecords)
		assert.Equal(t, footer.TotalSize, parsedFooter.TotalSize)
		assert.Equal(t, footer.IndexOffset, parsedFooter.IndexOffset)
		assert.Equal(t, footer.IndexLength, parsedFooter.IndexLength)
		assert.Equal(t, footer.Version, parsedFooter.Version)
		assert.Equal(t, footer.Flags, parsedFooter.Flags)
		assert.Equal(t, footer.LAC, parsedFooter.LAC)
		assert.True(t, parsedFooter.HasLAC())
		assert.True(t, parsedFooter.IsCompacted())
	})

	t.Run("BackwardCompatibilityV5ToV6", func(t *testing.T) {
		// Test that V5 footers can be read and converted to V6 structure
		footerV5 := &FooterRecord{
			TotalBlocks:  100,
			TotalRecords: 1000,
			TotalSize:    100000000,
			IndexOffset:  99000000,
			IndexLength:  4096,
			Version:      5,
			Flags:        FooterFlagCompacted,
		}

		encoded := EncodeRecord(footerV5)
		maxSize := GetMaxFooterReadSize()

		// Simulate reading from end of file
		readBuffer := make([]byte, maxSize)
		copy(readBuffer[maxSize-len(encoded):], encoded)

		parsed, err := ParseFooterFromBytes(readBuffer)
		require.NoError(t, err)

		// V5 footer should be parsed correctly
		assert.Equal(t, uint16(5), parsed.Version)
		assert.Equal(t, footerV5.TotalBlocks, parsed.TotalBlocks)
		assert.False(t, parsed.HasLAC())
		assert.Equal(t, int64(-1), parsed.LAC) // Default value for V5
		assert.True(t, parsed.IsCompacted())
	})
}
