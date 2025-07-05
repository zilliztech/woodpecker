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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &HeaderRecord{
		Version:      1,
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
		BlockNumber:       10,
		StartOffset:       1024,
		FirstRecordOffset: 100,
		FirstEntryID:      500,
		LastEntryID:       600,
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
	assert.Equal(t, original.FirstRecordOffset, indexRecord.FirstRecordOffset)
	assert.Equal(t, original.FirstEntryID, indexRecord.FirstEntryID)
	assert.Equal(t, original.LastEntryID, indexRecord.LastEntryID)
}

func TestBlockLastRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &BlockLastRecord{
		FirstEntryID: 1000,
		LastEntryID:  2000,
	}

	// Test encoding
	encoded := EncodeRecord(original)
	assert.NotEmpty(t, encoded)

	// Test decoding
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)

	// Verify type and content
	blockLastRecord, ok := decoded.(*BlockLastRecord)
	require.True(t, ok)
	assert.Equal(t, original.FirstEntryID, blockLastRecord.FirstEntryID)
	assert.Equal(t, original.LastEntryID, blockLastRecord.LastEntryID)
}

func TestFooterRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &FooterRecord{
		TotalBlocks:  100,
		TotalRecords: 5000,
		IndexOffset:  10240,
		IndexLength:  512,
		Version:      1,
		Flags:        0x5678,
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
	assert.Equal(t, original.Version, footerRecord.Version)
	assert.Equal(t, original.Flags, footerRecord.Flags)
}

func TestDecodeRecordList_MultipleRecords(t *testing.T) {
	// Create multiple records
	records := []Record{
		&HeaderRecord{Version: 1, Flags: 0x1234, FirstEntryID: 1000},
		&DataRecord{Payload: []byte("hello world")},
		&IndexRecord{BlockNumber: 1, StartOffset: 100, FirstRecordOffset: 50, FirstEntryID: 1000, LastEntryID: 1010},
		&BlockLastRecord{FirstEntryID: 1000, LastEntryID: 1010},
		&FooterRecord{TotalBlocks: 1, TotalRecords: 2, IndexOffset: 200, IndexLength: 36, Version: 1, Flags: 0x5678},
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
			assert.Equal(t, original.FirstRecordOffset, indexRecord.FirstRecordOffset)
			assert.Equal(t, original.FirstEntryID, indexRecord.FirstEntryID)
			assert.Equal(t, original.LastEntryID, indexRecord.LastEntryID)
		case *BlockLastRecord:
			blockLastRecord, ok := decoded.(*BlockLastRecord)
			require.True(t, ok)
			assert.Equal(t, original.FirstEntryID, blockLastRecord.FirstEntryID)
			assert.Equal(t, original.LastEntryID, blockLastRecord.LastEntryID)
		case *FooterRecord:
			footerRecord, ok := decoded.(*FooterRecord)
			require.True(t, ok)
			assert.Equal(t, original.TotalBlocks, footerRecord.TotalBlocks)
			assert.Equal(t, original.TotalRecords, footerRecord.TotalRecords)
			assert.Equal(t, original.IndexOffset, footerRecord.IndexOffset)
			assert.Equal(t, original.IndexLength, footerRecord.IndexLength)
			assert.Equal(t, original.Version, footerRecord.Version)
			assert.Equal(t, original.Flags, footerRecord.Flags)
		}
	}
}

func TestDecodeRecordList_EmptyBuffer(t *testing.T) {
	records, err := DecodeRecordList([]byte{})
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestDecodeRecordList_IncompleteBuffer(t *testing.T) {
	// Create a record and truncate it
	original := &DataRecord{Payload: []byte("hello world")}
	encoded := EncodeRecord(original)

	// Truncate to incomplete record
	truncated := encoded[:len(encoded)-5]

	records, err := DecodeRecordList(truncated)
	require.NoError(t, err)
	assert.Empty(t, records) // Should return empty list for incomplete records
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
		{BlockNumber: 1, StartOffset: 100, FirstRecordOffset: 50, FirstEntryID: 1000, LastEntryID: 1010},
		{BlockNumber: 2, StartOffset: 200, FirstRecordOffset: 150, FirstEntryID: 1011, LastEntryID: 1020},
		{BlockNumber: 3, StartOffset: 300, FirstRecordOffset: 250, FirstEntryID: 1021, LastEntryID: 1030},
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
		assert.Equal(t, original.FirstRecordOffset, parsedRecord.FirstRecordOffset)
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
		{"BlockLastRecord", &BlockLastRecord{}, BlockLastRecordType},
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
			BlockNumber:       2147483647,          // max int32
			StartOffset:       9223372036854775807, // max int64
			FirstRecordOffset: 9223372036854775807,
			FirstEntryID:      9223372036854775807,
			LastEntryID:       9223372036854775807,
		}

		encoded := EncodeRecord(indexRecord)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedIndex, ok := decoded.(*IndexRecord)
		require.True(t, ok)
		assert.Equal(t, indexRecord.BlockNumber, decodedIndex.BlockNumber)
		assert.Equal(t, indexRecord.StartOffset, decodedIndex.StartOffset)
		assert.Equal(t, indexRecord.FirstRecordOffset, decodedIndex.FirstRecordOffset)
		assert.Equal(t, indexRecord.FirstEntryID, decodedIndex.FirstEntryID)
		assert.Equal(t, indexRecord.LastEntryID, decodedIndex.LastEntryID)
	})

	t.Run("MinValues", func(t *testing.T) {
		// Test with minimum values
		indexRecord := &IndexRecord{
			BlockNumber:       -2147483648,          // min int32
			StartOffset:       -9223372036854775808, // min int64
			FirstRecordOffset: -9223372036854775808,
			FirstEntryID:      -9223372036854775808,
			LastEntryID:       -9223372036854775808,
		}

		encoded := EncodeRecord(indexRecord)
		decoded, err := DecodeRecord(encoded)
		require.NoError(t, err)

		decodedIndex, ok := decoded.(*IndexRecord)
		require.True(t, ok)
		assert.Equal(t, indexRecord.BlockNumber, decodedIndex.BlockNumber)
		assert.Equal(t, indexRecord.StartOffset, decodedIndex.StartOffset)
		assert.Equal(t, indexRecord.FirstRecordOffset, decodedIndex.FirstRecordOffset)
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
		assert.Contains(t, err.Error(), "invalid header magic")
	})

	t.Run("InvalidFooterMagic", func(t *testing.T) {
		// Create a payload with invalid magic
		payload := make([]byte, 28)
		payload[24] = 0xFF // Invalid magic

		_, err := ParseFooter(payload)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid footer magic")
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
			IndexOffset:  10240,
			IndexLength:  512,
			Version:      1,
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
			Version:      1,
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
		{"IndexRecord", &IndexRecord{BlockNumber: 10, StartOffset: 1024, FirstRecordOffset: 100, FirstEntryID: 500, LastEntryID: 600}},
		{"BlockLastRecord", &BlockLastRecord{FirstEntryID: 1000, LastEntryID: 2000}},
		{"FooterRecord", &FooterRecord{TotalBlocks: 100, TotalRecords: 5000, IndexOffset: 10240, IndexLength: 512, Version: 1, Flags: 0x5678}},
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
		{"IndexRecord", &IndexRecord{BlockNumber: 10, StartOffset: 1024, FirstRecordOffset: 100, FirstEntryID: 500, LastEntryID: 600}},
		{"BlockLastRecord", &BlockLastRecord{FirstEntryID: 1000, LastEntryID: 2000}},
		{"FooterRecord", &FooterRecord{TotalBlocks: 100, TotalRecords: 5000, IndexOffset: 10240, IndexLength: 512, Version: 1, Flags: 0x5678}},
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
		&IndexRecord{BlockNumber: 1, StartOffset: 100, FirstRecordOffset: 50, FirstEntryID: 1000, LastEntryID: 1010},
		&BlockLastRecord{FirstEntryID: 1000, LastEntryID: 1010},
		&FooterRecord{TotalBlocks: 1, TotalRecords: 2, IndexOffset: 200, IndexLength: 36, Version: 1, Flags: 0x5678},
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
			BlockNumber:       int32(i),
			StartOffset:       int64(i * 1024),
			FirstRecordOffset: int64(i * 100),
			FirstEntryID:      int64(i * 1000),
			LastEntryID:       int64(i*1000 + 999),
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
