package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordTypes tests the Type() method for all record types
func TestRecordTypes(t *testing.T) {
	tests := []struct {
		name     string
		record   Record
		expected byte
	}{
		{
			name:     "HeaderRecord",
			record:   &HeaderRecord{},
			expected: HeaderRecordType,
		},
		{
			name:     "DataRecord",
			record:   &DataRecord{},
			expected: DataRecordType,
		},
		{
			name:     "IndexRecord",
			record:   &IndexRecord{},
			expected: IndexRecordType,
		},
		{
			name:     "FooterRecord",
			record:   &FooterRecord{},
			expected: FooterRecordType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.record.Type())
		})
	}
}

// TestConstants tests all format constants
func TestConstants(t *testing.T) {
	assert.Equal(t, byte(0x1), HeaderRecordType)
	assert.Equal(t, byte(0x2), DataRecordType)
	assert.Equal(t, byte(0x3), IndexRecordType)
	assert.Equal(t, byte(0x4), FooterRecordType)

	assert.Equal(t, [4]byte{'W', 'P', 'H', 'D'}, HeaderMagic)
	assert.Equal(t, [4]byte{'W', 'P', 'F', 'T'}, FooterMagic)
	assert.Equal(t, uint16(1), FormatVersion)
}

// TestHeaderRecordCodec tests HeaderRecord encoding/decoding
func TestHeaderRecordCodec(t *testing.T) {
	tests := []struct {
		name   string
		record *HeaderRecord
	}{
		{
			name: "basic header",
			record: &HeaderRecord{
				Version: FormatVersion,
				Flags:   0,
			},
		},
		{
			name: "header with flags",
			record: &HeaderRecord{
				Version: FormatVersion,
				Flags:   0x1234,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encoding
			encoded, err := EncodeRecord(tt.record)
			require.NoError(t, err)
			assert.True(t, len(encoded) > 0)

			// Decoding
			decoded, err := DecodeRecord(encoded)
			require.NoError(t, err)

			// Verify type
			headerRecord, ok := decoded.(*HeaderRecord)
			require.True(t, ok, "decoded record should be HeaderRecord")

			// Verify content
			assert.Equal(t, tt.record.Version, headerRecord.Version)
			assert.Equal(t, tt.record.Flags, headerRecord.Flags)
			assert.Equal(t, HeaderRecordType, headerRecord.Type())
		})
	}
}

// TestDataRecordCodec tests DataRecord encoding/decoding
func TestDataRecordCodec(t *testing.T) {
	tests := []struct {
		name   string
		record *DataRecord
	}{
		{
			name: "empty payload",
			record: &DataRecord{
				Payload: nil, // Use nil instead of an empty slice, as decoding will return nil
			},
		},
		{
			name: "small payload",
			record: &DataRecord{
				Payload: []byte("hello world"),
			},
		},
		{
			name: "binary payload",
			record: &DataRecord{
				Payload: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			},
		},
		{
			name: "large payload",
			record: &DataRecord{
				Payload: bytes.Repeat([]byte("test"), 1000),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encoding
			encoded, err := EncodeRecord(tt.record)
			require.NoError(t, err)

			// Decoding
			decoded, err := DecodeRecord(encoded)
			require.NoError(t, err)

			// Verify type
			dataRecord, ok := decoded.(*DataRecord)
			require.True(t, ok, "decoded record should be DataRecord")

			// Verify content
			assert.Equal(t, tt.record.Payload, dataRecord.Payload)
			assert.Equal(t, DataRecordType, dataRecord.Type())
		})
	}
}

// TestIndexRecordCodec tests IndexRecord encoding/decoding
func TestIndexRecordCodec(t *testing.T) {
	tests := []struct {
		name   string
		record *IndexRecord
	}{
		{
			name: "empty offsets",
			record: &IndexRecord{
				Offsets: []uint32{},
			},
		},
		{
			name: "single offset",
			record: &IndexRecord{
				Offsets: []uint32{1000},
			},
		},
		{
			name: "multiple offsets",
			record: &IndexRecord{
				Offsets: []uint32{1000, 2000, 3000, 4000, 5000},
			},
		},
		{
			name: "large offsets",
			record: &IndexRecord{
				Offsets: []uint32{0xFFFFFFFF, 0x80000000, 0x12345678},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encoding
			encoded, err := EncodeRecord(tt.record)
			require.NoError(t, err)

			// Decoding
			decoded, err := DecodeRecord(encoded)
			require.NoError(t, err)

			// Verify type
			indexRecord, ok := decoded.(*IndexRecord)
			require.True(t, ok, "decoded record should be IndexRecord")

			// Verify content
			assert.Equal(t, tt.record.Offsets, indexRecord.Offsets)
			assert.Equal(t, IndexRecordType, indexRecord.Type())
		})
	}
}

// TestFooterRecordCodec tests FooterRecord encoding/decoding
func TestFooterRecordCodec(t *testing.T) {
	tests := []struct {
		name   string
		record *FooterRecord
	}{
		{
			name: "basic footer",
			record: &FooterRecord{
				IndexOffset:  12345,
				IndexLength:  678,
				FirstEntryID: 100,
				Count:        50,
				Version:      FormatVersion,
				Flags:        0,
			},
		},
		{
			name: "footer with flags",
			record: &FooterRecord{
				IndexOffset:  0xFFFFFFFFFFFFFFFF,
				IndexLength:  0xFFFFFFFF,
				FirstEntryID: -12345,
				Count:        0xFFFFFFFF,
				Version:      FormatVersion,
				Flags:        0x5678,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encoding
			encoded, err := EncodeRecord(tt.record)
			require.NoError(t, err)

			// Decoding
			decoded, err := DecodeRecord(encoded)
			require.NoError(t, err)

			// Verify type
			footerRecord, ok := decoded.(*FooterRecord)
			require.True(t, ok, "decoded record should be FooterRecord")

			// Verify content
			assert.Equal(t, tt.record.IndexOffset, footerRecord.IndexOffset)
			assert.Equal(t, tt.record.IndexLength, footerRecord.IndexLength)
			assert.Equal(t, tt.record.FirstEntryID, footerRecord.FirstEntryID)
			assert.Equal(t, tt.record.Count, footerRecord.Count)
			assert.Equal(t, tt.record.Version, footerRecord.Version)
			assert.Equal(t, tt.record.Flags, footerRecord.Flags)
			assert.Equal(t, FooterRecordType, footerRecord.Type())
		})
	}
}

// TestCRCValidation tests CRC validation
func TestCRCValidation(t *testing.T) {
	original := &DataRecord{
		Payload: []byte("test data for crc validation"),
	}

	// Encoding
	encoded, err := EncodeRecord(original)
	require.NoError(t, err)
	require.True(t, len(encoded) > 10)

	// Normal decoding should succeed
	decoded, err := DecodeRecord(encoded)
	require.NoError(t, err)
	assert.Equal(t, original.Payload, decoded.(*DataRecord).Payload)

	// Corrupt CRC
	corruptedCRC := make([]byte, len(encoded))
	copy(corruptedCRC, encoded)
	corruptedCRC[0] = ^corruptedCRC[0] // Modify the first byte of CRC

	_, err = DecodeRecord(corruptedCRC)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crc mismatch")

	// Corrupt data
	corruptedData := make([]byte, len(encoded))
	copy(corruptedData, encoded)
	corruptedData[len(corruptedData)-1] = ^corruptedData[len(corruptedData)-1] // Modify the last byte

	_, err = DecodeRecord(corruptedData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crc mismatch")
}

// TestInvalidRecordData tests error handling for invalid record data
func TestInvalidRecordData(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectedErr string
	}{
		{
			name:        "too short",
			data:        []byte{0x01, 0x02},
			expectedErr: "too short",
		},
		{
			name: "truncated payload",
			data: func() []byte {
				// Create a record claiming to have a 16-byte payload but actually only has 4 bytes
				buf := make([]byte, 13)                       // CRC(4) + Type(1) + Length(4) + PartialPayload(4)
				buf[4] = 0x02                                 // DataRecord type
				binary.LittleEndian.PutUint32(buf[5:9], 16)   // Claims to have a 16-byte payload
				copy(buf[9:], []byte{0x01, 0x02, 0x03, 0x04}) // But only has 4 bytes
				// Calculate correct CRC
				crc := crc32.ChecksumIEEE(buf[4:])
				binary.LittleEndian.PutUint32(buf[0:4], crc)
				return buf
			}(),
			expectedErr: "truncated payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeRecord(tt.data)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

// TestUnsupportedRecordType tests handling of unsupported record types
func TestUnsupportedRecordType(t *testing.T) {
	// Create a valid record structure but use an unknown type
	validDataRecord := &DataRecord{Payload: []byte("test")}
	encoded, err := EncodeRecord(validDataRecord)
	require.NoError(t, err)

	// Modify type to an unknown type
	encoded[4] = 0xFF // Modify the type field

	// Recalculate CRC
	crc := crc32.ChecksumIEEE(encoded[4:])
	binary.LittleEndian.PutUint32(encoded[:4], crc)

	// Attempt to decode
	_, err = DecodeRecord(encoded)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no decoder for type")
}

// TestHeaderVersionValidation tests header version validation
func TestHeaderVersionValidation(t *testing.T) {
	// Create a valid header
	header := &HeaderRecord{
		Version:      FormatVersion,
		Flags:        0,
		FirstEntryID: 100,
	}

	encoded, err := EncodeRecord(header)
	require.NoError(t, err)

	// Modify the version number in the payload
	// Header payload: [Version:2][Flags:2][FirstEntryID:8][Magic:4]
	// Version number is in the first 2 bytes of the payload, payload starts at byte 9
	binary.LittleEndian.PutUint16(encoded[9:11], 999) // Set an unsupported version number

	// Recalculate CRC
	crc := crc32.ChecksumIEEE(encoded[4:])
	binary.LittleEndian.PutUint32(encoded[:4], crc)

	// Attempt to decode
	_, err = DecodeRecord(encoded)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupport file format")
}

// TestMagicValidation tests magic number validation
func TestMagicValidation(t *testing.T) {
	// Test Header magic
	header := &HeaderRecord{
		Version:      FormatVersion,
		Flags:        0,
		FirstEntryID: 100,
	}

	encoded, err := EncodeRecord(header)
	require.NoError(t, err)

	// Corrupt Header magic (magic is at the end of the payload: Version(2) + Flags(2) + FirstEntryID(8) + Magic(4))
	// Payload starts at byte 9, magic is at bytes 21-24 (9+2+2+8 to 9+2+2+8+4)
	copy(encoded[21:25], []byte("XXXX"))

	// Recalculate CRC
	crc := crc32.ChecksumIEEE(encoded[4:])
	binary.LittleEndian.PutUint32(encoded[:4], crc)

	// Attempt to decode
	_, err = DecodeRecord(encoded)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid header magic")

	// Test Footer magic
	footer := &FooterRecord{
		IndexOffset:  12345,
		IndexLength:  678,
		FirstEntryID: 100,
		Count:        50,
		Version:      FormatVersion,
		Flags:        0,
	}

	encoded, err = EncodeRecord(footer)
	require.NoError(t, err)

	// Corrupt Footer magic (in the last 4 bytes of the payload)
	payloadStart := 9
	payloadLen := len(encoded) - payloadStart
	copy(encoded[payloadStart+payloadLen-4:], []byte("YYYY"))

	// Recalculate CRC
	crc = crc32.ChecksumIEEE(encoded[4:])
	binary.LittleEndian.PutUint32(encoded[:4], crc)

	// Attempt to decode
	_, err = DecodeRecord(encoded)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid footer magic")
}

// TestRecordRoundTrip tests complete encode/decode round trip for all types
func TestRecordRoundTrip(t *testing.T) {
	records := []Record{
		&HeaderRecord{Version: FormatVersion, Flags: 0x1234},
		&DataRecord{Payload: []byte("test payload data")},
		&IndexRecord{Offsets: []uint32{100, 200, 300}},
		&FooterRecord{
			IndexOffset:  98765,
			IndexLength:  432,
			FirstEntryID: 12345,
			Count:        123,
			Version:      FormatVersion,
			Flags:        0x5678,
		},
	}

	for i, record := range records {
		t.Run(fmt.Sprintf("record_%d", i), func(t *testing.T) {
			// Encoding
			encoded, err := EncodeRecord(record)
			require.NoError(t, err)

			// Decoding
			decoded, err := DecodeRecord(encoded)
			require.NoError(t, err)

			// Verify type match
			assert.Equal(t, record.Type(), decoded.Type())

			// Re-encode the decoded record, should get the same result
			reencoded, err := EncodeRecord(decoded)
			require.NoError(t, err)
			assert.Equal(t, encoded, reencoded)
		})
	}
}
