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
	"encoding/binary"
	"hash/crc32"

	"github.com/cockroachdb/errors"
)

// EncodeRecord encoding and decoding functions
func EncodeRecord(r Record) []byte {
	var payload []byte

	switch record := r.(type) {
	case *HeaderRecord:
		payload = make([]byte, HeaderRecordSize) // Version(2) + Flags(2) + FirstEntryID(8) + Magic(4)
		binary.LittleEndian.PutUint16(payload[0:], record.Version)
		binary.LittleEndian.PutUint16(payload[2:], record.Flags)
		binary.LittleEndian.PutUint64(payload[4:], uint64(record.FirstEntryID))
		copy(payload[12:], HeaderMagic[:])

	case *DataRecord:
		payload = append([]byte(nil), record.Payload...)

	case *IndexRecord:
		payload = make([]byte, IndexRecordSize) // BlockNumber(4) + StartOffset(8) + BlockSize(4) + FirstEntryID(8) + LastEntryID(8)
		binary.LittleEndian.PutUint32(payload[0:], uint32(record.BlockNumber))
		binary.LittleEndian.PutUint64(payload[4:], uint64(record.StartOffset))
		binary.LittleEndian.PutUint32(payload[12:], record.BlockSize)
		binary.LittleEndian.PutUint64(payload[16:], uint64(record.FirstEntryID))
		binary.LittleEndian.PutUint64(payload[24:], uint64(record.LastEntryID))

	case *BlockHeaderRecord:
		payload = make([]byte, BlockHeaderRecordSize) // BlockNumber(4) + FirstEntryID(8) + LastEntryID(8) + BlockLength(4) + BlockCrc(4)
		binary.LittleEndian.PutUint32(payload[0:], uint32(record.BlockNumber))
		binary.LittleEndian.PutUint64(payload[4:], uint64(record.FirstEntryID))
		binary.LittleEndian.PutUint64(payload[12:], uint64(record.LastEntryID))
		binary.LittleEndian.PutUint32(payload[20:], record.BlockLength)
		binary.LittleEndian.PutUint32(payload[24:], record.BlockCrc)

	case *FooterRecord:
		payload = make([]byte, FooterRecordSize) // TotalBlocks(4) + TotalRecords(4) + TotalSize(8) + IndexOffset(8) + IndexLength(4) + Version(2) + Flags(2) + Magic(4)
		binary.LittleEndian.PutUint32(payload[0:], uint32(record.TotalBlocks))
		binary.LittleEndian.PutUint32(payload[4:], record.TotalRecords)
		binary.LittleEndian.PutUint64(payload[8:], record.TotalSize)
		binary.LittleEndian.PutUint64(payload[16:], record.IndexOffset)
		binary.LittleEndian.PutUint32(payload[24:], record.IndexLength)
		binary.LittleEndian.PutUint16(payload[28:], record.Version)
		binary.LittleEndian.PutUint16(payload[30:], record.Flags)
		copy(payload[32:], FooterMagic[:])
	}

	// Create record with header: CRC32(4) + Type(1) + Length(4) + Payload
	buf := make([]byte, RecordHeaderSize+len(payload))
	buf[4] = r.Type()                                            // Type
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(payload))) // Length
	copy(buf[RecordHeaderSize:], payload)                        // Payload

	// Calculate CRC32 over Type + Length + Payload
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:], crc) // CRC32

	return buf
}

func DecodeRecord(buf []byte) (Record, error) {
	// Check minimum length for record header
	if len(buf) < RecordHeaderSize {
		return nil, errors.Errorf("buffer too short for record header: %d", len(buf))
	}

	// Parse record header: CRC32(4) + Type(1) + Length(4)
	crc := binary.LittleEndian.Uint32(buf[0:4])
	recordType := buf[4]
	payloadLength := binary.LittleEndian.Uint32(buf[5:9])

	// Check total buffer length
	totalLength := RecordHeaderSize + int(payloadLength)
	if len(buf) < totalLength {
		return nil, errors.Errorf("buffer too short for record: expected %d, got %d", totalLength, len(buf))
	}

	// Verify CRC32 over Type + Length + Payload
	calculatedCRC := crc32.ChecksumIEEE(buf[4:totalLength])
	if crc != calculatedCRC {
		return nil, errors.Errorf("CRC32 mismatch: expected %x, got %x", crc, calculatedCRC)
	}

	// Extract payload
	payload := buf[RecordHeaderSize:totalLength]

	// Parse record based on type
	return ParseRecord(recordType, payload)
}

func DecodeRecordList(buf []byte) ([]Record, error) {
	records := make([]Record, 0)
	offset := 0

	for offset < len(buf) {
		// Check if we have enough bytes for record header
		if offset+RecordHeaderSize > len(buf) {
			// Not enough bytes for a complete record header, return what we have parsed so far
			break
		}

		// Parse record header: CRC32(4) + Type(1) + Length(4)
		crc := binary.LittleEndian.Uint32(buf[offset : offset+4])
		recordType := buf[offset+4]
		payloadLength := binary.LittleEndian.Uint32(buf[offset+5 : offset+9])

		// Check if we have enough bytes for the complete record
		totalRecordLength := RecordHeaderSize + int(payloadLength)
		if offset+totalRecordLength > len(buf) {
			// Not enough bytes for complete record, return what we have parsed so far
			break
		}

		// Verify CRC32 over Type + Length + Payload
		calculatedCRC := crc32.ChecksumIEEE(buf[offset+4 : offset+totalRecordLength])
		if crc != calculatedCRC {
			// CRC mismatch indicates corruption, return what we have parsed so far
			// Don't return error - just stop parsing and return successful records
			break
		}

		// Extract payload
		payload := buf[offset+RecordHeaderSize : offset+totalRecordLength]

		// Parse record based on type
		record, err := ParseRecord(recordType, payload)
		if err != nil {
			// Failed to parse record, return what we have parsed so far
			// Don't return error - just stop parsing and return successful records
			break
		}

		// Add successfully parsed record to list
		records = append(records, record)

		// Move to next record
		offset += totalRecordLength
	}

	return records, nil
}

func ParseRecord(recordType byte, payload []byte) (Record, error) {
	switch recordType {
	case HeaderRecordType:
		return ParseHeader(payload)
	case DataRecordType:
		return ParseData(payload)
	case IndexRecordType:
		return ParseBlockIndex(payload)
	case BlockHeaderRecordType:
		return ParseBlockHeader(payload)
	case FooterRecordType:
		return ParseFooter(payload)
	default:
		return nil, errors.Errorf("unknown record type: %d", recordType)
	}
}

func ParseHeader(payload []byte) (*HeaderRecord, error) {
	if len(payload) != HeaderRecordSize {
		return nil, errors.Errorf("invalid header payload length: %d", len(payload))
	}

	h := &HeaderRecord{
		Version:      binary.LittleEndian.Uint16(payload[0:]),
		Flags:        binary.LittleEndian.Uint16(payload[2:]),
		FirstEntryID: int64(binary.LittleEndian.Uint64(payload[4:])),
	}

	// Verify version
	if h.Version != FormatVersion {
		return nil, errors.New("invalid format version")
	}

	// Verify magic
	if !bytes.Equal(payload[12:16], HeaderMagic[:]) {
		return nil, errors.New("invalid header magic")
	}

	return h, nil
}

func ParseData(payload []byte) (*DataRecord, error) {
	// Ensure we always return a non-nil slice, even for empty payloads
	if len(payload) == 0 {
		return &DataRecord{
			Payload: []byte{},
		}, nil
	}
	return &DataRecord{
		Payload: payload,
	}, nil
}

func ParseBlockIndex(payload []byte) (*IndexRecord, error) {
	if len(payload) < IndexRecordSize {
		return nil, errors.Errorf("invalid index payload length: %d", len(payload))
	}

	return &IndexRecord{
		BlockNumber:  int32(binary.LittleEndian.Uint32(payload[0:])),
		StartOffset:  int64(binary.LittleEndian.Uint64(payload[4:])),
		BlockSize:    binary.LittleEndian.Uint32(payload[12:]),
		FirstEntryID: int64(binary.LittleEndian.Uint64(payload[16:])),
		LastEntryID:  int64(binary.LittleEndian.Uint64(payload[24:])),
	}, nil
}

func ParseBlockIndexList(payload []byte) ([]*IndexRecord, error) {
	indexRecords := make([]*IndexRecord, 0)
	for len(payload) > 0 {
		indexRecord, err := ParseBlockIndex(payload)
		if err != nil {
			return nil, err
		}

		indexRecords = append(indexRecords, indexRecord)
		payload = payload[IndexRecordSize:]
	}
	return indexRecords, nil
}

func ParseFooter(payload []byte) (*FooterRecord, error) {
	if len(payload) != FooterRecordSize {
		return nil, errors.Errorf("invalid footer payload length: %d", len(payload))
	}

	f := &FooterRecord{
		TotalBlocks:  int32(binary.LittleEndian.Uint32(payload[0:])),
		TotalRecords: binary.LittleEndian.Uint32(payload[4:]),
		TotalSize:    binary.LittleEndian.Uint64(payload[8:]),
		IndexOffset:  binary.LittleEndian.Uint64(payload[16:]),
		IndexLength:  binary.LittleEndian.Uint32(payload[24:]),
		Version:      binary.LittleEndian.Uint16(payload[28:]),
		Flags:        binary.LittleEndian.Uint16(payload[30:]),
	}

	// Verify version
	if f.Version != FormatVersion {
		return nil, errors.New("invalid format version")
	}

	// Verify magic
	if !bytes.Equal(payload[32:36], FooterMagic[:]) {
		return nil, errors.New("invalid footer magic")
	}

	return f, nil
}

func ParseBlockHeader(payload []byte) (*BlockHeaderRecord, error) {
	if len(payload) != BlockHeaderRecordSize {
		return nil, errors.Errorf("invalid block header payload length: %d", len(payload))
	}

	b := &BlockHeaderRecord{
		BlockNumber:  int32(binary.LittleEndian.Uint32(payload[0:])),
		FirstEntryID: int64(binary.LittleEndian.Uint64(payload[4:])),
		LastEntryID:  int64(binary.LittleEndian.Uint64(payload[12:])),
		BlockLength:  binary.LittleEndian.Uint32(payload[20:]),
		BlockCrc:     binary.LittleEndian.Uint32(payload[24:]),
	}

	return b, nil
}

// VerifyBlockDataIntegrity verifies the integrity of block data using BlockHeaderRecord
func VerifyBlockDataIntegrity(blockHeaderRecord *BlockHeaderRecord, blockData []byte) error {
	// Verify block length
	if uint32(len(blockData)) != blockHeaderRecord.BlockLength {
		return errors.Errorf("block length mismatch: expected %d, got %d",
			blockHeaderRecord.BlockLength, len(blockData))
	}

	// Verify block CRC
	calculatedCrc := crc32.ChecksumIEEE(blockData)
	if calculatedCrc != blockHeaderRecord.BlockCrc {
		return errors.Errorf("block CRC mismatch: expected %x, got %x",
			blockHeaderRecord.BlockCrc, calculatedCrc)
	}

	return nil
}

// 16 bits Flags
// Compacted: 1 bits
// Reserved: 2-16 bits

func IsCompacted(flags uint16) bool {
	return (flags & 1) != 0
}
func SetCompacted(flags uint16) uint16 {
	return flags | 1
}
