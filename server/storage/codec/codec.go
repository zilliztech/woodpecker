package codec

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/cockroachdb/errors"
)

// Constants for sizes
const (
	MagicString        = "woodpecker01"
	VERSION            = uint32(1)
	FileHeaderSize     = 16
	PayloadSize        = 4
	CrcSize            = 4
	EntryHeaderSize    = PayloadSize + CrcSize
	OffsetSize         = 4
	SequenceNumberSize = 4
	IndexItemSize      = 4
)

var (
	ErrCRCMismatch        = errors.New("CRC mismatch")
	ErrInvalidPayloadSize = errors.New("invalid payload size")
)

// Codec handles encoding and decoding of buffer entries.
type Codec struct{}

// NewCodec creates and returns a new Codec instance.
func NewCodec() *Codec {
	return &Codec{}
}

// EncodeEntry encodes the payload and sequence number into a byte slice.
// Format: [PayloadSize (4 bytes)] + [CRC (4 bytes)] + [Payload].
func (c *Codec) EncodeEntry(payload []byte) ([]byte, error) {
	payloadSize := len(payload)
	if payloadSize <= 0 {
		return nil, ErrInvalidPayloadSize
	}

	// Calculate CRC
	crc := crc32.ChecksumIEEE(payload)

	// Prepare entry buffer
	entry := make([]byte, EntryHeaderSize+payloadSize)
	copy(entry[:PayloadSize], intToBytes(payloadSize, PayloadSize))
	copy(entry[PayloadSize:PayloadSize+CrcSize], intToBytes(int(crc), CrcSize))
	copy(entry[EntryHeaderSize:], payload)

	return entry, nil
}

// DecodeEntry decodes a byte slice into payload and validates the CRC.
// Assumes format: [PayloadSize (4 bytes)] + [CRC (4 bytes)] + [Payload].
func (c *Codec) DecodeEntry(entry []byte) ([]byte, error) {
	if len(entry) < EntryHeaderSize {
		return nil, ErrInvalidPayloadSize
	}

	// Extract payload size
	payloadSize := bytesToInt(entry[:PayloadSize])

	if payloadSize <= 0 || len(entry) < EntryHeaderSize+payloadSize {
		return nil, ErrInvalidPayloadSize
	}

	// Extract and validate CRC
	crc := uint32(bytesToInt(entry[PayloadSize : PayloadSize+CrcSize]))
	payload := entry[EntryHeaderSize : EntryHeaderSize+payloadSize]
	if crc != crc32.ChecksumIEEE(payload) {
		return nil, ErrCRCMismatch
	}

	return payload, nil
}

// intToBytes converts an integer to a byte slice of the specified size.
func intToBytes(value int, size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte(value >> (8 * i))
	}
	return buf
}

func int64ToBytes(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

// bytesToInt converts a byte slice to an integer.
func bytesToInt(data []byte) int {
	result := 0
	for i := 0; i < len(data); i++ {
		result |= int(data[i]) << (8 * i)
	}
	return result
}

func bytesToInt64(data []byte) int64 {
	result := binary.BigEndian.Uint64(data)
	return int64(result)
}
