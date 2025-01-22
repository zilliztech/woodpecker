package objectstorage

import (
	"errors"
	"sync"

	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var (
	ErrTooMuchDataToWrite    = errors.New("too much data to write")
	ErrIsFull                = errors.New("buffer is full")
	ErrIsEmpty               = errors.New("buffer is empty")
	ErrNotEnoughDataToRead   = errors.New("not enough data to read")
	ErrInvalidSequenceNumber = errors.New("sequence number not found")
)

const (
	// MaxBufferSize is the maximum size of the buffer (in bytes).
	MaxBufferSize = 1024 * 1024 // Example: 1 MB limit
)

// Buffer is a simple in-memory buffer for storing entries. It is thread-safe.
// The buffer is a flat byte slice, with each entry encoded in a specific format,
// and entries can be read from the buffer sequentially. The sequence number of each
// entry is tracked, starting from 0 that corresponds to the first entry in the buffer
// and equal with the entry index in the buffer.

type Buffer struct {
	mu     sync.Mutex
	buffer []byte       // Buffer to store entries (a flat byte slice)
	codec  *codec.Codec // Codec for encoding and decoding entries.

	entryOffsets []int // Indices of each entry in the buffer (for tracking purposes), the start index of each entry in buffer

	// track the sequence number of the last entry written to the buffer, the global sequence number = bufferStartOffset + lastSequenceNum
	bufferStartOffset int // Start offset of the buffer, used to calculate the offset of each entry in buffer
	startSeqNum       int // Start sequence number of the buffer, used to calculate the global sequence number of each entry in buffer
	lastSequenceNum   int // Last sequence number written to the buffer, starts from 0
}

// NewBuffer initializes a new Buffer with the maximum size.
func NewBuffer(bufferStartOffset int, startSeqNum int) *Buffer {
	return &Buffer{
		buffer:            make([]byte, 0),
		entryOffsets:      make([]int, 0),
		bufferStartOffset: bufferStartOffset,
		codec:             codec.NewCodec(),
		lastSequenceNum:   0,
		startSeqNum:       startSeqNum,
	}
}

// WriteEntry writes a new entry into the buffer.
func (b *Buffer) WriteEntry(payload []byte) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	encodedEntry, err := b.codec.EncodeEntry(payload)
	if err != nil {
		return -1
	}

	// Append the entry to the buffer and track its index
	b.buffer = append(b.buffer, encodedEntry...)
	if len(b.entryOffsets) == 0 {
		b.entryOffsets = append(b.entryOffsets, len(encodedEntry))
	} else {
		b.entryOffsets = append(b.entryOffsets, b.entryOffsets[len(b.entryOffsets)-1]+len(encodedEntry))
	}
	b.lastSequenceNum++

	// return the sequenceNum of the last entry written to the buffer
	return b.lastSequenceNum + b.startSeqNum
}

// ReadEntry reads and decodes the next entry in the buffer.
func (b *Buffer) ReadEntry(entrySeqNum int) (*[]byte, int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return nil, 0, ErrIsEmpty
	}

	if entrySeqNum >= b.lastSequenceNum+b.startSeqNum {
		return nil, 0, ErrInvalidSequenceNumber
	}

	entrySeqNum = entrySeqNum - b.startSeqNum

	// Fetch the entry index and sequence number
	entryStartOffset := b.entryOffsets[entrySeqNum]
	encodedEntry := b.buffer[entryStartOffset:]

	// Decode the entry
	payload, err := b.codec.DecodeEntry(encodedEntry)
	if err != nil {
		return nil, 0, err
	}

	return &payload, b.bufferStartOffset + entrySeqNum, nil
}

func (b *Buffer) GetLastSequenceNum() int {
	return b.lastSequenceNum + b.startSeqNum
}

func (b *Buffer) ReadBytesFromSeqToLast(seqNum int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return nil, ErrIsEmpty
	}

	if seqNum >= b.lastSequenceNum {
		return nil, ErrInvalidSequenceNumber
	}

	return b.ReadBytesFromSeqRange(seqNum, b.lastSequenceNum)
}

// ReadBytesFromSeqRange reads bytes from the buffer starting from the startSeqNum to the endSeqNum (inclusive).
func (b *Buffer) ReadBytesFromSeqRange(startSeqNum, endSeqNum int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return nil, ErrIsEmpty
	}

	if startSeqNum < 0 || endSeqNum >= b.lastSequenceNum || startSeqNum > endSeqNum {
		return nil, ErrInvalidSequenceNumber
	}

	// Calculate the start and end indices of the range
	var startOffset, endOffset int
	if startSeqNum == 0 {
		startOffset = 0
	} else {
		startOffset = b.entryOffsets[startSeqNum-1]
	}
	endOffset = b.entryOffsets[endSeqNum]

	// Extract the bytes from the buffer
	ret := make([]byte, endOffset-startOffset)
	copy(ret, b.buffer[startOffset:endOffset])
	return ret, nil
}

// Reset clears the buffer and resets the sequence number.
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buffer = b.buffer[:0]
	b.entryOffsets = b.entryOffsets[:0]
	b.lastSequenceNum = 0
}
