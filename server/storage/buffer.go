package storage

import (
	"errors"
	"sync"
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
	buffer []byte // Buffer to store entries (a flat byte slice)
	codec  *Codec // Codec for encoding and decoding entries.

	entryIdx []int // Indices of each entry in the buffer (for tracking purposes), the start index of each entry in buffer

	// track the sequence number of the last entry written to the buffer, the global sequence number = bufferStartOffset + lastSequenceNum
	bufferStartOffset uint64 // Start offset of the buffer, used to calculate the offset of each entry in buffer
	lastSequenceNum   uint32 // Last sequence number written to the buffer, starts from 0
}

// NewBuffer initializes a new Buffer with the maximum size.
func NewBuffer(bufferSize int, bufferStartOffset uint64) *Buffer {
	return &Buffer{
		buffer:            make([]byte, 0, bufferSize),
		entryIdx:          make([]int, 0),
		bufferStartOffset: bufferStartOffset,
		codec:             NewCodec(),
	}
}

// WriteEntry writes a new entry into the buffer.
func (b *Buffer) WriteEntry(payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	encodedEntry, err := b.codec.EncodeEntry(payload)
	if err != nil {
		return err
	}

	// Check if there's enough space to append the new entry
	if len(b.buffer)+len(encodedEntry) > MaxBufferSize {
		return ErrTooMuchDataToWrite
	}

	// Append the entry to the buffer and track its index
	b.buffer = append(b.buffer, encodedEntry...)
	if len(b.entryIdx) == 0 {
		b.entryIdx = append(b.entryIdx, len(encodedEntry))
	} else {
		b.entryIdx = append(b.entryIdx, b.entryIdx[len(b.entryIdx)-1]+len(encodedEntry))
	}
	b.lastSequenceNum++

	return nil
}

// ReadEntry reads and decodes the next entry in the buffer.
func (b *Buffer) ReadEntry(entrySeqNum int) (*[]byte, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return nil, 0, ErrIsEmpty
	}

	if entrySeqNum >= int(b.lastSequenceNum) {
		return nil, 0, ErrInvalidSequenceNumber
	}

	// Fetch the entry index and sequence number
	entryStartIdx := b.entryIdx[entrySeqNum]
	encodedEntry := b.buffer[entryStartIdx:]

	// Decode the entry
	payload, err := b.codec.DecodeEntry(encodedEntry)
	if err != nil {
		return nil, 0, err
	}

	return &payload, b.bufferStartOffset + uint64(entrySeqNum), nil
}

func (b *Buffer) GetLastSequenceNum() uint32 {
	return b.lastSequenceNum
}

func (b *Buffer) ReadBytesFromSeqToLast(seqNum uint32) ([]byte, error) {
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
func (b *Buffer) ReadBytesFromSeqRange(startSeqNum, endSeqNum uint32) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return nil, ErrIsEmpty
	}

	if startSeqNum < 0 || endSeqNum >= b.lastSequenceNum || startSeqNum > endSeqNum {
		return nil, ErrInvalidSequenceNumber
	}

	// Calculate the start and end indices of the range
	var startIdx, endIdx int
	if startSeqNum == 0 {
		startIdx = 0
	} else {
		startIdx = b.entryIdx[startSeqNum-1]
	}
	endIdx = b.entryIdx[endSeqNum]

	// Extract the bytes from the buffer
	ret := make([]byte, endIdx-startIdx)
	copy(ret, b.buffer[startIdx:endIdx])
	return ret, nil
}

// Reset clears the buffer and resets the sequence number.
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buffer = b.buffer[:0]
	b.entryIdx = b.entryIdx[:0]
	b.lastSequenceNum = 0
}
