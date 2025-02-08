package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zilliztech/woodpecker/common/werr"
)

// SequentialBuffer is a buffer that stores entries in a sequential manner.
type SequentialBuffer struct {
	mu       sync.Mutex
	values   [][]byte
	maxSize  int64
	dataSize atomic.Int64

	firstEntryId        int64
	expectedNextEntryId atomic.Int64
}

func NewSequentialBuffer(startEntryId int64, maxSize int64) *SequentialBuffer {
	b := &SequentialBuffer{
		values:       make([][]byte, maxSize),
		maxSize:      maxSize,
		firstEntryId: startEntryId,
	}
	b.expectedNextEntryId.Store(startEntryId)
	return b
}

func NewSequentialBufferWithData(startEntryId int64, maxSize int64, restData [][]byte) *SequentialBuffer {
	v := make([][]byte, maxSize)
	copy(v, restData)
	b := &SequentialBuffer{
		values:       v,
		maxSize:      maxSize,
		firstEntryId: startEntryId,
	}
	b.expectedNextEntryId.Store(startEntryId)
	return b
}

// WriteEntry writes a new entry into the buffer.
func (b *SequentialBuffer) WriteEntry(entryId int64, value []byte) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if entryId < b.firstEntryId {
		return -1, errors.New(fmt.Sprintf("invalid entryId: %d smaller then %d", entryId, b.firstEntryId))
	}

	if entryId >= b.firstEntryId+b.maxSize {
		return -1, errors.New(fmt.Sprintf("buffer full, entryId: %d larger then %d", entryId, (b.firstEntryId + b.maxSize)))
	}

	relatedIdx := entryId - b.firstEntryId
	b.values[relatedIdx] = value
	b.dataSize.Add(int64(len(value)))

	// increase the expectedNextEntryId if necessary
	for addedId := entryId; addedId < b.firstEntryId+b.maxSize; addedId++ {
		if b.values[addedId-b.firstEntryId] != nil && addedId == b.expectedNextEntryId.Load() {
			b.expectedNextEntryId.Add(1)
		} else {
			break
		}
	}

	return entryId, nil
}

func (b *SequentialBuffer) ReadEntry(entryId int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if entryId < b.firstEntryId {
		return nil, errors.New(fmt.Sprintf("invalid entryId: %d smaller then %d", entryId, b.firstEntryId))
	}

	if entryId >= b.firstEntryId+b.maxSize {
		return nil, errors.New(fmt.Sprintf("invalid entryId: %d larger then %d", entryId, (b.firstEntryId + b.maxSize)))
	}

	relatedIdx := entryId - b.firstEntryId
	value := b.values[relatedIdx]
	if value == nil {
		return nil, errors.New(fmt.Sprintf("entry not found for entryId: %d", entryId))
	}

	return value, nil
}

func (b *SequentialBuffer) GetFirstEntryId() int64 {
	return b.firstEntryId
}

func (b *SequentialBuffer) GetExpectedNextEntryId() int64 {
	return b.expectedNextEntryId.Load()
}

func (b *SequentialBuffer) ReadEntriesToLast(fromEntryId int64) ([][]byte, error) {
	if len(b.values) == 0 {
		return nil, werr.ErrBufferIsEmpty
	}

	if fromEntryId < b.firstEntryId || fromEntryId > b.firstEntryId+b.maxSize {
		return nil, werr.ErrInvalidEntryId
	}

	if fromEntryId == b.firstEntryId+b.maxSize {
		return make([][]byte, 0), nil
	}

	return b.ReadEntriesRange(fromEntryId, b.firstEntryId+b.maxSize)
}

// ReadBytesFromSeqRange reads bytes from the buffer starting from the startEntryId to the endEntryId (Exclusive).
func (b *SequentialBuffer) ReadEntriesRange(startEntryId int64, endEntryId int64) ([][]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if startEntryId >= b.firstEntryId+b.maxSize || startEntryId < b.firstEntryId {
		return nil, werr.ErrInvalidEntryId
	}

	if endEntryId > b.firstEntryId+b.maxSize || endEntryId < startEntryId {
		return nil, werr.ErrInvalidEntryId
	}

	// Extract the bytes from the buffer
	ret := make([][]byte, endEntryId-startEntryId)
	copy(ret, b.values[startEntryId-b.firstEntryId:endEntryId-b.firstEntryId])
	return ret, nil
}

// Reset clears the buffer and resets the sequence number.
func (b *SequentialBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.values = make([][]byte, b.maxSize)
	b.dataSize.Store(0)
	b.expectedNextEntryId.Store(b.firstEntryId)
}
