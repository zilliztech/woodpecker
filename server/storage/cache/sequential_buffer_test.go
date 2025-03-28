package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewSequentialBuffer tests the creation of a new SequentialBuffer.
func TestNewSequentialBuffer(t *testing.T) {
	buffer := NewSequentialBuffer(1, 5)
	assert.NotNil(t, buffer)
	assert.Equal(t, int64(1), buffer.FirstEntryId)
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())
	assert.Len(t, buffer.Values, 5)
}

// TestNewSequentialBufferWithData tests the creation of a new SequentialBuffer with initial data.
func TestNewSequentialBufferWithData(t *testing.T) {
	data := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3"), []byte("data4"), []byte("data5")}
	buffer := NewSequentialBufferWithData(1, 5, data)
	assert.NotNil(t, buffer)
	assert.Equal(t, int64(1), buffer.FirstEntryId)
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())
	assert.Len(t, buffer.Values, 5)
	assert.Equal(t, data, buffer.Values)

	data2 := [][]byte{nil, []byte("data3"), nil, []byte("data5")}
	buffer2 := NewSequentialBufferWithData(2, 5, data2)
	assert.NotNil(t, buffer2)
	assert.Equal(t, int64(2), buffer2.FirstEntryId)
	assert.Equal(t, int64(2), buffer2.ExpectedNextEntryId.Load())
	assert.Len(t, buffer2.Values, 5)
	assert.Equal(t, [][]byte{nil, []byte("data3"), nil, []byte("data5"), nil}, buffer2.Values)
}

// TestWriteEntry tests the WriteEntry method.
func TestWriteEntry(t *testing.T) {
	buffer := NewSequentialBuffer(1, 5)

	// Test writing a valid entry
	id, err := buffer.WriteEntry(1, []byte("data1"))
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)
	assert.Equal(t, int64(2), buffer.ExpectedNextEntryId.Load())

	// Test writing another valid entry
	id, err = buffer.WriteEntry(2, []byte("data2"))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), id)
	assert.Equal(t, int64(3), buffer.ExpectedNextEntryId.Load())

	// Test writing an entry with an valid ID, but not in sequence
	id, err = buffer.WriteEntry(4, []byte("data4"))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), id)
	assert.Equal(t, int64(3), buffer.ExpectedNextEntryId.Load())

	// Test writing an entry that exceeds the buffer size
	id, err = buffer.WriteEntry(7, []byte("data7"))
	assert.Error(t, err)
	assert.Equal(t, int64(-1), id)
	assert.Equal(t, int64(3), buffer.ExpectedNextEntryId.Load())

	// Test writing another valid entry
	id, err = buffer.WriteEntry(3, []byte("data3"))
	assert.NoError(t, err)
	assert.Equal(t, int64(3), id)
	assert.Equal(t, int64(5), buffer.ExpectedNextEntryId.Load())

}

// TestReadEntry tests the ReadEntry method.
func TestReadEntry(t *testing.T) {
	buffer := NewSequentialBuffer(1, 5)
	buffer.WriteEntry(1, []byte("data1"))
	buffer.WriteEntry(2, []byte("data2"))
	buffer.WriteEntry(4, []byte("data4"))

	// Test reading a valid entry
	value, err := buffer.ReadEntry(1)
	assert.NoError(t, err)
	assert.Equal(t, []byte("data1"), value)

	// Test reading another valid entry
	value, err = buffer.ReadEntry(2)
	assert.NoError(t, err)
	assert.Equal(t, []byte("data2"), value)

	// Test reading an entry with an invalid ID
	value, err = buffer.ReadEntry(3)
	assert.Error(t, err)
	assert.Nil(t, value)

	// Test reading another valid entry
	value, err = buffer.ReadEntry(4)
	assert.NoError(t, err)
	assert.Equal(t, []byte("data4"), value)

	// Test reading an entry that is out of bounds
	value, err = buffer.ReadEntry(7)
	assert.Error(t, err)
	assert.Nil(t, value)
}

// TestReadEntriesToLast tests the ReadBytesToLast method.
func TestReadEntriesToLast(t *testing.T) {
	buffer := NewSequentialBuffer(1, 5)
	buffer.WriteEntry(1, []byte("data1"))
	buffer.WriteEntry(2, []byte("data2"))
	buffer.WriteEntry(3, []byte("data3"))
	buffer.WriteEntry(5, []byte("data5"))

	// Test reading bytes range
	values, err := buffer.ReadEntriesToLast(3)
	assert.NoError(t, err)
	assert.Equal(t, len(values), 3)
	assert.Equal(t, [][]byte{[]byte("data3"), nil, []byte("data5")}, values)

	// Test reading bytes with an invalid start ID
	values, err = buffer.ReadEntriesToLast(6)
	assert.NoError(t, err)
	assert.Equal(t, len(values), 0)
	assert.Equal(t, [][]byte{}, values)

	// Test reading bytes with a start ID out of bounds
	values, err = buffer.ReadEntriesToLast(7)
	assert.Error(t, err)
	assert.Nil(t, values)

	// Test reading bytes to the last entry
	values, err = buffer.ReadEntriesToLast(1)
	assert.NoError(t, err)
	assert.Equal(t, len(values), 5)
	assert.Equal(t, [][]byte{[]byte("data1"), []byte("data2"), []byte("data3"), nil, []byte("data5")}, values)

	// Test reading bytes to the last entry
	values, err = buffer.ReadEntriesToLast(5)
	assert.NoError(t, err)
	assert.Equal(t, len(values), 1)
	assert.Equal(t, [][]byte{[]byte("data5")}, values)
}

// TestReadEntriesRange tests the ReadBytesRange method.
func TestReadEntriesRange(t *testing.T) {
	buffer := NewSequentialBuffer(1, 5)
	buffer.WriteEntry(1, []byte("data1"))
	buffer.WriteEntry(2, []byte("data2"))
	buffer.WriteEntry(3, []byte("data3"))
	buffer.WriteEntry(5, []byte("data5"))

	// Test reading bytes in a valid range
	values, err := buffer.ReadEntriesRange(1, 3)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, [][]byte{[]byte("data1"), []byte("data2")}, values)

	// Test reading a nil entry
	values, err = buffer.ReadEntriesRange(4, 5)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, [][]byte{nil}, values)

	// Test reading bytes with a start ID out of bounds
	values, err = buffer.ReadEntriesRange(7, 8)
	assert.Error(t, err)
	assert.Nil(t, values)

	values, err = buffer.ReadEntriesRange(1, 6)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(values))
	assert.Equal(t, [][]byte{[]byte("data1"), []byte("data2"), []byte("data3"), nil, []byte("data5")}, values)

	values, err = buffer.ReadEntriesRange(5, 6)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, [][]byte{[]byte("data5")}, values)

	values, err = buffer.ReadEntriesRange(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, [][]byte{[]byte("data1")}, values)

}

// TestReset tests the Reset method.
func TestReset(t *testing.T) {
	buffer := NewSequentialBuffer(1, 5)
	buffer.WriteEntry(1, []byte("data1"))
	buffer.WriteEntry(2, []byte("data2"))
	buffer.Reset()

	assert.Len(t, buffer.Values, 5)
	assert.Equal(t, int64(0), buffer.DataSize.Load())
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())
	assert.Equal(t, [][]byte{nil, nil, nil, nil, nil}, buffer.Values)
}
