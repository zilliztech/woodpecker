package codec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchCodecHandler_WriteMode(t *testing.T) {
	// Create handler in write mode
	handler := NewBatchCodecHandlerForWriting(1000, 1, 0)

	// Verify initial state
	assert.True(t, handler.IsWriteMode())
	assert.False(t, handler.IsReadMode())
	assert.False(t, handler.HasData())
	assert.Equal(t, 0, handler.GetDataRecordCount())
	assert.Equal(t, int64(1000), handler.GetFirstEntryID())
	assert.Equal(t, int64(1000), handler.GetNextEntryID())
	assert.Equal(t, int64(-1), handler.GetLastEntryID())

	// Add some data
	entryID1 := handler.AddData([]byte("data 1"))
	entryID2 := handler.AddData([]byte("data 2"))
	entryID3 := handler.AddData([]byte("data 3"))

	assert.Equal(t, int64(1000), entryID1)
	assert.Equal(t, int64(1001), entryID2)
	assert.Equal(t, int64(1002), entryID3)

	// Verify state after adding data
	assert.True(t, handler.HasData())
	assert.Equal(t, 3, handler.GetDataRecordCount())
	assert.Equal(t, int64(1003), handler.GetNextEntryID())
	assert.Equal(t, int64(1002), handler.GetLastEntryID())

	// Get all data records
	dataRecords, err := handler.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 3)
	assert.Equal(t, "data 1", string(dataRecords[0].Payload))
	assert.Equal(t, "data 2", string(dataRecords[1].Payload))
	assert.Equal(t, "data 3", string(dataRecords[2].Payload))

	// Verify header
	header := handler.GetHeader()
	assert.NotNil(t, header)
	assert.Equal(t, uint16(1), header.Version)
	assert.Equal(t, uint16(0), header.Flags)
	assert.Equal(t, int64(1000), header.FirstEntryID)
}

func TestBatchCodecHandler_ReadMode(t *testing.T) {
	// First create some data using write mode
	writeHandler := NewBatchCodecHandlerForWriting(2000, 1, 0)
	writeHandler.AddData([]byte("read data 1"))
	writeHandler.AddData([]byte("read data 2"))
	writeHandler.AddData([]byte("read data 3"))

	data, err := writeHandler.ToBytes()
	require.NoError(t, err)

	// Now create handler in read mode
	readHandler, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	// Verify initial state
	assert.False(t, readHandler.IsWriteMode())
	assert.True(t, readHandler.IsReadMode())
	assert.True(t, readHandler.HasData())
	assert.Equal(t, 3, readHandler.GetDataRecordCount())
	assert.Equal(t, int64(2000), readHandler.GetFirstEntryID())
	assert.Equal(t, int64(2002), readHandler.GetLastEntryID())

	// Verify header
	header := readHandler.GetHeader()
	assert.NotNil(t, header)
	assert.Equal(t, uint16(1), header.Version)
	assert.Equal(t, uint16(0), header.Flags)
	assert.Equal(t, int64(2000), header.FirstEntryID)

	// Verify footer
	footer := readHandler.GetFooter()
	assert.NotNil(t, footer)
	assert.Equal(t, int64(2000), footer.FirstEntryID)
	assert.Equal(t, uint32(3), footer.Count)

	// Verify index
	index := readHandler.GetIndex()
	assert.NotNil(t, index)
	assert.Len(t, index.Offsets, 3)

	// Get all data records
	dataRecords, err := readHandler.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 3)
	assert.Equal(t, "read data 1", string(dataRecords[0].Payload))
	assert.Equal(t, "read data 2", string(dataRecords[1].Payload))
	assert.Equal(t, "read data 3", string(dataRecords[2].Payload))

	// Test random access
	record, err := readHandler.GetRecordByEntryID(2001)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, "read data 2", string(dataRecord.Payload))

	// Test range access
	records, err := readHandler.GetRecordsByRange(2000, 2001)
	require.NoError(t, err)
	assert.Len(t, records, 2)
}

func TestBatchCodecHandler_RoundTrip(t *testing.T) {
	// Create original data
	originalHandler := NewBatchCodecHandlerForWriting(3000, 1, 0x1234)
	originalHandler.AddData([]byte("round trip 1"))
	originalHandler.AddData([]byte("round trip 2"))

	// Serialize to bytes
	data, err := originalHandler.ToBytes()
	require.NoError(t, err)

	// Read back from bytes
	readHandler, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	// Verify data matches
	originalRecords, err := originalHandler.GetAllDataRecords()
	require.NoError(t, err)

	readRecords, err := readHandler.GetAllDataRecords()
	require.NoError(t, err)

	assert.Len(t, readRecords, len(originalRecords))
	for i, record := range readRecords {
		assert.Equal(t, originalRecords[i].Payload, record.Payload)
	}

	// Verify headers match
	assert.Equal(t, originalHandler.GetHeader().Version, readHandler.GetHeader().Version)
	assert.Equal(t, originalHandler.GetHeader().Flags, readHandler.GetHeader().Flags)
	assert.Equal(t, originalHandler.GetHeader().FirstEntryID, readHandler.GetHeader().FirstEntryID)

	// Verify counts match
	assert.Equal(t, originalHandler.GetDataRecordCount(), readHandler.GetDataRecordCount())
	assert.Equal(t, originalHandler.GetFirstEntryID(), readHandler.GetFirstEntryID())
	assert.Equal(t, originalHandler.GetLastEntryID(), readHandler.GetLastEntryID())
}

func TestBatchCodecHandler_EmptyData(t *testing.T) {
	// Test empty write mode
	writeHandler := NewBatchCodecHandlerForWriting(4000, 1, 0)
	assert.False(t, writeHandler.HasData())
	assert.Equal(t, 0, writeHandler.GetDataRecordCount())
	assert.Equal(t, int64(-1), writeHandler.GetLastEntryID())

	data, err := writeHandler.ToBytes()
	require.NoError(t, err)

	// Test empty read mode
	readHandler, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	assert.False(t, readHandler.HasData())
	assert.Equal(t, 0, readHandler.GetDataRecordCount())
	assert.Equal(t, int64(-1), readHandler.GetLastEntryID())

	dataRecords, err := readHandler.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 0)
}

func TestBatchCodecHandler_ReadModeCannotWrite(t *testing.T) {
	// Create some data
	writeHandler := NewBatchCodecHandlerForWriting(5000, 1, 0)
	writeHandler.AddData([]byte("test"))
	data, err := writeHandler.ToBytes()
	require.NoError(t, err)

	// Create read handler
	readHandler, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	// Try to add data (should fail)
	entryID := readHandler.AddData([]byte("should fail"))
	assert.Equal(t, int64(-1), entryID)
}

func TestBatchCodecHandler_FileInfo(t *testing.T) {
	// Test write mode file info
	writeHandler := NewBatchCodecHandlerForWriting(6000, 1, 0x5678)
	writeHandler.AddData([]byte("info test 1"))
	writeHandler.AddData([]byte("info test 2"))

	writeInfo := writeHandler.GetFileInfo()
	assert.Equal(t, uint16(1), writeInfo.Version)
	assert.Equal(t, uint16(0x5678), writeInfo.Flags)
	assert.Equal(t, int64(6000), writeInfo.FirstEntryID)
	assert.Equal(t, uint32(2), writeInfo.RecordCount)
	assert.True(t, writeInfo.IsComplete)
	assert.False(t, writeInfo.HasFooter) // Write mode doesn't have footer yet

	// Serialize and read back
	data, err := writeHandler.ToBytes()
	require.NoError(t, err)

	readHandler, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	readInfo := readHandler.GetFileInfo()
	assert.Equal(t, uint16(1), readInfo.Version)
	assert.Equal(t, uint16(0x5678), readInfo.Flags)
	assert.Equal(t, int64(6000), readInfo.FirstEntryID)
	assert.Equal(t, uint32(2), readInfo.RecordCount)
	assert.True(t, readInfo.IsComplete)
	assert.True(t, readInfo.HasFooter) // Read mode has footer
	assert.Greater(t, readInfo.IndexOffset, uint64(0))
	assert.Greater(t, readInfo.IndexLength, uint32(0))
}

func TestBatchCodecHandler_LargeDataSet(t *testing.T) {
	// Create large dataset
	handler := NewBatchCodecHandlerForWriting(10000, 1, 0)

	const numRecords = 100
	expectedData := make([]string, numRecords)
	for i := 0; i < numRecords; i++ {
		data := fmt.Sprintf("large record %d with content", i)
		expectedData[i] = data
		entryID := handler.AddData([]byte(data))
		assert.Equal(t, int64(10000+i), entryID)
	}

	// Serialize to bytes
	serializedData, err := handler.ToBytes()
	require.NoError(t, err)

	// Read back
	readHandler, err := NewBatchCodecHandlerFromData(serializedData)
	require.NoError(t, err)

	// Verify all records
	dataRecords, err := readHandler.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, numRecords)

	for i, record := range dataRecords {
		assert.Equal(t, expectedData[i], string(record.Payload))
	}

	// Test random access
	record, err := readHandler.GetRecordByEntryID(10050)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, expectedData[50], string(dataRecord.Payload))

	// Test range access
	records, err := readHandler.GetRecordsByRange(10000, 10004)
	require.NoError(t, err)
	assert.Len(t, records, 5)
	for i, record := range records {
		dataRecord := record.(*DataRecord)
		assert.Equal(t, expectedData[i], string(dataRecord.Payload))
	}
}

func TestBatchCodecHandler_ErrorHandling(t *testing.T) {
	// Test invalid data
	_, err := NewBatchCodecHandlerFromData([]byte("invalid data"))
	assert.Error(t, err)

	// Test empty data
	_, err = NewBatchCodecHandlerFromData([]byte{})
	assert.Error(t, err)

	// Create valid handler for other error tests
	handler := NewBatchCodecHandlerForWriting(7000, 1, 0)
	handler.AddData([]byte("test"))
	data, err := handler.ToBytes()
	require.NoError(t, err)

	readHandler, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	// Test invalid entry ID access
	_, err = readHandler.GetRecordByEntryID(6999) // Too small
	assert.Error(t, err)

	_, err = readHandler.GetRecordByEntryID(7001) // Too large
	assert.Error(t, err)

	// Test invalid range
	_, err = readHandler.GetRecordsByRange(7000, 7001) // End too large
	assert.Error(t, err)
}
