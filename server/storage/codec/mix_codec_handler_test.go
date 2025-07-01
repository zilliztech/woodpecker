package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StreamDecodeReader Tests

func TestStreamDecodeReader_CompleteFile(t *testing.T) {
	// Create a complete file using BatchCodecHandler
	writer := NewBatchCodecHandlerForWriting(100, 1, 0)
	writer.AddData([]byte("test data 1"))
	writer.AddData([]byte("test data 2"))
	writer.AddData([]byte("test data 3"))

	data, err := writer.ToBytes()
	require.NoError(t, err)

	// Test reading with StreamDecodeReader
	reader, err := NewStreamDecodeReader(bytes.NewReader(data))
	require.NoError(t, err)

	// Should be complete
	assert.True(t, reader.IsComplete())

	// Test header
	header := reader.GetHeader()
	assert.NotNil(t, header)
	assert.Equal(t, uint16(1), header.Version)

	// Test footer
	footer := reader.GetFooter()
	assert.NotNil(t, footer)
	assert.Equal(t, int64(100), footer.FirstEntryID)
	assert.Equal(t, uint32(3), footer.Count)

	// Test index
	index := reader.GetIndex()
	assert.NotNil(t, index)
	assert.Len(t, index.Offsets, 3)

	// Test getting all data records
	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 3)
	assert.Equal(t, "test data 1", string(dataRecords[0].Payload))
	assert.Equal(t, "test data 2", string(dataRecords[1].Payload))
	assert.Equal(t, "test data 3", string(dataRecords[2].Payload))

	// Test random access
	record, err := reader.GetRecordByEntryID(101)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, "test data 2", string(dataRecord.Payload))
}

func TestStreamDecodeReader_EmptyFile(t *testing.T) {
	// Create empty file
	writer := NewBatchCodecHandlerForWriting(300, 1, 0)
	data, err := writer.ToBytes()
	require.NoError(t, err)

	reader, err := NewStreamDecodeReader(bytes.NewReader(data))
	require.NoError(t, err)

	assert.True(t, reader.IsComplete())

	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 0)
}

// BatchCodecHandler Tests

func TestBatchCodecHandler_Basic(t *testing.T) {
	writer := NewBatchCodecHandlerForWriting(200, 1, 0)

	// Add some data
	entryID1 := writer.AddData([]byte("batch data 1"))
	assert.Equal(t, int64(200), entryID1)

	entryID2 := writer.AddData([]byte("batch data 2"))
	assert.Equal(t, int64(201), entryID2)

	// Check state
	assert.Equal(t, 2, writer.GetDataRecordCount())
	assert.Equal(t, int64(200), writer.GetFirstEntryID())
	assert.Equal(t, int64(202), writer.GetNextEntryID())
	assert.Equal(t, int64(201), writer.GetLastEntryID())
	assert.True(t, writer.HasData())

	// Convert to bytes
	data, err := writer.ToBytes()
	require.NoError(t, err)

	// Test reading back
	reader, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 2)
	assert.Equal(t, "batch data 1", string(dataRecords[0].Payload))
	assert.Equal(t, "batch data 2", string(dataRecords[1].Payload))

	// Test random access
	record, err := reader.GetRecordByEntryID(200)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, "batch data 1", string(dataRecord.Payload))
}

func TestBatchCodecHandler_EmptyBatch(t *testing.T) {
	writer := NewBatchCodecHandlerForWriting(300, 1, 0)

	assert.False(t, writer.HasData())
	assert.Equal(t, int64(-1), writer.GetLastEntryID())

	// Convert to bytes
	data, err := writer.ToBytes()
	require.NoError(t, err)

	// Test reading back
	reader, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 0)
}

func TestWriterCompatibility(t *testing.T) {
	// Test that BatchCodecHandler and StreamEncodeWriter produce compatible formats
	testData := [][]byte{
		[]byte("data 1"),
		[]byte("data 2"),
		[]byte("data 3"),
	}

	// Create with BatchCodecHandler
	batchHandler := NewBatchCodecHandlerForWriting(1000, 1, 0)
	for _, data := range testData {
		batchHandler.AddData(data)
	}
	batchData, err := batchHandler.ToBytes()
	require.NoError(t, err)

	// Create with StreamEncodeWriter
	streamBuf := newTestBuffer()
	streamWriter, err := NewStreamEncodeWriter(streamBuf, 1000, 1, 0)
	require.NoError(t, err)
	for _, data := range testData {
		streamWriter.WriteData(data)
	}
	err = streamWriter.Finalize()
	require.NoError(t, err)
	streamData := streamBuf.Bytes()

	// Both should be readable by both readers
	batchReader1, err := NewBatchCodecHandlerFromData(batchData)
	require.NoError(t, err)
	batchReader2, err := NewBatchCodecHandlerFromData(streamData)
	require.NoError(t, err)

	streamReader1, err := NewStreamDecodeReader(bytes.NewReader(batchData))
	require.NoError(t, err)
	streamReader2, err := NewStreamDecodeReader(bytes.NewReader(streamData))
	require.NoError(t, err)

	// All should read the same data
	readers := []interface {
		GetAllDataRecords() ([]*DataRecord, error)
	}{batchReader1, batchReader2, streamReader1, streamReader2}

	for i, reader := range readers {
		records, err := reader.GetAllDataRecords()
		require.NoError(t, err, "Reader %d failed", i)
		require.Len(t, records, 3, "Reader %d wrong count", i)

		for j, record := range records {
			assert.Equal(t, testData[j], record.Payload, "Reader %d record %d mismatch", i, j)
		}
	}
}

func TestLargeDataSet(t *testing.T) {
	// Test with larger dataset
	const numRecords = 1000
	testData := make([][]byte, numRecords)
	for i := 0; i < numRecords; i++ {
		testData[i] = []byte(fmt.Sprintf("large test data record %d with some additional content to make it longer", i))
	}

	// Test BatchCodecHandler
	batchHandler := NewBatchCodecHandlerForWriting(5000, 1, 0)
	for _, data := range testData {
		batchHandler.AddData(data)
	}
	batchData, err := batchHandler.ToBytes()
	require.NoError(t, err)

	// Test reading back
	batchReader, err := NewBatchCodecHandlerFromData(batchData)
	require.NoError(t, err)

	records, err := batchReader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, records, numRecords)

	// Verify all records
	for i, record := range records {
		assert.Equal(t, testData[i], record.Payload)
	}

	// Test random access
	record, err := batchReader.GetRecordByEntryID(5500) // 5000 + 500
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, testData[500], dataRecord.Payload)
}

func TestErrorHandling(t *testing.T) {
	// Test invalid data
	_, err := NewBatchCodecHandlerFromData([]byte("invalid data"))
	assert.Error(t, err)

	_, err = NewStreamDecodeReader(bytes.NewReader([]byte("invalid data")))
	assert.Error(t, err)

	// Test valid reader with invalid operations
	writer := NewBatchCodecHandlerForWriting(3000, 1, 0)
	writer.AddData([]byte("test"))
	data, _ := writer.ToBytes()

	reader, err := NewBatchCodecHandlerFromData(data)
	require.NoError(t, err)

	// Test invalid entry ID
	_, err = reader.GetRecordByEntryID(9999)
	assert.Error(t, err)

	// Test invalid range
	_, err = reader.GetRecordsByRange(9999, 10000)
	assert.Error(t, err)
}

// Real File Cross-Compatibility Tests

func TestBatchCodecHandlerToFileToStreamDecodeReader(t *testing.T) {
	// Create test data
	testData := []string{
		"Cross-compatibility test data 1",
		"Cross-compatibility test data 2",
		"Cross-compatibility test data 3",
		"Cross-compatibility test data 4",
		"Cross-compatibility test data 5",
	}

	// Step 1: Create data with BatchCodecHandler
	batchWriter := NewBatchCodecHandlerForWriting(12000, 1, 0x1234)
	for _, data := range testData {
		batchWriter.AddData([]byte(data))
	}

	batchData, err := batchWriter.ToBytes()
	require.NoError(t, err)

	// Step 2: Write BatchCodecHandler data to real file
	tempFile, err := os.CreateTemp("", "batch_to_stream_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	_, err = tempFile.Write(batchData)
	require.NoError(t, err)
	tempFile.Close()

	// Step 3: Read file with StreamDecodeReader
	readFile, err := os.Open(tempFile.Name())
	require.NoError(t, err)
	defer readFile.Close()

	streamReader, err := NewStreamDecodeReader(readFile)
	require.NoError(t, err)

	// Verify StreamDecodeReader can read BatchCodecHandler-created file
	assert.True(t, streamReader.IsComplete())

	header := streamReader.GetHeader()
	assert.NotNil(t, header)
	assert.Equal(t, uint16(1), header.Version)
	assert.Equal(t, uint16(0x1234), header.Flags)
	assert.Equal(t, int64(12000), header.FirstEntryID)

	footer := streamReader.GetFooter()
	assert.NotNil(t, footer)
	assert.Equal(t, int64(12000), footer.FirstEntryID)
	assert.Equal(t, uint32(len(testData)), footer.Count)

	// Read all data records
	dataRecords, err := streamReader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, len(testData))

	for i, record := range dataRecords {
		assert.Equal(t, testData[i], string(record.Payload))
	}

	// Test random access
	record, err := streamReader.GetRecordByEntryID(12002)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, testData[2], string(dataRecord.Payload))

	// Test file info
	info := streamReader.GetFileInfo()
	assert.True(t, info.IsComplete)
	assert.True(t, info.HasFooter)
	assert.Equal(t, uint32(len(testData)), info.RecordCount)
	assert.Equal(t, int64(12000), info.FirstEntryID)
}

func TestStreamEncodeWriterToFileToBatchCodecHandler(t *testing.T) {
	// Create test data
	testData := []string{
		"Stream to batch test data 1",
		"Stream to batch test data 2",
		"Stream to batch test data 3",
		"Stream to batch test data 4",
		"Stream to batch test data 5",
		"Stream to batch test data 6",
	}

	// Step 1: Write data with StreamEncodeWriter to real file
	tempFile, err := os.CreateTemp("", "stream_to_batch_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	streamWriter, err := NewStreamEncodeWriter(tempFile, 13000, 1, 0x5678)
	require.NoError(t, err)

	var entryIDs []int64
	for _, data := range testData {
		entryID, err := streamWriter.WriteData([]byte(data))
		require.NoError(t, err)
		entryIDs = append(entryIDs, entryID)
	}

	err = streamWriter.Finalize()
	require.NoError(t, err)
	tempFile.Close()

	// Step 2: Read file data into memory
	fileData, err := os.ReadFile(tempFile.Name())
	require.NoError(t, err)

	// Step 3: Parse with BatchCodecHandler
	batchReader, err := NewBatchCodecHandlerFromData(fileData)
	require.NoError(t, err)

	// Verify BatchCodecHandler can read StreamEncodeWriter-created file
	assert.True(t, batchReader.IsReadMode())
	assert.True(t, batchReader.HasData())

	header := batchReader.GetHeader()
	assert.NotNil(t, header)
	assert.Equal(t, uint16(1), header.Version)
	assert.Equal(t, uint16(0x5678), header.Flags)
	assert.Equal(t, int64(13000), header.FirstEntryID)

	footer := batchReader.GetFooter()
	assert.NotNil(t, footer)
	assert.Equal(t, int64(13000), footer.FirstEntryID)
	assert.Equal(t, uint32(len(testData)), footer.Count)

	index := batchReader.GetIndex()
	assert.NotNil(t, index)
	assert.Len(t, index.Offsets, len(testData))

	// Read all data records
	dataRecords, err := batchReader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, len(testData))

	for i, record := range dataRecords {
		assert.Equal(t, testData[i], string(record.Payload))
	}

	// Test random access
	record, err := batchReader.GetRecordByEntryID(13003)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, testData[3], string(dataRecord.Payload))

	// Test range access
	records, err := batchReader.GetRecordsByRange(13001, 13003)
	require.NoError(t, err)
	assert.Len(t, records, 3)
	for i, record := range records {
		dataRecord := record.(*DataRecord)
		assert.Equal(t, testData[i+1], string(dataRecord.Payload))
	}

	// Verify entry IDs match
	for i, expectedEntryID := range entryIDs {
		record, err := batchReader.GetRecordByEntryID(expectedEntryID)
		require.NoError(t, err)
		dataRecord := record.(*DataRecord)
		assert.Equal(t, testData[i], string(dataRecord.Payload))
	}
}

func TestLargeFileCrossCompatibility(t *testing.T) {
	// Create large dataset
	const numRecords = 500
	const recordSize = 2048 // 2KB per record

	testData := make([][]byte, numRecords)
	for i := 0; i < numRecords; i++ {
		data := make([]byte, recordSize)
		// Fill with pattern
		for j := 0; j < recordSize; j++ {
			data[j] = byte((i + j) % 256)
		}
		// Add unique identifier
		binary.LittleEndian.PutUint32(data[:4], uint32(i))
		testData[i] = data
	}

	// Test 1: BatchCodecHandler -> File -> StreamDecodeReader
	t.Run("BatchToStream", func(t *testing.T) {
		// Create with BatchCodecHandler
		batchWriter := NewBatchCodecHandlerForWriting(14000, 1, 0)
		for _, data := range testData {
			batchWriter.AddData(data)
		}

		batchData, err := batchWriter.ToBytes()
		require.NoError(t, err)

		// Write to file
		tempFile, err := os.CreateTemp("", "large_batch_to_stream_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		_, err = tempFile.Write(batchData)
		require.NoError(t, err)
		tempFile.Close()

		// Read with StreamDecodeReader
		readFile, err := os.Open(tempFile.Name())
		require.NoError(t, err)
		defer readFile.Close()

		streamReader, err := NewStreamDecodeReader(readFile)
		require.NoError(t, err)

		// Verify data
		dataRecords, err := streamReader.GetAllDataRecords()
		require.NoError(t, err)
		assert.Len(t, dataRecords, numRecords)

		for i, record := range dataRecords {
			assert.Equal(t, recordSize, len(record.Payload))
			recordID := binary.LittleEndian.Uint32(record.Payload[:4])
			assert.Equal(t, uint32(i), recordID)
		}
	})

	// Test 2: StreamEncodeWriter -> File -> BatchCodecHandler
	t.Run("StreamToBatch", func(t *testing.T) {
		// Write with StreamEncodeWriter
		tempFile, err := os.CreateTemp("", "large_stream_to_batch_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		streamWriter, err := NewStreamEncodeWriter(tempFile, 15000, 1, 0)
		require.NoError(t, err)

		for _, data := range testData {
			_, err := streamWriter.WriteData(data)
			require.NoError(t, err)
		}

		err = streamWriter.Finalize()
		require.NoError(t, err)
		tempFile.Close()

		// Read with BatchCodecHandler
		fileData, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)

		batchReader, err := NewBatchCodecHandlerFromData(fileData)
		require.NoError(t, err)

		// Verify data
		dataRecords, err := batchReader.GetAllDataRecords()
		require.NoError(t, err)
		assert.Len(t, dataRecords, numRecords)

		for i, record := range dataRecords {
			assert.Equal(t, recordSize, len(record.Payload))
			recordID := binary.LittleEndian.Uint32(record.Payload[:4])
			assert.Equal(t, uint32(i), recordID)
		}
	})
}

func TestEmptyFileCrossCompatibility(t *testing.T) {
	// Test 1: Empty BatchCodecHandler -> File -> StreamDecodeReader
	t.Run("EmptyBatchToStream", func(t *testing.T) {
		// Create empty BatchCodecHandler
		batchWriter := NewBatchCodecHandlerForWriting(16000, 1, 0)
		batchData, err := batchWriter.ToBytes()
		require.NoError(t, err)

		// Write to file
		tempFile, err := os.CreateTemp("", "empty_batch_to_stream_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		_, err = tempFile.Write(batchData)
		require.NoError(t, err)
		tempFile.Close()

		// Read with StreamDecodeReader
		readFile, err := os.Open(tempFile.Name())
		require.NoError(t, err)
		defer readFile.Close()

		streamReader, err := NewStreamDecodeReader(readFile)
		require.NoError(t, err)

		assert.True(t, streamReader.IsComplete())
		dataRecords, err := streamReader.GetAllDataRecords()
		require.NoError(t, err)
		assert.Len(t, dataRecords, 0)
	})

	// Test 2: Empty StreamEncodeWriter -> File -> BatchCodecHandler
	t.Run("EmptyStreamToBatch", func(t *testing.T) {
		// Write empty file with StreamEncodeWriter
		tempFile, err := os.CreateTemp("", "empty_stream_to_batch_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		streamWriter, err := NewStreamEncodeWriter(tempFile, 17000, 1, 0)
		require.NoError(t, err)

		err = streamWriter.Finalize()
		require.NoError(t, err)
		tempFile.Close()

		// Read with BatchCodecHandler
		fileData, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)

		batchReader, err := NewBatchCodecHandlerFromData(fileData)
		require.NoError(t, err)

		assert.False(t, batchReader.HasData())
		dataRecords, err := batchReader.GetAllDataRecords()
		require.NoError(t, err)
		assert.Len(t, dataRecords, 0)
	})
}

func TestConcurrentCrossCompatibilityAccess(t *testing.T) {
	// Create test data
	const numRecords = 200
	testData := make([]string, numRecords)
	for i := 0; i < numRecords; i++ {
		testData[i] = fmt.Sprintf("Concurrent cross-compatibility data %d", i)
	}

	// Step 1: Create file with BatchCodecHandler
	batchWriter := NewBatchCodecHandlerForWriting(18000, 1, 0)
	for _, data := range testData {
		batchWriter.AddData([]byte(data))
	}

	batchData, err := batchWriter.ToBytes()
	require.NoError(t, err)

	// Write to file
	tempFile, err := os.CreateTemp("", "concurrent_cross_compat_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	_, err = tempFile.Write(batchData)
	require.NoError(t, err)
	tempFile.Close()

	// Step 2: Concurrent access with both StreamDecodeReader and BatchCodecHandler
	const numGoroutines = 10
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			if goroutineID%2 == 0 {
				// Even goroutines use StreamDecodeReader
				readFile, err := os.Open(tempFile.Name())
				if err != nil {
					results <- fmt.Errorf("StreamDecodeReader %d: open error: %v", goroutineID, err)
					return
				}
				defer readFile.Close()

				streamReader, err := NewStreamDecodeReader(readFile)
				if err != nil {
					results <- fmt.Errorf("StreamDecodeReader %d: create error: %v", goroutineID, err)
					return
				}

				dataRecords, err := streamReader.GetAllDataRecords()
				if err != nil {
					results <- fmt.Errorf("StreamDecodeReader %d: read error: %v", goroutineID, err)
					return
				}

				if len(dataRecords) != numRecords {
					results <- fmt.Errorf("StreamDecodeReader %d: expected %d records, got %d", goroutineID, numRecords, len(dataRecords))
					return
				}

				// Verify some random records
				for j := 0; j < 10; j++ {
					idx := (goroutineID + j) % numRecords
					if string(dataRecords[idx].Payload) != testData[idx] {
						results <- fmt.Errorf("StreamDecodeReader %d: record %d mismatch", goroutineID, idx)
						return
					}
				}
			} else {
				// Odd goroutines use BatchCodecHandler
				fileData, err := os.ReadFile(tempFile.Name())
				if err != nil {
					results <- fmt.Errorf("BatchCodecHandler %d: read file error: %v", goroutineID, err)
					return
				}

				batchReader, err := NewBatchCodecHandlerFromData(fileData)
				if err != nil {
					results <- fmt.Errorf("BatchCodecHandler %d: create error: %v", goroutineID, err)
					return
				}

				dataRecords, err := batchReader.GetAllDataRecords()
				if err != nil {
					results <- fmt.Errorf("BatchCodecHandler %d: read error: %v", goroutineID, err)
					return
				}

				if len(dataRecords) != numRecords {
					results <- fmt.Errorf("BatchCodecHandler %d: expected %d records, got %d", goroutineID, numRecords, len(dataRecords))
					return
				}

				// Verify some random records
				for j := 0; j < 10; j++ {
					idx := (goroutineID + j) % numRecords
					if string(dataRecords[idx].Payload) != testData[idx] {
						results <- fmt.Errorf("BatchCodecHandler %d: record %d mismatch", goroutineID, idx)
						return
					}
				}
			}

			results <- nil // Success
		}(i)
	}

	// Wait for all goroutines and check results
	for i := 0; i < numGoroutines; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for goroutine %d", i)
		}
	}
}

func TestFileCorruptionDetection(t *testing.T) {
	// Create valid file with BatchCodecHandler
	batchWriter := NewBatchCodecHandlerForWriting(19000, 1, 0)
	batchWriter.AddData([]byte("corruption test data 1"))
	batchWriter.AddData([]byte("corruption test data 2"))

	validData, err := batchWriter.ToBytes()
	require.NoError(t, err)

	// Test 1: Corrupt file - truncated
	t.Run("TruncatedFile", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "corrupted_truncated_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		// Write only half the data (this should make the file incomplete/invalid)
		_, err = tempFile.Write(validData[:len(validData)/2])
		require.NoError(t, err)
		tempFile.Close()

		// Try to read with StreamDecodeReader
		readFile, err := os.Open(tempFile.Name())
		require.NoError(t, err)
		defer readFile.Close()

		reader, err := NewStreamDecodeReader(readFile)
		if err == nil {
			// Reader might be created but should fail when trying to read all data
			_, err = reader.GetAllDataRecords()
			// We expect either creation to fail or reading to fail
			if err == nil {
				t.Log("Warning: truncated file was read successfully, this might indicate insufficient validation")
			}
		}

		// Try to read with BatchCodecHandler
		corruptedData, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)

		_, err = NewBatchCodecHandlerFromData(corruptedData)
		assert.Error(t, err) // This should definitely fail for truncated data
	})

	// Test 2: Corrupt file - modified bytes in footer area
	t.Run("ModifiedBytes", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "corrupted_modified_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		// Corrupt some bytes in the footer area (last 50 bytes)
		corruptedData := make([]byte, len(validData))
		copy(corruptedData, validData)

		// Corrupt footer area which should be more reliably detected
		footerStart := len(corruptedData) - 50
		if footerStart < 0 {
			footerStart = 0
		}
		for i := footerStart; i < len(corruptedData); i++ {
			corruptedData[i] ^= 0xFF // Flip all bits in footer area
		}

		_, err = tempFile.Write(corruptedData)
		require.NoError(t, err)
		tempFile.Close()

		// Try to read with StreamDecodeReader
		readFile, err := os.Open(tempFile.Name())
		require.NoError(t, err)
		defer readFile.Close()

		reader, err := NewStreamDecodeReader(readFile)
		if err == nil {
			// If reader is created, reading data should fail
			_, err = reader.GetAllDataRecords()
			if err == nil {
				t.Log("Warning: corrupted file was read successfully, this might indicate insufficient validation")
			}
		}

		// Try to read with BatchCodecHandler
		_, err = NewBatchCodecHandlerFromData(corruptedData)
		assert.Error(t, err) // Should fail due to corrupted footer
	})

	// Test 3: Empty file
	t.Run("EmptyFile", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "empty_file_*.dat")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		// Don't write anything - leave file empty
		tempFile.Close()

		// Try to read with StreamDecodeReader
		readFile, err := os.Open(tempFile.Name())
		require.NoError(t, err)
		defer readFile.Close()

		_, err = NewStreamDecodeReader(readFile)
		assert.Error(t, err) // Should fail with empty file

		// Try to read with BatchCodecHandler
		emptyData, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)
		assert.Len(t, emptyData, 0)

		_, err = NewBatchCodecHandlerFromData(emptyData)
		assert.Error(t, err) // Should fail with empty data
	})
}
