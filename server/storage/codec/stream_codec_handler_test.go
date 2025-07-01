package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBuffer implements io.WriteSeeker for testing
type testBuffer struct {
	*bytes.Buffer
	pos int64 // Track current position
}

func (tb *testBuffer) Write(p []byte) (n int, err error) {
	n, err = tb.Buffer.Write(p)
	if err == nil {
		tb.pos += int64(n)
	}
	return n, err
}

func (tb *testBuffer) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	size := int64(tb.Len())

	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = tb.pos + offset
	case io.SeekEnd:
		newPos = size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}

	// For writes beyond current size, we'll extend when Write is called
	tb.pos = newPos
	return newPos, nil
}

func newTestBuffer() *testBuffer {
	return &testBuffer{Buffer: &bytes.Buffer{}}
}

// StreamEncodeWriter Tests

func TestStreamEncodeWriter_Basic(t *testing.T) {
	buf := newTestBuffer()
	writer, err := NewStreamEncodeWriter(buf, 1000, 0)
	require.NoError(t, err)

	// Test initial state
	assert.Equal(t, int64(1000), writer.GetFirstEntryID())
	assert.Equal(t, int64(1000), writer.GetNextEntryID())
	assert.Equal(t, int64(-1), writer.GetLastEntryID())
	assert.Equal(t, 0, writer.GetDataRecordCount())
	assert.False(t, writer.HasData())
	assert.False(t, writer.IsFinalized())

	// Write some data
	entryID1, err := writer.WriteData([]byte("test data 1"))
	require.NoError(t, err)
	assert.Equal(t, int64(1000), entryID1)

	entryID2, err := writer.WriteData([]byte("test data 2"))
	require.NoError(t, err)
	assert.Equal(t, int64(1001), entryID2)

	// Test state after writing
	assert.Equal(t, int64(1002), writer.GetNextEntryID())
	assert.Equal(t, int64(1001), writer.GetLastEntryID())
	assert.Equal(t, 2, writer.GetDataRecordCount())
	assert.True(t, writer.HasData())
	assert.False(t, writer.IsFinalized())

	// Finalize
	err = writer.Finalize()
	require.NoError(t, err)
	assert.True(t, writer.IsFinalized())

	// Try to write after finalization (should fail)
	_, err = writer.WriteData([]byte("should fail"))
	assert.Error(t, err)

	// Try to finalize again (should fail)
	err = writer.Finalize()
	assert.Error(t, err)
}

func TestStreamEncodeWriter_ErrorHandling(t *testing.T) {
	buf := newTestBuffer()
	writer, err := NewStreamEncodeWriter(buf, 2000, 0)
	require.NoError(t, err)

	// Test writing after finalization
	err = writer.Finalize()
	require.NoError(t, err)

	_, err = writer.WriteData([]byte("should fail"))
	assert.Error(t, err)
}

func TestStreamEncodeWriter_EmptyFile(t *testing.T) {
	buf := newTestBuffer()
	writer, err := NewStreamEncodeWriter(buf, 3000, 0)
	require.NoError(t, err)

	// Finalize without writing any data
	err = writer.Finalize()
	require.NoError(t, err)

	assert.True(t, writer.IsFinalized())
	assert.False(t, writer.HasData())
	assert.Equal(t, 0, writer.GetDataRecordCount())
}

// StreamDecodeReader Tests

func TestStreamDecodeReader_IncompleteFile(t *testing.T) {
	// Create an incomplete file (header + data, no index/footer)
	buf := newTestBuffer()
	writer, err := NewStreamEncodeWriter(buf, 200, 0)
	require.NoError(t, err)

	// Write some data but don't finalize
	writer.WriteData([]byte("incomplete data 1"))
	writer.WriteData([]byte("incomplete data 2"))

	// Test reading incomplete file
	reader, err := NewStreamDecodeReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Should not be complete
	assert.False(t, reader.IsComplete())

	// Should have header but no footer/index
	assert.NotNil(t, reader.GetHeader())
	assert.Nil(t, reader.GetFooter())
	assert.Nil(t, reader.GetIndex())

	// Should be able to read data sequentially
	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 2)
	assert.Equal(t, "incomplete data 1", string(dataRecords[0].Payload))
	assert.Equal(t, "incomplete data 2", string(dataRecords[1].Payload))

	// Random access should fail
	_, err = reader.GetRecordByEntryID(200)
	assert.Error(t, err)
}

// ConcurrentBuffer is a helper type that allows concurrent read/write access
// to simulate a file that's being written to while being read by multiple readers
// Each reader maintains its own position while sharing the same underlying data
type ConcurrentBuffer struct {
	data []byte
	mu   sync.RWMutex
}

func NewConcurrentBuffer() *ConcurrentBuffer {
	return &ConcurrentBuffer{
		data: make([]byte, 0),
	}
}

func (cb *ConcurrentBuffer) Write(p []byte) (n int, err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.data = append(cb.data, p...)
	return len(p), nil
}

func (cb *ConcurrentBuffer) Bytes() []byte {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	// Return a copy to avoid race conditions
	result := make([]byte, len(cb.data))
	copy(result, cb.data)
	return result
}

func (cb *ConcurrentBuffer) Len() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return len(cb.data)
}

// ConcurrentBufferReader provides a reader interface for ConcurrentBuffer
// Each reader maintains its own snapshot of data and position
type ConcurrentBufferReader struct {
	data []byte // snapshot of data when reader was created
	pos  int64
}

func (cb *ConcurrentBuffer) NewReader() *ConcurrentBufferReader {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	// Create a snapshot of current data
	snapshot := make([]byte, len(cb.data))
	copy(snapshot, cb.data)

	return &ConcurrentBufferReader{
		data: snapshot,
		pos:  0,
	}
}

func (cbr *ConcurrentBufferReader) Read(p []byte) (n int, err error) {
	if cbr.pos >= int64(len(cbr.data)) {
		return 0, io.EOF
	}

	n = copy(p, cbr.data[cbr.pos:])
	cbr.pos += int64(n)
	return n, nil
}

func (cbr *ConcurrentBufferReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = cbr.pos + offset
	case io.SeekEnd:
		newPos = int64(len(cbr.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}

	cbr.pos = newPos
	return cbr.pos, nil
}

// ConcurrentWriteSeeker combines writer and reader functionality
type ConcurrentWriteSeeker struct {
	buffer *ConcurrentBuffer
	pos    int64
}

func NewConcurrentWriteSeeker() *ConcurrentWriteSeeker {
	return &ConcurrentWriteSeeker{
		buffer: NewConcurrentBuffer(),
		pos:    0,
	}
}

func (cws *ConcurrentWriteSeeker) Write(p []byte) (n int, err error) {
	n, err = cws.buffer.Write(p)
	if err == nil {
		cws.pos += int64(n)
	}
	return n, err
}

func (cws *ConcurrentWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	cws.buffer.mu.RLock()
	defer cws.buffer.mu.RUnlock()

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = cws.pos + offset
	case io.SeekEnd:
		newPos = int64(len(cws.buffer.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}

	cws.pos = newPos
	return cws.pos, nil
}

func (cws *ConcurrentWriteSeeker) NewReader() *ConcurrentBufferReader {
	return cws.buffer.NewReader()
}

// Advanced StreamDecodeReader Tests

func TestConcurrentStreamEncodeWriterReaders(t *testing.T) {
	// Create a shared buffer that supports concurrent read/write
	sharedBuffer := NewConcurrentWriteSeeker()

	// Create a StreamEncodeWriter
	writer, err := NewStreamEncodeWriter(sharedBuffer, 5000, 0)
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Test 1: Write first record
	entryID1, err := writer.WriteData([]byte("First record"))
	assert.NoError(t, err)
	assert.Equal(t, int64(5000), entryID1)

	// Create readers after writing first record
	reader1, err := NewStreamDecodeReader(sharedBuffer.NewReader())
	assert.NoError(t, err)
	assert.NotNil(t, reader1)

	reader2, err := NewStreamDecodeReader(sharedBuffer.NewReader())
	assert.NoError(t, err)
	assert.NotNil(t, reader2)

	// Readers should see incomplete file with header + first record
	assert.False(t, reader1.IsComplete())
	assert.False(t, reader2.IsComplete())

	// Both readers should be able to read the first record sequentially
	dataRecords1, err := reader1.GetAllDataRecords()
	assert.NoError(t, err)
	assert.Len(t, dataRecords1, 1)
	assert.Equal(t, "First record", string(dataRecords1[0].Payload))

	dataRecords2, err := reader2.GetAllDataRecords()
	assert.NoError(t, err)
	assert.Len(t, dataRecords2, 1)
	assert.Equal(t, "First record", string(dataRecords2[0].Payload))

	// Test 2: Write more records
	entryID2, err := writer.WriteData([]byte("Second record"))
	assert.NoError(t, err)
	assert.Equal(t, int64(5001), entryID2)

	entryID3, err := writer.WriteData([]byte("Third record"))
	assert.NoError(t, err)
	assert.Equal(t, int64(5002), entryID3)

	// Test 3: Finalize the writer
	err = writer.Finalize()
	assert.NoError(t, err)

	// Test 4: Create new readers after finalization
	completeReader1, err := NewStreamDecodeReader(sharedBuffer.NewReader())
	assert.NoError(t, err)
	assert.True(t, completeReader1.IsComplete())

	completeReader2, err := NewStreamDecodeReader(sharedBuffer.NewReader())
	assert.NoError(t, err)
	assert.True(t, completeReader2.IsComplete())

	// Complete readers should be able to read all records
	allRecords1, err := completeReader1.GetAllDataRecords()
	assert.NoError(t, err)
	assert.Len(t, allRecords1, 3)

	allRecords2, err := completeReader2.GetAllDataRecords()
	assert.NoError(t, err)
	assert.Len(t, allRecords2, 3)

	// Test random access with complete readers
	record, err := completeReader1.GetRecordByEntryID(5001)
	assert.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, "Second record", string(dataRecord.Payload))

	record, err = completeReader2.GetRecordByEntryID(5002)
	assert.NoError(t, err)
	dataRecord = record.(*DataRecord)
	assert.Equal(t, "Third record", string(dataRecord.Payload))

	// Test 5: Verify file info
	info1 := completeReader1.GetFileInfo()
	info2 := completeReader2.GetFileInfo()

	assert.True(t, info1.IsComplete)
	assert.True(t, info1.HasFooter)
	assert.Equal(t, uint32(3), info1.RecordCount)
	assert.Equal(t, int64(5000), info1.FirstEntryID)

	assert.True(t, info2.IsComplete)
	assert.True(t, info2.HasFooter)
	assert.Equal(t, uint32(3), info2.RecordCount)
	assert.Equal(t, int64(5000), info2.FirstEntryID)
}

func TestStreamDecodeReader_SequentialReading(t *testing.T) {
	// Create a complete file
	buf := newTestBuffer()
	writer, err := NewStreamEncodeWriter(buf, 4000, 0)
	require.NoError(t, err)

	// Write test data
	testData := []string{"seq 1", "seq 2", "seq 3", "seq 4"}
	for _, data := range testData {
		_, err := writer.WriteData([]byte(data))
		require.NoError(t, err)
	}
	err = writer.Finalize()
	require.NoError(t, err)

	// Test sequential reading
	dataReader := bytes.NewReader(buf.Bytes())
	reader, err := NewStreamDecodeReader(dataReader)
	require.NoError(t, err)

	// Reset to beginning after initialization
	_, err = dataReader.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Read header first
	record, err := reader.ReadNext()
	require.NoError(t, err)
	_, ok := record.(*HeaderRecord)
	assert.True(t, ok)

	// Read data records
	for i, expectedData := range testData {
		record, err := reader.ReadNext()
		require.NoError(t, err, "Failed to read record %d", i)
		dataRecord, ok := record.(*DataRecord)
		require.True(t, ok, "Record %d is not a DataRecord", i)
		assert.Equal(t, expectedData, string(dataRecord.Payload), "Record %d data mismatch", i)
	}

	// Read index
	record, err = reader.ReadNext()
	require.NoError(t, err)
	_, ok = record.(*IndexRecord)
	assert.True(t, ok)

	// Read footer
	record, err = reader.ReadNext()
	require.NoError(t, err)
	_, ok = record.(*FooterRecord)
	assert.True(t, ok)

	// Should reach EOF
	_, err = reader.ReadNext()
	assert.Equal(t, io.EOF, err)
}

func TestStreamDecodeReader_IterateRecords(t *testing.T) {
	// Create a complete file
	buf := newTestBuffer()
	writer, err := NewStreamEncodeWriter(buf, 6000, 0)
	require.NoError(t, err)

	// Write test data
	testData := []string{"iter 1", "iter 2", "iter 3"}
	for _, data := range testData {
		_, err := writer.WriteData([]byte(data))
		require.NoError(t, err)
	}
	err = writer.Finalize()
	require.NoError(t, err)

	// Test iteration
	reader, err := NewStreamDecodeReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	var recordTypes []string
	var dataContents []string

	err = reader.IterateRecords(func(record Record) error {
		switch r := record.(type) {
		case *HeaderRecord:
			recordTypes = append(recordTypes, "header")
		case *DataRecord:
			recordTypes = append(recordTypes, "data")
			dataContents = append(dataContents, string(r.Payload))
		case *IndexRecord:
			recordTypes = append(recordTypes, "index")
		case *FooterRecord:
			recordTypes = append(recordTypes, "footer")
		}
		return nil
	})
	require.NoError(t, err)

	// Verify record types and order
	expectedTypes := []string{"header", "data", "data", "data", "index", "footer"}
	assert.Equal(t, expectedTypes, recordTypes)

	// Verify data contents
	assert.Equal(t, testData, dataContents)
}

// Real File System Tests

func TestStreamEncodeWriter_RealFile(t *testing.T) {
	// Create temporary file
	tempFile, err := os.CreateTemp("", "stream_test_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Test writing to real file
	writer, err := NewStreamEncodeWriter(tempFile, 7000, 0)
	require.NoError(t, err)

	// Write test data
	testData := []string{
		"Real file data 1",
		"Real file data 2",
		"Real file data 3",
		"Real file data 4",
		"Real file data 5",
	}

	var entryIDs []int64
	for _, data := range testData {
		entryID, err := writer.WriteData([]byte(data))
		require.NoError(t, err)
		entryIDs = append(entryIDs, entryID)
	}

	// Verify entry IDs are sequential
	for i, entryID := range entryIDs {
		assert.Equal(t, int64(7000+i), entryID)
	}

	// Finalize the file
	err = writer.Finalize()
	require.NoError(t, err)

	// Close and reopen for reading
	tempFile.Close()

	readFile, err := os.Open(tempFile.Name())
	require.NoError(t, err)
	defer readFile.Close()

	// Test reading from real file
	reader, err := NewStreamDecodeReader(readFile)
	require.NoError(t, err)

	// Verify file is complete
	assert.True(t, reader.IsComplete())

	// Read all data records
	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, len(testData))

	for i, record := range dataRecords {
		assert.Equal(t, testData[i], string(record.Payload))
	}

	// Test random access
	record, err := reader.GetRecordByEntryID(7002)
	require.NoError(t, err)
	dataRecord := record.(*DataRecord)
	assert.Equal(t, "Real file data 3", string(dataRecord.Payload))

	// Test file info
	info := reader.GetFileInfo()
	assert.True(t, info.IsComplete)
	assert.True(t, info.HasFooter)
	assert.Equal(t, uint32(len(testData)), info.RecordCount)
	assert.Equal(t, int64(7000), info.FirstEntryID)
}

func TestStreamDecodeReader_RealFileIncomplete(t *testing.T) {
	// Create temporary file
	tempFile, err := os.CreateTemp("", "stream_incomplete_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Write incomplete file (no finalization)
	writer, err := NewStreamEncodeWriter(tempFile, 8000, 0)
	require.NoError(t, err)

	writer.WriteData([]byte("Incomplete data 1"))
	writer.WriteData([]byte("Incomplete data 2"))

	// Don't finalize - close file directly
	tempFile.Close()

	// Reopen for reading
	readFile, err := os.Open(tempFile.Name())
	require.NoError(t, err)
	defer readFile.Close()

	// Test reading incomplete file
	reader, err := NewStreamDecodeReader(readFile)
	require.NoError(t, err)

	// Should not be complete
	assert.False(t, reader.IsComplete())
	assert.NotNil(t, reader.GetHeader())
	assert.Nil(t, reader.GetFooter())
	assert.Nil(t, reader.GetIndex())

	// Should be able to read data sequentially
	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, 2)
	assert.Equal(t, "Incomplete data 1", string(dataRecords[0].Payload))
	assert.Equal(t, "Incomplete data 2", string(dataRecords[1].Payload))
}

func TestConcurrentFileWriteRead(t *testing.T) {
	// Create temporary file
	tempFile, err := os.CreateTemp("", "concurrent_test_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Writer goroutine
	writer, err := NewStreamEncodeWriter(tempFile, 9000, 0)
	require.NoError(t, err)

	const numRecords = 100
	writeDone := make(chan bool)
	writeErrors := make(chan error, 1)

	go func() {
		defer close(writeDone)
		for i := 0; i < numRecords; i++ {
			data := fmt.Sprintf("Concurrent data %d", i)
			_, err := writer.WriteData([]byte(data))
			if err != nil {
				writeErrors <- err
				return
			}
			// Small delay to simulate real writing
			time.Sleep(1 * time.Millisecond)
		}

		if err := writer.Finalize(); err != nil {
			writeErrors <- err
			return
		}
	}()

	// Wait for writing to complete
	<-writeDone
	tempFile.Close()

	// Check for write errors
	select {
	case err := <-writeErrors:
		t.Fatalf("Write error: %v", err)
	default:
		// No errors
	}

	// Multiple concurrent readers
	const numReaders = 5
	var wg sync.WaitGroup
	readerErrors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Each reader opens its own file handle
			readFile, err := os.Open(tempFile.Name())
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d open error: %v", readerID, err)
				return
			}
			defer readFile.Close()

			reader, err := NewStreamDecodeReader(readFile)
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d create error: %v", readerID, err)
				return
			}

			// Verify complete file
			if !reader.IsComplete() {
				readerErrors <- fmt.Errorf("reader %d: file not complete", readerID)
				return
			}

			// Read all records
			dataRecords, err := reader.GetAllDataRecords()
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d read error: %v", readerID, err)
				return
			}

			if len(dataRecords) != numRecords {
				readerErrors <- fmt.Errorf("reader %d: expected %d records, got %d", readerID, numRecords, len(dataRecords))
				return
			}

			// Verify data integrity
			for j, record := range dataRecords {
				expected := fmt.Sprintf("Concurrent data %d", j)
				if string(record.Payload) != expected {
					readerErrors <- fmt.Errorf("reader %d: record %d mismatch", readerID, j)
					return
				}
			}

			// Test random access
			randomID := int64(9000 + numRecords/2)
			record, err := reader.GetRecordByEntryID(randomID)
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d random access error: %v", readerID, err)
				return
			}

			dataRecord := record.(*DataRecord)
			expected := fmt.Sprintf("Concurrent data %d", numRecords/2)
			if string(dataRecord.Payload) != expected {
				readerErrors <- fmt.Errorf("reader %d random access data mismatch", readerID)
				return
			}
		}(i)
	}

	// Wait for all readers to complete
	wg.Wait()

	// Check for reader errors
	close(readerErrors)
	for err := range readerErrors {
		t.Error(err)
	}
}

func TestLargeFileHandling(t *testing.T) {
	// Create temporary file
	tempFile, err := os.CreateTemp("", "large_file_test_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Write large file
	writer, err := NewStreamEncodeWriter(tempFile, 10000, 0)
	require.NoError(t, err)

	const numRecords = 1000
	const recordSize = 1024 // 1KB per record

	// Generate large data
	largeData := make([]byte, recordSize)
	for i := 0; i < recordSize; i++ {
		largeData[i] = byte(i % 256)
	}

	// Write records
	for i := 0; i < numRecords; i++ {
		// Modify some bytes to make each record unique
		binary.LittleEndian.PutUint32(largeData[:4], uint32(i))

		_, err := writer.WriteData(largeData)
		require.NoError(t, err)
	}

	err = writer.Finalize()
	require.NoError(t, err)
	tempFile.Close()

	// Read and verify
	readFile, err := os.Open(tempFile.Name())
	require.NoError(t, err)
	defer readFile.Close()

	reader, err := NewStreamDecodeReader(readFile)
	require.NoError(t, err)

	assert.True(t, reader.IsComplete())

	// Test file info
	info := reader.GetFileInfo()
	assert.Equal(t, uint32(numRecords), info.RecordCount)
	assert.Equal(t, int64(10000), info.FirstEntryID)

	// Read all records
	dataRecords, err := reader.GetAllDataRecords()
	require.NoError(t, err)
	assert.Len(t, dataRecords, numRecords)

	// Verify each record
	for i, record := range dataRecords {
		assert.Equal(t, recordSize, len(record.Payload))

		// Check the unique identifier we embedded
		recordID := binary.LittleEndian.Uint32(record.Payload[:4])
		assert.Equal(t, uint32(i), recordID)
	}

	// Test random access on large file
	middleID := int64(10000 + numRecords/2)
	record, err := reader.GetRecordByEntryID(middleID)
	require.NoError(t, err)

	dataRecord := record.(*DataRecord)
	recordID := binary.LittleEndian.Uint32(dataRecord.Payload[:4])
	assert.Equal(t, uint32(numRecords/2), recordID)
}

func TestFilePermissionsAndErrors(t *testing.T) {
	// Test write to read-only directory (if possible)
	readOnlyDir := "/tmp/readonly_test_dir"
	err := os.Mkdir(readOnlyDir, 0444) // Read-only directory
	if err == nil {
		defer os.RemoveAll(readOnlyDir)

		// Try to create file in read-only directory
		readOnlyFile := filepath.Join(readOnlyDir, "test.dat")
		file, err := os.Create(readOnlyFile)
		if err != nil {
			// Expected - cannot create file in read-only directory
			t.Logf("Expected error creating file in read-only directory: %v", err)
		} else {
			file.Close()
			os.Remove(readOnlyFile)
		}
	}

	// Test reading non-existent file
	_, err = NewStreamDecodeReader(strings.NewReader(""))
	assert.Error(t, err) // Should fail with empty reader

	// Test invalid file content
	invalidFile, err := os.CreateTemp("", "invalid_*.dat")
	require.NoError(t, err)
	defer func() {
		invalidFile.Close()
		os.Remove(invalidFile.Name())
	}()

	// Write invalid data
	invalidFile.Write([]byte("invalid file content"))
	invalidFile.Close()

	// Try to read invalid file
	readFile, err := os.Open(invalidFile.Name())
	require.NoError(t, err)
	defer readFile.Close()

	_, err = NewStreamDecodeReader(readFile)
	assert.Error(t, err) // Should fail with invalid content
}

func TestFileAppendSimulation(t *testing.T) {
	// Simulate append-only file writing pattern
	tempFile, err := os.CreateTemp("", "append_test_*.dat")
	require.NoError(t, err)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Initial write
	writer, err := NewStreamEncodeWriter(tempFile, 11000, 0)
	require.NoError(t, err)

	// Write initial data
	writer.WriteData([]byte("Initial data 1"))
	writer.WriteData([]byte("Initial data 2"))

	// Get current position before closing
	currentPos, err := tempFile.Seek(0, io.SeekCurrent)
	require.NoError(t, err)

	// Close writer (but don't finalize - simulating crash)
	tempFile.Close()

	// Reopen file for append
	appendFile, err := os.OpenFile(tempFile.Name(), os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)

	// Seek to where we left off
	_, err = appendFile.Seek(currentPos, io.SeekStart)
	require.NoError(t, err)

	// Continue writing with new writer instance
	writer2, err := NewStreamEncodeWriter(appendFile, 11002, 0) // Continue from where we left off
	require.NoError(t, err)

	writer2.WriteData([]byte("Appended data 1"))
	writer2.WriteData([]byte("Appended data 2"))
	err = writer2.Finalize()
	require.NoError(t, err)
	appendFile.Close()

	// Read the complete file
	readFile, err := os.Open(tempFile.Name())
	require.NoError(t, err)
	defer readFile.Close()

	// The file will have mixed structure - this test shows the complexity
	// of real append scenarios. In practice, you'd need recovery mechanisms.
	reader, err := NewStreamDecodeReader(readFile)
	if err != nil {
		// Expected - file structure is mixed due to multiple writers
		t.Logf("Expected error with mixed file structure: %v", err)
	} else {
		// If it works, verify what we can
		info := reader.GetFileInfo()
		t.Logf("File info: Complete=%v, RecordCount=%d", info.IsComplete, info.RecordCount)
	}
}
