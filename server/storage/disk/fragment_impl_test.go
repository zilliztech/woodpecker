package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
)

func TestNewFragmentFileWriter(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	fw, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, 100) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, fw)
	assert.Equal(t, int64(1), fw.GetFragmentId())

	// Verify file exists
	_, err = os.Stat(filePath)
	assert.NoError(t, err)

	// Verify file size
	info, err := os.Stat(filePath)
	assert.NoError(t, err)
	assert.Equal(t, int64(1024*1024), info.Size())

	// Cannot create a writer for an existing fragmentFile
	fw2, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, 100) // 1MB file, fragmentId=1, firstEntryID=100
	assert.Error(t, err)
	assert.Nil(t, fw2)
}

func TestFragmentFile_WriteAndRead(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write test data
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, startEntryID)
	assert.NoError(t, err)

	// Read data
	data, err := ff.GetEntry(startEntryID)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFragmentFile_MultipleEntries(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write multiple test data
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")
	testData3 := []byte("test data 3")

	// Write data
	err = ff.Write(context.Background(), testData1, startEntryID)
	assert.NoError(t, err)
	err = ff.Write(context.Background(), testData2, startEntryID+1)
	assert.NoError(t, err)
	err = ff.Write(context.Background(), testData3, startEntryID+2)
	assert.NoError(t, err)

	// Flush data
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// Check entry ID range
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+2, lastID)

	t.Logf("Entry ID range: %d to %d", firstID, lastID)

	// Get and output actual storage order (debug info)
	for i := firstID; i <= lastID; i++ {
		data, err := ff.GetEntry(i)
		if err != nil {
			t.Logf("Failed to read entry %d: %v", i, err)
		} else {
			t.Logf("Entry %d: %s", i, string(data))
		}
		assert.NoError(t, err)
	}

	// Verify data based on actual storage order
	data1, err := ff.GetEntry(startEntryID)
	assert.NoError(t, err)
	assert.Equal(t, testData1, data1, "First entry should be testData1")

	data2, err := ff.GetEntry(startEntryID + 1)
	assert.NoError(t, err)
	assert.Equal(t, testData2, data2, "Second entry should be testData2")

	data3, err := ff.GetEntry(startEntryID + 2)
	assert.NoError(t, err)
	assert.Equal(t, testData3, data3, "Third entry should be testData3")
}

func TestFragmentFile_LoadAndReload(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write test data
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, startEntryID)
	assert.NoError(t, err)

	// Flush data to disk (this step is important)
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// Close file
	err = ff.Release()
	assert.NoError(t, err)
	ff.Close()

	// Reopen file
	fr, err := NewFragmentFileReader(filePath, 1024*1024, 1, 0, 1) // firstEntryID will be ignored, loaded from file
	assert.NoError(t, err)
	assert.NotNil(t, fr)

	// Load data
	err = fr.Load(context.Background())
	assert.NoError(t, err)

	// Check loaded firstEntryID
	firstID, err := fr.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := fr.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, lastID)

	// Read data
	data, err := fr.GetEntry(startEntryID)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFragmentFileLargeWriteAndRead(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 128*1024*1024, 1, 0, 1, startEntryID) // 128MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write test data
	baseData := make([]byte, 1*1024*1024)
	for i := 0; i < 120; i++ {
		testData := []byte(fmt.Sprintf("test data_%d", i))
		testData = append(testData, baseData...)
		err = ff.Write(context.Background(), testData, startEntryID+int64(i))
		assert.NoError(t, err)
	}

	// Flush data to disk (this step is important)
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// Close file
	err = ff.Release()
	assert.NoError(t, err)

	// Reopen file
	ff2, err := NewFragmentFileReader(filePath, 128*1024*1024, 1, 0, 1) // firstEntryID will be ignored, loaded from file
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// Load data
	err = ff2.Load(context.Background())
	assert.NoError(t, err)

	// Check loaded firstEntryID
	firstID, err := ff2.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := ff2.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+119, lastID)

	// Read data
	for i := 0; i < 120; i++ {
		data, err := ff2.GetEntry(startEntryID + int64(i))
		assert.NoError(t, err)
		expected := append([]byte(fmt.Sprintf("test data_%d", i)), baseData...)
		assert.Equal(t, expected, data)
	}
}

func TestFragmentFile_CRCValidation(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write test data
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, startEntryID)
	assert.NoError(t, err)

	// Modify data in file (corrupt CRC)
	ff.mappedFile[ff.dataOffset-2] = 'X' // Modify part of the data

	// Try to read data
	_, err = ff.GetEntry(startEntryID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CRC mismatch")
}

func TestFragmentFile_OutOfSpace(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create a small file to cause out of space when writing large data
	// File structure:
	// - headerSize(4KB): File header
	// - dataArea: Actual data storage area
	// - indexArea: Index area, each entry takes 4 bytes(indexItemSize)
	// - footerSize(4KB): File footer
	fileSize := int64(12*1024 + 1024) // 13KB

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write small data to ensure success
	smallData := []byte("small test data")
	err = ff.Write(context.Background(), smallData, int64(startEntryID))
	assert.NoError(t, err)

	// Try to write large data, should cause out of space error
	largeData := make([]byte, 5*1024) // 5KB
	err = ff.Write(context.Background(), largeData, int64(startEntryID+1))
	assert.Error(t, err)
	assert.True(t, werr.ErrDiskFragmentNoSpace.Is(err))
}

func TestFragmentFile_InvalidEntryId(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Try to read non-existent entries
	_, err = ff.GetEntry(startEntryID - 1) // Less than start ID
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidEntryId.Is(err))
	assert.Contains(t, err.Error(), "not in the range ")

	_, err = ff.GetEntry(startEntryID + 1) // Greater than existing entry ID
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidEntryId.Is(err))
	assert.Contains(t, err.Error(), "not in the range ")
}

func TestFragmentFile_Release(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 0, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write test data
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, int64(0))
	assert.NoError(t, err)

	// Release resources
	ff.Close()

	// Try to write data after release
	err = ff.Write(context.Background(), testData, int64(1))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fragment file is closed")
}

func TestFragmentFile_ConcurrentReadWrite(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent_test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	fileSize := int64(10 * 1024 * 1024) // 10MB
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Number of entries to write
	entryCount := 100
	// Synchronization channels for coordinating read/write operations
	entryWritten := make(chan int64, entryCount)
	done := make(chan struct{})
	ctx := context.Background()

	// Start write goroutine
	go func() {
		defer close(entryWritten)

		for i := 0; i < entryCount; i++ {
			entryID := startEntryID + int64(i)
			testData := []byte(fmt.Sprintf("concurrent test data %d", i))

			// Write data
			err := ff.Write(ctx, testData, entryID)
			if err != nil {
				t.Logf("Failed to write entry %d: %v", entryID, err)
				return
			}

			// Flush data to disk every 10 entries
			if i > 0 && i%10 == 0 {
				err = ff.Flush(ctx)
				if err != nil {
					t.Logf("Failed to flush data: %v", err)
					return
				}
				t.Logf("Data flushed up to entry %d", entryID)
			}

			// Notify read goroutine of new entry
			entryWritten <- entryID
			// Brief sleep to allow read goroutine to read
			time.Sleep(10 * time.Millisecond)
		}

		// Final flush to ensure all data is written to disk
		err = ff.Flush(ctx)
		if err != nil {
			t.Logf("Final flush failed: %v", err)
		}

		t.Logf("Write completed, total entries written: %d", entryCount)
	}()

	// Start read goroutine
	go func() {
		defer close(done)

		readCount := 0
		lastReadID := startEntryID - 1

		for entryID := range entryWritten {
			// Try to read newly written entries and any previously unread entries
			for readID := lastReadID + 1; readID <= entryID; readID++ {
				expectedData := []byte(fmt.Sprintf("concurrent test data %d", int(readID-startEntryID)))

				// Retry reading a few times as writes may take time to be visible
				var readData []byte
				var readErr error
				success := false

				for attempt := 0; attempt < 5; attempt++ {
					readData, readErr = ff.GetEntry(readID)
					if readErr == nil {
						// Verify read data is correct
						if !assert.Equal(t, expectedData, readData, "Data mismatch for entry %d", readID) {
							t.Logf("Entry %d read successful but data mismatch: expected=%s, actual=%s",
								readID, string(expectedData), string(readData))
						} else {
							t.Logf("Entry %d read successful: %s", readID, string(readData))
							readCount++
							success = true
						}
						break
					}

					if strings.Contains(readErr.Error(), "out of range") {
						// Entry may not be visible for reading yet, wait and retry
						time.Sleep(20 * time.Millisecond)
						continue
					} else {
						// Other errors, report directly
						t.Logf("Failed to read entry %d: %v", readID, readErr)
						break
					}
				}

				if !success {
					t.Logf("Failed to read entry %d after 5 attempts: %v", readID, readErr)
				}

				lastReadID = readID
			}
		}

		t.Logf("Read completed, successfully read %d entries", readCount)
		// Verify final read count matches expected
		assert.Equal(t, entryCount, readCount, "Number of entries read does not match number written")
	}()

	// Wait for read completion
	<-done

	// Release resources
	err = ff.Release()
	assert.NoError(t, err)

	// Verify final written data
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+int64(entryCount-1), lastID)
}

func TestFragmentFile_ConcurrentReadWriteDifferentInstances(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent_diff_inst.frag")

	// Create FragmentFile for writing
	startEntryID := int64(100)
	fileSize := int64(10 * 1024 * 1024) // 10MB
	writerFF, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, writerFF)

	// Create FragmentFile for reading (different instance of the same file)
	readerFF, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
	assert.NoError(t, err)
	assert.NotNil(t, readerFF)

	// Load the reader instance first
	err = readerFF.Load(context.Background()) // TODO test another case: assert load error the first time, because invalid header
	assert.NoError(t, err)

	// Number of entries to write
	entryCount := 100
	// Synchronization channels for coordinating read/write operations
	entryWritten := make(chan int64, entryCount)
	done := make(chan struct{})
	ctx := context.Background()

	// Start write goroutine
	go func() {
		defer close(entryWritten)

		for i := 0; i < entryCount; i++ {
			entryID := startEntryID + int64(i)
			testData := []byte(fmt.Sprintf("concurrent different instances test data %d", i))

			// Write data
			err := writerFF.Write(ctx, testData, entryID)
			if err != nil {
				t.Logf("Failed to write entry %d: %v", entryID, err)
				return
			}

			// Flush data to disk every 5 entries
			if i > 0 && i%5 == 0 {
				err = writerFF.Flush(ctx)
				if err != nil {
					t.Logf("Failed to flush data: %v", err)
					return
				}
				t.Logf("Data flushed up to entry %d", entryID)
			}

			// Notify read goroutine of new entry
			entryWritten <- entryID
			// Brief sleep to allow read goroutine to read
			time.Sleep(10 * time.Millisecond)
		}

		// Final flush to ensure all data is written to disk
		err = writerFF.Flush(ctx)
		if err != nil {
			t.Logf("Final flush failed: %v", err)
		}

		t.Logf("Write completed, total entries written: %d", entryCount)
	}()

	// Start read goroutine
	go func() {
		defer close(done)

		readCount := 0
		lastReadID := startEntryID - 1

		for entryID := range entryWritten {
			// Try to read newly written entries
			for readID := lastReadID + 1; readID <= entryID; readID++ {
				expectedData := []byte(fmt.Sprintf("concurrent different instances test data %d", int(readID-startEntryID)))

				// Retry reading a few times as writes may take time to be visible to another mmap instance
				var readData []byte
				var readErr error
				success := false

				for attempt := 0; attempt < 10; attempt++ {
					readData, readErr = readerFF.GetEntry(readID)
					if readErr == nil {
						// Verify read data is correct
						if !assert.Equal(t, expectedData, readData, "Data mismatch for entry %d", readID) {
							t.Logf("Entry %d read successful but data mismatch: expected=%s, actual=%s",
								readID, string(expectedData), string(readData))
						} else {
							t.Logf("Entry %d read successful: %s", readID, string(readData))
							readCount++
							success = true
						}
						break
					}
					// Error handling and retry logic
					t.Logf("Attempt %d: Failed to read entry %d: %v", attempt+1, readID, readErr)
					time.Sleep(50 * time.Millisecond)
				}

				if !success {
					t.Logf("Failed to read entry %d after 10 attempts", readID)
				}

				lastReadID = readID
			}
		}

		t.Logf("Read completed, successfully read %d entries", readCount)
		// Verify final read count matches expected
		assert.Equal(t, entryCount, readCount, "Number of entries read does not match number written")
	}()

	// Wait for read completion
	<-done

	// Release resources
	err = writerFF.Release()
	assert.NoError(t, err)

	err = readerFF.Release()
	assert.NoError(t, err)
}

func TestFragmentFile_WriteAndReadMultipleEntries(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_multiple.frag")

	// Create new FragmentFile
	startEntryID := int64(1000)
	fileSize := int64(1 * 1024 * 1024) // 1MB
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write 10 entries
	entryCount := 10
	writtenData := make([][]byte, entryCount)
	ctx := context.Background()

	t.Log("Writing 10 entries...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		// Each entry has different content and size
		data := []byte(fmt.Sprintf("Test data #%d - Random content: %d", i, time.Now().UnixNano()))
		writtenData[i] = data

		err := ff.Write(ctx, data, entryID)
		assert.NoError(t, err, "Failed to write entry %d", entryID)
		t.Logf("Wrote entry %d: %s", entryID, string(data))
	}

	// Flush data to disk
	err = ff.Flush(ctx)
	assert.NoError(t, err, "Failed to flush data")
	t.Log("Data flushed to disk")

	// Verify first and last entry IDs
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID, "First entry ID mismatch")

	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+int64(entryCount-1), lastID, "Last entry ID mismatch")

	// Read and verify each entry
	t.Log("Reading and verifying 10 entries...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := ff.GetEntry(entryID)
		assert.NoError(t, err, "Failed to read entry %d", entryID)

		// Verify data content matches
		assert.Equal(t, writtenData[i], data, "Data mismatch for entry %d", entryID)
		t.Logf("Read entry %d: %s", entryID, string(data))
	}
	t.Log("All entries verified")

	// Try to read non-existent entries
	_, err = ff.GetEntry(startEntryID - 1)
	assert.Error(t, err, "Should not be able to read entry outside range")

	_, err = ff.GetEntry(startEntryID + int64(entryCount))
	assert.Error(t, err, "Should not be able to read entry outside range")

	// Release and reload test
	t.Log("Releasing and reloading file...")
	err = ff.Release()
	assert.NoError(t, err, "Failed to release resources")

	// Reopen file
	ff2, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// Load data
	err = ff2.Load(ctx)
	assert.NoError(t, err, "Failed to load data")

	// Verify data again
	t.Log("Verifying data after reload...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := ff2.GetEntry(entryID)
		assert.NoError(t, err, "Failed to read entry %d after reload", entryID)

		// Verify data content matches
		assert.Equal(t, writtenData[i], data, "Data mismatch for entry %d after reload", entryID)
	}
	t.Log("All entries verified after reload")

	// Final cleanup
	err = ff2.Release()
	assert.NoError(t, err, "Failed to release resources")
}

func TestFragmentFile_AlternatingWriteFlushRead(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "alternating_test.frag")

	// Create new FragmentFile
	startEntryID := int64(500)
	fileSize := int64(1 * 1024 * 1024) // 1MB
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Open read file
	ff2, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// Number of test entries
	entryCount := 15
	ctx := context.Background()

	t.Log("Starting alternating write/read test...")

	// Store all written data for final verification
	allWrittenData := make([][]byte, entryCount)

	// Alternating write and read
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)

		// 1. Write a piece of data
		testData := []byte(fmt.Sprintf("data#%d", entryID))
		allWrittenData[i] = testData

		t.Logf("Step %d.1: Writing entry %d", i+1, entryID)
		err = ff.Write(ctx, testData, entryID)
		assert.NoError(t, err, "Failed to write entry %d", entryID)

		// 2. Flush data immediately
		t.Logf("Step %d.2: Flushing entry %d", i+1, entryID)
		err = ff.Flush(ctx)
		assert.NoError(t, err, "Failed to flush entry %d", entryID)

		// 3. Read and verify the data just written
		t.Logf("Step %d.3: Reading entry %d", i+1, entryID)
		readData, err := ff2.GetEntry(entryID)
		assert.NoError(t, err, "Failed to read entry %d", entryID)
		assert.Equal(t, testData, readData, "Data mismatch for entry %d", entryID)
		t.Logf("Entry %d verified successfully: %s", entryID, string(readData))

		// 4. Ensure lastEntryID is updated correctly
		t.Logf("Step %d.4: Ensure lastEntryID is updated correctly", i+1)
		lastID, err := ff2.GetLastEntryId()
		assert.NoError(t, err, "Failed to get last entry ID")
		assert.Equal(t, entryID, lastID, "Last entry ID mismatch")

		// Add a small delay for test clarity
		time.Sleep(10 * time.Millisecond)
	}

	t.Log("Alternating write/read test completed")

	// Final verification of all entries
	t.Log("Performing final verification of all entries...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := ff2.GetEntry(entryID)
		assert.NoError(t, err, "Final verification: Failed to read entry %d", entryID)
		assert.Equal(t, allWrittenData[i], data, "Final verification: Data mismatch for entry %d", entryID)
	}

	// Reload test
	t.Log("Releasing and reloading file for verification...")
	err = ff2.Release()
	assert.NoError(t, err, "Failed to release resources")

	// Open file in read-only mode
	roFF, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
	assert.NoError(t, err, "Failed to create read-only file")
	assert.NotNil(t, roFF)

	// Load data
	err = roFF.Load(ctx)
	assert.NoError(t, err, "Failed to load data")

	// Verify first and last IDs
	firstID, err := roFF.GetFirstEntryId()
	assert.NoError(t, err, "Failed to get first entry ID")
	assert.Equal(t, startEntryID, firstID, "First entry ID mismatch")

	lastID, err := roFF.GetLastEntryId()
	assert.NoError(t, err, "Failed to get last entry ID")
	assert.Equal(t, startEntryID+int64(entryCount-1), lastID, "Last entry ID mismatch")

	// Re-verify all entries
	t.Log("Verifying all entries after reload...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := roFF.GetEntry(entryID)
		assert.NoError(t, err, "After reload: Failed to read entry %d", entryID)
		assert.Equal(t, allWrittenData[i], data, "After reload: Data mismatch for entry %d", entryID)
		t.Logf("After reload: Entry %d verified successfully", entryID)
	}

	// Clean up resources
	err = roFF.Release()
	assert.NoError(t, err, "Failed to release read-only file resources")
	t.Log("All alternating write/flush/read tests passed!")
}

func TestFragmentFileReader_InvalidFile(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create temporary directory
	tmpDir := t.TempDir()

	// Test case 1: File does not exist yet
	t.Run("NonExistentFile", func(t *testing.T) {
		nonExistentPath := filepath.Join(tmpDir, "non_existent.frag")
		fr, err := NewFragmentFileReader(nonExistentPath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for non-existent file should succeed")

		// Try to load the file - should fail with file not found error
		err = fr.Load(context.Background())
		assert.Error(t, err, "Loading non-existent file should fail")
		assert.Contains(t, err.Error(), "failed to open fragment file")
	})

	// Test case 2: Empty file (file exists but has zero size)
	t.Run("EmptyFile", func(t *testing.T) {
		emptyFilePath := filepath.Join(tmpDir, "empty.frag")
		emptyFile, err := os.Create(emptyFilePath)
		assert.NoError(t, err, "Failed to create empty file")
		emptyFile.Close()

		fr, err := NewFragmentFileReader(emptyFilePath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for empty file should succeed")

		// Try to load the file - should fail during validation
		err = fr.Load(context.Background())
		assert.Error(t, err, "Loading empty file should fail")
		// The error should indicate invalid file header
		assert.Contains(t, err.Error(), "failed to map fragment file")
	})

	// Test case 3: File with invalid magic string
	t.Run("InvalidMagicString", func(t *testing.T) {
		invalidMagicPath := filepath.Join(tmpDir, "invalid_magic.frag")

		// Create file with invalid magic string
		file, err := os.Create(invalidMagicPath)
		assert.NoError(t, err, "Failed to create file")

		// Write some garbage data with incorrect magic string
		_, err = file.Write([]byte("NOT_FRAG"))
		assert.NoError(t, err, "Failed to write to file")

		// Truncate to proper size
		err = file.Truncate(1024 * 1024)
		assert.NoError(t, err, "Failed to truncate file")
		file.Close()

		fr, err := NewFragmentFileReader(invalidMagicPath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader should succeed")

		// Try to load the file - should fail during magic string validation
		err = fr.Load(context.Background())
		assert.Error(t, err, "Loading file with invalid magic string should fail")
		assert.Contains(t, err.Error(), "invalid magic bytes")
	})

	// Test case 4: File with header but no footer (partial write)
	t.Run("HeaderButNoFooter", func(t *testing.T) {
		partialFilePath := filepath.Join(tmpDir, "partial.frag")

		// Create file and write only a valid header
		file, err := os.Create(partialFilePath)
		assert.NoError(t, err, "Failed to create file")

		// Write valid magic string
		_, err = file.Write([]byte("FRAGMENT"))
		assert.NoError(t, err, "Failed to write magic string")

		// Write version (4 bytes, little endian)
		versionBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(versionBytes, 1) // Version 1
		_, err = file.Write(versionBytes)
		assert.NoError(t, err, "Failed to write version")

		// Truncate to header size only (no footer)
		err = file.Truncate(headerSize)
		assert.NoError(t, err, "Failed to truncate file")
		file.Close()

		fr, err := NewFragmentFileReader(partialFilePath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader should succeed")

		// Try to load - should fail during footer reading
		err = fr.Load(context.Background())
		assert.Error(t, err, "Loading file with header but no footer should fail")
		assert.Contains(t, err.Error(), "invalid file size")
	})

	// Test case 5: File with incorrect size (smaller than expected)
	t.Run("FileSmallerThanExpected", func(t *testing.T) {
		smallFilePath := filepath.Join(tmpDir, "small_file.frag")

		// Write a properly initialized fragment file
		fw, err := NewFragmentFileWriter(smallFilePath, 1024*1024, 1, 0, 1, 100)
		assert.NoError(t, err, "Failed to create fragment writer")

		// Write some data and flush
		err = fw.Write(context.Background(), []byte("test data"), 100)
		assert.NoError(t, err, "Failed to write data")
		err = fw.Flush(context.Background())
		assert.NoError(t, err, "Failed to flush data")

		// Release to close the file
		err = fw.Release()
		assert.NoError(t, err, "Failed to release writer")

		// Truncate the file to a size smaller than expected (but larger than header)
		file, err := os.OpenFile(smallFilePath, os.O_RDWR, 0644)
		assert.NoError(t, err, "Failed to open file for truncation")
		err = file.Truncate(headerSize + 1000) // Smaller than original size but has header
		assert.NoError(t, err, "Failed to truncate file")
		file.Close()

		// Try to read with a reader expecting the original size
		fr, err := NewFragmentFileReader(smallFilePath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader should succeed")

		// Load should fail
		err = fr.Load(context.Background())
		assert.Error(t, err, "Loading truncated file should fail")
	})
}

func TestFragmentFileReader_IsMMapReadable(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create temporary directory
	tmpDir := t.TempDir()

	t.Run("NonExistentFile", func(t *testing.T) {
		// Test with a file that doesn't exist yet
		nonExistentPath := filepath.Join(tmpDir, "non_existent.frag")

		// Create reader for non-existent file
		fr, err := NewFragmentFileReader(nonExistentPath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for non-existent file should succeed")

		// Check if file is readable - should return false
		isReadable := fr.isMMapReadable(context.Background())
		assert.False(t, isReadable, "Non-existent file should not be readable")
	})

	t.Run("EmptyFile", func(t *testing.T) {
		// Test with an empty file (0 bytes)
		emptyFilePath := filepath.Join(tmpDir, "empty.frag")
		emptyFile, err := os.Create(emptyFilePath)
		assert.NoError(t, err, "Failed to create empty file")
		emptyFile.Close()

		// Create reader for empty file
		fr, err := NewFragmentFileReader(emptyFilePath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for empty file should succeed")

		// Check if file is readable - should return false
		isReadable := fr.isMMapReadable(context.Background())
		assert.False(t, isReadable, "Empty file should not be readable")
	})

	t.Run("IncompleteFile_JustHeader", func(t *testing.T) {
		// Test with a file that only has a header
		headerOnlyPath := filepath.Join(tmpDir, "header_only.frag")

		// Create and write only the header portion
		file, err := os.Create(headerOnlyPath)
		assert.NoError(t, err, "Failed to create header-only file")

		// Write magic string
		_, err = file.Write([]byte("FRAGMENT"))
		assert.NoError(t, err, "Failed to write magic string")

		// Write version (4 bytes, little endian)
		versionBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(versionBytes, 1)
		_, err = file.Write(versionBytes)
		assert.NoError(t, err, "Failed to write version")

		// Truncate to header size
		err = file.Truncate(headerSize)
		assert.NoError(t, err, "Failed to truncate file to header size")
		file.Close()

		// Create reader for header-only file
		fr, err := NewFragmentFileReader(headerOnlyPath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for header-only file should succeed")

		// Check if file is readable - should return false because no footer
		isReadable := fr.isMMapReadable(context.Background())
		assert.False(t, isReadable, "File with only header should not be readable")
	})

	t.Run("CompleteFile", func(t *testing.T) {
		// Test with a complete, properly written file
		completePath := filepath.Join(tmpDir, "complete.frag")

		// Create a complete fragment file
		fw, err := NewFragmentFileWriter(completePath, 1024*1024, 1, 0, 1, 100)
		assert.NoError(t, err, "Failed to create fragment writer")

		// Write some data
		err = fw.Write(context.Background(), []byte("test data"), 100)
		assert.NoError(t, err, "Failed to write data")

		// Flush to disk
		err = fw.Flush(context.Background())
		assert.NoError(t, err, "Failed to flush data")

		// Release resources
		err = fw.Release()
		assert.NoError(t, err, "Failed to release writer resources")

		// Create reader for complete file
		fr, err := NewFragmentFileReader(completePath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for complete file should succeed")

		// Check if file is readable - should return true
		isReadable := fr.isMMapReadable(context.Background())
		assert.True(t, isReadable, "Complete file should be readable")
	})

	t.Run("FileSizeMismatch", func(t *testing.T) {
		// Test with file where the expected size doesn't match actual size
		mismatchPath := filepath.Join(tmpDir, "size_mismatch.frag")

		// Create a complete fragment file with 1MB size
		fw, err := NewFragmentFileWriter(mismatchPath, 1024*1024, 1, 0, 1, 100)
		assert.NoError(t, err, "Failed to create fragment writer")

		// Write some data
		err = fw.Write(context.Background(), []byte("test data"), 100)
		assert.NoError(t, err, "Failed to write data")

		// Flush to disk
		err = fw.Flush(context.Background())
		assert.NoError(t, err, "Failed to flush data")

		// Release resources
		err = fw.Release()
		assert.NoError(t, err, "Failed to release writer resources")

		// Create reader with incorrect size expectation (2MB instead of 1MB)
		fr, err := NewFragmentFileReader(mismatchPath, 2*1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader with size mismatch should succeed")

		// Check if file is readable - should return false due to size mismatch
		isReadable := fr.isMMapReadable(context.Background())
		assert.False(t, isReadable, "File with size mismatch should not be readable")
	})

	t.Run("CorruptedFile", func(t *testing.T) {
		// Test with a corrupted file (valid size but invalid content)
		corruptPath := filepath.Join(tmpDir, "corrupt.frag")

		// Create file with invalid content
		file, err := os.Create(corruptPath)
		assert.NoError(t, err, "Failed to create corrupted file")

		// Write some garbage data
		garbage := make([]byte, 1024*1024) // 1MB of zeros
		_, err = file.Write(garbage)
		assert.NoError(t, err, "Failed to write garbage data")
		file.Close()

		// Create reader for corrupted file
		fr, err := NewFragmentFileReader(corruptPath, 1024*1024, 1, 0, 1)
		assert.NoError(t, err, "Creating reader for corrupted file should succeed")

		// Check if file is readable - should return false due to invalid content
		isReadable := fr.isMMapReadable(context.Background())
		assert.False(t, isReadable, "Corrupted file should not be readable")
	})
}

func TestFragmentFileReader_IsMMapReadable_ConcurrentAccess(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent_readable_test.frag")

	// File size to use for tests
	fileSize := int64(1024 * 1024) // 1MB

	// Test concurrent checking while file is being written
	t.Run("CheckDuringWrite", func(t *testing.T) {
		// Channel to signal writing has begun
		writeStarted := make(chan struct{})
		writeFinished := make(chan struct{})

		// Number of concurrent readers to check
		numCheckers := 3
		checkResults := make(chan bool, numCheckers)

		// Start a goroutine to create and write to the file
		go func() {
			defer close(writeFinished)

			// Create fragment file
			fw, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, 100)
			if err != nil {
				t.Logf("Writer creation failed: %v", err)
				return
			}

			// Signal that writing has started
			close(writeStarted)

			// Write entries slowly to simulate ongoing writing
			for i := 0; i < 5; i++ {
				data := []byte(fmt.Sprintf("test data %d", i))
				err = fw.Write(context.Background(), data, 100+int64(i))
				if err != nil {
					t.Logf("Write failed at entry %d: %v", i, err)
					return
				}

				// Sleep between writes to allow checkers to run
				time.Sleep(50 * time.Millisecond)

				// Flush after each write to make progress visible
				if err = fw.Flush(context.Background()); err != nil {
					t.Logf("Flush failed: %v", err)
					return
				}
			}

			// Final flush
			err = fw.Flush(context.Background())
			if err != nil {
				t.Logf("Final flush failed: %v", err)
			}

			// Release resources
			fw.Release()
		}()

		// Wait for writing to start
		<-writeStarted
		time.Sleep(10 * time.Millisecond) // Small delay to ensure file creation

		// Start multiple readers trying to check file readability
		for i := 0; i < numCheckers; i++ {
			go func(checkerID int) {
				// Create reader for the file
				fr, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
				if err != nil {
					t.Logf("Checker %d: Reader creation failed: %v", checkerID, err)
					checkResults <- false
					return
				}

				// Track number of attempts until readable
				attempts := 0
				readable := false

				// Try checking readability multiple times
				for j := 0; j < 10; j++ {
					attempts++
					// Check if the file is readable
					if fr.isMMapReadable(context.Background()) {
						readable = true
						t.Logf("Checker %d: File became readable after %d attempts", checkerID, attempts)
						break
					}
					t.Logf("Checker %d: File not readable on attempt %d", checkerID, attempts)
					time.Sleep(100 * time.Millisecond)
				}

				checkResults <- readable
			}(i)
		}

		// Wait for writing to finish
		<-writeFinished

		// Collect checker results
		successCount := 0
		for i := 0; i < numCheckers; i++ {
			if result := <-checkResults; result {
				successCount++
			}
		}

		t.Logf("%d out of %d checkers eventually found the file readable", successCount, numCheckers)

		// Final verification
		// The file should be readable after writing is complete
		fr, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
		assert.NoError(t, err, "Creating reader after write should succeed")
		isReadable := fr.isMMapReadable(context.Background())
		assert.True(t, isReadable, "File should be readable after complete writing")
	})
}

func TestFragmentFileReader_ReadersWaitingForWriter(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "waiting_test.frag")

	// Common parameters
	fileSize := int64(1024 * 1024) // 1MB
	entryCount := 10
	startEntryID := int64(100)

	// Channels for synchronization
	readerReady := make(chan struct{})
	fileCreated := make(chan struct{})
	writerFinished := make(chan struct{})

	// Channel to collect read results
	type ReadResult struct {
		EntryID int64
		Data    []byte
		Error   error
	}
	readResults := make(chan ReadResult, entryCount*2) // Buffer to avoid deadlocks

	// Start reader first, which will wait for file to become available
	go func() {
		// Create reader instance
		fr, err := NewFragmentFileReader(filePath, fileSize, 1, 0, 1)
		if err != nil {
			t.Logf("Reader creation failed: %v", err)
			close(readerReady)
			return
		}

		// Signal that reader is ready and waiting
		close(readerReady)

		t.Logf("Reader is waiting for file to become available")

		// Wait loop until the file becomes readable
		isReadable := false
		var successfulLoad bool
		for attempts := 1; attempts <= 20; attempts++ {
			// Check if file is readable
			isReadable = fr.isMMapReadable(context.Background())
			if isReadable {
				t.Logf("Reader: File is now readable after %d attempts", attempts)

				// Try to load the file
				err := fr.Load(context.Background())
				if err == nil {
					t.Logf("Reader: Successfully loaded the file")
					successfulLoad = true
					break
				}
				t.Logf("Reader: File appeared readable but load failed: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}

		// If file never became readable or loadable, report and return
		if !isReadable || !successfulLoad {
			t.Logf("Reader: File never became readable/loadable after 20 attempts")
			return
		}

		// Once file is readable, try reading entries
		// Keep trying to read entries until we have all expected entries or timeout
		entriesRead := 0
		lastReadEntryID := startEntryID - 1

		// Continue reading until we get all entries or reach timeout
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for entriesRead < entryCount {
			select {
			case <-ticker.C:
				// Try to read the next expected entry
				nextEntryID := lastReadEntryID + 1
				data, err := fr.GetEntry(nextEntryID)

				if err == nil {
					// Successfully read an entry
					entriesRead++
					lastReadEntryID = nextEntryID

					readResults <- ReadResult{
						EntryID: nextEntryID,
						Data:    data,
						Error:   nil,
					}

					t.Logf("Reader: Successfully read entry #%d: %s", nextEntryID, string(data))
				} else {
					// If entry not available yet, wait for next tick
					if werr.ErrInvalidEntryId.Is(err) {
						// This is expected when writer hasn't written this entry yet
						t.Logf("Reader: Entry #%d not available yet", nextEntryID)
					} else {
						// Other errors are unexpected
						t.Logf("Reader: Error reading entry #%d: %v", nextEntryID, err)
					}
				}

			case <-timeout:
				t.Logf("Reader: Timeout waiting for all entries, read %d/%d", entriesRead, entryCount)
				return
			}
		}

		t.Logf("Reader: Successfully read all %d entries", entriesRead)

		// Cleanup
		err = fr.Release()
		if err != nil {
			t.Logf("Reader: Error releasing resources: %v", err)
		}
	}()

	// Wait for reader to be ready
	<-readerReady

	// Start writer goroutine (after reader is ready)
	go func() {
		defer close(writerFinished)

		// Create fragment file for writing
		fw, err := NewFragmentFileWriter(filePath, fileSize, 1, 0, 1, startEntryID)
		if err != nil {
			t.Logf("Writer: Creation failed: %v", err)
			return
		}

		// Signal that file has been created
		close(fileCreated)

		// Write entries gradually
		for i := 0; i < entryCount; i++ {
			entryID := startEntryID + int64(i)
			data := []byte(fmt.Sprintf("test data %d", i))

			// Simulate some processing time
			time.Sleep(100 * time.Millisecond)

			err := fw.Write(context.Background(), data, entryID)
			if err != nil {
				t.Logf("Writer: Failed to write entry %d: %v", entryID, err)
				return
			}

			// Simulate some processing time before flush
			time.Sleep(20 * time.Millisecond)

			// Flush after each entry for testing purposes
			err = fw.Flush(context.Background())
			if err != nil {
				t.Logf("Writer: Failed to flush after entry %d: %v", entryID, err)
				return
			}

			t.Logf("Writer: Written entry #%d", entryID)
		}

		// Final flush and cleanup
		err = fw.Flush(context.Background())
		if err != nil {
			t.Logf("Writer: Final flush failed: %v", err)
		}

		err = fw.Release()
		if err != nil {
			t.Logf("Writer: Error releasing resources: %v", err)
		}

		t.Logf("Writer: Completed writing all %d entries", entryCount)
	}()

	// Wait for writer to finish
	<-writerFinished

	// Give some time for reader to finish processing
	time.Sleep(500 * time.Millisecond)

	// Collect and verify read results
	close(readResults)

	var collectedResults []ReadResult
	for result := range readResults {
		collectedResults = append(collectedResults, result)
	}

	// Verify results count
	assert.Equal(t, entryCount, len(collectedResults), "Should have read exactly %d entries", entryCount)

	// Verify each entry's content
	for i := 0; i < entryCount; i++ {
		expectedID := startEntryID + int64(i)
		expectedData := []byte(fmt.Sprintf("test data %d", i))

		// Find matching result
		found := false
		for _, result := range collectedResults {
			if result.EntryID == expectedID {
				assert.Equal(t, expectedData, result.Data, "Data mismatch for entry %d", expectedID)
				found = true
				break
			}
		}

		assert.True(t, found, "Entry %d was not found in read results", expectedID)
	}

	t.Logf("Test completed: Reader successfully read all %d entries written by the writer", entryCount)
}
