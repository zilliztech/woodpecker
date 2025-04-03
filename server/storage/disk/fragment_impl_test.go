package disk

import (
	"context"
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
	fw, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 100) // 1MB file, fragmentId=1, firstEntryID=100
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
	fw2, err := NewFragmentFileWriter(filePath, 1024*1024, 1, 100) // 1MB file, fragmentId=1, firstEntryID=100
	assert.Error(t, err)
	assert.Nil(t, fw2)
}

func TestFragmentFile_WriteAndRead(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// Create new FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
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
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
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
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
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
	fr, err := NewFragmentFileReader(filePath, 1024*1024, 1) // firstEntryID will be ignored, loaded from file
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
	ff, err := NewFragmentFileWriter(filePath, 128*1024*1024, 1, startEntryID) // 128MB file, fragmentId=1, firstEntryID=100
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
	ff2, err := NewFragmentFileReader(filePath, 128*1024*1024, 1) // firstEntryID will be ignored, loaded from file
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
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
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
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, startEntryID)
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
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
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
	ff, err := NewFragmentFileWriter(filePath, 1024*1024, 1, startEntryID) // 1MB file, fragmentId=1, firstEntryID=100
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
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, startEntryID)
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
	writerFF, err := NewFragmentFileWriter(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, writerFF)

	// Create FragmentFile for reading (different instance of the same file)
	readerFF, err := NewFragmentFileReader(filePath, fileSize, 1)
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
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, startEntryID)
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
	ff2, err := NewFragmentFileReader(filePath, fileSize, 1)
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
	ff, err := NewFragmentFileWriter(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Open read file
	ff2, err := NewFragmentFileReader(filePath, fileSize, 1)
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
	roFF, err := NewFragmentFileReader(filePath, fileSize, 1)
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
