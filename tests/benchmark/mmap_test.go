package benchmark

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/edsrzf/mmap-go"
)

const (
	// File header and footer sizes
	headerSize = 4 * 1024
	footerSize = 4 * 1024
)

// TestMmapWritePerformance tests mmap write performance
// Focus: impact of write count, data block size, and file size on performance
func TestMmapWritePerformance(t *testing.T) {
	// Set test parameters
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // How many writes before executing a flush
	}{
		{"SmallFile_SmallBlock_FrequentFlush", 10 * 1024 * 1024, 10000, 512, 100},
		{"SmallFile_SmallBlock_InfrequentFlush", 10 * 1024 * 1024, 10000, 512, 1000},
		{"SmallFile_LargeBlock_FrequentFlush", 50 * 1024 * 1024, 5000, 4096, 50},
		{"SmallFile_LargeBlock_InfrequentFlush", 50 * 1024 * 1024, 5000, 4096, 500},
		{"LargeFile_SmallBlock_FrequentFlush", 100 * 1024 * 1024, 50000, 512, 500},
		{"LargeFile_SmallBlock_InfrequentFlush", 100 * 1024 * 1024, 50000, 512, 5000},
		{"LargeFile_LargeBlock_FrequentFlush", 200 * 1024 * 1024, 25000, 4096, 250},
		{"LargeFile_LargeBlock_InfrequentFlush", 200 * 1024 * 1024, 25000, 4096, 2500},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("test_%s.data", tc.name))

			// Create file
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("Unable to create file: %v", err)
			}

			// Set file size
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("Unable to set file size: %v", err)
			}

			// Map file to memory
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("Unable to map file: %v", err)
			}

			defer func() {
				// Clean up resources
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("Unable to unmap file: %v", err)
				}
				file.Close()
			}()

			// Write basic file header
			copy(mappedFile[0:8], []byte("TESTMMAP"))
			binary.LittleEndian.PutUint32(mappedFile[8:12], 1) // Version number

			// Performance test: write data
			dataOffset := uint32(headerSize)
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // Record data amount for each flush

			// Generate random data
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// Calculate required memory
				requiredSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// Check if it will exceed file size
				if requiredSpace >= uint32(tc.fileSize) {
					t.Logf("Warning: Data will exceed file size, stopping after %d/%d writes", i, tc.writeCount)
					break
				}

				// Write data length (4 bytes)
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))

				// Write data
				copy(mappedFile[dataOffset+4:], randomData)

				// Update offset
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4
				bytesSinceLastFlush += tc.dataBlockSize + 4

				// Execute flush according to flush interval
				if (i+1)%tc.flushInterval == 0 {
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}
					flushSizes = append(flushSizes, bytesSinceLastFlush) // Record data amount for this flush
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// Final flush to ensure all data is written to disk
			if err := mappedFile.Flush(); err != nil {
				t.Fatalf("Final flush failed: %v", err)
			}
			if bytesSinceLastFlush > 0 {
				flushSizes = append(flushSizes, bytesSinceLastFlush) // Record data amount for the last flush
			}
			flushCount++

			duration := time.Since(start)

			// Statistics for flush data amounts
			var minFlushSize, maxFlushSize int
			totalFlushSize := 0
			if len(flushSizes) > 0 {
				minFlushSize = flushSizes[0]
				maxFlushSize = flushSizes[0]
				for _, size := range flushSizes {
					totalFlushSize += size
					if size < minFlushSize {
						minFlushSize = size
					}
					if size > maxFlushSize {
						maxFlushSize = size
					}
				}
			}
			avgFlushSize := 0
			if flushCount > 0 {
				avgFlushSize = totalFlushSize / flushCount
			}

			// Calculate performance metrics
			throughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(tc.writeCount) / duration.Seconds()

			t.Logf("Test Results - %s:", tc.name)
			t.Logf("  File size: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  Write count: %d", tc.writeCount)
			t.Logf("  Data block size: %d bytes", tc.dataBlockSize)
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("  Average data per flush: %.2f KB", float64(avgFlushSize)/1024)
			t.Logf("  Minimum flush data: %.2f KB", float64(minFlushSize)/1024)
			t.Logf("  Maximum flush data: %.2f KB", float64(maxFlushSize)/1024)
			t.Logf("  Duration: %v", duration)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Operations rate: %.2f ops/s", opsPerSecond)
		})
	}
}

// TestMmapFlushStrategies tests the impact of different flush strategies on performance
func TestMmapFlushStrategies(t *testing.T) {
	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_flush_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Fixed parameters
	fileSize := int64(200 * 1024 * 1024) // 200MB
	dataBlockSize := 1024                // 1KB data block
	writeCount := 50000                  // Write 50000 times

	// Different flush strategies
	strategies := []struct {
		name       string
		flushType  string
		flushParam int // Depending on strategy type, may represent interval count or bytes
	}{
		{"NoFlush", "none", 0},                        // Only flush once at the end
		{"FlushEvery100Writes", "count", 100},         // Flush every 100 writes
		{"FlushEvery500Writes", "count", 500},         // Flush every 500 writes
		{"FlushEvery2500Writes", "count", 2500},       // Flush every 2500 writes
		{"FlushEvery1MB", "bytes", 1 * 1024 * 1024},   // Flush after every 1MB of data written
		{"FlushEvery5MB", "bytes", 5 * 1024 * 1024},   // Flush after every 5MB of data written
		{"FlushEvery10MB", "bytes", 10 * 1024 * 1024}, // Flush after every 10MB of data written
		{"FlushEvery30MB", "bytes", 30 * 1024 * 1024}, // Flush after every 30MB of data written
	}

	for _, strategy := range strategies {
		t.Run(strategy.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("flush_%s.data", strategy.name))

			// Create file
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("Unable to create file: %v", err)
			}

			// Set file size
			if err := file.Truncate(fileSize); err != nil {
				file.Close()
				t.Fatalf("Unable to set file size: %v", err)
			}

			// Map file to memory
			mappedFile, err := mmap.MapRegion(file, int(fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("Unable to map file: %v", err)
			}

			defer func() {
				// Clean up resources
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("Unable to unmap file: %v", err)
				}
				file.Close()
			}()

			// Prepare test data
			testData := make([]byte, dataBlockSize)
			rand.Read(testData)

			// Performance test
			dataOffset := uint32(headerSize)
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // Record data amount for each flush

			start := time.Now()

			for i := 0; i < writeCount; i++ {
				// Calculate required memory
				requiredSpace := dataOffset + 4 + uint32(dataBlockSize)

				// Check if it will exceed file size
				if requiredSpace >= uint32(fileSize) {
					t.Logf("Warning: Data will exceed file size, stopping after %d/%d writes", i, writeCount)
					break
				}

				// Write length
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(dataBlockSize))

				// Write data
				copy(mappedFile[dataOffset+4:], testData)

				// Update offset and counters
				dataOffset += 4 + uint32(dataBlockSize)
				totalBytesWritten += dataBlockSize + 4
				bytesSinceLastFlush += dataBlockSize + 4

				// Flush according to strategy
				shouldFlush := false

				switch strategy.flushType {
				case "none":
					// No intermediate flushes
					shouldFlush = false
				case "count":
					shouldFlush = (i+1)%strategy.flushParam == 0
				case "bytes":
					shouldFlush = bytesSinceLastFlush >= strategy.flushParam
				}

				if shouldFlush {
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}
					flushSizes = append(flushSizes, bytesSinceLastFlush) // Record data amount for this flush
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// Final flush to ensure all data is written to disk
			if err := mappedFile.Flush(); err != nil {
				t.Fatalf("Final flush failed: %v", err)
			}
			if bytesSinceLastFlush > 0 {
				flushSizes = append(flushSizes, bytesSinceLastFlush) // Record data amount for the last flush
			}
			flushCount++

			duration := time.Since(start)

			// Write footer (simulate real scenario)
			footerOffset := fileSize - footerSize
			binary.LittleEndian.PutUint32(mappedFile[footerOffset:], uint32(writeCount))

			// Statistics for flush data amounts
			var minFlushSize, maxFlushSize int
			totalFlushSize := 0
			if len(flushSizes) > 0 {
				minFlushSize = flushSizes[0]
				maxFlushSize = flushSizes[0]
				for _, size := range flushSizes {
					totalFlushSize += size
					if size < minFlushSize {
						minFlushSize = size
					}
					if size > maxFlushSize {
						maxFlushSize = size
					}
				}
			}
			avgFlushSize := 0
			if flushCount > 0 {
				avgFlushSize = totalFlushSize / flushCount
			}

			// Calculate performance metrics
			throughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()

			t.Logf("Flush Strategy Test Results - %s:", strategy.name)
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("  Average data per flush: %.2f KB", float64(avgFlushSize)/1024)
			t.Logf("  Minimum flush data: %.2f KB", float64(minFlushSize)/1024)
			t.Logf("  Maximum flush data: %.2f KB", float64(maxFlushSize)/1024)
			t.Logf("  Duration: %v", duration)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Average time per operation: %.3f µs", duration.Seconds()*1000000/float64(writeCount))
		})
	}
}

// TestMmapLargeBlockPerformance tests write performance with large data blocks (1MB/2MB/4MB)
func TestMmapLargeBlockPerformance(t *testing.T) {
	// Set test parameters
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // How many writes before executing a flush
	}{
		{"LargeData_1MBBlock_InfrequentFlush", 500 * 1024 * 1024, 50, 1 * 1024 * 1024, 5},
		{"LargeData_1MBBlock_SingleFlush", 500 * 1024 * 1024, 50, 1 * 1024 * 1024, 50},
		{"LargeData_2MBBlock_InfrequentFlush", 1024 * 1024 * 1024, 30, 2 * 1024 * 1024, 5},
		{"LargeData_2MBBlock_SingleFlush", 1024 * 1024 * 1024, 30, 2 * 1024 * 1024, 30},
		{"LargeData_4MBBlock_InfrequentFlush", 1024 * 1024 * 1024, 20, 4 * 1024 * 1024, 5},
		{"LargeData_4MBBlock_SingleFlush", 1024 * 1024 * 1024, 20, 4 * 1024 * 1024, 20},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_large_block_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("test_%s.data", tc.name))

			// Create file
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("Unable to create file: %v", err)
			}

			// Set file size
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("Unable to set file size: %v", err)
			}

			// Map file to memory
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("Unable to map file: %v", err)
			}

			defer func() {
				// Clean up resources
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("Unable to unmap file: %v", err)
				}
				file.Close()
			}()

			// Write basic file header
			copy(mappedFile[0:8], []byte("TESTMMAP"))
			binary.LittleEndian.PutUint32(mappedFile[8:12], 1) // Version number

			// Performance test: write data
			dataOffset := uint32(headerSize)
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // Record data amount for each flush

			// Generate random data
			t.Logf("Generating %d MB of random data...", tc.dataBlockSize/(1024*1024))
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// Calculate required memory
				requiredSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// Check if it will exceed file size
				if requiredSpace >= uint32(tc.fileSize) {
					t.Logf("Warning: Data will exceed file size, stopping after %d/%d writes", i, tc.writeCount)
					break
				}

				// Block write start time
				blockStart := time.Now()

				// Write data length (4 bytes)
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))

				// Write data
				copy(mappedFile[dataOffset+4:], randomData)

				blockDuration := time.Since(blockStart)

				// Update offset
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4
				bytesSinceLastFlush += tc.dataBlockSize + 4

				t.Logf("  Block %d write time: %v", i+1, blockDuration)

				// Execute flush according to flush interval
				if (i+1)%tc.flushInterval == 0 {
					flushStart := time.Now()
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}
					flushDuration := time.Since(flushStart)

					flushSizes = append(flushSizes, bytesSinceLastFlush) // Record data amount for this flush
					t.Logf("  Flush %d time: %v (data amount: %.2f MB)",
						flushCount+1, flushDuration, float64(bytesSinceLastFlush)/(1024*1024))

					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// Final flush to ensure all data is written to disk
			if bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := mappedFile.Flush(); err != nil {
					t.Fatalf("Final flush failed: %v", err)
				}
				flushDuration := time.Since(flushStart)

				flushSizes = append(flushSizes, bytesSinceLastFlush) // Record data amount for the last flush
				t.Logf("  Final flush time: %v (data amount: %.2f MB)",
					flushDuration, float64(bytesSinceLastFlush)/(1024*1024))

				flushCount++
			}

			duration := time.Since(start)

			// Statistics for flush data amounts
			var minFlushSize, maxFlushSize int
			totalFlushSize := 0
			if len(flushSizes) > 0 {
				minFlushSize = flushSizes[0]
				maxFlushSize = flushSizes[0]
				for _, size := range flushSizes {
					totalFlushSize += size
					if size < minFlushSize {
						minFlushSize = size
					}
					if size > maxFlushSize {
						maxFlushSize = size
					}
				}
			}
			avgFlushSize := 0
			if flushCount > 0 {
				avgFlushSize = totalFlushSize / flushCount
			}

			// Calculate performance metrics
			throughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			avgOpTime := duration.Seconds() * 1000 / float64(tc.writeCount) // milliseconds

			t.Logf("Test Results - %s:", tc.name)
			t.Logf("  File size: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  Write count: %d", tc.writeCount)
			t.Logf("  Data block size: %d MB", tc.dataBlockSize/(1024*1024))
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("  Average data per flush: %.2f MB", float64(avgFlushSize)/(1024*1024))
			t.Logf("  Minimum flush data: %.2f MB", float64(minFlushSize)/(1024*1024))
			t.Logf("  Maximum flush data: %.2f MB", float64(maxFlushSize)/(1024*1024))
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Average time per operation: %.2f ms", avgOpTime)
		})
	}
}

// TestMmapMixedWritePattern tests mixed write pattern:
// 1. Sequential data block writes from the beginning of the file
// 2. Simultaneously writing 8-byte index/metadata at the end of the file
// This pattern is common in log files or databases, with data at the front and index/metadata at the end
func TestMmapMixedWritePattern(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // How many writes before executing a flush
	}{
		{"SmallFile_SmallBlock_FrequentFlush", 50 * 1024 * 1024, 1000, 4 * 1024, 10},
		{"SmallFile_SmallBlock_InfrequentFlush", 50 * 1024 * 1024, 1000, 4 * 1024, 100},
		{"SmallFile_LargeBlock_FrequentFlush", 200 * 1024 * 1024, 500, 32 * 1024, 10},
		{"SmallFile_LargeBlock_InfrequentFlush", 200 * 1024 * 1024, 500, 32 * 1024, 100},
		{"LargeFile_SmallBlock_FrequentFlush", 500 * 1024 * 1024, 5000, 4 * 1024, 50},
		{"LargeFile_SmallBlock_InfrequentFlush", 500 * 1024 * 1024, 5000, 4 * 1024, 500},
		{"LargeFile_LargeBlock_FrequentFlush", 1024 * 1024 * 1024, 1000, 64 * 1024, 20},
		{"LargeFile_LargeBlock_InfrequentFlush", 1024 * 1024 * 1024, 1000, 64 * 1024, 200},
		{"LargeFile_VeryLargeBlock_FrequentFlush", 1024 * 1024 * 1024, 500, 1024 * 1024, 10},
		{"LargeFile_VeryLargeBlock_InfrequentFlush", 1024 * 1024 * 1024, 500, 1024 * 1024, 100},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_mixed_write_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("mixed_%s.data", tc.name))

			// Create file
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("Unable to create file: %v", err)
			}

			// Set file size
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("Unable to set file size: %v", err)
			}

			// Map file to memory
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("Unable to map file: %v", err)
			}

			defer func() {
				// Clean up resources
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("Unable to unmap file: %v", err)
				}
				file.Close()
			}()

			// Write basic file header
			copy(mappedFile[0:8], []byte("MIXWRITE"))
			binary.LittleEndian.PutUint32(mappedFile[8:12], 1) // Version number

			// Prepare footer metadata area (last 4KB of file used for metadata indices)
			footerOffset := tc.fileSize - footerSize

			// Performance test parameters
			dataOffset := uint32(headerSize)
			totalHeadWrites := 0
			totalTailWrites := 0
			totalBytesWritten := 0
			flushCount := 0
			headWriteTime := time.Duration(0)
			tailWriteTime := time.Duration(0)
			flushTime := time.Duration(0)

			// Generate random data block
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// Calculate required space for head write
				requiredHeadSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// Check if it will exceed the safe area (preserving footer area)
				if requiredHeadSpace >= uint32(footerOffset) {
					t.Logf("Warning: Head data will exceed safe area, stopping after %d/%d writes", i, tc.writeCount)
					break
				}

				// 1. Write data block to head
				headStart := time.Now()

				// Write data length (4 bytes)
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))

				// Write data block
				copy(mappedFile[dataOffset+4:], randomData)

				// Update offset
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4
				totalHeadWrites++

				headWriteTime += time.Since(headStart)

				// 2. Write metadata at tail (8 bytes, recording latest data block position)
				tailStart := time.Now()

				// Write latest data block offset and sequence number in the tail area
				tailOffset := footerOffset + int64(i*8)%(footerSize-8)
				binary.LittleEndian.PutUint32(mappedFile[tailOffset:], dataOffset-uint32(tc.dataBlockSize)-4) // Data block start position
				binary.LittleEndian.PutUint32(mappedFile[tailOffset+4:], uint32(i))                           // Data block sequence number

				totalTailWrites++
				totalBytesWritten += 8

				tailWriteTime += time.Since(tailStart)

				// Execute flush according to flush interval
				if (i+1)%tc.flushInterval == 0 {
					flushStart := time.Now()
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}
					flushTime += time.Since(flushStart)
					flushCount++
				}
			}

			// Final flush to ensure all data is written to disk
			flushStart := time.Now()
			if err := mappedFile.Flush(); err != nil {
				t.Fatalf("Final flush failed: %v", err)
			}
			flushTime += time.Since(flushStart)
			flushCount++

			totalDuration := time.Since(start)

			// Calculate performance metrics
			dataThroughput := float64(totalBytesWritten) / (1024 * 1024) / totalDuration.Seconds()
			headAvgTime := headWriteTime.Seconds() * 1000 / float64(totalHeadWrites) // milliseconds
			tailAvgTime := tailWriteTime.Seconds() * 1000 / float64(totalTailWrites) // milliseconds
			flushAvgTime := flushTime.Seconds() * 1000 / float64(flushCount)         // milliseconds

			// Output results
			t.Logf("Mixed Write Pattern Test Results - %s:", tc.name)
			t.Logf("  File size: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  Data block size: %d KB", tc.dataBlockSize/1024)
			t.Logf("  Write count: %d (head), %d (tail)", totalHeadWrites, totalTailWrites)
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("  Flush interval: every %d writes", tc.flushInterval)
			t.Logf("  Total duration: %v", totalDuration)
			t.Logf("  Head write total time: %v (%.1f%%)", headWriteTime, float64(headWriteTime)/float64(totalDuration)*100)
			t.Logf("  Tail write total time: %v (%.1f%%)", tailWriteTime, float64(tailWriteTime)/float64(totalDuration)*100)
			t.Logf("  Flush total time: %v (%.1f%%)", flushTime, float64(flushTime)/float64(totalDuration)*100)
			t.Logf("  Throughput: %.2f MB/s", dataThroughput)
			t.Logf("  Average time per head write: %.3f ms", headAvgTime)
			t.Logf("  Average time per tail write: %.3f ms", tailAvgTime)
			t.Logf("  Average time per flush: %.3f ms", flushAvgTime)
		})
	}
}

// TestMmapWritePerformanceDetailed tests detailed performance metrics for mmap write operations
// Special focus on time distribution of copy and flush operations
func TestMmapWritePerformanceDetailed(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // How many writes before executing a flush
	}{
		{"SmallDataBlock_4KB", 1024 * 1024 * 1024, 100000, 4 * 1024, 1000},
		{"MediumDataBlock_64KB", 1024 * 1024 * 1024, 10000, 64 * 1024, 100},
		{"LargeDataBlock_1MB", 1024 * 1024 * 1024, 1000, 1 * 1024 * 1024, 10},
		{"VeryLargeDataBlock_4MB", 4 * 1024 * 1024 * 1024, 250, 4 * 1024 * 1024, 5},
		{"VeryLargeDataBlock_16MB", 4 * 1024 * 1024 * 1024, 62, 16 * 1024 * 1024, 2},
		{"ExtremeDataBlock_64MB", 8 * 1024 * 1024 * 1024, 16, 64 * 1024 * 1024, 1},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_detailed_perf_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("detailed_%s.data", tc.name))

			// Measure file creation time
			fileCreateStart := time.Now()
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("Unable to create file: %v", err)
			}

			// Set file size
			truncateStart := time.Now()
			fileCreateTime := truncateStart.Sub(fileCreateStart)

			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("Unable to set file size: %v", err)
			}
			truncateTime := time.Since(truncateStart)

			// Measure mapping time
			mmapStart := time.Now()
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("Unable to map file: %v", err)
			}
			mmapTime := time.Since(mmapStart)

			defer func() {
				// Measure unmapping time
				unmapStart := time.Now()
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("Unable to unmap file: %v", err)
				}
				unmapTime := time.Since(unmapStart)
				t.Logf("  Unmapping time: %v", unmapTime)
				file.Close()
			}()

			// Generate random data
			dataGenStart := time.Now()
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)
			dataGenTime := time.Since(dataGenStart)

			// Data write statistics
			copyTimes := make([]time.Duration, 0, tc.writeCount)
			flushTimes := make([]time.Duration, 0, tc.writeCount/tc.flushInterval+1)
			totalBytesWritten := 0
			flushCount := 0

			// Data structure to store write sub-operation times
			type WriteOpTimes struct {
				lengthWriteTime time.Duration
				dataCopyTime    time.Duration
				totalWriteTime  time.Duration
			}
			writeOpTimes := make([]WriteOpTimes, 0, tc.writeCount)

			// Starting offset for data writes
			dataOffset := uint32(headerSize)

			// Measure total write time
			totalWriteStart := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				opStart := time.Now()

				// Calculate required memory
				requiredSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// Check if it will exceed file size
				if requiredSpace >= uint32(tc.fileSize) {
					t.Logf("Warning: Data will exceed file size, stopping after %d/%d writes", i, tc.writeCount)
					break
				}

				// Measure time to write length field
				lengthWriteStart := time.Now()
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))
				lengthWriteTime := time.Since(lengthWriteStart)

				// Measure data copy time
				copyStart := time.Now()
				copy(mappedFile[dataOffset+4:], randomData)
				copyTime := time.Since(copyStart)
				copyTimes = append(copyTimes, copyTime)

				// Update statistics
				writeOpTimes = append(writeOpTimes, WriteOpTimes{
					lengthWriteTime: lengthWriteTime,
					dataCopyTime:    copyTime,
					totalWriteTime:  time.Since(opStart),
				})

				// Update offset and counters
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4

				// Execute flush according to flush interval
				if (i+1)%tc.flushInterval == 0 || i == tc.writeCount-1 {
					flushStart := time.Now()
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}
					flushTime := time.Since(flushStart)
					flushTimes = append(flushTimes, flushTime)
					flushCount++
				}
			}

			totalWriteTime := time.Since(totalWriteStart)

			// Calculate performance metrics
			bytesPerSecond := float64(0)
			mbPerSecond := float64(0)
			if totalWriteTime > 0 {
				bytesPerSecond = float64(totalBytesWritten) / totalWriteTime.Seconds()
				mbPerSecond = bytesPerSecond / (1024 * 1024)
			}

			// Calculate copy and flush time statistics
			var totalCopyTime time.Duration
			var totalFlushTime time.Duration
			var minCopyTime, maxCopyTime time.Duration
			var minFlushTime, maxFlushTime time.Duration

			if len(copyTimes) > 0 {
				minCopyTime = copyTimes[0]
				maxCopyTime = copyTimes[0]
				for _, t := range copyTimes {
					totalCopyTime += t
					if t < minCopyTime {
						minCopyTime = t
					}
					if t > maxCopyTime {
						maxCopyTime = t
					}
				}
			}

			if len(flushTimes) > 0 {
				minFlushTime = flushTimes[0]
				maxFlushTime = flushTimes[0]
				for _, t := range flushTimes {
					totalFlushTime += t
					if t < minFlushTime {
						minFlushTime = t
					}
					if t > maxFlushTime {
						maxFlushTime = t
					}
				}
			}

			avgCopyTimeNs := int64(0)
			if len(copyTimes) > 0 {
				avgCopyTimeNs = totalCopyTime.Nanoseconds() / int64(len(copyTimes))
			}

			avgFlushTimeMs := float64(0)
			if len(flushTimes) > 0 {
				avgFlushTimeMs = float64(totalFlushTime.Milliseconds()) / float64(len(flushTimes))
			}

			copyThroughputMBs := float64(0)
			if totalCopyTime > 0 {
				copyThroughputMBs = float64(totalBytesWritten) / (1024 * 1024) / totalCopyTime.Seconds()
			}

			// Calculate operations per second (IOPS)
			iops := float64(0)
			if totalWriteTime > 0 {
				iops = float64(len(copyTimes)) / totalWriteTime.Seconds()
			}

			// Calculate copy operation distribution
			copyNanosP50 := calculatePercentile(copyTimes, 50)
			copyNanosP95 := calculatePercentile(copyTimes, 95)
			copyNanosP99 := calculatePercentile(copyTimes, 99)

			// Calculate flush operation distribution
			flushMsP50 := float64(calculatePercentile(flushTimes, 50)) / float64(time.Millisecond)
			flushMsP95 := float64(calculatePercentile(flushTimes, 95)) / float64(time.Millisecond)
			flushMsP99 := float64(calculatePercentile(flushTimes, 99)) / float64(time.Millisecond)

			// Output detailed performance report
			t.Logf("======== Mmap Write Detailed Performance Test - %s ========", tc.name)
			t.Logf("Initialization Phase:")
			t.Logf("  File creation time: %v", fileCreateTime)
			t.Logf("  File truncate time: %v", truncateTime)
			t.Logf("  Mmap mapping time: %v", mmapTime)
			t.Logf("  Test data generation time: %v", dataGenTime)
			t.Logf("\nFile and Data Information:")
			t.Logf("  File size: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  Data block size: %s", formatSize(int64(tc.dataBlockSize)))
			t.Logf("  Write count: %d", len(copyTimes))
			t.Logf("  Total data written: %s", formatSize(int64(totalBytesWritten)))
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("\nPerformance Summary:")
			t.Logf("  Total write time: %v", totalWriteTime)
			t.Logf("  Average write throughput: %.2f MB/s", mbPerSecond)
			t.Logf("  I/O operations per second (IOPS): %.2f", iops)
			t.Logf("\nCopy Operation Performance Analysis:")
			t.Logf("  Total copy operation time: %v (%.1f%% of total time)", totalCopyTime, float64(totalCopyTime)/float64(totalWriteTime)*100)
			t.Logf("  Average copy time per operation: %d ns", avgCopyTimeNs)
			t.Logf("  Copy operation latency distribution: P50=%d ns, P95=%d ns, P99=%d ns",
				copyNanosP50, copyNanosP95, copyNanosP99)
			t.Logf("  Copy operation throughput: %.2f MB/s", copyThroughputMBs)
			t.Logf("  Shortest copy time: %v", minCopyTime)
			t.Logf("  Longest copy time: %v", maxCopyTime)
			t.Logf("\nFlush Operation Performance Analysis:")
			t.Logf("  Total flush operation time: %v (%.1f%% of total time)", totalFlushTime, float64(totalFlushTime)/float64(totalWriteTime)*100)
			t.Logf("  Average flush time per operation: %.2f ms", avgFlushTimeMs)
			t.Logf("  Flush operation latency distribution: P50=%.2f ms, P95=%.2f ms, P99=%.2f ms",
				flushMsP50, flushMsP95, flushMsP99)
			t.Logf("  Shortest flush time: %v", minFlushTime)
			t.Logf("  Longest flush time: %v", maxFlushTime)

			// Fix divide by zero error
			avgFlushDataSize := "0 bytes"
			if flushCount > 0 {
				avgFlushDataSize = formatSize(int64(totalBytesWritten / flushCount))
			}
			t.Logf("  Average data per flush: %s", avgFlushDataSize)
			t.Logf("\nConclusion:")

			if copyThroughputMBs > 3000 {
				t.Logf("  Memory copy speed is very fast (%.2f MB/s), approaching theoretical memory bandwidth limits", copyThroughputMBs)
			} else if copyThroughputMBs > 1000 {
				t.Logf("  Memory copy speed is good (%.2f MB/s)", copyThroughputMBs)
			} else {
				t.Logf("  Memory copy speed is slow (%.2f MB/s), there may be bottlenecks", copyThroughputMBs)
			}

			// Analyze overall performance bottlenecks
			copyPercentage := float64(0)
			flushPercentage := float64(0)
			otherPercentage := float64(0)

			if totalWriteTime > 0 {
				copyPercentage = float64(totalCopyTime) / float64(totalWriteTime) * 100
				flushPercentage = float64(totalFlushTime) / float64(totalWriteTime) * 100
				otherPercentage = 100 - copyPercentage - flushPercentage
			}

			t.Logf("  Overall performance distribution: copy operations %.1f%%, flush operations %.1f%%, other operations %.1f%%",
				copyPercentage, flushPercentage, otherPercentage)

			if flushPercentage > 50 {
				t.Logf("  Performance bottleneck: Flush operations are the main bottleneck, reducing flush frequency may improve overall performance")
			} else if copyPercentage > 50 {
				t.Logf("  Performance bottleneck: Memory copy is the main bottleneck, consider using larger block sizes or optimizing memory access patterns")
			} else {
				t.Logf("  Performance distribution is balanced, no obvious bottlenecks")
			}
		})
	}
}

// calculatePercentile calculates the specified percentile of a duration slice (returns nanoseconds)
func calculatePercentile(durations []time.Duration, percentile int) int64 {
	if len(durations) == 0 {
		return 0
	}

	// Copy slice to avoid modifying original data
	durationsCopy := make([]time.Duration, len(durations))
	copy(durationsCopy, durations)

	// Convert to nanoseconds and sort
	nanoseconds := make([]int64, len(durationsCopy))
	for i, d := range durationsCopy {
		nanoseconds[i] = d.Nanoseconds()
	}
	sort.Slice(nanoseconds, func(i, j int) bool { return nanoseconds[i] < nanoseconds[j] })

	// Calculate percentile
	index := int(float64(len(nanoseconds)-1) * float64(percentile) / 100.0)
	return nanoseconds[index]
}

// formatSize formats bytes into human-readable size
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}
