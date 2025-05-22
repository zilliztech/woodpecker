// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFileAppendPerformance tests standard file append write performance
// Focus: impact of append block size, flush frequency, and preallocation size on performance
func TestFileAppendPerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name           string
		preallocSize   int64  // Preallocation size, 0 means no preallocation
		totalWriteSize int64  // Total data size to write
		blockSize      int    // Size of each data block to write
		flushInterval  int    // How many writes before executing a flush
		syncMode       string // none, flush, sync, datasync
	}{
		// Small block tests
		{"SmallBlock_NoPrealloc_NoFlush", 0, 50 * 1024 * 1024, 4 * 1024, 0, "none"},
		{"SmallBlock_NoPrealloc_FewFlush", 0, 50 * 1024 * 1024, 4 * 1024, 100, "flush"},
		{"SmallBlock_NoPrealloc_FrequentFlush", 0, 50 * 1024 * 1024, 4 * 1024, 10, "flush"},
		{"SmallBlock_Prealloc_NoFlush", 100 * 1024 * 1024, 50 * 1024 * 1024, 4 * 1024, 0, "none"},
		{"SmallBlock_Prealloc_FewFlush", 100 * 1024 * 1024, 50 * 1024 * 1024, 4 * 1024, 100, "flush"},
		{"SmallBlock_Prealloc_FrequentFlush", 100 * 1024 * 1024, 50 * 1024 * 1024, 4 * 1024, 10, "flush"},

		// Medium block tests
		{"MediumBlock_NoPrealloc_NoFlush", 0, 100 * 1024 * 1024, 32 * 1024, 0, "none"},
		{"MediumBlock_NoPrealloc_FewFlush", 0, 100 * 1024 * 1024, 32 * 1024, 50, "flush"},
		{"MediumBlock_NoPrealloc_FrequentFlush", 0, 100 * 1024 * 1024, 32 * 1024, 10, "flush"},
		{"MediumBlock_Prealloc_NoFlush", 200 * 1024 * 1024, 100 * 1024 * 1024, 32 * 1024, 0, "none"},
		{"MediumBlock_Prealloc_FewFlush", 200 * 1024 * 1024, 100 * 1024 * 1024, 32 * 1024, 50, "flush"},
		{"MediumBlock_Prealloc_FrequentFlush", 200 * 1024 * 1024, 100 * 1024 * 1024, 32 * 1024, 10, "flush"},

		// Large block tests
		{"LargeBlock_NoPrealloc_NoFlush", 0, 200 * 1024 * 1024, 1 * 1024 * 1024, 0, "none"},
		{"LargeBlock_NoPrealloc_FewFlush", 0, 200 * 1024 * 1024, 1 * 1024 * 1024, 20, "flush"},
		{"LargeBlock_NoPrealloc_FrequentFlush", 0, 200 * 1024 * 1024, 1 * 1024 * 1024, 5, "flush"},
		{"LargeBlock_Prealloc_NoFlush", 500 * 1024 * 1024, 200 * 1024 * 1024, 1 * 1024 * 1024, 0, "none"},
		{"LargeBlock_Prealloc_FewFlush", 500 * 1024 * 1024, 200 * 1024 * 1024, 1 * 1024 * 1024, 20, "flush"},
		{"LargeBlock_Prealloc_FrequentFlush", 500 * 1024 * 1024, 200 * 1024 * 1024, 1 * 1024 * 1024, 5, "flush"},

		// Sync tests (ensure data persistence)
		{"SmallBlock_Sync", 0, 10 * 1024 * 1024, 4 * 1024, 50, "sync"},
		{"MediumBlock_Sync", 0, 20 * 1024 * 1024, 32 * 1024, 10, "sync"},
		{"LargeBlock_Sync", 0, 50 * 1024 * 1024, 1 * 1024 * 1024, 5, "sync"},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("file_append_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("append_%s.data", tc.name))

			// Create file
			var file *os.File
			var err error

			flags := os.O_CREATE | os.O_WRONLY
			if tc.preallocSize > 0 {
				// If preallocation is needed, create the file and set its size first
				file, err = os.OpenFile(filePath, flags, 0644)
				if err != nil {
					t.Fatalf("Cannot create file: %v", err)
				}

				// Use truncate to preallocate space
				if err := file.Truncate(tc.preallocSize); err != nil {
					file.Close()
					t.Fatalf("Cannot preallocate file space: %v", err)
				}

				// Close file and reopen in append mode
				file.Close()
				flags |= os.O_APPEND
				file, err = os.OpenFile(filePath, flags, 0644)
			} else {
				// No preallocation, open directly in append mode
				flags |= os.O_APPEND
				file, err = os.OpenFile(filePath, flags, 0644)
			}

			if err != nil {
				t.Fatalf("Cannot open file: %v", err)
			}

			defer func() {
				// Clean up resources
				if err := file.Close(); err != nil {
					t.Errorf("Cannot close file: %v", err)
				}
			}()

			// Prepare test parameters
			writeCount := int(tc.totalWriteSize) / tc.blockSize
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // Record data amount for each flush

			// Generate random test data
			randomData := make([]byte, tc.blockSize)
			rand.Read(randomData)

			// Record write and flush times
			writeTime := time.Duration(0)
			flushTime := time.Duration(0)

			start := time.Now()

			for i := 0; i < writeCount; i++ {
				// Write data
				writeStart := time.Now()
				n, err := file.Write(randomData)
				writeTime += time.Since(writeStart)

				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}

				totalBytesWritten += n
				bytesSinceLastFlush += n

				// Execute flush according to flush strategy
				shouldFlush := tc.flushInterval > 0 && (i+1)%tc.flushInterval == 0

				if shouldFlush {
					flushStart := time.Now()

					switch tc.syncMode {
					case "flush":
						// Only flush file buffer
						err = file.Sync()
					case "sync":
						// Complete sync to disk, including metadata
						err = file.Sync()
					case "datasync":
						// Only sync data to disk, not including metadata
						// On Linux, fdatasync system call could be used, simplified to Sync here
						err = file.Sync()
					}

					if err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}

					flushTime += time.Since(flushStart)
					flushSizes = append(flushSizes, bytesSinceLastFlush) // Record amount of data for this flush
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// Final flush to ensure all data is written to disk
			if tc.syncMode != "none" && bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := file.Sync(); err != nil {
					t.Fatalf("Final file flush failed: %v", err)
				}
				flushTime += time.Since(flushStart)
				flushSizes = append(flushSizes, bytesSinceLastFlush)
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
			opsPerSecond := float64(writeCount) / duration.Seconds()
			writeAvgTime := writeTime.Seconds() * 1000 / float64(writeCount) // milliseconds
			flushAvgTime := 0.0
			if flushCount > 0 {
				flushAvgTime = flushTime.Seconds() * 1000 / float64(flushCount) // milliseconds
			}

			// Output results
			t.Logf("Standard File Append Test Results - %s:", tc.name)
			t.Logf("  Preallocation size: %d MB", tc.preallocSize/(1024*1024))
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Write count: %d", writeCount)
			t.Logf("  Data block size: %d KB", tc.blockSize/1024)
			t.Logf("  Flush mode: %s", tc.syncMode)
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("  Flush interval: every %d writes", tc.flushInterval)
			if flushCount > 0 {
				t.Logf("  Average data per flush: %.2f KB", float64(avgFlushSize)/1024)
				t.Logf("  Minimum flush data: %.2f KB", float64(minFlushSize)/1024)
				t.Logf("  Maximum flush data: %.2f KB", float64(maxFlushSize)/1024)
			}
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Total write time: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			if flushCount > 0 {
				t.Logf("  Total flush time: %v (%.1f%%)", flushTime, float64(flushTime)/float64(duration)*100)
			}
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Operations rate: %.2f ops/s", opsPerSecond)
			t.Logf("  Average write time: %.3f ms", writeAvgTime)
			if flushCount > 0 {
				t.Logf("  Average flush time: %.3f ms", flushAvgTime)
			}

			// Get final file size
			fileInfo, err := os.Stat(filePath)
			if err == nil {
				t.Logf("  Final file size: %.2f MB", float64(fileInfo.Size())/(1024*1024))
			}
		})
	}
}

// TestFileReadPerformance tests standard file sequential read performance
// Focus: impact of read block size and file size on read performance
func TestFileReadPerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name          string
		fileSize      int64 // File size for testing
		readBlockSize int   // Size of each data block to read
	}{
		{"SmallFile_SmallBlockRead", 10 * 1024 * 1024, 4 * 1024},
		{"SmallFile_LargeBlockRead", 10 * 1024 * 1024, 64 * 1024},
		{"MediumFile_SmallBlockRead", 50 * 1024 * 1024, 4 * 1024},
		{"MediumFile_LargeBlockRead", 50 * 1024 * 1024, 64 * 1024},
		{"LargeFile_SmallBlockRead", 200 * 1024 * 1024, 4 * 1024},
		{"LargeFile_MediumBlockRead", 200 * 1024 * 1024, 64 * 1024},
		{"LargeFile_LargeBlockRead", 200 * 1024 * 1024, 1 * 1024 * 1024},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("file_read_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("read_%s.data", tc.name))

			// First create and fill the test file
			t.Logf("Preparing test file, size: %d MB...", tc.fileSize/(1024*1024))

			// Create file and preallocate space
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("Cannot create file: %v", err)
			}

			// Use truncate to preallocate space
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("Cannot preallocate file space: %v", err)
			}

			// Generate random data and fill the file
			writeBlockSize := 1 * 1024 * 1024 // Use 1MB blocks to quickly fill the file
			writeBuffer := make([]byte, writeBlockSize)
			rand.Read(writeBuffer) // Generate random content

			bytesWritten := int64(0)
			for bytesWritten < tc.fileSize {
				writeSize := writeBlockSize
				if bytesWritten+int64(writeSize) > tc.fileSize {
					writeSize = int(tc.fileSize - bytesWritten)
				}

				n, err := file.Write(writeBuffer[:writeSize])
				if err != nil {
					file.Close()
					t.Fatalf("Failed to write to file: %v", err)
				}

				bytesWritten += int64(n)
			}

			file.Close()

			// Start read test
			readFile, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("Cannot open file for reading: %v", err)
			}

			defer readFile.Close()

			readBuffer := make([]byte, tc.readBlockSize)
			totalBytesRead := 0
			readCount := 0

			start := time.Now()

			// Sequential read of the entire file
			for {
				n, err := readFile.Read(readBuffer)
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Fatalf("Failed to read file: %v", err)
				}

				totalBytesRead += n
				readCount++
			}

			duration := time.Since(start)

			// Calculate performance metrics
			throughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(readCount) / duration.Seconds()
			avgReadSize := float64(totalBytesRead) / float64(readCount) / 1024 // KB

			// Output results
			t.Logf("Standard File Read Test Results - %s:", tc.name)
			t.Logf("  File size: %.2f MB", float64(tc.fileSize)/(1024*1024))
			t.Logf("  Total data read: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  Read count: %d", readCount)
			t.Logf("  Read block size: %d KB", tc.readBlockSize/1024)
			t.Logf("  Average actual read: %.2f KB/op", avgReadSize)
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Operations rate: %.2f ops/s", opsPerSecond)
			t.Logf("  Average read time: %.3f µs", duration.Seconds()*1000000/float64(readCount))
		})
	}
}

func TestFileFsyncPerformance(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestFileFsyncPerformance.log")
	file, err := os.Create(rootPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buf := make([]byte, 8*1024*1024) // write 1MB and flush immediately
	total := 0
	start := time.Now()

	for i := 0; i < 100; i++ {
		_, err := file.Write(buf)
		if err != nil {
			panic(err)
		}

		// 开启 fsync
		err = file.Sync()
		if err != nil {
			panic(err)
		}

		total += len(buf)
	}

	duration := time.Since(start).Seconds()
	fmt.Printf("Total written: %.2f MB, Time: %.2f s, Throughput: %.2f MB/s\n",
		float64(total)/1024/1024, duration, float64(total)/1024/1024/duration)
}
