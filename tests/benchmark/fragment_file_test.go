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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/server/storage/disk"
)

// TestFragmentWritePerformance tests FragmentFile write performance
// Focus: impact of data block size, flush frequency, and file size on write performance
func TestFragmentWritePerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name          string
		fileSize      int64 // File size
		dataBlockSize int   // Size of each data block to write
		flushInterval int   // How many writes before executing a flush
		writeCount    int   // Total number of writes
	}{
		// Small data block tests
		{"SmallBlock_SmallFile_NoFlush", 10 * 1024 * 1024, 4 * 1024, 0, 1000},
		{"SmallBlock_SmallFile_ModerateFlush", 10 * 1024 * 1024, 4 * 1024, 100, 1000},
		{"SmallBlock_SmallFile_FrequentFlush", 10 * 1024 * 1024, 4 * 1024, 10, 1000},
		{"SmallBlock_LargeFile_NoFlush", 100 * 1024 * 1024, 4 * 1024, 0, 10000},
		{"SmallBlock_LargeFile_ModerateFlush", 100 * 1024 * 1024, 4 * 1024, 100, 10000},
		{"SmallBlock_LargeFile_FrequentFlush", 100 * 1024 * 1024, 4 * 1024, 10, 10000},

		// Medium data block tests
		{"MediumBlock_SmallFile_NoFlush", 10 * 1024 * 1024, 32 * 1024, 0, 250},
		{"MediumBlock_SmallFile_ModerateFlush", 10 * 1024 * 1024, 32 * 1024, 50, 250},
		{"MediumBlock_SmallFile_FrequentFlush", 10 * 1024 * 1024, 32 * 1024, 10, 250},
		{"MediumBlock_LargeFile_NoFlush", 100 * 1024 * 1024, 32 * 1024, 0, 2500},
		{"MediumBlock_LargeFile_ModerateFlush", 100 * 1024 * 1024, 32 * 1024, 50, 2500},
		{"MediumBlock_LargeFile_FrequentFlush", 100 * 1024 * 1024, 32 * 1024, 10, 2500},

		// Large data block tests
		{"LargeBlock_SmallFile_NoFlush", 20 * 1024 * 1024, 256 * 1024, 0, 50},
		{"LargeBlock_SmallFile_ModerateFlush", 20 * 1024 * 1024, 256 * 1024, 10, 50},
		{"LargeBlock_SmallFile_FrequentFlush", 20 * 1024 * 1024, 256 * 1024, 5, 50},
		{"LargeBlock_LargeFile_NoFlush", 200 * 1024 * 1024, 256 * 1024, 0, 500},
		{"LargeBlock_LargeFile_ModerateFlush", 200 * 1024 * 1024, 256 * 1024, 10, 500},
		{"LargeBlock_LargeFile_FrequentFlush", 200 * 1024 * 1024, 256 * 1024, 5, 500},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fragment_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("fragment_%s.data", tc.name))

			// Create FragmentFile instance
			fragment, err := disk.NewFragmentFileWriter(filePath, tc.fileSize, 1, 0, 1, 1)
			if err != nil {
				t.Fatalf("Failed to create FragmentFile: %v", err)
			}

			defer fragment.Release()

			// Prepare test data and context
			ctx := context.Background()
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // Record data amount for each flush

			// Generate random test data
			dataSet := make([][]byte, tc.writeCount)
			for i := 0; i < tc.writeCount; i++ {
				data := make([]byte, tc.dataBlockSize)
				rand.Read(data)
				dataSet[i] = data
			}

			// Record write and flush times
			writeTime := time.Duration(0)
			flushTime := time.Duration(0)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// Write data
				writeStart := time.Now()
				err := fragment.Write(ctx, dataSet[i], int64(i))
				writeTime += time.Since(writeStart)

				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}

				totalBytesWritten += len(dataSet[i])
				bytesSinceLastFlush += len(dataSet[i])

				// Execute flush according to flush strategy
				shouldFlush := tc.flushInterval > 0 && (i+1)%tc.flushInterval == 0

				if shouldFlush {
					flushStart := time.Now()
					err := fragment.Flush(ctx)
					flushTime += time.Since(flushStart)

					if err != nil {
						t.Fatalf("Failed to flush file: %v", err)
					}

					flushSizes = append(flushSizes, bytesSinceLastFlush) // Record amount of data for this flush
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// Final flush to ensure all data is written to disk
			if bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := fragment.Flush(ctx); err != nil {
					t.Fatalf("Final flush failed: %v", err)
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
			opsPerSecond := float64(tc.writeCount) / duration.Seconds()
			writeAvgTime := writeTime.Seconds() * 1000 / float64(tc.writeCount) // milliseconds
			flushAvgTime := 0.0
			if flushCount > 0 {
				flushAvgTime = flushTime.Seconds() * 1000 / float64(flushCount) // milliseconds
			}

			// Output results
			t.Logf("FragmentFile Write Test Results - %s:", tc.name)
			t.Logf("  File size: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Write count: %d", tc.writeCount)
			t.Logf("  Data block size: %d KB", tc.dataBlockSize/1024)
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

// TestFragmentReadPerformance tests FragmentFile read performance
// Focus: impact of read mode and data block size on read performance
func TestFragmentReadPerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name          string
		fileSize      int64 // File size
		dataBlockSize int   // Size of data blocks to write
		entryCount    int   // Number of entries to write
	}{
		{"SmallFile_SmallDataBlock", 10 * 1024 * 1024, 4 * 1024, 1000},
		{"SmallFile_MediumDataBlock", 10 * 1024 * 1024, 32 * 1024, 250},
		{"MediumFile_SmallDataBlock", 50 * 1024 * 1024, 4 * 1024, 5000},
		{"MediumFile_MediumDataBlock", 50 * 1024 * 1024, 32 * 1024, 1250},
		{"LargeFile_SmallDataBlock", 100 * 1024 * 1024, 4 * 1024, 10000},
		{"LargeFile_MediumDataBlock", 100 * 1024 * 1024, 32 * 1024, 2500},
		{"LargeFile_LargeDataBlock", 100 * 1024 * 1024, 256 * 1024, 350},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fragment_read_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("fragment_read_%s.data", tc.name))
			ctx := context.Background()

			// Step 1: First create and populate FragmentFile
			t.Logf("Preparing test file, writing %d entries...", tc.entryCount)
			fragmentWriter, err := disk.NewFragmentFileWriter(filePath, tc.fileSize, 1, 0, 1, 1)
			if err != nil {
				t.Fatalf("Failed to create FragmentFile: %v", err)
			}

			// Generate random test data and store references for later validation
			testData := make([][]byte, tc.entryCount)
			for i := 0; i < tc.entryCount; i++ {
				data := make([]byte, tc.dataBlockSize)
				rand.Read(data)
				testData[i] = data

				// Write data
				if err := fragmentWriter.Write(ctx, data, int64(i)); err != nil {
					fragmentWriter.Release()
					t.Fatalf("Failed to write data: %v", err)
				}
			}

			// Ensure all data is written to disk
			if err := fragmentWriter.Flush(ctx); err != nil {
				fragmentWriter.Release()
				t.Fatalf("Failed to flush data: %v", err)
			}

			// Close and reopen fragment to ensure reading from disk
			fragmentWriter.Release()

			// Step 2: Reopen FragmentFile and test read performance
			fragmentReader, err := disk.NewFragmentFileReader(filePath, tc.fileSize, 1, 0, 1)
			if err != nil {
				t.Fatalf("Failed to open FragmentFile: %v", err)
			}
			defer fragmentReader.Release()

			// Get entry range
			firstId, err := fragmentReader.GetFirstEntryId()
			if err != nil {
				t.Fatalf("Failed to get first entry ID: %v", err)
			}
			lastId, err := fragmentReader.GetLastEntryId()
			if err != nil {
				t.Fatalf("Failed to get last entry ID: %v", err)
			}

			t.Logf("Read test - Entry range: %d to %d", firstId, lastId)

			// Prepare variables to measure read performance
			totalBytesRead := 0
			readCount := 0
			readErrors := 0

			start := time.Now()

			// Sequentially read all entries
			for id := firstId; id <= lastId; id++ {
				data, err := fragmentReader.GetEntry(id)
				if err != nil {
					readErrors++
					continue
				}

				totalBytesRead += len(data)
				readCount++

				// Verify read data is correct
				expectedData := testData[id-firstId]
				if !bytes.Equal(data, expectedData) {
					t.Fatalf("Data mismatch: Entry ID %d", id)
				}
			}

			duration := time.Since(start)

			// Calculate performance metrics
			throughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(readCount) / duration.Seconds()
			avgReadSize := float64(totalBytesRead) / float64(readCount) / 1024 // KB

			// Output results
			t.Logf("FragmentFile Read Test Results - %s:", tc.name)
			t.Logf("  File size: %.2f MB", float64(tc.fileSize)/(1024*1024))
			t.Logf("  Total data read: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  Total entries: %d", tc.entryCount)
			t.Logf("  Successfully read entries: %d", readCount)
			t.Logf("  Read errors: %d", readErrors)
			t.Logf("  Data block size: %d KB", tc.dataBlockSize/1024)
			t.Logf("  Average actual read: %.2f KB/op", avgReadSize)
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Operations rate: %.2f ops/s", opsPerSecond)
			t.Logf("  Average read time: %.3f µs", duration.Seconds()*1000000/float64(readCount))
		})
	}
}

// TestFragmentMixedPerformance tests FragmentFile performance in mixed read/write scenarios
func TestFragmentMixedPerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name           string
		fileSize       int
		dataBlockSize  int
		flushInterval  int
		writeCount     int
		readPercentage int
	}{
		{"SmallFile_BalancedReadWrite", 10 * 1024 * 1024, 4 * 1024, 10, 1000, 50},
		{"MediumFile_BalancedReadWrite", 50 * 1024 * 1024, 32 * 1024, 10, 1000, 50},
		{"LargeFile_BalancedReadWrite", 200 * 1024 * 1024, 256 * 1024, 10, 500, 50},
		{"SmallFile_ReadHeavy", 10 * 1024 * 1024, 4 * 1024, 10, 1000, 80},
		{"MediumFile_ReadHeavy", 50 * 1024 * 1024, 32 * 1024, 10, 1000, 80},
		{"LargeFile_ReadHeavy", 200 * 1024 * 1024, 256 * 1024, 10, 500, 80},
		{"SmallFile_WriteHeavy", 10 * 1024 * 1024, 4 * 1024, 10, 1000, 20},
		{"MediumFile_WriteHeavy", 50 * 1024 * 1024, 32 * 1024, 10, 1000, 20},
		{"LargeFile_WriteHeavy", 200 * 1024 * 1024, 256 * 1024, 10, 500, 20},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fragment_mixed_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("fragment_mixed_%s.data", tc.name))
			ctx := context.Background()

			// Create FragmentFile instance
			fragmentWriter, err := disk.NewFragmentFileWriter(filePath, int64(tc.fileSize), 1, 0, 1, 1)
			if err != nil {
				t.Fatalf("Failed to create FragmentFile: %v", err)
			}
			defer fragmentWriter.Release()

			// Prepare test data
			totalOps := tc.writeCount
			readOps := totalOps * tc.readPercentage / 100
			writeOps := totalOps - readOps
			dataSet := make([][]byte, writeOps)

			for i := 0; i < writeOps; i++ {
				data := make([]byte, tc.dataBlockSize)
				rand.Read(data)
				dataSet[i] = data
			}

			// Operation statistics
			writeCount := 0
			readCount := 0
			flushCount := 0
			totalBytesWritten := 0
			totalBytesRead := 0
			bytesSinceLastFlush := 0
			writeTime := time.Duration(0)
			readTime := time.Duration(0)
			flushTime := time.Duration(0)

			// Generate operation sequence (0=write, 1=read)
			operations := make([]int, totalOps)
			for i := 0; i < totalOps; i++ {
				if i < readOps {
					operations[i] = 1 // Read
				} else {
					operations[i] = 0 // Write
				}
			}
			// Shuffle operation order
			rand.Shuffle(totalOps, func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			start := time.Now()

			// Execute mixed operations
			for i, op := range operations {
				if op == 0 && writeCount < writeOps {
					// Write operation
					writeStart := time.Now()
					err := fragmentWriter.Write(ctx, dataSet[writeCount], int64(i))
					writeTime += time.Since(writeStart)

					if err != nil {
						t.Fatalf("Write failed: %v", err)
					}

					totalBytesWritten += len(dataSet[writeCount])
					bytesSinceLastFlush += len(dataSet[writeCount])
					writeCount++

					// Check if flush is needed
					shouldFlush := tc.flushInterval > 0 && writeCount%tc.flushInterval == 0
					if shouldFlush {
						flushStart := time.Now()
						err := fragmentWriter.Flush(ctx)
						flushTime += time.Since(flushStart)

						if err != nil {
							t.Fatalf("Failed to flush file: %v", err)
						}

						flushCount++
						bytesSinceLastFlush = 0
					}
				} else if op == 1 && readCount < writeCount { // Can only read already written entries
					// Read operation
					// Randomly select an already written entry ID
					entryId := int64(1 + rand.Intn(writeCount))

					readStart := time.Now()
					data, err := fragmentWriter.GetEntry(entryId)
					readTime += time.Since(readStart)

					if err != nil {
						// In mixed testing, we might try to read entries not yet written, ignore these errors
						continue
					}

					totalBytesRead += len(data)
					readCount++
				}

				// Report progress every 100 operations
				if (i+1)%100 == 0 || i == totalOps-1 {
					t.Logf("Progress: %d/%d operations completed", i+1, totalOps)
				}
			}

			// Final flush
			if bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := fragmentWriter.Flush(ctx); err != nil {
					t.Fatalf("Final flush failed: %v", err)
				}
				flushTime += time.Since(flushStart)
				flushCount++
			}

			duration := time.Since(start)

			// Calculate performance metrics
			writeThroughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			readThroughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			totalThroughput := float64(totalBytesWritten+totalBytesRead) / (1024 * 1024) / duration.Seconds()
			writeOpsPerSecond := float64(writeCount) / duration.Seconds()
			readOpsPerSecond := float64(readCount) / duration.Seconds()
			totalOpsPerSecond := float64(writeCount+readCount) / duration.Seconds()

			// Calculate average times if operations exist
			writeAvgTime := 0.0
			if writeCount > 0 {
				writeAvgTime = writeTime.Seconds() * 1000 / float64(writeCount) // milliseconds
			}
			readAvgTime := 0.0
			if readCount > 0 {
				readAvgTime = readTime.Seconds() * 1000 / float64(readCount) // milliseconds
			}
			flushAvgTime := 0.0
			if flushCount > 0 {
				flushAvgTime = flushTime.Seconds() * 1000 / float64(flushCount) // milliseconds
			}

			// Output results
			t.Logf("FragmentFile Mixed Test Results - %s:", tc.name)
			t.Logf("  File size: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  Data block size: %d KB", tc.dataBlockSize/1024)
			t.Logf("  Read/write ratio: %d%%read/%d%%write", tc.readPercentage, 100-tc.readPercentage)
			t.Logf("  Total operations: %d (Writes: %d, Reads: %d)", writeCount+readCount, writeCount, readCount)
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Total data read: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  Flush count: %d", flushCount)
			t.Logf("  Flush interval: every %d writes", tc.flushInterval)
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Total write time: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			t.Logf("  Total read time: %v (%.1f%%)", readTime, float64(readTime)/float64(duration)*100)
			if flushCount > 0 {
				t.Logf("  Total flush time: %v (%.1f%%)", flushTime, float64(flushTime)/float64(duration)*100)
			}
			t.Logf("  Total throughput: %.2f MB/s", totalThroughput)
			t.Logf("  Write throughput: %.2f MB/s", writeThroughput)
			t.Logf("  Read throughput: %.2f MB/s", readThroughput)
			t.Logf("  Total operations rate: %.2f ops/s", totalOpsPerSecond)
			t.Logf("  Write operations rate: %.2f ops/s", writeOpsPerSecond)
			t.Logf("  Read operations rate: %.2f ops/s", readOpsPerSecond)
			t.Logf("  Average write time: %.3f ms", writeAvgTime)
			t.Logf("  Average read time: %.3f ms", readAvgTime)
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
