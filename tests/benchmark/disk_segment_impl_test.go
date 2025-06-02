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
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
)

// Returns the smaller of a and b
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestDiskSegmentImplWritePerformance tests DiskSegmentImpl write performance
// Focus: impact of buffer size, flush frequency, and file size on write performance
func TestDiskSegmentImplWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "info"
	logger.InitLogger(cfg)

	// Test parameters
	testCases := []struct {
		name         string
		fragmentSize int64         // Fragment file size
		bufferSize   int64         // Maximum buffer size
		dataSize     int           // Size of each data entry
		writeCount   int           // Total number of writes
		flushRate    time.Duration // Flush frequency, 0 means no auto flush
	}{
		// Small data test - reduced data volume for easier debugging
		{"SmallFile_SmallBuffer_NoAutoFlush", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 100, 0},
		{"SmallFile_SmallBuffer_HighFreqFlush", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 100, 100 * time.Millisecond},
		{"SmallFile_LargeBuffer_NoAutoFlush", 10 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024, 1000, 0},
		{"SmallFile_LargeBuffer_LowFreqFlush", 10 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024, 1000, 500 * time.Millisecond},

		// Medium data test
		{"MediumFile_SmallBuffer_NoAutoFlush", 50 * 1024 * 1024, 1 * 1024 * 1024, 32 * 1024, 500, 0},
		{"MediumFile_SmallBuffer_HighFreqFlush", 50 * 1024 * 1024, 1 * 1024 * 1024, 32 * 1024, 500, 100 * time.Millisecond},
		{"MediumFile_LargeBuffer_NoAutoFlush", 50 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 0},
		{"MediumFile_LargeBuffer_LowFreqFlush", 50 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 500 * time.Millisecond},

		// Large data test
		{"LargeFile_SmallBuffer_NoAutoFlush", 100 * 1024 * 1024, 4 * 1024 * 1024, 256 * 1024, 200, 0},
		{"LargeFile_SmallBuffer_HighFreqFlush", 100 * 1024 * 1024, 4 * 1024 * 1024, 256 * 1024, 200, 100 * time.Millisecond},
		{"LargeFile_LargeBuffer_NoAutoFlush", 100 * 1024 * 1024, 32 * 1024 * 1024, 256 * 1024, 200, 0},
		{"LargeFile_LargeBuffer_LowFreqFlush", 100 * 1024 * 1024, 32 * 1024 * 1024, 256 * 1024, 200, 500 * time.Millisecond},

		// Little big dataset Test, 2MB*500 = 1GB
		{"LargeFile_LargeBuffer_LowFreqFlush", 128 * 1024 * 1024, 32 * 1024 * 1024, 2 * 1024 * 1024, 500, 10 * time.Millisecond},

		{"LargeFile_LargeBuffer_UltraHighFreqFlush", 2 * 1024 * 1024 * 1024, 64 * 1024 * 1024, 2 * 1024 * 1024, 500, 0},
		{"LargeFile_LargeBuffer_UltraHighFreqFlush", 1 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 4 * 1024, 5000, 0},
		{"LargeFile_LargeBuffer_UltraHighFreqFlush", 1 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 2 * 1024, 5000, 0},
		{"LargeFile_LargeBuffer_UltraHighFreqFlush", 1 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 1 * 1024, 5000, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tempDir := filepath.Join(tmpDir, fmt.Sprintf("diskSegmentImpl_test_%d", time.Now().UnixNano()))

			var options []disk.Option
			options = append(options, disk.WithWriteFragmentSize(tc.fragmentSize))
			options = append(options, disk.WithWriteMaxBufferSize(tc.bufferSize))

			// If not auto-flushing, disable auto sync
			if tc.flushRate == 0 {
				options = append(options, disk.WithWriteDisableAutoSync())
			}

			// Create DiskSegmentImpl instance
			segmentImpl, err := disk.NewDiskSegmentImpl(context.TODO(), 1, 0, tempDir, options...)
			if err != nil {
				t.Fatalf("Failed to create DiskSegmentImpl: %v", err)
			}
			defer segmentImpl.Close(context.TODO())

			// Prepare test data and context
			ctx := context.Background()
			totalBytesWritten := 0
			manualFlushCount := 0
			dataSet := make([][]byte, tc.writeCount)

			// Generate random test data
			for i := 0; i < tc.writeCount; i++ {
				data := make([]byte, tc.dataSize)
				rand.Read(data)
				dataSet[i] = data
			}

			// Record write and sync times
			writeTime := time.Duration(0)
			flushTime := time.Duration(0)
			totalManualFlushTime := time.Duration(0)
			lastFlushTime := time.Now()

			start := time.Now()

			// Execute write test
			for i := 0; i < tc.writeCount; i += 1 {
				// Determine batch size
				batchSize := min(1, tc.writeCount-i)

				// Save channel for each write operation
				resultChannels := make([]<-chan int64, batchSize)
				entryIds := make([]int64, batchSize)

				// Batch submit write requests
				for j := 0; j < batchSize; j++ {
					writeStart := time.Now()
					entryId := int64(i + j)
					entryIds[j] = entryId

					_, ch, err := segmentImpl.AppendAsync(ctx, entryId, dataSet[i+j])
					if err != nil {
						t.Fatalf("Write failed: %v", err)
					}
					resultChannels[j] = ch

					writeTime += time.Since(writeStart)
					totalBytesWritten += len(dataSet[i+j])
				}

				// Check if manual flush is needed (for no auto-flush case)
				if tc.flushRate == 0 {
					flushStart := time.Now()
					err := segmentImpl.Sync(ctx)
					flushDuration := time.Since(flushStart)
					totalManualFlushTime += flushDuration

					if err != nil {
						t.Fatalf("Manual sync failed: %v", err)
					}
					manualFlushCount++
				}
				// For auto-flush cases, record time since last flush
				if tc.flushRate > 0 && time.Since(lastFlushTime) >= tc.flushRate {
					flushStart := time.Now()
					err := segmentImpl.Sync(ctx)
					flushDuration := time.Since(flushStart)
					flushTime += flushDuration

					if err != nil {
						t.Fatalf("Scheduled sync failed: %v", err)
					}
					lastFlushTime = time.Now()
				}

				// Wait for all writes to complete
				for j, ch := range resultChannels {
					select {
					case result := <-ch:
						if result < 0 {
							t.Fatalf("Write did not complete successfully: id=%d", entryIds[j])
						}
					case <-time.After(5 * time.Second):
						t.Fatalf("Write timeout: id=%d", entryIds[j])
					}
				}

				// Report progress
				t.Logf("Progress: %d/%d operations completed", min(i+batchSize, tc.writeCount), tc.writeCount)
			}

			// Final sync to ensure all data is written to disk
			finalFlushStart := time.Now()
			if err := segmentImpl.Sync(ctx); err != nil {
				t.Fatalf("Final sync failed: %v", err)
			}
			finalFlushTime := time.Since(finalFlushStart)

			if tc.flushRate == 0 {
				totalManualFlushTime += finalFlushTime
				manualFlushCount++
			} else {
				flushTime += finalFlushTime
			}

			duration := time.Since(start)

			// Calculate performance metrics
			throughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(tc.writeCount) / duration.Seconds()
			writeAvgTime := writeTime.Seconds() * 1000 / float64(tc.writeCount) // milliseconds

			var flushAvgTime float64
			if tc.flushRate == 0 && manualFlushCount > 0 {
				flushAvgTime = totalManualFlushTime.Seconds() * 1000 / float64(manualFlushCount) // milliseconds
			}

			// Output results
			t.Logf("DisksegmentImpl Write Test Results - %s:", tc.name)
			t.Logf("  Fragment size: %d MB", tc.fragmentSize/(1024*1024))
			t.Logf("  Buffer size: %d MB", tc.bufferSize/(1024*1024))
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Write count: %d", tc.writeCount)
			t.Logf("  Data block size: %d KB", tc.dataSize/1024)

			if tc.flushRate == 0 {
				t.Logf("  Manual flush: every 100 writes")
				t.Logf("  Manual flush count: %d", manualFlushCount)
				t.Logf("  Manual flush total time: %v (%.1f%%)", totalManualFlushTime, float64(totalManualFlushTime)/float64(duration)*100)
				t.Logf("  Average manual flush time: %.3f ms", flushAvgTime)
			} else {
				t.Logf("  Auto flush frequency: %v", tc.flushRate)
				t.Logf("  Auto flush total time: %v (%.1f%%)", flushTime, float64(flushTime)/float64(duration)*100)
			}

			t.Logf("  Total duration: %v", duration)
			t.Logf("  Write total time: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Operation rate: %.2f ops/s", opsPerSecond)
			t.Logf("  Average write time: %.3f ms", writeAvgTime)
		})
	}
}

// TestDiskSegmentImplReadPerformance tests DiskSegmentImpl read performance
// Focus: impact of read mode and data block size on read performance
func TestDiskSegmentImplReadPerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name       string
		dataSize   int  // Size of each data entry
		entryCount int  // Number of entries to write
		sequential bool // Whether to read sequentially
	}{
		{"SmallData_SequentialRead", 4 * 1024, 1000, true},
		{"SmallData_RandomRead", 4 * 1024, 1000, false},
		{"MediumData_SequentialRead", 32 * 1024, 500, true},
		{"MediumData_RandomRead", 32 * 1024, 500, false},
		{"LargeData_SequentialRead", 256 * 1024, 100, true},
		{"LargeData_RandomRead", 256 * 1024, 100, false},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("diskSegmentImpl_read_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Step 1: First create and populate DiskSegmentImpl
			t.Logf("Preparing test file, writing %d entries...", tc.entryCount)
			options := []disk.Option{
				disk.WithWriteFragmentSize(128 * 1024 * 1024), // 128MB
			}
			segmentImpl, err := disk.NewDiskSegmentImpl(context.TODO(), 1, 0, tempDir, options...)
			if err != nil {
				t.Fatalf("Failed to create DisksegmentImpl: %v", err)
			}

			// Generate random test data and store references for later validation
			testData := make([][]byte, tc.entryCount)
			entryIds := make([]int64, tc.entryCount)
			resultChannels := make([]<-chan int64, tc.entryCount)

			// Batch generate test data
			for i := 0; i < tc.entryCount; i++ {
				data := make([]byte, tc.dataSize)
				rand.Read(data)
				testData[i] = data
				entryIds[i] = int64(i) // Consecutive IDs starting from 0
			}

			// Submit async write requests in batches
			batchSize := 100
			for i := 0; i < tc.entryCount; i += batchSize {
				end := min(i+batchSize, tc.entryCount)
				// Submit a batch of data
				for j := i; j < end; j++ {
					_, ch, err := segmentImpl.AppendAsync(ctx, entryIds[j], testData[j])
					if err != nil {
						segmentImpl.Close(context.TODO())
						t.Fatalf("Failed to write data: %v", err)
					}
					resultChannels[j] = ch
				}

				// Wait for this batch to complete
				for j := i; j < end; j++ {
					select {
					case result := <-resultChannels[j]:
						if result < 0 {
							segmentImpl.Close(context.TODO())
							t.Fatalf("Write did not complete successfully: %d", j)
						}
					case <-time.After(5 * time.Second):
						segmentImpl.Close(context.TODO())
						t.Fatalf("Write timeout: %d", j)
					}
				}

				t.Logf("Batch write progress: %d/%d data entries written", end, tc.entryCount)
			}

			// Ensure all data is written to disk
			if err := segmentImpl.Sync(ctx); err != nil {
				segmentImpl.Close(context.TODO())
				t.Fatalf("Failed to sync data: %v", err)
			}

			t.Logf("Preparation complete, starting read test...")

			// Step 2: Create Reader and test read performance
			roSegmentImpl, err := disk.NewRODiskSegmentImpl(context.TODO(), 1, 0, tempDir)
			reader, err := roSegmentImpl.NewReader(ctx, storage.ReaderOpt{
				StartSequenceNum: 1, // Start from the first entry
				EndSequenceNum:   0, // 0 means read to the end
			})
			if err != nil {
				roSegmentImpl.Close(context.TODO())
				t.Fatalf("Failed to create Reader: %v", err)
			}

			// Prepare variables to measure read performance
			totalBytesRead := 0
			readCount := 0
			readErrors := 0

			// If random read, shuffle the read order
			readOrder := make([]int, tc.entryCount)
			for i := 0; i < tc.entryCount; i++ {
				readOrder[i] = i
			}

			if !tc.sequential {
				rand.Shuffle(tc.entryCount, func(i, j int) {
					readOrder[i], readOrder[j] = readOrder[j], readOrder[i]
				})
			}

			// Start read test
			start := time.Now()

			if tc.sequential {
				// Sequential read
				for {
					hasNext, err := reader.HasNext(context.TODO())
					assert.NoError(t, err)
					if !hasNext {
						break
					}
					entry, err := reader.ReadNext(context.TODO())
					if err != nil {
						readErrors++
						continue
					}

					totalBytesRead += len(entry.Values)
					readCount++
				}
			} else {
				// Random read - first close the current Reader and create multiple new Readers for random position reads
				reader.Close()

				for _, idx := range readOrder {
					// Create a separate Reader for each ID
					entryId := entryIds[idx]

					singleReader, err := segmentImpl.NewReader(ctx, storage.ReaderOpt{
						StartSequenceNum: entryId,     // Start from specific ID
						EndSequenceNum:   entryId + 1, // Read only one entry
					})

					if err != nil {
						readErrors++
						continue
					}

					hasNext, err := singleReader.HasNext(context.TODO())
					assert.NoError(t, err)
					if hasNext {
						entry, err := singleReader.ReadNext(context.TODO())
						if err != nil {
							readErrors++
							singleReader.Close()
							continue
						}

						totalBytesRead += len(entry.Values)
						readCount++
					}

					singleReader.Close()
				}
			}

			duration := time.Since(start)

			// Close file
			reader.Close()
			segmentImpl.Close(context.TODO())

			// Calculate performance metrics
			throughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(readCount) / duration.Seconds()
			avgReadSize := float64(totalBytesRead) / float64(readCount) / 1024 // KB

			// Output results
			t.Logf("DiskSegmentImpl Read Test Results - %s:", tc.name)
			t.Logf("  Total data read: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  Total entries: %d", tc.entryCount)
			t.Logf("  Successfully read entries: %d", readCount)
			t.Logf("  Read errors: %d", readErrors)
			t.Logf("  Data block size: %d KB", tc.dataSize/1024)
			t.Logf("  Average actual read: %.2f KB/op", avgReadSize)
			readModeStr := "Sequential Read"
			if !tc.sequential {
				readModeStr = "Random Read"
			}
			t.Logf("  Read mode: %s", readModeStr)
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Throughput: %.2f MB/s", throughput)
			t.Logf("  Operation rate: %.2f ops/s", opsPerSecond)
			t.Logf("  Average read time: %.3f µs", duration.Seconds()*1000000/float64(readCount))
		})
	}
}

// TestDiskSegmentImplMixedPerformance tests DiskSegmentImpl performance in mixed read/write scenarios
func TestDiskSegmentImplMixedPerformance(t *testing.T) {
	// Test parameters
	testCases := []struct {
		name           string
		fragmentSize   int64 // Fragment file size
		bufferSize     int64 // Buffer size
		dataSize       int   // Size of each data entry
		totalOps       int   // Total operations
		readPercentage int   // Read operation percentage
	}{
		{"SmallFile_BalancedReadWrite", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 1000, 50},
		{"SmallFile_ReadHeavy", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 1000, 80},
		{"SmallFile_WriteHeavy", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 1000, 20},
		{"LargeFile_BalancedReadWrite", 100 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 50},
		{"LargeFile_ReadHeavy", 100 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 80},
		{"LargeFile_WriteHeavy", 100 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 20},
	}

	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("DiskSegmentImpl_mixed_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Create DiskSegmentImpl instance
			options := []disk.Option{
				disk.WithWriteFragmentSize(tc.fragmentSize),
				disk.WithWriteMaxBufferSize(tc.bufferSize),
			}

			segmentImpl, err := disk.NewDiskSegmentImpl(context.TODO(), 1, 0, tempDir, options...)
			if err != nil {
				t.Fatalf("Failed to create DiskSegmentImpl: %v", err)
			}
			defer segmentImpl.Close(context.TODO())

			// Prepare parameters
			readOps := tc.totalOps * tc.readPercentage / 100
			writeOps := tc.totalOps - readOps

			// Create operations array (0=write, 1=read)
			operations := make([]int, tc.totalOps)
			for i := 0; i < readOps; i++ {
				operations[i] = 1 // Read
			}
			for i := readOps; i < tc.totalOps; i++ {
				operations[i] = 0 // Write
			}

			// Shuffle operations order
			rand.Shuffle(tc.totalOps, func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			// Statistics variables
			syncCount := 0
			totalBytesWritten := 0
			totalBytesRead := 0
			writeTime := time.Duration(0)
			readTime := time.Duration(0)
			syncTime := time.Duration(0)
			maxEntryId := int64(-1)

			// Pre-generate write data
			writeData := make([][]byte, writeOps)
			for i := 0; i < writeOps; i++ {
				data := make([]byte, tc.dataSize)
				rand.Read(data)
				writeData[i] = data
			}

			start := time.Now()

			// Execute mixed operations
			writeOpsCount := 0
			readOpsCount := 0

			// Process write operations
			{
				var pendingWrites []int
				resultChannels := make([]<-chan int64, 0, writeOps)
				entryIds := make([]int64, 0, writeOps)

				// Find all write operation indices
				for i, op := range operations {
					if op == 0 {
						pendingWrites = append(pendingWrites, i)
					}
				}

				// Process write operations in batches, maximum 10 per batch
				for i := 0; i < len(pendingWrites); i += 10 {
					batchSize := min(10, len(pendingWrites)-i)
					batchIndices := pendingWrites[i : i+batchSize]

					// Submit writes for this batch
					for range batchIndices {
						if writeOpsCount >= writeOps {
							continue
						}

						entryId := int64(maxEntryId + 1)
						maxEntryId++

						writeStart := time.Now()
						_, ch, err := segmentImpl.AppendAsync(ctx, entryId, writeData[writeOpsCount])
						if err != nil {
							t.Fatalf("Write failed: %v", err)
						}

						resultChannels = append(resultChannels, ch)
						entryIds = append(entryIds, entryId)
						writeOpsCount++
						writeTime += time.Since(writeStart)
						totalBytesWritten += len(writeData[writeOpsCount-1])
					}

					// Wait for this batch of write operations to complete
					for j, ch := range resultChannels[len(resultChannels)-batchSize:] {
						select {
						case result := <-ch:
							if result < 0 {
								t.Fatalf("Write did not complete successfully: %d", entryIds[len(entryIds)-batchSize+j])
							}
						case <-time.After(5 * time.Second):
							t.Fatalf("Write timeout: %d", entryIds[len(entryIds)-batchSize+j])
						}
					}

					// Execute Sync after every 10 writes
					syncStart := time.Now()
					err := segmentImpl.Sync(ctx)
					syncTime += time.Since(syncStart)

					if err != nil {
						t.Fatalf("Sync failed: %v", err)
					}
					syncCount++

					// Report progress
					completedOps := writeOpsCount + readOpsCount
					if completedOps%100 == 0 || completedOps == tc.totalOps {
						t.Logf("Progress: %d/%d operations completed (Writes: %d, Reads: %d)",
							completedOps, tc.totalOps, writeOpsCount, readOpsCount)
					}
				}
			}

			// Process read operations
			{
				for _, op := range operations {
					if op == 1 {
						// Read operation - can only read entries that have been written
						if maxEntryId == 0 || readOpsCount >= readOps {
							continue // No data to read or read limit reached
						}

						// Randomly select a written entry ID
						entryId := int64(rand.Intn(int(maxEntryId))) + 1

						// Create Reader to read specific entry
						readStart := time.Now()
						roSegmentImpl, err := disk.NewRODiskSegmentImpl(context.TODO(), 1, 0, tempDir)
						reader, err := roSegmentImpl.NewReader(ctx, storage.ReaderOpt{
							StartSequenceNum: entryId,
							EndSequenceNum:   entryId + 1,
						})

						if err != nil {
							t.Fatalf("Failed to create Reader: %v", err)
						}

						hasNext, err := reader.HasNext(context.TODO())
						assert.NoError(t, err)
						if hasNext {
							entry, err := reader.ReadNext(context.TODO())
							if err == nil {
								totalBytesRead += len(entry.Values)
								readOpsCount++
							}
						}

						reader.Close()
						readTime += time.Since(readStart)

						// Report progress
						completedOps := writeOpsCount + readOpsCount
						if completedOps%100 == 0 || completedOps == tc.totalOps {
							t.Logf("Progress: %d/%d operations completed (Writes: %d, Reads: %d)",
								completedOps, tc.totalOps, writeOpsCount, readOpsCount)
						}
					}
				}
			}

			// Final sync
			finalSyncStart := time.Now()
			if err := segmentImpl.Sync(ctx); err != nil {
				t.Fatalf("Final sync failed: %v", err)
			}
			syncTime += time.Since(finalSyncStart)
			syncCount++

			duration := time.Since(start)

			// Calculate performance metrics
			writeThroughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			readThroughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			totalThroughput := float64(totalBytesWritten+totalBytesRead) / (1024 * 1024) / duration.Seconds()

			writeOpsPerSecond := float64(writeOpsCount) / duration.Seconds()
			readOpsPerSecond := float64(readOpsCount) / duration.Seconds()
			totalOpsPerSecond := float64(writeOpsCount+readOpsCount) / duration.Seconds()

			// If there are operations, calculate average times
			writeAvgTime := 0.0
			if writeOpsCount > 0 {
				writeAvgTime = writeTime.Seconds() * 1000 / float64(writeOpsCount) // milliseconds
			}
			readAvgTime := 0.0
			if readOpsCount > 0 {
				readAvgTime = readTime.Seconds() * 1000 / float64(readOpsCount) // milliseconds
			}
			syncAvgTime := 0.0
			if syncCount > 0 {
				syncAvgTime = syncTime.Seconds() * 1000 / float64(syncCount) // milliseconds
			}

			// Output results
			t.Logf("DiskSegmentImpl Mixed Test Results - %s:", tc.name)
			t.Logf("  Fragment size: %d MB", tc.fragmentSize/(1024*1024))
			t.Logf("  Buffer size: %d MB", tc.bufferSize/(1024*1024))
			t.Logf("  Data block size: %d KB", tc.dataSize/1024)
			t.Logf("  Read/Write ratio: %d%%read/%d%%write", tc.readPercentage, 100-tc.readPercentage)
			t.Logf("  Total operations: %d (Writes: %d, Reads: %d)", writeOpsCount+readOpsCount, writeOpsCount, readOpsCount)
			t.Logf("  Total data written: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  Total data read: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  Sync count: %d", syncCount)
			t.Logf("  Sync interval: every 10 writes")
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Write total time: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			t.Logf("  Read total time: %v (%.1f%%)", readTime, float64(readTime)/float64(duration)*100)
			t.Logf("  Sync total time: %v (%.1f%%)", syncTime, float64(syncTime)/float64(duration)*100)
			t.Logf("  Total throughput: %.2f MB/s", totalThroughput)
			t.Logf("  Write throughput: %.2f MB/s", writeThroughput)
			t.Logf("  Read throughput: %.2f MB/s", readThroughput)
			t.Logf("  Total operation rate: %.2f ops/s", totalOpsPerSecond)
			t.Logf("  Write operation rate: %.2f ops/s", writeOpsPerSecond)
			t.Logf("  Read operation rate: %.2f ops/s", readOpsPerSecond)
			t.Logf("  Average write time: %.3f ms", writeAvgTime)
			t.Logf("  Average read time: %.3f ms", readAvgTime)
			t.Logf("  Average sync time: %.3f ms", syncAvgTime)
		})
	}
}
