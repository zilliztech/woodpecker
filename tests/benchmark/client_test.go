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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestE2EWrite(t *testing.T) {
	startGopsAgent()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestE2E",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)
			cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_%s", tc.name)
			client.GetMetadataProvider().CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			assert.NoError(t, openErr)
			logHandle.GetName()

			//	### OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr)
			writeResultChan := logWriter.WriteAsync(context.Background(),
				&log.WriterMessage{
					Payload: []byte("hello world 1"),
				},
			)
			writeResult := <-writeResultChan
			assert.NoError(t, writeResult.Err)
			t.Logf("write success, returned recordId:%v\n", writeResult.LogMessageId)

			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			err = client.Close(context.TODO())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestE2ERead(t *testing.T) {
	startGopsAgent()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestE2E",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			//cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_%s", tc.name)
			client.CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			earliest := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest, "TestReadThroughput")
			if openReaderErr != nil {
				t.Logf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop with timeout mechanism
			totalEntries := atomic.Int64{}
			consecutiveTimeouts := 0
			maxConsecutiveTimeouts := 5
			readTimeout := 5 * time.Second

			for {
				// Create context with timeout for ReadNext
				ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
				msg, readErr := logReader.ReadNext(ctx)
				cancel() // Always cancel context to avoid resource leak
				if readErr != nil {
					consecutiveTimeouts++
					t.Logf("read failed (timeout %d/%d), err:%v\n", consecutiveTimeouts, maxConsecutiveTimeouts, err)
					// Check if we've exceeded the maximum consecutive timeouts
					if consecutiveTimeouts >= maxConsecutiveTimeouts {
						t.Logf("Reached maximum consecutive timeouts (%d), stopping read loop\n", maxConsecutiveTimeouts)
						break
					}
					continue
				} else {
					// Reset timeout counter on successful read
					consecutiveTimeouts = 0
					// Update statistics
					totalEntries.Add(1)
					t.Logf("read success, recordId:%v\n", msg.Id)
				}
			}

			finalEntries := totalEntries.Load()
			t.Logf("Total entries read: %d\n", finalEntries)
			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestEmptyRuntime(t *testing.T) {
	startGopsAgentWithPort(6060)
	startMetrics()
	startReporting()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// ### Create client
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}
			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}
			defer client.Close(context.TODO())
			for {
				time.Sleep(5 * time.Second)
				t.Logf("sleep 5 second")
			}
		})
	}
}

// Simulate throughput performance using single-threaded asynchronous writes
func TestAsyncWriteThroughput(t *testing.T) {
	startGopsAgentWithPort(6060)
	startMetrics()
	startReporting()
	entrySize := 1_000_000 // 1MB per row
	batchCount := 1_000    // wait for batch entries to finish
	writeCount := 10_000   // total rows to write

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// ### Create client
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			//cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_%s", tc.name)
			client.CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}
			logHandle.GetName()

			//	### OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			if openWriterErr != nil {
				t.Logf("Open writer failed, err:%v\n", openWriterErr)
				panic(openWriterErr)
			}

			// gen static data
			payloadStaticData, err := generateRandomBytes(entrySize) // 1KB per row
			assert.NoError(t, err)

			// ### Write
			successCount := 0
			writingResultChan := make([]<-chan *log.WriteResult, 0) // 10M*1k=10GB
			writingMessages := make([]*log.WriterMessage, 0)
			failMessages := make([]*log.WriterMessage, 0)
			startTime := time.Now()
			for i := 0; i < writeCount; i++ {
				// append async
				msg := &log.WriterMessage{
					Payload: payloadStaticData,
					Properties: map[string]string{
						"key": fmt.Sprintf("value%d", i),
					},
				}
				resultChan := logWriter.WriteAsync(context.Background(), msg)
				writingResultChan = append(writingResultChan, resultChan)
				writingMessages = append(writingMessages, msg)

				// wait for batch finish
				if len(writingMessages)%batchCount == 0 { // wait 64000 entries or 64MB to completed
					t.Logf("start wait for %d entries\n", len(writingMessages))
					for idx, ch := range writingResultChan {
						writeResult := <-ch
						if writeResult.Err != nil {
							fmt.Println(writeResult.Err.Error())
							failMessages = append(failMessages, writingMessages[idx])
						} else {
							//t.Logf("write success, returned recordId:%v \n", writeResult.LogMessageId)
							MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(startTime).Milliseconds()))
							startTime = time.Now()
							successCount++
						}
					}
					t.Logf("finish wait for %d entries. success:%d , failed: %d, i: %d \n", len(writingMessages), successCount, len(failMessages), i)
					time.Sleep(1 * time.Second) // wait a moment to avoid too much retry
					writingResultChan = make([]<-chan *log.WriteResult, 0)
					writingMessages = make([]*log.WriterMessage, 0)
					for _, m := range failMessages {
						retryCh := logWriter.WriteAsync(context.Background(), m)
						writingResultChan = append(writingResultChan, retryCh)
						writingMessages = append(writingMessages, m)
					}
					failMessages = make([]*log.WriterMessage, 0)
				}
			}

			// wait&retry the rest
			{
				for idx, ch := range writingResultChan {
					writeResult := <-ch
					if writeResult.Err != nil {
						fmt.Println(writeResult.Err.Error())
						failMessages = append(failMessages, writingMessages[idx])
					} else {
						successCount++
					}
				}
				time.Sleep(1 * time.Second) // wait a moment to avoid too much retry
				for {
					if len(failMessages) == 0 {
						break
					}
					writingResultChan = make([]<-chan *log.WriteResult, 0)
					writingMessages = make([]*log.WriterMessage, 0)
					for _, m := range failMessages {
						retryCh := logWriter.WriteAsync(context.Background(), m)
						writingResultChan = append(writingResultChan, retryCh)
						writingMessages = append(writingMessages, m)
					}
					failMessages = make([]*log.WriterMessage, 0)
					for idx, ch := range writingResultChan {
						writeResult := <-ch
						if writeResult.Err != nil {
							fmt.Println(writeResult.Err.Error())
							failMessages = append(failMessages, writingMessages[idx])
						} else {
							MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(startTime).Milliseconds()))
							startTime = time.Now()
							successCount++
						}
					}
				}
			}

			// ### close and print result
			t.Logf("start close log writer \n")
			closeErr := logWriter.Close(context.Background())
			if closeErr != nil {
				t.Logf("close failed, err:%v\n", closeErr)
				panic(closeErr)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			t.Logf("Test Write finished  %d entries\n", successCount)
		})
	}
}

// Simulate throughput performance using multithreaded synchronous writes
func TestWriteThroughput(t *testing.T) {
	startGopsAgentWithPort(6060)
	startMetrics()
	startReporting()
	entrySize := 1_000_000 // 1MB per row
	concurrency := 1_000   // concurrent goroutines
	writeCount := 10_000   // total rows to write

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// ### Create client
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			//cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				t.Logf("%v", err)
			}

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_%s", tc.name)
			client.CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			if openWriterErr != nil {
				t.Logf("Open writer failed, err:%v\n", openWriterErr)
				panic(openWriterErr)
			}

			// gen static data
			payloadStaticData, err := generateRandomBytes(entrySize)
			assert.NoError(t, err)

			// ### Concurrent write with controlled concurrency
			var successCount atomic.Int64
			var failCount atomic.Int64
			var totalAttempts atomic.Int64

			// Semaphore to control concurrency
			semaphore := make(chan struct{}, concurrency)

			// WaitGroup to wait for all writeCount tasks to complete
			var wg sync.WaitGroup
			wg.Add(writeCount)

			// Start time for throughput calculation
			startTime := time.Now()

			// Start exactly writeCount tasks, but limit concurrency
			for i := 0; i < writeCount; i++ {
				// Acquire semaphore to control concurrency
				semaphore <- struct{}{}
				msgIndex := i
				go func(index int) {
					defer wg.Done()
					defer func() {
						// Release semaphore
						<-semaphore
					}()

					msg := &log.WriterMessage{
						Payload: payloadStaticData,
						Properties: map[string]string{
							"key": fmt.Sprintf("value%d", index),
						},
					}

					// Retry loop until this message is successfully written
					for {
						totalAttempts.Add(1)
						writeStart := time.Now()
						resultChan := logWriter.WriteAsync(context.Background(), msg)
						writeResult := <-resultChan

						if writeResult.Err != nil {
							failCount.Add(1)
							t.Logf("Write failed for index %d, retrying: %v\n", index, writeResult.Err)
							time.Sleep(100 * time.Millisecond) // Brief pause before retry
							continue
						} else {
							// Success
							currentSuccess := successCount.Add(1)
							MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(writeStart).Milliseconds()))

							// Log progress every 100 successful writes
							if currentSuccess%100 == 0 {
								t.Logf("Completed %d/%d successful writes", currentSuccess, writeCount)
							}
							break
						}
					}
				}(msgIndex)
			}

			// Wait for all writeCount successful writes to complete
			t.Logf("Waiting for exactly %d successful writes to complete...\n", writeCount)
			wg.Wait()

			// Calculate final statistics
			elapsed := time.Since(startTime)
			finalSuccess := successCount.Load()
			finalFail := failCount.Load()
			totalAttemptsMade := totalAttempts.Load()
			throughput := float64(finalSuccess) / elapsed.Seconds()
			totalBytes := finalSuccess * int64(entrySize)
			throughputMB := float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()

			t.Logf("\n=== Final Results ===\n")
			t.Logf("Total time: %v\n", elapsed)
			t.Logf("Target successful writes: %d\n", writeCount)
			t.Logf("Actual successful writes: %d\n", finalSuccess)
			t.Logf("Total write attempts: %d\n", totalAttemptsMade)
			t.Logf("Failed attempts: %d\n", finalFail)
			t.Logf("Success rate: %.2f%% (successful/attempts)\n", float64(finalSuccess)/float64(totalAttemptsMade)*100)
			t.Logf("Throughput: %.2f ops/sec\n", throughput)
			t.Logf("Data throughput: %.2f MB/sec\n", throughputMB)

			// ### close and print result
			t.Logf("Closing log writer...\n")
			closeErr := logWriter.Close(context.Background())
			if closeErr != nil {
				t.Logf("close failed, err:%v\n", closeErr)
				panic(closeErr)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			t.Logf("TestWriteThroughput finished with exactly %d successful entries (target: %d)\n", finalSuccess, writeCount)
		})
	}
}

func TestReadThroughput(t *testing.T) {
	startGopsAgentWithPort(6060)
	startMetrics()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			//cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_%s", tc.name)
			client.CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			earliest := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest, "TestReadThroughput")
			if openReaderErr != nil {
				t.Logf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop with timeout mechanism
			totalEntries := atomic.Int64{}
			totalBytes := atomic.Int64{}
			var totalReadTime atomic.Int64 // in milliseconds
			var minReadTime atomic.Int64   // in milliseconds
			var maxReadTime atomic.Int64   // in milliseconds

			readStartTime := time.Now()
			consecutiveTimeouts := 0
			maxConsecutiveTimeouts := 5
			readTimeout := 5 * time.Second

			for {
				// Create context with timeout for ReadNext
				ctx, cancel := context.WithTimeout(context.Background(), readTimeout)

				readStart := time.Now()
				msg, err := logReader.ReadNext(ctx)
				readDuration := time.Since(readStart)

				cancel() // Always cancel context to avoid resource leak

				if err != nil {
					consecutiveTimeouts++
					t.Logf("read failed (timeout %d/%d), err:%v\n", consecutiveTimeouts, maxConsecutiveTimeouts, err)

					// Check if we've exceeded the maximum consecutive timeouts
					if consecutiveTimeouts >= maxConsecutiveTimeouts {
						t.Logf("Reached maximum consecutive timeouts (%d), stopping read loop\n", maxConsecutiveTimeouts)
						break
					}
					continue
				} else {
					// Reset timeout counter on successful read
					consecutiveTimeouts = 0

					// Update statistics
					readTimeMs := readDuration.Milliseconds()
					totalReadTime.Add(readTimeMs)

					// Update min/max read times
					for {
						currentMin := minReadTime.Load()
						if currentMin == 0 || readTimeMs < currentMin {
							if minReadTime.CompareAndSwap(currentMin, readTimeMs) {
								break
							}
						} else {
							break
						}
					}

					for {
						currentMax := maxReadTime.Load()
						if readTimeMs > currentMax {
							if maxReadTime.CompareAndSwap(currentMax, readTimeMs) {
								break
							}
						} else {
							break
						}
					}

					totalBytes.Add(int64(len(msg.Payload)))
					totalEntries.Add(1)

					if totalEntries.Load()%100 == 0 {
						t.Logf("Read %d entries, %d bytes, current msg(seg:%d,entry:%d)\n",
							totalEntries.Load(), totalBytes.Load(), msg.Id.SegmentId, msg.Id.EntryId)
					}
				}
			}

			// Calculate and print final statistics
			totalElapsed := time.Since(readStartTime)
			finalEntries := totalEntries.Load()
			finalBytes := totalBytes.Load()

			t.Logf("\n=== Read Performance Results ===\n")
			t.Logf("Total time: %v\n", totalElapsed)
			t.Logf("Total entries read: %d\n", finalEntries)
			t.Logf("Total bytes read: %d (%.2f MB)\n", finalBytes, float64(finalBytes)/(1024*1024))

			if finalEntries > 0 {
				throughput := float64(finalEntries) / totalElapsed.Seconds()
				dataThroughput := float64(finalBytes) / (1024 * 1024) / totalElapsed.Seconds()
				avgReadTime := float64(totalReadTime.Load()) / float64(finalEntries)

				t.Logf("Throughput: %.2f ops/sec\n", throughput)
				t.Logf("Data throughput: %.2f MB/sec\n", dataThroughput)
				t.Logf("Average read time: %.2f ms\n", avgReadTime)
				t.Logf("Min read time: %d ms\n", minReadTime.Load())
				t.Logf("Max read time: %d ms\n", maxReadTime.Load())
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			t.Logf("TestReadThroughput finished - read %d entries successfully\n", finalEntries)
		})
	}
}

func TestReadFromEarliest(t *testing.T) {
	startGopsAgent()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_%s", tc.name)
			client.CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			start := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &start, "TestReadFromEarliest")
			if openReaderErr != nil {
				t.Logf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop
			for {
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					t.Logf("read failed, err:%v\n", err)
					t.Error(err)
					break
				} else {
					t.Logf("read success, msg:%v\n", msg)
				}
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test Read finished")
		})
	}
}

func TestReadFromLatest(t *testing.T) {
	startGopsAgent()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// ### OpenLog
			logName := fmt.Sprintf("test_log_%s", tc.name)
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			latest := log.LatestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest, "TestReadFromLatest")
			if openReaderErr != nil {
				t.Logf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			more := false
			go func() {
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					t.Logf("read failed, err:%v\n", err)
					t.Error(err)
				} else {
					more = true
					t.Logf("read success, msg:%v\n", msg)
				}
			}()
			time.Sleep(time.Second * 2)
			assert.False(t, more, "should read nothing and timeout")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestReadFromSpecifiedPosition(t *testing.T) {
	startGopsAgent()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// ### OpenLog
			logName := fmt.Sprintf("test_log_%s", tc.name)
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				t.Logf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			start := &log.LogMessageId{
				SegmentId: 5,
				EntryId:   0,
			}
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start, "TestReadFromSpecifiedPosition")
			if openReaderErr != nil {
				t.Logf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop
			for {
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					t.Logf("read failed, err:%v\n", err)
					t.Error(err)
					break
				} else {
					t.Logf("read success, msg:%v\n", msg)
				}
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test Read finished")
		})
	}
}
