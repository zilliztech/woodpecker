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
			rootPath:    "/tmp/TestE2EWrite",
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

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_e2e_log_%s", tc.name)
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
			fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)

			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			err = client.Close()
			assert.NoError(t, err)

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
			defer client.Close()
			for {
				time.Sleep(5 * time.Second)
				fmt.Printf("sleep 5 second")
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
				fmt.Printf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}
			logHandle.GetName()

			//	### OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			if openWriterErr != nil {
				fmt.Printf("Open writer failed, err:%v\n", openWriterErr)
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
					fmt.Printf("start wait for %d entries\n", len(writingMessages))
					for idx, ch := range writingResultChan {
						writeResult := <-ch
						if writeResult.Err != nil {
							fmt.Println(writeResult.Err.Error())
							failMessages = append(failMessages, writingMessages[idx])
						} else {
							//fmt.Printf("write success, returned recordId:%v \n", writeResult.LogMessageId)
							MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(startTime).Milliseconds()))
							startTime = time.Now()
							successCount++
						}
					}
					fmt.Printf("finish wait for %d entries. success:%d , failed: %d, i: %d \n", len(writingMessages), successCount, len(failMessages), i)
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
			fmt.Printf("start close log writer \n")
			closeErr := logWriter.Close(context.Background())
			if closeErr != nil {
				fmt.Printf("close failed, err:%v\n", closeErr)
				panic(closeErr)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Printf("Test Write finished  %d entries\n", successCount)
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
				fmt.Println(err)
			}

			// ###  CreateLog if not exists
			logName := fmt.Sprintf("test_log_pro_%s", tc.name)
			client.CreateLog(context.Background(), logName)

			// ### OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			if openErr != nil {
				fmt.Printf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			if openWriterErr != nil {
				fmt.Printf("Open writer failed, err:%v\n", openWriterErr)
				panic(openWriterErr)
			}

			// gen static data
			payloadStaticData, err := generateRandomBytes(entrySize)
			assert.NoError(t, err)

			// ### Fully concurrent write with producer-consumer pattern
			var successCount atomic.Int64
			var failCount atomic.Int64
			var totalSent atomic.Int64

			// Create a semaphore to control concurrency
			semaphore := make(chan struct{}, concurrency)

			// Channel to track completion
			done := make(chan bool, writeCount)

			// Start time for throughput calculation
			startTime := time.Now()

			// Status reporting goroutine
			go func() {
				ticker := time.NewTicker(5 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						elapsed := time.Since(startTime)
						sent := totalSent.Load()
						success := successCount.Load()
						failed := failCount.Load()
						if sent > 0 {
							throughput := float64(success) / elapsed.Seconds()
							fmt.Printf("Progress: sent=%d, success=%d, failed=%d, throughput=%.2f ops/sec\n",
								sent, success, failed, throughput)
						}
					case <-done:
						return
					}
				}
			}()

			// Producer: continuously send write requests
			for i := 0; i < writeCount; i++ {
				// Acquire semaphore to control concurrency
				semaphore <- struct{}{}
				totalSent.Add(1)

				// Start async write in goroutine
				go func(index int) {
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

					// Retry loop for failed writes
					for {
						writeStart := time.Now()
						resultChan := logWriter.WriteAsync(context.Background(), msg)
						writeResult := <-resultChan

						if writeResult.Err != nil {
							failCount.Add(1)
							fmt.Printf("Write failed for index %d, retrying: %v\n", index, writeResult.Err)
							time.Sleep(100 * time.Millisecond) // Brief pause before retry
							continue
						} else {
							// Success
							successCount.Add(1)
							MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(writeStart).Milliseconds()))
							break
						}
					}
				}(i)
			}

			// Wait for all writes to complete
			fmt.Printf("Waiting for all %d writes to complete...\n", writeCount)
			for successCount.Load()+failCount.Load() < int64(writeCount) {
				time.Sleep(100 * time.Millisecond)
			}

			// Signal completion to status reporter
			close(done)

			// Calculate final statistics
			elapsed := time.Since(startTime)
			finalSuccess := successCount.Load()
			finalFail := failCount.Load()
			throughput := float64(finalSuccess) / elapsed.Seconds()
			totalBytes := finalSuccess * int64(entrySize)
			throughputMB := float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()

			fmt.Printf("\n=== Final Results ===\n")
			fmt.Printf("Total time: %v\n", elapsed)
			fmt.Printf("Total writes sent: %d\n", writeCount)
			fmt.Printf("Successful writes: %d\n", finalSuccess)
			fmt.Printf("Failed writes: %d\n", finalFail)
			fmt.Printf("Success rate: %.2f%%\n", float64(finalSuccess)/float64(writeCount)*100)
			fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
			fmt.Printf("Data throughput: %.2f MB/sec\n", throughputMB)

			// ### close and print result
			fmt.Printf("Closing log writer...\n")
			closeErr := logWriter.Close(context.Background())
			if closeErr != nil {
				fmt.Printf("close failed, err:%v\n", closeErr)
				panic(closeErr)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Printf("TestAsyncWriteThroughputPro finished with %d successful entries\n", finalSuccess)
		})
	}
}

func TestReadThroughput(t *testing.T) {
	startGopsAgentWithPort(6060)
	startMetrics()
	//startReporting()

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
				fmt.Printf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			earliest := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest, "TestReadThroughput")
			if openReaderErr != nil {
				fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop
			totalEntries := atomic.Int64{}
			totalBytes := atomic.Int64{}
			go func() {
				for {
					fmt.Printf("Result: read %d entries, %d bytes success \n", totalEntries.Load(), totalBytes.Load())
					time.Sleep(5 * time.Second)
				}
			}()

			for {
				start := time.Now()
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					fmt.Printf("read failed, err:%v\n", err)
					break
				} else {
					//fmt.Printf("read success, msg:%v\n", msg)
					cost := time.Now().Sub(start)
					MinioIOBytes.WithLabelValues("0").Observe(float64(len(msg.Payload)))
					MinioIOLatency.WithLabelValues("0").Observe(float64(cost.Milliseconds()))
				}
				totalBytes.Add(int64(len(msg.Payload)))
				totalEntries.Add(1)
				if totalEntries.Load()%100 == 0 {
					fmt.Printf(" read %d entries, %d bytes success, current msg(seg:%d,entry:%d) \n", totalEntries.Load(), totalBytes.Load(), msg.Id.SegmentId, msg.Id.EntryId)
				}
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Printf("final read %d success \n", totalEntries.Load())
			fmt.Printf("Test Read finished\n")
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
				fmt.Printf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			start := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &start, "TestReadFromEarliest")
			if openReaderErr != nil {
				fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop
			for {
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					fmt.Printf("read failed, err:%v\n", err)
					t.Error(err)
					break
				} else {
					fmt.Printf("read success, msg:%v\n", msg)
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
				fmt.Printf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			latest := log.LatestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest, "TestReadFromLatest")
			if openReaderErr != nil {
				fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			more := false
			go func() {
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					fmt.Printf("read failed, err:%v\n", err)
					t.Error(err)
				} else {
					more = true
					fmt.Printf("read success, msg:%v\n", msg)
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
				fmt.Printf("Open log failed, err:%v\n", openErr)
				panic(openErr)
			}

			//	### OpenReader
			start := &log.LogMessageId{
				SegmentId: 5,
				EntryId:   0,
			}
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start, "TestReadFromSpecifiedPosition")
			if openReaderErr != nil {
				fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// read loop
			for {
				msg, err := logReader.ReadNext(context.Background())
				if err != nil {
					fmt.Printf("read failed, err:%v\n", err)
					t.Error(err)
					break
				} else {
					fmt.Printf("read success, msg:%v\n", msg)
				}
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test Read finished")
		})
	}
}
