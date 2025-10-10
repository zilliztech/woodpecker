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
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// WriterManager handles concurrent access to log writer and automatic reopening on lock lost
type WriterManager struct {
	logName       string
	client        woodpecker.Client
	logHandle     log.LogHandle
	currentWriter log.LogWriter

	mu          sync.Mutex // Mutex for writer reopening
	reopenCount atomic.Int64
}

// NewWriterManager creates a new writer manager with initial writer
func NewWriterManager(logName string, client woodpecker.Client, logHandle log.LogHandle, initialWriter log.LogWriter) *WriterManager {
	return &WriterManager{
		logName:       logName,
		client:        client,
		logHandle:     logHandle,
		currentWriter: initialWriter,
	}
}

// GetCurrentWriter returns the current writer in a thread-safe manner
func (wm *WriterManager) GetCurrentWriter() log.LogWriter {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.currentWriter
}

// TriggerReopen triggers a writer reopen if the current writer matches the failed writer
// This should be called when ErrLogWriterLockLost is encountered
func (wm *WriterManager) TriggerReopen(failedWriter log.LogWriter, index int64) log.LogWriter {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Check if the failed writer is still the current writer
	if failedWriter != wm.currentWriter {
		// Another goroutine already reopened the writer
		logger.Ctx(context.TODO()).Info("writer already reopened by another goroutine", zap.Int64("idx", index), zap.String("inst", fmt.Sprintf("%p", wm.currentWriter)))
		return wm.currentWriter
	}

	// Close the old writer
	if wm.currentWriter != nil {
		logger.Ctx(context.TODO()).Info("closing current writer", zap.Int64("idx", index), zap.String("inst", fmt.Sprintf("%p", wm.currentWriter)))
		closeErr := wm.currentWriter.Close(context.Background())
		if closeErr != nil {
			logger.Ctx(context.TODO()).Warn("close log writer failed", zap.Int64("idx", index), zap.String("inst", fmt.Sprintf("%p", wm.currentWriter)), zap.Error(closeErr))
		}
		logger.Ctx(context.TODO()).Info("closed current writer", zap.Int64("idx", index), zap.String("inst", fmt.Sprintf("%p", wm.currentWriter)))
	}

	// Infinite retry loop to reopen writer
	for {
		// 1. open logHandle
		newLogHandle, err := wm.client.OpenLog(context.TODO(), wm.logName)
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			logger.Ctx(context.TODO()).Info("open log fail and wait 1s", zap.Int64("idx", index), zap.Error(err))
			continue
		}

		// 2. open new writer
		//newWriter, err := wm.logHandle.OpenLogWriter(context.Background())
		newWriter, err := newLogHandle.OpenInternalLogWriter(context.Background())
		if err != nil {
			// Log error and retry after a brief pause
			time.Sleep(1000 * time.Millisecond)
			logger.Ctx(context.TODO()).Info("open writer fail and wait 1s", zap.Int64("idx", index), zap.Error(err))
			newLogHandle.Close(context.TODO())
			continue
		}

		// Successfully opened new writer
		wm.currentWriter = newWriter
		wm.logHandle = newLogHandle
		wm.reopenCount.Add(1)
		break
	}

	logger.Ctx(context.TODO()).Info("open success, return new writer", zap.Int64("idx", index), zap.String("inst", fmt.Sprintf("%p", wm.currentWriter)))
	return wm.currentWriter
}

// GetReopenCount returns the number of times the writer has been reopened
func (wm *WriterManager) GetReopenCount() int64 {
	return wm.reopenCount.Load()
}

// Close closes the current writer
func (wm *WriterManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.currentWriter != nil {
		return wm.currentWriter.Close(context.Background())
	}
	return nil
}

func TestE2EWrite(t *testing.T) {
	utils.StartGopsAgent()

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
				&log.WriteMessage{
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
	utils.StartGopsAgent()

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
	utils.StartGopsAgentWithPort(6060)
	utils.StartMetrics()
	utils.StartReporting()

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
	utils.StartGopsAgentWithPort(6060)
	utils.StartMetrics()
	utils.StartReporting()
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
			payloadStaticData, err := utils.GenerateRandomBytes(entrySize) // 1KB per row
			assert.NoError(t, err)

			// ### Write
			successCount := 0
			writingResultChan := make([]<-chan *log.WriteResult, 0) // 10M*1k=10GB
			writingMessages := make([]*log.WriteMessage, 0)
			failMessages := make([]*log.WriteMessage, 0)
			startTime := time.Now()
			for i := 0; i < writeCount; i++ {
				// append async
				msg := &log.WriteMessage{
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
							utils.MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							utils.MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(startTime).Milliseconds()))
							startTime = time.Now()
							successCount++
						}
					}
					t.Logf("finish wait for %d entries. success:%d , failed: %d, i: %d \n", len(writingMessages), successCount, len(failMessages), i)
					time.Sleep(1 * time.Second) // wait a moment to avoid too much retry
					writingResultChan = make([]<-chan *log.WriteResult, 0)
					writingMessages = make([]*log.WriteMessage, 0)
					for _, m := range failMessages {
						retryCh := logWriter.WriteAsync(context.Background(), m)
						writingResultChan = append(writingResultChan, retryCh)
						writingMessages = append(writingMessages, m)
					}
					failMessages = make([]*log.WriteMessage, 0)
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
					writingMessages = make([]*log.WriteMessage, 0)
					for _, m := range failMessages {
						retryCh := logWriter.WriteAsync(context.Background(), m)
						writingResultChan = append(writingResultChan, retryCh)
						writingMessages = append(writingMessages, m)
					}
					failMessages = make([]*log.WriteMessage, 0)
					for idx, ch := range writingResultChan {
						writeResult := <-ch
						if writeResult.Err != nil {
							fmt.Println(writeResult.Err.Error())
							failMessages = append(failMessages, writingMessages[idx])
						} else {
							utils.MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							utils.MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(startTime).Milliseconds()))
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
	utils.StartGopsAgentWithPort(6060)
	utils.StartMetrics()
	utils.StartReporting()
	entrySize := 1_000_000 // 1MB per row
	concurrency := 1_000   // concurrent goroutines
	writeCount := 10_000   // total rows to write

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    "/tmp/TestWriteReadPerf_service",
			needCluster: true,
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

			var client woodpecker.Client

			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, tc.rootPath)
				cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(context.TODO(), cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(context.TODO())
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(context.TODO(), cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

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
			//logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			logWriter, openWriterErr := logHandle.OpenInternalLogWriter(context.Background())
			if openWriterErr != nil {
				t.Logf("Open writer failed, err:%v\n", openWriterErr)
				panic(openWriterErr)
			}
			// ### Create WriterManager for handling ErrLogWriterLockLost
			writerMgr := NewWriterManager(logName, client, logHandle, logWriter)

			// gen static data
			payloadStaticData, err := utils.GenerateRandomBytes(entrySize)
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

					msg := &log.WriteMessage{
						Payload: payloadStaticData,
						Properties: map[string]string{
							"key": fmt.Sprintf("value%d", index),
						},
					}

					// Retry loop until this message is successfully written
					for {
						// Always get the current writer (thread-safe)
						currentWriter := writerMgr.GetCurrentWriter()

						totalAttempts.Add(1)
						writeStart := time.Now()
						writeResult := currentWriter.Write(context.Background(), msg)

						if writeResult.Err != nil {
							failCount.Add(1)
							t.Logf("Write failed for index %d, retrying: %v", index, writeResult.Err)

							// If it's a lock lost error, trigger reopen
							if werr.ErrLogWriterLockLost.Is(writeResult.Err) {
								newWriter := writerMgr.TriggerReopen(currentWriter, int64(index))
								t.Logf("Writer reopened for index %d, new writer: %p", index, newWriter)
							}

							time.Sleep(100 * time.Millisecond) // Brief pause before retry
							continue
						} else {
							// Success
							currentSuccess := successCount.Add(1)
							utils.MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
							utils.MinioIOLatency.WithLabelValues("0").Observe(float64(time.Since(writeStart).Milliseconds()))

							// Log progress every 100 successful writes
							if currentSuccess%100 == 0 {
								t.Logf("Completed %d/%d successful writes, current index: %d", currentSuccess, writeCount, index)
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
			reopenCount := writerMgr.GetReopenCount()
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
			t.Logf("Writer reopen count: %d\n", reopenCount)
			t.Logf("Success rate: %.2f%% (successful/attempts)\n", float64(finalSuccess)/float64(totalAttemptsMade)*100)
			t.Logf("Throughput: %.2f ops/sec\n", throughput)
			t.Logf("Data throughput: %.2f MB/sec\n", throughputMB)

			// ### close and print result
			t.Logf("Closing log writer...\n")
			closeErr := writerMgr.Close()
			if closeErr != nil {
				t.Logf("close failed, err:%v\n", closeErr)
				panic(closeErr)
			}

			t.Logf("TestWriteThroughput finished with exactly %d successful entries (target: %d)\n", finalSuccess, writeCount)
		})
	}
}

func TestReadThroughput(t *testing.T) {
	utils.StartGopsAgentWithPort(6060)
	utils.StartMetrics()

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestWriteReadPerf",
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    "/tmp/TestWriteReadPerf_service",
			needCluster: true,
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

			var client woodpecker.Client

			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, tc.rootPath)
				cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(context.TODO(), cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(context.TODO())
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(context.TODO(), cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

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
			lastDataReadSuccessTime := time.Now()
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
					lastDataReadSuccessTime = time.Now()

					if totalEntries.Load()%100 == 0 {
						t.Logf("Read %d entries, %d bytes, current msg(seg:%d,entry:%d, properties:%v)\n",
							totalEntries.Load(), totalBytes.Load(), msg.Id.SegmentId, msg.Id.EntryId, msg.Properties)
					}
				}
			}

			// Calculate and print final statistics
			totalElapsed := lastDataReadSuccessTime.Sub(readStartTime) // read data elapsed time
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
	utils.StartGopsAgent()

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
	utils.StartGopsAgent()

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
	utils.StartGopsAgent()

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

// TestSequentialWriteAndReadPerformance tests the performance of sequential writes followed by sequential reads
// This creates many small files by writing one record at a time synchronously, then measures read performance
func TestSequentialWriteAndReadPerformance(t *testing.T) {
	utils.StartGopsAgentWithPort(6060)
	utils.StartMetrics()
	utils.StartReporting()

	entrySize := 100_000 // 100KB per record to create reasonably sized files
	writeCount := 1_000  // Total records to write sequentially

	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestSequentialWriteRead",
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
				t.Logf("Failed to create client: %v", err)
				return
			}

			// ###  CreateLog if not exists
			logName := "test_sequential_log_" + t.Name() + "_" + time.Now().Format("20060102150405")
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

			// Generate static data for consistency
			payloadStaticData, err := utils.GenerateRandomBytes(entrySize)
			assert.NoError(t, err)

			// === PHASE 1: Sequential Synchronous Writes ===
			t.Logf("\n=== Starting Sequential Write Phase ===\n")
			t.Logf("Writing %d records of %d bytes each, one at a time...\n", writeCount, entrySize)

			var writeStats struct {
				totalWriteTime time.Duration
				minWriteTime   time.Duration
				maxWriteTime   time.Duration
				successCount   int
				totalAttempts  int
				writeTimes     []time.Duration // Store individual write times for analysis
			}
			writeStats.minWriteTime = time.Hour // Initialize to a large value

			writeStartTime := time.Now()

			for i := 0; i < writeCount; i++ {
				msg := &log.WriteMessage{
					Payload: payloadStaticData,
					Properties: map[string]string{
						"sequence": fmt.Sprintf("%d", i),
						"size":     fmt.Sprintf("%d", entrySize),
					},
				}

				// Retry loop for this single message until success
				for {
					writeStats.totalAttempts++
					singleWriteStart := time.Now()

					resultChan := logWriter.WriteAsync(context.Background(), msg)
					writeResult := <-resultChan

					singleWriteDuration := time.Since(singleWriteStart)

					if writeResult.Err != nil {
						t.Logf("Write failed for record %d (attempt %d), retrying: %v\n",
							i, writeStats.totalAttempts, writeResult.Err)
						time.Sleep(100 * time.Millisecond) // Brief pause before retry
						continue
					} else {
						// Success - record timing statistics
						writeStats.successCount++
						writeStats.totalWriteTime += singleWriteDuration
						writeStats.writeTimes = append(writeStats.writeTimes, singleWriteDuration)

						if singleWriteDuration < writeStats.minWriteTime {
							writeStats.minWriteTime = singleWriteDuration
						}
						if singleWriteDuration > writeStats.maxWriteTime {
							writeStats.maxWriteTime = singleWriteDuration
						}

						// Log progress every 100 writes
						if (i+1)%100 == 0 {
							t.Logf("Completed %d/%d writes, avg time: %.2f ms\n",
								i+1, writeCount, float64(writeStats.totalWriteTime.Milliseconds())/float64(writeStats.successCount))
						}
						break
					}
				}
			}

			totalWriteElapsed := time.Since(writeStartTime)

			// Close writer to ensure all data is flushed
			t.Logf("Closing writer to flush all data...\n")
			closeErr := logWriter.Close(context.Background())
			if closeErr != nil {
				t.Logf("Close writer failed, err:%v\n", closeErr)
				panic(closeErr)
			}

			// === Write Statistics ===
			avgWriteTime := writeStats.totalWriteTime / time.Duration(writeStats.successCount)
			writeOpsPerSec := float64(writeStats.successCount) / totalWriteElapsed.Seconds()
			writeMBPerSec := float64(writeStats.successCount*entrySize) / (1024 * 1024) / totalWriteElapsed.Seconds()

			t.Logf("\n=== Write Phase Results ===\n")
			t.Logf("Total write time: %v\n", totalWriteElapsed)
			t.Logf("Successfully written: %d/%d records\n", writeStats.successCount, writeCount)
			t.Logf("Total write attempts: %d\n", writeStats.totalAttempts)
			t.Logf("Success rate: %.2f%%\n", float64(writeStats.successCount)/float64(writeStats.totalAttempts)*100)
			t.Logf("Average write time: %v\n", avgWriteTime)
			t.Logf("Min write time: %v\n", writeStats.minWriteTime)
			t.Logf("Max write time: %v\n", writeStats.maxWriteTime)
			t.Logf("Write throughput: %.2f ops/sec\n", writeOpsPerSec)
			t.Logf("Write data throughput: %.2f MB/sec\n", writeMBPerSec)

			// Sleep a bit to ensure all data is properly stored
			t.Logf("Waiting 2 seconds for data to be fully committed...\n")
			time.Sleep(2 * time.Second)

			// === PHASE 2: Sequential Reads with Detailed Performance Analysis ===
			t.Logf("\n=== Starting Sequential Read Phase ===\n")

			//	### OpenReader
			earliest := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest, "TestSequentialWriteRead")
			if openReaderErr != nil {
				t.Logf("Open reader failed, err:%v\n", openReaderErr)
				panic(openReaderErr)
			}

			// Read statistics
			var readStats struct {
				totalEntries    int64
				totalBytes      int64
				totalReadTime   time.Duration
				minReadTime     time.Duration
				maxReadTime     time.Duration
				readTimes       []time.Duration // Store individual read times for analysis
				successfulReads int
				failedReads     int
			}
			readStats.minReadTime = time.Hour // Initialize to a large value

			readStartTime := time.Now()
			lastSuccessTime := readStartTime
			consecutiveTimeouts := 0
			maxConsecutiveTimeouts := 5
			readTimeout := 10 * time.Second // Longer timeout for more stable measurement

			t.Logf("Starting to read back %d records...\n", writeStats.successCount)

			for {
				// Create context with timeout for ReadNext
				ctx, cancel := context.WithTimeout(context.Background(), readTimeout)

				singleReadStart := time.Now()
				msg, err := logReader.ReadNext(ctx)
				singleReadDuration := time.Since(singleReadStart)

				cancel() // Always cancel context to avoid resource leak

				if err != nil {
					readStats.failedReads++
					consecutiveTimeouts++
					t.Logf("Read failed (timeout %d/%d), err:%v\n", consecutiveTimeouts, maxConsecutiveTimeouts, err)

					// Check if we've exceeded the maximum consecutive timeouts
					if consecutiveTimeouts >= maxConsecutiveTimeouts {
						t.Logf("Reached maximum consecutive timeouts (%d), stopping read loop\n", maxConsecutiveTimeouts)
						break
					}
					continue
				} else {
					// Reset timeout counter on successful read
					consecutiveTimeouts = 0
					readStats.successfulReads++
					lastSuccessTime = time.Now()

					// Update statistics
					readStats.totalReadTime += singleReadDuration
					readStats.readTimes = append(readStats.readTimes, singleReadDuration)
					readStats.totalBytes += int64(len(msg.Payload))
					readStats.totalEntries++

					if singleReadDuration < readStats.minReadTime {
						readStats.minReadTime = singleReadDuration
					}
					if singleReadDuration > readStats.maxReadTime {
						readStats.maxReadTime = singleReadDuration
					}

					// Log progress every 100 reads
					if readStats.totalEntries%100 == 0 {
						avgReadTime := readStats.totalReadTime / time.Duration(readStats.successfulReads)
						t.Logf("Read %d entries, avg read time: %v, current msg(seg:%d,entry:%d)\n",
							readStats.totalEntries, avgReadTime, msg.Id.SegmentId, msg.Id.EntryId)
					}
				}
			}

			totalReadElapsed := lastSuccessTime.Sub(readStartTime)

			// === Read Statistics ===
			if readStats.successfulReads > 0 {
				avgReadTime := readStats.totalReadTime / time.Duration(readStats.successfulReads)
				readOpsPerSec := float64(readStats.totalEntries) / totalReadElapsed.Seconds()
				readMBPerSec := float64(readStats.totalBytes) / (1024 * 1024) / totalReadElapsed.Seconds()

				t.Logf("\n=== Read Phase Results ===\n")
				t.Logf("Total read time: %v\n", totalReadElapsed)
				t.Logf("Records read: %d\n", readStats.totalEntries)
				t.Logf("Successful reads: %d\n", readStats.successfulReads)
				t.Logf("Failed reads: %d\n", readStats.failedReads)
				t.Logf("Total bytes read: %d (%.2f MB)\n", readStats.totalBytes, float64(readStats.totalBytes)/(1024*1024))
				t.Logf("Average read time: %v\n", avgReadTime)
				t.Logf("Min read time: %v\n", readStats.minReadTime)
				t.Logf("Max read time: %v\n", readStats.maxReadTime)
				t.Logf("Read throughput: %.2f ops/sec\n", readOpsPerSec)
				t.Logf("Read data throughput: %.2f MB/sec\n", readMBPerSec)

				// Additional analysis: Check read time stability
				if len(readStats.readTimes) > 10 {
					// Calculate standard deviation
					sum := int64(0)
					for _, d := range readStats.readTimes {
						sum += d.Microseconds()
					}
					mean := sum / int64(len(readStats.readTimes))

					variance := int64(0)
					for _, d := range readStats.readTimes {
						diff := d.Microseconds() - mean
						variance += diff * diff
					}
					variance /= int64(len(readStats.readTimes))
					stdDev := time.Duration(int64(float64(variance)*0.5)) * time.Microsecond

					t.Logf("\n=== Read Time Stability Analysis ===\n")
					t.Logf("Mean read time: %v\n", time.Duration(mean)*time.Microsecond)
					t.Logf("Standard deviation: %v\n", stdDev)
					t.Logf("Coefficient of variation: %.2f%%\n", float64(stdDev)/float64(mean)*100)
				}

				// === Overall Performance Comparison ===
				t.Logf("\n=== Overall Performance Summary ===\n")
				t.Logf("Write vs Read throughput ratio: %.2fx\n", writeOpsPerSec/readOpsPerSec)
				t.Logf("Average write time vs read time ratio: %.2fx\n",
					float64(avgWriteTime)/float64(avgReadTime))
				t.Logf("Data verification: Written %d records, Read %d records\n",
					writeStats.successCount, readStats.totalEntries)
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			t.Logf("\nTestSequentialWriteAndReadPerformance completed successfully!\n")
		})
	}
}
