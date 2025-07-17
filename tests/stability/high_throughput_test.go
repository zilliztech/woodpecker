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

package integration

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/gops/agent"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func startGopsAgentWithPort(port int) {
	// start gops agent
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(err)
	}
	go func() {
		fmt.Printf("Starting gops agent on localhost:%d\n", port)
		http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	}()
}

// Simplified high throughput test - only tests basic writer and tail reader collaboration
func TestSimpleHighThroughputWriteAndRead(t *testing.T) {
	startGopsAgentWithPort(6060)

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestSimpleHighThroughput")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			defer client.Close(context.TODO())

			// Test parameters - minimal parameters
			// 4*500*1MB = 2GB
			const concurrentThreads = 4   // 4 concurrent write thread
			const messageSize = 1000000   // 1MB per message
			const messagesPerThread = 500 // Each thread writes 500 messages

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_simple_throughput_" + tc.name + "_" + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)
			t.Logf("Test parameters: %d write threads, %d messages per thread, %d bytes per message",
				concurrentThreads, messagesPerThread, messageSize)

			// Communication channels
			writerDone := make(chan struct{}, 1)
			readerDone := make(chan int, 1)
			errorChan := make(chan error, 20)

			var wg sync.WaitGroup

			// 1. Start tail reader
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(readerDone)

				t.Log("Tail Reader: Starting...")

				startPoint := &log.LogMessageId{SegmentId: 0, EntryId: 0}
				reader, err := logHandle.OpenLogReader(ctx, startPoint, "simple-tail-reader")
				if err != nil {
					errorChan <- fmt.Errorf("tail reader: failed to open: %v", err)
					readerDone <- 0
					return
				}
				defer reader.Close(ctx)

				messageCount := 0
				readTimeout := 10 * time.Second
				lastReadTime := time.Now()

				for {
					// Check if writer is done and read timeout
					select {
					case <-writerDone:
						// Writer is done, continue reading for a while to ensure all data is read
						if time.Since(lastReadTime) > 3*time.Second {
							t.Logf("Tail Reader: Writer done and no recent reads, stopping with %d messages", messageCount)
							readerDone <- messageCount
							return
						}
					default:
					}

					readCtx, cancel := context.WithTimeout(ctx, readTimeout)
					msg, err := reader.ReadNext(readCtx)
					cancel()

					if err != nil {
						if time.Since(lastReadTime) > readTimeout {
							t.Logf("Tail Reader: Read timeout, stopping with %d messages", messageCount)
							readerDone <- messageCount
							return
						}
						time.Sleep(500 * time.Millisecond)
						continue
					}

					if msg == nil {
						time.Sleep(200 * time.Millisecond)
						continue
					}

					// Message read
					messageCount++
					lastReadTime = time.Now()

					if messageCount%5 == 0 {
						t.Logf("Tail Reader: Read %d messages", messageCount)
					}
				}
			}()

			// 2. Start concurrent writers
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(writerDone)

				t.Log("Concurrent Writers: Starting...")

				var writerWg sync.WaitGroup
				writer, err := logHandle.OpenLogWriter(ctx)
				if err != nil {
					errorChan <- fmt.Errorf("writer failed to open: %v", err)
					return
				}
				defer writer.Close(ctx)

				// Start concurrent writers
				for writerID := 1; writerID <= concurrentThreads; writerID++ {
					writerWg.Add(1)
					go func(wID int) {
						defer writerWg.Done()

						// Write messages
						for i := 0; i < messagesPerThread; i++ {
							data := generateSimpleTestData(messageSize, fmt.Sprintf("W%dM%d", wID, i))
							message := &log.WriterMessage{
								Payload: data,
								Properties: map[string]string{
									"thread_id": fmt.Sprintf("%d", wID),
									"msg_idx":   fmt.Sprintf("%d", i),
								},
							}

							result := writer.Write(ctx, message)
							if result.Err != nil {
								t.Logf("thread %d msg %d: write failed: %v", wID, i, result.Err)
								errorChan <- fmt.Errorf("thread %d msg %d: write failed: %v", wID, i, result.Err)
								continue
							} else {
								t.Logf("thread %d msg %d: writtenMsgId: %d ", wID, i, result.LogMessageId)
							}

							if i%2 == 0 {
								t.Logf("thread %d: Written message %d/%d", wID, i+1, messagesPerThread)
							}
						}

						t.Logf("thread %d: Completed all %d messages", wID, concurrentThreads)
					}(writerID)
				}

				// Wait for all threads to complete
				writerWg.Wait()
				t.Log("Concurrent Writers: All threads completed")
			}()

			// Wait for all goroutines to complete
			wg.Wait()

			// Collect results
			totalMessagesRead := <-readerDone
			close(errorChan)

			var errors []error
			for err := range errorChan {
				errors = append(errors, err)
			}

			// Report results
			expectedMessages := concurrentThreads * messagesPerThread
			t.Logf("=== Test Results ===")
			t.Logf("Expected messages: %d", expectedMessages)
			t.Logf("Messages read: %d", totalMessagesRead)
			t.Logf("Errors: %d", len(errors))

			for _, err := range errors {
				t.Logf("Error: %v", err)
			}

			// Verification
			assert.True(t, totalMessagesRead > 0, "Should read at least some messages")
			assert.True(t, totalMessagesRead >= expectedMessages/2, "Should read at least half of expected messages")
			assert.True(t, len(errors) < 5, "Error count should be low")

			t.Log("Test completed successfully - simple high throughput test")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

// Generate simple test data
func generateSimpleTestData(size int, identifier string) []byte {
	data := make([]byte, size)
	pattern := []byte(identifier + ":")
	for i := 0; i < size; i++ {
		data[i] = pattern[i%len(pattern)]
	}
	return data
}
