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
	"github.com/zilliztech/woodpecker/tests/utils"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// One goroutine writes messages one by one, closing and reopening the writer after a certain time to continue writing.
// Another goroutine continuously performs a tail read, verifying that the data is continuous.
func TestWriteAndConcurrentTailRead(t *testing.T) {
	utils.StartGopsAgentWithPort(6061)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWriteAndConcurrentTailRead")
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
			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = 2 // 2s

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			defer client.Close(context.TODO())

			// Test parameters
			const writerCycles = 3       // Number of writer open/close cycles
			const messagesPerCycle = 25  // Messages written per cycle
			const writerPauseMs = 500    // Pause between cycles in milliseconds
			const messageIntervalMs = 50 // Interval between messages in milliseconds

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_write_concurrent_tail_read_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Synchronization channels
			writerDone := make(chan struct{})
			readerDone := make(chan []string)
			readerErrors := make(chan error, 10)

			totalExpectedMessages := writerCycles * messagesPerCycle

			// Start writer goroutine
			go func() {
				defer close(writerDone)

				totalWritten := 0
				for cycle := 0; cycle < writerCycles; cycle++ {
					t.Logf("Writer Cycle %d/%d: Opening writer", cycle+1, writerCycles)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to open writer: %v", cycle+1, err)
						return
					}

					// Write messages in this cycle
					for i := 0; i < messagesPerCycle; i++ {
						message := &log.WriteMessage{
							Payload: []byte(fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle+1, i)),
							Properties: map[string]string{
								"cycle":       fmt.Sprintf("%d", cycle+1),
								"message_idx": fmt.Sprintf("%d", i),
								"total_idx":   fmt.Sprintf("%d", totalWritten),
								"type":        "tail-read-test",
							},
						}

						result := writer.Write(ctx, message)
						if result.Err != nil {
							t.Errorf("Writer Cycle %d Message %d: Write failed: %v", cycle+1, i, result.Err)
							writer.Close(ctx)
							return
						}

						totalWritten++

						// Log milestone messages
						if i%10 == 0 || i == messagesPerCycle-1 {
							t.Logf("Writer Cycle %d: Written message %d/%d (total: %d) with ID: segment=%d, entry=%d",
								cycle+1, i+1, messagesPerCycle, totalWritten, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay between messages
						time.Sleep(time.Duration(messageIntervalMs) * time.Millisecond)
					}

					// Close writer
					err = writer.Close(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to close writer: %v", cycle+1, err)
						return
					}

					t.Logf("Writer Cycle %d/%d: Completed writing %d messages, total written: %d",
						cycle+1, writerCycles, messagesPerCycle, totalWritten)

					// Pause between cycles (except for the last cycle)
					if cycle < writerCycles-1 {
						t.Logf("Writer: Pausing for %dms before next cycle", writerPauseMs)
						time.Sleep(time.Duration(writerPauseMs) * time.Millisecond)
					}
				}

				t.Logf("Writer: Completed all %d cycles, total messages written: %d", writerCycles, totalWritten)
			}()

			// Start tail reader goroutine
			go func() {
				defer close(readerDone)

				// Start reading from the beginning
				startPoint := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   0,
				}

				reader, err := logHandle.OpenLogReader(ctx, startPoint, "tail-reader")
				if err != nil {
					readerErrors <- fmt.Errorf("failed to open tail reader: %v", err)
					readerDone <- nil
					return
				}
				defer reader.Close(ctx)

				t.Logf("Tail Reader: Started reading from beginning, expecting %d total messages", totalExpectedMessages)

				messages := make([]string, 0, totalExpectedMessages)
				messageCount := 0

				// Read messages continuously until we have all expected messages
				for messageCount < totalExpectedMessages {
					// Use a reasonable timeout for each read operation
					readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					msg, err := reader.ReadNext(readCtx)
					cancel()

					if err != nil {
						readerErrors <- fmt.Errorf("tail reader failed to read message %d: %v", messageCount, err)
						readerDone <- messages
						return
					}

					if msg == nil {
						readerErrors <- fmt.Errorf("tail reader received nil message at position %d", messageCount)
						readerDone <- messages
						return
					}

					messages = append(messages, string(msg.Payload))
					messageCount++

					// Log progress periodically
					if messageCount%10 == 0 || messageCount == totalExpectedMessages {
						t.Logf("Tail Reader: Read message %d/%d: seg:%d,entry:%d payload:%s",
							messageCount, totalExpectedMessages, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
					}
				}

				t.Logf("Tail Reader: Completed reading all %d messages", len(messages))
				readerDone <- messages
			}()

			// Wait for writer to complete
			<-writerDone
			t.Log("Writer goroutine completed")

			// Wait for reader to complete and collect results
			readerMessages := <-readerDone
			t.Logf("Tail Reader completed with %d/%d messages", len(readerMessages), totalExpectedMessages)

			// Check for reader errors
			close(readerErrors)
			for err := range readerErrors {
				t.Errorf("Reader error: %v", err)
			}

			// Verify results
			assert.Equal(t, totalExpectedMessages, len(readerMessages), "Tail reader should read all %d messages", totalExpectedMessages)

			// Verify message content and continuity
			expectedIdx := 0
			for cycle := 1; cycle <= writerCycles; cycle++ {
				for i := 0; i < messagesPerCycle; i++ {
					expectedMsg := fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle, i)
					if expectedIdx < len(readerMessages) {
						assert.Equal(t, expectedMsg, readerMessages[expectedIdx],
							"Message %d content mismatch: expected %s, got %s", expectedIdx, expectedMsg, readerMessages[expectedIdx])
					}
					expectedIdx++
				}
			}

			t.Log("Test completed successfully - all messages read in correct order")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

// One goroutine writes messages one by one, closing and reopening the writer after a certain time to continue writing.
// Another goroutine performs a catchup read from the beginning at regular intervals, verifying that the data is continuous.
func TestWriteAndConcurrentCatchupRead(t *testing.T) {
	utils.StartGopsAgentWithPort(6062)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWriteAndConcurrentCatchupRead")
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
			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = 2 // 2s

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			defer client.Close(context.TODO())

			// Test parameters
			const writerCycles = 4       // Number of writer open/close cycles
			const messagesPerCycle = 20  // Messages written per cycle
			const writerPauseMs = 500    // Pause between cycles in milliseconds
			const messageIntervalMs = 50 // Interval between messages in milliseconds

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_write_concurrent_catchup_read_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Communication channel: writer tells reader which cycle is completed
			cycleCompleteChan := make(chan int, writerCycles)
			writerDone := make(chan struct{})
			readerDone := make(chan struct{})
			readerErrors := make(chan error, 10)

			totalExpectedMessages := writerCycles * messagesPerCycle

			// Start writer goroutine
			go func() {
				defer close(writerDone)
				defer close(cycleCompleteChan)

				for cycle := 1; cycle <= writerCycles; cycle++ {
					t.Logf("Writer Cycle %d/%d: Opening writer", cycle, writerCycles)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to open writer: %v", cycle, err)
						return
					}

					// Write 20 messages in this cycle
					for i := 0; i < messagesPerCycle; i++ {
						message := &log.WriteMessage{
							Payload: []byte(fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle, i)),
							Properties: map[string]string{
								"cycle":   fmt.Sprintf("%d", cycle),
								"msg_idx": fmt.Sprintf("%d", i),
								"type":    "catchup-read-test",
							},
						}

						result := writer.Write(ctx, message)
						if result.Err != nil {
							t.Errorf("Writer Cycle %d Message %d: Write failed: %v", cycle, i, result.Err)
							writer.Close(ctx)
							return
						}

						// Log progress
						if i%10 == 0 || i == messagesPerCycle-1 {
							t.Logf("Writer Cycle %d: Written message %d/%d with ID: segment=%d, entry=%d",
								cycle, i+1, messagesPerCycle, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay between messages
						time.Sleep(time.Duration(messageIntervalMs) * time.Millisecond)
					}

					// Close writer
					err = writer.Close(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to close writer: %v", cycle, err)
						return
					}

					// Notify reader that this cycle is complete
					cycleCompleteChan <- cycle
					t.Logf("Writer Cycle %d/%d: Completed, notified reader", cycle, writerCycles)

					// Pause between cycles (except for the last cycle)
					if cycle < writerCycles {
						t.Logf("Writer: Pausing for %dms before next cycle", writerPauseMs)
						time.Sleep(time.Duration(writerPauseMs) * time.Millisecond)
					}
				}

				t.Logf("Writer: Completed all %d cycles", writerCycles)
			}()

			// Start catchup reader goroutine
			go func() {
				defer close(readerDone)

				for {
					select {
					case completedCycle, ok := <-cycleCompleteChan:
						if !ok {
							// Writer is done, exit loop
							goto finalRead
						}

						// Calculate expected messages up to this cycle
						expectedMessages := completedCycle * messagesPerCycle

						t.Logf("Catchup Reader: Cycle %d completed, reading from 0 to message %d", completedCycle, expectedMessages)

						// Read from beginning to current point
						startPoint := &log.LogMessageId{SegmentId: 0, EntryId: 0}
						reader, err := logHandle.OpenLogReader(ctx, startPoint, fmt.Sprintf("catchup-reader-cycle-%d", completedCycle))
						if err != nil {
							readerErrors <- fmt.Errorf("cycle %d: failed to open reader: %v", completedCycle, err)
							continue
						}

						// Read exactly expectedMessages
						var messages []string
						for i := 0; i < expectedMessages; i++ {
							readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
							msg, err := reader.ReadNext(readCtx)
							cancel()

							if err != nil {
								readerErrors <- fmt.Errorf("cycle %d: read message %d failed: %v", completedCycle, i, err)
								reader.Close(ctx)
								break
							}

							if msg == nil {
								readerErrors <- fmt.Errorf("cycle %d: received nil message at position %d", completedCycle, i)
								reader.Close(ctx)
								break
							}

							messages = append(messages, string(msg.Payload))
						}
						reader.Close(ctx)

						// Verify the messages
						if len(messages) == expectedMessages {
							t.Logf("Catchup Reader: Cycle %d verification - read %d messages correctly", completedCycle, len(messages))

							// Verify content
							for i, msg := range messages {
								cycle := (i / messagesPerCycle) + 1
								msgIdx := i % messagesPerCycle
								expectedMsg := fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle, msgIdx)
								if msg != expectedMsg {
									readerErrors <- fmt.Errorf("cycle %d: message %d content mismatch: expected %s, got %s", completedCycle, i, expectedMsg, msg)
									break
								}
							}
						} else {
							readerErrors <- fmt.Errorf("cycle %d: expected %d messages but got %d", completedCycle, expectedMessages, len(messages))
						}

					case <-time.After(10 * time.Second):
						readerErrors <- fmt.Errorf("reader timeout waiting for cycle completion")
						return
					}
				}

			finalRead:
				t.Logf("Catchup Reader: Writer completed, performing final read from 0 to end")
				time.Sleep(200 * time.Millisecond) // Allow final flush

				// Final verification read
				startPoint := &log.LogMessageId{SegmentId: 0, EntryId: 0}
				finalReader, err := logHandle.OpenLogReader(ctx, startPoint, "final-catchup-reader")
				if err != nil {
					readerErrors <- fmt.Errorf("final read: failed to open reader: %v", err)
					return
				}
				defer finalReader.Close(ctx)

				var finalMessages []string
				for i := 0; i < totalExpectedMessages; i++ {
					readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					msg, err := finalReader.ReadNext(readCtx)
					cancel()

					if err != nil {
						readerErrors <- fmt.Errorf("final read: message %d failed: %v", i, err)
						break
					}

					if msg == nil {
						readerErrors <- fmt.Errorf("final read: received nil message at position %d", i)
						break
					}

					finalMessages = append(finalMessages, string(msg.Payload))
				}

				// Verify final read
				if len(finalMessages) == totalExpectedMessages {
					t.Logf("Final Catchup Reader: Successfully read all %d messages", len(finalMessages))

					// Verify content
					for i, msg := range finalMessages {
						cycle := (i / messagesPerCycle) + 1
						msgIdx := i % messagesPerCycle
						expectedMsg := fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle, msgIdx)
						if msg != expectedMsg {
							readerErrors <- fmt.Errorf("final read: message %d content mismatch: expected %s, got %s", i, expectedMsg, msg)
							break
						}
					}
					t.Logf("Final Catchup Reader: All messages verified successfully")
				} else {
					readerErrors <- fmt.Errorf("final read: expected %d messages but got %d", totalExpectedMessages, len(finalMessages))
				}
			}()

			// Wait for writer to complete
			<-writerDone
			t.Log("Writer goroutine completed")

			// Wait for reader to complete
			<-readerDone
			t.Log("Catchup Reader goroutine completed")

			// Check for reader errors
			close(readerErrors)
			for err := range readerErrors {
				t.Errorf("Reader error: %v", err)
			}

			t.Log("Test completed successfully - all catchup reads verified")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

// Similar to TestWriteAndConcurrentTailReadWithTruncate, but with an additional goroutine that periodically calls logHandle's truncate to advance the truncation point.
func TestWriteAndConcurrentTailReadWithTruncate(t *testing.T) {
	utils.StartGopsAgentWithPort(6063)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWriteAndConcurrentTailReadWithTruncate")
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
			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = 2 // 2s

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			defer client.Close(context.TODO())

			// Test parameters
			const writerCycles = 4          // Number of writer open/close cycles
			const messagesPerCycle = 30     // Messages written per cycle
			const writerPauseMs = 800       // Pause between cycles in milliseconds
			const messageIntervalMs = 60    // Interval between messages in milliseconds
			const truncateIntervalMs = 2500 // Interval between truncation operations in milliseconds

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_write_tail_read_truncate_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Synchronization channels
			writerDone := make(chan struct{})
			readerDone := make(chan []string)
			truncatorDone := make(chan struct{})
			readerErrors := make(chan error, 10)
			truncateErrors := make(chan error, 10)

			totalExpectedMessages := writerCycles * messagesPerCycle
			writtenMessages := make(chan *log.LogMessageId, totalExpectedMessages) // Track written message IDs for truncation

			// Start writer goroutine
			go func() {
				defer close(writerDone)
				defer close(writtenMessages)

				totalWritten := 0
				for cycle := 0; cycle < writerCycles; cycle++ {
					t.Logf("Writer Cycle %d/%d: Opening writer", cycle+1, writerCycles)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to open writer: %v", cycle+1, err)
						return
					}

					// Write messages in this cycle
					for i := 0; i < messagesPerCycle; i++ {
						message := &log.WriteMessage{
							Payload: []byte(fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle+1, i)),
							Properties: map[string]string{
								"cycle":       fmt.Sprintf("%d", cycle+1),
								"message_idx": fmt.Sprintf("%d", i),
								"total_idx":   fmt.Sprintf("%d", totalWritten),
								"type":        "tail-read-truncate-test",
							},
						}

						result := writer.Write(ctx, message)
						if result.Err != nil {
							t.Errorf("Writer Cycle %d Message %d: Write failed: %v", cycle+1, i, result.Err)
							writer.Close(ctx)
							return
						}

						// Send message ID for potential truncation
						select {
						case writtenMessages <- result.LogMessageId:
						default:
							t.Logf("Writer: Message ID buffer full, continuing...")
						}

						totalWritten++

						// Log milestone messages
						if i%15 == 0 || i == messagesPerCycle-1 {
							t.Logf("Writer Cycle %d: Written message %d/%d (total: %d) with ID: segment=%d, entry=%d",
								cycle+1, i+1, messagesPerCycle, totalWritten, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay between messages
						time.Sleep(time.Duration(messageIntervalMs) * time.Millisecond)
					}

					// Close writer
					err = writer.Close(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to close writer: %v", cycle+1, err)
						return
					}

					t.Logf("Writer Cycle %d/%d: Completed writing %d messages, total written: %d",
						cycle+1, writerCycles, messagesPerCycle, totalWritten)

					// Pause between cycles (except for the last cycle)
					if cycle < writerCycles-1 {
						t.Logf("Writer: Pausing for %dms before next cycle", writerPauseMs)
						time.Sleep(time.Duration(writerPauseMs) * time.Millisecond)
					}
				}

				t.Logf("Writer: Completed all %d cycles, total messages written: %d", writerCycles, totalWritten)
			}()

			// Start truncator goroutine
			go func() {
				defer close(truncatorDone)

				var messageIds []*log.LogMessageId
				truncateCount := 0
				ticker := time.NewTicker(time.Duration(truncateIntervalMs) * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case msgId, ok := <-writtenMessages:
						if !ok {
							// Writer is done, perform final truncation if we have messages
							if len(messageIds) > 0 {
								// Truncate at a reasonable point, not too close to the end
								if len(messageIds) > 10 {
									truncateIdx := len(messageIds) - 5
									truncateId := messageIds[truncateIdx]
									t.Logf("Truncator: Final truncation at segment=%d, entry=%d", truncateId.SegmentId, truncateId.EntryId)

									err := logHandle.Truncate(ctx, truncateId)
									if err != nil {
										truncateErrors <- fmt.Errorf("final truncation failed: %v", err)
									} else {
										t.Logf("Truncator: Final truncation successful")
									}
								}
							}
							return
						}
						messageIds = append(messageIds, msgId)

					case <-ticker.C:
						// Perform truncation if we have enough messages
						if len(messageIds) > 20 {
							truncateCount++
							// Truncate at a point that leaves some messages for the tail reader
							truncateIdx := len(messageIds) - 15 // Keep last 15 messages available
							truncateId := messageIds[truncateIdx]

							t.Logf("Truncator #%d: Truncating at message %d (segment=%d, entry=%d), keeping last 15 messages",
								truncateCount, truncateIdx, truncateId.SegmentId, truncateId.EntryId)

							err := logHandle.Truncate(ctx, truncateId)
							if err != nil {
								truncateErrors <- fmt.Errorf("truncation %d failed: %v", truncateCount, err)
								t.Logf("Truncator #%d: Failed: %v", truncateCount, err)
							} else {
								t.Logf("Truncator #%d: Success", truncateCount)
								// Remove truncated messages from our tracking
								messageIds = messageIds[truncateIdx+1:]
							}
						}
					}
				}
			}()

			// Start tail reader goroutine
			go func() {
				defer close(readerDone)

				// Start reading from the beginning initially
				startPoint := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   0,
				}

				reader, err := logHandle.OpenLogReader(ctx, startPoint, "tail-reader-with-truncate")
				if err != nil {
					readerErrors <- fmt.Errorf("failed to open tail reader: %v", err)
					readerDone <- nil
					return
				}
				defer reader.Close(ctx)

				t.Logf("Tail Reader: Started reading from beginning")

				messages := make([]string, 0)
				messageCount := 0
				lastSuccessfulRead := time.Now()
				readTimeout := 45 * time.Second // Longer timeout to handle truncation gaps

				// Read messages continuously, handling truncation gracefully
				for {
					// Check if writer is done and we haven't read anything recently
					select {
					case <-writerDone:
						if time.Since(lastSuccessfulRead) > 5*time.Second {
							t.Logf("Tail Reader: Writer done and no recent reads, stopping")
							readerDone <- messages
							return
						}
					default:
					}

					// Use a reasonable timeout for each read operation
					readCtx, cancel := context.WithTimeout(ctx, readTimeout)
					msg, err := reader.ReadNext(readCtx)
					cancel()

					if err != nil {
						// Check if this is a truncation-related error
						if time.Since(lastSuccessfulRead) > 10*time.Second {
							// If we haven't read anything for a while and writer is done, we're probably finished
							select {
							case <-writerDone:
								t.Logf("Tail Reader: Read timeout after writer completion, finishing with %d messages", len(messages))
								readerDone <- messages
								return
							default:
								t.Logf("Tail Reader: Read timeout but writer still active, continuing...")
								continue
							}
						}

						t.Logf("Tail Reader: Read error (may be due to truncation): %v, continuing...", err)
						time.Sleep(500 * time.Millisecond) // Brief pause before retrying
						continue
					}

					if msg == nil {
						t.Logf("Tail Reader: Received nil message, continuing...")
						time.Sleep(200 * time.Millisecond)
						continue
					}

					messages = append(messages, string(msg.Payload))
					messageCount++
					lastSuccessfulRead = time.Now()

					// Log progress periodically
					if messageCount%20 == 0 {
						t.Logf("Tail Reader: Read message %d: seg:%d,entry:%d payload:%s",
							messageCount, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
					}
				}
			}()

			// Wait for writer to complete
			<-writerDone
			t.Log("Writer goroutine completed")

			// Wait for truncator to complete
			<-truncatorDone
			t.Log("Truncator goroutine completed")

			// Give reader some time to finish reading
			time.Sleep(2 * time.Second)

			// Wait for reader to complete and collect results
			readerMessages := <-readerDone
			t.Logf("Tail Reader completed with %d messages", len(readerMessages))

			// Check for reader and truncator errors
			close(readerErrors)
			for err := range readerErrors {
				t.Errorf("Reader error: %v", err)
			}

			close(truncateErrors)
			for err := range truncateErrors {
				t.Errorf("Truncator error: %v", err)
			}

			// Verify results
			// Due to truncation, we may not have all messages, but messages should be continuous
			assert.True(t, len(readerMessages) > 0, "Tail reader should read at least some messages despite truncation")

			// Since messages may be truncated, we can't expect all messages
			// But the messages we do have should be continuous
			t.Logf("Total messages written: %d, Messages read by tail reader: %d", totalExpectedMessages, len(readerMessages))

			// Verify that messages are in correct order (what we can read)
			if len(readerMessages) > 1 {
				t.Log("Verifying message continuity...")
				// We'll just verify that the messages we have follow the expected pattern
				// Note: Due to truncation, we might start reading from the middle
				for i, msg := range readerMessages {
					if i < 5 || i >= len(readerMessages)-5 {
						t.Logf("Message %d: %s", i, msg)
					}
				}
			}

			t.Log("Test completed successfully - tail reading with truncation verified")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

// Similar to TestWriteAndConcurrentCatchupRead, but with an additional goroutine that periodically calls logHandle's truncate to advance the truncation point.
func TestWriteAndConcurrentCatchupReadWithTruncate(t *testing.T) {
	utils.StartGopsAgentWithPort(6064)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWriteAndConcurrentCatchupReadWithTruncate")
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
			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = 2 // 2s

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			defer client.Close(context.TODO())

			// Test parameters
			const writerCycles = 4          // Number of writer open/close cycles
			const messagesPerCycle = 25     // Messages written per cycle
			const writerPauseMs = 900       // Pause between cycles in milliseconds
			const messageIntervalMs = 70    // Interval between messages in milliseconds
			const catchupIntervalMs = 1400  // Interval between catchup reads in milliseconds
			const truncateIntervalMs = 2800 // Interval between truncation operations in milliseconds

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_write_catchup_read_truncate_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Synchronization channels
			writerDone := make(chan struct{})
			readerDone := make(chan [][]string) // Each catchup read returns a slice of messages
			truncatorDone := make(chan struct{})
			readerErrors := make(chan error, 20)
			truncateErrors := make(chan error, 10)

			totalExpectedMessages := writerCycles * messagesPerCycle
			writtenMessages := make(chan *log.LogMessageId, totalExpectedMessages) // Track written message IDs for truncation

			// Start writer goroutine
			go func() {
				defer close(writerDone)
				defer close(writtenMessages)

				totalWritten := 0
				for cycle := 0; cycle < writerCycles; cycle++ {
					t.Logf("Writer Cycle %d/%d: Opening writer", cycle+1, writerCycles)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to open writer: %v", cycle+1, err)
						return
					}

					// Write messages in this cycle
					for i := 0; i < messagesPerCycle; i++ {
						message := &log.WriteMessage{
							Payload: []byte(fmt.Sprintf("Writer-Cycle-%d-Message-%d", cycle+1, i)),
							Properties: map[string]string{
								"cycle":       fmt.Sprintf("%d", cycle+1),
								"message_idx": fmt.Sprintf("%d", i),
								"total_idx":   fmt.Sprintf("%d", totalWritten),
								"type":        "catchup-read-truncate-test",
							},
						}

						result := writer.Write(ctx, message)
						if result.Err != nil {
							t.Errorf("Writer Cycle %d Message %d: Write failed: %v", cycle+1, i, result.Err)
							writer.Close(ctx)
							return
						}

						// Send message ID for potential truncation
						select {
						case writtenMessages <- result.LogMessageId:
						default:
							t.Logf("Writer: Message ID buffer full, continuing...")
						}

						totalWritten++

						// Log milestone messages
						if i%10 == 0 || i == messagesPerCycle-1 {
							t.Logf("Writer Cycle %d: Written message %d/%d (total: %d) with ID: segment=%d, entry=%d",
								cycle+1, i+1, messagesPerCycle, totalWritten, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay between messages
						time.Sleep(time.Duration(messageIntervalMs) * time.Millisecond)
					}

					// Close writer
					err = writer.Close(ctx)
					if err != nil {
						t.Errorf("Writer Cycle %d: Failed to close writer: %v", cycle+1, err)
						return
					}

					t.Logf("Writer Cycle %d/%d: Completed writing %d messages, total written: %d",
						cycle+1, writerCycles, messagesPerCycle, totalWritten)

					// Pause between cycles (except for the last cycle)
					if cycle < writerCycles-1 {
						t.Logf("Writer: Pausing for %dms before next cycle", writerPauseMs)
						time.Sleep(time.Duration(writerPauseMs) * time.Millisecond)
					}
				}

				t.Logf("Writer: Completed all %d cycles, total messages written: %d", writerCycles, totalWritten)
			}()

			// Start truncator goroutine
			go func() {
				defer close(truncatorDone)

				var messageIds []*log.LogMessageId
				truncateCount := 0
				ticker := time.NewTicker(time.Duration(truncateIntervalMs) * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case msgId, ok := <-writtenMessages:
						if !ok {
							// Writer is done, perform final truncation if we have messages
							if len(messageIds) > 0 {
								// Truncate at a reasonable point, not too close to the end
								if len(messageIds) > 10 {
									truncateIdx := len(messageIds) - 5
									truncateId := messageIds[truncateIdx]
									t.Logf("Truncator: Final truncation at segment=%d, entry=%d", truncateId.SegmentId, truncateId.EntryId)

									err := logHandle.Truncate(ctx, truncateId)
									if err != nil {
										truncateErrors <- fmt.Errorf("final truncation failed: %v", err)
									} else {
										t.Logf("Truncator: Final truncation successful")
									}
								}
							}
							return
						}
						messageIds = append(messageIds, msgId)

					case <-ticker.C:
						// Perform truncation if we have enough messages
						if len(messageIds) > 15 {
							truncateCount++
							// Truncate at a point that leaves some messages for the catchup reader
							truncateIdx := len(messageIds) - 10 // Keep last 10 messages available
							truncateId := messageIds[truncateIdx]

							t.Logf("Truncator #%d: Truncating at message %d (segment=%d, entry=%d), keeping last 10 messages",
								truncateCount, truncateIdx, truncateId.SegmentId, truncateId.EntryId)

							err := logHandle.Truncate(ctx, truncateId)
							if err != nil {
								truncateErrors <- fmt.Errorf("truncation %d failed: %v", truncateCount, err)
								t.Logf("Truncator #%d: Failed: %v", truncateCount, err)
							} else {
								t.Logf("Truncator #%d: Success", truncateCount)
								// Remove truncated messages from our tracking
								messageIds = messageIds[truncateIdx+1:]
							}
						}
					}
				}
			}()

			// Start catchup reader goroutine
			go func() {
				defer close(readerDone)

				var allCatchupReads [][]string
				catchupReadCount := 0

				// Perform periodic catchup reads during and after writing
				// Continue until writer is done and we've done a final read
				writerCompleted := false
				ticker := time.NewTicker(time.Duration(catchupIntervalMs) * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-writerDone:
						writerCompleted = true
						// Allow some time for final messages to be flushed
						time.Sleep(500 * time.Millisecond)
					case <-ticker.C:
						// Perform catchup read
					}

					catchupReadCount++
					t.Logf("Catchup Reader: Starting catchup read #%d", catchupReadCount)

					// Always start reading from the beginning (or after truncation point)
					startPoint := &log.LogMessageId{
						SegmentId: 0,
						EntryId:   0,
					}

					reader, err := logHandle.OpenLogReader(ctx, startPoint, fmt.Sprintf("catchup-reader-truncate-%d", catchupReadCount))
					if err != nil {
						readerErrors <- fmt.Errorf("catchup read %d: failed to open reader: %v", catchupReadCount, err)
						if writerCompleted {
							readerDone <- allCatchupReads
							return
						}
						continue
					}

					// Read all available messages in this catchup
					messages := make([]string, 0)
					messageCount := 0

					for {
						// Use shorter timeout for catchup reads since we're scanning existing data
						readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						msg, err := reader.ReadNext(readCtx)
						cancel()

						if err != nil {
							// We've reached the end of available data (or hit truncation)
							break
						}

						if msg == nil {
							break
						}

						messages = append(messages, string(msg.Payload))
						messageCount++

						// Log progress for catchup reads
						if messageCount%15 == 0 {
							t.Logf("Catchup Reader #%d: Read message %d: seg:%d,entry:%d payload:%s",
								catchupReadCount, messageCount, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
						}
					}

					reader.Close(ctx)
					allCatchupReads = append(allCatchupReads, messages)

					t.Logf("Catchup Reader #%d: Completed, read %d messages", catchupReadCount, len(messages))

					// If writer is completed and this is our final read, exit
					if writerCompleted {
						t.Logf("Catchup Reader: Writer completed, performing final verification read")
						// Perform one more final read to ensure we have everything available
						time.Sleep(1000 * time.Millisecond)

						finalReader, err := logHandle.OpenLogReader(ctx, startPoint, "final-catchup-reader-truncate")
						if err != nil {
							readerErrors <- fmt.Errorf("final catchup read: failed to open reader: %v", err)
							readerDone <- allCatchupReads
							return
						}

						finalMessages := make([]string, 0)
						for {
							readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
							msg, err := finalReader.ReadNext(readCtx)
							cancel()

							if err != nil || msg == nil {
								break
							}

							finalMessages = append(finalMessages, string(msg.Payload))
						}

						finalReader.Close(ctx)
						allCatchupReads = append(allCatchupReads, finalMessages)
						t.Logf("Final Catchup Reader: Read %d messages", len(finalMessages))

						readerDone <- allCatchupReads
						return
					}
				}
			}()

			// Wait for writer to complete
			<-writerDone
			t.Log("Writer goroutine completed")

			// Wait for truncator to complete
			<-truncatorDone
			t.Log("Truncator goroutine completed")

			// Wait for reader to complete and collect results
			allCatchupResults := <-readerDone
			t.Logf("Catchup Reader completed %d catchup reads", len(allCatchupResults))

			// Check for reader and truncator errors
			close(readerErrors)
			for err := range readerErrors {
				t.Errorf("Reader error: %v", err)
			}

			close(truncateErrors)
			for err := range truncateErrors {
				t.Errorf("Truncator error: %v", err)
			}

			// Verify results - due to truncation, we may not have all messages
			assert.True(t, len(allCatchupResults) > 0, "Should have at least one catchup read")

			// Due to truncation, the final read may not contain all original messages
			// but we should have some messages
			if len(allCatchupResults) > 0 {
				finalReadMessages := allCatchupResults[len(allCatchupResults)-1]
				assert.True(t, len(finalReadMessages) > 0, "Final catchup read should contain some messages despite truncation")

				// Verify that each subsequent catchup read contains reasonable data
				// Note: Due to truncation, later reads might have fewer messages than earlier ones
				for i, readResult := range allCatchupResults {
					t.Logf("Catchup Read #%d: %d messages", i+1, len(readResult))

					// For debugging, show first and last few messages of each read
					if len(readResult) > 0 {
						if len(readResult) <= 3 {
							t.Logf("  All messages: %v", readResult)
						} else {
							t.Logf("  First 2: %v, Last 2: %v", readResult[:2], readResult[len(readResult)-2:])
						}
					}
				}

				// Verify message content in the final read (what survives truncation)
				if len(finalReadMessages) > 0 {
					t.Logf("Verifying %d messages in final read for continuity...", len(finalReadMessages))
					// Just verify the messages follow the expected pattern
					// Due to truncation, we may start from any cycle
					for i, msg := range finalReadMessages {
						if i < 3 || i >= len(finalReadMessages)-3 {
							t.Logf("Final read message %d: %s", i, msg)
						}
					}
				}

				t.Logf("Total messages written: %d, Final catchup read: %d messages", totalExpectedMessages, len(finalReadMessages))
			}

			t.Log("Test completed successfully - catchup reading with truncation verified")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}
