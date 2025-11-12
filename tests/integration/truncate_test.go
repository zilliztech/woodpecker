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
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestTruncateBasicOperation(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTruncateBasicOperation")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize client
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_test_log_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// 1. Write some initial data
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write 10 entries
			totalMsgs := 10
			writtenIds := make([]*log.LogMessageId, totalMsgs)
			for i := 0; i < totalMsgs; i++ {
				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    []byte(fmt.Sprintf("message-%d", i)),
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId
				t.Logf("Written message %d: segmentId=%d, entryId=%d\n",
					i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
			}

			// 2. Truncate at message 5
			truncatePoint := writtenIds[4] // truncate at index 4 (inclusive)
			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// 3. Verify truncation point is set correctly
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			// 4. Try to read from earliest and verify we start at the truncation point+1
			earliest := log.EarliestLogMessageID()
			logReader, err := logHandle.OpenLogReader(context.Background(), &earliest, "truncate-basic-reader")
			assert.NoError(t, err)

			// The first message we read should be message 5 (the one after truncation point)
			msg, err := logReader.ReadNext(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, msg)

			// These checks depend on the implementation details, but should work in most cases:
			// If all messages are in the same segment, the entryId should be truncatePoint.EntryId + 1
			if msg.Id.SegmentId == truncatePoint.SegmentId {
				assert.Equal(t, truncatePoint.EntryId+1, msg.Id.EntryId)
			} else {
				// If message is in a different segment, it should be the first message in that segment
				assert.Equal(t, int64(0), msg.Id.EntryId)
				assert.Greater(t, msg.Id.SegmentId, truncatePoint.SegmentId)
			}

			// Close reader and writer
			err = logReader.Close(context.Background())
			assert.NoError(t, err)
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestWriteAndTruncateConcurrently(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWriteAndTruncateConcurrently")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize client
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_test_concurrent_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open writer
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Constants for test scenario
			totalMessages := 20
			pauseReadPoint := 5
			truncatePoint := 10
			triggerTruncateAfter := 15

			// Coordination channels
			readPaused := make(chan struct{})
			truncateDone := make(chan struct{})
			doneWriting := make(chan struct{})
			doneReading := make(chan struct{})

			// Message tracking
			var writtenIds []*log.LogMessageId
			var mutex sync.Mutex
			var readMessages []*log.LogMessage

			// Start writer goroutine
			go func() {
				defer close(doneWriting)

				for i := 0; i < totalMessages; i++ {
					result := logWriter.Write(context.Background(), &log.WriteMessage{
						Payload:    []byte(fmt.Sprintf("message-%d", i)),
						Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
					})
					assert.NoError(t, result.Err)

					mutex.Lock()
					writtenIds = append(writtenIds, result.LogMessageId)
					count := len(writtenIds)
					mutex.Unlock()

					t.Logf("Written message %d: segmentId=%d, entryId=%d",
						i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

					// If we've written enough messages, trigger truncation
					if count == triggerTruncateAfter {
						// Truncate at the specified truncation point
						truncateAt := writtenIds[truncatePoint-1] // Zero-based index
						err := logHandle.Truncate(context.Background(), truncateAt)
						assert.NoError(t, err)

						truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
						assert.NoError(t, err)
						t.Logf("Truncated at message %d: segmentId=%d, entryId=%d",
							truncatePoint-1, truncatedId.SegmentId, truncatedId.EntryId)

						// Signal truncation is done
						close(truncateDone)
					}

					// Small delay to allow reader to process
					time.Sleep(10 * time.Millisecond)
				}

				// Close writer
				err = logWriter.Close(context.Background())
				assert.NoError(t, err)
				t.Logf("Closed writer")
			}()

			// Start reader goroutine
			go func() {
				defer close(doneReading)

				// Read from earliest
				earliest := log.EarliestLogMessageID()
				logReader, err := logHandle.OpenLogReader(context.Background(), &earliest, "truncate-concurrent-reader")
				assert.NoError(t, err)

				readCount := 0
				for {
					// If we've read up to the pause point, wait for truncation
					if readCount == pauseReadPoint {
						t.Logf("Reader paused after reading %d messages", readCount)
						close(readPaused)
						<-truncateDone
						t.Logf("Reader resuming after truncation")
					}

					// Read next message with timeout to avoid blocking indefinitely
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					msg, err := logReader.ReadNext(ctx)
					cancel()

					if err != nil {
						// Check if we've read all messages
						mutex.Lock()
						allMsgsWritten := len(writtenIds) == totalMessages
						mutex.Unlock()

						if allMsgsWritten && readCount >= pauseReadPoint+(totalMessages-truncatePoint) {
							t.Logf("Reader completed, read %d messages", readCount)
							break
						}

						t.Logf("Read timed out or error, waiting: %v", err)
						time.Sleep(100 * time.Millisecond)
						continue
					}

					// Extract message index for verification
					indexStr, ok := msg.Properties["index"]
					assert.True(t, ok, "Message should have index property")
					msgIndex, err := strconv.Atoi(indexStr)
					assert.NoError(t, err)

					t.Logf("Read message index=%d segmentId=%d, entryId=%d",
						msgIndex, msg.Id.SegmentId, msg.Id.EntryId)

					mutex.Lock()
					readMessages = append(readMessages, msg)
					mutex.Unlock()
					readCount++
				}

				err = logReader.Close(context.Background())
				assert.NoError(t, err)
				t.Logf("Closed reader")
			}()

			// Wait for reading and writing to complete
			<-readPaused
			t.Log("Reader has paused as expected after 5 messages")
			<-doneWriting
			<-doneReading

			// Verification: should have read exactly 20 messages (5 before + 15 after truncation)
			assert.Equal(t, totalMessages, len(readMessages), "Should have read all messages, because it is opened before truncate action")

			// Verify message indices
			indices := make([]int, 0, len(readMessages))
			for _, msg := range readMessages {
				indexStr := msg.Properties["index"]
				index, err := strconv.Atoi(indexStr)
				assert.NoError(t, err)
				indices = append(indices, index)
			}

			// Sort message indices for easier inspection
			sort.Ints(indices)
			t.Logf("Read message indices: %v", indices)

			// Verify we read the first 5 messages (0-4) and the messages after truncation point (10-19)
			// Messages 5-9 should be truncated
			for i := 0; i < pauseReadPoint; i++ {
				assert.Contains(t, indices, i, "Should have read message with index %d", i)
			}
			for i := pauseReadPoint; i < truncatePoint; i++ {
				assert.Contains(t, indices, i, "The Reader open before truncate, should have read truncated message with index %d", i)
			}
			for i := truncatePoint; i < totalMessages; i++ {
				assert.Contains(t, indices, i, "Should have read message with index %d", i)
			}

			// Final verification
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			t.Logf("Final truncation point: segmentId=%d, entryId=%d",
				truncatedId.SegmentId, truncatedId.EntryId)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestMultiSegmentTruncation(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMultiSegmentTruncation")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize client with small segment size to force multiple segments
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_test_multi_segment_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// 1. Write enough messages to create multiple segments
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages with enough data to force segment rotation
			const totalMsgs = 10
			const msgSize = 1024 // 1KB per message
			writtenIds := make([]*log.LogMessageId, totalMsgs)

			// Track segments seen
			segmentsSeen := make(map[int64]bool)

			for i := 0; i < totalMsgs; i++ {
				// Create large payload to force segment rotation
				payload := make([]byte, msgSize)
				for j := 0; j < msgSize; j++ {
					payload[j] = byte(i % 256)
				}

				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId

				// Track segments
				segmentsSeen[result.LogMessageId.SegmentId] = true

				// Print progress and segment info
				if i%2 == 0 {
					t.Logf("Written %d messages, currently in segment %d\n",
						i, result.LogMessageId.SegmentId)
				}
			}

			// Verify we've got multiple segments
			numSegments := len(segmentsSeen)
			t.Logf("Created %d segments\n", numSegments)
			assert.Equal(t, 5, numSegments, "Expected multiple segments to be created")

			// 2. Find a message in the middle of a segment for truncation
			var truncatePoint *log.LogMessageId
			for i := totalMsgs / 3; i < 2*totalMsgs/3; i++ {
				if i > 0 && writtenIds[i].SegmentId != writtenIds[i-1].SegmentId {
					// We found the start of a new segment - use the previous message
					truncatePoint = writtenIds[i-1]
					break
				}
			}

			if truncatePoint == nil {
				// If we didn't find a segment boundary, just use a message in the middle
				truncatePoint = writtenIds[totalMsgs/2]
			}

			t.Logf("Truncating at message with segmentId=%d, entryId=%d\n",
				truncatePoint.SegmentId, truncatePoint.EntryId)

			// 3. Truncate at the selected point
			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// 4. Verify truncation
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			// 5. Read from earliest and verify we start after truncation point
			earliest := log.EarliestLogMessageID()
			logReader, err := logHandle.OpenLogReader(context.Background(), &earliest, "truncate-multi-segment-reader")
			assert.NoError(t, err)

			// Read the first message
			msg, err := logReader.ReadNext(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, msg)

			// Verify it's after the truncation point
			isAfterTruncation := msg.Id.SegmentId > truncatePoint.SegmentId ||
				(msg.Id.SegmentId == truncatePoint.SegmentId && msg.Id.EntryId > truncatePoint.EntryId)
			assert.True(t, isAfterTruncation,
				"Expected first message to be after truncation point, got segmentId=%d, entryId=%d",
				msg.Id.SegmentId, msg.Id.EntryId)

			// Close everything
			err = logReader.Close(context.Background())
			assert.NoError(t, err)
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestReadBeforeTruncationPoint(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestReadBeforeTruncationPoint")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize client
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_read_before_truncation_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Write 20 messages (reduced from 100)
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			totalMsgs := 20 // Reduced from 100
			writtenIds := make([]*log.LogMessageId, totalMsgs)
			for i := 0; i < totalMsgs; i++ {
				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    []byte(fmt.Sprintf("message-%d", i)),
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId

				t.Logf("Written message %d: segmentId=%d, entryId=%d",
					i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
			}

			// Start reading from message 0 (earliest)
			earliest := log.EarliestLogMessageID()

			// Create a reader starting at earliest position
			logReader, err := logHandle.OpenLogReader(context.Background(), &earliest, "read-before-truncation-reader")
			assert.NoError(t, err)

			// Read 5 messages to simulate an active reader at position 5
			var readMessages []*log.LogMessage
			for i := 0; i < 5; i++ {
				msg, err := logReader.ReadNext(context.Background())
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				readMessages = append(readMessages, msg)
			}

			// Verify we read messages 0-4
			for i, msg := range readMessages {
				indexStr := msg.Properties["index"]
				index, err := strconv.Atoi(indexStr)
				assert.NoError(t, err)
				assert.Equal(t, i, index, "Should have read message with index %d", i)
			}

			t.Log("Successfully read first 5 messages (0-4)")

			// Now truncate at message 10 (inclusive)
			truncatePoint := writtenIds[9] // 0-based index, message 10 is at index 9
			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// Verify truncation point
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			t.Logf("Truncated at message 10: segmentId=%d, entryId=%d",
				truncatedId.SegmentId, truncatedId.EntryId)

			// Continue reading with the existing reader (which is at position 5)
			// It should still be able to read messages 5-19 even though 0-9 are truncated
			// Reading 15 more messages (for a total of 20)
			for i := 0; i < 15; i++ {
				msg, err := logReader.ReadNext(context.Background())
				assert.NoError(t, err)
				assert.NotNil(t, msg)

				indexStr := msg.Properties["index"]
				index, err := strconv.Atoi(indexStr)
				assert.NoError(t, err)
				assert.Equal(t, 5+i, index, "Should have read message with index %d", 5+i)

				readMessages = append(readMessages, msg)
				t.Logf("Read message after truncation: index=%d, segmentId=%d, entryId=%d",
					index, msg.Id.SegmentId, msg.Id.EntryId)
			}

			t.Log("Successfully read messages 5-19 with reader that started before truncation point")
			assert.Equal(t, 20, len(readMessages), "Should have read a total of 20 messages")

			// Now create a new reader from earliest position
			// It should start reading from position 11
			newReader, err := logHandle.OpenLogReader(context.Background(), &earliest, "new-reader-after-truncation")
			assert.NoError(t, err)

			// Read first message
			msg, err := newReader.ReadNext(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, msg)

			// Extract index to verify we're starting after truncation point
			indexStr := msg.Properties["index"]
			index, err := strconv.Atoi(indexStr)
			assert.NoError(t, err)

			t.Logf("New reader first message: index=%d, segmentId=%d, entryId=%d",
				index, msg.Id.SegmentId, msg.Id.EntryId)

			// The first message should be message 11 (index 10) or later
			assert.GreaterOrEqual(t, index, 10, "New reader should start at or after truncation point")

			// Clean up
			err = logReader.Close(context.Background())
			assert.NoError(t, err)
			err = newReader.Close(context.Background())
			assert.NoError(t, err)
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestSegmentCleanupAfterTruncation(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestSegmentCleanupAfterTruncation")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize client with small segment size to force multiple segments
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB
			// Set short cleanup check interval for tests
			cfg.Woodpecker.Client.Auditor.MaxInterval = 2 // 2 seconds

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "segment_cleanup_after_truncation_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// 1. Write enough messages to create multiple segments
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages with enough data to force segment rotation
			const totalMsgs = 30
			const msgSize = 1024 // 1KB per message
			writtenIds := make([]*log.LogMessageId, totalMsgs)

			// Track segments seen
			segmentsSeen := make(map[int64]bool)

			for i := 0; i < totalMsgs; i++ {
				// Create payload to force segment rotation
				payload := make([]byte, msgSize)
				for j := 0; j < msgSize; j++ {
					payload[j] = byte(i % 256)
				}

				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId

				// Track segments
				segmentsSeen[result.LogMessageId.SegmentId] = true

				// Print progress
				if i%5 == 0 {
					t.Logf("Written message %d: segmentId=%d, entryId=%d",
						i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
				}
			}

			// Verify we've got multiple segments
			numSegments := len(segmentsSeen)
			t.Logf("Created %d segments", numSegments)
			assert.Greater(t, numSegments, 5, "Expected at least 5 segments to be created")

			// Get all segments
			segments, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Create a map of segment IDs to track which ones should be eligible for cleanup
			initialSegments := make(map[int64]bool)
			for segId := range segments {
				initialSegments[segId] = true
				t.Logf("Initial segment: %d", segId)
			}

			// 2. Scenario 1: Create readers at different positions
			// Reader 1: Starts at beginning
			earliest := log.EarliestLogMessageID()
			reader1, err := logHandle.OpenLogReader(context.Background(), &earliest, "reader1-from-beginning")
			assert.NoError(t, err)

			// Read a few messages to simulate active usage
			for i := 0; i < 5; i++ {
				msg, err := reader1.ReadNext(context.Background())
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				t.Logf("Reader1 read message: %s, segmentId=%d, entryId=%d",
					string(msg.Payload), msg.Id.SegmentId, msg.Id.EntryId)
			}

			// Reader 2: Starts at middle segment
			middleIndex := totalMsgs / 2
			middlePoint := writtenIds[middleIndex]
			reader2, err := logHandle.OpenLogReader(context.Background(), middlePoint, "reader2-from-middle")
			assert.NoError(t, err)

			// Read a few messages
			for i := 0; i < 3; i++ {
				msg, err := reader2.ReadNext(context.Background())
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				t.Logf("Reader2 read message: %s, segmentId=%d, entryId=%d",
					string(msg.Payload), msg.Id.SegmentId, msg.Id.EntryId)
			}

			// 3. Perform truncation at 1/3 point
			truncateIndex := totalMsgs / 3
			truncatePoint := writtenIds[truncateIndex]
			t.Logf("Truncating at message index %d: segmentId=%d, entryId=%d",
				truncateIndex, truncatePoint.SegmentId, truncatePoint.EntryId)

			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// 4. Verify truncation point
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			// 5. Reader 1 continues reading
			for i := 0; i < 10; i++ {
				msg, err := reader1.ReadNext(context.Background())
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				t.Logf("Reader1 after truncation read message: segmentId=%d, entryId=%d",
					msg.Id.SegmentId, msg.Id.EntryId)
			}

			// 6. Close reader 1 to allow cleanup of early segments
			err = reader1.Close(context.Background())
			assert.NoError(t, err)
			t.Log("Closed reader1, early segments should now be eligible for cleanup")

			// 7. Create reader 3 from earliest (which should now be after truncation point)
			reader3, err := logHandle.OpenLogReader(context.Background(), &earliest, "reader3-after-truncation")
			assert.NoError(t, err)

			// Read first message to verify it starts after truncation point
			msg, err := reader3.ReadNext(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, msg)

			// Verify it's after truncation point
			isAfterTruncation := msg.Id.SegmentId > truncatePoint.SegmentId ||
				(msg.Id.SegmentId == truncatePoint.SegmentId && msg.Id.EntryId > truncatePoint.EntryId)
			assert.True(t, isAfterTruncation,
				"Expected first message to be after truncation point, got segmentId=%d, entryId=%d",
				msg.Id.SegmentId, msg.Id.EntryId)

			t.Logf("Reader3 starts at correct position after truncation: segmentId=%d, entryId=%d",
				msg.Id.SegmentId, msg.Id.EntryId)

			// 8. Wait for segment cleanup to occur - the LogWriter's auditor should detect and clean up
			// truncated segments that are no longer needed by any reader
			t.Log("Waiting for segment cleanup to occur...")

			// Wait for a reasonable time to allow cleanup to happen
			// The auditor runs every few seconds (maxInterval configured earlier)
			time.Sleep(10 * time.Second)

			// 9. Check if segments before truncation point were cleaned up
			segments, err = logHandle.GetMetadataProvider().GetAllSegmentMetadata(context.Background(), logHandle.GetName())
			assert.NoError(t, err)

			var segmentsAfterCleanup []int64
			for segId := range segments {
				segmentsAfterCleanup = append(segmentsAfterCleanup, segId)
			}

			// Sort segment IDs for readability
			sort.Slice(segmentsAfterCleanup, func(i, j int) bool {
				return segmentsAfterCleanup[i] < segmentsAfterCleanup[j]
			})

			t.Logf("Segments after cleanup: %v", segmentsAfterCleanup)

			// 10. Verify segments that should be cleaned up are no longer in metadata
			// Check if segments that were completely before the truncation point
			// are marked as truncated and eventually cleaned up
			for segId, seg := range segments {
				if segId < truncatePoint.SegmentId {
					// Segments completely before truncation point should be marked as truncated
					assert.Equal(t, proto.SegmentState_Truncated, seg.Metadata.State,
						"Segment %d should be marked as truncated, as truncated Point:%d/%d", segId, truncatePoint.SegmentId, truncatePoint.EntryId)
				}
			}

			// 11. Continue reading with reader 2 and reader 3 to ensure they still work
			// Read more messages with reader 2
			for i := 0; i < 5; i++ {
				msg, err := reader2.ReadNext(context.Background())
				if err != nil {
					t.Logf("Reader2 reached end or error: %v", err)
					break
				}
				assert.NotNil(t, msg)
				t.Logf("Reader2 after cleanup read message: segmentId=%d, entryId=%d",
					msg.Id.SegmentId, msg.Id.EntryId)
			}

			// Read more messages with reader 3
			for i := 0; i < 5; i++ {
				msg, err := reader3.ReadNext(context.Background())
				if err != nil {
					t.Logf("Reader3 reached end or error: %v", err)
					break
				}
				assert.NotNil(t, msg)
				t.Logf("Reader3 after cleanup read message: segmentId=%d, entryId=%d",
					msg.Id.SegmentId, msg.Id.EntryId)
			}

			// 12. Cleanup
			err = reader2.Close(context.Background())
			assert.NoError(t, err)
			err = reader3.Close(context.Background())
			assert.NoError(t, err)
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			t.Log("Test completed successfully")
		})
	}
}

func TestTruncateAndWriteWithNewSegment(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTruncateAndWriteWithNewSegment")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize client with small segment size to force multiple segments
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_continuity_test_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// 1. Write enough messages to create multiple segments
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages with enough data to force segment rotation
			const totalMsgs = 30
			const msgSize = 1024 // 1KB per message
			writtenIds := make([]*log.LogMessageId, totalMsgs)

			// Track segments seen
			segmentMap := make(map[int64]bool)

			for i := 0; i < totalMsgs; i++ {
				// Create large payload to force segment rotation
				payload := make([]byte, msgSize)
				for j := 0; j < msgSize; j++ {
					payload[j] = byte(i % 256)
				}

				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId
				segmentMap[result.LogMessageId.SegmentId] = true
			}

			// Get all segments and verify we have multiple
			segments, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Sort segment IDs for better readability in test output
			var segmentIds []int64
			for segId := range segments {
				segmentIds = append(segmentIds, segId)
			}
			sort.Slice(segmentIds, func(i, j int) bool {
				return segmentIds[i] < segmentIds[j]
			})

			t.Logf("Created %d segments: %v", len(segmentIds), segmentIds)
			assert.GreaterOrEqual(t, len(segmentIds), 5, "Expected at least 5 segments to be created")

			// Find the truncation point - we want to truncate the first two segments
			truncatePoint := writtenIds[0]
			for _, id := range writtenIds {
				if id.SegmentId == segmentIds[1] { // Get an entry in the second segment
					truncatePoint = id
					break
				}
			}

			t.Logf("Truncating at segmentId=%d, entryId=%d", truncatePoint.SegmentId, truncatePoint.EntryId)

			// 2. Truncate at the selected point which should include the first two segments
			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// Verify truncation
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			// 3. Close the writer
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			t.Log("Closed first writer")

			// Get segments after truncation
			segmentsAfterTruncation, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Check which segments are marked as truncated
			truncatedSegments := 0
			for segId, segMeta := range segmentsAfterTruncation {
				if segMeta.Metadata.State == proto.SegmentState_Truncated {
					t.Logf("Segment %d is now marked as truncated", segId)
					truncatedSegments++
				}
			}

			// 4. Open a new writer
			newLogWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)
			t.Log("Opened new writer after truncation")

			// 5. Write a new message to force a new segment
			newPayload := make([]byte, msgSize)
			for j := 0; j < msgSize; j++ {
				newPayload[j] = byte(totalMsgs % 256) // Use a different pattern
			}

			result := newLogWriter.Write(context.Background(), &log.WriteMessage{
				Payload:    newPayload,
				Properties: map[string]string{"index": fmt.Sprintf("%d", totalMsgs)},
			})
			assert.NoError(t, result.Err)
			t.Logf("Written new message after truncation: segmentId=%d, entryId=%d",
				result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

			// 6. Verify the new segment ID is as expected (should be max segment ID + 1)
			maxSegId := int64(0)
			for segId := range segmentMap {
				if segId > maxSegId {
					maxSegId = segId
				}
			}

			t.Logf("Maximum segment ID before new write: %d", maxSegId)
			t.Logf("New segment ID: %d", result.LogMessageId.SegmentId)

			// The new segment ID should be consecutive to the highest previous segment ID
			assert.Equal(t, maxSegId+1, result.LogMessageId.SegmentId,
				"New segment ID should be consecutive to previous max segment ID")

			// 7. Get all segments again to verify the new segment was created
			finalSegments, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			var finalSegmentIds []int64
			for segId := range finalSegments {
				finalSegmentIds = append(finalSegmentIds, segId)
			}
			sort.Slice(finalSegmentIds, func(i, j int) bool {
				return finalSegmentIds[i] < finalSegmentIds[j]
			})

			t.Logf("Final segments: %v", finalSegmentIds)
			assert.Contains(t, finalSegmentIds, result.LogMessageId.SegmentId,
				"New segment should be in the final segment list")
			assert.Equal(t, finalSegmentIds[len(finalSegmentIds)-1], result.LogMessageId.SegmentId,
				"New segment should be in the last segment")

			// 8. Close everything
			err = newLogWriter.Close(context.Background())
			assert.NoError(t, err)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestTruncateAndReopenClient(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTruncateAndReopenClient")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Initialize client with small segment size to force multiple segments
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB
			cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 5               // 5 seconds, This means that as long as the waiting time exceeds this period and the auditor clearance time, truncated segment should be cleared

			// First client instance
			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_reopen_client_test_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Write enough messages to create multiple segments
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages with enough data to force segment rotation
			const totalMsgs = 30
			const msgSize = 1024 // 1KB per message
			writtenIds := make([]*log.LogMessageId, totalMsgs)

			// Track segments seen
			segmentMap := make(map[int64]bool)

			for i := 0; i < totalMsgs; i++ {
				// Create large payload to force segment rotation
				payload := make([]byte, msgSize)
				for j := 0; j < msgSize; j++ {
					payload[j] = byte(i % 256)
				}

				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId
				segmentMap[result.LogMessageId.SegmentId] = true
			}

			// Get all segments and verify we have multiple
			segments, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Sort segment IDs for better readability in test output
			var segmentIds []int64
			for segId := range segments {
				segmentIds = append(segmentIds, segId)
			}
			sort.Slice(segmentIds, func(i, j int) bool {
				return segmentIds[i] < segmentIds[j]
			})

			t.Logf("Created %d segments: %v", len(segmentIds), segmentIds)
			assert.GreaterOrEqual(t, len(segmentIds), 5, "Expected at least 5 segments to be created")

			// Find the truncation point - we want to truncate the first two three segments
			truncatePoint := writtenIds[0]
			for _, id := range writtenIds {
				if id.SegmentId == segmentIds[2] { // Get an entry in the third segment
					truncatePoint = id
					break
				}
			}

			t.Logf("Truncating at segmentId=%d, entryId=%d", truncatePoint.SegmentId, truncatePoint.EntryId)

			// 2. Truncate at the selected point which should include the first two segments
			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// Verify truncation
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			// wait auditor to mark segment as truncated and cleanup
			time.Sleep(20 * time.Second)

			// Get segments after truncation
			segmentsAfterTruncation, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Check which segments are marked as truncated
			for segId, segMeta := range segmentsAfterTruncation {
				if segMeta.Metadata.State == proto.SegmentState_Truncated {
					t.Logf("Segment %d is now marked as truncated", segId)
				} else {
					t.Logf("Segment %d is now %v", segId, segMeta.Metadata.State)
				}
			}
			assert.Equal(t, len(segmentsAfterTruncation), len(segmentMap)-2, "Expected cleanup 2 segments, which 0,1")

			// Identify the maximum segment ID
			maxSegId := int64(0)
			for segId := range segmentMap {
				if segId > maxSegId {
					maxSegId = segId
				}
			}
			t.Logf("Maximum segment ID before shutdown: %d", maxSegId)

			// 3. Close writer and logHandle
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			t.Log("Closed writer")

			// 4. Shutdown client entirely
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
			t.Log("Shutdown first client")

			// 5. Create a new client instance
			newClient, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)
			t.Log("Created new client")

			// Open the same log again
			newLogHandle, err := newClient.OpenLog(context.Background(), logName)
			assert.NoError(t, err)
			t.Log("Reopened log with new client")

			// Get segments after reopening - verify they match what we had before
			reopenedSegments, err := newLogHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Verify reopened segments are consistent with segments after truncation
			assert.Equal(t, len(segmentsAfterTruncation), len(reopenedSegments),
				"Number of segments should be the same after reopening client")

			// Check that truncated segments are still marked as truncated
			for segId, segMeta := range reopenedSegments {
				if segId <= truncatePoint.SegmentId && segId != truncatePoint.SegmentId {
					assert.Equal(t, proto.SegmentState_Truncated, segMeta.Metadata.State,
						"Segment %d should still be marked as truncated after client restart", segId)
				}
			}

			// 6. Open a new writer with the new logHandle
			newLogWriter, err := newLogHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)
			t.Log("Opened new writer after client restart and truncation")

			// 7. Write a new message to force a new segment
			newPayload := make([]byte, msgSize)
			for j := 0; j < msgSize; j++ {
				newPayload[j] = byte(totalMsgs % 256) // Use a different pattern
			}

			result := newLogWriter.Write(context.Background(), &log.WriteMessage{
				Payload:    newPayload,
				Properties: map[string]string{"index": fmt.Sprintf("%d", totalMsgs)},
			})
			assert.NoError(t, result.Err)
			t.Logf("Written new message after client restart and truncation: segmentId=%d, entryId=%d",
				result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

			// 8. Verify the new segment ID is as expected (should be max segment ID + 1)
			t.Logf("New segment ID: %d", result.LogMessageId.SegmentId)

			// The new segment ID should be consecutive to the highest previous segment ID
			assert.Equal(t, maxSegId+1, result.LogMessageId.SegmentId,
				"New segment ID should be consecutive to previous max segment ID")

			// 9. Get all segments again to verify the new segment was created
			finalSegments, err := newLogHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			var finalSegmentIds []int64
			for segId := range finalSegments {
				finalSegmentIds = append(finalSegmentIds, segId)
			}
			sort.Slice(finalSegmentIds, func(i, j int) bool {
				return finalSegmentIds[i] < finalSegmentIds[j]
			})

			t.Logf("Final segments: %v", finalSegmentIds)
			assert.Contains(t, finalSegmentIds, result.LogMessageId.SegmentId,
				"New segment should be in the final segment list")
			assert.Equal(t, finalSegmentIds[len(finalSegmentIds)-1], result.LogMessageId.SegmentId,
				"New segment should be in the last segment")

			// 10. Close everything
			err = newLogWriter.Close(context.Background())
			assert.NoError(t, err)

			// Stop embed LogStore singleton
			stopEmbedLogStoreErr = woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestTruncateAndReopenClientWithinTTLProtected(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTruncateAndReopenClient")
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
			storageType: "", // Use default storage type minio-compatible
			rootPath:    "", // No need to specify path when using default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Initialize client with small segment size to force multiple segments
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Set small segment rolling policy to force multiple segments
			cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB
			cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 3600            // 1h, indicates that the truncated segment has not been cleared due to TTL protection

			// First client instance
			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create log
			logName := "truncate_reopen_client_test_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Write enough messages to create multiple segments
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages with enough data to force segment rotation
			const totalMsgs = 30
			const msgSize = 1024 // 1KB per message
			writtenIds := make([]*log.LogMessageId, totalMsgs)

			// Track segments seen
			segmentMap := make(map[int64]bool)

			for i := 0; i < totalMsgs; i++ {
				// Create large payload to force segment rotation
				payload := make([]byte, msgSize)
				for j := 0; j < msgSize; j++ {
					payload[j] = byte(i % 256)
				}

				result := logWriter.Write(context.Background(), &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId
				segmentMap[result.LogMessageId.SegmentId] = true
			}

			// Get all segments and verify we have multiple
			segments, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Sort segment IDs for better readability in test output
			var segmentIds []int64
			for segId := range segments {
				segmentIds = append(segmentIds, segId)
			}
			sort.Slice(segmentIds, func(i, j int) bool {
				return segmentIds[i] < segmentIds[j]
			})

			t.Logf("Created %d segments: %v", len(segmentIds), segmentIds)
			assert.GreaterOrEqual(t, len(segmentIds), 5, "Expected at least 5 segments to be created")

			// Find the truncation point - we want to truncate the first two three segments
			truncatePoint := writtenIds[0]
			for _, id := range writtenIds {
				if id.SegmentId == segmentIds[2] { // Get an entry in the third segment
					truncatePoint = id
					break
				}
			}

			t.Logf("Truncating at segmentId=%d, entryId=%d", truncatePoint.SegmentId, truncatePoint.EntryId)

			// 2. Truncate at the selected point which should include the first two segments
			err = logHandle.Truncate(context.Background(), truncatePoint)
			assert.NoError(t, err)

			// Verify truncation
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, truncatePoint.SegmentId, truncatedId.SegmentId)
			assert.Equal(t, truncatePoint.EntryId, truncatedId.EntryId)

			// wait auditor to mark segment as truncated and cleanup
			time.Sleep(20 * time.Second)

			// Get segments after truncation
			segmentsAfterTruncation, err := logHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Check which segments are marked as truncated
			for segId, segMeta := range segmentsAfterTruncation {
				if segMeta.Metadata.State == proto.SegmentState_Truncated {
					t.Logf("Segment %d is now marked as truncated", segId)
				} else {
					t.Logf("Segment %d is now %v", segId, segMeta.Metadata.State)
				}
			}
			assert.Equal(t, len(segmentsAfterTruncation), len(segmentMap), "Expected cleanup no truncated segment has been cleared")

			// Identify the maximum segment ID
			maxSegId := int64(0)
			for segId := range segmentMap {
				if segId > maxSegId {
					maxSegId = segId
				}
			}
			t.Logf("Maximum segment ID before shutdown: %d", maxSegId)

			// 3. Close writer and logHandle
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			t.Log("Closed writer")

			// 4. Shutdown client entirely
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
			t.Log("Shutdown first client")

			// 5. Create a new client instance
			newClient, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)
			t.Log("Created new client")

			// Open the same log again
			newLogHandle, err := newClient.OpenLog(context.Background(), logName)
			assert.NoError(t, err)
			t.Log("Reopened log with new client")

			// Get segments after reopening - verify they match what we had before
			reopenedSegments, err := newLogHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			// Verify reopened segments are consistent with segments after truncation
			assert.Equal(t, len(segmentsAfterTruncation), len(reopenedSegments),
				"Number of segments should be the same after reopening client")

			// Check that truncated segments are still marked as truncated
			for segId, segMeta := range reopenedSegments {
				if segId <= truncatePoint.SegmentId && segId != truncatePoint.SegmentId {
					assert.Equal(t, proto.SegmentState_Truncated, segMeta.Metadata.State,
						"Segment %d should still be marked as truncated after client restart", segId)
				}
			}

			// 6. Open a new writer with the new logHandle
			newLogWriter, err := newLogHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)
			t.Log("Opened new writer after client restart and truncation")

			// 7. Write a new message to force a new segment
			newPayload := make([]byte, msgSize)
			for j := 0; j < msgSize; j++ {
				newPayload[j] = byte(totalMsgs % 256) // Use a different pattern
			}

			result := newLogWriter.Write(context.Background(), &log.WriteMessage{
				Payload:    newPayload,
				Properties: map[string]string{"index": fmt.Sprintf("%d", totalMsgs)},
			})
			assert.NoError(t, result.Err)
			t.Logf("Written new message after client restart and truncation: segmentId=%d, entryId=%d",
				result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

			// 8. Verify the new segment ID is as expected (should be max segment ID + 1)
			t.Logf("New segment ID: %d", result.LogMessageId.SegmentId)

			// The new segment ID should be consecutive to the highest previous segment ID
			assert.Equal(t, maxSegId+1, result.LogMessageId.SegmentId,
				"New segment ID should be consecutive to previous max segment ID")

			// 9. Get all segments again to verify the new segment was created
			finalSegments, err := newLogHandle.GetSegments(context.Background())
			assert.NoError(t, err)

			var finalSegmentIds []int64
			for segId := range finalSegments {
				finalSegmentIds = append(finalSegmentIds, segId)
			}
			sort.Slice(finalSegmentIds, func(i, j int) bool {
				return finalSegmentIds[i] < finalSegmentIds[j]
			})

			t.Logf("Final segments: %v", finalSegmentIds)
			assert.Contains(t, finalSegmentIds, result.LogMessageId.SegmentId,
				"New segment should be in the final segment list")
			assert.Equal(t, finalSegmentIds[len(finalSegmentIds)-1], result.LogMessageId.SegmentId,
				"New segment should be in the last segment")

			// 10. Close everything
			err = newLogWriter.Close(context.Background())
			assert.NoError(t, err)

			// Stop embed LogStore singleton
			stopEmbedLogStoreErr = woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}
