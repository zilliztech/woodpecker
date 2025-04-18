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

			// Create log
			logName := "truncate_test_log_" + time.Now().Format("20060102150405")
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
				result := logWriter.Write(context.Background(), &log.WriterMessage{
					Payload:    []byte(fmt.Sprintf("message-%d", i)),
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId
				fmt.Printf("Written message %d: segmentId=%d, entryId=%d\n",
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

			// Create log
			logName := "truncate_test_concurrent_" + time.Now().Format("20060102150405")
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
					result := logWriter.Write(context.Background(), &log.WriterMessage{
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

			// Verification: should have read exactly 15 messages (5 before + 10 after truncation)
			assert.Equal(t, pauseReadPoint+(totalMessages-truncatePoint), len(readMessages),
				"Should have read 5 messages before truncation + messages after truncation point")

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
			for i := truncatePoint; i < totalMessages; i++ {
				assert.Contains(t, indices, i, "Should have read message with index %d", i)
			}
			for i := pauseReadPoint; i < truncatePoint; i++ {
				assert.NotContains(t, indices, i, "Should not have read truncated message with index %d", i)
			}

			// Final verification
			truncatedId, err := logHandle.GetTruncatedRecordId(context.Background())
			assert.NoError(t, err)
			t.Logf("Final truncation point: segmentId=%d, entryId=%d",
				truncatedId.SegmentId, truncatedId.EntryId)
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
			cfg, err := config.NewConfiguration()
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
			logName := "truncate_test_multi_segment_" + time.Now().Format("20060102150405")
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

				result := logWriter.Write(context.Background(), &log.WriterMessage{
					Payload:    payload,
					Properties: map[string]string{"index": fmt.Sprintf("%d", i)},
				})
				assert.NoError(t, result.Err)
				writtenIds[i] = result.LogMessageId

				// Track segments
				segmentsSeen[result.LogMessageId.SegmentId] = true

				// Print progress and segment info
				if i%2 == 0 {
					fmt.Printf("Written %d messages, currently in segment %d\n",
						i, result.LogMessageId.SegmentId)
				}
			}

			// Verify we've got multiple segments
			numSegments := len(segmentsSeen)
			fmt.Printf("Created %d segments\n", numSegments)
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

			fmt.Printf("Truncating at message with segmentId=%d, entryId=%d\n",
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
		})
	}
}
