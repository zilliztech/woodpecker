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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestOpenWriterMultiTimesInSingleClient(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestOpenWriterMultiTimesInSingleClient")
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

			// CreateLog if not exists
			logName := "test_log_single_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(context.Background(), logName)
			if createErr != nil {
				assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
			}
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			assert.NoError(t, openErr)

			logWriter1, openWriterErr1 := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr1)
			assert.NotNil(t, logWriter1)
			logWriter2, openWriterErr2 := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr2)
			assert.NotNil(t, logWriter2)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestOpenWriterMultiTimesInMultiClient(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestOpenWriterMultiTimesInMultiClient")
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
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}
			client1, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)
			client2, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			logName := "test_log_multi_" + tc.name + time.Now().Format("20060102150405")
			createErr := client1.CreateLog(context.Background(), logName)
			if createErr != nil {
				assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
			}

			logHandle1, openErr := client1.OpenLog(context.Background(), logName)
			assert.NoError(t, openErr)
			logHandle2, openErr := client2.OpenLog(context.Background(), logName)
			assert.NoError(t, openErr)

			// client1 get writer, client2 get fail
			logWriter1, openWriterErr1 := logHandle1.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr1)
			assert.NotNil(t, logWriter1)
			logWriter2, openWriterErr2 := logHandle2.OpenLogWriter(context.Background())
			assert.Error(t, openWriterErr2)
			assert.Nil(t, logWriter2)

			// client1 release, client 2 get writer
			releaseErr := logWriter1.Close(context.Background())
			assert.NoError(t, releaseErr)
			logWriter3, openWriterErr3 := logHandle2.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr3)
			assert.NotNil(t, logWriter3)

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestRepeatedOpenCloseWriterAndReader(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestRepeatedOpenCloseWriterAndReader")
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
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_repeated_open_close" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			if createErr != nil {
				assert.True(t, errors.IsAny(createErr, werr.ErrMetadataCreateLog, werr.ErrLogHandleLogAlreadyExists))
			}

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			// Loop 3 times, each iteration:
			// 1. Open writer, write data, close writer
			// 2. Open empty writer, don't write, close writer
			totalWrittenMessages := 0

			for i := 0; i < 3; i++ {
				t.Logf("=== Cycle %d ===", i+1)

				// Part 1: Open writer and write data
				t.Logf("Cycle %d: Opening writer with data", i+1)
				writer, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)
				assert.NotNil(t, writer)

				// Write 5 messages
				for j := 0; j < 5; j++ {
					message := &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("Cycle %d, message %d", i+1, j)),
						Properties: map[string]string{
							"cycle": fmt.Sprintf("%d", i+1),
							"index": fmt.Sprintf("%d", j),
						},
					}

					result := writer.Write(ctx, message)
					assert.NoError(t, result.Err)
					assert.NotNil(t, result.LogMessageId)
					totalWrittenMessages++

					t.Logf("Written message %d with ID: segment=%d, entry=%d",
						totalWrittenMessages, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
				}

				// Close writer with data
				err = writer.Close(ctx)
				assert.NoError(t, err)
				t.Logf("Cycle %d: Writer with data closed successfully after writing 5 messages", i+1)

				// Wait a moment to ensure operations complete
				time.Sleep(100 * time.Millisecond)

				// Part 2: Open empty writer and close without writing
				t.Logf("Cycle %d: Opening empty writer without data", i+1)
				emptyWriter, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)
				assert.NotNil(t, emptyWriter)

				// Close empty writer without writing
				err = emptyWriter.Close(ctx)
				assert.NoError(t, err)
				t.Logf("Cycle %d: Empty writer closed without writing", i+1)

				// Wait a moment to ensure operations complete
				time.Sleep(100 * time.Millisecond)
			}

			t.Logf("Total messages written: %d (expected to be 15)", totalWrittenMessages)

			// Open reader and read all data
			t.Log("Opening reader to verify all messages")
			startPoint := &log.LogMessageId{
				SegmentId: 0,
				EntryId:   0,
			}

			reader, err := logHandle.OpenLogReader(ctx, startPoint, "verification-reader")
			assert.NoError(t, err)
			assert.NotNil(t, reader)

			// Read and verify all messages
			var readCount int
			var lastMessage *log.LogMessage

			for readCount < totalWrittenMessages {
				readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				message, err := reader.ReadNext(readCtx)
				cancel()

				if err != nil {
					t.Logf("Error reading message %d: %v", readCount+1, err)
					break
				}

				if message == nil {
					t.Logf("Received nil message at position %d", readCount+1)
					break
				}

				readCount++
				lastMessage = message

				// Log every message (since there are only 15 in total)
				t.Logf("Read message %d: %s, properties: %v",
					readCount, string(message.Payload), message.Properties)
			}

			// Verify we read all expected messages
			assert.Equal(t, totalWrittenMessages, readCount, "Should read exactly the number of messages written")

			if lastMessage != nil {
				t.Logf("Last message read: %s, properties: %v",
					string(lastMessage.Payload), lastMessage.Properties)

				// The last message should be from the last cycle
				assert.Contains(t, string(lastMessage.Payload), "Cycle 3")
			}

			// Try to read one more - should timeout or error
			readExtraCtx, cancelExtra := context.WithTimeout(ctx, 500*time.Millisecond)
			extraMessage, err := reader.ReadNext(readExtraCtx)
			cancelExtra()

			if err != nil {
				t.Log("No extra messages as expected (timeout/error)")
			} else if extraMessage == nil {
				t.Log("No extra messages as expected (nil response)")
			} else {
				t.Logf("Unexpected extra message: %s", string(extraMessage.Payload))
				t.Fail()
			}

			// Close reader
			err = reader.Close(ctx)
			assert.NoError(t, err)
			t.Log("Reader closed successfully")

			// Close client
			err = client.Close(context.TODO())
			assert.NoError(t, err)
			t.Log("Client closed successfully")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestWriterCloseWithoutWrite(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWriterCloseWithoutWrite")
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
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Create a new embed client
			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_writer_close_without_write" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			if createErr != nil {
				assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
			}

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			// Initialize message counter
			totalWrittenMessages := 0

			// Run 2 cycles of: write 100 messages, then open a writer without writing
			for cycle := 0; cycle < 2; cycle++ {
				t.Logf("=== Cycle %d ===", cycle+1)

				// Step 1: Open a writer and write 100 messages
				t.Logf("Cycle %d: Opening writer with data", cycle+1)
				writerWithData, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)
				assert.NotNil(t, writerWithData)

				// Write 20 messages
				for j := 0; j < 20; j++ {
					message := &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("Cycle %d, data message %d", cycle+1, j)),
						Properties: map[string]string{
							"cycle": fmt.Sprintf("%d", cycle+1),
							"index": fmt.Sprintf("%d", j),
							"type":  "data",
						},
					}

					result := writerWithData.Write(ctx, message)
					assert.NoError(t, result.Err)
					assert.NotNil(t, result.LogMessageId)
					totalWrittenMessages++

					// Log only certain messages to reduce output volume
					if j%5 == 0 || j == 19 {
						t.Logf("Written message %d with ID: segment=%d, entry=%d",
							totalWrittenMessages, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
					}
				}

				// Close writer with data
				err = writerWithData.Close(ctx)
				assert.NoError(t, err)
				t.Logf("Cycle %d: Writer with data closed successfully after writing 20 messages", cycle+1)

				// Wait a moment to ensure operations complete
				time.Sleep(500 * time.Millisecond)

				// Step 2: Open a writer but don't write anything
				t.Logf("Cycle %d: Opening writer without data", cycle+1)
				emptyWriter, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)
				assert.NotNil(t, emptyWriter)

				// Close without writing
				err = emptyWriter.Close(ctx)
				assert.NoError(t, err)
				t.Logf("Cycle %d: Empty writer closed without writing", cycle+1)

				// Wait a moment to ensure operations complete
				time.Sleep(500 * time.Millisecond)
			}

			// Log the total messages written
			t.Logf("Total messages written: %d", totalWrittenMessages)

			// Open reader and verify messages
			t.Log("Opening reader to verify all messages")
			startPoint := &log.LogMessageId{
				SegmentId: 0,
				EntryId:   0,
			}

			reader, err := logHandle.OpenLogReader(ctx, startPoint, "verification-reader")
			assert.NoError(t, err)
			assert.NotNil(t, reader)

			// Read and verify first message
			t.Log("Verifying first message...")
			readCtx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
			readMsg1, err := reader.ReadNext(readCtx1)
			cancel1()
			if err != nil {
				t.Logf("Error reading first message: %v", err)
			} else if readMsg1 == nil {
				t.Log("First message is nil, unexpected")
			} else {
				t.Logf("First message: %s, properties: %v",
					string(readMsg1.Payload), readMsg1.Properties)
				assert.Contains(t, string(readMsg1.Payload), "Cycle 1")
			}

			// Count the total messages we can read
			var readCount int = 1 // Already read the first message
			var lastReadMsg *log.LogMessage = readMsg1

			// Read with a timeout for the entire operation
			readTimeoutCtx, readTimeoutCancel := context.WithTimeout(ctx, 30*time.Second)
			defer readTimeoutCancel()

			for {
				readCtx, cancel := context.WithTimeout(readTimeoutCtx, 2*time.Second)
				msg, err := reader.ReadNext(readCtx)
				cancel()

				if err != nil {
					t.Logf("Error at message %d: %v - stopping read", readCount+1, err)
					break
				}
				if msg == nil {
					t.Logf("Received nil message at position %d - stopping read", readCount+1)
					break
				}

				lastReadMsg = msg
				readCount++

				// Log milestone messages
				if readCount == 20 || readCount == 40 || readCount%10 == 0 {
					t.Logf("Message %d: %s, properties: %v",
						readCount, string(msg.Payload), msg.Properties)
				}

				// Check if we've read all expected messages (with some buffer)
				if readCount >= totalWrittenMessages+10 {
					t.Logf("Read more messages than expected (%d > %d) - stopping", readCount, totalWrittenMessages)
					break
				}

				// Check if the timeout context is done
				select {
				case <-readTimeoutCtx.Done():
					t.Log("Timeout while reading messages")
					break
				default:
					// Continue
				}
			}

			if lastReadMsg != nil {
				t.Logf("Last successfully read message: %s, properties: %v",
					string(lastReadMsg.Payload), lastReadMsg.Properties)
			}

			// Close reader
			err = reader.Close(ctx)
			assert.NoError(t, err)
			t.Log("Reader closed successfully")

			// Compare written vs read message counts
			expectedReadCount := totalWrittenMessages
			if readCount == expectedReadCount {
				t.Logf("PASS: Successfully read exactly the expected number of messages: %d", readCount)
			} else if readCount < expectedReadCount {
				t.Logf("WARN: Read fewer messages than expected: %d < %d", readCount, expectedReadCount)
			} else {
				t.Logf("WARN: Read more messages than expected: %d > %d", readCount, expectedReadCount)
			}

			// Close client
			err = client.Close(context.TODO())
			assert.NoError(t, err)
			t.Log("Client closed successfully")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestClientRecreation(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestClientRecreation")
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
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			logName := "test_client_recreation" + tc.name + time.Now().Format("20060102150405")

			// First client lifecycle
			{
				// Create first client
				client1, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)

				// Create a log or use existing one
				createErr := client1.CreateLog(ctx, logName)
				if createErr != nil {
					assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
				}

				// Open the log
				logHandle, err := client1.OpenLog(ctx, logName)
				assert.NoError(t, err)

				// Open a writer and write data
				writer, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)

				// Write some data with the first client
				message := &log.WriteMessage{
					Payload:    []byte("First client message"),
					Properties: map[string]string{"client": "first"},
				}

				result := writer.Write(ctx, message)
				assert.NoError(t, result.Err)

				// Close writer
				err = writer.Close(ctx)
				assert.NoError(t, err)

				// Close the first client
				err = client1.Close(context.TODO())
				assert.NoError(t, err)

				t.Log("First client closed successfully")
			}

			// Wait a brief moment to ensure clean shutdown
			time.Sleep(500 * time.Millisecond)

			// Second client lifecycle
			{
				// Create second client
				client2, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)

				// Verify the log exists
				exists, err := client2.LogExists(ctx, logName)
				assert.NoError(t, err)
				assert.True(t, exists, "Log should still exist after client recreation")

				// Open the same log
				logHandle, err := client2.OpenLog(ctx, logName)
				assert.NoError(t, err)

				// Open a writer and write more data
				writer, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)

				// Write some data with the second client
				message := &log.WriteMessage{
					Payload:    []byte("Second client message"),
					Properties: map[string]string{"client": "second"},
				}

				result := writer.Write(ctx, message)
				assert.NoError(t, result.Err)

				// Close writer
				err = writer.Close(ctx)
				assert.NoError(t, err)

				// Now read all messages to verify both writes succeeded
				startPoint := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   0,
				}

				reader, err := logHandle.OpenLogReader(ctx, startPoint, "recreation-test-reader")
				assert.NoError(t, err)

				// Read first message
				readCtx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
				message1, err := reader.ReadNext(readCtx1)
				cancel1()
				assert.NoError(t, err)
				assert.NotNil(t, message1)

				if message1 != nil {
					t.Logf("First message: %s, properties: %v", string(message1.Payload), message1.Properties)
					assert.Equal(t, "First client message", string(message1.Payload))
					assert.Equal(t, "first", message1.Properties["client"])
				}

				// Read second message
				readCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
				message2, err := reader.ReadNext(readCtx2)
				cancel2()
				assert.NoError(t, err)
				assert.NotNil(t, message2)

				if message2 != nil {
					t.Logf("Second message: %s, properties: %v", string(message2.Payload), message2.Properties)
					assert.Equal(t, "Second client message", string(message2.Payload))
					assert.Equal(t, "second", message2.Properties["client"])
				}

				// Close reader
				err = reader.Close(ctx)
				assert.NoError(t, err)

				// Close the second client
				err = client2.Close(context.TODO())
				assert.NoError(t, err)

				t.Log("Second client closed successfully")
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}

}

func TestClientRecreationWithManagedCli(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestClientRecreationWithManagedCli")
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
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			logName := "test_client_recreation_with_managed_clients" + tc.name + time.Now().Format("20060102150405")
			// First client lifecycle
			{
				// Create first client
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				var storageCli storageclient.ObjectStorage
				if cfg.Woodpecker.Storage.IsStorageMinio() {
					storageCli, err = storageclient.NewObjectStorage(ctx, cfg)
					assert.NoError(t, err)
				}
				client1, err := woodpecker.NewEmbedClient(ctx, cfg, etcdCli, storageCli, true)
				assert.NoError(t, err)

				// Create a log or use existing one
				createErr := client1.CreateLog(ctx, logName)
				if createErr != nil {
					assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
				}

				// Open the log
				logHandle, err := client1.OpenLog(ctx, logName)
				assert.NoError(t, err)

				// Open a writer and write data
				writer, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)

				// Write some data with the first client
				message := &log.WriteMessage{
					Payload:    []byte("First client message"),
					Properties: map[string]string{"client": "first"},
				}

				result := writer.Write(ctx, message)
				assert.NoError(t, result.Err)

				// Close writer
				err = writer.Close(ctx)
				assert.NoError(t, err)

				// Close the first client
				err = client1.Close(context.TODO())
				assert.NoError(t, err)

				t.Log("First client closed successfully")
			}

			// Wait a brief moment to ensure clean shutdown
			time.Sleep(500 * time.Millisecond)

			// Second client lifecycle
			{
				// Create second client
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				var storageCli storageclient.ObjectStorage
				if cfg.Woodpecker.Storage.IsStorageMinio() {
					storageCli, err = storageclient.NewObjectStorage(ctx, cfg)
					assert.NoError(t, err)
				}
				client2, err := woodpecker.NewEmbedClient(ctx, cfg, etcdCli, storageCli, true)
				assert.NoError(t, err)

				// Verify the log exists
				exists, err := client2.LogExists(ctx, logName)
				assert.NoError(t, err)
				assert.True(t, exists, "Log should still exist after client recreation")

				// Open the same log
				logHandle, err := client2.OpenLog(ctx, logName)
				assert.NoError(t, err)

				// Open a writer and write more data
				writer, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)

				// Write some data with the second client
				message := &log.WriteMessage{
					Payload:    []byte("Second client message"),
					Properties: map[string]string{"client": "second"},
				}

				result := writer.Write(ctx, message)
				assert.NoError(t, result.Err)

				// Close writer
				err = writer.Close(ctx)
				assert.NoError(t, err)

				// Now read all messages to verify both writes succeeded
				startPoint := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   0,
				}

				reader, err := logHandle.OpenLogReader(ctx, startPoint, "recreation-test-reader")
				assert.NoError(t, err)

				// Read first message
				readCtx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
				message1, err := reader.ReadNext(readCtx1)
				cancel1()
				assert.NoError(t, err)
				assert.NotNil(t, message1)

				if message1 != nil {
					t.Logf("First message: %s, properties: %v", string(message1.Payload), message1.Properties)
					assert.Equal(t, "First client message", string(message1.Payload))
					assert.Equal(t, "first", message1.Properties["client"])
				}

				// Read second message
				readCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
				message2, err := reader.ReadNext(readCtx2)
				cancel2()
				assert.NoError(t, err)
				assert.NotNil(t, message2)

				if message2 != nil {
					t.Logf("Second message: %s, properties: %v", string(message2.Payload), message2.Properties)
					assert.Equal(t, "Second client message", string(message2.Payload))
					assert.Equal(t, "second", message2.Properties["client"])
				}

				// Close reader
				err = reader.Close(ctx)
				assert.NoError(t, err)

				// Close the second client
				err = client2.Close(context.TODO())
				assert.NoError(t, err)

				t.Log("Second client closed successfully")
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestMultiClientOpenCloseWriteRead(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMultiClientOpenCloseWriteRead")
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

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_multi_client_open_close" + tc.name + time.Now().Format("20060102150405")
			totalWrittenMessages := 0

			// Loop 3 times, each iteration:
			// 1. Create new client, open writer, write data, close writer and client
			// 2. Create new client, open empty writer, don't write, close writer and client
			for i := 0; i < 3; i++ {
				t.Logf("=== Cycle %d ===", i+1)

				// Part 1: Create client, open writer and write data
				t.Logf("Cycle %d: Creating client with data writer", i+1)

				// Create new client for writing data
				cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
				assert.NoError(t, err)
				if tc.storageType != "" {
					cfg.Woodpecker.Storage.Type = tc.storageType
				}
				if tc.rootPath != "" {
					cfg.Woodpecker.Storage.RootPath = tc.rootPath
				}
				dataClient, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)

				// Create log if first iteration, otherwise use existing
				if i == 0 {
					createErr := dataClient.CreateLog(ctx, logName)
					assert.NoError(t, createErr)
				}

				// Open log handle
				logHandle, err := dataClient.OpenLog(ctx, logName)
				assert.NoError(t, err)

				// Open writer
				writer, err := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)
				assert.NotNil(t, writer)

				// Write 5 messages
				for j := 0; j < 5; j++ {
					message := &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("Cycle %d, message %d", i+1, j)),
						Properties: map[string]string{
							"cycle":       fmt.Sprintf("%d", i+1),
							"index":       fmt.Sprintf("%d", j),
							"client_type": "data_writer",
						},
					}

					result := writer.Write(ctx, message)
					assert.NoError(t, result.Err)
					assert.NotNil(t, result.LogMessageId)
					totalWrittenMessages++

					t.Logf("Written message %d with ID: segment=%d, entry=%d",
						totalWrittenMessages, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
				}

				// Close writer
				err = writer.Close(ctx)
				assert.NoError(t, err)

				// Close client
				err = dataClient.Close(context.TODO())
				assert.NoError(t, err)
				t.Logf("Cycle %d: Client with data writer closed successfully after writing 5 messages", i+1)

				// Wait a moment to ensure operations complete
				time.Sleep(100 * time.Millisecond)

				// Part 2: Create client, open empty writer and close without writing
				t.Logf("Cycle %d: Creating client with empty writer", i+1)

				// Create new client for empty writer
				emptyClientCfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
				assert.NoError(t, err)
				emptyClient, err := woodpecker.NewEmbedClientFromConfig(ctx, emptyClientCfg)
				assert.NoError(t, err)

				// Open log handle
				emptyLogHandle, err := emptyClient.OpenLog(ctx, logName)
				assert.NoError(t, err)

				// Open writer but don't write anything
				emptyWriter, err := emptyLogHandle.OpenLogWriter(ctx)
				assert.NoError(t, err)
				assert.NotNil(t, emptyWriter)

				// Close empty writer without writing
				err = emptyWriter.Close(ctx)
				assert.NoError(t, err)

				// Close client
				err = emptyClient.Close(context.TODO())
				assert.NoError(t, err)
				t.Logf("Cycle %d: Client with empty writer closed without writing", i+1)

				// Wait a moment to ensure operations complete
				time.Sleep(100 * time.Millisecond)
			}

			t.Logf("Total messages written: %d (expected to be 15)", totalWrittenMessages)

			// Create final client to read and verify all data
			t.Log("Creating reader client to verify all messages")
			readerCfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			readerClient, err := woodpecker.NewEmbedClientFromConfig(ctx, readerCfg)
			assert.NoError(t, err)

			// Open log handle
			readerLogHandle, err := readerClient.OpenLog(ctx, logName)
			assert.NoError(t, err)

			// Open reader from beginning
			startPoint := &log.LogMessageId{
				SegmentId: 0,
				EntryId:   0,
			}

			reader, err := readerLogHandle.OpenLogReader(ctx, startPoint, "verification-reader")
			assert.NoError(t, err)
			assert.NotNil(t, reader)

			// Read and verify all messages
			var readCount int
			var lastMessage *log.LogMessage

			for readCount < totalWrittenMessages {
				readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				message, err := reader.ReadNext(readCtx)
				cancel()

				if err != nil {
					t.Logf("Error reading message %d: %v", readCount+1, err)
					break
				}

				if message == nil {
					t.Logf("Received nil message at position %d", readCount+1)
					break
				}

				readCount++
				lastMessage = message

				// Log every message (since there are only 15 in total)
				t.Logf("Read message %d: %s, properties: %v",
					readCount, string(message.Payload), message.Properties)
			}

			// Verify we read all expected messages
			assert.Equal(t, totalWrittenMessages, readCount, "Should read exactly the number of messages written")

			if lastMessage != nil {
				t.Logf("Last message read: %s, properties: %v",
					string(lastMessage.Payload), lastMessage.Properties)

				// The last message should be from the last cycle
				assert.Contains(t, string(lastMessage.Payload), "Cycle 3")
			}

			// Try to read one more - should timeout or error
			readExtraCtx, cancelExtra := context.WithTimeout(ctx, 500*time.Millisecond)
			extraMessage, err := reader.ReadNext(readExtraCtx)
			cancelExtra()

			if err != nil {
				t.Log("No extra messages as expected (timeout/error)")
			} else if extraMessage == nil {
				t.Log("No extra messages as expected (nil response)")
			} else {
				t.Logf("Unexpected extra message: %s", string(extraMessage.Payload))
				t.Fail()
			}

			// Close reader
			err = reader.Close(ctx)
			assert.NoError(t, err)
			t.Log("Reader closed successfully")

			// Close client
			err = readerClient.Close(context.TODO())
			assert.NoError(t, err)
			t.Log("Reader client closed successfully")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestConcurrentWriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentWriteAndRead")
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

			// Number of overall test cycles to run
			const testCycles = 3
			// Number of messages to write in each cycle
			const messageCount = 20

			// Create a single test log with timestamp to ensure uniqueness
			logName := "test_concurrent_write_read_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Track total messages written for verification
			totalMessagesWritten := 0

			// Run the complete test multiple times on the same log
			for cycle := 0; cycle < testCycles; cycle++ {
				t.Logf("====== Starting Test Cycle %d/%d ======", cycle+1, testCycles)

				// Create channels to track completion and results
				writerDone := make(chan struct{})
				reader1Done := make(chan []string)
				reader2Done := make(chan []string)
				reader3Done := make(chan []string)
				reader4Done := make(chan []string)
				reader5Done := make(chan []string)
				reader6Done := make(chan []string)
				reader7Done := make(chan []string)
				reader8Done := make(chan []string)
				reader9Done := make(chan []string)
				reader10Done := make(chan []string)
				reader11Done := make(chan []string)
				reader12Done := make(chan []string)
				reader13Done := make(chan []string)
				reader14Done := make(chan []string)
				reader15Done := make(chan []string)
				reader16Done := make(chan []string)

				// Start writer goroutine
				go func() {
					defer close(writerDone)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					assert.NoError(t, err)
					defer writer.Close(ctx)

					t.Logf("Cycle %d - Writer: Started writing messages", cycle+1)

					// Write messages
					for i := 0; i < messageCount; i++ {
						msgIdx := i
						message := &log.WriteMessage{
							Payload: []byte(fmt.Sprintf("Cycle %d - Message %d", cycle+1, msgIdx)),
							Properties: map[string]string{
								"cycle": fmt.Sprintf("%d", cycle+1),
								"index": fmt.Sprintf("%d", msgIdx),
								"type":  "concurrent-test",
							},
						}

						result := writer.Write(ctx, message)
						assert.NoError(t, result.Err)

						// Log milestone messages
						if i%5 == 0 || msgIdx == messageCount-1 {
							t.Logf("Cycle %d - Writer: Written message %d with ID: segment=%d, entry=%d",
								cycle+1, msgIdx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay to allow readers to catch up
						time.Sleep(10 * time.Millisecond)
					}

					t.Logf("Cycle %d - Writer: Completed writing all messages", cycle+1)
				}()

				// Update total messages to be read in this cycle
				totalMessagesWritten += messageCount
				expectedMessagesThisCycle := totalMessagesWritten

				// Function to create a reader goroutine that reads all messages from the beginning
				createReader := func(readerName string, resultChan chan<- []string) {
					// Add a small delay to ensure writer has started
					time.Sleep(100 * time.Millisecond)

					// Always start reading from the beginning
					startPoint := &log.LogMessageId{
						SegmentId: 0,
						EntryId:   0,
					}

					reader, err := logHandle.OpenLogReader(ctx, startPoint, readerName)
					assert.NoError(t, err)
					defer reader.Close(ctx)

					t.Logf("Cycle %d - %s: Started reading from beginning, expecting %d total messages",
						cycle+1, readerName, expectedMessagesThisCycle)

					// Read messages with simple for loop and fixed timeout
					messages := make([]string, 0, expectedMessagesThisCycle)

					// Simple loop to read the expected number of messages
					for i := 0; i < expectedMessagesThisCycle; i++ {
						// Use a fixed timeout for each read
						readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						msg, err := reader.ReadNext(readCtx)
						cancel()

						if err != nil {
							t.Logf("Cycle %d - %s: Error reading message %d: %v",
								cycle+1, readerName, i, err)
							resultChan <- messages
							return
						}

						if msg == nil {
							t.Logf("Cycle %d - %s: Received nil message at position %d",
								cycle+1, readerName, i)
							resultChan <- messages
							return
						}

						messages = append(messages, string(msg.Payload))

						// Log milestone messages
						//if (i+1)%messageCount == 0 || (i+1)%10 == 0 || i == 0 || i == expectedMessagesThisCycle-1 {
						t.Logf("[%s] Cycle %d - %s: Read message %d/%d:  seg:%d,entry:%d  payload:%s",
							time.Now().Format("20060102150405"), cycle+1, readerName, i+1, expectedMessagesThisCycle, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
						//}
					}

					t.Logf("Cycle %d - %s: Completed reading all %d messages",
						cycle+1, readerName, len(messages))
					resultChan <- messages
				}

				// Start reader goroutines
				go createReader(fmt.Sprintf("Cycle%d-Reader1", cycle+1), reader1Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader2", cycle+1), reader2Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader3", cycle+1), reader3Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader4", cycle+1), reader4Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader5", cycle+1), reader5Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader6", cycle+1), reader6Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader7", cycle+1), reader7Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader8", cycle+1), reader8Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader9", cycle+1), reader9Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader10", cycle+1), reader10Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader11", cycle+1), reader11Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader12", cycle+1), reader12Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader13", cycle+1), reader13Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader14", cycle+1), reader14Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader15", cycle+1), reader15Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader16", cycle+1), reader16Done)

				// Wait for writer to complete
				<-writerDone
				t.Logf("Cycle %d - Writer goroutine completed", cycle+1)

				// Wait for readers to complete and collect results
				reader1Messages := <-reader1Done
				t.Logf("Cycle %d - Reader-1 completed with %d/%d messages", cycle+1, len(reader1Messages), expectedMessagesThisCycle)
				reader2Messages := <-reader2Done
				t.Logf("Cycle %d - Reader-2 completed with %d/%d messages", cycle+1, len(reader2Messages), expectedMessagesThisCycle)
				reader3Messages := <-reader3Done
				t.Logf("Cycle %d - Reader-3 completed with %d/%d messages", cycle+1, len(reader3Messages), expectedMessagesThisCycle)
				reader4Messages := <-reader4Done
				t.Logf("Cycle %d - Reader-4 completed with %d/%d messages", cycle+1, len(reader4Messages), expectedMessagesThisCycle)
				reader5Messages := <-reader5Done
				t.Logf("Cycle %d - Reader-5 completed with %d/%d messages", cycle+1, len(reader5Messages), expectedMessagesThisCycle)
				reader6Messages := <-reader6Done
				t.Logf("Cycle %d - Reader-6 completed with %d/%d messages", cycle+1, len(reader6Messages), expectedMessagesThisCycle)
				reader7Messages := <-reader7Done
				t.Logf("Cycle %d - Reader-7 completed with %d/%d messages", cycle+1, len(reader7Messages), expectedMessagesThisCycle)
				reader8Messages := <-reader8Done
				t.Logf("Cycle %d - Reader-8 completed with %d/%d messages", cycle+1, len(reader8Messages), expectedMessagesThisCycle)
				reader9Messages := <-reader9Done
				t.Logf("Cycle %d - Reader-9 completed with %d/%d messages", cycle+1, len(reader9Messages), expectedMessagesThisCycle)
				reader10Messages := <-reader10Done
				t.Logf("Cycle %d - Reader-10 completed with %d/%d messages", cycle+1, len(reader10Messages), expectedMessagesThisCycle)
				reader11Messages := <-reader11Done
				t.Logf("Cycle %d - Reader-11 completed with %d/%d messages", cycle+1, len(reader11Messages), expectedMessagesThisCycle)
				reader12Messages := <-reader12Done
				t.Logf("Cycle %d - Reader-12 completed with %d/%d messages", cycle+1, len(reader12Messages), expectedMessagesThisCycle)
				reader13Messages := <-reader13Done
				t.Logf("Cycle %d - Reader-13 completed with %d/%d messages", cycle+1, len(reader13Messages), expectedMessagesThisCycle)
				reader14Messages := <-reader14Done
				t.Logf("Cycle %d - Reader-14 completed with %d/%d messages", cycle+1, len(reader14Messages), expectedMessagesThisCycle)
				reader15Messages := <-reader15Done
				t.Logf("Cycle %d - Reader-15 completed with %d/%d messages", cycle+1, len(reader15Messages), expectedMessagesThisCycle)
				reader16Messages := <-reader16Done
				t.Logf("Cycle %d - Reader-16 completed with %d/%d messages", cycle+1, len(reader16Messages), expectedMessagesThisCycle)

				// Verify results - each reader should read all messages from all cycles so far
				assert.Equal(t, expectedMessagesThisCycle, len(reader1Messages), "Cycle %d - Reader-1 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader2Messages), "Cycle %d - Reader-2 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader3Messages), "Cycle %d - Reader-3 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader4Messages), "Cycle %d - Reader-4 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader5Messages), "Cycle %d - Reader-5 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader6Messages), "Cycle %d - Reader-6 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader7Messages), "Cycle %d - Reader-7 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader8Messages), "Cycle %d - Reader-8 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader9Messages), "Cycle %d - Reader-9 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader10Messages), "Cycle %d - Reader-10 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader11Messages), "Cycle %d - Reader-11 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader12Messages), "Cycle %d - Reader-12 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader13Messages), "Cycle %d - Reader-13 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader14Messages), "Cycle %d - Reader-14 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader15Messages), "Cycle %d - Reader-15 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader16Messages), "Cycle %d - Reader-16 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)

				// Verify message content - need to verify all messages from all cycles
				// First verify cycle 1's messages
				for cycleNum := 1; cycleNum <= cycle+1; cycleNum++ {
					t.Logf("Verifying messages from cycle %d", cycleNum)

					// Calculate the start and end indices for messages from this cycle
					startIdx := (cycleNum - 1) * messageCount

					for i := 0; i < messageCount; i++ {
						msgIdx := startIdx + i
						expectedMsg := fmt.Sprintf("Cycle %d - Message %d", cycleNum, i)

						// Verify in reader1
						if msgIdx < len(reader1Messages) {
							assert.Equal(t, expectedMsg, reader1Messages[msgIdx], "Cycle %d - Reader-1 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader2
						if msgIdx < len(reader2Messages) {
							assert.Equal(t, expectedMsg, reader2Messages[msgIdx], "Cycle %d - Reader-2 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader3
						if msgIdx < len(reader3Messages) {
							assert.Equal(t, expectedMsg, reader3Messages[msgIdx], "Cycle %d - Reader-3 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader4
						if msgIdx < len(reader4Messages) {
							assert.Equal(t, expectedMsg, reader4Messages[msgIdx], "Cycle %d - Reader-4 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader5
						if msgIdx < len(reader5Messages) {
							assert.Equal(t, expectedMsg, reader5Messages[msgIdx], "Cycle %d - Reader-5 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader6
						if msgIdx < len(reader6Messages) {
							assert.Equal(t, expectedMsg, reader6Messages[msgIdx], "Cycle %d - Reader-6 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader7
						if msgIdx < len(reader7Messages) {
							assert.Equal(t, expectedMsg, reader7Messages[msgIdx], "Cycle %d - Reader-7 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader8
						if msgIdx < len(reader8Messages) {
							assert.Equal(t, expectedMsg, reader8Messages[msgIdx], "Cycle %d - Reader-8 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader9
						if msgIdx < len(reader9Messages) {
							assert.Equal(t, expectedMsg, reader9Messages[msgIdx], "Cycle %d - Reader-9 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader10
						if msgIdx < len(reader10Messages) {
							assert.Equal(t, expectedMsg, reader10Messages[msgIdx], "Cycle %d - Reader-10 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader11
						if msgIdx < len(reader11Messages) {
							assert.Equal(t, expectedMsg, reader11Messages[msgIdx], "Cycle %d - Reader-11 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader12
						if msgIdx < len(reader12Messages) {
							assert.Equal(t, expectedMsg, reader12Messages[msgIdx], "Cycle %d - Reader-12 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader13
						if msgIdx < len(reader13Messages) {
							assert.Equal(t, expectedMsg, reader13Messages[msgIdx], "Cycle %d - Reader-13 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader14
						if msgIdx < len(reader14Messages) {
							assert.Equal(t, expectedMsg, reader14Messages[msgIdx], "Cycle %d - Reader-14 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader15
						if msgIdx < len(reader15Messages) {
							assert.Equal(t, expectedMsg, reader15Messages[msgIdx], "Cycle %d - Reader-15 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader16
						if msgIdx < len(reader16Messages) {
							assert.Equal(t, expectedMsg, reader16Messages[msgIdx], "Cycle %d - Reader-16 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
					}
				}

				// Add a short delay between cycles
				time.Sleep(500 * time.Millisecond)

				// Open empty writer then close
				emptyWriter, createEmptyWriterErr := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, createEmptyWriterErr)
				closeEmptyWriterErr := emptyWriter.Close(ctx)
				assert.NoError(t, closeEmptyWriterErr, fmt.Sprintf("%v", closeEmptyWriterErr))

				t.Logf("====== Completed Test Cycle %d/%d Successfully ======", cycle+1, testCycles)
			}
			t.Log("All test cycles completed successfully")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestConcurrentWriteAndReadWithSegmentRollingFrequently(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentWriteAndReadWithSegmentRollingFrequently")
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

			// Number of overall test cycles to run
			const testCycles = 3
			// Number of messages to write in each cycle
			const messageCount = 20

			// Create a single test log with timestamp to ensure uniqueness
			logName := "test_concurrent_write_read_rolling_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Track total messages written for verification
			totalMessagesWritten := 0

			// Run the complete test multiple times on the same log
			for cycle := 0; cycle < testCycles; cycle++ {
				t.Logf("====== Starting Test Cycle %d/%d ======", cycle+1, testCycles)

				// Create channels to track completion and results
				writerDone := make(chan struct{})
				reader1Done := make(chan []string)
				reader2Done := make(chan []string)
				reader3Done := make(chan []string)
				reader4Done := make(chan []string)
				reader5Done := make(chan []string)
				reader6Done := make(chan []string)
				reader7Done := make(chan []string)
				reader8Done := make(chan []string)
				reader9Done := make(chan []string)
				reader10Done := make(chan []string)
				reader11Done := make(chan []string)
				reader12Done := make(chan []string)
				reader13Done := make(chan []string)
				reader14Done := make(chan []string)
				reader15Done := make(chan []string)
				reader16Done := make(chan []string)

				// Start writer goroutine
				go func() {
					defer close(writerDone)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					assert.NoError(t, err)
					defer writer.Close(ctx)

					t.Logf("Cycle %d - Writer: Started writing messages", cycle+1)

					// Write messages
					for i := 0; i < messageCount; i++ {
						msgIdx := i
						message := &log.WriteMessage{
							Payload: []byte(fmt.Sprintf("Cycle %d - Message %d", cycle+1, msgIdx)),
							Properties: map[string]string{
								"cycle": fmt.Sprintf("%d", cycle+1),
								"index": fmt.Sprintf("%d", msgIdx),
								"type":  "concurrent-test",
							},
						}

						result := writer.Write(ctx, message)
						assert.NoError(t, result.Err)

						// Log milestone messages
						if i%5 == 0 || msgIdx == messageCount-1 {
							t.Logf("Cycle %d - Writer: Written message %d with ID: segment=%d, entry=%d",
								cycle+1, msgIdx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay to allow readers to catch up
						time.Sleep(10 * time.Millisecond)
					}

					t.Logf("Cycle %d - Writer: Completed writing all messages", cycle+1)
				}()

				// Update total messages to be read in this cycle
				totalMessagesWritten += messageCount
				expectedMessagesThisCycle := totalMessagesWritten

				// Function to create a reader goroutine that reads all messages from the beginning
				createReader := func(readerName string, resultChan chan<- []string) {
					// Add a small delay to ensure writer has started
					time.Sleep(100 * time.Millisecond)

					// Always start reading from the beginning
					startPoint := &log.LogMessageId{
						SegmentId: 0,
						EntryId:   0,
					}

					reader, err := logHandle.OpenLogReader(ctx, startPoint, readerName)
					assert.NoError(t, err)
					defer reader.Close(ctx)

					t.Logf("Cycle %d - %s: Started reading from beginning, expecting %d total messages",
						cycle+1, readerName, expectedMessagesThisCycle)

					// Read messages with simple for loop and fixed timeout
					messages := make([]string, 0, expectedMessagesThisCycle)

					// Simple loop to read the expected number of messages
					for i := 0; i < expectedMessagesThisCycle; i++ {
						// Use a fixed timeout for each read
						readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
						msg, err := reader.ReadNext(readCtx)
						cancel()

						if err != nil {
							t.Logf("Cycle %d - %s: Error reading message %d: %v",
								cycle+1, readerName, i, err)
							resultChan <- messages
							return
						}

						if msg == nil {
							t.Logf("Cycle %d - %s: Received nil message at position %d",
								cycle+1, readerName, i)
							resultChan <- messages
							return
						}

						messages = append(messages, string(msg.Payload))

						// Log milestone messages
						//if (i+1)%messageCount == 0 || (i+1)%10 == 0 || i == 0 || i == expectedMessagesThisCycle-1 {
						t.Logf("[%s] Cycle %d - %s: Read message %d/%d:  seg:%d,entry:%d  payload:%s",
							time.Now().Format("20060102150405"), cycle+1, readerName, i+1, expectedMessagesThisCycle, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
						//}
					}

					t.Logf("Cycle %d - %s: Completed reading all %d messages",
						cycle+1, readerName, len(messages))
					resultChan <- messages
				}

				// Start reader goroutines
				go createReader(fmt.Sprintf("Cycle%d-Reader1", cycle+1), reader1Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader2", cycle+1), reader2Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader3", cycle+1), reader3Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader4", cycle+1), reader4Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader5", cycle+1), reader5Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader6", cycle+1), reader6Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader7", cycle+1), reader7Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader8", cycle+1), reader8Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader9", cycle+1), reader9Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader10", cycle+1), reader10Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader11", cycle+1), reader11Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader12", cycle+1), reader12Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader13", cycle+1), reader13Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader14", cycle+1), reader14Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader15", cycle+1), reader15Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader16", cycle+1), reader16Done)

				// Wait for writer to complete
				<-writerDone
				t.Logf("Cycle %d - Writer goroutine completed", cycle+1)

				// Wait for readers to complete and collect results
				reader1Messages := <-reader1Done
				t.Logf("Cycle %d - Reader-1 completed with %d/%d messages", cycle+1, len(reader1Messages), expectedMessagesThisCycle)
				reader2Messages := <-reader2Done
				t.Logf("Cycle %d - Reader-2 completed with %d/%d messages", cycle+1, len(reader2Messages), expectedMessagesThisCycle)
				reader3Messages := <-reader3Done
				t.Logf("Cycle %d - Reader-3 completed with %d/%d messages", cycle+1, len(reader3Messages), expectedMessagesThisCycle)
				reader4Messages := <-reader4Done
				t.Logf("Cycle %d - Reader-4 completed with %d/%d messages", cycle+1, len(reader4Messages), expectedMessagesThisCycle)
				reader5Messages := <-reader5Done
				t.Logf("Cycle %d - Reader-5 completed with %d/%d messages", cycle+1, len(reader5Messages), expectedMessagesThisCycle)
				reader6Messages := <-reader6Done
				t.Logf("Cycle %d - Reader-6 completed with %d/%d messages", cycle+1, len(reader6Messages), expectedMessagesThisCycle)
				reader7Messages := <-reader7Done
				t.Logf("Cycle %d - Reader-7 completed with %d/%d messages", cycle+1, len(reader7Messages), expectedMessagesThisCycle)
				reader8Messages := <-reader8Done
				t.Logf("Cycle %d - Reader-8 completed with %d/%d messages", cycle+1, len(reader8Messages), expectedMessagesThisCycle)
				reader9Messages := <-reader9Done
				t.Logf("Cycle %d - Reader-9 completed with %d/%d messages", cycle+1, len(reader9Messages), expectedMessagesThisCycle)
				reader10Messages := <-reader10Done
				t.Logf("Cycle %d - Reader-10 completed with %d/%d messages", cycle+1, len(reader10Messages), expectedMessagesThisCycle)
				reader11Messages := <-reader11Done
				t.Logf("Cycle %d - Reader-11 completed with %d/%d messages", cycle+1, len(reader11Messages), expectedMessagesThisCycle)
				reader12Messages := <-reader12Done
				t.Logf("Cycle %d - Reader-12 completed with %d/%d messages", cycle+1, len(reader12Messages), expectedMessagesThisCycle)
				reader13Messages := <-reader13Done
				t.Logf("Cycle %d - Reader-13 completed with %d/%d messages", cycle+1, len(reader13Messages), expectedMessagesThisCycle)
				reader14Messages := <-reader14Done
				t.Logf("Cycle %d - Reader-14 completed with %d/%d messages", cycle+1, len(reader14Messages), expectedMessagesThisCycle)
				reader15Messages := <-reader15Done
				t.Logf("Cycle %d - Reader-15 completed with %d/%d messages", cycle+1, len(reader15Messages), expectedMessagesThisCycle)
				reader16Messages := <-reader16Done
				t.Logf("Cycle %d - Reader-16 completed with %d/%d messages", cycle+1, len(reader16Messages), expectedMessagesThisCycle)

				// Verify results - each reader should read all messages from all cycles so far
				assert.Equal(t, expectedMessagesThisCycle, len(reader1Messages), "Cycle %d - Reader-1 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader2Messages), "Cycle %d - Reader-2 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader3Messages), "Cycle %d - Reader-3 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader4Messages), "Cycle %d - Reader-4 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader5Messages), "Cycle %d - Reader-5 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader6Messages), "Cycle %d - Reader-6 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader7Messages), "Cycle %d - Reader-7 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader8Messages), "Cycle %d - Reader-8 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader9Messages), "Cycle %d - Reader-9 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader10Messages), "Cycle %d - Reader-10 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader11Messages), "Cycle %d - Reader-11 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader12Messages), "Cycle %d - Reader-12 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader13Messages), "Cycle %d - Reader-13 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader14Messages), "Cycle %d - Reader-14 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader15Messages), "Cycle %d - Reader-15 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader16Messages), "Cycle %d - Reader-16 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)

				// Verify message content - need to verify all messages from all cycles
				// First verify cycle 1's messages
				for cycleNum := 1; cycleNum <= cycle+1; cycleNum++ {
					t.Logf("Verifying messages from cycle %d", cycleNum)

					// Calculate the start and end indices for messages from this cycle
					startIdx := (cycleNum - 1) * messageCount

					for i := 0; i < messageCount; i++ {
						msgIdx := startIdx + i
						expectedMsg := fmt.Sprintf("Cycle %d - Message %d", cycleNum, i)

						// Verify in reader1
						if msgIdx < len(reader1Messages) {
							assert.Equal(t, expectedMsg, reader1Messages[msgIdx], "Cycle %d - Reader-1 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader2
						if msgIdx < len(reader2Messages) {
							assert.Equal(t, expectedMsg, reader2Messages[msgIdx], "Cycle %d - Reader-2 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader3
						if msgIdx < len(reader3Messages) {
							assert.Equal(t, expectedMsg, reader3Messages[msgIdx], "Cycle %d - Reader-3 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader4
						if msgIdx < len(reader4Messages) {
							assert.Equal(t, expectedMsg, reader4Messages[msgIdx], "Cycle %d - Reader-4 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader5
						if msgIdx < len(reader5Messages) {
							assert.Equal(t, expectedMsg, reader5Messages[msgIdx], "Cycle %d - Reader-5 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader6
						if msgIdx < len(reader6Messages) {
							assert.Equal(t, expectedMsg, reader6Messages[msgIdx], "Cycle %d - Reader-6 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader7
						if msgIdx < len(reader7Messages) {
							assert.Equal(t, expectedMsg, reader7Messages[msgIdx], "Cycle %d - Reader-7 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader8
						if msgIdx < len(reader8Messages) {
							assert.Equal(t, expectedMsg, reader8Messages[msgIdx], "Cycle %d - Reader-8 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader9
						if msgIdx < len(reader9Messages) {
							assert.Equal(t, expectedMsg, reader9Messages[msgIdx], "Cycle %d - Reader-9 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader10
						if msgIdx < len(reader10Messages) {
							assert.Equal(t, expectedMsg, reader10Messages[msgIdx], "Cycle %d - Reader-10 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader11
						if msgIdx < len(reader11Messages) {
							assert.Equal(t, expectedMsg, reader11Messages[msgIdx], "Cycle %d - Reader-11 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader12
						if msgIdx < len(reader12Messages) {
							assert.Equal(t, expectedMsg, reader12Messages[msgIdx], "Cycle %d - Reader-12 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader13
						if msgIdx < len(reader13Messages) {
							assert.Equal(t, expectedMsg, reader13Messages[msgIdx], "Cycle %d - Reader-13 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader14
						if msgIdx < len(reader14Messages) {
							assert.Equal(t, expectedMsg, reader14Messages[msgIdx], "Cycle %d - Reader-14 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader15
						if msgIdx < len(reader15Messages) {
							assert.Equal(t, expectedMsg, reader15Messages[msgIdx], "Cycle %d - Reader-15 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader16
						if msgIdx < len(reader16Messages) {
							assert.Equal(t, expectedMsg, reader16Messages[msgIdx], "Cycle %d - Reader-16 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
					}
				}

				// Add a short delay between cycles
				time.Sleep(500 * time.Millisecond)

				// Open empty writer then close
				emptyWriter, createEmptyWriterErr := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, createEmptyWriterErr)
				closeEmptyWriterErr := emptyWriter.Close(ctx)
				assert.NoError(t, closeEmptyWriterErr, fmt.Sprintf("%v", closeEmptyWriterErr))

				t.Logf("====== Completed Test Cycle %d/%d Successfully ======", cycle+1, testCycles)
			}
			t.Log("All test cycles completed successfully")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestConcurrentWriteAndReadWithSegmentRollingFrequentlyAndFinalVerification(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentWriteAndReadWithSegmentRollingFrequentlyAndFinalVerification")
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

			// Number of overall test cycles to run
			const testCycles = 3
			// Number of messages to write in each cycle
			const messageCount = 20

			// Create a single test log with timestamp to ensure uniqueness
			logName := "test_concurrent_write_read_rolling_final_verification_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Track total messages written for verification
			totalMessagesWritten := 0
			// Store all expected messages for final verification
			allExpectedMessages := make([]string, 0, testCycles*messageCount)

			// Run the complete test multiple times on the same log
			for cycle := 0; cycle < testCycles; cycle++ {
				t.Logf("====== Starting Test Cycle %d/%d ======", cycle+1, testCycles)

				// Create channels to track completion and results
				writerDone := make(chan struct{})
				reader1Done := make(chan []string)
				reader2Done := make(chan []string)
				reader3Done := make(chan []string)
				reader4Done := make(chan []string)
				reader5Done := make(chan []string)
				reader6Done := make(chan []string)
				reader7Done := make(chan []string)
				reader8Done := make(chan []string)
				reader9Done := make(chan []string)
				reader10Done := make(chan []string)
				reader11Done := make(chan []string)
				reader12Done := make(chan []string)
				reader13Done := make(chan []string)
				reader14Done := make(chan []string)
				reader15Done := make(chan []string)
				reader16Done := make(chan []string)

				// Start writer goroutine
				go func() {
					defer close(writerDone)

					// Open writer
					writer, err := logHandle.OpenLogWriter(ctx)
					assert.NoError(t, err)
					defer writer.Close(ctx)

					t.Logf("Cycle %d - Writer: Started writing messages", cycle+1)

					// Write messages
					for i := 0; i < messageCount; i++ {
						msgIdx := i
						messageContent := fmt.Sprintf("Cycle %d - Message %d", cycle+1, msgIdx)
						message := &log.WriteMessage{
							Payload: []byte(messageContent),
							Properties: map[string]string{
								"cycle": fmt.Sprintf("%d", cycle+1),
								"index": fmt.Sprintf("%d", msgIdx),
								"type":  "concurrent-test",
							},
						}

						result := writer.Write(ctx, message)
						assert.NoError(t, result.Err)

						// Store expected message for final verification
						allExpectedMessages = append(allExpectedMessages, messageContent)

						// Log milestone messages
						if i%5 == 0 || msgIdx == messageCount-1 {
							t.Logf("Cycle %d - Writer: Written message %d with ID: segment=%d, entry=%d",
								cycle+1, msgIdx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
						}

						// Small delay to allow readers to catch up
						time.Sleep(10 * time.Millisecond)
					}

					t.Logf("Cycle %d - Writer: Completed writing all messages", cycle+1)
				}()

				// Update total messages to be read in this cycle
				totalMessagesWritten += messageCount
				expectedMessagesThisCycle := totalMessagesWritten

				// Function to create a reader goroutine that reads all messages from the beginning
				createReader := func(readerName string, resultChan chan<- []string) {
					// Add a small delay to ensure writer has started
					time.Sleep(100 * time.Millisecond)

					// Always start reading from the beginning
					startPoint := &log.LogMessageId{
						SegmentId: 0,
						EntryId:   0,
					}

					reader, err := logHandle.OpenLogReader(ctx, startPoint, readerName)
					assert.NoError(t, err)
					defer reader.Close(ctx)

					t.Logf("Cycle %d - %s: Started reading from beginning, expecting %d total messages",
						cycle+1, readerName, expectedMessagesThisCycle)

					// Read messages with simple for loop and fixed timeout
					messages := make([]string, 0, expectedMessagesThisCycle)

					// Simple loop to read the expected number of messages
					for i := 0; i < expectedMessagesThisCycle; i++ {
						// Use a fixed timeout for each read
						readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						msg, err := reader.ReadNext(readCtx)
						cancel()

						if err != nil {
							t.Logf("Cycle %d - %s: Error reading message %d: %v",
								cycle+1, readerName, i, err)
							resultChan <- messages
							return
						}

						if msg == nil {
							t.Logf("Cycle %d - %s: Received nil message at position %d",
								cycle+1, readerName, i)
							resultChan <- messages
							return
						}

						messages = append(messages, string(msg.Payload))

						// Log milestone messages
						//if (i+1)%messageCount == 0 || (i+1)%10 == 0 || i == 0 || i == expectedMessagesThisCycle-1 {
						t.Logf("[%s] Cycle %d - %s: Read message %d/%d:  seg:%d,entry:%d  payload:%s",
							time.Now().Format("20060102150405"), cycle+1, readerName, i+1, expectedMessagesThisCycle, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
						//}
					}

					t.Logf("Cycle %d - %s: Completed reading all %d messages",
						cycle+1, readerName, len(messages))
					resultChan <- messages
				}

				// Start reader goroutines
				go createReader(fmt.Sprintf("Cycle%d-Reader1", cycle+1), reader1Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader2", cycle+1), reader2Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader3", cycle+1), reader3Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader4", cycle+1), reader4Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader5", cycle+1), reader5Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader6", cycle+1), reader6Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader7", cycle+1), reader7Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader8", cycle+1), reader8Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader9", cycle+1), reader9Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader10", cycle+1), reader10Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader11", cycle+1), reader11Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader12", cycle+1), reader12Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader13", cycle+1), reader13Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader14", cycle+1), reader14Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader15", cycle+1), reader15Done)
				go createReader(fmt.Sprintf("Cycle%d-Reader16", cycle+1), reader16Done)

				// Wait for writer to complete
				<-writerDone
				t.Logf("Cycle %d - Writer goroutine completed", cycle+1)

				// Wait for readers to complete and collect results
				reader1Messages := <-reader1Done
				t.Logf("Cycle %d - Reader-1 completed with %d/%d messages", cycle+1, len(reader1Messages), expectedMessagesThisCycle)
				reader2Messages := <-reader2Done
				t.Logf("Cycle %d - Reader-2 completed with %d/%d messages", cycle+1, len(reader2Messages), expectedMessagesThisCycle)
				reader3Messages := <-reader3Done
				t.Logf("Cycle %d - Reader-3 completed with %d/%d messages", cycle+1, len(reader3Messages), expectedMessagesThisCycle)
				reader4Messages := <-reader4Done
				t.Logf("Cycle %d - Reader-4 completed with %d/%d messages", cycle+1, len(reader4Messages), expectedMessagesThisCycle)
				reader5Messages := <-reader5Done
				t.Logf("Cycle %d - Reader-5 completed with %d/%d messages", cycle+1, len(reader5Messages), expectedMessagesThisCycle)
				reader6Messages := <-reader6Done
				t.Logf("Cycle %d - Reader-6 completed with %d/%d messages", cycle+1, len(reader6Messages), expectedMessagesThisCycle)
				reader7Messages := <-reader7Done
				t.Logf("Cycle %d - Reader-7 completed with %d/%d messages", cycle+1, len(reader7Messages), expectedMessagesThisCycle)
				reader8Messages := <-reader8Done
				t.Logf("Cycle %d - Reader-8 completed with %d/%d messages", cycle+1, len(reader8Messages), expectedMessagesThisCycle)
				reader9Messages := <-reader9Done
				t.Logf("Cycle %d - Reader-9 completed with %d/%d messages", cycle+1, len(reader9Messages), expectedMessagesThisCycle)
				reader10Messages := <-reader10Done
				t.Logf("Cycle %d - Reader-10 completed with %d/%d messages", cycle+1, len(reader10Messages), expectedMessagesThisCycle)
				reader11Messages := <-reader11Done
				t.Logf("Cycle %d - Reader-11 completed with %d/%d messages", cycle+1, len(reader11Messages), expectedMessagesThisCycle)
				reader12Messages := <-reader12Done
				t.Logf("Cycle %d - Reader-12 completed with %d/%d messages", cycle+1, len(reader12Messages), expectedMessagesThisCycle)
				reader13Messages := <-reader13Done
				t.Logf("Cycle %d - Reader-13 completed with %d/%d messages", cycle+1, len(reader13Messages), expectedMessagesThisCycle)
				reader14Messages := <-reader14Done
				t.Logf("Cycle %d - Reader-14 completed with %d/%d messages", cycle+1, len(reader14Messages), expectedMessagesThisCycle)
				reader15Messages := <-reader15Done
				t.Logf("Cycle %d - Reader-15 completed with %d/%d messages", cycle+1, len(reader15Messages), expectedMessagesThisCycle)
				reader16Messages := <-reader16Done
				t.Logf("Cycle %d - Reader-16 completed with %d/%d messages", cycle+1, len(reader16Messages), expectedMessagesThisCycle)

				// Verify results - each reader should read all messages from all cycles so far
				assert.Equal(t, expectedMessagesThisCycle, len(reader1Messages), "Cycle %d - Reader-1 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader2Messages), "Cycle %d - Reader-2 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader3Messages), "Cycle %d - Reader-3 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader4Messages), "Cycle %d - Reader-4 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader5Messages), "Cycle %d - Reader-5 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader6Messages), "Cycle %d - Reader-6 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader7Messages), "Cycle %d - Reader-7 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader8Messages), "Cycle %d - Reader-8 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader9Messages), "Cycle %d - Reader-9 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader10Messages), "Cycle %d - Reader-10 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader11Messages), "Cycle %d - Reader-11 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader12Messages), "Cycle %d - Reader-12 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader13Messages), "Cycle %d - Reader-13 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader14Messages), "Cycle %d - Reader-14 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader15Messages), "Cycle %d - Reader-15 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)
				assert.Equal(t, expectedMessagesThisCycle, len(reader16Messages), "Cycle %d - Reader-16 should read all %d messages written so far", cycle+1, expectedMessagesThisCycle)

				// Verify message content - need to verify all messages from all cycles
				// First verify cycle 1's messages
				for cycleNum := 1; cycleNum <= cycle+1; cycleNum++ {
					t.Logf("Verifying messages from cycle %d", cycleNum)

					// Calculate the start and end indices for messages from this cycle
					startIdx := (cycleNum - 1) * messageCount

					for i := 0; i < messageCount; i++ {
						msgIdx := startIdx + i
						expectedMsg := fmt.Sprintf("Cycle %d - Message %d", cycleNum, i)

						// Verify in reader1
						if msgIdx < len(reader1Messages) {
							assert.Equal(t, expectedMsg, reader1Messages[msgIdx], "Cycle %d - Reader-1 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader2
						if msgIdx < len(reader2Messages) {
							assert.Equal(t, expectedMsg, reader2Messages[msgIdx], "Cycle %d - Reader-2 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader3
						if msgIdx < len(reader3Messages) {
							assert.Equal(t, expectedMsg, reader3Messages[msgIdx], "Cycle %d - Reader-3 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader4
						if msgIdx < len(reader4Messages) {
							assert.Equal(t, expectedMsg, reader4Messages[msgIdx], "Cycle %d - Reader-4 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader5
						if msgIdx < len(reader5Messages) {
							assert.Equal(t, expectedMsg, reader5Messages[msgIdx], "Cycle %d - Reader-5 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader6
						if msgIdx < len(reader6Messages) {
							assert.Equal(t, expectedMsg, reader6Messages[msgIdx], "Cycle %d - Reader-6 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader7
						if msgIdx < len(reader7Messages) {
							assert.Equal(t, expectedMsg, reader7Messages[msgIdx], "Cycle %d - Reader-7 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader8
						if msgIdx < len(reader8Messages) {
							assert.Equal(t, expectedMsg, reader8Messages[msgIdx], "Cycle %d - Reader-8 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader9
						if msgIdx < len(reader9Messages) {
							assert.Equal(t, expectedMsg, reader9Messages[msgIdx], "Cycle %d - Reader-9 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader10
						if msgIdx < len(reader10Messages) {
							assert.Equal(t, expectedMsg, reader10Messages[msgIdx], "Cycle %d - Reader-10 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader11
						if msgIdx < len(reader11Messages) {
							assert.Equal(t, expectedMsg, reader11Messages[msgIdx], "Cycle %d - Reader-11 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader12
						if msgIdx < len(reader12Messages) {
							assert.Equal(t, expectedMsg, reader12Messages[msgIdx], "Cycle %d - Reader-12 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader13
						if msgIdx < len(reader13Messages) {
							assert.Equal(t, expectedMsg, reader13Messages[msgIdx], "Cycle %d - Reader-13 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader14
						if msgIdx < len(reader14Messages) {
							assert.Equal(t, expectedMsg, reader14Messages[msgIdx], "Cycle %d - Reader-14 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader15
						if msgIdx < len(reader15Messages) {
							assert.Equal(t, expectedMsg, reader15Messages[msgIdx], "Cycle %d - Reader-15 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
						// Verify in reader16
						if msgIdx < len(reader16Messages) {
							assert.Equal(t, expectedMsg, reader16Messages[msgIdx], "Cycle %d - Reader-16 message %d (from cycle %d) content mismatch", cycle+1, msgIdx, cycleNum)
						}
					}
				}

				// Add a short delay between cycles
				time.Sleep(500 * time.Millisecond)

				// Open empty writer then close
				emptyWriter, createEmptyWriterErr := logHandle.OpenLogWriter(ctx)
				assert.NoError(t, createEmptyWriterErr)
				closeEmptyWriterErr := emptyWriter.Close(ctx)
				assert.NoError(t, closeEmptyWriterErr)

				t.Logf("====== Completed Test Cycle %d/%d Successfully ======", cycle+1, testCycles)
			}
			t.Log("All test cycles completed successfully")

			// ====== FINAL VERIFICATION: Read all data from beginning to end ======
			t.Log("====== Starting Final Verification: Reading all data from beginning to end ======")

			// Wait a moment to ensure all operations are complete
			time.Sleep(1 * time.Second)

			// Create a final verification reader
			startPoint := &log.LogMessageId{
				SegmentId: 0,
				EntryId:   0,
			}

			finalReader, err := logHandle.OpenLogReader(ctx, startPoint, "final-verification-reader")
			assert.NoError(t, err)
			assert.NotNil(t, finalReader)
			defer finalReader.Close(ctx)

			t.Logf("Final Verification: Started reading from beginning, expecting %d total messages", totalMessagesWritten)

			// Read all messages sequentially
			finalMessages := make([]string, 0, totalMessagesWritten)
			readTimeout := 10 * time.Second // Longer timeout for final verification

			for i := 0; i < totalMessagesWritten; i++ {
				readCtx, cancel := context.WithTimeout(ctx, readTimeout)
				msg, err := finalReader.ReadNext(readCtx)
				cancel()

				if err != nil {
					t.Logf("Final Verification: Error reading message %d: %v", i+1, err)
					break
				}

				if msg == nil {
					t.Logf("Final Verification: Received nil message at position %d", i+1)
					break
				}

				finalMessages = append(finalMessages, string(msg.Payload))

				// Log milestone messages
				if i%10 == 0 || i == totalMessagesWritten-1 {
					t.Logf("Final Verification: Read message %d/%d: seg:%d,entry:%d payload:%s",
						i+1, totalMessagesWritten, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
				}
			}

			// Verify the total count
			assert.Equal(t, totalMessagesWritten, len(finalMessages), "Final Verification: Should read exactly %d messages", totalMessagesWritten)
			assert.Equal(t, len(allExpectedMessages), len(finalMessages), "Final Verification: Should read exactly the same number of messages as expected")

			// Verify each message content matches expected
			for i, expectedMsg := range allExpectedMessages {
				if i < len(finalMessages) {
					assert.Equal(t, expectedMsg, finalMessages[i], "Final Verification: Message %d content mismatch", i)
				} else {
					t.Errorf("Final Verification: Missing message %d: expected '%s'", i, expectedMsg)
				}
			}

			// Try to read one more message - should timeout or return nil
			extraReadCtx, cancelExtra := context.WithTimeout(ctx, 1*time.Second)
			extraMessage, err := finalReader.ReadNext(extraReadCtx)
			cancelExtra()

			if err != nil {
				t.Log("Final Verification: No extra messages as expected (timeout/error)")
			} else if extraMessage == nil {
				t.Log("Final Verification: No extra messages as expected (nil response)")
			} else {
				t.Errorf("Final Verification: Unexpected extra message: %s", string(extraMessage.Payload))
			}

			t.Logf("====== Final Verification Completed Successfully: Read and verified %d messages ======", len(finalMessages))

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}

func TestConcurrentReaderWriterWithHangingBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentReaderWriterWithHangingBehavior")
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

			// Create a test log with timestamp to ensure uniqueness
			logName := "test_concurrent_reader_writer_hanging_" + tc.name + time.Now().Format("20060102150405")
			createErr := client.CreateLog(ctx, logName)
			assert.NoError(t, createErr)

			// Open log handle
			logHandle, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			t.Logf("Created test log: %s", logName)

			// Phase 1: Create empty writer (no data written)
			t.Log("Phase 1: Creating empty writer without writing data")
			emptyWriter, err := logHandle.OpenLogWriter(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, emptyWriter)

			// Phase 2: Start reader in goroutine - should hang until writer writes data
			t.Log("Phase 2: Starting reader - should hang until writer writes data")
			readerMessages := make(chan string, 15) // Buffer for 10 + 1 messages
			readerErrors := make(chan error, 15)
			readerDone := make(chan struct{})
			readerStarted := make(chan struct{}) // Signal when reader actually starts

			// Arrays to collect messages consumed during hang checks
			var collectedMessages []string
			var collectedErrors []error

			go func() {
				defer close(readerDone)

				// Start reading from the beginning
				startPoint := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   0,
				}

				reader, err := logHandle.OpenLogReader(ctx, startPoint, "hanging-behavior-reader")
				if err != nil {
					readerErrors <- err
					return
				}
				defer reader.Close(ctx)

				t.Log("Reader started, waiting for data...")
				close(readerStarted) // Signal that reader has started

				// Read messages one by one - should hang until data is available
				for i := 0; i < 11; i++ { // Expect 10 + 1 messages total
					t.Logf("Reader: Attempting to read message %d", i+1)

					// Use a longer timeout to allow for the hanging behavior
					readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					msg, err := reader.ReadNext(readCtx)
					cancel()

					if err != nil {
						t.Logf("Reader: Error reading message %d: %v", i+1, err)
						readerErrors <- err
						return
					}

					if msg == nil {
						t.Logf("Reader: Received nil message at position %d", i+1)
						readerErrors <- fmt.Errorf("received nil message at position %d", i+1)
						return
					}

					t.Logf("Reader: Successfully read message %d: seg:%d,entry:%d payload:%s",
						i+1, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
					readerMessages <- string(msg.Payload)
				}

				t.Log("Reader: Completed reading all expected messages")
			}()

			// Wait for reader to start
			<-readerStarted

			// Phase 3: Wait a bit to ensure reader is hanging
			t.Log("Phase 3: Waiting to ensure reader is hanging...")
			time.Sleep(2 * time.Second)

			// Verify reader hasn't received any messages yet (non-blocking check)
			select {
			case msg := <-readerMessages:
				t.Errorf("Reader should be hanging but received message: %s", msg)
				// Collect the message instead of putting it back
				collectedMessages = append(collectedMessages, msg)
			case err := <-readerErrors:
				t.Errorf("Reader should be hanging but received error: %v", err)
				// Collect the error instead of putting it back
				collectedErrors = append(collectedErrors, err)
			default:
				t.Log("✓ Reader is correctly hanging, no messages received yet")
			}

			// Phase 4: Close empty writer and start writing data
			t.Log("Phase 4: Closing empty writer and starting to write data")
			err = emptyWriter.Close(ctx)
			assert.NoError(t, err)

			// Open a new writer for writing data
			dataWriter, err := logHandle.OpenLogWriter(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, dataWriter)

			// Phase 5: Write 10 messages, one per second
			t.Log("Phase 5: Writing 10 messages, one per second")
			for i := 0; i < 10; i++ {
				message := &log.WriteMessage{
					Payload: []byte(fmt.Sprintf("Message %d", i+1)),
					Properties: map[string]string{
						"index": fmt.Sprintf("%d", i+1),
						"type":  "hanging-test",
					},
				}

				result := dataWriter.Write(ctx, message)
				assert.NoError(t, result.Err)
				assert.NotNil(t, result.LogMessageId)

				t.Logf("Writer: Written message %d with ID: segment=%d, entry=%d",
					i+1, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

				// Wait 1 second before writing next message
				time.Sleep(1 * time.Second)

				// Check if reader received the message
				select {
				case receivedMsg := <-readerMessages:
					t.Logf("✓ Reader received message %d: %s", i+1, receivedMsg)
					assert.Equal(t, fmt.Sprintf("Message %d", i+1), receivedMsg)
					collectedMessages = append(collectedMessages, receivedMsg)
				case err := <-readerErrors:
					t.Errorf("Reader error while reading message %d: %v", i+1, err)
				case <-time.After(2 * time.Second):
					t.Errorf("Reader did not receive message %d within timeout", i+1)
				}
			}

			// Phase 6: Close the data writer
			t.Log("Phase 6: Closing data writer")
			err = dataWriter.Close(ctx)
			assert.NoError(t, err)

			// Phase 7: Wait a bit to ensure reader is hanging again
			t.Log("Phase 7: Waiting to ensure reader is hanging again after writer closed...")
			time.Sleep(2 * time.Second)

			// Verify reader is hanging again (no new messages) - non-blocking check
			select {
			case msg := <-readerMessages:
				t.Errorf("Reader should be hanging but received unexpected message: %s", msg)
				// Collect the message instead of putting it back
				collectedMessages = append(collectedMessages, msg)
			case err := <-readerErrors:
				t.Errorf("Reader should be hanging but received error: %v", err)
				// Collect the error instead of putting it back
				collectedErrors = append(collectedErrors, err)
			default:
				t.Log("✓ Reader is correctly hanging again after writer closed")
			}

			// Phase 8: Open a new writer and write one more message
			t.Log("Phase 8: Opening new writer and writing one more message")
			newWriter, err := logHandle.OpenLogWriter(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, newWriter)

			finalMessage := &log.WriteMessage{
				Payload: []byte("Final Message"),
				Properties: map[string]string{
					"index": "11",
					"type":  "hanging-test-final",
				},
			}

			result := newWriter.Write(ctx, finalMessage)
			assert.NoError(t, result.Err)
			assert.NotNil(t, result.LogMessageId)

			t.Logf("Writer: Written final message with ID: segment=%d, entry=%d",
				result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

			// Phase 9: Verify reader receives the final message
			t.Log("Phase 9: Verifying reader receives the final message")
			select {
			case receivedMsg := <-readerMessages:
				t.Logf("✓ Reader received final message: %s", receivedMsg)
				assert.Equal(t, "Final Message", receivedMsg)
				collectedMessages = append(collectedMessages, receivedMsg)
			case err := <-readerErrors:
				t.Errorf("Reader error while reading final message: %v", err)
			case <-time.After(5 * time.Second):
				t.Errorf("Reader did not receive final message within timeout")
			}

			// Phase 10: Close the new writer
			t.Log("Phase 10: Closing new writer")
			err = newWriter.Close(ctx)
			assert.NoError(t, err)

			// Phase 11: Wait for reader goroutine to complete
			t.Log("Phase 11: Waiting for reader goroutine to complete")
			select {
			case <-readerDone:
				t.Log("✓ Reader goroutine completed successfully")
			case <-time.After(10 * time.Second):
				t.Error("Reader goroutine did not complete within timeout")
			}

			// Phase 12: Verify all messages were received in correct order
			t.Log("Phase 12: Verifying all messages were received in correct order")

			// Wait a bit to ensure all messages are processed
			time.Sleep(500 * time.Millisecond)

			// Drain remaining messages from channel to count them
			var allMessages []string
			var allErrors []error

			// First add any messages collected during hang checks
			allMessages = append(allMessages, collectedMessages...)
			allErrors = append(allErrors, collectedErrors...)

			// Then collect all remaining messages from channel
			for {
				select {
				case msg := <-readerMessages:
					allMessages = append(allMessages, msg)
				case err := <-readerErrors:
					allErrors = append(allErrors, err)
				default:
					// No more messages, break the loop
					goto done
				}
			}

		done:
			receivedCount := len(allMessages)
			t.Logf("Total messages received: %d", receivedCount)

			// Should have received exactly 11 messages (10 + 1 final)
			assert.Equal(t, 11, receivedCount, "Should have received exactly 11 messages")

			// Verify message content
			expectedMessages := []string{
				"Message 1", "Message 2", "Message 3", "Message 4", "Message 5",
				"Message 6", "Message 7", "Message 8", "Message 9", "Message 10",
				"Final Message",
			}

			for i, expectedMsg := range expectedMessages {
				if i < len(allMessages) {
					assert.Equal(t, expectedMsg, allMessages[i], "Message %d content mismatch", i+1)
				}
			}

			// Verify no errors occurred
			errorCount := len(allErrors)
			assert.Equal(t, 0, errorCount, "Should have no reader errors")

			if errorCount > 0 {
				t.Logf("Errors encountered: %v", allErrors)
			}

			t.Log("✓ Test completed successfully - reader hanging behavior verified")

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
		})
	}
}
