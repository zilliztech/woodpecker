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
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// generateUniqueLogId creates a unique log ID using timestamp
func generateUniqueLogId() int64 {
	return time.Now().UnixNano() / 1000000 // Use milliseconds to ensure uniqueness
}

// waitForServerReady waits for the gRPC server to be ready by attempting to connect
func waitForServerReady(ctx context.Context, target string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	for time.Now().Before(deadline) {
		// Try to get a client connection
		testClient, err := clientPool.GetLogStoreClient(ctx, target)
		if err == nil && testClient != nil {
			// Connection successful, server is ready
			_ = testClient.Close(ctx)
			return true
		}

		// Wait a bit before retrying
		time.Sleep(50 * time.Millisecond)
	}

	return false
}

func TestRemoteClient_BasicAppend(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestRemoteClient_BasicAppend")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath, // Will be set to t.TempDir()
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup configuration
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			cfg.Woodpecker.Storage.RootPath = tc.rootPath

			ctx := context.Background()

			// Find a free port for service
			listener, err := net.Listen("tcp", "localhost:0")
			assert.NoError(t, err)
			servicePort := listener.Addr().(*net.TCPAddr).Port
			listener.Close() // Close it so server can use it

			// Create and start server
			service := server.NewServer(ctx, cfg, 0, servicePort, []string{})

			// Prepare server (sets up listener and gossip)
			err = service.Prepare()
			assert.NoError(t, err)

			// Run server (starts grpc server and log store)
			go func() {
				err := service.Run()
				if err != nil {
					t.Logf("Server run error: %v", err)
				}
			}()

			// Clean up server
			defer func() {
				if service != nil {
					err := service.Stop()
					if err != nil {
						t.Logf("Error stopping service: %v", err)
					}
				}
			}()

			// Use the known service port
			target := fmt.Sprintf("localhost:%d", servicePort)

			// Wait for server to be ready with health check
			if !waitForServerReady(ctx, target, 10*time.Second) {
				t.Fatal("Server failed to start within timeout")
			}

			// Create remote client pool
			clientPool := client.NewLogStoreClientPool(268435456, 536870912)
			defer clientPool.Close(ctx)

			remoteClient, err := clientPool.GetLogStoreClient(ctx, target)
			assert.NoError(t, err)

			// Test basic append with subscription - use unique logId for each test
			logId := generateUniqueLogId()

			// Create result channel
			resultCh := channel.NewRemoteResultChannel("test")

			// Prepare entry
			entry := &proto.LogEntry{
				SegId:   100,
				EntryId: 0, // Start from 0 as expected by the log system
				Values:  []byte("test data"),
			}

			// Append entry
			entryId, err := remoteClient.AppendEntry(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logId, entry, resultCh)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), entryId) // Expect entryId 0

			// Wait for async result with timeout
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			result, err := resultCh.ReadResult(ctxWithTimeout)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, int64(0), result.SyncedId) // Expect syncedId 0
			assert.NoError(t, result.Err)

			// Test reading the entry back
			batchResult, err := remoteClient.ReadEntriesBatchAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logId, 100, 0, 100, nil)
			assert.NoError(t, err)
			assert.NotNil(t, batchResult)
			assert.Len(t, batchResult.Entries, 1) // Should have 1 entry

			// Verify the entry content
			readEntry := batchResult.Entries[0]
			assert.Equal(t, int64(100), readEntry.SegId)           // Same segment ID
			assert.Equal(t, int64(0), readEntry.EntryId)           // Entry ID should be 0
			assert.Equal(t, []byte("test data"), readEntry.Values) // Same data we wrote

			t.Cleanup(func() {
				_ = remoteClient.Close(context.TODO())
			})
		})
	}
}

func TestRemoteClient_MultipleAppends(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestRemoteClient_MultipleAppends")
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
			// Setup configuration
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			cfg.Woodpecker.Storage.RootPath = tc.rootPath

			ctx := context.Background()

			// Find a free port for service
			listener, err := net.Listen("tcp", "localhost:0")
			assert.NoError(t, err)
			servicePort := listener.Addr().(*net.TCPAddr).Port
			listener.Close() // Close it so server can use it

			// Create and start server
			service := server.NewServer(ctx, cfg, 0, servicePort, []string{})

			// Prepare server (sets up listener and gossip)
			err = service.Prepare()
			assert.NoError(t, err)

			// Run server (starts grpc server and log store)
			go func() {
				err := service.Run()
				if err != nil {
					t.Logf("Server run error: %v", err)
				}
			}()

			// Clean up server
			defer func() {
				if service != nil {
					err := service.Stop()
					if err != nil {
						t.Logf("Error stopping service: %v", err)
					}
				}
			}()

			// Use the known service port
			target := fmt.Sprintf("localhost:%d", servicePort)

			// Wait for server to be ready with health check
			if !waitForServerReady(ctx, target, 10*time.Second) {
				t.Fatal("Server failed to start within timeout")
			}

			// Create remote client pool
			clientPool := client.NewLogStoreClientPool(268435456, 536870912)
			defer clientPool.Close(ctx)

			remoteClient, err := clientPool.GetLogStoreClient(ctx, target)
			assert.NoError(t, err)

			logId := generateUniqueLogId() // Use unique logId for multiple appends test

			// Test multiple concurrent appends
			numAppends := 5
			resultChannels := make([]channel.ResultChannel, numAppends)
			entryIds := make([]int64, numAppends)

			// Submit all appends
			for i := 0; i < numAppends; i++ {
				resultChannels[i] = channel.NewRemoteResultChannel(fmt.Sprintf("test-%d", i))
				entry := &proto.LogEntry{
					SegId:   100,
					EntryId: int64(i), // Start from 0, 1, 2, ...
					Values:  []byte(fmt.Sprintf("test data %d", i)),
				}

				entryIds[i], err = remoteClient.AppendEntry(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logId, entry, resultChannels[i])
				assert.NoError(t, err)
				assert.Equal(t, int64(i), entryIds[i])
			}

			// Wait for all async results
			for i := 0; i < numAppends; i++ {
				ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
				result, err := resultChannels[i].ReadResult(ctxWithTimeout)
				cancel()
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, int64(i), result.SyncedId)
				assert.NoError(t, result.Err)
			}

			// Test reading all entries back
			batchResult, err := remoteClient.ReadEntriesBatchAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logId, 100, 0, int64(numAppends), nil)
			assert.NoError(t, err)
			assert.NotNil(t, batchResult)
			assert.Len(t, batchResult.Entries, numAppends) // Should have all entries

			// Verify each entry content in order
			for i := 0; i < numAppends; i++ {
				readEntry := batchResult.Entries[i]
				assert.Equal(t, int64(100), readEntry.SegId)                              // Same segment ID
				assert.Equal(t, int64(i), readEntry.EntryId)                              // Entry ID should match index
				assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), readEntry.Values) // Same data we wrote
			}

			t.Cleanup(func() {
				_ = remoteClient.Close(context.TODO())
			})
		})
	}
}

func TestRemoteClient_ClientClose(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestRemoteClient_ClientClose")
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
			// Setup configuration
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			cfg.Woodpecker.Storage.RootPath = tc.rootPath

			ctx := context.Background()

			// Find a free port for service
			listener, err := net.Listen("tcp", "localhost:0")
			assert.NoError(t, err)
			servicePort := listener.Addr().(*net.TCPAddr).Port
			listener.Close() // Close it so server can use it

			// Create and start server
			service := server.NewServer(ctx, cfg, 0, servicePort, []string{})

			// Prepare server (sets up listener and gossip)
			err = service.Prepare()
			assert.NoError(t, err)

			// Run server (starts grpc server and log store)
			go func() {
				err := service.Run()
				if err != nil {
					t.Logf("Server run error: %v", err)
				}
			}()

			// Clean up server
			defer func() {
				if service != nil {
					err := service.Stop()
					if err != nil {
						t.Logf("Error stopping service: %v", err)
					}
				}
			}()

			// Use the known service port
			target := fmt.Sprintf("localhost:%d", servicePort)

			// Wait for server to be ready with health check
			if !waitForServerReady(ctx, target, 10*time.Second) {
				t.Fatal("Server failed to start within timeout")
			}

			// Create remote client pool
			clientPool := client.NewLogStoreClientPool(268435456, 536870912)
			defer clientPool.Close(ctx)

			remoteClient, err := clientPool.GetLogStoreClient(ctx, target)
			assert.NoError(t, err)

			logId := generateUniqueLogId() // Use unique logId for client close test

			// Submit an append
			resultCh := channel.NewRemoteResultChannel("test")
			entry := &proto.LogEntry{
				SegId:   100,
				EntryId: 0,
				Values:  []byte("test data"),
			}

			entryId, err := remoteClient.AppendEntry(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logId, entry, resultCh)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), entryId)

			// Close the client
			err = remoteClient.Close(context.TODO())
			assert.NoError(t, err)

			// Subsequent appends should fail
			_, err = remoteClient.AppendEntry(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logId, entry, resultCh)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "client is closed")
		})
	}
}
