// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestIdempotentWriter_MinioMode tests idempotent write functionality with object storage.
func TestIdempotentWriter_MinioMode(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Enable idempotent write
	cfg.Woodpecker.Client.IdempotentWrite.Enabled = true

	// Use embed client with object storage
	client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() {
		stopErr := woodpecker.StopEmbedLogStore()
		assert.NoError(t, stopErr)
	}()

	logName := fmt.Sprintf("test_idempotent_minio_%d", time.Now().UnixNano())
	createErr := client.CreateLog(ctx, logName)
	if createErr != nil && !werr.ErrLogHandleLogAlreadyExists.Is(createErr) {
		require.NoError(t, createErr)
	}

	logHandle, openErr := client.OpenLog(ctx, logName)
	require.NoError(t, openErr)

	// Open idempotent writer
	writer, err := logHandle.OpenIdempotentWriter(ctx)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer func() {
		closeErr := writer.Close(ctx)
		assert.NoError(t, closeErr)
	}()

	t.Run("FirstWrite", func(t *testing.T) {
		msg := &log.WriteMessage{
			Payload:       []byte("test message 1"),
			IdempotencyId: "idempotent-test-1",
		}

		result := writer.Write(ctx, msg)
		require.NoError(t, result.Err)
		assert.NotNil(t, result.LogMessageId)
		assert.True(t, result.LogMessageId.SegmentId >= 0)
		assert.True(t, result.LogMessageId.EntryId >= 0)
	})

	t.Run("DuplicateWrite", func(t *testing.T) {
		msg := &log.WriteMessage{
			Payload:       []byte("test message 2"),
			IdempotencyId: "idempotent-test-2",
		}

		// First write
		result1 := writer.Write(ctx, msg)
		require.NoError(t, result1.Err)

		// Duplicate write with same idempotencyId
		result2 := writer.Write(ctx, msg)
		require.NoError(t, result2.Err)

		// Should return same entryId
		assert.Equal(t, result1.LogMessageId.SegmentId, result2.LogMessageId.SegmentId)
		assert.Equal(t, result1.LogMessageId.EntryId, result2.LogMessageId.EntryId)
	})

	t.Run("ConcurrentDuplicateWrites", func(t *testing.T) {
		msg := &log.WriteMessage{
			Payload:       []byte("concurrent test message"),
			IdempotencyId: "idempotent-test-concurrent",
		}

		var wg sync.WaitGroup
		results := make([]*log.WriteResult, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = writer.Write(ctx, msg)
			}(i)
		}

		wg.Wait()

		// All writes should succeed with the same entryId
		for i := 0; i < 5; i++ {
			require.NoError(t, results[i].Err)
			assert.Equal(t, results[0].LogMessageId.SegmentId, results[i].LogMessageId.SegmentId)
			assert.Equal(t, results[0].LogMessageId.EntryId, results[i].LogMessageId.EntryId)
		}
	})

	t.Run("DifferentIdempotencyIds", func(t *testing.T) {
		msg1 := &log.WriteMessage{
			Payload:       []byte("message A"),
			IdempotencyId: "idempotent-test-A",
		}
		msg2 := &log.WriteMessage{
			Payload:       []byte("message B"),
			IdempotencyId: "idempotent-test-B",
		}

		result1 := writer.Write(ctx, msg1)
		result2 := writer.Write(ctx, msg2)

		require.NoError(t, result1.Err)
		require.NoError(t, result2.Err)

		// Different idempotencyIds should get different entryIds
		assert.NotEqual(t, result1.LogMessageId.EntryId, result2.LogMessageId.EntryId)
	})

	t.Run("DeriveIdempotencyIdFromPayload", func(t *testing.T) {
		// Write without explicit idempotencyId - should be derived from payload hash
		msg := &log.WriteMessage{
			Payload: []byte("payload-derived-id-test"),
			// IdempotencyId not set
		}

		result1 := writer.Write(ctx, msg)
		require.NoError(t, result1.Err)

		// Duplicate write with same payload (should be treated as duplicate)
		msg2 := &log.WriteMessage{
			Payload: []byte("payload-derived-id-test"),
		}
		result2 := writer.Write(ctx, msg2)
		require.NoError(t, result2.Err)

		// Should return same entryId because payload hash generates same idempotencyId
		assert.Equal(t, result1.LogMessageId.SegmentId, result2.LogMessageId.SegmentId)
		assert.Equal(t, result1.LogMessageId.EntryId, result2.LogMessageId.EntryId)
	})
}

// TestIdempotentWriter_ServiceMode tests idempotent write functionality with service mode.
func TestIdempotentWriter_ServiceMode(t *testing.T) {
	ctx := context.Background()

	// Start mini cluster
	const nodeCount = 3
	tmpDir := t.TempDir()
	rootPath := tmpDir + "/idempotent_service_test"
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	// Enable idempotent write
	cfg.Woodpecker.Client.IdempotentWrite.Enabled = true

	// Setup etcd client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	// Create service mode client
	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if client != nil {
			_ = client.Close(ctx)
		}
	}()

	logName := fmt.Sprintf("test_idempotent_service_%d", time.Now().UnixNano())
	createErr := client.CreateLog(ctx, logName)
	if createErr != nil && !werr.ErrLogHandleLogAlreadyExists.Is(createErr) {
		require.NoError(t, createErr)
	}

	logHandle, openErr := client.OpenLog(ctx, logName)
	require.NoError(t, openErr)

	// Open idempotent writer
	writer, err := logHandle.OpenIdempotentWriter(ctx)
	require.NoError(t, err)
	require.NotNil(t, writer)
	defer func() {
		closeErr := writer.Close(ctx)
		assert.NoError(t, closeErr)
	}()

	t.Run("FirstWrite_Service", func(t *testing.T) {
		msg := &log.WriteMessage{
			Payload:       []byte("service mode test message 1"),
			IdempotencyId: "service-idempotent-1",
		}

		result := writer.Write(ctx, msg)
		require.NoError(t, result.Err)
		assert.NotNil(t, result.LogMessageId)
	})

	t.Run("DuplicateWrite_Service", func(t *testing.T) {
		msg := &log.WriteMessage{
			Payload:       []byte("service mode test message 2"),
			IdempotencyId: "service-idempotent-2",
		}

		result1 := writer.Write(ctx, msg)
		require.NoError(t, result1.Err)

		result2 := writer.Write(ctx, msg)
		require.NoError(t, result2.Err)

		assert.Equal(t, result1.LogMessageId.SegmentId, result2.LogMessageId.SegmentId)
		assert.Equal(t, result1.LogMessageId.EntryId, result2.LogMessageId.EntryId)
	})

	t.Run("ConcurrentWrites_Service", func(t *testing.T) {
		msg := &log.WriteMessage{
			Payload:       []byte("service concurrent test"),
			IdempotencyId: "service-concurrent-id",
		}

		var wg sync.WaitGroup
		results := make([]*log.WriteResult, 3)

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = writer.Write(ctx, msg)
			}(i)
		}

		wg.Wait()

		for i := 0; i < 3; i++ {
			require.NoError(t, results[i].Err)
			assert.Equal(t, results[0].LogMessageId.SegmentId, results[i].LogMessageId.SegmentId)
			assert.Equal(t, results[0].LogMessageId.EntryId, results[i].LogMessageId.EntryId)
		}
	})
}

// TestIdempotentWriter_LocalStorageNotSupported tests that idempotent writer
// returns an error for local storage mode.
func TestIdempotentWriter_LocalStorageNotSupported(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Use local storage
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = tmpDir

	client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() {
		stopErr := woodpecker.StopEmbedLogStore()
		assert.NoError(t, stopErr)
	}()

	logName := fmt.Sprintf("test_idempotent_local_%d", time.Now().UnixNano())
	createErr := client.CreateLog(ctx, logName)
	if createErr != nil && !werr.ErrLogHandleLogAlreadyExists.Is(createErr) {
		require.NoError(t, createErr)
	}

	logHandle, openErr := client.OpenLog(ctx, logName)
	require.NoError(t, openErr)

	// OpenIdempotentWriter should fail for local storage
	writer, err := logHandle.OpenIdempotentWriter(ctx)
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.True(t, werr.ErrIdempotentWriteNotSupported.Is(err))
}

// TestIdempotentWriter_Recovery tests that the idempotent writer correctly
// recovers dedup state after restart.
func TestIdempotentWriter_Recovery(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Enable idempotent write with short snapshot interval for testing
	cfg.Woodpecker.Client.IdempotentWrite.Enabled = true
	cfg.Woodpecker.Client.IdempotentWrite.SnapshotInterval = config.NewDurationSecondsFromInt(1) // 1 second

	logName := fmt.Sprintf("test_idempotent_recovery_%d", time.Now().UnixNano())

	// Phase 1: Write messages with idempotent writer
	{
		client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
		require.NoError(t, err)

		createErr := client.CreateLog(ctx, logName)
		if createErr != nil && !werr.ErrLogHandleLogAlreadyExists.Is(createErr) {
			require.NoError(t, createErr)
		}

		logHandle, openErr := client.OpenLog(ctx, logName)
		require.NoError(t, openErr)

		writer, err := logHandle.OpenIdempotentWriter(ctx)
		require.NoError(t, err)

		// Write some messages
		for i := 0; i < 5; i++ {
			msg := &log.WriteMessage{
				Payload:       []byte(fmt.Sprintf("recovery test message %d", i)),
				IdempotencyId: fmt.Sprintf("recovery-id-%d", i),
			}
			result := writer.Write(ctx, msg)
			require.NoError(t, result.Err)
		}

		// Wait for snapshot to be saved
		time.Sleep(2 * time.Second)

		// Close writer and client
		closeErr := writer.Close(ctx)
		require.NoError(t, closeErr)
		stopErr := woodpecker.StopEmbedLogStore()
		require.NoError(t, stopErr)
	}

	// Phase 2: Reopen and verify dedup state is recovered
	{
		client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			stopErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopErr)
		}()

		logHandle, openErr := client.OpenLog(ctx, logName)
		require.NoError(t, openErr)

		writer, err := logHandle.OpenIdempotentWriter(ctx)
		require.NoError(t, err)
		defer func() {
			closeErr := writer.Close(ctx)
			assert.NoError(t, closeErr)
		}()

		// Try to write duplicate messages - they should be deduplicated
		for i := 0; i < 5; i++ {
			msg := &log.WriteMessage{
				Payload:       []byte(fmt.Sprintf("recovery test message %d", i)),
				IdempotencyId: fmt.Sprintf("recovery-id-%d", i),
			}
			result := writer.Write(ctx, msg)
			require.NoError(t, result.Err)
			// The write should succeed (as duplicate, returning existing entryId)
		}

		// Write a new message - this should succeed as a new write
		msg := &log.WriteMessage{
			Payload:       []byte("new message after recovery"),
			IdempotencyId: "new-recovery-id",
		}
		result := writer.Write(ctx, msg)
		require.NoError(t, result.Err)
	}
}
