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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestStagedStorageService_Normal_BasicRW tests basic read-write functionality
func TestStagedStorageService_Normal_BasicRW(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_BasicRW")

	// Start a 3-node cluster for quorum testing
	const nodeCount = 3
	cluster, cfg, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	seedList := strings.Join(seeds, ",")
	cfg.Woodpecker.Client.ServiceSeedNodes = seedList // set service seed nodes
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()
	// Setup etcd client for woodpecker client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	assert.NoError(t, err)
	defer etcdCli.Close()

	// Create woodpecker client using etcd for service discovery
	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	assert.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			_ = woodpeckerClient.Close(ctx)
		}
	}()

	// Create a unique log name for this test
	logName := "test_log_quorum_basic_" + time.Now().Format("20060102150405")
	t.Logf("Creating log: %s", logName)

	// Create log if not exists
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr), "Error should be 'log already exists' but got: %v", createErr)
	}

	// Open log handle
	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	assert.NoError(t, openErr)
	assert.NotNil(t, logHandle)

	// Test 1: Open log writers
	t.Logf("Opening log writer...")
	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	assert.NoError(t, openWriterErr)
	assert.NotNil(t, logWriter)

	// Test 2: Write data using writer
	t.Logf("Writing data using multiple writers...")

	// Write some entries using writer1
	testData := [][]byte{
		[]byte("test data from writer1 - entry 1"),
		[]byte("test data from writer1 - entry 2"),
		[]byte("test data from writer1 - entry 3"),
		[]byte("test data from writer2 - entry 4"),
		[]byte("test data from writer2 - entry 5"),
	}

	for i, data := range testData {
		writeMsg := &log.WriteMessage{
			Payload: data,
		}
		result := logWriter.Write(ctx, writeMsg)
		assert.NoError(t, result.Err)
		assert.NotNil(t, result.LogMessageId)
		assert.Equal(t, int64(i), result.LogMessageId.EntryId)
		t.Logf("Writer wrote entry %d with data: %s", result.LogMessageId.EntryId, string(data))
	}

	// Test 3: Read data back and verify
	t.Logf("Reading data back to verify...")

	// Start reading from the beginning
	startMsgId := &log.LogMessageId{
		SegmentId: 0,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-reader")
	assert.NoError(t, openReaderErr)
	assert.NotNil(t, logReader)

	// Read all entries using ReadNext
	totalEntries := len(testData)
	allTestData := testData

	var readMessages []*log.LogMessage
	for i := 0; i < totalEntries; i++ {
		msg, readErr := logReader.ReadNext(ctx)
		assert.NoError(t, readErr)
		assert.NotNil(t, msg)
		readMessages = append(readMessages, msg)
		t.Logf("Read entry %d: %s", msg.Id.EntryId, string(msg.Payload))
	}

	// Verify the content of all entries
	assert.Len(t, readMessages, totalEntries)
	for i, msg := range readMessages {
		assert.Equal(t, int64(i), msg.Id.EntryId)
		assert.Equal(t, allTestData[i], msg.Payload)
		t.Logf("Verified entry %d: %s", msg.Id.EntryId, string(msg.Payload))
	}

	// Test 4: Close resources
	t.Logf("Cleaning up resources...")
	assert.NoError(t, logWriter.Close(ctx))
	assert.NoError(t, logReader.Close(ctx))
	assert.NoError(t, logHandle.Close(ctx))

	t.Logf("TestStagedStorageService_Basic completed successfully with %d nodes", len(cluster.Servers))
}

// TestStagedStorageService_Failover_Case1_NodeFailure_WriteReaderContinues tests failover case 1:
// es=3, wq=3, aq=2 configuration: Start a writer continuously writing data, a reader continuously tail reading data.
// When writer writes 20 entries, kill one of the current writable segment's quorum nodes;
// Writer should continue writing 10 more entries, ultimately writer writes 30 entries normally,
// reader should also read 30 entries normally.
// Simplified test to verify segment rolling basic functionality
func TestStagedStorageService_Failover_Simple_SegmentRollingVerification(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Simple")

	// Start minimal 3-node cluster for basic quorum testing
	const nodeCount = 3
	cluster, cfg, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	seedList := strings.Join(seeds, ",")
	cfg.Woodpecker.Client.ServiceSeedNodes = seedList

	// Ensure proper cleanup
	defer func() {
		t.Logf("Starting cluster cleanup...")
		cluster.StopMultiNodeCluster(t)
		t.Logf("Cluster cleanup completed")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup etcd client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	assert.NoError(t, err)
	defer etcdCli.Close()

	// Create woodpecker client
	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	assert.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	// Create a unique log name
	logName := "test_log_simple_" + time.Now().Format("20060102150405")
	t.Logf("Creating log: %s", logName)

	// Create and open log
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr), "Unexpected error: %v", createErr)
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	assert.NoError(t, openErr)
	assert.NotNil(t, logHandle)

	// Phase 1: Write some initial entries successfully
	t.Logf("Phase 1: Writing initial entries...")
	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	assert.NoError(t, openWriterErr)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// Write 5 entries successfully first
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("test data entry %d", i))
		writeMsg := &log.WriteMessage{Payload: data}

		result := logWriter.Write(ctx, writeMsg)
		assert.NoError(t, result.Err, "Failed to write entry %d", i)
		assert.NotNil(t, result.LogMessageId)
		t.Logf("Successfully wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
	}

	// Phase 2: Simulate node failure by stopping one node
	t.Logf("Phase 2: Simulating node failure...")

	// Get current quorum information (if available)
	if currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx); currentSegmentHandle != nil {
		segmentMetadata := currentSegmentHandle.GetMetadata(ctx)
		if segmentMetadata != nil {
			t.Logf("Current segment ID: %d", segmentMetadata.Metadata.SegNo)
		}

		quorumInfo, qErr := currentSegmentHandle.GetQuorumInfo(ctx)
		if qErr == nil && quorumInfo != nil {
			t.Logf("Current quorum nodes: %v", quorumInfo.Nodes)
		}
	}

	// Stop one node (simulate node failure)
	activeNodes := cluster.GetActiveNodeIndexes()
	if len(activeNodes) > 2 {
		nodeToStop := activeNodes[0] // Stop the first node
		t.Logf("Stopping node %d to simulate failure", nodeToStop)
		_, err := cluster.LeaveNodeWithIndex(t, nodeToStop)
		assert.NoError(t, err)

		// Give system time to detect the failure
		time.Sleep(2 * time.Second)
	}

	// Phase 3: Try to write more entries (this should trigger segment rolling if our logic works)
	t.Logf("Phase 3: Writing entries after node failure to trigger rolling...")

	for i := 5; i <= 6; i++ {
		data := []byte(fmt.Sprintf("test data entry %d (after failure)", i))
		writeMsg := &log.WriteMessage{Payload: data}

		// Use without ctx timeout for individual writes
		// it should wait for response until operation timeout(3 retry*30s=90s)
		writeCtx, writeCancel := context.WithTimeout(ctx, 100*time.Second)
		result := logWriter.Write(writeCtx, writeMsg)
		writeCancel()
		if result.Err != nil {
			// entry 6 will fail，because segment is rolling state
			t.Logf("Write failed for entry %d (expected during failover): %v", i, result.Err)
			break
		}
		// entry 5 will success
		// Although one failed, two succeeded and the rolling was triggered
		t.Logf("Successfully wrote entry %d after node failure", i)
	}

	// Phase 4: Verify basic functionality
	t.Logf("Phase 4: Verifying basic read functionality...")

	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-simple-reader")
	assert.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	// Try to read some entries
	readCount := 0
	maxReads := 10
	for i := 0; i < maxReads; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()

		if readErr != nil {
			t.Logf("Read error (attempt %d): %v", i+1, readErr)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if msg != nil {
			readCount++
			t.Logf("Read entry %d: %s", msg.Id.EntryId, string(msg.Payload))
		}

	}

	t.Logf("Test completed - successfully read %d entries", readCount)
	assert.GreaterOrEqual(t, readCount, 5, "Should be able to read at least 5 entries")
	assert.NotEqual(t, readCount, 10, "Should not be able to read 10 entries")
}
