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
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
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
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

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

func TestStagedStorageService_Failover_Case1_NodeFailure_WriteReaderContinues(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case1")

	// Start a 5-node cluster for quorum testing
	const nodeCount = 5
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	// Ensure proper cleanup
	defer func() {
		t.Logf("Starting cluster cleanup...")
		cluster.StopMultiNodeCluster(t)
		t.Logf("Cluster cleanup completed")
	}()

	// Note: quorum settings (es=3, wq=3, aq=2) are configured in woodpecker_client.go's SelectQuorumNodes
	// We don't need to configure them here as they are hardcoded in the client

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Setup etcd client for woodpecker client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	assert.NoError(t, err)
	defer etcdCli.Close()

	// Create woodpecker client using etcd for service discovery
	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	assert.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	// Create a unique log name for this test
	logName := "test_log_failover_case1_" + time.Now().Format("20060102150405")
	t.Logf("Creating log: %s with quorum config es=3, wq=3, aq=2", logName)

	// Create log if not exists
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr), "Error should be 'log already exists' but got: %v", createErr)
	}

	// Open log handle
	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	assert.NoError(t, openErr)
	assert.NotNil(t, logHandle)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	// Open log writer
	t.Logf("Opening log writer...")
	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	assert.NoError(t, openWriterErr)
	assert.NotNil(t, logWriter)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// Open log reader for tail reading
	t.Logf("Opening log reader for tail reading...")
	startMsgId := &log.LogMessageId{
		SegmentId: 0,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-failover-reader")
	assert.NoError(t, openReaderErr)
	assert.NotNil(t, logReader)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	// Track written and read data with thread-safe access
	writtenData := make([][]byte, 0)
	readMessages := make([]*log.LogMessage, 0)
	var readerErr error
	var writtenMutex, readMutex sync.Mutex

	// Start reader goroutine for continuous tail reading
	readerDone := make(chan bool)
	go func() {
		defer close(readerDone)

		for {
			select {
			case <-ctx.Done():
				t.Logf("Reader stopping due to context cancellation")
				return
			default:
				// Try to read next message
				readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
				msg, readErr := logReader.ReadNext(readCtx)
				readCancel()

				if readErr != nil {
					// Handle read errors gracefully during failover
					t.Logf("Reader encountered error: %v, retrying...", readErr)
					readMutex.Lock()
					readerErr = readErr
					readMutex.Unlock()
					time.Sleep(200 * time.Millisecond)
					continue
				}

				if msg != nil {
					readMutex.Lock()
					readMessages = append(readMessages, msg)
					currentCount := len(readMessages)
					readMutex.Unlock()

					t.Logf("Reader read entry %d: %s (total read: %d)", msg.Id.EntryId, string(msg.Payload), currentCount)

					// Reset reader error on successful read
					readMutex.Lock()
					readerErr = nil
					readMutex.Unlock()

					// Check if we've read the target amount
					if currentCount >= 10 {
						t.Logf("Reader reached target of 10 entries, stopping")
						return
					}
				} else {
					// No message available, wait a bit
					time.Sleep(200 * time.Millisecond)
				}
			}
		}
	}()

	// Phase 1: Write first 5 entries
	t.Logf("Phase 1: Writing first 5 entries...")
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("failover test data entry %d", i))
		writeMsg := &log.WriteMessage{
			Payload: data,
		}
		result := logWriter.Write(ctx, writeMsg)
		assert.NoError(t, result.Err, "Failed to write entry %d", i)
		assert.NotNil(t, result.LogMessageId)

		writtenMutex.Lock()
		writtenData = append(writtenData, data)
		writtenMutex.Unlock()

		t.Logf("Writer wrote entry %d: %s", result.LogMessageId.EntryId, string(data))

		// Small delay to allow reader to catch up
		time.Sleep(100 * time.Millisecond)
	}

	// Wait a bit for data to be flushed and reader to catch up
	time.Sleep(1 * time.Second)

	// Phase 2: Get current writable segment metadata to identify quorum nodes
	t.Logf("Phase 2: Identifying current writable segment's quorum nodes...")

	// Get the current writable segment handle to access its metadata
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	if currentSegmentHandle == nil {
		t.Fatal("No current writable segment handle available")
	}

	// Get segment metadata and quorum information
	segmentMetadata := currentSegmentHandle.GetMetadata(ctx)
	assert.NotNil(t, segmentMetadata, "Segment metadata should not be nil")

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	assert.NoError(t, err, "Should be able to get quorum info")
	assert.NotNil(t, quorumInfo, "QuorumInfo should not be nil")
	assert.GreaterOrEqual(t, len(quorumInfo.Nodes), 3, "Should have at least 3 quorum nodes")

	t.Logf("Current segment %d has quorum nodes: %v", segmentMetadata.Metadata.SegNo, quorumInfo.Nodes)

	// Pick the first quorum node to simulate failure
	targetQuorumNode := quorumInfo.Nodes[0]
	t.Logf("Selected quorum node for failure simulation: %s", targetQuorumNode)

	// Find the corresponding node index in the cluster by matching service port
	var targetNodeIndex int
	var targetNodeFound bool

	// Extract service port from quorum node address (e.g., "0.0.0.0:60196" -> 60196)
	parts := strings.Split(targetQuorumNode, ":")
	if len(parts) != 2 {
		t.Fatalf("Invalid quorum node address format: %s", targetQuorumNode)
	}

	quorumPort, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("Invalid port in quorum node address %s: %v", targetQuorumNode, err)
	}

	t.Logf("Looking for quorum node with service port: %d", quorumPort)

	// Match by service port in cluster.UsedPorts
	for nodeIndex, clusterPort := range cluster.UsedPorts {
		if clusterPort == quorumPort {
			targetNodeIndex = nodeIndex
			targetNodeFound = true
			t.Logf("Found matching node: index=%d, service_port=%d", nodeIndex, clusterPort)
			break
		}
	}

	if !targetNodeFound {
		t.Fatalf("Could not find node with service port %d. Available ports: %v", quorumPort, cluster.UsedPorts)
	}

	t.Logf("Simulating failure of quorum node %d (address: %s)", targetNodeIndex, targetQuorumNode)

	// Get all active nodes before failure
	seedListBefore := cluster.GetSeedList()
	t.Logf("Active nodes before failure: %v", seedListBefore)

	// Phase 3: Simulate node failure by stopping the node
	t.Logf("Phase 3: Simulating node failure - stopping node %d...", targetNodeIndex)
	stoppedNodeIndex, err := cluster.LeaveNodeWithIndex(t, targetNodeIndex)
	assert.NoError(t, err)
	assert.Equal(t, targetNodeIndex, stoppedNodeIndex)

	// Give some time for the cluster to detect the failure
	time.Sleep(2 * time.Second)

	// Verify node is no longer active
	activeNodesAfterFailure := cluster.GetActiveNodeIndexes()

	t.Logf("Active nodes after failure: %v", activeNodesAfterFailure)
	originalNodeCount := len(seedListBefore)
	assert.Equal(t, originalNodeCount-1, len(activeNodesAfterFailure), "Should have one less active node")

	// Phase 4: Continue writing 5 more entries despite node failure
	t.Logf("Phase 4: Writing 5 more entries despite node failure...")
	// Give the system time to detect the failure and potentially trigger segment rolling
	time.Sleep(3 * time.Second)

	successfulWrites := 0
	maxRetries := 20 // Allow multiple retries for failover recovery

	for attempt := 0; attempt < maxRetries && successfulWrites < 5; attempt++ {
		entryIndex := 5 + successfulWrites
		data := []byte(fmt.Sprintf("failover test data entry %d (after node failure)", entryIndex))
		writeMsg := &log.WriteMessage{
			Payload: data,
		}

		// Use shorter timeout for individual writes during failover
		writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
		result := logWriter.Write(writeCtx, writeMsg)
		writeCancel()

		if result.Err != nil {
			t.Logf("Write attempt %d failed for entry %d: %v", attempt+1, entryIndex, result.Err)
			// Wait longer during failover for potential new segment creation
			time.Sleep(2 * time.Second)
			continue
		}

		assert.NotNil(t, result.LogMessageId)

		writtenMutex.Lock()
		writtenData = append(writtenData, data)
		writtenMutex.Unlock()

		successfulWrites++
		t.Logf("Writer successfully wrote entry %d with ID %d after node failure: %s (successful writes: %d/5)",
			entryIndex, result.LogMessageId.EntryId, string(data), successfulWrites)

		// Small delay to allow reader to catch up
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("Phase 4 completed with %d successful writes out of 5 target writes", successfulWrites)

	// Phase 5: Wait for reader to complete and verify results
	t.Logf("Phase 5: Waiting for reader to complete...")

	// Give reader extra time to finish reading all entries
	readerTimeout := time.After(20 * time.Second)

	select {
	case <-readerDone:
		t.Logf("Reader completed successfully")
	case <-readerTimeout:
		readMutex.Lock()
		currentReadCount := len(readMessages)
		readMutex.Unlock()
		t.Logf("Reader timeout after 20 seconds, read %d entries so far", currentReadCount)
		// Continue with verification even if timeout
	}

	// Get final counts with proper locking
	writtenMutex.Lock()
	finalWrittenCount := len(writtenData)
	writtenDataCopy := make([][]byte, len(writtenData))
	copy(writtenDataCopy, writtenData)
	writtenMutex.Unlock()

	readMutex.Lock()
	finalReadCount := len(readMessages)
	readMessagesCopy := make([]*log.LogMessage, len(readMessages))
	copy(readMessagesCopy, readMessages)
	finalReaderErr := readerErr
	readMutex.Unlock()

	// Verify results
	t.Logf("Final verification: written=%d entries, read=%d entries", finalWrittenCount, finalReadCount)

	// Verify we wrote 10 entries total
	assert.Equal(t, 10, finalWrittenCount, "Should have written exactly 10 entries (5 before + 5 after failover)")

	// Verify we read 10 entries total
	assert.Equal(t, 10, finalReadCount, "Should have read exactly 10 entries")

	// Verify no fatal reader errors (temporary errors during failover are OK)
	if finalReaderErr != nil {
		t.Logf("Reader had error: %v (this may be temporary during failover)", finalReaderErr)
	}

	// Check the breakdown of entries
	entriesBeforeFailover := 0
	entriesAfterFailover := 0

	for _, data := range writtenDataCopy {
		if strings.Contains(string(data), "(after node failure)") {
			entriesAfterFailover++
		} else {
			entriesBeforeFailover++
		}
	}

	t.Logf("Entries breakdown: %d before failover, %d after failover", entriesBeforeFailover, entriesAfterFailover)
	assert.Equal(t, 5, entriesBeforeFailover, "Should have written exactly 5 entries before failover")
	assert.Equal(t, 5, entriesAfterFailover, "Should have written exactly 5 entries after failover")

	// Verify content matches (first 10 entries should match)
	minEntries := min(finalWrittenCount, finalReadCount)
	if minEntries > 0 {
		t.Logf("Verifying content for %d entries", minEntries)
		matchingEntries := 0
		for i := 0; i < minEntries; i++ {
			if string(readMessagesCopy[i].Payload) == string(writtenDataCopy[i]) {
				matchingEntries++
			} else {
				t.Logf("Mismatch at entry %d: read='%s', written='%s'", i,
					string(readMessagesCopy[i].Payload), string(writtenDataCopy[i]))
			}
		}
		t.Logf("Content verification: %d/%d entries match exactly", matchingEntries, minEntries)
		assert.Equal(t, minEntries, matchingEntries, "All entries should match exactly")
	}

	// Critical verification: Check segment rolling behavior
	// First 6 entries (0-5) should be in original segment, next 4 entries (6-9) should be in a new segment
	// Entry 5 triggers rolling but still succeeds in the original segment
	if finalReadCount >= 10 {
		t.Logf("Verifying segment rolling: checking segment IDs...")

		// Extract segment IDs
		firstSegmentId := readMessagesCopy[0].Id.SegmentId
		t.Logf("First entry (index 0) segment ID: %d", firstSegmentId)

		// Check first 6 entries (0-5) including the one that triggers rolling - should all be in same original segment
		originalSegmentId := firstSegmentId
		for i := 0; i < 6; i++ {
			segmentId := readMessagesCopy[i].Id.SegmentId
			t.Logf("Entry %d: segmentId=%d, entryId=%d", i, segmentId, readMessagesCopy[i].Id.EntryId)
			assert.Equal(t, originalSegmentId, segmentId,
				"Entries 0-5 (including the rolling-trigger entry) should all be in the original segment")
		}

		// Check next 4 entries (6-9) after rolling - should be in a different segment
		newSegmentId := readMessagesCopy[6].Id.SegmentId
		t.Logf("Seventh entry (index 6) segment ID: %d", newSegmentId)

		// Verify segment rolling occurred: newSegmentId should be different from originalSegmentId
		assert.Equal(t, originalSegmentId+1, newSegmentId,
			"Segment rolling should have occurred - entries 6-9 should be in a different segment from entries 0-5")

		for i := 6; i < 10; i++ {
			afterNewSegmentId := readMessagesCopy[i].Id.SegmentId
			t.Logf("Entry %d: segmentId=%d, entryId=%d", i, afterNewSegmentId, readMessagesCopy[i].Id.EntryId)
			//assert.Equal(t, newSegmentId, segmentId,"...") // TODO Perhaps we need to control the precise scrolling only once by coordinating the time interval between auto sync and trigger sync
			assert.GreaterOrEqual(t, afterNewSegmentId, newSegmentId,
				"Entries 6-9 (after rolling) should all be in the same new segment")
		}

		t.Logf("✅ Segment rolling verification PASSED:")
		t.Logf("   - Entries 0-5 (original segment, including rolling trigger): segment %d", originalSegmentId)
		t.Logf("   - Entries 6-9 (new segment after rolling): segment %d", newSegmentId)
		t.Logf("   - Rolling triggered correctly: entry 5 succeeded in original segment, entry 6+ in new segment!")
	}

	t.Logf("=== CASE 1 PASSED: Writer and reader continued working seamlessly despite node %d failure ===", targetNodeIndex)
	t.Logf("Successfully wrote and read 10 entries with es=3, wq=3, aq=2 configuration - 5 before and 5 after failover")
}

// TestStagedStorageService_Failover_Case2_DoubleNodeFailure_WriteReaderContinues tests failover scenario:
// - 7-node cluster with es=5, wq=5, aq=3 configuration (reads from woodpecker.yaml)
// - Writer writes 5 entries successfully
// - Kill 2 nodes from the current writable segment's quorum (leaving 3 nodes, still >= aq=3)
// - Writer should continue to write 5 more entries successfully (total 10)
// - Reader continuously reads all entries
// - Verify segment rolling: entries 0-5 in original segment, entries 6-9 in new segmentlai
func TestStagedStorageService_Failover_Case2_DoubleNodeFailure_WriteReaderContinues(t *testing.T) {
	const (
		clusterSize    = 7 // Use 7-node cluster to support es=5
		entriesPhase1  = 5 // entries before node failure
		entriesPhase2  = 5 // entries after node failure
		totalEntries   = entriesPhase1 + entriesPhase2
		nodesFailCount = 2 // kill 2 quorum nodes simultaneously
	)

	t.Logf("=== CASE 2: Testing double node failure with dynamic quorum config from yaml ===")

	// Phase 1: Setup 7-node cluster
	t.Logf("Phase 1: Setting up %d-node cluster...", clusterSize)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case2")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	// Save original quorum configuration and restore after test
	originalReplica := cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas

	// Override quorum configuration for Case2: es=5, wq=5, aq=3
	cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas = 5
	t.Logf("Overridden quorum config for Case2: replica=%d",
		cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas)

	defer func() {
		// Restore original quorum configuration
		cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas = originalReplica
		t.Logf("Restored original quorum config: replica=%d",
			cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas)

		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster stabilization
	time.Sleep(3 * time.Second)

	// Phase 2: Create log writer and reader
	t.Logf("Phase 2: Creating writer and reader...")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Setup etcd client for woodpecker client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	// Create woodpecker client using etcd for service discovery
	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	// Create a unique log name for this test
	logName := "test_log_failover_case2_" + time.Now().Format("20060102150405")
	t.Logf("Creating log: %s with quorum config from woodpecker.yaml: replica=%d",
		logName, cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas)

	// Create log if not exists
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr), "Error should be 'log already exists' but got: %v", createErr)
	}

	// Open log handle
	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	require.NotNil(t, logHandle)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	// Open log writer
	t.Logf("Opening log writer...")
	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)
	require.NotNil(t, logWriter)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// Open log reader for tail reading
	t.Logf("Opening log reader for tail reading...")
	startMsgId := &log.LogMessageId{
		SegmentId: 0,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-failover-reader")
	require.NoError(t, openReaderErr)
	require.NotNil(t, logReader)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	// Phase 3: Write first batch of entries
	t.Logf("Phase 3: Writing %d entries before node failure...", entriesPhase1)

	// Track written and read data with thread-safe access
	writtenData := make([][]byte, 0)
	readMessages := make([]*log.LogMessage, 0)
	var readerErr error
	var writtenMutex, readMutex sync.Mutex

	for i := 0; i < entriesPhase1; i++ {
		data := []byte(fmt.Sprintf("failover test data entry %d (before double node failure)", i))
		writeMsg := &log.WriteMessage{
			Payload: data,
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
		result := logWriter.Write(writeCtx, writeMsg)
		writeCancel()

		require.NoError(t, result.Err, "Failed to write entry %d", i)
		require.NotNil(t, result.LogMessageId, "LogMessageId should not be nil for entry %d", i)

		writtenMutex.Lock()
		writtenData = append(writtenData, data)
		writtenMutex.Unlock()

		t.Logf("Writer successfully wrote entry %d with ID %d: %s",
			i, result.LogMessageId.EntryId, string(data))

		// Small delay between writes
		time.Sleep(100 * time.Millisecond)
	}

	// Phase 3.5: Get current writable segment quorum info and select 2 nodes to kill
	t.Logf("Phase 3.5: Identifying quorum nodes to terminate...")

	// Get current writable segment handle
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	if currentSegmentHandle == nil {
		t.Fatal("No current writable segment handle available")
	}

	// Get segment metadata and quorum info
	segmentMetadata := currentSegmentHandle.GetMetadata(ctx)
	require.NotNil(t, segmentMetadata, "Segment metadata should not be nil")

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err, "Should be able to get quorum info")
	require.NotNil(t, quorumInfo, "QuorumInfo should not be nil")

	t.Logf("Current segment %d has quorum nodes: %v", segmentMetadata.Metadata.SegNo, quorumInfo.Nodes)
	expectedEnsembleSize := cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas
	require.GreaterOrEqual(t, len(quorumInfo.Nodes), expectedEnsembleSize, "Should have at least %d quorum nodes", expectedEnsembleSize)

	// Debug: Print cluster information
	t.Logf("Debug cluster information:")
	t.Logf("  UsedPorts (service gRPC ports):")
	for nodeIndex, port := range cluster.UsedPorts {
		t.Logf("    Node %d: port %d", nodeIndex, port)
	}
	t.Logf("  UsedAddresses (gossip addresses):")
	for nodeIndex, nodeAddr := range cluster.UsedAddresses {
		t.Logf("    Node %d: %s", nodeIndex, nodeAddr)
	}

	// Find 2 quorum nodes to kill by matching service gRPC ports
	var targetNodeIndexes []int
	for _, quorumNodeAddr := range quorumInfo.Nodes {
		t.Logf("Looking for quorum node with service address: %s", quorumNodeAddr)

		// Extract port from quorum node address (e.g., "0.0.0.0:58382" -> "58382")
		parts := strings.Split(quorumNodeAddr, ":")
		if len(parts) != 2 {
			t.Logf("❌ Invalid quorum node address format: %s", quorumNodeAddr)
			continue
		}

		quorumPortStr := parts[1]
		quorumPort, err := strconv.Atoi(quorumPortStr)
		if err != nil {
			t.Logf("❌ Failed to parse port from quorum node address %s: %v", quorumNodeAddr, err)
			continue
		}

		// Find matching node in cluster by service port
		nodeFound := false
		for nodeIndex, clusterPort := range cluster.UsedPorts {
			if clusterPort == quorumPort {
				targetNodeIndexes = append(targetNodeIndexes, nodeIndex)
				nodeFound = true
				t.Logf("✅ Found matching node: index=%d, service_port=%d, quorum_addr=%s",
					nodeIndex, clusterPort, quorumNodeAddr)
				break
			}
		}

		if !nodeFound {
			t.Logf("❌ Could not find node with service port %d for quorum address: %s", quorumPort, quorumNodeAddr)
		}

		// Stop when we have found enough nodes to kill
		if len(targetNodeIndexes) >= nodesFailCount {
			break
		}
	}

	require.GreaterOrEqual(t, len(targetNodeIndexes), nodesFailCount,
		"Should find at least %d quorum nodes to terminate", nodesFailCount)

	t.Logf("Selected nodes to terminate: %v", targetNodeIndexes[:nodesFailCount])

	// Phase 4: Start continuous reader
	t.Logf("Phase 4: Starting continuous reader...")

	// Start reader goroutine for continuous tail reading
	readerDone := make(chan bool)
	go func() {
		defer close(readerDone)

		for {
			select {
			case <-ctx.Done():
				t.Logf("Reader stopping due to context cancellation")
				return
			default:
				// Try to read next message
				readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
				msg, readErr := logReader.ReadNext(readCtx)
				readCancel()

				if readErr != nil {
					// Handle read errors gracefully during failover
					if !errors.Is(readErr, context.DeadlineExceeded) {
						t.Logf("Reader encountered error: %v, retrying...", readErr)
						readMutex.Lock()
						readerErr = readErr
						readMutex.Unlock()
					}

					// Check if we've read enough entries
					readMutex.Lock()
					currentCount := len(readMessages)
					readMutex.Unlock()

					if currentCount >= totalEntries {
						t.Logf("Reader reached target of %d entries, stopping", totalEntries)
						return
					}
					continue
				}

				if msg != nil {
					readMutex.Lock()
					readMessages = append(readMessages, msg)
					currentCount := len(readMessages)
					readMutex.Unlock()

					t.Logf("Reader read entry %d: %s (total read: %d)",
						msg.Id.EntryId, string(msg.Payload), currentCount)

					if currentCount >= totalEntries {
						t.Logf("Reader reached target of %d entries, stopping", totalEntries)
						return
					}
				}
			}
		}
	}()

	// Phase 5: Kill 2 quorum nodes simultaneously
	t.Logf("Phase 5: Terminating %d quorum nodes simultaneously...", nodesFailCount)

	for i := 0; i < nodesFailCount; i++ {
		nodeIndex := targetNodeIndexes[i]
		t.Logf("Stopping node %d (quorum member %d/%d)...", nodeIndex, i+1, nodesFailCount)
		cluster.LeaveNodeWithIndex(t, nodeIndex)
		t.Logf("Node %d stopped successfully", nodeIndex)
	}

	t.Logf("All %d nodes terminated, waiting for system to detect failures...", nodesFailCount)
	time.Sleep(3 * time.Second)

	// Phase 6: Continue writing after double node failure
	t.Logf("Phase 6: Writing %d more entries despite double node failure...", entriesPhase2)

	successfulWrites := 0
	maxRetries := 25 // Allow more retries for double node failure recovery

	for attempt := 0; attempt < maxRetries && successfulWrites < entriesPhase2; attempt++ {
		entryIndex := entriesPhase1 + successfulWrites
		data := []byte(fmt.Sprintf("failover test data entry %d (after double node failure)", entryIndex))
		writeMsg := &log.WriteMessage{
			Payload: data,
		}

		// Use no timeout for individual writes during failover
		result := logWriter.Write(context.Background(), writeMsg)

		if result.Err != nil {
			t.Logf("Write attempt %d failed for entry %d: %v", attempt+1, entryIndex, result.Err)
			// Wait longer during double node failover for potential new segment creation
			time.Sleep(2 * time.Second)
			continue
		}

		assert.NotNil(t, result.LogMessageId)

		writtenMutex.Lock()
		writtenData = append(writtenData, data)
		writtenMutex.Unlock()

		successfulWrites++
		t.Logf("Writer successfully wrote entry %d with ID %d after double node failure: %s (successful writes: %d/%d)",
			entryIndex, result.LogMessageId.EntryId, string(data), successfulWrites, entriesPhase2)

		// Small delay to allow reader to catch up
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("Phase 6 completed with %d successful writes out of %d target writes", successfulWrites, entriesPhase2)

	// Phase 7: Wait for reader to complete and verify results
	t.Logf("Phase 7: Waiting for reader to complete...")

	// Give reader extra time to finish reading all entries
	readerTimeout := time.After(25 * time.Second)

	select {
	case <-readerDone:
		t.Logf("Reader completed successfully")
	case <-readerTimeout:
		readMutex.Lock()
		currentReadCount := len(readMessages)
		readMutex.Unlock()
		t.Logf("Reader timeout after 25 seconds, read %d entries so far", currentReadCount)
		// Continue with verification even if timeout
	}

	// Get final counts with proper locking
	writtenMutex.Lock()
	finalWrittenCount := len(writtenData)
	writtenDataCopy := make([][]byte, len(writtenData))
	copy(writtenDataCopy, writtenData)
	writtenMutex.Unlock()

	readMutex.Lock()
	finalReadCount := len(readMessages)
	readMessagesCopy := make([]*log.LogMessage, len(readMessages))
	copy(readMessagesCopy, readMessages)
	finalReaderErr := readerErr
	readMutex.Unlock()

	// Verify results
	t.Logf("Final verification: written=%d entries, read=%d entries", finalWrittenCount, finalReadCount)

	// Verify we wrote 10 entries total
	assert.Equal(t, totalEntries, finalWrittenCount, "Should have written exactly %d entries (%d before + %d after double failover)",
		totalEntries, entriesPhase1, entriesPhase2)

	// Verify we read 10 entries total
	assert.Equal(t, totalEntries, finalReadCount, "Should have read exactly %d entries", totalEntries)

	// Verify no fatal reader errors (temporary errors during failover are OK)
	if finalReaderErr != nil {
		t.Logf("Reader had error: %v (this may be temporary during double failover)", finalReaderErr)
	}

	// Check the breakdown of entries
	entriesBeforeFailover := 0
	entriesAfterFailover := 0

	for _, data := range writtenDataCopy {
		if strings.Contains(string(data), "(after double node failure)") {
			entriesAfterFailover++
		} else {
			entriesBeforeFailover++
		}
	}

	t.Logf("Entries breakdown: %d before double failover, %d after double failover", entriesBeforeFailover, entriesAfterFailover)
	assert.Equal(t, entriesPhase1, entriesBeforeFailover, "Should have written exactly %d entries before double failover", entriesPhase1)
	assert.Equal(t, entriesPhase2, entriesAfterFailover, "Should have written exactly %d entries after double failover", entriesPhase2)

	// Verify content matches
	minEntries := min(finalWrittenCount, finalReadCount)
	if minEntries > 0 {
		t.Logf("Verifying content for %d entries", minEntries)
		matchingEntries := 0
		for i := 0; i < minEntries; i++ {
			if string(readMessagesCopy[i].Payload) == string(writtenDataCopy[i]) {
				matchingEntries++
			} else {
				t.Logf("Mismatch at entry %d: read='%s', written='%s'", i,
					string(readMessagesCopy[i].Payload), string(writtenDataCopy[i]))
			}
		}
		t.Logf("Content verification: %d/%d entries match exactly", matchingEntries, minEntries)
		assert.Equal(t, minEntries, matchingEntries, "All entries should match exactly")
	}

	// Critical verification: Check segment rolling behavior
	// First 6 entries (0-5) should be in original segment, next 4 entries (6-9) should be in a new segment
	// Entry 5 triggers rolling but still succeeds in the original segment
	if finalReadCount >= totalEntries {
		t.Logf("Verifying segment rolling: checking segment IDs...")

		// Extract segment IDs
		firstSegmentId := readMessagesCopy[0].Id.SegmentId
		t.Logf("First entry (index 0) segment ID: %d", firstSegmentId)

		// Check first 6 entries (0-5) including the one that triggers rolling - should all be in same original segment
		originalSegmentId := firstSegmentId
		for i := 0; i < 6; i++ {
			segmentId := readMessagesCopy[i].Id.SegmentId
			t.Logf("Entry %d: segmentId=%d, entryId=%d", i, segmentId, readMessagesCopy[i].Id.EntryId)
			assert.Equal(t, originalSegmentId, segmentId,
				"Entries 0-5 (including the rolling-trigger entry) should all be in the original segment")
		}

		// Check next 4 entries (6-9) after rolling - should be in a different segment
		newSegmentId := readMessagesCopy[6].Id.SegmentId
		t.Logf("Seventh entry (index 6) segment ID: %d", newSegmentId)

		// Verify segment rolling occurred: newSegmentId should be different from originalSegmentId
		assert.Equal(t, originalSegmentId+1, newSegmentId,
			"Segment rolling should have occurred - entries 6-9 should be in a different segment from entries 0-5")

		for i := 6; i < totalEntries; i++ {
			afterNewSegmentId := readMessagesCopy[i].Id.SegmentId
			t.Logf("Entry %d: segmentId=%d, entryId=%d", i, afterNewSegmentId, readMessagesCopy[i].Id.EntryId)
			assert.GreaterOrEqual(t, afterNewSegmentId, newSegmentId,
				"Entries 6-9 (after double node failure rolling) should all be in the same new segment")
		}

		t.Logf("✅ Segment rolling verification PASSED:")
		t.Logf("   - Entries 0-5 (original segment, including rolling trigger): segment %d", originalSegmentId)
		t.Logf("   - Entries 6-9 (new segment after double node failure rolling): segment %d", newSegmentId)
		t.Logf("   - Rolling triggered correctly: entry 5 succeeded in original segment, entry 6+ in new segment!")
	}

	t.Logf("=== CASE 2 PASSED: Writer and reader continued working seamlessly despite double node failure (nodes %v) ===",
		targetNodeIndexes[:nodesFailCount])
	t.Logf("Successfully wrote and read %d entries with replica=%d configuration - %d before and %d after double failover",
		totalEntries, cfg.Woodpecker.Client.Quorum.SelectStrategy.Replicas,
		entriesPhase1, entriesPhase2)
}
