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

// TestStagedStorageService_Failover_Simple_SegmentRollingVerification tests basic segment rolling:
// 3-node cluster with es=3, wq=3, aq=2.
// Write 5 entries, kill one quorum node, write 1-2 more entries to trigger rolling,
// then verify that entries before and after failure are readable.
// This is a simplified smoke test for segment rolling; see Case1-Case3 for full scenarios.
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

	// Setup etcd client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	assert.NoError(t, err)
	defer etcdCli.Close()

	// Create woodpecker client
	woodpeckerClient, err := woodpecker.NewClient(context.Background(), cfg, etcdCli, true)
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
	createErr := woodpeckerClient.CreateLog(context.Background(), logName)
	if createErr != nil {
		assert.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr), "Unexpected error: %v", createErr)
	}

	logHandle, openErr := woodpeckerClient.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)
	assert.NotNil(t, logHandle)

	// Phase 1: Write some initial entries successfully
	t.Logf("Phase 1: Writing initial entries...")
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
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

		writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := logWriter.Write(writeCtx, writeMsg)
		writeCancel()
		assert.NoError(t, result.Err, "Failed to write entry %d", i)
		assert.NotNil(t, result.LogMessageId)
		t.Logf("Successfully wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
	}

	// Phase 2: Simulate node failure by stopping one node
	t.Logf("Phase 2: Simulating node failure...")

	// Get current quorum information (if available)
	if currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(context.Background()); currentSegmentHandle != nil {
		segmentMetadata := currentSegmentHandle.GetMetadata(context.Background())
		if segmentMetadata != nil {
			t.Logf("Current segment ID: %d", segmentMetadata.Metadata.SegNo)
		}

		quorumInfo, qErr := currentSegmentHandle.GetQuorumInfo(context.Background())
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
		writeCtx, writeCancel := context.WithTimeout(context.Background(), 100*time.Second)
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
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), startMsgId, "test-simple-reader")
	require.NoError(t, openReaderErr)
	require.NotNil(t, logReader)
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
		readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
				fmt.Printf("[Case1 Reader] stopping due to context cancellation\n")
				return
			default:
				// Try to read next message
				readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
				msg, readErr := logReader.ReadNext(readCtx)
				readCancel()

				if readErr != nil {
					// Handle read errors gracefully during failover
					fmt.Printf("[Case1 Reader] encountered error: %v, retrying...\n", readErr)
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

					fmt.Printf("[Case1 Reader] read entry %d: %s (total read: %d)\n", msg.Id.EntryId, string(msg.Payload), currentCount)

					// Reset reader error on successful read
					readMutex.Lock()
					readerErr = nil
					readMutex.Unlock()

					// Check if we've read the target amount
					if currentCount >= 10 {
						fmt.Printf("[Case1 Reader] reached target of 10 entries, stopping\n")
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
				fmt.Printf("[Case2 Reader] stopping due to context cancellation\n")
				return
			default:
				// Try to read next message
				readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
				msg, readErr := logReader.ReadNext(readCtx)
				readCancel()

				if readErr != nil {
					// Handle read errors gracefully during failover
					if !errors.Is(readErr, context.DeadlineExceeded) {
						fmt.Printf("[Case2 Reader] encountered error: %v, retrying...\n", readErr)
						readMutex.Lock()
						readerErr = readErr
						readMutex.Unlock()
					}

					// Check if we've read enough entries
					readMutex.Lock()
					currentCount := len(readMessages)
					readMutex.Unlock()

					if currentCount >= totalEntries {
						fmt.Printf("[Case2 Reader] reached target of %d entries, stopping\n", totalEntries)
						return
					}
					continue
				}

				if msg != nil {
					readMutex.Lock()
					readMessages = append(readMessages, msg)
					currentCount := len(readMessages)
					readMutex.Unlock()

					fmt.Printf("[Case2 Reader] read entry %d: %s (total read: %d)\n",
						msg.Id.EntryId, string(msg.Payload), currentCount)

					if currentCount >= totalEntries {
						fmt.Printf("[Case2 Reader] reached target of %d entries, stopping\n", totalEntries)
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

// TestStagedStorageService_Failover_Case3_NodeRestartTriggersSegmentRolling tests failover scenario:
// - 3-node cluster with es=3, wq=2, aq=2 configuration
// - Writer writes 5 entries successfully
// - Kill one node from the current writable segment's quorum
// - Immediately restart the killed node
// - The killed node's segment writer enters recovery mode (read-only)
// - This should trigger segment rolling
// - Writer should continue to write 5 more entries in a NEW segment (total 10)
// - Verify segment rolling: entries 0-4 in original segment, entries 5-9 in new segment
func TestStagedStorageService_Failover_Case3_NodeRestartTriggersSegmentRolling(t *testing.T) {
	const (
		clusterSize   = 3 // Use 3-node cluster
		entriesPhase1 = 5 // entries before node restart
		entriesPhase2 = 5 // entries after node restart
		totalEntries  = entriesPhase1 + entriesPhase2
	)

	t.Logf("=== CASE 3: Testing node restart with recovery mode triggering segment rolling ===")

	// Phase 1: Setup 3-node cluster
	t.Logf("Phase 1: Setting up %d-node cluster...", clusterSize)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case3")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	defer func() {
		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster stabilization
	time.Sleep(2 * time.Second)

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
	logName := "test_log_failover_case3_" + time.Now().Format("20060102150405")
	t.Logf("Creating log: %s with quorum config es=3, wq=2, aq=2", logName)

	// Create log if not exists
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr), "Unexpected error: %v", createErr)
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
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case3-reader")
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
	t.Logf("Phase 3: Writing %d entries before node restart...", entriesPhase1)

	writtenData := make([][]byte, 0)
	var writtenMutex sync.Mutex

	for i := 0; i < entriesPhase1; i++ {
		data := []byte(fmt.Sprintf("failover test data entry %d (before restart)", i))
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

	// Phase 4: Get current writable segment quorum info and select one node to restart
	t.Logf("Phase 4: Identifying quorum node to restart...")

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

	originalSegmentId := segmentMetadata.Metadata.SegNo
	t.Logf("Current segment %d has quorum nodes: %v", originalSegmentId, quorumInfo.Nodes)
	require.GreaterOrEqual(t, len(quorumInfo.Nodes), 3, "Should have at least 3 quorum nodes")

	// Pick the first quorum node to restart
	targetQuorumNode := quorumInfo.Nodes[0]
	t.Logf("Selected quorum node for restart: %s", targetQuorumNode)

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

	// Phase 5: Kill and immediately restart the node
	t.Logf("Phase 5: Killing node %d and immediately restarting...", targetNodeIndex)

	// Stop the node first (RestartNode requires the node to be stopped)
	stoppedNodeIndex, err := cluster.LeaveNodeWithIndex(t, targetNodeIndex)
	require.NoError(t, err)
	require.Equal(t, targetNodeIndex, stoppedNodeIndex)
	t.Logf("Node %d stopped", targetNodeIndex)

	// Restart the node (RestartNode includes internal wait logic)
	currentSeeds := cluster.GetSeedList()
	restartedAddr, restartErr := cluster.RestartNode(t, targetNodeIndex, currentSeeds)
	require.NoError(t, restartErr)
	t.Logf("Node %d restarted successfully at address: %s", targetNodeIndex, restartedAddr)

	// Give the system time to detect the restart and potentially trigger segment rolling
	// The restarted node will have segment writer in recovery mode, which should trigger rolling
	time.Sleep(3 * time.Second)

	// Phase 6: Continue writing after node restart
	t.Logf("Phase 6: Writing %d more entries after node restart...", entriesPhase2)

	successfulWrites := 0
	maxRetries := 25 // Allow retries for segment rolling recovery

	for attempt := 0; attempt < maxRetries && successfulWrites < entriesPhase2; attempt++ {
		entryIndex := entriesPhase1 + successfulWrites
		data := []byte(fmt.Sprintf("failover test data entry %d (after restart)", entryIndex))
		writeMsg := &log.WriteMessage{
			Payload: data,
		}

		// Use longer timeout for writes during failover/rolling
		writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
		result := logWriter.Write(writeCtx, writeMsg)
		writeCancel()

		if result.Err != nil {
			t.Logf("Write attempt %d failed for entry %d: %v", attempt+1, entryIndex, result.Err)
			// Wait during segment rolling
			time.Sleep(2 * time.Second)
			continue
		}

		assert.NotNil(t, result.LogMessageId)

		writtenMutex.Lock()
		writtenData = append(writtenData, data)
		writtenMutex.Unlock()

		successfulWrites++
		t.Logf("Writer successfully wrote entry %d with ID %d after restart: %s (successful writes: %d/%d)",
			entryIndex, result.LogMessageId.EntryId, string(data), successfulWrites, entriesPhase2)

		// Small delay to allow system to stabilize
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("Phase 6 completed with %d successful writes out of %d target writes", successfulWrites, entriesPhase2)

	// Phase 7: Read all entries and verify segment rolling
	t.Logf("Phase 7: Reading all entries and verifying segment rolling...")

	readMessages := make([]*log.LogMessage, 0)

	// Read all entries
	for i := 0; i < totalEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()

		if readErr != nil {
			t.Logf("Read error for entry %d: %v", i, readErr)
			// Try a few more times
			time.Sleep(500 * time.Millisecond)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 5*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()

			if readErr != nil {
				t.Logf("Failed to read entry %d after retry: %v", i, readErr)
				continue
			}
		}

		if msg != nil {
			readMessages = append(readMessages, msg)
			t.Logf("Read entry %d: segmentId=%d, entryId=%d, payload=%s",
				i, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
		}
	}

	// Get final counts with proper locking
	writtenMutex.Lock()
	finalWrittenCount := len(writtenData)
	writtenDataCopy := make([][]byte, len(writtenData))
	copy(writtenDataCopy, writtenData)
	writtenMutex.Unlock()

	finalReadCount := len(readMessages)

	// Verify results
	t.Logf("Final verification: written=%d entries, read=%d entries", finalWrittenCount, finalReadCount)

	// Verify we wrote 10 entries total
	assert.Equal(t, totalEntries, finalWrittenCount, "Should have written exactly %d entries (%d before + %d after restart)",
		totalEntries, entriesPhase1, entriesPhase2)

	// Verify we read 10 entries total
	assert.Equal(t, totalEntries, finalReadCount, "Should have read exactly %d entries", totalEntries)

	// Verify content matches
	minEntries := min(finalWrittenCount, finalReadCount)
	if minEntries > 0 {
		t.Logf("Verifying content for %d entries", minEntries)
		matchingEntries := 0
		for i := 0; i < minEntries; i++ {
			if string(readMessages[i].Payload) == string(writtenDataCopy[i]) {
				matchingEntries++
			} else {
				t.Logf("Mismatch at entry %d: read='%s', written='%s'", i,
					string(readMessages[i].Payload), string(writtenDataCopy[i]))
			}
		}
		t.Logf("Content verification: %d/%d entries match exactly", matchingEntries, minEntries)
		assert.Equal(t, minEntries, matchingEntries, "All entries should match exactly")
	}

	// CRITICAL verification: Check segment rolling behavior
	// Entry 5 triggers rolling but still succeeds in segment 0 (wq=2 satisfied by other 2 nodes)
	// Entries 0-5 should be in segment 0
	// Entries 6-9 should be in a NEW segment (segment 1) after rolling
	if finalReadCount >= totalEntries {
		t.Logf("Verifying segment rolling triggered by recovery mode...")

		// Extract segment IDs
		firstSegmentId := readMessages[0].Id.SegmentId
		t.Logf("First entry (index 0) segment ID: %d", firstSegmentId)

		// Check first 6 entries (0-5) including the rolling trigger - should all be in original segment
		// Entry 5 succeeds in original segment because wq=2 is satisfied by the other 2 nodes
		for i := 0; i <= entriesPhase1; i++ { // 0-5 (6 entries)
			segmentId := readMessages[i].Id.SegmentId
			t.Logf("Entry %d: segmentId=%d, entryId=%d", i, segmentId, readMessages[i].Id.EntryId)
			assert.Equal(t, firstSegmentId, segmentId,
				"Entries 0-5 (including rolling trigger) should all be in the original segment %d", firstSegmentId)
		}

		// Check next 4 entries (6-9) after rolling - should be in a DIFFERENT segment
		newSegmentId := readMessages[entriesPhase1+1].Id.SegmentId
		t.Logf("Entry %d (first after rolling) segment ID: %d", entriesPhase1+1, newSegmentId)

		// Verify segment rolling occurred: newSegmentId should be different from firstSegmentId
		assert.Greater(t, newSegmentId, firstSegmentId,
			"Segment rolling should have occurred - entries 6-9 should be in a different segment from entries 0-5")

		for i := entriesPhase1 + 1; i < totalEntries; i++ { // 6-9
			segmentId := readMessages[i].Id.SegmentId
			t.Logf("Entry %d: segmentId=%d, entryId=%d", i, segmentId, readMessages[i].Id.EntryId)
			assert.GreaterOrEqual(t, segmentId, newSegmentId,
				"Entries 6-9 (after rolling) should be in new segment(s)")
		}

		t.Logf("✅ Segment rolling verification PASSED:")
		t.Logf("   - Entries 0-5 (original segment, including rolling trigger): segment %d", firstSegmentId)
		t.Logf("   - Entry 5 succeeded in original segment because wq=2 satisfied by other 2 nodes")
		t.Logf("   - Entries 6-9 (new segment after rolling): segment %d", newSegmentId)
		t.Logf("   - Recovery mode correctly triggered segment rolling!")
	}

	t.Logf("=== CASE 3 PASSED: Node %d restart with recovery mode triggered segment rolling ===", targetNodeIndex)
	t.Logf("Successfully wrote and read %d entries with es=3, wq=2, aq=2 configuration - %d before and %d after restart",
		totalEntries, entriesPhase1, entriesPhase2)
}

// =============================================================================
// Helper: map quorum node address (e.g. "0.0.0.0:60196") to cluster node index
// =============================================================================

func findClusterNodeByQuorumAddr(t *testing.T, cluster *utils.MiniCluster, quorumAddr string) (int, bool) {
	t.Helper()
	parts := strings.Split(quorumAddr, ":")
	if len(parts) != 2 {
		return -1, false
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return -1, false
	}
	for nodeIndex, clusterPort := range cluster.UsedPorts {
		if clusterPort == port {
			return nodeIndex, true
		}
	}
	return -1, false
}

// =============================================================================
// Case 4: Non-quorum node failure — writer and reader completely unaffected
// =============================================================================
//
// 5-node cluster, es=3. Kill a node that is NOT in the segment's quorum.
// Writer should keep writing to the same segment with no rolling.
// Reader should read all entries from a single segment.
func TestStagedStorageService_Failover_Case4_NonQuorumNodeFailure(t *testing.T) {
	const (
		clusterSize   = 5
		entriesPhase1 = 5
		entriesPhase2 = 5
		totalEntries  = entriesPhase1 + entriesPhase2
	)

	t.Logf("=== CASE 4: Non-quorum node failure — writer/reader unaffected ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case4")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	logName := "test_log_failover_case4_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// Phase 1: Write first batch
	t.Logf("Phase 1: Writing %d entries...", entriesPhase1)
	for i := 0; i < entriesPhase1; i++ {
		data := []byte(fmt.Sprintf("case4 entry %d", i))
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: data})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		t.Logf("Wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
	}

	// Phase 2: Identify quorum nodes and find a NON-quorum node
	t.Logf("Phase 2: Finding a non-quorum node to kill...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	// Build set of quorum node indexes
	quorumNodeIndexes := make(map[int]bool)
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		if found {
			quorumNodeIndexes[nodeIdx] = true
		}
	}

	// Find a node NOT in the quorum
	var nonQuorumNodeIndex int
	nonQuorumFound := false
	for _, nodeIdx := range cluster.GetActiveNodeIndexes() {
		if !quorumNodeIndexes[nodeIdx] {
			nonQuorumNodeIndex = nodeIdx
			nonQuorumFound = true
			break
		}
	}
	require.True(t, nonQuorumFound, "Should find a non-quorum node in a 5-node cluster with es=3")
	t.Logf("Non-quorum node to kill: %d", nonQuorumNodeIndex)

	// Kill the non-quorum node
	_, err = cluster.LeaveNodeWithIndex(t, nonQuorumNodeIndex)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Phase 3: Write more entries — should succeed with NO rolling
	t.Logf("Phase 3: Writing %d more entries after non-quorum node failure...", entriesPhase2)
	for i := entriesPhase1; i < totalEntries; i++ {
		data := []byte(fmt.Sprintf("case4 entry %d", i))
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: data})
		require.NoError(t, result.Err, "Write should succeed despite non-quorum node failure, entry %d", i)
		t.Logf("Wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
	}

	// Phase 4: Read all entries and verify they are all in the SAME segment
	t.Logf("Phase 4: Reading all entries and verifying no segment rolling...")
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case4-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	for i := 0; i < totalEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		require.NoError(t, readErr, "Failed to read entry %d", i)
		require.NotNil(t, msg)
		t.Logf("Read entry %d: segmentId=%d, entryId=%d", i, msg.Id.SegmentId, msg.Id.EntryId)

		// All entries should be in the same segment (no rolling)
		assert.Equal(t, int64(0), msg.Id.SegmentId,
			"Entry %d should be in segment 0 (no rolling expected)", i)
		expected := fmt.Sprintf("case4 entry %d", i)
		assert.Equal(t, expected, string(msg.Payload), "Entry %d content mismatch", i)
	}

	t.Logf("=== CASE 4 PASSED: Non-quorum node %d failure had no impact on writer/reader ===", nonQuorumNodeIndex)
}

// =============================================================================
// Case 5: All quorum nodes fail → write fails → restart nodes → write recovers
// =============================================================================
//
// 5-node cluster, es=3. Kill all 3 quorum nodes so no write can succeed.
// Writes should fail. Restart 2 killed nodes so 4 nodes are alive (enough for
// new quorum es=3). Writes should recover via segment rolling.
func TestStagedStorageService_Failover_Case5_QuorumLossAndRecovery(t *testing.T) {
	const (
		clusterSize   = 5
		entriesPhase1 = 5
		entriesPhase2 = 5
		totalEntries  = entriesPhase1 + entriesPhase2
	)

	t.Logf("=== CASE 5: Total quorum loss → write fails → restart → recovery ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case5")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	logName := "test_log_failover_case5_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// Phase 1: Write first batch
	t.Logf("Phase 1: Writing %d entries...", entriesPhase1)
	for i := 0; i < entriesPhase1; i++ {
		data := []byte(fmt.Sprintf("case5 entry %d", i))
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: data})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		time.Sleep(100 * time.Millisecond)
	}

	// Phase 2: Get quorum info and kill ALL 3 quorum nodes
	t.Logf("Phase 2: Killing all quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	killedNodeIndexes := make([]int, 0)
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find node for quorum address %s", addr)
		t.Logf("Killing quorum node %d (address: %s)", nodeIdx, addr)
		_, killErr := cluster.LeaveNodeWithIndex(t, nodeIdx)
		require.NoError(t, killErr)
		killedNodeIndexes = append(killedNodeIndexes, nodeIdx)
	}
	t.Logf("Killed all %d quorum nodes: %v", len(killedNodeIndexes), killedNodeIndexes)

	// Only 2 non-quorum nodes alive — not enough for es=3
	time.Sleep(3 * time.Second)
	assert.Equal(t, 2, cluster.GetActiveNodes(), "Should have only 2 nodes alive")

	// Phase 3: Restart 2 killed nodes → 4 nodes alive, enough for new quorum
	t.Logf("Phase 3: Restarting 2 killed nodes to enable recovery...")
	for i := 0; i < 2; i++ {
		nodeIdx := killedNodeIndexes[i]
		currentSeeds := cluster.GetSeedList()
		_, restartErr := cluster.RestartNode(t, nodeIdx, currentSeeds)
		require.NoError(t, restartErr)
		t.Logf("Restarted node %d", nodeIdx)
	}
	time.Sleep(3 * time.Second)
	assert.Equal(t, 4, cluster.GetActiveNodes(), "Should have 4 nodes alive after restart")

	// Phase 4: Write more entries — should recover via segment rolling
	t.Logf("Phase 4: Writing %d more entries after recovery...", entriesPhase2)
	successfulWrites := 0
	maxRetries := 30
	for attempt := 0; attempt < maxRetries && successfulWrites < entriesPhase2; attempt++ {
		entryIndex := entriesPhase1 + successfulWrites
		data := []byte(fmt.Sprintf("case5 entry %d (after recovery)", entryIndex))
		result := logWriter.Write(context.Background(), &log.WriteMessage{Payload: data})
		if result.Err != nil {
			t.Logf("Write attempt %d failed: %v", attempt+1, result.Err)
			time.Sleep(2 * time.Second)
			continue
		}
		successfulWrites++
		t.Logf("Wrote entry %d with ID %d after recovery (%d/%d)",
			entryIndex, result.LogMessageId.EntryId, successfulWrites, entriesPhase2)
		time.Sleep(200 * time.Millisecond)
	}
	assert.Equal(t, entriesPhase2, successfulWrites,
		"Should have written all %d entries after recovery", entriesPhase2)

	// Phase 5: Read all entries
	t.Logf("Phase 5: Reading all entries...")
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case5-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	for i := 0; i < totalEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		if readErr != nil {
			t.Logf("Read error at entry %d: %v, retrying...", i, readErr)
			time.Sleep(1 * time.Second)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()
			if readErr != nil {
				t.Logf("Read still failed: %v", readErr)
				continue
			}
		}
		if msg != nil {
			readCount++
			t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
				msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
		}
	}
	assert.Equal(t, totalEntries, readCount, "Should read all %d entries", totalEntries)

	t.Logf("=== CASE 5 PASSED: Recovered from total quorum loss, wrote and read %d entries ===", readCount)
}

// =============================================================================
// Case 6: Data durability after full cluster restart
// =============================================================================
//
// 3-node cluster. Write 10 entries, close writer to flush. Stop ALL nodes.
// Restart ALL nodes. Open new reader, verify all 10 entries are readable.
func TestStagedStorageService_Failover_Case6_FullClusterRestartDurability(t *testing.T) {
	const (
		clusterSize  = 3
		totalEntries = 10
	)

	t.Logf("=== CASE 6: Data durability after full cluster restart ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case6")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	logName := "test_log_failover_case6_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)

	// Phase 1: Write entries and close writer to ensure data is flushed
	t.Logf("Phase 1: Writing %d entries...", totalEntries)
	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)

	writtenPayloads := make([]string, totalEntries)
	for i := 0; i < totalEntries; i++ {
		payload := fmt.Sprintf("case6 durable entry %d", i)
		writtenPayloads[i] = payload
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		t.Logf("Wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
	}

	// Close writer to flush
	require.NoError(t, logWriter.Close(ctx))
	logWriter = nil
	require.NoError(t, logHandle.Close(ctx))
	logHandle = nil
	t.Logf("Writer and log handle closed, data should be flushed")

	// Phase 2: Stop ALL nodes
	t.Logf("Phase 2: Stopping all %d nodes...", clusterSize)
	cluster.StopMultiNodeCluster(t)
	assert.Equal(t, 0, cluster.GetActiveNodes(), "All nodes should be stopped")
	time.Sleep(2 * time.Second)

	// Phase 3: Restart ALL nodes
	t.Logf("Phase 3: Restarting all %d nodes...", clusterSize)
	for i := 0; i < clusterSize; i++ {
		restartSeeds := cluster.GetSeedList()
		_, restartErr := cluster.RestartNode(t, i, restartSeeds)
		require.NoError(t, restartErr, "Failed to restart node %d", i)
		t.Logf("Restarted node %d", i)
	}
	time.Sleep(3 * time.Second)
	assert.Equal(t, clusterSize, cluster.GetActiveNodes(), "All nodes should be active after restart")

	// Update seeds for new client
	newSeeds := cluster.GetSeedList()
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = newSeeds

	// Phase 4: Create new client and reader, verify all data is readable
	t.Logf("Phase 4: Creating new client and reading data after full restart...")

	// Re-open log handle
	logHandle, openErr = woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case6-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	for i := 0; i < totalEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		if readErr != nil {
			t.Logf("Read error at entry %d: %v, retrying...", i, readErr)
			time.Sleep(1 * time.Second)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()
			if readErr != nil {
				t.Logf("Read still failed after retry: %v", readErr)
				continue
			}
		}
		require.NotNil(t, msg, "Entry %d should be readable after full cluster restart", i)
		assert.Equal(t, writtenPayloads[i], string(msg.Payload), "Entry %d content should match", i)
		readCount++
		t.Logf("Read entry %d: segmentId=%d, entryId=%d, payload=%s",
			i, msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	}

	assert.Equal(t, totalEntries, readCount, "Should read all %d entries after full cluster restart", totalEntries)

	t.Logf("=== CASE 6 PASSED: All %d entries survived full cluster restart ===", readCount)
}

// =============================================================================
// Case 7: Rolling restart — restart nodes one by one during continuous writes
// =============================================================================
//
// 5-node cluster, es=3. Writer writes continuously. Kill and restart each of the
// 5 nodes one at a time. Writer should keep writing throughout (possibly with
// segment rolling when quorum nodes are affected). Reader verifies all data.
func TestStagedStorageService_Failover_Case7_RollingRestart(t *testing.T) {
	const (
		clusterSize      = 5
		entriesPerPhase  = 3
		entriesBeforeOps = 5
	)

	t.Logf("=== CASE 7: Rolling restart — restart nodes one by one during writes ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case7")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	logName := "test_log_failover_case7_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// Write helper with retries
	writeEntries := func(count int, label string) int {
		t.Helper()
		written := 0
		maxAttempts := count * 10
		for attempt := 0; attempt < maxAttempts && written < count; attempt++ {
			data := []byte(fmt.Sprintf("case7 %s entry %d", label, written))
			result := logWriter.Write(context.Background(), &log.WriteMessage{Payload: data})
			if result.Err != nil {
				t.Logf("Write attempt %d failed (%s): %v", attempt+1, label, result.Err)
				time.Sleep(2 * time.Second)
				continue
			}
			written++
			t.Logf("Wrote %s entry %d (ID %d)", label, written, result.LogMessageId.EntryId)
			time.Sleep(100 * time.Millisecond)
		}
		return written
	}

	// Phase 1: Write initial entries
	t.Logf("Phase 1: Writing %d initial entries...", entriesBeforeOps)
	written := writeEntries(entriesBeforeOps, "initial")
	require.Equal(t, entriesBeforeOps, written)

	totalWritten := written

	// Phase 2: Rolling restart — kill and restart each node with writes in between
	t.Logf("Phase 2: Rolling restart of all %d nodes...", clusterSize)
	for nodeIdx := 0; nodeIdx < clusterSize; nodeIdx++ {
		t.Logf("--- Rolling restart: node %d ---", nodeIdx)

		// Kill node
		_, leaveErr := cluster.LeaveNodeWithIndex(t, nodeIdx)
		require.NoError(t, leaveErr, "Failed to leave node %d", nodeIdx)
		time.Sleep(2 * time.Second)

		// Write some entries while node is down
		label := fmt.Sprintf("node%d-down", nodeIdx)
		w := writeEntries(entriesPerPhase, label)
		totalWritten += w
		t.Logf("Wrote %d entries while node %d was down", w, nodeIdx)

		// Restart node
		currentSeeds := cluster.GetSeedList()
		_, restartErr := cluster.RestartNode(t, nodeIdx, currentSeeds)
		require.NoError(t, restartErr, "Failed to restart node %d", nodeIdx)
		time.Sleep(2 * time.Second)

		t.Logf("Node %d restarted, active nodes: %d", nodeIdx, cluster.GetActiveNodes())
	}

	// Phase 3: Write final entries
	t.Logf("Phase 3: Writing %d final entries...", entriesPerPhase)
	w := writeEntries(entriesPerPhase, "final")
	totalWritten += w

	t.Logf("Total entries written: %d", totalWritten)

	// Phase 4: Read all entries
	t.Logf("Phase 4: Reading all %d entries...", totalWritten)
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case7-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	for i := 0; i < totalWritten; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		if readErr != nil {
			t.Logf("Read error at %d: %v, retrying...", i, readErr)
			time.Sleep(1 * time.Second)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()
			if readErr != nil {
				t.Logf("Read still failed: %v", readErr)
				continue
			}
		}
		if msg != nil {
			readCount++
			t.Logf("Read entry: segmentId=%d, entryId=%d", msg.Id.SegmentId, msg.Id.EntryId)
		}
	}

	assert.Equal(t, totalWritten, readCount, "Should read all %d entries after rolling restart", totalWritten)

	t.Logf("=== CASE 7 PASSED: Rolling restart completed, wrote and read %d entries ===", readCount)
}

// =============================================================================
// Case 8: Multiple sequential segment rollings
// =============================================================================
//
// 5-node cluster, es=3. Trigger rolling twice by killing different quorum nodes
// at different times. Verify reader traverses all 3 segments correctly.
func TestStagedStorageService_Failover_Case8_MultipleSequentialRollings(t *testing.T) {
	const (
		clusterSize      = 5
		entriesPerBatch  = 5
		numRollingEvents = 2
	)

	t.Logf("=== CASE 8: Multiple sequential segment rollings ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case8")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	logName := "test_log_failover_case8_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)
	defer func() {
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
	}()

	// reopenWriter closes the current writer and opens a new one.
	// This is needed when the writer's lock session expires after a node failure.
	reopenWriter := func() error {
		t.Helper()
		if logWriter != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter.Close(closeCtx)
		}
		newWriter, err := logHandle.OpenLogWriter(ctx)
		if err != nil {
			return err
		}
		logWriter = newWriter
		t.Logf("Reopened log writer successfully")
		return nil
	}

	// Write helper with retries.
	// When the writer's lock session has expired (expected after killing a quorum node),
	// the application should close the old writer and reopen a new one.
	writeN := func(count int, prefix string) int {
		t.Helper()
		written := 0
		maxAttempts := count * 10
		for attempt := 0; attempt < maxAttempts && written < count; attempt++ {
			data := []byte(fmt.Sprintf("case8 %s %d", prefix, written))
			result := logWriter.Write(context.Background(), &log.WriteMessage{Payload: data})
			if result.Err != nil {
				t.Logf("Write attempt %d failed (%s): %v", attempt+1, prefix, result.Err)
				if werr.ErrLogWriterLockLost.Is(result.Err) {
					t.Logf("Writer lock lost, reopening writer...")
					if reopenErr := reopenWriter(); reopenErr != nil {
						t.Logf("Failed to reopen writer: %v", reopenErr)
					}
				}
				time.Sleep(2 * time.Second)
				continue
			}
			written++
			time.Sleep(100 * time.Millisecond)
		}
		return written
	}

	totalWritten := 0

	// Phase 1: Write initial batch
	t.Logf("Phase 1: Writing initial %d entries...", entriesPerBatch)
	w := writeN(entriesPerBatch, "batch0")
	require.Equal(t, entriesPerBatch, w)
	totalWritten += w

	// Phase 2 & 3: Trigger rolling events
	killedNodes := make([]int, 0)
	for rolling := 0; rolling < numRollingEvents; rolling++ {
		t.Logf("--- Rolling event %d ---", rolling+1)

		// Get current quorum and kill one node
		segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
		if segHandle == nil {
			t.Logf("No writable segment handle, waiting...")
			time.Sleep(3 * time.Second)
			segHandle = logHandle.GetCurrentWritableSegmentHandle(ctx)
		}
		require.NotNil(t, segHandle, "Should have writable segment handle for rolling %d", rolling+1)

		qi, qErr := segHandle.GetQuorumInfo(ctx)
		require.NoError(t, qErr)
		t.Logf("Rolling %d: quorum nodes = %v", rolling+1, qi.Nodes)

		// Find a quorum node that is still alive and not already killed
		var targetIdx int
		targetFound := false
		for _, addr := range qi.Nodes {
			nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
			if found {
				// Check node is still alive
				srv := cluster.Servers[nodeIdx]
				if srv != nil {
					targetIdx = nodeIdx
					targetFound = true
					break
				}
			}
		}
		require.True(t, targetFound, "Should find a live quorum node to kill for rolling %d", rolling+1)

		t.Logf("Killing quorum node %d to trigger rolling %d", targetIdx, rolling+1)
		_, killErr := cluster.LeaveNodeWithIndex(t, targetIdx)
		require.NoError(t, killErr)
		killedNodes = append(killedNodes, targetIdx)
		time.Sleep(3 * time.Second)

		// Write entries that will go to the new segment after rolling
		label := fmt.Sprintf("batch%d", rolling+1)
		t.Logf("Writing %d entries after rolling %d...", entriesPerBatch, rolling+1)
		w = writeN(entriesPerBatch, label)
		assert.Equal(t, entriesPerBatch, w, "Should write %d entries after rolling %d", entriesPerBatch, rolling+1)
		totalWritten += w
	}

	t.Logf("Total entries written across %d rolling events: %d", numRollingEvents, totalWritten)

	// Phase 4: Read all entries and verify multiple segments were used
	t.Logf("Phase 4: Reading all %d entries and checking segment distribution...", totalWritten)
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case8-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	segmentIds := make(map[int64]int) // segmentId -> count of entries
	for i := 0; i < totalWritten; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		if readErr != nil {
			t.Logf("Read error at %d: %v, retrying...", i, readErr)
			time.Sleep(1 * time.Second)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()
			if readErr != nil {
				t.Logf("Read still failed: %v", readErr)
				continue
			}
		}
		if msg != nil {
			readCount++
			segmentIds[msg.Id.SegmentId]++
			t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
				msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
		}
	}

	assert.Equal(t, totalWritten, readCount, "Should read all %d entries", totalWritten)

	// Verify that multiple segments were used (at least numRollingEvents + 1)
	t.Logf("Segment distribution: %v", segmentIds)
	assert.GreaterOrEqual(t, len(segmentIds), numRollingEvents+1,
		"Should have at least %d segments after %d rolling events", numRollingEvents+1, numRollingEvents)

	t.Logf("=== CASE 8 PASSED: %d rolling events, %d segments, %d entries read ===",
		numRollingEvents, len(segmentIds), readCount)
}

// =============================================================================
// Case 9: Reader node failover — reader switches replica when node fails
// =============================================================================
//
// 5-node cluster, es=3. Write entries, open reader, read some entries.
// Kill a quorum node. Reader should continue reading remaining entries from
// another replica without any gaps.
func TestStagedStorageService_Failover_Case9_ReaderNodeFailover(t *testing.T) {
	const (
		clusterSize  = 5
		totalEntries = 10
	)

	t.Logf("=== CASE 9: Reader node failover ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case9")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	woodpeckerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if woodpeckerClient != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient.Close(closeCtx)
		}
	}()

	logName := "test_log_failover_case9_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrLogHandleLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)
	defer func() {
		if logHandle != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle.Close(closeCtx)
		}
	}()

	// Phase 1: Write all entries
	t.Logf("Phase 1: Writing %d entries...", totalEntries)
	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)

	writtenPayloads := make([]string, totalEntries)
	for i := 0; i < totalEntries; i++ {
		payload := fmt.Sprintf("case9 entry %d", i)
		writtenPayloads[i] = payload
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		time.Sleep(100 * time.Millisecond)
	}

	// Close writer to ensure data is flushed
	require.NoError(t, logWriter.Close(ctx))
	logWriter = nil

	// Phase 2: Open reader and read first half
	t.Logf("Phase 2: Reading first half of entries...")
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case9-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	halfEntries := totalEntries / 2
	readMessages := make([]*log.LogMessage, 0, totalEntries)

	for i := 0; i < halfEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		require.NoError(t, readErr, "Failed to read entry %d", i)
		require.NotNil(t, msg)
		readMessages = append(readMessages, msg)
		t.Logf("Read entry %d: segmentId=%d, entryId=%d", i, msg.Id.SegmentId, msg.Id.EntryId)
	}

	// Phase 3: Kill a quorum node while reader is mid-stream
	t.Logf("Phase 3: Killing a quorum node while reader is mid-stream...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	if currentSegmentHandle == nil {
		// If no writable handle, get the segment from reader's perspective
		t.Logf("No writable segment handle, will kill first available quorum node")
	}

	// Kill the first active quorum node
	var killedNodeIdx int
	if currentSegmentHandle != nil {
		qi, qErr := currentSegmentHandle.GetQuorumInfo(ctx)
		if qErr == nil && qi != nil {
			for _, addr := range qi.Nodes {
				nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
				if found && cluster.Servers[nodeIdx] != nil {
					killedNodeIdx = nodeIdx
					break
				}
			}
		}
	}
	if killedNodeIdx == 0 {
		// Fallback: kill the first active node
		killedNodeIdx = cluster.GetActiveNodeIndexes()[0]
	}

	t.Logf("Killing quorum node %d while reader is mid-stream", killedNodeIdx)
	_, killErr := cluster.LeaveNodeWithIndex(t, killedNodeIdx)
	require.NoError(t, killErr)
	time.Sleep(2 * time.Second)

	// Phase 4: Continue reading remaining entries — reader should failover to another replica
	t.Logf("Phase 4: Reading remaining entries after quorum node failure...")
	for i := halfEntries; i < totalEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()

		if readErr != nil {
			t.Logf("Read error at entry %d (expected during failover): %v, retrying...", i, readErr)
			time.Sleep(1 * time.Second)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()
		}

		require.NoError(t, readErr, "Reader should failover and read entry %d", i)
		require.NotNil(t, msg)
		readMessages = append(readMessages, msg)
		t.Logf("Read entry %d after failover: segmentId=%d, entryId=%d",
			i, msg.Id.SegmentId, msg.Id.EntryId)
	}

	// Phase 5: Verify all entries are correct and no gaps
	t.Logf("Phase 5: Verifying all entries...")
	assert.Equal(t, totalEntries, len(readMessages), "Should have read all %d entries", totalEntries)

	for i, msg := range readMessages {
		assert.Equal(t, writtenPayloads[i], string(msg.Payload),
			"Entry %d content should match after reader failover", i)
	}

	// Verify entry IDs are monotonically increasing (no gaps)
	for i := 1; i < len(readMessages); i++ {
		prev := readMessages[i-1]
		curr := readMessages[i]
		if prev.Id.SegmentId == curr.Id.SegmentId {
			assert.Equal(t, prev.Id.EntryId+1, curr.Id.EntryId,
				"Entry IDs should be consecutive within segment at index %d", i)
		}
	}

	t.Logf("=== CASE 9 PASSED: Reader survived node %d failure, read all %d entries without gaps ===",
		killedNodeIdx, totalEntries)
}
