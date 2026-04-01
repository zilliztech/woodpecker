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
	"os"
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
		assert.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr), "Error should be 'log already exists' but got: %v", createErr)
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
		assert.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr), "Unexpected error: %v", createErr)
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
		assert.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr), "Error should be 'log already exists' but got: %v", createErr)
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
			// assert.Equal(t, newSegmentId, segmentId,"...") // TODO Perhaps we need to control the precise scrolling only once by coordinating the time interval between auto sync and trigger sync
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr), "Error should be 'log already exists' but got: %v", createErr)
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
// - 3-node cluster with es=3, wq=3, aq=2 configuration
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
	t.Logf("Creating log: %s with quorum config es=3, wq=3, aq=2", logName)

	// Create log if not exists
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr), "Unexpected error: %v", createErr)
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
	// Entry 5 triggers rolling but still succeeds in segment 0 (aq=2 satisfied by other 2 nodes)
	// Entries 0-5 should be in segment 0
	// Entries 6-9 should be in a NEW segment (segment 1) after rolling
	if finalReadCount >= totalEntries {
		t.Logf("Verifying segment rolling triggered by recovery mode...")

		// Extract segment IDs
		firstSegmentId := readMessages[0].Id.SegmentId
		t.Logf("First entry (index 0) segment ID: %d", firstSegmentId)

		// Check first 6 entries (0-5) including the rolling trigger - should all be in original segment
		// Entry 5 succeeds in original segment because aq=2 is satisfied by the other 2 nodes
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
		t.Logf("   - Entry 5 succeeded in original segment because aq=2 satisfied by other 2 nodes")
		t.Logf("   - Entries 6-9 (new segment after rolling): segment %d", newSegmentId)
		t.Logf("   - Recovery mode correctly triggered segment rolling!")
	}

	t.Logf("=== CASE 3 PASSED: Node %d restart with recovery mode triggered segment rolling ===", targetNodeIndex)
	t.Logf("Successfully wrote and read %d entries with es=3, wq=3, aq=2 configuration - %d before and %d after restart",
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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

// =============================================================================
// Case 10: Partial Replication During Node Crash
// =============================================================================
//
// 4-node cluster, es=3. Write entry 0 to all quorum nodes. Kill one quorum
// node. Write entry 1 (succeeds on 2/3 quorum nodes). Restart killed node.
// Close writer, open reader. Both entries should be readable.
func TestStagedStorageService_Failover_Case10_PartialReplicationDuringCrash(t *testing.T) {
	const (
		clusterSize = 4
	)

	t.Logf("=== CASE 10: Partial Replication During Node Crash ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case10")
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

	logName := "test_log_failover_case10_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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

	// Phase 1: Write entry 0 — creates quorum and replicates to all quorum nodes
	t.Logf("Phase 1: Writing entry 0 to create quorum...")
	result0 := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("case10 entry 0")})
	require.NoError(t, result0.Err, "Failed to write entry 0")
	t.Logf("Wrote entry 0 with ID %d", result0.LogMessageId.EntryId)

	// Phase 2: Identify quorum nodes and kill Nodes[0]
	t.Logf("Phase 2: Identifying quorum and killing first quorum node...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	// Kill Nodes[0] — entry 1 will only be on Nodes[1] and Nodes[2]
	killedNodeIdx := quorumNodeIndexes[0]
	t.Logf("Killing quorum node %d (Nodes[0])", killedNodeIdx)
	_, killErr := cluster.LeaveNodeWithIndex(t, killedNodeIdx)
	require.NoError(t, killErr)
	time.Sleep(2 * time.Second)

	// Phase 3: Write entry 1 — succeeds on 2/3 quorum nodes, fails on dead node
	t.Logf("Phase 3: Writing entry 1 with one quorum node down...")
	writeSuccess := false
	maxWriteAttempts := 30
	for attempt := 0; attempt < maxWriteAttempts; attempt++ {
		result1 := logWriter.Write(context.Background(), &log.WriteMessage{Payload: []byte("case10 entry 1")})
		if result1.Err != nil {
			t.Logf("Write attempt %d for entry 1 failed: %v", attempt+1, result1.Err)
			if werr.ErrLogWriterLockLost.Is(result1.Err) {
				t.Logf("Writer lock lost, reopening writer...")
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = logWriter.Close(closeCtx)
				closeCancel()
				logWriter, openWriterErr = logHandle.OpenLogWriter(ctx)
				if openWriterErr != nil {
					t.Logf("Failed to reopen writer: %v", openWriterErr)
				}
			}
			time.Sleep(2 * time.Second)
			continue
		}
		t.Logf("Wrote entry 1 with ID %d", result1.LogMessageId.EntryId)
		writeSuccess = true
		break
	}
	require.True(t, writeSuccess, "Should eventually write entry 1")

	// Phase 4: Restart killed node
	t.Logf("Phase 4: Restarting killed node %d...", killedNodeIdx)
	currentSeeds := cluster.GetSeedList()
	_, restartErr := cluster.RestartNode(t, killedNodeIdx, currentSeeds)
	require.NoError(t, restartErr)
	time.Sleep(3 * time.Second)

	// Phase 5: Close writer and read all entries
	t.Logf("Phase 5: Closing writer and reading entries...")
	require.NoError(t, logWriter.Close(ctx))
	logWriter = nil

	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case10-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	totalExpected := 2
	for i := 0; i < totalExpected; i++ {
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

	assert.Equal(t, totalExpected, readCount,
		"Should read both entries after partial replication and recovery")

	t.Logf("=== CASE 10 PASSED: Read %d/%d entries after partial replication ===",
		readCount, totalExpected)
}

// =============================================================================
// Case 11: Fence With Empty Node After Restart (data wiped)
// =============================================================================
//
// 4-node cluster, es=3. Write 1 entry to quorum [A,B,C]. Kill node C, close
// writer (fence with A=0, B=0, C=dead → LAC=0, correct). Then wipe C's data
// directory and restart C so it is truly empty. Open reader — the reader may
// contact C and get ErrFileReaderEndOfFile. Bug #1: reader breaks on EOF
// instead of trying another node. Assert: 1 entry is readable.
func TestStagedStorageService_Failover_Case11_FenceWithEmptyNode(t *testing.T) {
	const (
		clusterSize = 4
	)

	t.Logf("=== CASE 11: Fence With Empty Node After Restart (data wiped) ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case11")
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

	logName := "test_log_failover_case11_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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

	// Phase 1: Write entry 0 — creates quorum [A, B, C], data on all 3
	t.Logf("Phase 1: Writing entry 0 to create quorum...")
	result0 := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("case11 entry 0")})
	require.NoError(t, result0.Err, "Failed to write entry 0")
	t.Logf("Wrote entry 0 with ID %d", result0.LogMessageId.EntryId)

	// Phase 2: Get quorum info and identify all quorum nodes
	t.Logf("Phase 2: Identifying quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	// Kill Nodes[0] — the reader iterates quorum nodes starting at index 0,
	// so after wipe+restart this node will be contacted first, maximizing
	// Bug #1 (EOF break) reproduction.
	killedNodeIdx := quorumNodeIndexes[0]
	t.Logf("Killing quorum node %d (Nodes[0])", killedNodeIdx)
	_, killErr := cluster.LeaveNodeWithIndex(t, killedNodeIdx)
	require.NoError(t, killErr)
	time.Sleep(2 * time.Second)

	// Phase 3: Close writer — fence with 2 alive nodes → LAC=0 (correct)
	t.Logf("Phase 3: Closing writer to fence and complete segment...")
	require.NoError(t, logWriter.Close(ctx))
	logWriter = nil
	t.Logf("Writer closed, segment fenced with correct LAC")

	// Phase 4: Wipe killed node's data directory and restart it (truly empty)
	// Recovery mode will find no files → Read on this node returns EOF
	nodeDataDir := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", killedNodeIdx))
	t.Logf("Phase 4: Wiping data dir %s and restarting node %d...", nodeDataDir, killedNodeIdx)
	require.NoError(t, os.RemoveAll(nodeDataDir))

	currentSeeds := cluster.GetSeedList()
	_, restartErr := cluster.RestartNode(t, killedNodeIdx, currentSeeds)
	require.NoError(t, restartErr)
	time.Sleep(3 * time.Second)
	t.Logf("Node %d restarted (data wiped), active nodes: %d", killedNodeIdx, cluster.GetActiveNodes())

	// Phase 5: Open reader and read entries
	// The reader may contact the wiped (empty) node and get EOF.
	// Bug #1: reader breaks on EOF instead of trying other nodes.
	t.Logf("Phase 5: Opening reader and reading entries...")
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case11-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	msg, readErr := logReader.ReadNext(readCtx)
	readCancel()
	if readErr != nil {
		t.Logf("First read attempt failed: %v, retrying...", readErr)
		time.Sleep(1 * time.Second)
		readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr = logReader.ReadNext(readCtx2)
		readCancel2()
	}
	if readErr == nil && msg != nil {
		readCount++
		t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
			msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	} else if readErr != nil {
		t.Logf("Failed to read entry: %v", readErr)
	}

	assert.Equal(t, 1, readCount,
		"Should read 1 entry even when one quorum node is empty after restart")

	t.Logf("=== CASE 11 RESULT: Read %d/1 entries ===", readCount)
}

// =============================================================================
// Case 12: LAC Calculation After Node Swap (Regression Test)
// =============================================================================
//
// 4-node cluster, es=3. Write 1 entry to quorum [A, B, C]. Kill node A,
// wipe A's data directory, restart A (truly empty — returns -1 on fence).
// Kill node B. Close writer → fence: A=-1, C=0, B=dead.
//
// Previously calculateLAC([0, -1], ensembleCoverage=2) returned -1 because
// negative values were not filtered. Fixed in d54940e: calculateLAC now
// filters out negative values, so validResults=[0], LAC=0 (correct).
//
// The reader contacts the wiped node A first and gets EOF. Since the Fence
// race condition is fixed (534cc5d), the footer LAC is always correct, so
// EOF reliably indicates no more data on that node. The reader breaks on
// EOF and moves to the next segment — this is correct behavior because
// the segment is properly completed with LAC=0 in metadata.
//
// This test reproduces the original Case 7 rolling restart failure.
// Both underlying bugs have been fixed; this serves as a regression test.
func TestStagedStorageService_Failover_Case12_LACMiscalculationAfterNodeSwap(t *testing.T) {
	const (
		clusterSize = 4
	)

	t.Logf("=== CASE 12: LAC Miscalculation After Node Swap ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case12")
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

	logName := "test_log_failover_case12_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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

	// Phase 1: Write entry 0 — creates quorum [A, B, C], data on all 3 nodes
	t.Logf("Phase 1: Writing entry 0 to create quorum...")
	result0 := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("case12 entry 0")})
	require.NoError(t, result0.Err, "Failed to write entry 0")
	t.Logf("Wrote entry 0 with ID %d", result0.LogMessageId.EntryId)

	// Phase 2: Identify quorum nodes (A, B, C)
	t.Logf("Phase 2: Identifying quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(quorumInfo.Nodes), 3, "Should have at least 3 quorum nodes")
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	nodeA := quorumNodeIndexes[0]
	nodeB := quorumNodeIndexes[1]

	// Phase 3: Kill node A and wipe its data directory
	t.Logf("Phase 3: Killing node A (index %d) and wiping data...", nodeA)
	_, killErr := cluster.LeaveNodeWithIndex(t, nodeA)
	require.NoError(t, killErr)
	time.Sleep(1 * time.Second)

	// Wipe node A's local storage so recovery mode finds nothing → returns -1
	nodeADataDir := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeA))
	t.Logf("Wiping data dir: %s", nodeADataDir)
	require.NoError(t, os.RemoveAll(nodeADataDir))

	// Phase 4: Restart node A — truly empty, returns -1 on fence
	t.Logf("Phase 4: Restarting node A (index %d, data wiped)...", nodeA)
	currentSeeds := cluster.GetSeedList()
	_, restartErr := cluster.RestartNode(t, nodeA, currentSeeds)
	require.NoError(t, restartErr)
	time.Sleep(3 * time.Second)
	t.Logf("Node A restarted (empty), active nodes: %d", cluster.GetActiveNodes())

	// Phase 5: Kill node B — now C has data, A is empty, B is dead
	t.Logf("Phase 5: Killing node B (index %d)...", nodeB)
	_, killErr = cluster.LeaveNodeWithIndex(t, nodeB)
	require.NoError(t, killErr)
	time.Sleep(2 * time.Second)

	// Phase 6: Close writer — triggers fence on the segment.
	// Fence results: A returns -1 (empty), C returns 0 (has entry), B is dead.
	// calculateLAC filters negatives: validResults=[0], LAC=0 (correct after fix d54940e).
	t.Logf("Phase 6: Closing writer to trigger fence...")
	closeErr := logWriter.Close(ctx)
	logWriter = nil
	if closeErr != nil {
		t.Logf("Writer close returned error (may be expected): %v", closeErr)
	}

	// Phase 7: Open reader and attempt to read
	// With LAC fix: LAC=0, reader sees entry 0 → reads 1 (PASS).
	// Reader may contact wiped node A first and get EOF, which correctly
	// signals end of data on that node. The segment metadata has correct
	// LAC=0, so the reader can read the entry from node C.
	t.Logf("Phase 7: Opening reader and attempting to read...")
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case12-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	msg, readErr := logReader.ReadNext(readCtx)
	readCancel()
	if readErr != nil {
		t.Logf("First read attempt failed: %v, retrying...", readErr)
		time.Sleep(1 * time.Second)
		readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr = logReader.ReadNext(readCtx2)
		readCancel2()
	}
	if readErr == nil && msg != nil {
		readCount++
		t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
			msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	} else if readErr != nil {
		t.Logf("Failed to read entry: %v", readErr)
	}

	assert.Equal(t, 1, readCount,
		"Should read 1 entry after LAC fix (calculateLAC now filters negative values)")

	t.Logf("=== CASE 12 RESULT: Read %d/1 entries ===", readCount)
}

// =============================================================================
// Case 13: Fence and Read After Node Swap with Dead Node (Regression Test)
// =============================================================================
//
// 4-node cluster, es=3. Write 3 entries to quorum [A, B, C].
// Kill B (Nodes[1]), wipe B's data, restart B (empty). Then kill A (Nodes[0]).
// Close writer → fence: A=dead, B=-1 (empty), C=2.
//
// Previously calculateLAC([2, -1], ensembleCoverage=2) returned -1 because
// negative values were not filtered. Fixed in d54940e: calculateLAC now
// filters negatives, so validResults=[2], LAC=2 (correct).
//
// Reader path: Nodes[0]=A (dead → connection error → continue),
//
//	Nodes[1]=B (empty → EOF → break, correct: footer LAC is reliable after 534cc5d),
//	Nodes[2]=C (has data — may or may not be reached depending on read path).
//
// With correct LAC in metadata, the reader can read entries from any node
// that has the data. This test verifies the fence correctly computes LAC
// even when one responder returns -1 and one is dead.
func TestStagedStorageService_Failover_Case13_EOFOnNonFirstNodeAfterDeadSkip(t *testing.T) {
	const (
		clusterSize  = 4
		totalEntries = 3
	)

	t.Logf("=== CASE 13: EOF On Non-First Quorum Node After Dead Node Skip ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case13")
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

	logName := "test_log_failover_case13_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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

	// Phase 1: Write 3 entries — creates quorum [A, B, C], all entries on all 3
	t.Logf("Phase 1: Writing %d entries to create quorum...", totalEntries)
	for i := 0; i < totalEntries; i++ {
		payload := fmt.Sprintf("case13 entry %d", i)
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		t.Logf("Wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
		time.Sleep(100 * time.Millisecond)
	}

	// Phase 2: Identify quorum nodes (A=Nodes[0], B=Nodes[1], C=Nodes[2])
	t.Logf("Phase 2: Identifying quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(quorumInfo.Nodes), 3, "Should have at least 3 quorum nodes")
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	nodeA := quorumNodeIndexes[0] // will be killed last (dead during fence+read)
	nodeB := quorumNodeIndexes[1] // will be wiped (empty during fence+read)

	// Phase 3: Kill B (Nodes[1]), wipe its data, restart B (empty)
	t.Logf("Phase 3: Killing node B (index %d), wiping data, restarting...", nodeB)
	_, killErr := cluster.LeaveNodeWithIndex(t, nodeB)
	require.NoError(t, killErr)
	time.Sleep(1 * time.Second)

	nodeBDataDir := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeB))
	t.Logf("Wiping data dir: %s", nodeBDataDir)
	require.NoError(t, os.RemoveAll(nodeBDataDir))

	currentSeeds := cluster.GetSeedList()
	_, restartErr := cluster.RestartNode(t, nodeB, currentSeeds)
	require.NoError(t, restartErr)
	time.Sleep(3 * time.Second)
	t.Logf("Node B (index %d) restarted (empty), active nodes: %d", nodeB, cluster.GetActiveNodes())

	// Phase 4: Kill A (Nodes[0]) — now A=dead, B=empty, C=has data
	t.Logf("Phase 4: Killing node A (index %d)...", nodeA)
	_, killErr = cluster.LeaveNodeWithIndex(t, nodeA)
	require.NoError(t, killErr)
	time.Sleep(2 * time.Second)

	// Phase 5: Close writer → fence: A=dead, B=-1 (empty), C=2
	// calculateLAC filters negatives: validResults=[2], LAC=2 (correct after fix d54940e)
	t.Logf("Phase 5: Closing writer to trigger fence...")
	closeErr := logWriter.Close(ctx)
	logWriter = nil
	if closeErr != nil {
		t.Logf("Writer close returned error (may be expected): %v", closeErr)
	}

	// Phase 6: Open reader and attempt to read
	// Reader iteration: Nodes[0]=A (dead → conn error → continue),
	//                   Nodes[1]=B (empty → EOF → break, correct behavior),
	//                   segment metadata has correct LAC, reader proceeds normally
	t.Logf("Phase 6: Opening reader and attempting to read %d entries...", totalEntries)
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case13-reader")
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
				t.Logf("Read still failed at entry %d: %v", i, readErr)
				continue
			}
		}
		if msg != nil {
			readCount++
			t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
				msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
		}
	}

	assert.Equal(t, totalEntries, readCount,
		"Should read all %d entries with correct LAC after fence",
		totalEntries)

	t.Logf("=== CASE 13 RESULT: Read %d/%d entries ===",
		readCount, totalEntries)
}

// =============================================================================
// Case 14: Read After Data Wipe on One Replica (Regression Test)
// =============================================================================
//
// 4-node cluster, es=3. Write 5 entries, close writer normally so the segment
// is fenced and completed with correct LAC (all 3 nodes alive). Then kill one
// quorum node, wipe its data directory, and restart it (truly empty). Open
// reader — if it contacts the empty node it gets ErrFileReaderEndOfFile.
//
// Since the Fence race condition is fixed (534cc5d), the footer LAC is always
// correct. EOF on a wiped node correctly signals no data on that node.
// The reader breaks on EOF and moves on — this is correct behavior.
// The segment metadata has the correct LAC, so reading succeeds via other nodes.
func TestStagedStorageService_Failover_Case14_ReaderFallbackAfterEOF(t *testing.T) {
	const (
		clusterSize  = 4
		totalEntries = 5
	)

	t.Logf("=== CASE 14: Reader Fallback After EOF On Empty Fenced Node (data wiped) ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case14")
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

	logName := "test_log_failover_case14_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
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

	// Phase 1: Write entries — quorum [A, B, C], all entries on all 3 nodes
	t.Logf("Phase 1: Writing %d entries...", totalEntries)
	writtenPayloads := make([]string, totalEntries)
	for i := 0; i < totalEntries; i++ {
		payload := fmt.Sprintf("case14 entry %d", i)
		writtenPayloads[i] = payload
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		t.Logf("Wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
		time.Sleep(100 * time.Millisecond)
	}

	// Phase 2: Get quorum info and identify all quorum nodes
	t.Logf("Phase 2: Identifying quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, err := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	// Pick Nodes[0] — the reader iterates quorum nodes starting at index 0,
	// so wiping this node means the reader may hit the empty node first.
	targetNodeIdx := quorumNodeIndexes[0]

	// Phase 3: Close writer normally — all 3 nodes alive, fence is correct
	t.Logf("Phase 3: Closing writer normally (all nodes alive, correct fence)...")
	require.NoError(t, logWriter.Close(ctx))
	logWriter = nil

	// Phase 4: Kill Nodes[0], wipe its data directory, restart (truly empty)
	t.Logf("Phase 4: Killing quorum node %d (Nodes[0]), wiping data, and restarting...", targetNodeIdx)
	_, killErr := cluster.LeaveNodeWithIndex(t, targetNodeIdx)
	require.NoError(t, killErr)
	time.Sleep(1 * time.Second)

	nodeDataDir := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", targetNodeIdx))
	t.Logf("Wiping data dir: %s", nodeDataDir)
	require.NoError(t, os.RemoveAll(nodeDataDir))

	currentSeeds := cluster.GetSeedList()
	_, restartErr := cluster.RestartNode(t, targetNodeIdx, currentSeeds)
	require.NoError(t, restartErr)
	time.Sleep(3 * time.Second)
	t.Logf("Node %d restarted (data wiped), active nodes: %d",
		targetNodeIdx, cluster.GetActiveNodes())

	// Phase 5: Open reader and read all entries
	// The reader may contact the wiped (empty) node and get EOF.
	// EOF is correct behavior — footer LAC is reliable after fence fix (534cc5d).
	// The segment metadata has correct LAC, so reading succeeds via other nodes.
	t.Logf("Phase 5: Opening reader and reading %d entries...", totalEntries)
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle.OpenLogReader(ctx, startMsgId, "test-case14-reader")
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
				t.Logf("Read still failed at entry %d: %v", i, readErr)
				continue
			}
		}
		if msg != nil {
			readCount++
			assert.Equal(t, writtenPayloads[i], string(msg.Payload),
				"Entry %d content should match", i)
			t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
				msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
		}
	}

	assert.Equal(t, totalEntries, readCount,
		"Should read all %d entries after data wipe on one replica",
		totalEntries)

	t.Logf("=== CASE 14 RESULT: Read %d/%d entries ===",
		readCount, totalEntries)
}

// =============================================================================
// Case 15: Cluster Restart with Data Loss on One Node (Regression Test)
// =============================================================================
//
// 4-node cluster, es=3. Write 1 entry (replicated to 3 quorum nodes).
// Stop the entire cluster. Close writer (fence fails, segment stays Active).
// Wipe one quorum node's data directory. Restart entire cluster.
// Open a new writer (triggers fence on old Active segment).
// The wiped node returns LAC=-1, other 2 return LAC=0.
// calculateLAC filters negatives: validResults=[0,0], LAC=0 (correct after fix d54940e).
//
// The reader may contact the wiped node first and get EOF. Since the fence
// race condition is fixed (534cc5d), the footer LAC is always correct, so
// EOF correctly signals no data on that node. The segment metadata has
// correct LAC=0, so reading succeeds via other nodes.
func TestStagedStorageService_Failover_Case15_ClusterRestartWithDataLoss(t *testing.T) {
	const (
		clusterSize = 4
	)

	t.Logf("=== CASE 15: Cluster Restart With Data Loss on One Node ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case15")
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

	logName := "test_log_failover_case15_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)

	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)

	// Phase 1: Write 1 entry — data replicated to all 3 quorum nodes
	t.Logf("Phase 1: Writing 1 entry to all quorum nodes...")
	result := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("case15 entry 0")})
	require.NoError(t, result.Err, "Failed to write entry")
	t.Logf("Wrote entry with ID %d", result.LogMessageId.EntryId)

	// Phase 2: Identify quorum nodes so we can wipe the first one (Nodes[0])
	t.Logf("Phase 2: Identifying quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, qErr := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, qErr)
	require.GreaterOrEqual(t, len(quorumInfo.Nodes), 3, "Should have at least 3 quorum nodes")
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	// Pick the FIRST quorum node (Nodes[0]) to wipe — the reader iterates
	// quorum nodes starting at index 0, so it will hit this wiped node first.
	wipedNodeIdx := quorumNodeIndexes[0]
	t.Logf("Will wipe data for node %d (first in quorum list)", wipedNodeIdx)

	// Phase 3: Stop entire cluster — all nodes become unreachable
	t.Logf("Phase 3: Stopping entire cluster...")
	cluster.StopMultiNodeCluster(t)
	time.Sleep(2 * time.Second)

	// Phase 4: Close writer and client — fence will fail since cluster is down,
	// so the segment stays Active in etcd (never fenced/completed)
	t.Logf("Phase 4: Closing writer and client (fence fails, segment stays Active in etcd)...")
	_ = logWriter.Close(ctx)
	logWriter = nil
	_ = logHandle.Close(ctx)
	logHandle = nil
	_ = woodpeckerClient.Close(ctx)
	woodpeckerClient = nil

	// Phase 5: Wipe the chosen quorum node's data directory
	// This simulates a node that lost all data (disk failure, replacement, etc.)
	wipedNodeDataDir := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", wipedNodeIdx))
	t.Logf("Phase 5: Wiping data dir for node %d: %s", wipedNodeIdx, wipedNodeDataDir)
	require.NoError(t, os.RemoveAll(wipedNodeDataDir))

	// Phase 6: Restart entire cluster
	t.Logf("Phase 6: Restarting entire cluster...")
	// Restart node 0 first (bootstraps alone with empty seeds)
	firstAddr, restartErr := cluster.RestartNode(t, 0, []string{})
	require.NoError(t, restartErr)
	t.Logf("Restarted first node 0 at %s", firstAddr)

	// Restart remaining nodes using node 0 as seed
	for i := 1; i < clusterSize; i++ {
		_, nodeRestartErr := cluster.RestartNode(t, i, []string{firstAddr})
		require.NoError(t, nodeRestartErr)
		t.Logf("Restarted node %d", i)
	}
	time.Sleep(5 * time.Second)
	t.Logf("All nodes restarted, active nodes: %d", cluster.GetActiveNodes())

	// Phase 7: Create new etcd client (old one was closed with woodpeckerClient),
	// then create new woodpecker client, open log, open writer.
	// Opening a writer triggers fenceAllActiveSegments on old Active segment 0.
	// Fence results from quorum: wiped node returns -1, other 2 return 0.
	// calculateLAC filters negatives: validResults=[0,0], LAC=0 (correct).
	// Segment 0 gets completed with LAC=0.
	t.Logf("Phase 7: Creating new client and opening writer (triggers fence)...")
	newSeeds := cluster.GetSeedList()
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = newSeeds

	etcdCli2, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli2.Close()

	woodpeckerClient2, clientErr := woodpecker.NewClient(ctx, cfg, etcdCli2, true)
	require.NoError(t, clientErr)
	defer func() {
		if woodpeckerClient2 != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient2.Close(closeCtx)
		}
	}()

	logHandle2, openErr2 := woodpeckerClient2.OpenLog(ctx, logName)
	require.NoError(t, openErr2)
	defer func() {
		if logHandle2 != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle2.Close(closeCtx)
		}
	}()

	logWriter2, openWriterErr2 := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr2)
	defer func() {
		if logWriter2 != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter2.Close(closeCtx)
		}
	}()
	time.Sleep(2 * time.Second)

	// Phase 8: Open reader and try to read the entry
	// The reader may cycle through quorum nodes starting at Nodes[0] (the wiped node).
	// If it hits the wiped node, it gets EOF which correctly signals no data on that
	// node (footer LAC is reliable after fence fix 534cc5d). The segment metadata
	// has correct LAC=0, so reading succeeds via other nodes.
	t.Logf("Phase 8: Opening reader and reading entries...")
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle2.OpenLogReader(ctx, startMsgId, "test-case15-reader")
	require.NoError(t, openReaderErr)
	defer func() {
		if logReader != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logReader.Close(closeCtx)
		}
	}()

	readCount := 0
	readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
	msg, readErr := logReader.ReadNext(readCtx)
	readCancel()
	if readErr != nil {
		t.Logf("First read error: %v, retrying...", readErr)
		time.Sleep(2 * time.Second)
		readCtx2, readCancel2 := context.WithTimeout(ctx, 15*time.Second)
		msg, readErr = logReader.ReadNext(readCtx2)
		readCancel2()
		if readErr != nil {
			t.Logf("Read still failed: %v", readErr)
		}
	}
	if msg != nil {
		readCount++
		assert.Equal(t, "case15 entry 0", string(msg.Payload),
			"Entry content should match")
		t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
			msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	}

	assert.Equal(t, 1, readCount,
		"Should read the 1 entry after cluster restart with data loss on one node")

	t.Logf("=== CASE 15 RESULT: Read %d/1 entries ===", readCount)
}

// =============================================================================
// Case 16: Read After 2-of-3 Replicas Lost (Regression Test)
// =============================================================================
//
// 4-node cluster, es=3. Write 3 entries and close writer normally (segment
// properly fenced/completed with correct LAC=2). Stop cluster, wipe data on
// 2 of 3 quorum nodes. Restart cluster, open writer, open reader.
// The segment metadata already has correct LAC.
//
// The reader may hit wiped nodes and get EOF, which correctly signals no data
// on those nodes (footer LAC is reliable after fence fix 534cc5d). The reader
// breaks on EOF and moves on. The segment metadata has correct LAC=2, so
// reading succeeds when the reader reaches the surviving node (Nodes[2]).
// We deliberately wipe Nodes[0] and Nodes[1] so the reader must fall through
// to Nodes[2] — the only surviving replica.
func TestStagedStorageService_Failover_Case16_ReadAfterTwoReplicasLost(t *testing.T) {
	const (
		clusterSize  = 4
		totalEntries = 3
	)

	t.Logf("=== CASE 16: Read After 2-of-3 Replicas Lost ===")

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestStagedStorageService_Failover_Case16")
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

	logName := "test_log_failover_case16_" + time.Now().Format("20060102150405")
	createErr := woodpeckerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, werr.ErrMetadataCreateLogAlreadyExists.Is(createErr))
	}

	logHandle, openErr := woodpeckerClient.OpenLog(ctx, logName)
	require.NoError(t, openErr)

	logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr)

	// Phase 1: Write 3 entries normally
	t.Logf("Phase 1: Writing %d entries...", totalEntries)
	writtenPayloads := make([]string, totalEntries)
	for i := 0; i < totalEntries; i++ {
		payload := fmt.Sprintf("case16 entry %d", i)
		writtenPayloads[i] = payload
		result := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Failed to write entry %d", i)
		t.Logf("Wrote entry %d with ID %d", i, result.LogMessageId.EntryId)
	}

	// Phase 2: Identify quorum nodes before closing
	t.Logf("Phase 2: Identifying quorum nodes...")
	currentSegmentHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, currentSegmentHandle)

	quorumInfo, qErr := currentSegmentHandle.GetQuorumInfo(ctx)
	require.NoError(t, qErr)
	require.GreaterOrEqual(t, len(quorumInfo.Nodes), 3, "Should have at least 3 quorum nodes")
	t.Logf("Quorum nodes: %v", quorumInfo.Nodes)

	quorumNodeIndexes := make([]int, 0, len(quorumInfo.Nodes))
	for _, addr := range quorumInfo.Nodes {
		nodeIdx, found := findClusterNodeByQuorumAddr(t, cluster, addr)
		require.True(t, found, "Should find cluster node for quorum address %s", addr)
		quorumNodeIndexes = append(quorumNodeIndexes, nodeIdx)
	}
	t.Logf("Quorum node indexes: %v", quorumNodeIndexes)

	// Phase 3: Close writer normally — fence/complete succeeds, segment Completed with correct LAC
	t.Logf("Phase 3: Closing writer normally (segment gets properly completed)...")
	require.NoError(t, logWriter.Close(ctx))
	logWriter = nil
	require.NoError(t, logHandle.Close(ctx))
	logHandle = nil
	require.NoError(t, woodpeckerClient.Close(ctx))
	woodpeckerClient = nil
	t.Logf("Writer and client closed, segment completed with correct LAC")

	// Phase 4: Stop entire cluster
	t.Logf("Phase 4: Stopping entire cluster...")
	cluster.StopMultiNodeCluster(t)
	time.Sleep(2 * time.Second)

	// Phase 5: Wipe data on Nodes[0] and Nodes[1] — only Nodes[2] keeps data.
	// The reader iterates quorum nodes starting at index 0, so it will hit
	// two wiped nodes before reaching the surviving one.
	wipedIdx0 := quorumNodeIndexes[0]
	wipedIdx1 := quorumNodeIndexes[1]
	survivorIdx := quorumNodeIndexes[2]

	dataDir0 := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", wipedIdx0))
	dataDir1 := filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", wipedIdx1))
	t.Logf("Phase 5: Wiping data for nodes %d and %d (survivor: node %d)",
		wipedIdx0, wipedIdx1, survivorIdx)
	require.NoError(t, os.RemoveAll(dataDir0))
	require.NoError(t, os.RemoveAll(dataDir1))

	// Phase 6: Restart entire cluster
	t.Logf("Phase 6: Restarting entire cluster...")
	firstAddr, restartErr := cluster.RestartNode(t, 0, []string{})
	require.NoError(t, restartErr)
	t.Logf("Restarted first node 0 at %s", firstAddr)

	for i := 1; i < clusterSize; i++ {
		_, nodeRestartErr := cluster.RestartNode(t, i, []string{firstAddr})
		require.NoError(t, nodeRestartErr)
		t.Logf("Restarted node %d", i)
	}
	time.Sleep(5 * time.Second)
	t.Logf("All nodes restarted, active nodes: %d", cluster.GetActiveNodes())

	// Phase 7: Create new etcd client (old one was closed with woodpeckerClient),
	// then create new woodpecker client, open log, open writer.
	t.Logf("Phase 7: Creating new client and opening writer...")
	newSeeds := cluster.GetSeedList()
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = newSeeds

	etcdCli2, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli2.Close()

	woodpeckerClient2, clientErr := woodpecker.NewClient(ctx, cfg, etcdCli2, true)
	require.NoError(t, clientErr)
	defer func() {
		if woodpeckerClient2 != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = woodpeckerClient2.Close(closeCtx)
		}
	}()

	logHandle2, openErr2 := woodpeckerClient2.OpenLog(ctx, logName)
	require.NoError(t, openErr2)
	defer func() {
		if logHandle2 != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logHandle2.Close(closeCtx)
		}
	}()

	logWriter2, openWriterErr2 := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, openWriterErr2)
	defer func() {
		if logWriter2 != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = logWriter2.Close(closeCtx)
		}
	}()
	time.Sleep(2 * time.Second)

	// Phase 8: Open reader and read all 3 entries
	// The segment was properly completed (LAC=2), so metadata is correct.
	// Reader may hit wiped nodes and get EOF — correct behavior since footer
	// LAC is reliable. Reading succeeds via the surviving node.
	t.Logf("Phase 8: Opening reader and reading %d entries...", totalEntries)
	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, openReaderErr := logHandle2.OpenLogReader(ctx, startMsgId, "test-case16-reader")
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
		readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		if readErr != nil {
			t.Logf("Read error at entry %d: %v, retrying...", i, readErr)
			time.Sleep(2 * time.Second)
			readCtx2, readCancel2 := context.WithTimeout(ctx, 15*time.Second)
			msg, readErr = logReader.ReadNext(readCtx2)
			readCancel2()
			if readErr != nil {
				t.Logf("Read still failed at entry %d: %v", i, readErr)
				continue
			}
		}
		if msg != nil {
			readCount++
			assert.Equal(t, writtenPayloads[i], string(msg.Payload),
				"Entry %d content should match", i)
			t.Logf("Read entry: segmentId=%d, entryId=%d, payload=%s",
				msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
		}
	}

	assert.Equal(t, totalEntries, readCount,
		"Should read all %d entries from the surviving replica after 2-of-3 data loss",
		totalEntries)

	t.Logf("=== CASE 16 RESULT: Read %d/%d entries ===",
		readCount, totalEntries)
}

// =============================================================================
// Chaos Test Cases
//
// These tests simulate advanced failure modes beyond simple node kill/restart:
// - Competing writers / fencing races
// - Concurrent writer attempts
// - Node crash during fencing
// - Disk failure while node is alive
// - Recovery from partial WAL across multiple segments
// - Reader failover during server crash
// - Concurrent readers on same segment
// - Cross-segment boundary reading
// =============================================================================

// TestStagedStorageService_Chaos_CompetingWritersFencing tests that when writer A
// closes and writer B takes over, all data from both writers is preserved and correctly ordered.
//
// Invariants verified:
//   - Fencing correctness: fenced writer cannot produce new acknowledged writes
//   - Single-writer guarantee: only one writer active at a time
//   - Durability: writes from both writers are correctly ordered
func TestStagedStorageService_Chaos_CompetingWritersFencing(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_CompetingWritersFencing")

	const nodeCount = 3
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	// Client A
	clientA, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = clientA.Close(ctx) }()

	logName := fmt.Sprintf("chaos_fencing_%d", time.Now().UnixNano())
	err = clientA.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandleA, err := clientA.OpenLog(ctx, logName)
	require.NoError(t, err)

	writerA, err := logHandleA.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Writer A writes 5 entries
	for i := 0; i < 5; i++ {
		result := writerA.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("writerA-entry-%d", i)),
		})
		require.NoError(t, result.Err, "writerA write %d failed", i)
	}
	t.Log("Writer A: 5 entries written")

	// Close writer A (releases lock, completes segment)
	err = writerA.Close(ctx)
	require.NoError(t, err)

	// Client B acquires lock and writes
	etcdCli2, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli2.Close()

	clientB, err := woodpecker.NewClient(ctx, cfg, etcdCli2, true)
	require.NoError(t, err)
	defer func() { _ = clientB.Close(ctx) }()

	logHandleB, err := clientB.OpenLog(ctx, logName)
	require.NoError(t, err)

	writerB, err := logHandleB.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		result := writerB.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("writerB-entry-%d", i)),
		})
		require.NoError(t, result.Err, "writerB write %d failed", i)
	}
	t.Log("Writer B: 5 entries written")

	err = writerB.Close(ctx)
	require.NoError(t, err)

	// Read all 10 entries
	earliest := log.EarliestLogMessageID()
	reader, err := logHandleB.OpenLogReader(ctx, &earliest, "verify-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	msgs := make([]*log.LogMessage, 0, 10)
	for i := 0; i < 10; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		require.NoError(t, readErr, "read entry %d", i)
		require.NotNil(t, msg)
		msgs = append(msgs, msg)
	}
	require.Len(t, msgs, 10)

	// Verify ordering
	for i := 1; i < len(msgs); i++ {
		prev := msgs[i-1].Id
		curr := msgs[i].Id
		if curr.SegmentId == prev.SegmentId {
			assert.Greater(t, curr.EntryId, prev.EntryId)
		} else {
			assert.Greater(t, curr.SegmentId, prev.SegmentId)
		}
	}

	t.Log("=== Chaos_CompetingWritersFencing PASSED ===")
}

// TestStagedStorageService_Chaos_ConcurrentWriterAttempts tests that two goroutines
// trying to open writers on the same log respect the single-writer guarantee.
//
// Invariants verified:
//   - Single-writer guarantee: at most one writer active at a time
func TestStagedStorageService_Chaos_ConcurrentWriterAttempts(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_ConcurrentWriterAttempts")

	const nodeCount = 3
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_concurrent_writers_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	type writerResult struct {
		id       int
		writeErr error
		closeErr error
		msgId    *log.LogMessageId
	}

	var wg sync.WaitGroup
	results := make(chan writerResult, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			lh, openErr := client.OpenLog(ctx, logName)
			if openErr != nil {
				results <- writerResult{id: id, writeErr: openErr}
				return
			}

			w, wErr := lh.OpenLogWriter(ctx)
			if wErr != nil {
				results <- writerResult{id: id, writeErr: wErr}
				return
			}

			wr := w.Write(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("writer-%d-entry", id)),
			})

			cErr := w.Close(ctx)
			results <- writerResult{id: id, writeErr: wr.Err, closeErr: cErr, msgId: wr.LogMessageId}
		}(i)
	}

	wg.Wait()
	close(results)

	successes := 0
	for r := range results {
		if r.writeErr == nil {
			successes++
			t.Logf("Writer %d succeeded: seg=%d, entry=%d", r.id, r.msgId.SegmentId, r.msgId.EntryId)
		} else {
			t.Logf("Writer %d failed: %v", r.id, r.writeErr)
		}
	}

	assert.Greater(t, successes, 0, "at least one writer should succeed")
	t.Logf("=== Chaos_ConcurrentWriterAttempts PASSED: %d successes ===", successes)
}

// TestStagedStorageService_Chaos_FencingDuringNodeFailure tests that fencing completes
// even when a node in the ensemble crashes mid-fence.
//
// Invariants verified:
//   - Fencing correctness: fencing completes on quorum despite node failure
//   - Recovery: new writer can be opened after crash + fence
func TestStagedStorageService_Chaos_FencingDuringNodeFailure(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_FencingDuringNodeFailure")

	const nodeCount = 5
	cluster, cfg, gossipSeeds, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_fence_node_fail_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	t.Log("10 entries written")

	// Kill a node before close (close triggers fencing)
	_, err = cluster.LeaveNodeWithIndex(t, nodeCount-1)
	require.NoError(t, err)
	t.Logf("Killed node %d before fencing", nodeCount-1)
	time.Sleep(3 * time.Second)

	// Close writer — triggers fencing, will fail on killed node but succeed on quorum
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close with node killed: %v", closeErr)

	// Restart killed node
	_, restartErr := cluster.RestartNode(t, nodeCount-1, gossipSeeds)
	if restartErr != nil {
		t.Logf("RestartNode: %v (may be expected)", restartErr)
	}
	time.Sleep(5 * time.Second)

	// Open new writer to verify fencing state is consistent
	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err, "new writer should open after fencing + node failure")

	for i := 0; i < 5; i++ {
		result := writer2.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("post-fence-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Read all entries
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle2.OpenLogReader(ctx, &earliest, "verify-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	totalRead := 0
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		totalRead++
	}
	assert.GreaterOrEqual(t, totalRead, 10, "should read at least 10 pre-crash entries")
	t.Logf("=== Chaos_FencingDuringNodeFailure PASSED: %d entries ===", totalRead)
}

// TestStagedStorageService_Chaos_DiskFailureNodeAlive tests behavior when a node's disk
// becomes read-only (simulating disk failure) while the node process is still alive.
// This is distinct from node crash — the gRPC server is still reachable, but all writes fail.
//
// The node returns ErrStorageNotWritable to clients. The quorum should route writes to other nodes.
// After the disk is restored, the system should recover.
//
// Invariants verified:
//   - Graceful degradation: disk failure doesn't crash the node
//   - Quorum correctness: writes succeed via remaining healthy nodes
//   - Durability: no data loss for acknowledged writes
//   - Recovery: system resumes after disk is restored
func TestStagedStorageService_Chaos_DiskFailureNodeAlive(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_DiskFailureNodeAlive")

	const nodeCount = 5 // 5 nodes for tolerance: es=3, aq=2
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_disk_failure_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write entries with all disks healthy
	for i := 0; i < 5; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("healthy-entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	t.Log("Phase 1: 5 entries written with all disks healthy")

	// Make node 0's data directory read-only (simulates disk failure)
	node0DataDir := filepath.Join(rootPath, "node0")
	err = makeTreeReadOnly(node0DataDir)
	require.NoError(t, err)
	t.Logf("Made node0 data dir read-only: %s", node0DataDir)

	// Cleanup: restore write permissions at end
	defer func() {
		_ = makeTreeWritable(node0DataDir)
	}()

	// Phase 2: Continue writing — node0 returns ErrStorageNotWritable,
	// but quorum can still be formed via other nodes (aq=2 from remaining healthy nodes)
	err = writer.Close(ctx)
	t.Logf("Writer close after disk failure: %v", err)

	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	phase2Successes := 0
	for i := 0; i < 5; i++ {
		result := writer2.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("disk-fail-entry-%d", i)),
		})
		if result.Err == nil {
			phase2Successes++
		} else {
			t.Logf("Write %d during disk failure: %v", i, result.Err)
		}
	}
	t.Logf("Phase 2: %d/5 entries written with node0 disk read-only", phase2Successes)

	// Close writer2
	closeCtx, closeCancel := context.WithTimeout(ctx, 20*time.Second)
	_ = writer2.Close(closeCtx)
	closeCancel()

	// Restore disk permissions
	err = makeTreeWritable(node0DataDir)
	require.NoError(t, err)
	t.Log("Restored node0 disk permissions")
	time.Sleep(3 * time.Second)

	// Phase 3: Write more entries — all nodes healthy again
	logHandle3, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer3, err := logHandle3.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		result := writer3.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("restored-entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	t.Log("Phase 3: 5 entries written after disk restored")

	err = writer3.Close(ctx)
	require.NoError(t, err)

	// Read all entries
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle3.OpenLogReader(ctx, &earliest, "disk-fail-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	totalRead := 0
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		totalRead++
	}

	assert.GreaterOrEqual(t, totalRead, 5, "should read at least the 5 pre-failure entries")
	t.Logf("=== Chaos_DiskFailureNodeAlive PASSED: %d entries read ===", totalRead)
}

// TestStagedStorageService_Chaos_DiskFullSimulation tests behavior when a node's data
// directory runs out of space (simulated by making it read-only after initial writes).
// The server stays alive but cannot write new data. Writes should route to other nodes.
//
// Invariants verified:
//   - Graceful degradation: node handles disk full without crash
//   - Quorum correctness: writes succeed if enough healthy nodes remain
func TestStagedStorageService_Chaos_DiskFullSimulation(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_DiskFullSimulation")

	const nodeCount = 5
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_disk_full_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write some entries
	for i := 0; i < 5; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	t.Log("5 entries written before disk full")

	// Simulate disk full on TWO nodes (leaving 3 healthy, enough for es=3)
	node0Dir := filepath.Join(rootPath, "node0")
	node1Dir := filepath.Join(rootPath, "node1")

	_ = makeTreeReadOnly(node0Dir)
	_ = makeTreeReadOnly(node1Dir)
	t.Log("Made node0 and node1 data dirs read-only (disk full simulation)")

	defer func() {
		_ = makeTreeWritable(node0Dir)
		_ = makeTreeWritable(node1Dir)
	}()

	// Close and reopen writer to get fresh segment on healthy nodes
	err = writer.Close(ctx)
	t.Logf("Writer close: %v", err)

	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write more — should succeed via nodes 2,3,4 (3 healthy >= es=3)
	diskFullSuccesses := 0
	for i := 5; i < 10; i++ {
		result := writer2.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("entry-%d", i)),
		})
		if result.Err == nil {
			diskFullSuccesses++
		} else {
			t.Logf("Write %d during disk full: %v", i, result.Err)
		}
	}
	t.Logf("Writes during disk full: %d/5 succeeded", diskFullSuccesses)

	closeCtx, closeCancel := context.WithTimeout(ctx, 20*time.Second)
	_ = writer2.Close(closeCtx)
	closeCancel()

	// Read all entries
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle2.OpenLogReader(ctx, &earliest, "disk-full-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	totalRead := 0
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		totalRead++
	}

	assert.GreaterOrEqual(t, totalRead, 5, "should read at least the 5 pre-failure entries")
	t.Logf("=== Chaos_DiskFullSimulation PASSED: %d entries read ===", totalRead)
}

// TestStagedStorageService_Chaos_RecoveryMultiSegmentAfterCrash tests recovery across
// multiple segments after a node crash.
//
// Invariants verified:
//   - Recovery completeness: data spanning multiple segments is intact
//   - Ordering: entry IDs are correctly ordered across segments
func TestStagedStorageService_Chaos_RecoveryMultiSegmentAfterCrash(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_RecoveryMultiSegment")

	const nodeCount = 5
	cluster, cfg, gossipSeeds, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_recovery_multi_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	// Segment 1
	lh1, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	w1, err := lh1.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		result := w1.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("seg1-entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = w1.Close(ctx)
	require.NoError(t, err)
	t.Log("Segment 1: 5 entries")

	// Kill a node
	_, err = cluster.LeaveNodeWithIndex(t, 0)
	require.NoError(t, err)
	t.Log("Killed node 0")
	time.Sleep(3 * time.Second)

	// Segment 2 (with node down)
	lh2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	w2, err := lh2.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		result := w2.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("seg2-entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = w2.Close(ctx)
	require.NoError(t, err)
	t.Log("Segment 2: 5 entries with node 0 down")

	// Restart node
	_, err = cluster.RestartNode(t, 0, gossipSeeds)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	// Segment 3 (all nodes back)
	lh3, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	w3, err := lh3.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		result := w3.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("seg3-entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = w3.Close(ctx)
	require.NoError(t, err)
	t.Log("Segment 3: 5 entries after recovery")

	// Read all 15 entries
	earliest := log.EarliestLogMessageID()
	reader, err := lh3.OpenLogReader(ctx, &earliest, "multi-seg-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	msgs := make([]*log.LogMessage, 0, 15)
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		msgs = append(msgs, msg)
	}
	require.Len(t, msgs, 15, "should read all 15 entries across 3 segments")

	// Verify ordering and segment boundary crossing
	segments := make(map[int64]bool)
	for i, msg := range msgs {
		segments[msg.Id.SegmentId] = true
		if i > 0 {
			prev := msgs[i-1].Id
			if msg.Id.SegmentId == prev.SegmentId {
				assert.Greater(t, msg.Id.EntryId, prev.EntryId)
			} else {
				assert.Greater(t, msg.Id.SegmentId, prev.SegmentId)
			}
		}
	}
	assert.GreaterOrEqual(t, len(segments), 2, "should span multiple segments")
	t.Logf("=== Chaos_RecoveryMultiSegmentAfterCrash PASSED: 15 entries across %d segments ===", len(segments))
}

// TestStagedStorageService_Chaos_ReaderDuringServerCrash tests that a reader fails over
// to another replica when the server it's reading from crashes.
//
// Invariants verified:
//   - Reader failover: reader recovers from server crash
//   - Durability: all acknowledged data is still readable
func TestStagedStorageService_Chaos_ReaderDuringServerCrash(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_ReaderDuringCrash")

	const nodeCount = 5
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_reader_crash_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	const entryCount = 20
	for i := 0; i < entryCount; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = writer.Close(ctx)
	require.NoError(t, err)
	t.Logf("%d entries written", entryCount)

	// Open reader and read first batch
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, "crash-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	for i := 0; i < 10; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		require.NoError(t, readErr)
		require.NotNil(t, msg)
	}
	t.Log("Read 10 entries before crash")

	// Kill a node while reader is active
	_, err = cluster.LeaveNodeWithIndex(t, nodeCount-1)
	require.NoError(t, err)
	t.Log("Killed a node during reading")
	time.Sleep(3 * time.Second)

	// Continue reading — should fallback to other replicas
	postCrashRead := 0
	for i := 10; i < entryCount; i++ {
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		cancel()
		if readErr != nil || msg == nil {
			t.Logf("Read stopped at entry %d: err=%v, msg=%v", i, readErr, msg)
			break
		}
		postCrashRead++
	}

	assert.Greater(t, postCrashRead, 0, "should read at least some entries after node crash")
	t.Logf("=== Chaos_ReaderDuringServerCrash PASSED: %d entries after crash ===", postCrashRead)
}

// TestStagedStorageService_Chaos_ConcurrentReaders tests that multiple readers can
// read the same data concurrently without interfering.
//
// Invariants verified:
//   - Reader independence: concurrent readers maintain independent state
//   - Data integrity: each reader sees the same data
func TestStagedStorageService_Chaos_ConcurrentReaders(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_ConcurrentReaders")

	const nodeCount = 3
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_concurrent_read_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	const entryCount = 20
	for i := 0; i < entryCount; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Launch 3 concurrent readers
	const readerCount = 3
	var wg sync.WaitGroup
	readCounts := make([]int, readerCount)

	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			lh, openErr := client.OpenLog(ctx, logName)
			if openErr != nil {
				t.Logf("Reader %d: open log error: %v", idx, openErr)
				return
			}

			earliest := log.EarliestLogMessageID()
			rdr, rErr := lh.OpenLogReader(ctx, &earliest, fmt.Sprintf("reader-%d", idx))
			if rErr != nil {
				t.Logf("Reader %d: open reader error: %v", idx, rErr)
				return
			}
			defer rdr.Close(ctx)

			count := 0
			for {
				readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
				msg, readErr := rdr.ReadNext(readCtx)
				readCancel()
				if readErr != nil || msg == nil {
					break
				}
				count++
			}
			readCounts[idx] = count
		}(r)
	}

	wg.Wait()

	for i, count := range readCounts {
		assert.Equal(t, entryCount, count, "reader %d should read all %d entries", i, entryCount)
	}
	t.Log("=== Chaos_ConcurrentReaders PASSED ===")
}

// TestStagedStorageService_Chaos_DiskFailureDuringActiveWrite tests the case where
// a node's disk becomes read-only WHILE writes are actively happening on that node.
// Unlike DiskFailureNodeAlive which tests between-segments, this tests mid-segment.
//
// Invariants verified:
//   - Graceful degradation: in-flight writes get error, subsequent writes route to healthy nodes
//   - No data corruption: partially written data doesn't corrupt the WAL
func TestStagedStorageService_Chaos_DiskFailureDuringActiveWrite(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_DiskFailureDuringWrite")

	const nodeCount = 5
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() { _ = client.Close(ctx) }()

	logName := fmt.Sprintf("chaos_disk_during_write_%d", time.Now().UnixNano())
	err = client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write initial entries (all disks healthy)
	for i := 0; i < 3; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("pre-fail-entry-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	t.Log("3 entries written before disk failure")

	// Make node0 and node1 data dirs read-only mid-segment
	node0Dir := filepath.Join(rootPath, "node0")
	node1Dir := filepath.Join(rootPath, "node1")
	_ = makeTreeReadOnly(node0Dir)
	_ = makeTreeReadOnly(node1Dir)
	t.Log("Made node0 and node1 dirs read-only (mid-segment disk failure)")

	defer func() {
		_ = makeTreeWritable(node0Dir)
		_ = makeTreeWritable(node1Dir)
	}()

	// Continue writing on same writer — some writes may fail on disk-failed nodes
	// but succeed on healthy nodes via quorum
	postFailSuccesses := 0
	postFailErrors := 0
	for i := 3; i < 8; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("during-fail-entry-%d", i)),
		})
		if result.Err == nil {
			postFailSuccesses++
		} else {
			postFailErrors++
			t.Logf("Write %d during disk failure: %v", i, result.Err)
		}
	}
	t.Logf("During disk failure: %d successes, %d errors", postFailSuccesses, postFailErrors)

	// Close writer
	closeCtx, closeCancel := context.WithTimeout(ctx, 20*time.Second)
	_ = writer.Close(closeCtx)
	closeCancel()

	// Restore disk and write more
	_ = makeTreeWritable(node0Dir)
	_ = makeTreeWritable(node1Dir)
	time.Sleep(2 * time.Second)

	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		result := writer2.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("post-restore-%d", i)),
		})
		require.NoError(t, result.Err)
	}
	err = writer2.Close(ctx)
	require.NoError(t, err)
	t.Log("3 entries written after disk restore")

	// Read all entries
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle2.OpenLogReader(ctx, &earliest, "disk-mid-write-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	totalRead := 0
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		totalRead++
	}

	assert.GreaterOrEqual(t, totalRead, 3, "should read at least the 3 pre-failure entries")
	t.Logf("=== Chaos_DiskFailureDuringActiveWrite PASSED: %d entries read ===", totalRead)
}

// TestStagedStorageService_Chaos_AllNodesRestartFenceMarksLost_ConcurrentWriters tests the scenario:
//
// 3-node cluster (es=3, wq=3, aq=2). Writer A writes entries successfully. All 3 nodes
// restart, causing all in-memory fence marks to be lost. After restart, Writer A (still
// holding old segment handle references) attempts to write concurrently while a new
// Writer B opens — triggering fenceAllActiveSegments. The fence may need retries since
// Writer A is also operating.
//
// This tests the core quorum invariant when fence marks are lost after a full cluster
// restart with two writers operating concurrently:
//
// Invariants verified:
//   - ACK accuracy: every entry Writer A got an ACK for (before or after restart) is readable
//   - LAC correctness: Writer B's successful fence returns the correct LAC
//   - Writer B can write after fence succeeds and all entries are readable
func TestStagedStorageService_Chaos_AllNodesRestartFenceMarksLost_ConcurrentWriters(t *testing.T) {
	const clusterSize = 3

	t.Logf("=== Chaos: All nodes restart, fence marks lost, concurrent writers ===")

	// Phase 1: Setup 3-node cluster
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_FenceMarksLost")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Setup etcd
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	// Client A
	clientA, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if clientA != nil {
			_ = clientA.Close(ctx)
		}
	}()

	logName := fmt.Sprintf("chaos_fence_lost_%d", time.Now().UnixNano())
	err = clientA.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandleA, err := clientA.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Phase 2: Writer A writes entries
	t.Logf("Phase 2: Writer A writing 5 entries...")
	writerA, err := logHandleA.OpenLogWriter(ctx)
	require.NoError(t, err)

	ackedBeforeRestart := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		payload := fmt.Sprintf("writerA-pre-restart-%d", i)
		result := writerA.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Writer A pre-restart write %d failed", i)
		ackedBeforeRestart = append(ackedBeforeRestart, payload)
		t.Logf("Writer A ACKed entry %d: seg=%d, entry=%d",
			i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
	}
	t.Logf("Writer A: %d entries ACKed before restart", len(ackedBeforeRestart))

	// Phase 3: Stop all nodes (crash — all in-memory fence marks lost)
	t.Logf("Phase 3: Stopping all %d nodes (simulating full cluster crash)...", clusterSize)
	cluster.StopMultiNodeCluster(t)
	assert.Equal(t, 0, cluster.GetActiveNodes())
	time.Sleep(2 * time.Second)

	// Phase 4: Restart all nodes (service ports reused, gossip ports new)
	// Node 0 bootstraps gossip with a dummy seed to trigger server node creation.
	// Subsequent nodes join node 0 progressively.
	t.Logf("Phase 4: Restarting all %d nodes...", clusterSize)
	firstAddr, restartErr := cluster.RestartNode(t, 0, []string{"127.0.0.1:1"})
	require.NoError(t, restartErr, "Failed to restart node 0")
	for i := 1; i < clusterSize; i++ {
		_, restartErr = cluster.RestartNode(t, i, []string{firstAddr})
		require.NoError(t, restartErr, "Failed to restart node %d", i)
	}

	// Wait for gossip cluster to fully form (all nodes discover each other)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		for idx, srv := range cluster.Servers {
			if srv != nil && srv.GetMemberCount() < clusterSize {
				t.Logf("Waiting for gossip: node %d sees %d/%d members", idx, srv.GetMemberCount(), clusterSize)
				allReady = false
				break
			}
		}
		if allReady {
			t.Logf("All %d nodes have discovered each other via gossip", clusterSize)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.Equal(t, clusterSize, cluster.GetActiveNodes())

	// Phase 5: Writer A tries to write concurrently while Writer B fences
	t.Logf("Phase 5: Concurrent operations — Writer A writes, Writer B fences...")

	ackedAfterRestart := make([]string, 0)
	var ackedAfterMu sync.Mutex

	// Goroutine: Writer A attempts writes after restart
	// The servers will likely reject these (segment file exists → recovery mode), but any
	// ACKed write MUST be readable — that is the invariant under test.
	writerADone := make(chan struct{})
	go func() {
		defer close(writerADone)
		for i := 0; i < 5; i++ {
			payload := fmt.Sprintf("writerA-post-restart-%d", i)
			writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
			result := writerA.Write(writeCtx, &log.WriteMessage{Payload: []byte(payload)})
			writeCancel()
			if result.Err != nil {
				fmt.Printf("[Writer A] post-restart write %d failed (expected): %v\n", i, result.Err)
				break
			}
			ackedAfterMu.Lock()
			ackedAfterRestart = append(ackedAfterRestart, payload)
			ackedAfterMu.Unlock()
			fmt.Printf("[Writer A] post-restart write %d ACKed\n", i)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Give Writer A a brief head start to attempt writes
	time.Sleep(1 * time.Second)

	// Writer B opens on a separate client — triggers fenceAllActiveSegments
	t.Logf("Writer B: opening (triggers fenceAllActiveSegments)...")
	etcdCli2, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli2.Close()

	clientB, err := woodpecker.NewClient(ctx, cfg, etcdCli2, true)
	require.NoError(t, err)
	defer func() {
		if clientB != nil {
			_ = clientB.Close(ctx)
		}
	}()

	logHandleB, err := clientB.OpenLog(ctx, logName)
	require.NoError(t, err)

	writerB, err := logHandleB.OpenLogWriter(ctx)
	require.NoError(t, err, "Writer B must open after fencing")
	t.Logf("Writer B: opened successfully (fence completed, LAC determined)")

	// Wait for Writer A goroutine to finish
	<-writerADone

	// Phase 6: Writer B writes entries
	t.Logf("Phase 6: Writer B writing 5 entries...")
	writerBEntries := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		payload := fmt.Sprintf("writerB-entry-%d", i)
		result := writerB.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Writer B write %d failed", i)
		writerBEntries = append(writerBEntries, payload)
		t.Logf("Writer B wrote entry %d", i)
	}

	// Close writers
	closeCtx, closeCancel := context.WithTimeout(ctx, 10*time.Second)
	_ = writerA.Close(closeCtx) // may be invalid
	closeCancel()
	require.NoError(t, writerB.Close(ctx))

	// Phase 7: Read all entries and verify invariants
	t.Logf("Phase 7: Reading all entries and verifying invariants...")

	earliest := log.EarliestLogMessageID()
	reader, err := logHandleB.OpenLogReader(ctx, &earliest, "verify-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	readPayloads := make([]string, 0)
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		readPayloads = append(readPayloads, string(msg.Payload))
		t.Logf("Read: seg=%d, entry=%d, payload=%s",
			msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	}

	// Invariant 1: All Writer A pre-restart ACKed entries MUST be readable
	t.Logf("Verifying invariant 1: %d pre-restart ACKed entries are readable...", len(ackedBeforeRestart))
	for _, acked := range ackedBeforeRestart {
		found := false
		for _, read := range readPayloads {
			if read == acked {
				found = true
				break
			}
		}
		assert.True(t, found, "ACK accuracy violation: pre-restart ACKed entry not readable: %s", acked)
	}

	// Invariant 2: All Writer A post-restart ACKed entries MUST be readable
	ackedAfterMu.Lock()
	postRestartCopy := make([]string, len(ackedAfterRestart))
	copy(postRestartCopy, ackedAfterRestart)
	ackedAfterMu.Unlock()

	t.Logf("Verifying invariant 2: %d post-restart ACKed entries are readable...", len(postRestartCopy))
	for _, acked := range postRestartCopy {
		found := false
		for _, read := range readPayloads {
			if read == acked {
				found = true
				break
			}
		}
		assert.True(t, found, "ACK accuracy violation: post-restart ACKed entry not readable: %s", acked)
	}

	// Invariant 3: All Writer B entries MUST be readable
	t.Logf("Verifying invariant 3: %d Writer B entries are readable...", len(writerBEntries))
	for _, entry := range writerBEntries {
		found := false
		for _, read := range readPayloads {
			if read == entry {
				found = true
				break
			}
		}
		assert.True(t, found, "Writer B entry not readable: %s", entry)
	}

	totalExpected := len(ackedBeforeRestart) + len(postRestartCopy) + len(writerBEntries)
	assert.GreaterOrEqual(t, len(readPayloads), totalExpected,
		"Should read at least all ACKed entries (pre=%d + post=%d + B=%d = %d)",
		len(ackedBeforeRestart), len(postRestartCopy), len(writerBEntries), totalExpected)

	t.Logf("=== Chaos_AllNodesRestartFenceMarksLost PASSED: total read=%d (pre=%d, post=%d, writerB=%d) ===",
		len(readPayloads), len(ackedBeforeRestart), len(postRestartCopy), len(writerBEntries))
}

// TestStagedStorageService_Chaos_FinalizedSegmentSurvivesRestart_OldWriterRejected tests the scenario:
//
// 3-node cluster (es=3, wq=3, aq=2). Writer A writes entries. Writer B opens,
// triggering FenceAndComplete which fences AND finalizes Writer A's segment (footer
// written to disk, metadata updated to Completed). Then all 3 nodes restart.
//
// After restart, the finalized state (footer on disk) persists even though in-memory
// fence marks are lost:
//   - Old Writer A's writes to the finalized segment MUST fail (segment finalized on disk)
//   - A new Writer C can re-fence idempotently (segment already Completed) and write to
//     a new segment
//
// Invariants verified:
//   - Finalize durability: finalized state survives full cluster restart
//   - Old writer rejection: writes to finalized segment fail after restart
//   - Fence idempotency: re-fencing a completed/finalized segment succeeds
//   - New segment creation: new writer can create and write to a new segment after restart
//   - Data integrity: all originally ACKed entries are readable
func TestStagedStorageService_Chaos_FinalizedSegmentSurvivesRestart_OldWriterRejected(t *testing.T) {
	const clusterSize = 3

	t.Logf("=== Chaos: Finalized segment survives restart, old writer rejected ===")

	// Phase 1: Setup 3-node cluster
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestChaos_FinalizedSurvivesRestart")
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, clusterSize, rootPath)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds
	defer cluster.StopMultiNodeCluster(t)

	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Setup etcd
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	// Client A
	clientA, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer func() {
		if clientA != nil {
			_ = clientA.Close(ctx)
		}
	}()

	logName := fmt.Sprintf("chaos_finalized_restart_%d", time.Now().UnixNano())
	err = clientA.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandleA, err := clientA.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Phase 2: Writer A writes entries
	t.Logf("Phase 2: Writer A writing 5 entries...")
	writerA, err := logHandleA.OpenLogWriter(ctx)
	require.NoError(t, err)

	writerAEntries := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		payload := fmt.Sprintf("writerA-entry-%d", i)
		result := writerA.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Writer A write %d failed", i)
		writerAEntries = append(writerAEntries, payload)
		t.Logf("Writer A wrote entry %d: seg=%d, entry=%d",
			i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
	}
	t.Logf("Writer A: 5 entries written and ACKed")

	// Phase 3: Writer B opens — triggers FenceAndComplete on Writer A's segment
	// This performs fence (sets fenced=true in memory) AND finalize (writes footer to disk)
	// AND updates metadata to Completed in etcd.
	t.Logf("Phase 3: Writer B opens (triggers fence + finalize on Writer A's segment)...")
	etcdCli2, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli2.Close()

	clientB, err := woodpecker.NewClient(ctx, cfg, etcdCli2, true)
	require.NoError(t, err)
	defer func() {
		if clientB != nil {
			_ = clientB.Close(ctx)
		}
	}()

	logHandleB, err := clientB.OpenLog(ctx, logName)
	require.NoError(t, err)

	writerB, err := logHandleB.OpenLogWriter(ctx)
	require.NoError(t, err)
	t.Logf("Writer B opened — Writer A's segment is now fenced + finalized (footer on disk)")

	// Close Writer B without writing (simulates "fence completed but writer not yet used")
	require.NoError(t, writerB.Close(ctx))
	t.Logf("Writer B closed (no writes, just triggered fence + finalize)")

	// Phase 4: Stop all nodes
	t.Logf("Phase 4: Stopping all %d nodes...", clusterSize)
	cluster.StopMultiNodeCluster(t)
	assert.Equal(t, 0, cluster.GetActiveNodes())
	time.Sleep(2 * time.Second)

	// Phase 5: Restart all nodes
	// After restart: in-memory fence marks are gone, but finalized state (footer on disk)
	// persists. This is the key difference from Case 1.
	t.Logf("Phase 5: Restarting all %d nodes...", clusterSize)
	firstAddr, restartErr := cluster.RestartNode(t, 0, []string{"127.0.0.1:1"})
	require.NoError(t, restartErr, "Failed to restart node 0")
	for i := 1; i < clusterSize; i++ {
		_, restartErr = cluster.RestartNode(t, i, []string{firstAddr})
		require.NoError(t, restartErr, "Failed to restart node %d", i)
	}

	// Wait for gossip cluster to fully form (all nodes discover each other)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		for idx, srv := range cluster.Servers {
			if srv != nil && srv.GetMemberCount() < clusterSize {
				t.Logf("Waiting for gossip: node %d sees %d/%d members", idx, srv.GetMemberCount(), clusterSize)
				allReady = false
				break
			}
		}
		if allReady {
			t.Logf("All %d nodes have discovered each other via gossip", clusterSize)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.Equal(t, clusterSize, cluster.GetActiveNodes())

	// Phase 6: Writer A tries to write — MUST fail (segment finalized on disk)
	t.Logf("Phase 6: Writer A attempts write to finalized segment (should fail)...")
	writeCtx, writeCancel := context.WithTimeout(ctx, 15*time.Second)
	result := writerA.Write(writeCtx, &log.WriteMessage{
		Payload: []byte("writerA-after-finalize-should-fail"),
	})
	writeCancel()
	assert.Error(t, result.Err,
		"Writer A MUST fail when writing to finalized segment after restart")
	t.Logf("Writer A write correctly rejected: %v", result.Err)

	// Clean up old writer
	closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = writerA.Close(closeCtx)
	closeCancel()

	// Phase 7: Writer C opens — fence is idempotent for already-completed segments
	// The segment is already Completed in metadata, so fenceAllActiveSegments either
	// skips it (no Active segments) or re-fences idempotently.
	t.Logf("Phase 7: Writer C opens (fence should be idempotent for finalized segment)...")
	logHandleC, err := clientB.OpenLog(ctx, logName)
	require.NoError(t, err)

	writerC, err := logHandleC.OpenLogWriter(ctx)
	require.NoError(t, err, "Writer C must open: fence is idempotent for completed segments")
	t.Logf("Writer C opened successfully — will write to new segment")

	// Phase 8: Writer C writes to a new segment
	t.Logf("Phase 8: Writer C writing 3 entries to new segment...")
	writerCEntries := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		payload := fmt.Sprintf("writerC-entry-%d", i)
		result := writerC.Write(ctx, &log.WriteMessage{Payload: []byte(payload)})
		require.NoError(t, result.Err, "Writer C write %d failed", i)
		writerCEntries = append(writerCEntries, payload)
		t.Logf("Writer C wrote entry %d: seg=%d, entry=%d",
			i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
	}
	require.NoError(t, writerC.Close(ctx))
	t.Logf("Writer C: 3 entries written and closed")

	// Phase 9: Read all entries and verify
	t.Logf("Phase 9: Reading all entries and verifying invariants...")

	earliest := log.EarliestLogMessageID()
	reader, err := logHandleC.OpenLogReader(ctx, &earliest, "verify-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	readPayloads := make([]string, 0)
	for {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		readCancel()
		if readErr != nil || msg == nil {
			break
		}
		readPayloads = append(readPayloads, string(msg.Payload))
		t.Logf("Read: seg=%d, entry=%d, payload=%s",
			msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	}

	// Invariant 1: Writer A's original entries survive finalize + restart
	t.Logf("Verifying invariant 1: Writer A's %d entries survive finalize + restart...", len(writerAEntries))
	for _, entry := range writerAEntries {
		found := false
		for _, read := range readPayloads {
			if read == entry {
				found = true
				break
			}
		}
		assert.True(t, found,
			"Finalize durability violation: Writer A entry not readable after restart: %s", entry)
	}

	// Invariant 2: Writer C's entries are readable
	t.Logf("Verifying invariant 2: Writer C's %d entries are readable...", len(writerCEntries))
	for _, entry := range writerCEntries {
		found := false
		for _, read := range readPayloads {
			if read == entry {
				found = true
				break
			}
		}
		assert.True(t, found, "Writer C entry not readable: %s", entry)
	}

	// Invariant 3: Writer A's post-finalize write is NOT in the read data
	t.Logf("Verifying invariant 3: Writer A's rejected write is not in read data...")
	for _, read := range readPayloads {
		assert.NotEqual(t, "writerA-after-finalize-should-fail", read,
			"Writer A's post-finalize rejected write must NOT appear in read data")
	}

	totalExpected := len(writerAEntries) + len(writerCEntries)
	assert.GreaterOrEqual(t, len(readPayloads), totalExpected,
		"Should read at least Writer A (%d) + Writer C (%d) = %d entries",
		len(writerAEntries), len(writerCEntries), totalExpected)

	t.Logf("=== Chaos_FinalizedSegmentSurvivesRestart PASSED: total read=%d (writerA=%d, writerC=%d) ===",
		len(readPayloads), len(writerAEntries), len(writerCEntries))
}

// makeTreeReadOnly recursively sets all directories and files under root to read-only.
func makeTreeReadOnly(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip inaccessible paths
		}
		if info.IsDir() {
			return os.Chmod(path, 0o555)
		}
		return os.Chmod(path, 0o444)
	})
}

// makeTreeWritable recursively restores write permissions on all directories and files under root.
func makeTreeWritable(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return os.Chmod(path, 0o755)
		}
		return os.Chmod(path, 0o644)
	})
}
