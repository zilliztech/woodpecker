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

// TestStagedStorageService_BasicRW tests basic read-write functionality
func TestStagedStorageService_BasicRW(t *testing.T) {
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

	// Test 1: Open multiple log writers (simulating multi-writer scenario)
	t.Logf("Opening multiple log writers...")
	logWriter1, openWriterErr1 := logHandle.OpenLogWriter(ctx)
	assert.NoError(t, openWriterErr1)
	assert.NotNil(t, logWriter1)

	logWriter2, openWriterErr2 := logHandle.OpenLogWriter(ctx)
	assert.NoError(t, openWriterErr2)
	assert.NotNil(t, logWriter2)

	// Test 2: Write data using different writers
	t.Logf("Writing data using multiple writers...")

	// Write some entries using writer1
	testData1 := [][]byte{
		[]byte("test data from writer1 - entry 1"),
		[]byte("test data from writer1 - entry 2"),
		[]byte("test data from writer1 - entry 3"),
	}

	for i, data := range testData1 {
		writeMsg := &log.WriteMessage{
			Payload: data,
		}
		result := logWriter1.Write(ctx, writeMsg)
		assert.NoError(t, result.Err)
		assert.NotNil(t, result.LogMessageId)
		assert.Equal(t, int64(i), result.LogMessageId.EntryId)
		t.Logf("Writer1 wrote entry %d with data: %s", result.LogMessageId.EntryId, string(data))
	}

	// Write some entries using writer2
	testData2 := [][]byte{
		[]byte("test data from writer2 - entry 4"),
		[]byte("test data from writer2 - entry 5"),
	}

	for i, data := range testData2 {
		writeMsg := &log.WriteMessage{
			Payload: data,
		}
		result := logWriter2.Write(ctx, writeMsg)
		assert.NoError(t, result.Err)
		assert.NotNil(t, result.LogMessageId)
		assert.Equal(t, int64(i+len(testData1)), result.LogMessageId.EntryId) // Should continue from where writer1 left off
		t.Logf("Writer2 wrote entry %d with data: %s", result.LogMessageId.EntryId, string(data))
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
	totalEntries := len(testData1) + len(testData2)
	allTestData := append(testData1, testData2...)

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
	assert.NoError(t, logWriter1.Close(ctx))
	assert.NoError(t, logWriter2.Close(ctx))
	assert.NoError(t, logReader.Close(ctx))
	assert.NoError(t, logHandle.Close(ctx))

	t.Logf("TestStagedStorageService_Basic completed successfully with %d nodes", len(cluster.Servers))
}
