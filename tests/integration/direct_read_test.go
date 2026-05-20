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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestDirectRead_SealedSegments_AfterClusterStop tests that a client with direct read enabled
// can still read data from sealed segments after the cluster (quorum nodes) is stopped.
//
// Flow:
//  1. Start a 3-node service-mode cluster with short rolling interval and auditor interval
//  2. Write entries, triggering segment rolling to create multiple segments
//  3. Wait for auditor to compact Completed segments to Sealed
//  4. Verify sealed segment count via metadata
//  5. Stop the cluster (quorum nodes go down; etcd and MinIO remain available)
//  6. Create a new client with directRead.enabled=true
//  7. Read all entries from sealed segments and verify data integrity
func TestDirectRead_SealedSegments_AfterClusterStop(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestDirectRead_SealedSegments")

	// Load configuration with custom settings for fast segment rolling and auditing
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err, "Failed to load configuration")

	// Configure short segment rolling interval (2s) so segments roll quickly after writes
	cfg.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = config.NewDurationSecondsFromInt(2)
	// Configure short auditor interval (2s) so Completed segments get compacted to Sealed quickly
	cfg.Woodpecker.Client.Auditor.MaxInterval = config.NewDurationSecondsFromInt(2)

	// Start a 3-node cluster
	const nodeCount = 3
	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = seeds

	ctx := context.Background()

	// Setup etcd client
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	// Phase 1: Create client (without direct read) and write data
	t.Log("Phase 1: Writing data to create multiple segments...")

	writerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, false)
	require.NoError(t, err)

	logName := "test_direct_read_" + time.Now().Format("20060102150405")
	t.Logf("Creating log: %s", logName)

	createErr := writerClient.CreateLog(ctx, logName)
	if createErr != nil {
		require.True(t, false, "Failed to create log: %v", createErr)
	}

	logHandle, err := writerClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	require.NotNil(t, logHandle)

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	require.NotNil(t, logWriter)

	// Write entries in rounds, with sleep between rounds to trigger time-based segment rolling.
	// Each round writes 5 entries, then we sleep to exceed MaxInterval (2s),
	// and the next write triggers rolling.
	type writtenEntry struct {
		payload []byte
	}
	var allWritten []writtenEntry
	rounds := 3
	entriesPerRound := 5

	for round := 0; round < rounds; round++ {
		for i := 0; i < entriesPerRound; i++ {
			entryIdx := round*entriesPerRound + i
			data := []byte(fmt.Sprintf("direct-read-test-entry-%04d-round-%d", entryIdx, round))
			writeMsg := &log.WriteMessage{Payload: data}

			writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
			result := logWriter.Write(writeCtx, writeMsg)
			writeCancel()
			require.NoError(t, result.Err, "Failed to write entry %d", entryIdx)
			require.NotNil(t, result.LogMessageId)

			allWritten = append(allWritten, writtenEntry{payload: data})
			t.Logf("Wrote entry %d (seg=%d, entry=%d): %s",
				entryIdx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId, string(data))
		}

		if round < rounds-1 {
			// Sleep longer than MaxInterval to trigger time-based rolling on next write
			t.Logf("Sleeping 3s to trigger segment rolling after round %d...", round)
			time.Sleep(3 * time.Second)
		}
	}

	totalWritten := len(allWritten)
	t.Logf("Total entries written: %d", totalWritten)

	// Phase 2: Wait for auditor to compact Completed segments to Sealed
	t.Log("Phase 2: Waiting for auditor to compact segments to Sealed...")

	// The auditor runs every 2s. Wait enough time for it to compact all completed segments.
	// We check periodically until we see at least one sealed segment.
	var sealedCount int
	var sealedEntries int64
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		segmentsMeta, getErr := writerClient.GetMetadataProvider().GetAllSegmentMetadata(ctx, logName)
		if getErr != nil {
			t.Logf("GetAllSegmentMetadata error (retrying): %v", getErr)
			time.Sleep(1 * time.Second)
			continue
		}

		sealedCount = 0
		sealedEntries = 0
		for segId, seg := range segmentsMeta {
			t.Logf("  Segment %d: state=%s, lastEntryId=%d",
				segId, seg.Metadata.State.String(), seg.Metadata.LastEntryId)
			if seg.Metadata.State == proto.SegmentState_Sealed {
				sealedCount++
				sealedEntries += seg.Metadata.LastEntryId + 1
			}
		}

		if sealedCount >= 2 {
			t.Logf("Found %d sealed segments with %d total entries", sealedCount, sealedEntries)
			break
		}
		t.Logf("Only %d sealed segments so far, waiting...", sealedCount)
		time.Sleep(2 * time.Second)
	}

	require.GreaterOrEqual(t, sealedCount, 2,
		"Expected at least 2 sealed segments, but found %d", sealedCount)
	require.Greater(t, sealedEntries, int64(0),
		"Expected sealed segments to contain entries")

	// Phase 3: Close writer and client, then stop the cluster
	t.Log("Phase 3: Closing writer and stopping cluster...")

	closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
	assert.NoError(t, logWriter.Close(closeCtx))
	closeCancel()

	closeCtx, closeCancel = context.WithTimeout(ctx, 5*time.Second)
	assert.NoError(t, logHandle.Close(closeCtx))
	closeCancel()

	closeCtx, closeCancel = context.WithTimeout(ctx, 5*time.Second)
	assert.NoError(t, writerClient.Close(closeCtx))
	closeCancel()

	// Stop all quorum nodes — after this, quorum-based reads will fail
	cluster.StopMultiNodeCluster(t)
	t.Log("Cluster stopped. Quorum nodes are no longer available.")

	// Phase 4: Create a new client with directRead enabled and read sealed segment data
	t.Log("Phase 4: Creating direct-read client and reading sealed segments...")

	// Enable direct read
	cfg.Woodpecker.Client.DirectRead.Enabled = true

	readerClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, false)
	require.NoError(t, err)
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
		defer closeCancel()
		_ = readerClient.Close(closeCtx)
	}()

	readerLogHandle, err := readerClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	require.NotNil(t, readerLogHandle)
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
		defer closeCancel()
		_ = readerLogHandle.Close(closeCtx)
	}()

	startMsgId := &log.LogMessageId{SegmentId: 0, EntryId: 0}
	logReader, err := readerLogHandle.OpenLogReader(ctx, startMsgId, "direct-read-test-reader")
	require.NoError(t, err)
	require.NotNil(t, logReader)
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
		defer closeCancel()
		_ = logReader.Close(closeCtx)
	}()

	// Read entries from sealed segments.
	// We know sealedEntries count — read exactly that many entries.
	// Non-sealed segments (Completed/Active) won't be readable since quorum nodes are down.
	var readMessages []*log.LogMessage
	for i := int64(0); i < sealedEntries; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		msg, readErr := logReader.ReadNext(readCtx)
		readCancel()
		require.NoError(t, readErr, "Failed to read entry %d from sealed segment", i)
		require.NotNil(t, msg)
		readMessages = append(readMessages, msg)
		t.Logf("Direct-read entry (seg=%d, entry=%d): %s",
			msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
	}

	// Phase 5: Verify data integrity
	t.Log("Phase 5: Verifying data integrity...")

	assert.Equal(t, int(sealedEntries), len(readMessages),
		"Should have read exactly %d entries from sealed segments", sealedEntries)

	// Verify each entry's payload matches what was written
	for i, msg := range readMessages {
		assert.Equal(t, allWritten[i].payload, msg.Payload,
			"Entry %d payload mismatch: expected %s, got %s",
			i, string(allWritten[i].payload), string(msg.Payload))
	}

	t.Logf("Successfully direct-read %d entries from %d sealed segments after cluster stop",
		len(readMessages), sealedCount)
}
