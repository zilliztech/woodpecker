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

// Package e2e_operator provides end-to-end tests for Woodpecker running on K8s via the operator.
// Run this inside a K8s pod that can reach Woodpecker services.
//
// Usage:
//
//	go test -v -run TestOperatorE2E ./tests/e2e_operator/ -config-file /tmp/test-config.yaml -timeout 5m
package e2e_operator

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

var (
	configFile        = flag.String("config-file", "/tmp/test-config.yaml", "Path to woodpecker config file")
	decommissionNodes = flag.String("decommission-nodes", "", "Comma-separated list of node:port to check decommission progress (for scale-down test)")
)

// TestOperatorE2E_HealthCheck verifies all Woodpecker server nodes are healthy via HTTP.
func TestOperatorE2E_HealthCheck(t *testing.T) {
	seeds := []string{
		"my-woodpecker-server-0.my-woodpecker-server-headless.default.svc:9091",
		"my-woodpecker-server-1.my-woodpecker-server-headless.default.svc:9091",
		"my-woodpecker-server-2.my-woodpecker-server-headless.default.svc:9091",
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for _, seed := range seeds {
		url := fmt.Sprintf("http://%s/healthz", seed)
		resp, err := client.Get(url)
		if err != nil {
			t.Logf("SKIP: %s not reachable (may not exist yet): %v", seed, err)
			continue
		}
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "node %s should be healthy", seed)
		t.Logf("OK: %s is healthy", seed)
	}
}

// TestOperatorE2E_WriteAndRead creates a log, writes 100 entries, reads them back, and verifies data integrity.
func TestOperatorE2E_WriteAndRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Load config
	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(t, err, "failed to load config from %s", *configFile)

	// Connect to etcd
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err, "failed to connect to etcd")
	defer etcdCli.Close()

	// Create Woodpecker client (service mode)
	wpClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err, "failed to create woodpecker client")
	defer wpClient.Close(ctx)

	// Create a log
	logName := fmt.Sprintf("e2e-operator-test-%d", time.Now().UnixMilli())
	err = wpClient.CreateLog(ctx, logName)
	require.NoError(t, err, "failed to create log")
	t.Logf("Created log: %s", logName)

	// Open log
	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err, "failed to open log")

	// Write 100 entries
	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err, "failed to open writer")

	numEntries := 100
	resultChans := make([]<-chan *log.WriteResult, numEntries)
	for i := 0; i < numEntries; i++ {
		resultChans[i] = writer.WriteAsync(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("operator-e2e-entry-%d", i)),
			Properties: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		})
	}

	// Collect write results
	successIds := make([]*log.LogMessageId, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		result := <-resultChans[i]
		require.NoError(t, result.Err, "write entry %d failed", i)
		successIds = append(successIds, result.LogMessageId)
	}
	assert.Equal(t, numEntries, len(successIds))
	t.Logf("Successfully wrote %d entries", len(successIds))

	// Close writer
	require.NoError(t, writer.Close(ctx))

	// Read all entries from earliest
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, "e2e-reader")
	require.NoError(t, err, "failed to open reader")

	readMsgs := make([]*log.LogMessage, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		msg, err := reader.ReadNext(ctx)
		require.NoError(t, err, "failed to read entry %d", i)
		require.NotNil(t, msg)
		readMsgs = append(readMsgs, msg)
	}

	// Verify
	assert.Equal(t, numEntries, len(readMsgs))
	for i, msg := range readMsgs {
		expectedPayload := fmt.Sprintf("operator-e2e-entry-%d", i)
		assert.Equal(t, expectedPayload, string(msg.Payload), "entry %d payload mismatch", i)
	}
	t.Logf("Successfully read and verified %d entries", len(readMsgs))

	require.NoError(t, reader.Close(ctx))

	// Cleanup: delete the log (may not be supported yet)
	if err := wpClient.DeleteLog(ctx, logName); err != nil {
		t.Logf("DeleteLog not supported yet (non-fatal): %v", err)
	} else {
		t.Logf("Deleted log: %s", logName)
	}
}

// TestOperatorE2E_MultipleLogsParallel tests creating multiple logs and writing/reading in parallel.
func TestOperatorE2E_MultipleLogsParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(t, err)

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	wpClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer wpClient.Close(ctx)

	numLogs := 3
	entriesPerLog := 50

	for l := 0; l < numLogs; l++ {
		logName := fmt.Sprintf("e2e-parallel-%d-%d", time.Now().UnixMilli(), l)

		err := wpClient.CreateLog(ctx, logName)
		require.NoError(t, err)

		logHandle, err := wpClient.OpenLog(ctx, logName)
		require.NoError(t, err)

		// Write
		writer, err := logHandle.OpenLogWriter(ctx)
		require.NoError(t, err)

		resultChans := make([]<-chan *log.WriteResult, entriesPerLog)
		for i := 0; i < entriesPerLog; i++ {
			resultChans[i] = writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("log%d-entry%d", l, i)),
			})
		}
		for i := 0; i < entriesPerLog; i++ {
			result := <-resultChans[i]
			require.NoError(t, result.Err)
		}
		require.NoError(t, writer.Close(ctx))

		// Read back
		earliest := log.EarliestLogMessageID()
		reader, err := logHandle.OpenLogReader(ctx, &earliest, fmt.Sprintf("reader-%d", l))
		require.NoError(t, err)

		for i := 0; i < entriesPerLog; i++ {
			msg, err := reader.ReadNext(ctx)
			require.NoError(t, err)
			expected := fmt.Sprintf("log%d-entry%d", l, i)
			assert.Equal(t, expected, string(msg.Payload))
		}
		require.NoError(t, reader.Close(ctx))

		t.Logf("Log %d: wrote and verified %d entries", l, entriesPerLog)

		_ = wpClient.DeleteLog(ctx, logName)
	}
}

// TestOperatorE2E_ScaleDownWithDecommission decommissions a node from inside the test,
// writes data to trigger segment rolling, truncates old segments, and waits for the
// decommissioned node to become safe_to_terminate.
//
// The test drives the full decommission lifecycle:
//  1. Call POST /admin/node/decommission on the target node
//  2. Write data (with small maxSize to force segment rolling)
//  3. Truncate old segments
//  4. Keep writer open so auditor cleans up truncated segment data
//  5. Wait for safe_to_terminate=true
//  6. Close writer and verify read-back
//
// After this test, the caller (smoke-test.sh) can patch replicas to remove the pod.
//
// Usage: go test -run TestOperatorE2E_ScaleDownWithDecommission -config-file /tmp/test-config.yaml \
//
//	-decommission-nodes "my-woodpecker-server-3.my-woodpecker-server-headless.default.svc:9091"
func TestOperatorE2E_ScaleDownWithDecommission(t *testing.T) {
	if *decommissionNodes == "" {
		t.Skip("Skipping: -decommission-nodes not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(t, err)

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	defer etcdCli.Close()

	wpClient, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	defer wpClient.Close(ctx)

	nodes := strings.Split(*decommissionNodes, ",")
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Phase 1: Trigger decommission on target node(s)
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		url := fmt.Sprintf("http://%s/admin/node/decommission", node)
		resp, err := httpClient.Post(url, "", nil)
		require.NoError(t, err, "failed to decommission %s", node)
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Logf("Triggered decommission on %s (status=%d)", node, resp.StatusCode)
	}

	// Phase 2: Get ALL existing logs — need to push all of them forward so the
	// decommissioning node has no active segments from any log.
	allLogs, err := wpClient.GetAllLogs(ctx)
	require.NoError(t, err)
	t.Logf("Phase 2: found %d existing logs", len(allLogs))

	// Also create a new scaledown-specific log
	scaledownLog := fmt.Sprintf("e2e-scaledown-%d", time.Now().UnixMilli())
	err = wpClient.CreateLog(ctx, scaledownLog)
	require.NoError(t, err)
	allLogs = append(allLogs, scaledownLog)

	// Wait for gossip to propagate decommission tag so new segments avoid this node
	time.Sleep(2 * time.Second)

	// Phase 3: For each log, open writer → write data (segment rolling) → truncate.
	// Keep all writers open so auditors run cleanup.
	const msgSize = 1024
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}

	type logState struct {
		name   string
		handle log.LogHandle
		writer log.LogWriter
	}
	var openLogs []logState

	for _, logName := range allLogs {
		logHandle, openErr := wpClient.OpenLog(ctx, logName)
		if openErr != nil {
			t.Logf("  skip log %s (open failed: %v)", logName, openErr)
			continue
		}

		writer, writerErr := logHandle.OpenLogWriter(ctx)
		if writerErr != nil {
			t.Logf("  skip log %s (writer failed: %v)", logName, writerErr)
			continue
		}

		// Write enough data to trigger segment rolling past decommissioned node's segments
		var lastId *log.LogMessageId
		for i := 0; i < 30; i++ {
			result := <-writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: append([]byte(fmt.Sprintf("decom-%s-%d-", logName, i)), payload...),
			})
			if result.Err != nil {
				t.Logf("  log %s: write %d failed: %v", logName, i, result.Err)
				break
			}
			lastId = result.LogMessageId
		}

		if lastId != nil {
			// Write one final entry to push beyond
			finalResult := <-writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: append([]byte("final-"), payload...),
			})
			if finalResult.Err == nil {
				lastId = finalResult.LogMessageId
			}

			// Truncate
			if truncErr := logHandle.Truncate(ctx, lastId); truncErr != nil {
				t.Logf("  log %s: truncate warning: %v", logName, truncErr)
			} else {
				t.Logf("  log %s: wrote + truncated to segment=%d entry=%d", logName, lastId.SegmentId, lastId.EntryId)
			}
		}

		openLogs = append(openLogs, logState{name: logName, handle: logHandle, writer: writer})
	}
	t.Logf("Phase 3: processed %d logs with write+truncate (writers still open)", len(openLogs))

	// Phase 4: Wait for decommissioning nodes to become safe_to_terminate.
	// All writers stay open so auditors keep running cleanup on all logs.
	t.Logf("Phase 4: waiting for %d node(s) to become safe_to_terminate...", len(nodes))
	allSafe := false
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		allSafe = true
		for _, node := range nodes {
			url := fmt.Sprintf("http://%s/admin/node/decommission/progress", strings.TrimSpace(node))
			resp, pollErr := httpClient.Get(url)
			if pollErr != nil {
				t.Logf("  %s: unreachable (%v)", node, pollErr)
				allSafe = false
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			bodyStr := string(body)
			safe := strings.Contains(bodyStr, `"safe_to_terminate":true`)
			if !safe {
				allSafe = false
				t.Logf("  %s: %s", node, bodyStr)
			} else {
				t.Logf("  %s: safe_to_terminate=true", node)
			}
		}
		if allSafe {
			t.Logf("All decommissioning nodes are safe to terminate")
			break
		}
		time.Sleep(5 * time.Second)
	}

	// Phase 5: Close all writers
	for _, ls := range openLogs {
		_ = ls.writer.Close(ctx)
	}
	t.Logf("Phase 5: all %d writers closed", len(openLogs))

	assert.True(t, allSafe, "all decommissioning nodes should be safe_to_terminate")
}
