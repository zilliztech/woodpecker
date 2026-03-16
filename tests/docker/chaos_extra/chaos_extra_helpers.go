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

package chaos_extra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// writeEntriesAllowFailures writes n entries and returns successful IDs and failure count.
// Each entry has a timeout — timeouts count as failures.
func writeEntriesAllowFailures(t *testing.T, ctx context.Context, writer log.LogWriter, offset, count int, timeout time.Duration) ([]*log.LogMessageId, int) {
	t.Helper()
	ids := make([]*log.LogMessageId, 0, count)
	failures := 0
	for i := 0; i < count; i++ {
		resultCh := make(chan *log.WriteResult, 1)
		go func(idx int) {
			ch := writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("entry-%d", offset+idx)),
				Properties: map[string]string{
					"index": fmt.Sprintf("%d", offset+idx),
				},
			})
			if ch != nil {
				resultCh <- <-ch
			}
		}(i)

		select {
		case result := <-resultCh:
			if result.Err != nil {
				failures++
				t.Logf("write entry %d failed: %v", offset+i, result.Err)
			} else {
				ids = append(ids, result.LogMessageId)
			}
		case <-time.After(timeout):
			failures++
			t.Logf("write entry %d timed out after %v", offset+i, timeout)
		}
	}
	return ids, failures
}

// verifyEntryOrder checks that entries have monotonically increasing IDs.
func verifyEntryOrder(t *testing.T, msgs []*log.LogMessage) {
	t.Helper()
	for i := 1; i < len(msgs); i++ {
		prev := msgs[i-1].Id
		curr := msgs[i].Id
		if curr.SegmentId == prev.SegmentId {
			assert.Greater(t, curr.EntryId, prev.EntryId,
				"entry %d: entryId should increase within segment %d", i, curr.SegmentId)
		} else {
			assert.Greater(t, curr.SegmentId, prev.SegmentId,
				"entry %d: segmentId should increase across segments", i)
		}
	}
}

// newChaosExtraCluster creates a ChaosExtraCluster for use in chaos tests.
// It does NOT call Up/Down — that is managed by run_chaos_extra_tests.sh.
func newChaosExtraCluster(t *testing.T) *ChaosExtraCluster {
	t.Helper()
	return NewChaosExtraCluster(t)
}

// writeAndVerify is a convenience function that writes entries, reads them back, and verifies order.
func writeAndVerify(t *testing.T, ctx context.Context, cluster *ChaosExtraCluster, logName string, offset, count int) []*log.LogMessageId {
	t.Helper()

	client, _ := cluster.NewClient(t, ctx)

	logHandle, err := client.OpenLog(ctx, logName)
	if err != nil {
		t.Fatalf("failed to open log: %v", err)
	}

	writer, err := logHandle.OpenLogWriter(ctx)
	if err != nil {
		t.Fatalf("failed to open writer: %v", err)
	}

	ids := framework.WriteEntries(t, ctx, writer, offset, count)
	if len(ids) != count {
		t.Fatalf("expected %d entries, got %d", count, len(ids))
	}

	err = writer.Close(ctx)
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return ids
}

// readAndVerifyAll reads all entries from earliest and verifies count and order.
func readAndVerifyAll(t *testing.T, ctx context.Context, cluster *ChaosExtraCluster, logName string, expectedCount int) []*log.LogMessage {
	t.Helper()

	client, _ := cluster.NewClient(t, ctx)

	logHandle, err := client.OpenLog(ctx, logName)
	if err != nil {
		t.Fatalf("failed to open log: %v", err)
	}

	msgs := framework.ReadAllEntries(t, ctx, logHandle, expectedCount)
	if len(msgs) != expectedCount {
		t.Fatalf("expected %d entries, got %d", expectedCount, len(msgs))
	}

	verifyEntryOrder(t, msgs)
	return msgs
}
