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

package framework

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

const (
	// WriteResultTimeout is the maximum time to wait for a single WriteAsync result.
	WriteResultTimeout = 60 * time.Second
)

// WriteEntries writes n entries to the given writer and returns the successful message IDs.
// It uses WriteAsync and collects all results, requiring all writes to succeed.
// Each entry has a timeout to prevent hanging on quorum failures.
//
// IMPORTANT: WriteAsync can block synchronously on SelectQuorum (e.g., when retrying
// to find enough nodes). We run each WriteAsync in a goroutine so the timeout works
// even when WriteAsync blocks before returning the result channel.
func WriteEntries(t *testing.T, ctx context.Context, writer log.LogWriter, offset, count int) []*log.LogMessageId {
	t.Helper()
	ids := make([]*log.LogMessageId, 0, count)
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
			require.NoError(t, result.Err, "write entry %d failed", offset+i)
			require.NotNil(t, result.LogMessageId, "write entry %d returned nil ID", offset+i)
			ids = append(ids, result.LogMessageId)
		case <-time.After(WriteResultTimeout):
			t.Fatalf("write entry %d timed out after %v (possible quorum failure)", offset+i, WriteResultTimeout)
		}
	}
	return ids
}

// ReadAllEntries reads entries from the earliest position and returns them.
func ReadAllEntries(t *testing.T, ctx context.Context, logHandle log.LogHandle, expectedCount int) []*log.LogMessage {
	t.Helper()
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, fmt.Sprintf("reader-%d", time.Now().UnixNano()))
	require.NoError(t, err, "failed to open log reader")
	defer reader.Close(ctx)

	msgs := make([]*log.LogMessage, 0, expectedCount)
	for i := 0; i < expectedCount; i++ {
		msg, err := reader.ReadNext(ctx)
		require.NoError(t, err, "failed to read entry %d", i)
		require.NotNil(t, msg, "entry %d is nil", i)
		msgs = append(msgs, msg)
	}
	return msgs
}
