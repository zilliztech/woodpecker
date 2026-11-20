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

package segment

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

func shrinkAppendAckReadTimeout(t *testing.T, d time.Duration) {
	old := appendAckReadTimeout
	appendAckReadTimeout = d
	t.Cleanup(func() { appendAckReadTimeout = old })
}

func TestReplicaAppenderACKTimeoutSpendsEachAttemptBudget(t *testing.T) {
	shrinkAppendAckReadTimeout(t, 40*time.Millisecond)
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "embed", true: "service"}[service], func(t *testing.T) {
			var attempts atomic.Int32
			c := &appenderTestClient{remote: service, send: func(context.Context, []*proto.LogEntry, []channel.ResultChannel) error { attempts.Add(1); return nil }}
			h := appenderFixture(t, service, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 3}, c)
			start := time.Now()
			err := receive(t, appendForTest(h, []byte("v")))
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.True(t, werr.IsRetryableErr(err))
			require.Equal(t, int32(3), attempts.Load())
			require.GreaterOrEqual(t, time.Since(start), 3*40*time.Millisecond)
		})
	}
}

func TestReplicaAppenderQueueTimeDoesNotConsumeACKBudget(t *testing.T) {
	shrinkAppendAckReadTimeout(t, 40*time.Millisecond)
	entered, release := make(chan struct{}), make(chan struct{})
	c := &appenderTestClient{send: func(ctx context.Context, es []*proto.LogEntry, chs []channel.ResultChannel) error {
		if es[0].EntryId == 0 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return ackEntries(ctx, es, chs)
	}}
	h := appenderFixture(t, true, config.SegmentAppendConfig{QueueSize: 8, MaxRetries: 2}, c)
	first := appendForTest(h, []byte("first"))
	receive(t, entered)
	second := appendForTest(h, []byte("second"))
	time.Sleep(60 * time.Millisecond)
	close(release)
	require.NoError(t, receive(t, first))
	require.NoError(t, receive(t, second))
	require.False(t, h.rollingState.Load())
}
