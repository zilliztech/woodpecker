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
	"fmt"
	"sync"
	"time"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/proto"
)

var _ Operation = (*BatchAppendOp)(nil)

// batchAckReadTimeout bounds how long the drain waits for a single entry's
// durability ack. Package var so tests can shrink it. TODO make configurable
// (like the append-path waits); tracked with the timeout audit.
var batchAckReadTimeout = 30 * time.Second

// BatchAppendOp groups a run of consecutive AppendOps from the same segment and
// sends their entries to each quorum replica in a single AddEntries request
// (client-side group commit). It amortizes the per-entry send round-trip that
// otherwise caps single-log append throughput.
//
// It is a SEND-side grouping only: every entry keeps its own AppendOp, result
// channel, receivedAckCallback, quorum tracking, and in-order acknowledgement
// via SendAppendSuccessCallbacks. Partial success therefore falls out of the
// existing ordered-LAC machinery — the committed prefix is acknowledged in order
// and promptly, and any not-yet-acked suffix is retried (or fenced/rolled) by
// the per-entry failure path — without any new out-of-order bookkeeping here.
type BatchAppendOp struct {
	ops []*AppendOp
}

func NewBatchAppendOp(ops []*AppendOp) *BatchAppendOp {
	return &BatchAppendOp{ops: ops}
}

func (b *BatchAppendOp) Identifier() string {
	if len(b.ops) == 0 {
		return "batch[]"
	}
	first := b.ops[0]
	last := b.ops[len(b.ops)-1]
	return fmt.Sprintf("batch[%d/%d/%d..%d]", first.logId, first.segmentId, first.entryId, last.entryId)
}

// Execute sends the batch to every quorum replica concurrently. Per replica, all
// entries travel in one AddEntries request; the per-entry results are routed
// back into each op's per-replica result channel, where the op's own
// receivedAckCallback handles quorum counting and acknowledgement exactly as in
// the single-entry path.
func (b *BatchAppendOp) Execute() {
	if len(b.ops) == 0 {
		return
	}
	ctx, sp := logger.NewIntentCtx("BatchAppendOp", "Execute")
	defer sp.End()

	quorumInfo := b.ops[0].quorumInfo
	nodeCount := len(quorumInfo.Nodes)

	// Pre-size each op's per-replica result-channel slice before fanning out, so
	// the per-replica goroutines below each write a disjoint index without racing
	// on the slice header.
	for _, op := range b.ops {
		op.mu.Lock()
		if len(op.resultChannels) == 0 {
			op.resultChannels = make([]channel.ResultChannel, nodeCount)
		}
		op.mu.Unlock()
	}

	// The entries payload is identical for every replica and immutable once built.
	entries := make([]*proto.LogEntry, len(b.ops))
	for i, op := range b.ops {
		entries[i] = op.toLogEntry()
	}

	var wg sync.WaitGroup
	wg.Add(nodeCount)
	for nodeIdx := 0; nodeIdx < nodeCount; nodeIdx++ {
		go func(nodeIdx int) {
			defer wg.Done()
			b.sendBatchToNode(ctx, entries, nodeIdx)
		}(nodeIdx)
	}
	wg.Wait()
}

func (b *BatchAppendOp) sendBatchToNode(ctx context.Context, entries []*proto.LogEntry, nodeIdx int) {
	first := b.ops[0]
	serverAddr := first.quorumInfo.Nodes[nodeIdx]

	cli, clientErr := first.clientPool.GetLogStoreClient(ctx, serverAddr)
	if clientErr != nil {
		b.failNode(ctx, nodeIdx, serverAddr, clientErr)
		return
	}

	// Each op gets an in-process result sink for THIS replica; the client routes
	// the per-entry Synced/Failed responses into them by entry id.
	resultChs := make([]channel.ResultChannel, len(b.ops))
	for i, op := range b.ops {
		lc := channel.NewLocalResultChannel(op.Identifier())
		op.mu.Lock()
		op.resultChannels[nodeIdx] = lc
		op.mu.Unlock()
		resultChs[i] = lc
	}

	startRequestTime := time.Now()
	// The drain goroutine below owns the stream's lifetime: streamCtx is cancelled
	// when the drain exits (normally or on give-up), so a stalled peer can't leave
	// the client demux goroutine + gRPC stream + server handler hanging.
	streamCtx, streamCancel := context.WithCancel(ctx)
	_, err := cli.AppendEntries(streamCtx, first.bucketName, first.rootPath, first.logId, entries, resultChs)
	if err != nil {
		streamCancel()
		b.failNode(ctx, nodeIdx, serverAddr, err)
		return
	}

	// Send succeeded: ONE goroutine per node drains the per-entry durability
	// acks IN ORDER (the server streams them in entry-id / flush order) and
	// applies quorum inline, instead of spawning a receivedAckCallback goroutine
	// per op. For a batch of N entries this is 1 goroutine per node instead of N.
	go func() {
		defer streamCancel()
		for i, op := range b.ops {
			// Per-entry deadline: a slow early entry must not shrink a later
			// (still-arriving) entry's budget into a false timeout.
			readCtx, cancel := context.WithTimeout(context.Background(), batchAckReadTimeout)
			result, readErr := resultChs[i].ReadResult(readCtx)
			cancel()
			op.applyNodeAck(ctx, startRequestTime, result, readErr, nodeIdx, serverAddr)
			if readErr != nil {
				// The stream stalled (no ack within the deadline). Acks are ordered,
				// so entries after i won't arrive either — fail them fast instead of
				// blocking a full deadline each, and let the deferred streamCancel tear
				// down the hung stream promptly.
				for j := i + 1; j < len(b.ops); j++ {
					b.ops[j].applyNodeAck(ctx, startRequestTime, nil, readErr, nodeIdx, serverAddr)
				}
				return
			}
		}
	}()
}

// failNode marks every op in the batch as failed on the given replica, routing
// each through the per-entry failure path (which decides retry vs. quorum loss).
func (b *BatchAppendOp) failNode(ctx context.Context, nodeIdx int, serverAddr string, err error) {
	for _, op := range b.ops {
		op.channelErrors[nodeIdx] = err
		go op.handle.HandleAppendRequestFailure(ctx, op.entryId, err, nodeIdx, serverAddr)
	}
}
