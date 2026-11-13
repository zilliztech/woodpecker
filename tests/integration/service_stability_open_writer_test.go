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
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/tests/utils/stability"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// OpenWriter has its own budget: unlike a steady append, it synchronously fences
// and completes EVERY Active segment. These are observation limits in this test,
// not RPC deadlines configured in the product (or in the embedded modes).
const (
	openWriterObservation       = 10 * time.Second
	openWriterSilentObservation = 5 * time.Second
	openWriterSeedEntries       = 5
)

type openWriterRPC struct {
	Operation   string    `json:"operation"`
	SegmentID   int64     `json:"segment_id"`
	Node        string    `json:"node"`
	StartedAt   time.Time `json:"started_at"`
	ElapsedMS   float64   `json:"elapsed_ms"`
	LastEntryID int64     `json:"last_entry_id"`
	Error       string    `json:"error,omitempty"`
}

type openWriterTiming struct {
	Case           string  `json:"case"`
	ActiveSegments int     `json:"active_segments"`
	Attempt        string  `json:"attempt"`
	BudgetMS       int64   `json:"budget_ms"`
	OpenWriterMS   float64 `json:"open_writer_ms"`
	FenceAllMS     float64 `json:"fence_all_ms"`
	// Per segment, these are the wall-clock envelopes of its parallel node RPCs,
	// not sums or individual-node percentiles. FenceAll also includes metadata.
	Segments []openWriterSegmentTiming `json:"segments"`
	RPCs     []openWriterRPC           `json:"rpcs"`
	Error    string                    `json:"error,omitempty"`
}

type openWriterSegmentTiming struct {
	SegmentID         int64   `json:"segment_id"`
	FenceRPCWallMS    float64 `json:"fence_rpc_wall_ms"`
	CompleteRPCWallMS float64 `json:"complete_rpc_wall_ms"`
}

type openWriterPool struct {
	client.LogStoreClientPool
	mu   sync.Mutex
	rpcs []openWriterRPC
}

func (p *openWriterPool) GetLogStoreClient(ctx context.Context, node string) (client.LogStoreClient, error) {
	c, err := p.LogStoreClientPool.GetLogStoreClient(ctx, node)
	if err != nil {
		return nil, err
	}
	return &openWriterClient{LogStoreClient: c, pool: p, node: node}, nil
}

type openWriterClient struct {
	client.LogStoreClient
	pool *openWriterPool
	node string
}

func (c *openWriterClient) record(operation string, seg int64, started time.Time, last int64, err error) {
	r := openWriterRPC{
		Operation: operation, SegmentID: seg, Node: c.node, StartedAt: started,
		ElapsedMS: float64(time.Since(started)) / float64(time.Millisecond), LastEntryID: last,
	}
	if err != nil {
		r.Error = err.Error()
	}
	c.pool.mu.Lock()
	c.pool.rpcs = append(c.pool.rpcs, r)
	c.pool.mu.Unlock()
}

func (c *openWriterClient) FenceSegment(ctx context.Context, bucket, root string, logID, seg int64) (int64, error) {
	start := time.Now()
	last, err := c.LogStoreClient.FenceSegment(ctx, bucket, root, logID, seg)
	c.record("fence", seg, start, last, err)
	return last, err
}

func (c *openWriterClient) CompleteSegment(ctx context.Context, bucket, root string, logID, seg, lac int64) (int64, error) {
	start := time.Now()
	last, err := c.LogStoreClient.CompleteSegment(ctx, bucket, root, logID, seg, lac)
	c.record("complete", seg, start, last, err)
	return last, err
}

func (p *openWriterPool) snapshot() []openWriterRPC {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]openWriterRPC(nil), p.rpcs...)
}

// TestServiceStabilityProcess_OpenWriterFenceAll is selected by the existing
// process stability Makefile/CI regex. Do not run its subtests in parallel: each
// measurement temporarily installs a trace recorder for the existing fence-all
// span. No production timing hooks are needed.
func TestServiceStabilityProcess_OpenWriterFenceAll(t *testing.T) {
	cases := []struct {
		name string
		warm bool
	}{
		{"HealthyFresh", false},
		{"HealthyWarm", true},
		{"KillRefused", true},
		{"SlowRefusal", true},
		{"NewConnectionSilent", false},
		{"EstablishedConnectionSilent", true},
	}
	t.Run("EmptyLog", func(t *testing.T) { runOpenWriterFenceCase(t, "HealthyFresh", false, 0) })
	for _, tc := range cases {
		for _, segments := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/segments_%d", tc.name, segments), func(t *testing.T) {
				runOpenWriterFenceCase(t, tc.name, tc.warm, segments)
			})
		}
	}
}

// The three rotations preserve the #320 history: data on two replicas, an
// empty third replica, and one data-bearing replica missing from Fence replies.
func TestServiceStabilityProcess_OpenWriterMissingFenceReply(t *testing.T) {
	for rotation := 0; rotation < 3; rotation++ {
		t.Run(fmt.Sprintf("rotation_%d", rotation), func(t *testing.T) {
			runOpenWriterFenceCase(t, "MissingDataBearingReply", false, 1, rotation)
		})
	}
}

func runOpenWriterFenceCase(t *testing.T, fault string, warm bool, active int, rotation ...int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Etcd.RootPath = fmt.Sprintf("stability-open-writer-%d", time.Now().UnixNano())
	cluster, cfg := utils.StartPodClusterWithCfg(t, 4, filepath.Join(t.TempDir(), "service"), cfg)
	t.Cleanup(func() { stability.Bounded(t, "cluster stop", time.Minute, func() { cluster.Stop(t) }) })
	nodes := cluster.ServiceSeeds()
	if len(rotation) > 0 {
		old := append([]string(nil), nodes[:3]...)
		for i := 0; i < 3; i++ {
			nodes[i] = old[(i+rotation[0])%3]
		}
	}
	faultNode, ok := cluster.NodeIndexByServiceAddr(nodes[0])
	require.True(t, ok)
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	t.Cleanup(func() { _ = etcdCli.Close() })
	md := meta.NewMetadataProvider(ctx, etcdCli, cfg)
	require.NoError(t, md.InitIfNecessary(ctx))
	t.Cleanup(func() { _ = md.Close() })
	name := fmt.Sprintf("open_writer_%d", time.Now().UnixNano())
	require.NoError(t, md.CreateLog(ctx, name))
	lm, _, err := md.OpenLog(ctx, name)
	require.NoError(t, err)
	logID := lm.Metadata.GetLogId()
	oldQuorum := &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: append([]string(nil), nodes[:3]...)}
	newQuorum := &proto.QuorumInfo{Id: 2, Es: 3, Wq: 3, Aq: 2, Nodes: append([]string(nil), nodes[1:]...)}
	require.NoError(t, md.StoreQuorumInfo(ctx, oldQuorum))
	require.NoError(t, md.StoreQuorumInfo(ctx, newQuorum))
	newPool := func() client.LogStoreClientPool {
		return client.NewLogStoreClientPool(268435456, 536870912, client.WithGRPCClientConfig(cfg.Woodpecker.Client.GRPC))
	}
	seedPool := newPool()
	t.Cleanup(func() { _ = seedPool.Close(context.Background()) })
	// A crashed writer leaves Active metadata and durable entries, without a
	// graceful Close or a live auditor completing them during the measurement.
	// Seed that state through real etcd and service RPCs, not mocked segments.
	for seg := 0; seg < active; seg++ {
		require.NoError(t, md.StoreSegmentMetadata(ctx, name, logID, &meta.SegmentMeta{Metadata: &proto.SegmentMetadata{
			SegNo: int64(seg), CreateTime: time.Now().UnixMilli(), State: proto.SegmentState_Active,
			LastEntryId: -1, QuorumId: oldQuorum.Id, Quorum: oldQuorum,
		}}))
		for entry := 0; entry < openWriterSeedEntries; entry++ {
			payload, marshalErr := log.MarshalMessage(&log.WriteMessage{Payload: openWriterPayload(seg, entry)})
			require.NoError(t, marshalErr)
			for _, node := range oldQuorum.Nodes {
				if fault == "MissingDataBearingReply" && node == nodes[2] {
					continue
				}
				c, getErr := seedPool.GetLogStoreClient(ctx, node)
				require.NoError(t, getErr)
				ch := channel.NewRemoteResultChannel(fmt.Sprintf("seed/%d/%d", seg, entry))
				_, sendErr := c.AppendEntry(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, logID, &proto.LogEntry{
					SegId: int64(seg), EntryId: int64(entry), Values: payload,
				}, ch)
				require.NoError(t, sendErr)
				result, readErr := ch.ReadResult(ctx)
				require.NoError(t, readErr)
				require.NoError(t, result.Err)
				require.GreaterOrEqual(t, result.SyncedId, int64(entry))
				require.NoError(t, ch.Close(ctx))
			}
		}
	}
	pool := &openWriterPool{LogStoreClientPool: seedPool}
	if !warm {
		pool.LogStoreClientPool = newPool()
		t.Cleanup(func() { _ = pool.Close(context.Background()) })
	}
	_, segments, err := md.OpenLog(ctx, name)
	require.NoError(t, err)
	// This is the same constructor used by Client.OpenLog. Pool decoration only
	// records real node RPCs; quorum selection for the first NEW segment uses
	// the three healthy nodes so it is outside the failed old quorum.
	handle := log.NewLogHandle(name, logID, segments, md, pool, cfg,
		func(context.Context) (*proto.QuorumInfo, error) { return newQuorum, nil }, nil)
	t.Cleanup(func() {
		stability.Bounded(t, "log handle close", 15*time.Second, func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer closeCancel()
			_ = handle.Close(closeCtx)
		})
	})
	proxy := cluster.Proxies[faultNode]
	switch fault {
	case "KillRefused", "SlowRefusal":
		if fault == "SlowRefusal" {
			require.NoError(t, proxy.RefuseAfter(200*time.Millisecond))
		} else {
			proxy.Refuse()
		}
		require.NoError(t, cluster.Servers[faultNode].Kill())
		cluster.Servers[faultNode] = nil
		proxy.ResetAll()
	case "NewConnectionSilent", "EstablishedConnectionSilent", "MissingDataBearingReply":
		require.NoError(t, proxy.Blackhole())
	}
	// Cleanup restores the path BEFORE handle.Close, including on assertion
	// failures. A formerly frozen connection remains frozen until reset.
	if fault == "NewConnectionSilent" || fault == "EstablishedConnectionSilent" || fault == "MissingDataBearingReply" {
		t.Cleanup(func() {
			proxy.ResetAll()
			require.NoError(t, proxy.SetForward(fmt.Sprintf("127.0.0.1:%d", cluster.UsedPorts[faultNode])))
		})
	}
	budget := openWriterObservation
	if fault == "EstablishedConnectionSilent" {
		budget = openWriterSilentObservation
	}
	writer, openErr := measureOpenWriter(t, handle, pool, active, "initial", budget)
	require.NoError(t, openErr)
	require.NotNil(t, writer)
	// Recovery read/write checks are separate from the measured fault window.
	if fault == "NewConnectionSilent" || fault == "EstablishedConnectionSilent" || fault == "MissingDataBearingReply" {
		proxy.ResetAll()
		require.NoError(t, proxy.SetForward(fmt.Sprintf("127.0.0.1:%d", cluster.UsedPorts[faultNode])))
		pool.Clear(ctx, nodes[0])
	}
	t.Cleanup(func() {
		stability.Bounded(t, "writer close", 15*time.Second, func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer closeCancel()
			require.NoError(t, writer.Close(closeCtx))
		})
	})
	for seg := 0; seg < active; seg++ {
		m, getErr := md.GetSegmentMetadata(ctx, name, int64(seg))
		require.NoError(t, getErr)
		require.Equal(t, proto.SegmentState_Completed, m.Metadata.State)
		require.EqualValues(t, openWriterSeedEntries-1, m.Metadata.LastEntryId)
	}
	result := writer.Write(ctx, &log.WriteMessage{Payload: []byte("after-open")})
	require.NoError(t, result.Err)
	require.EqualValues(t, active, result.LogMessageId.SegmentId, "reopen must advance beyond ALL old segments")
	require.Zero(t, result.LogMessageId.EntryId)
	// Check the recovery boundary all the way through object-storage compaction,
	// using a fresh handle and direct object reads for the sealed old segments.
	for seg := 0; seg < active; seg++ {
		sh, getErr := handle.GetExistsReadonlySegmentHandle(ctx, int64(seg))
		require.NoError(t, getErr)
		require.NoError(t, sh.Compact(ctx))
		m, getErr := md.GetSegmentMetadata(ctx, name, int64(seg))
		require.NoError(t, getErr)
		require.Equal(t, proto.SegmentState_Sealed, m.Metadata.State)
		require.EqualValues(t, openWriterSeedEntries-1, m.Metadata.LastEntryId)
	}
	objects, err := objectstorage.NewObjectStorage(ctx, cfg)
	require.NoError(t, err)
	readCfg := *cfg
	readCfg.Woodpecker.Client.DirectRead.Enabled = true
	_, freshSegments, err := md.OpenLog(ctx, name)
	require.NoError(t, err)
	freshPool := newPool()
	defer func() { _ = freshPool.Close(context.Background()) }()
	fresh := log.NewLogHandle(name, logID, freshSegments, md, freshPool, &readCfg, nil, objects)
	defer func() { _ = fresh.Close(context.Background()) }()
	reader, err := fresh.OpenLogReader(ctx, &log.LogMessageId{SegmentId: 0, EntryId: 0}, "open-writer-verify")
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close(ctx)) }()
	for seg := 0; seg < active; seg++ {
		for entry := 0; entry < openWriterSeedEntries; entry++ {
			msg, readErr := reader.ReadNext(ctx)
			require.NoError(t, readErr)
			require.Equal(t, openWriterPayload(seg, entry), msg.Payload)
			require.Equal(t, &log.LogMessageId{SegmentId: int64(seg), EntryId: int64(entry)}, msg.Id)
		}
	}
	msg, err := reader.ReadNext(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("after-open"), msg.Payload)
	require.Equal(t, result.LogMessageId, msg.Id)
}

func openWriterPayload(seg, entry int) []byte {
	return []byte(fmt.Sprintf("before-open/%d/%d", seg, entry))
}

func measureOpenWriter(t *testing.T, handle log.LogHandle, pool *openWriterPool, active int, attempt string, budget time.Duration) (log.LogWriter, error) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) }()
	before := len(pool.snapshot())
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	type openResult struct {
		writer log.LogWriter
		err    error
	}
	done := make(chan openResult, 1)
	started := time.Now()
	go func() {
		writer, err := handle.OpenLogWriter(ctx)
		done <- openResult{writer: writer, err: err}
	}()
	var result openResult
	select {
	case result = <-done:
	case <-time.After(budget + 5*time.Second):
		t.Fatal("OpenLogWriter did not honour the observation context")
	}
	writer, err := result.writer, result.err
	report := openWriterTiming{Case: t.Name(), ActiveSegments: active, Attempt: attempt, BudgetMS: budget.Milliseconds(), OpenWriterMS: float64(time.Since(started)) / float64(time.Millisecond)}
	contextErr := ctx.Err()
	// Retire any straggler RPCs after OpenWriter returns, including if a future
	// implementation can safely finish without waiting for every replica.
	cancel()
	if err != nil {
		report.Error = err.Error()
		require.ErrorIs(t, contextErr, context.DeadlineExceeded)
	}
	// Wait for the successful Fence bookkeeping. Surplus RPCs are cancelled
	// independently and may be absent from this snapshot; per-node wall envelopes
	// below describe recorded calls, not the foreground collector duration.
	require.Eventually(t, func() bool {
		n := 0
		for _, rpc := range pool.snapshot()[before:] {
			if rpc.Operation == "fence" {
				n++
			}
		}
		return n >= active*2
	}, time.Second, time.Millisecond)
	report.RPCs = pool.snapshot()[before:]
	for seg := 0; seg < active; seg++ {
		fenced, completed := 0, 0
		for _, rpc := range report.RPCs {
			if rpc.SegmentID != int64(seg) || rpc.Error != "" {
				continue
			}
			require.GreaterOrEqual(t, rpc.LastEntryID, int64(-1))
			require.LessOrEqual(t, rpc.LastEntryID, int64(openWriterSeedEntries-1))
			if rpc.Operation == "fence" {
				fenced++
			} else if rpc.Operation == "complete" {
				completed++
			}
		}
		require.GreaterOrEqual(t, fenced, 2, "both healthy replicas must actually answer the fence")
		if err == nil {
			require.GreaterOrEqual(t, completed, 2, "the completion boundary must be durably recorded")
		}
	}
	for _, span := range recorder.Ended() {
		if span.InstrumentationScope().Name == "LogHandle" && span.Name() == "fenceAllActiveSegments" {
			report.FenceAllMS = float64(span.EndTime().Sub(span.StartTime())) / float64(time.Millisecond)
		}
	}
	require.Positive(t, report.FenceAllMS, "must measure the real OpenWriter fence-all span")
	for seg := 0; seg < active; seg++ {
		s := openWriterSegmentTiming{SegmentID: int64(seg)}
		for _, operation := range []string{"fence", "complete"} {
			var first, last time.Time
			for _, rpc := range report.RPCs {
				if rpc.SegmentID != int64(seg) || rpc.Operation != operation {
					continue
				}
				end := rpc.StartedAt.Add(time.Duration(rpc.ElapsedMS * float64(time.Millisecond)))
				if first.IsZero() || rpc.StartedAt.Before(first) {
					first = rpc.StartedAt
				}
				if last.IsZero() || end.After(last) {
					last = end
				}
			}
			if first.IsZero() {
				continue
			}
			ms := float64(last.Sub(first)) / float64(time.Millisecond)
			if operation == "fence" {
				s.FenceRPCWallMS = ms
			} else {
				s.CompleteRPCWallMS = ms
			}
		}
		report.Segments = append(report.Segments, s)
	}
	sort.Slice(report.RPCs, func(i, j int) bool { return report.RPCs[i].StartedAt.Before(report.RPCs[j].StartedAt) })
	encoded, encodeErr := json.Marshal(report)
	require.NoError(t, encodeErr)
	t.Log("OPEN_WRITER_RESULT " + string(encoded))
	return writer, err
}
