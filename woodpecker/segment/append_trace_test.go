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
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// This fixture uses the actual remote client, generated gRPC protocol and
// executor. The server acknowledges protocol frames without storing data: it
// isolates propagation and never claims disk/durability or Milvus coverage.
type traceAppendServer struct {
	proto.UnimplementedLogStoreServer
	fail bool
}

func (s *traceAppendServer) AddEntry(req *proto.AddEntryRequest, stream grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
	id := req.GetEntry().GetEntryId()
	if err := stream.Send(&proto.AddEntryResponse{EntryId: id, State: proto.AddEntryState_Buffered}); err != nil {
		return err
	}
	// An actual delayed ack exercises the asynchronous response path.
	time.Sleep(10 * time.Millisecond)
	state, status := proto.AddEntryState_Synced, werr.Success()
	if s.fail {
		state, status = proto.AddEntryState_Failed, werr.Status(werr.ErrSegmentFenced)
	}
	return stream.Send(&proto.AddEntryResponse{EntryId: id, State: state, Status: status})
}

func (s *traceAppendServer) AddEntries(req *proto.AddEntriesRequest, stream grpc.ServerStreamingServer[proto.AddEntriesResponse]) error {
	ids := make([]int64, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		ids[i] = e.GetEntryId()
	}
	if err := stream.Send(&proto.AddEntriesResponse{EntryId: ids, State: proto.AddEntryState_Buffered}); err != nil {
		return err
	}
	time.Sleep(10 * time.Millisecond)
	return stream.Send(&proto.AddEntriesResponse{EntryId: ids, State: proto.AddEntryState_Synced, Status: werr.Success()})
}

func appendTraceFixture(t *testing.T, fail bool) (*tracetest.InMemoryExporter, client.LogStoreClientPool, *proto.QuorumInfo) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := func(service string) *sdk.TracerProvider {
		options := []sdk.TracerProviderOption{
			sdk.WithSyncer(exporter),
			sdk.WithSampler(sdk.ParentBased(sdk.AlwaysSample())),
			sdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(service))),
		}
		// The explicit test-only endpoint lets the same regression emit real OTLP
		// spans to a disposable Jaeger. Normal unit tests require no backend.
		if endpoint := os.Getenv("WP_TRACE_TEST_OTLP"); endpoint != "" {
			e, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
			require.NoError(t, err)
			options = append(options, sdk.WithSyncer(e))
		}
		p := sdk.NewTracerProvider(options...)
		t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })
		return p
	}
	clientProvider, serverProvider := provider("wp-client-trace-fixture"), provider("wp-server-trace-fixture")
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(clientProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider); otel.SetTextMapPropagator(previousPropagator) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(serverProvider))))
	proto.RegisterLogStoreServer(server, &traceAppendServer{fail: fail})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	pool := client.NewLogStoreClientPool(1<<20, 1<<20)
	t.Cleanup(func() { require.NoError(t, pool.Close(context.Background())) })
	return exporter, pool, &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{listener.Addr().String()}}
}

func queuedTraceOp(t *testing.T, ctx context.Context, id int64, pool client.LogStoreClientPool, q *proto.QuorumInfo, fail bool) *AppendOp {
	t.Helper()
	handle := mocks_segment_handle.NewSegmentHandle(t)
	if fail {
		handle.EXPECT().HandleAppendRequestFailure(mock.Anything, id, mock.Anything, 0, q.Nodes[0]).Return().Once()
	} else {
		handle.EXPECT().SendAppendSuccessCallbacks(mock.Anything, id).Return().Once()
	}
	// Use the production queue-capture boundary, then replace only the ack sink.
	s := &segmentHandleImpl{bucketName: "fixture", rootPath: "trace", logId: 9007199254740993, segmentId: 7, ClientPool: pool, quorumInfo: q}
	op := s.createPendingAppendOp(ctx, []byte("trace-fixture"), id, func(int64, int64, error) {})
	op.handle = handle
	return op
}

func traceSpan(t *testing.T, exporter *tracetest.InMemoryExporter, scope, name string) tracetest.SpanStub {
	t.Helper()
	var found tracetest.SpanStub
	require.Eventually(t, func() bool {
		for _, span := range exporter.GetSpans() {
			if span.InstrumentationScope.Name == scope && span.Name == name {
				found = span
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond)
	return found
}

func assertRemoteTrace(t *testing.T, exporter *tracetest.InMemoryExporter, method string, id trace.TraceID) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, span := range exporter.GetSpans() {
			if span.SpanKind == trace.SpanKindServer && strings.HasSuffix(span.Name, "/"+method) {
				if span.SpanContext.TraceID() != id || !span.Parent.IsValid() || !span.Parent.IsRemote() {
					return false
				}
				for _, caller := range exporter.GetSpans() {
					if caller.SpanKind == trace.SpanKindClient && caller.SpanContext.SpanID() == span.Parent.SpanID() && caller.SpanContext.TraceID() == id {
						return true
					}
				}
				return false
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond, "real gRPC server span must retain the execution trace and an exported client parent")
}

func writeTraceReceipt(t *testing.T, name string, execution tracetest.SpanStub, parents ...trace.SpanContext) {
	t.Helper()
	if folder := os.Getenv("WP_TRACE_TEST_RECEIPTS"); folder != "" {
		inputs := []string{}
		identities := []map[string]string{}
		for _, sc := range parents {
			inputs = append(inputs, sc.TraceID().String())
			identities = append(identities, map[string]string{"trace_id": sc.TraceID().String(), "span_id": sc.SpanID().String()})
		}
		body, err := json.MarshalIndent(map[string]any{
			"schema_version": "wp.trace.propagation.fixture/v1", "case": name,
			"execution_trace_id": execution.SpanContext.TraceID().String(), "input_trace_ids": inputs, "input_spans": identities,
			"scope": "Actual WP queue/remote-gRPC protocol fixture, not a Milvus or disk durability test",
		}, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(folder, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(folder, name+".json"), body, 0o600))
	}
}

func TestAppendTraceSingleRemote(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "single"
		if fail {
			name = "single-error"
		}
		t.Run(name, func(t *testing.T) {
			exporter, pool, quorum := appendTraceFixture(t, fail)
			ctx, parent := otel.Tracer("upstream-fixture").Start(context.Background(), "write")
			parentSC := parent.SpanContext()
			ctx, cancel := context.WithCancel(ctx)
			op := queuedTraceOp(t, ctx, 11, pool, quorum, fail)
			cancel()
			parent.End()
			// An expired request must not newly cancel an already queued append.
			executor := NewSequentialExecutor(4)
			executor.Start(context.Background())
			require.True(t, executor.Submit(context.Background(), op))
			executor.Stop(context.Background())
			waitAckGoroutinesDone(t, op)
			execution := traceSpan(t, exporter, "AppendOp", "Execute")
			require.Equal(t, parentSC.TraceID(), execution.SpanContext.TraceID())
			require.Equal(t, parentSC.SpanID(), execution.Parent.SpanID())
			assertRemoteTrace(t, exporter, "AddEntry", parentSC.TraceID())
			writeTraceReceipt(t, name, execution, parentSC)
		})
	}
}

func TestAppendTraceBatchRemote(t *testing.T) {
	exporter, pool, quorum := appendTraceFixture(t, false)
	ctx1, parent1 := otel.Tracer("upstream-fixture").Start(context.Background(), "write-1")
	ctx2, parent2 := otel.Tracer("upstream-fixture").Start(context.Background(), "write-2")
	sc1, sc2 := parent1.SpanContext(), parent2.SpanContext()
	ops := []*AppendOp{queuedTraceOp(t, ctx1, 21, pool, quorum, false), queuedTraceOp(t, ctx2, 22, pool, quorum, false), queuedTraceOp(t, ctx1, 23, pool, quorum, false)}
	parent1.End()
	parent2.End()
	batch := NewBatchAppendOp(ops)
	batch.Execute()
	for _, op := range ops {
		waitAckGoroutinesDone(t, op)
	}
	execution := traceSpan(t, exporter, "BatchAppendOp", "Execute")
	require.False(t, execution.Parent.IsValid(), "independent requests are linked, not assigned an arbitrary common parent")
	require.NotEqual(t, sc1.TraceID(), execution.SpanContext.TraceID())
	require.NotEqual(t, sc2.TraceID(), execution.SpanContext.TraceID())
	require.Len(t, execution.Links, 2, "duplicate observations of one input are not independent links")
	require.Equal(t, sc1, execution.Links[0].SpanContext)
	require.Equal(t, sc2, execution.Links[1].SpanContext)
	assertRemoteTrace(t, exporter, "AddEntries", execution.SpanContext.TraceID())
	writeTraceReceipt(t, "batch", execution, sc1, sc2)
}

func TestAppendTraceLinkIdentityAndUntracedInputs(t *testing.T) {
	exporter, _, _ := appendTraceFixture(t, false)
	ctx, parent := otel.Tracer("upstream-fixture").Start(context.Background(), "request")
	_, child := otel.Tracer("upstream-fixture").Start(ctx, "other-message")
	defer parent.End()
	defer child.End()
	batch := NewBatchAppendOp([]*AppendOp{{traceParent: parent.SpanContext()}, {traceParent: child.SpanContext()}, {}, {traceParent: parent.SpanContext()}})
	require.Len(t, batch.traceLinks(), 2, "different spans within one trace retain their distinct identities")
	// Zero-entry execution remains a no-op, including when tracing is enabled.
	NewBatchAppendOp(nil).Execute()
	require.Empty(t, exporter.GetSpans())
}

func TestAppendTraceSamplingAndLinkLimits(t *testing.T) {
	previous := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	limits := sdk.NewSpanLimits()
	limits.LinkCountLimit = 2
	provider := sdk.NewTracerProvider(sdk.WithSyncer(exporter), sdk.WithSampler(sdk.ParentBased(sdk.AlwaysSample())), sdk.WithSpanLimits(limits))
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) }()
	contexts := []trace.SpanContext{}
	for i := byte(1); i <= 3; i++ {
		contexts = append(contexts, trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{i}, SpanID: trace.SpanID{i}, TraceFlags: trace.FlagsSampled}))
	}
	NewBatchAppendOp([]*AppendOp{{traceParent: contexts[0], quorumInfo: &proto.QuorumInfo{}}, {traceParent: contexts[1]}, {traceParent: contexts[2]}}).Execute()
	batch := traceSpan(t, exporter, "BatchAppendOp", "Execute")
	require.Len(t, batch.Links, 2)
	require.Equal(t, 1, batch.DroppedLinks)
	// A single queued unsampled request must not be forced into sampling.
	unsampled := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{9}, SpanID: trace.SpanID{9}})
	(&AppendOp{traceParent: unsampled, quorumInfo: &proto.QuorumInfo{}}).Execute()
	for _, span := range exporter.GetSpans() {
		require.NotEqual(t, unsampled.TraceID(), span.SpanContext.TraceID())
	}
	(&AppendOp{quorumInfo: &proto.QuorumInfo{}}).Execute()
	root := traceSpan(t, exporter, "AppendOp", "Execute")
	require.False(t, root.Parent.IsValid())
}
