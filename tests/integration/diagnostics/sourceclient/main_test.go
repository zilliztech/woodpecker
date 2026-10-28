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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/woodpecker/proto"
)

type verifyServer struct {
	proto.UnimplementedLogStoreServer
	writes  atomic.Int32
	corrupt bool
	owner   string
}

func (s *verifyServer) AddEntry(_ *proto.AddEntryRequest, _ grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
	s.writes.Add(1)
	return nil
}

func (s *verifyServer) GetBatchEntriesAdv(_ context.Context, request *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error) {
	if request.FromEntryId == 2 {
		return &proto.GetBatchEntriesAdvResponse{Status: &proto.Status{Code: 8102}}, nil
	}
	values := []*proto.LogEntry{
		{EntryId: 0, SegId: 1, Values: []byte(s.owner + "/entry/0")},
		{EntryId: 1, SegId: 1, Values: []byte(s.owner + "/entry/1")},
	}
	if s.corrupt {
		values[1].Values = []byte("wrong acknowledged value")
	}
	return &proto.GetBatchEntriesAdvResponse{Status: &proto.Status{}, Result: &proto.BatchReadResult{Entries: values}}, nil
}

func TestVerifyOnlyNeverAppendsAndChecksOriginalPayloads(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		server := grpc.NewServer()
		worker := &verifyServer{owner: "source-20261028t060000z-1234abcd", corrupt: corrupt}
		proto.RegisterLogStoreServer(server, worker)
		go func() { _ = server.Serve(listener) }()
		var output bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = verify(ctx, listener.Addr().String(), worker.owner, &output)
		cancel()
		server.Stop()
		require.Zero(t, worker.writes.Load(), "Verification must not rewrite acknowledged entries")
		if corrupt {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			decoder := json.NewDecoder(&output)
			for _, name := range []string{"read", "eof"} {
				var r receipt
				require.NoError(t, decoder.Decode(&r))
				require.Equal(t, name, r.Case)
				require.True(t, r.Verified)
			}
		}
	}
}

// Protocol-only counterexamples; never substituted for a real acceptance run.
type badServer struct {
	proto.UnimplementedLogStoreServer
	mode string
}

func (s *badServer) AddEntry(_ *proto.AddEntryRequest, stream grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
	if s.mode == "empty" {
		return nil
	}
	if s.mode == "reject" {
		return stream.Send(&proto.AddEntryResponse{State: proto.AddEntryState_Failed, Status: &proto.Status{Code: 8101}})
	}
	return stream.Send(&proto.AddEntryResponse{State: proto.AddEntryState_Synced, Status: &proto.Status{Code: 0}})
}

func TestExplicitFixtureBoundary(t *testing.T) {
	owner := "source-20261028t060000z-1234abcd"
	require.True(t, selection("127.0.0.1:18080", owner))
	for _, endpoint := range []string{"localhost:18080", "10.0.0.1:18080", "127.0.0.1:0", "127.0.0.1:65536"} {
		require.False(t, selection(endpoint, owner))
	}
	require.False(t, selection("127.0.0.1:18080", "milvus"))
	require.Error(t, run(context.Background(), "10.0.0.1:18080", owner, io.Discard))
}

func TestTaggedContextIsRealUnsavedParent(t *testing.T) {
	ctx, id, err := traced(context.Background())
	require.NoError(t, err)
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid())
	require.True(t, sc.IsSampled())
	require.True(t, sc.IsRemote())
	require.Equal(t, id, sc.TraceID().String())
	require.False(t, trace.SpanFromContext(ctx).IsRecording(), "Supplying a parent must not fabricate an exported span")
}

func TestRejectionRequiresReceivedApplicationStatus(t *testing.T) {
	for _, mode := range []string{"empty", "reject", "synced_without_buffered"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			s := grpc.NewServer()
			proto.RegisterLogStoreServer(s, &badServer{mode: mode})
			go func() { _ = s.Serve(listener) }()
			t.Cleanup(s.Stop)
			c, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r := &receipt{}
			err = appendEntry(ctx, proto.NewLogStoreClient(c), "fixture", 0, true, r)
			if mode == "reject" {
				require.NoError(t, err)
				require.EqualValues(t, 8101, r.StatusCode)
			} else {
				require.Error(t, err, "Transport EOF or an invalid acknowledgement must not prove rejection")
			}
		})
	}
}
