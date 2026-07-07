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

package client

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/proto"
)

// mockAddEntriesStream mocks grpc.ServerStreamingClient[proto.AddEntriesResponse].
type mockAddEntriesStream struct {
	responses []*proto.AddEntriesResponse
	index     int
}

func (s *mockAddEntriesStream) Recv() (*proto.AddEntriesResponse, error) {
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *mockAddEntriesStream) Header() (metadata.MD, error) { return nil, nil }
func (s *mockAddEntriesStream) Trailer() metadata.MD         { return nil }
func (s *mockAddEntriesStream) CloseSend() error             { return nil }
func (s *mockAddEntriesStream) Context() context.Context     { return context.Background() }
func (s *mockAddEntriesStream) SendMsg(m any) error          { return nil }
func (s *mockAddEntriesStream) RecvMsg(m any) error          { return nil }

// TestRemoteClient_AppendEntries_Demux verifies that AppendEntries returns the
// per-entry buffered ids after the single send round-trip and routes each
// Synced response to its result channel by entry id (responses arrive
// out-of-order on purpose to exercise the keyed demux).
func TestRemoteClient_AppendEntries_Demux(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	const n = 3
	entries := make([]*proto.LogEntry, n)
	resultChs := make([]channel.ResultChannel, n)
	for i := 0; i < n; i++ {
		entries[i] = &proto.LogEntry{SegId: 2, EntryId: int64(10 + i), Values: []byte("v")}
		resultChs[i] = channel.NewLocalResultChannel(fmt.Sprintf("e%d", 10+i))
	}

	responses := []*proto.AddEntriesResponse{
		// One Buffered frame carries the whole batch's ids.
		{State: proto.AddEntryState_Buffered, EntryId: []int64{10, 11, 12}},
		// Synced acks arrive as grouped, out-of-order frames (a multi-id run plus a
		// singleton) to exercise the keyed fan-out demux.
		{State: proto.AddEntryState_Synced, EntryId: []int64{12, 10}},
		{State: proto.AddEntryState_Synced, EntryId: []int64{11}},
	}
	mockClient.On("AddEntries", mock.Anything, mock.AnythingOfType("*proto.AddEntriesRequest")).
		Return(&mockAddEntriesStream{responses: responses}, nil)

	bufferedIds, err := client.AppendEntries(ctx, "bucket", "root", 1, entries, resultChs)
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 11, 12}, bufferedIds)

	for i := 0; i < n; i++ {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		res, rerr := resultChs[i].ReadResult(readCtx)
		cancel()
		assert.NoError(t, rerr)
		assert.NoError(t, res.Err)
		assert.Equal(t, entries[i].EntryId, res.SyncedId, "result must be routed to the matching entry")
	}
}

// TestRemoteClient_AppendEntries_StreamErrorFailsRemaining verifies that if the
// stream breaks before all entries are acked, the un-acked result channels are
// failed (so their callbacks don't block).
func TestRemoteClient_AppendEntries_StreamErrorFailsRemaining(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	const n = 2
	entries := make([]*proto.LogEntry, n)
	resultChs := make([]channel.ResultChannel, n)
	for i := 0; i < n; i++ {
		entries[i] = &proto.LogEntry{SegId: 2, EntryId: int64(20 + i), Values: []byte("v")}
		resultChs[i] = channel.NewLocalResultChannel(fmt.Sprintf("e%d", 20+i))
	}

	// Whole batch Buffered, then only one entry Synced before the stream ends (io.EOF).
	responses := []*proto.AddEntriesResponse{
		{State: proto.AddEntryState_Buffered, EntryId: []int64{20, 21}},
		{State: proto.AddEntryState_Synced, EntryId: []int64{20}},
	}
	mockClient.On("AddEntries", mock.Anything, mock.AnythingOfType("*proto.AddEntriesRequest")).
		Return(&mockAddEntriesStream{responses: responses}, nil)

	_, err := client.AppendEntries(ctx, "bucket", "root", 1, entries, resultChs)
	assert.NoError(t, err)

	// entry 20 synced ok
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	res0, rerr0 := resultChs[0].ReadResult(readCtx)
	cancel()
	assert.NoError(t, rerr0)
	assert.NoError(t, res0.Err)

	// entry 21 never got a Synced; the stream EOF must fail it rather than hang.
	readCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	res1, rerr1 := resultChs[1].ReadResult(readCtx2)
	cancel2()
	assert.NoError(t, rerr1)
	assert.Error(t, res1.Err, "un-acked entry must be failed on stream end")
}
