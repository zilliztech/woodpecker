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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

// ============================================================================
// Mock types for proto.LogStoreClient (gRPC client interface)
// ============================================================================

// mockProtoLogStoreClient mocks the proto.LogStoreClient gRPC interface
type mockProtoLogStoreClient struct {
	mock.Mock
}

func (m *mockProtoLogStoreClient) AddEntry(ctx context.Context, in *proto.AddEntryRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[proto.AddEntryResponse], error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(grpc.ServerStreamingClient[proto.AddEntryResponse]), args.Error(1)
}

func (m *mockProtoLogStoreClient) GetBatchEntriesAdv(ctx context.Context, in *proto.GetBatchEntriesAdvRequest, opts ...grpc.CallOption) (*proto.GetBatchEntriesAdvResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.GetBatchEntriesAdvResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) FenceSegment(ctx context.Context, in *proto.FenceSegmentRequest, opts ...grpc.CallOption) (*proto.FenceSegmentResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.FenceSegmentResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) CompleteSegment(ctx context.Context, in *proto.CompleteSegmentRequest, opts ...grpc.CallOption) (*proto.CompleteSegmentResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.CompleteSegmentResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) CompactSegment(ctx context.Context, in *proto.CompactSegmentRequest, opts ...grpc.CallOption) (*proto.CompactSegmentResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.CompactSegmentResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) GetSegmentLastAddConfirmed(ctx context.Context, in *proto.GetSegmentLastAddConfirmedRequest, opts ...grpc.CallOption) (*proto.GetSegmentLastAddConfirmedResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.GetSegmentLastAddConfirmedResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) GetSegmentBlockCount(ctx context.Context, in *proto.GetSegmentBlockCountRequest, opts ...grpc.CallOption) (*proto.GetSegmentBlockCountResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.GetSegmentBlockCountResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) UpdateLastAddConfirmed(ctx context.Context, in *proto.UpdateLastAddConfirmedRequest, opts ...grpc.CallOption) (*proto.UpdateLastAddConfirmedResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.UpdateLastAddConfirmedResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) CleanSegment(ctx context.Context, in *proto.CleanSegmentRequest, opts ...grpc.CallOption) (*proto.CleanSegmentResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.CleanSegmentResponse), args.Error(1)
}

func (m *mockProtoLogStoreClient) SelectNodes(ctx context.Context, in *proto.SelectNodesRequest, opts ...grpc.CallOption) (*proto.SelectNodesResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.SelectNodesResponse), args.Error(1)
}

// mockAddEntryStream mocks grpc.ServerStreamingClient[proto.AddEntryResponse]
type mockAddEntryStream struct {
	responses []*proto.AddEntryResponse
	index     int
}

func (s *mockAddEntryStream) Recv() (*proto.AddEntryResponse, error) {
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *mockAddEntryStream) Header() (metadata.MD, error) { return nil, nil }
func (s *mockAddEntryStream) Trailer() metadata.MD         { return nil }
func (s *mockAddEntryStream) CloseSend() error             { return nil }
func (s *mockAddEntryStream) Context() context.Context     { return context.Background() }
func (s *mockAddEntryStream) SendMsg(m any) error          { return nil }
func (s *mockAddEntryStream) RecvMsg(m any) error          { return nil }

func newRemoteClientWithMock(_ *testing.T) (*logStoreClientRemote, *mockProtoLogStoreClient) {
	mockClient := &mockProtoLogStoreClient{}
	client := &logStoreClientRemote{innerClient: mockClient}
	return client, mockClient
}

// ============================================================================
// logStoreClientRemote Tests
// ============================================================================

func TestRemoteClient_IsRemoteClient(t *testing.T) {
	client := &logStoreClientRemote{}
	assert.True(t, client.IsRemoteClient())
}

func TestRemoteClient_Close(t *testing.T) {
	client := &logStoreClientRemote{}

	err := client.Close(context.Background())
	assert.NoError(t, err)
}

func TestRemoteClient_Close_Idempotent(t *testing.T) {
	client := &logStoreClientRemote{}
	ctx := context.Background()

	err1 := client.Close(ctx)
	assert.NoError(t, err1)

	err2 := client.Close(ctx)
	assert.NoError(t, err2)
}

func TestRemoteClient_ImplementsInterface(t *testing.T) {
	var _ LogStoreClient = (*logStoreClientRemote)(nil)
}

func TestRemoteClient_CompleteSegment_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CompleteSegment", ctx, mock.AnythingOfType("*proto.CompleteSegmentRequest")).
		Return(&proto.CompleteSegmentResponse{LastEntryId: 99}, nil)

	lastEntryId, err := client.CompleteSegment(ctx, "bucket", "root", 1, 0, 99)
	assert.NoError(t, err)
	assert.Equal(t, int64(99), lastEntryId)
}

func TestRemoteClient_CompleteSegment_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CompleteSegment", ctx, mock.Anything).
		Return(nil, fmt.Errorf("grpc error"))

	_, err := client.CompleteSegment(ctx, "bucket", "root", 1, 0, 99)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "grpc error")
}

func TestRemoteClient_CompleteSegment_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CompleteSegment", ctx, mock.Anything).
		Return(&proto.CompleteSegmentResponse{
			Status:      werr.Status(werr.ErrSegmentNotFound),
			LastEntryId: -1,
		}, nil)

	_, err := client.CompleteSegment(ctx, "bucket", "root", 1, 0, 99)
	assert.Error(t, err)
}

func TestRemoteClient_ReadEntriesBatchAdv_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	expectedResult := &proto.BatchReadResult{
		Entries: []*proto.LogEntry{{EntryId: 0}, {EntryId: 1}},
	}
	mockClient.On("GetBatchEntriesAdv", ctx, mock.AnythingOfType("*proto.GetBatchEntriesAdvRequest")).
		Return(&proto.GetBatchEntriesAdvResponse{Result: expectedResult}, nil)

	result, err := client.ReadEntriesBatchAdv(ctx, "bucket", "root", 1, 0, 0, 100, nil)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func TestRemoteClient_ReadEntriesBatchAdv_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetBatchEntriesAdv", ctx, mock.Anything).
		Return(nil, fmt.Errorf("read error"))

	result, err := client.ReadEntriesBatchAdv(ctx, "bucket", "root", 1, 0, 0, 100, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestRemoteClient_ReadEntriesBatchAdv_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetBatchEntriesAdv", ctx, mock.Anything).
		Return(&proto.GetBatchEntriesAdvResponse{
			Status: werr.Status(werr.ErrEntryNotFound),
		}, nil)

	result, err := client.ReadEntriesBatchAdv(ctx, "bucket", "root", 1, 0, 0, 100, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestRemoteClient_FenceSegment_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("FenceSegment", ctx, mock.AnythingOfType("*proto.FenceSegmentRequest")).
		Return(&proto.FenceSegmentResponse{LastEntryId: 50}, nil)

	lastEntryId, err := client.FenceSegment(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), lastEntryId)
}

func TestRemoteClient_FenceSegment_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("FenceSegment", ctx, mock.Anything).
		Return(nil, fmt.Errorf("fence error"))

	_, err := client.FenceSegment(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
}

func TestRemoteClient_FenceSegment_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("FenceSegment", ctx, mock.Anything).
		Return(&proto.FenceSegmentResponse{
			Status: werr.Status(werr.ErrSegmentNotFound),
		}, nil)

	_, err := client.FenceSegment(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
}

func TestRemoteClient_GetLastAddConfirmed_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetSegmentLastAddConfirmed", ctx, mock.AnythingOfType("*proto.GetSegmentLastAddConfirmedRequest")).
		Return(&proto.GetSegmentLastAddConfirmedResponse{LastEntryId: 42}, nil)

	lac, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), lac)
}

func TestRemoteClient_GetLastAddConfirmed_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetSegmentLastAddConfirmed", ctx, mock.Anything).
		Return(nil, fmt.Errorf("lac error"))

	_, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
}

func TestRemoteClient_GetLastAddConfirmed_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetSegmentLastAddConfirmed", ctx, mock.Anything).
		Return(&proto.GetSegmentLastAddConfirmedResponse{
			Status: werr.Status(werr.ErrSegmentNotFound),
		}, nil)

	_, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
}

func TestRemoteClient_GetBlockCount_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetSegmentBlockCount", ctx, mock.AnythingOfType("*proto.GetSegmentBlockCountRequest")).
		Return(&proto.GetSegmentBlockCountResponse{BlockCount: 5}, nil)

	count, err := client.GetBlockCount(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestRemoteClient_GetBlockCount_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetSegmentBlockCount", ctx, mock.Anything).
		Return(nil, fmt.Errorf("block count error"))

	_, err := client.GetBlockCount(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
}

func TestRemoteClient_GetBlockCount_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("GetSegmentBlockCount", ctx, mock.Anything).
		Return(&proto.GetSegmentBlockCountResponse{
			Status: werr.Status(werr.ErrSegmentNotFound),
		}, nil)

	_, err := client.GetBlockCount(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
}

func TestRemoteClient_SegmentCompact_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	expectedMeta := &proto.SegmentMetadata{SegNo: 0, State: proto.SegmentState_Sealed}
	mockClient.On("CompactSegment", ctx, mock.AnythingOfType("*proto.CompactSegmentRequest")).
		Return(&proto.CompactSegmentResponse{Metadata: expectedMeta}, nil)

	meta, err := client.SegmentCompact(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, meta)
}

func TestRemoteClient_SegmentCompact_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CompactSegment", ctx, mock.Anything).
		Return(nil, fmt.Errorf("compact error"))

	meta, err := client.SegmentCompact(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
	assert.Nil(t, meta)
}

func TestRemoteClient_SegmentCompact_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CompactSegment", ctx, mock.Anything).
		Return(&proto.CompactSegmentResponse{
			Status: werr.Status(werr.ErrSegmentNotFound),
		}, nil)

	meta, err := client.SegmentCompact(ctx, "bucket", "root", 1, 0)
	assert.Error(t, err)
	assert.Nil(t, meta)
}

func TestRemoteClient_SegmentClean_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CleanSegment", ctx, mock.AnythingOfType("*proto.CleanSegmentRequest")).
		Return(&proto.CleanSegmentResponse{}, nil)

	err := client.SegmentClean(ctx, "bucket", "root", 1, 0, 1)
	assert.NoError(t, err)
}

func TestRemoteClient_SegmentClean_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CleanSegment", ctx, mock.Anything).
		Return(nil, fmt.Errorf("clean error"))

	err := client.SegmentClean(ctx, "bucket", "root", 1, 0, 1)
	assert.Error(t, err)
}

func TestRemoteClient_SegmentClean_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("CleanSegment", ctx, mock.Anything).
		Return(&proto.CleanSegmentResponse{
			Status: werr.Status(werr.ErrSegmentNotFound),
		}, nil)

	err := client.SegmentClean(ctx, "bucket", "root", 1, 0, 1)
	assert.Error(t, err)
}

func TestRemoteClient_UpdateLastAddConfirmed_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("UpdateLastAddConfirmed", ctx, mock.AnythingOfType("*proto.UpdateLastAddConfirmedRequest")).
		Return(&proto.UpdateLastAddConfirmedResponse{}, nil)

	err := client.UpdateLastAddConfirmed(ctx, "bucket", "root", 1, 0, 42)
	assert.NoError(t, err)
}

func TestRemoteClient_UpdateLastAddConfirmed_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("UpdateLastAddConfirmed", ctx, mock.Anything).
		Return(nil, fmt.Errorf("update lac error"))

	err := client.UpdateLastAddConfirmed(ctx, "bucket", "root", 1, 0, 42)
	assert.Error(t, err)
}

func TestRemoteClient_UpdateLastAddConfirmed_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("UpdateLastAddConfirmed", ctx, mock.Anything).
		Return(&proto.UpdateLastAddConfirmedResponse{
			Status: werr.Status(werr.ErrSegmentNotFound),
		}, nil)

	err := client.UpdateLastAddConfirmed(ctx, "bucket", "root", 1, 0, 42)
	assert.Error(t, err)
}

func TestRemoteClient_SelectNodes_Success(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	expectedNodes := []*proto.NodeMeta{{NodeId: "node1"}, {NodeId: "node2"}}
	mockClient.On("SelectNodes", ctx, mock.AnythingOfType("*proto.SelectNodesRequest")).
		Return(&proto.SelectNodesResponse{Nodes: expectedNodes}, nil)

	nodes, err := client.SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, nil)
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)
}

func TestRemoteClient_SelectNodes_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("SelectNodes", ctx, mock.Anything).
		Return(nil, fmt.Errorf("select error"))

	nodes, err := client.SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, nil)
	assert.Error(t, err)
	assert.Nil(t, nodes)
}

func TestRemoteClient_SelectNodes_StatusError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	mockClient.On("SelectNodes", ctx, mock.Anything).
		Return(&proto.SelectNodesResponse{
			Status: werr.Status(werr.ErrUnknownError),
		}, nil)

	nodes, err := client.SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, nil)
	assert.Error(t, err)
	assert.Nil(t, nodes)
}

func TestRemoteClient_AppendEntry_GrpcError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch")

	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("stream error"))

	_, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream error")
}

func TestRemoteClient_AppendEntry_RecvError(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch")

	// Stream returns error on Recv
	stream := &mockAddEntryStream{responses: nil}
	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(stream, nil)

	_, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err) // io.EOF from empty stream
}

func TestRemoteClient_AppendEntry_Synced(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch")

	stream := &mockAddEntryStream{
		responses: []*proto.AddEntryResponse{
			{State: proto.AddEntryState_Synced, EntryId: 10},
		},
	}
	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(stream, nil)

	entryId, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), entryId)
}

func TestRemoteClient_AppendEntry_Buffered_WrongChannelType(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	// Use a local result channel which is NOT a RemoteResultChannel
	resultCh := channel.NewLocalResultChannel("ch")

	stream := &mockAddEntryStream{
		responses: []*proto.AddEntryResponse{
			{State: proto.AddEntryState_Buffered, EntryId: 5},
		},
	}
	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(stream, nil)

	_, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err)
	assert.True(t, werr.ErrInternalError.Is(err))
}

func TestRemoteClient_AppendEntry_Failed_WithStatus(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch")

	stream := &mockAddEntryStream{
		responses: []*proto.AddEntryResponse{
			{State: proto.AddEntryState_Failed, EntryId: 0, Status: werr.Status(werr.ErrSegmentFenced)},
		},
	}
	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(stream, nil)

	_, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err)
}

func TestRemoteClient_AppendEntry_Failed_NilStatus(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch")

	stream := &mockAddEntryStream{
		responses: []*proto.AddEntryResponse{
			{State: proto.AddEntryState_Failed, EntryId: 0, Status: nil},
		},
	}
	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(stream, nil)

	_, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err)
	assert.True(t, werr.ErrUnknownError.Is(err))
}

func TestRemoteClient_AppendEntry_AfterClose(t *testing.T) {
	client, _ := newRemoteClientWithMock(t)
	ctx := context.Background()

	_ = client.Close(ctx)

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewLocalResultChannel("ch")

	_, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client is closed")
}

func TestRemoteClient_AppendEntry_Buffered_WithRemoteChannel(t *testing.T) {
	client, mockClient := newRemoteClientWithMock(t)
	ctx := context.Background()

	entry := &proto.LogEntry{SegId: 1, EntryId: 0, Values: []byte("data")}
	resultCh := channel.NewRemoteResultChannel("remote-ch")

	stream := &mockAddEntryStream{
		responses: []*proto.AddEntryResponse{
			{State: proto.AddEntryState_Buffered, EntryId: 5},
		},
	}
	mockClient.On("AddEntry", mock.Anything, mock.Anything).
		Return(stream, nil)

	entryId, err := client.AppendEntry(ctx, "bucket", "root", 1, entry, resultCh)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), entryId)
}
