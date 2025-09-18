// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

type Server struct {
	serverNode   *membership.ServerNode
	serverConfig *membership.ServerConfig // Configuration to be used for creating server node
	seeds        []string                 // Seeds for cluster joining
	logStore     LogStore
	grpcWG       sync.WaitGroup
	grpcErrChan  chan error
	grpcServer   *grpc.Server
	listener     net.Listener

	ctx    context.Context
	cancel context.CancelFunc
}

// NewServer creates a new server instance with same bind/advertise ip/port
func NewServer(ctx context.Context, configuration *config.Configuration, bindPort int, servicePort int, seeds []string) *Server {
	return NewServerWithConfig(ctx, configuration, &membership.ServerConfig{
		NodeID:               "", // Will be set in Prepare()
		BindPort:             bindPort,
		ServicePort:          servicePort,
		AdvertisePort:        bindPort,    // Use same port for gossip advertise
		AdvertiseServicePort: servicePort, // Use same port for service advertise
		ResourceGroup:        "default",   // Default resource group
		AZ:                   "default",   // Default availability zone
		Tags:                 map[string]string{"role": "logstore"},
	}, seeds)
}

// NewServerWithConfig creates a new server instance with custom configuration
func NewServerWithConfig(ctx context.Context, configuration *config.Configuration, serverConfig *membership.ServerConfig, seeds []string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	var minioCli minio.MinioHandler
	var err error
	if configuration.Woodpecker.Storage.IsStorageMinio() || configuration.Woodpecker.Storage.IsStorageService() {
		minioCli, err = minio.NewMinioHandler(ctx, configuration)
		if err != nil {
			panic(err)
		}
	}
	s := &Server{
		ctx:         ctx,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.logStore = NewLogStore(ctx, configuration, minioCli)

	// Store the server config and seeds for later use in Prepare()
	s.serverConfig = serverConfig
	s.seeds = seeds
	return s
}

func (s *Server) Prepare() error {
	// start listener for business service
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.serverConfig.ServicePort))
	if err != nil {
		return err
	}
	s.listener = l
	s.logStore.SetAddress(s.listener.Addr().String())

	// Set the NodeID from the listener address if not already set
	if s.serverConfig.NodeID == "" {
		s.serverConfig.NodeID = l.Addr().String()
	}

	// Create server node directly using the stored config
	node, err := membership.NewServerNode(s.serverConfig)
	if err != nil {
		return err
	}
	s.serverNode = node

	// Start async join if seeds are provided
	if len(s.seeds) > 0 {
		go asyncJoinSeeds(context.TODO(), node, s.seeds, s.serverConfig.NodeID, s.serverNode.GetMeta().Endpoint)
	}

	return nil
}

func (s *Server) Run() error {
	// init server
	if err := s.init(); err != nil {
		return err
	}
	// start server
	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	// start grpc server
	s.grpcWG.Add(1)
	go s.startGrpcLoop()
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

// start grpc server loop
func (s *Server) startGrpcLoop() {
	defer s.grpcWG.Done()
	_, cancel := context.WithCancel(s.ctx)
	defer cancel()
	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()),
	}
	s.grpcServer = grpc.NewServer(grpcOpts...)
	proto.RegisterLogStoreServer(s.grpcServer, s)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) start() error {
	// start log store
	if err := s.logStore.Start(); err != nil {
		return err
	}
	logger.Ctx(s.ctx).Info("log store started", zap.String("node", s.serverNode.GetMemberlist().LocalNode().Name), zap.String("address", s.logStore.GetAddress()))
	return nil
}

func (s *Server) Stop() error {
	leaveErr := s.serverNode.Leave()
	if leaveErr != nil {
		logger.Ctx(s.ctx).Error("server node leave failed", zap.Error(leaveErr))
	}
	stopErr := s.logStore.Stop()
	if stopErr != nil {
		logger.Ctx(s.ctx).Error("log store stop failed", zap.Error(stopErr))
	}
	s.grpcServer.GracefulStop()
	s.cancel()
	logger.Ctx(s.ctx).Info("server stopped", zap.String("node", s.serverNode.GetMemberlist().LocalNode().Name), zap.String("address", s.logStore.GetAddress()))
	return nil
}

func (s *Server) AddEntry(request *proto.AddEntryRequest, serverStream grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
	// Convert proto.LogEntry -> internal proto.LogEntry (same struct in this repo)
	entry := &proto.LogEntry{
		SegId:   request.Entry.SegId,
		EntryId: request.Entry.EntryId,
		Values:  request.Entry.Values,
	}

	// Use stream context for consistency
	streamCtx := serverStream.Context()
	resultCh := channel.NewLocalResultChannel(fmt.Sprintf("srv/%d/%d/%d", request.LogId, request.Entry.SegId, request.Entry.EntryId))
	bufferedId, err := s.logStore.AddEntry(streamCtx, request.LogId, entry, resultCh)
	if err != nil {
		// entry add to buffer failed - send error response and close stream
		sendErr := serverStream.Send(&proto.AddEntryResponse{
			State:   proto.AddEntryState_Failed,
			EntryId: bufferedId,
			Status:  werr.Status(err)},
		)
		if sendErr != nil {
			logger.Ctx(streamCtx).Warn("failed to send error response", zap.Error(sendErr))
		}
		return err
	}

	// entry buffered - send buffered response immediately
	sendErr := serverStream.Send(&proto.AddEntryResponse{
		State:   proto.AddEntryState_Buffered,
		EntryId: bufferedId,
		Status:  werr.Success()},
	)
	if sendErr != nil {
		logger.Ctx(streamCtx).Warn("failed to send buffered response", zap.Error(sendErr))
		return sendErr
	}
	// wait for entry to be synced
	result, err := resultCh.ReadResult(streamCtx)
	if err != nil {
		id := int64(-1)
		if result != nil {
			id = result.SyncedId
		}
		sendErr = serverStream.Send(&proto.AddEntryResponse{
			State:   proto.AddEntryState_Failed,
			EntryId: id,
			Status:  werr.Status(err)},
		)
		return sendErr
	}
	// persist added entry success
	sendErr = serverStream.Send(&proto.AddEntryResponse{
		State:   proto.AddEntryState_Synced,
		EntryId: result.SyncedId,
		Status:  werr.Success()},
	)
	return sendErr // Return nil for normal closure, not the context error
}

func (s *Server) GetBatchEntriesAdv(ctx context.Context, request *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error) {
	result, err := s.logStore.GetBatchEntriesAdv(ctx, request.LogId, request.SegmentId, request.FromEntryId, request.MaxEntries, request.LastReadState)
	if err != nil {
		return &proto.GetBatchEntriesAdvResponse{
			Status: werr.Status(err),
		}, nil
	}
	return &proto.GetBatchEntriesAdvResponse{Status: werr.Success(), Result: result}, nil
}

func (s *Server) FenceSegment(ctx context.Context, request *proto.FenceSegmentRequest) (*proto.FenceSegmentResponse, error) {
	lastId, err := s.logStore.FenceSegment(ctx, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.FenceSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.FenceSegmentResponse{Status: werr.Success(), LastEntryId: lastId}, nil
}

func (s *Server) CompleteSegment(ctx context.Context, request *proto.CompleteSegmentRequest) (*proto.CompleteSegmentResponse, error) {
	lastId, err := s.logStore.CompleteSegment(ctx, request.LogId, request.SegmentId, request.LastAddConfirmed)
	if err != nil {
		return &proto.CompleteSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.CompleteSegmentResponse{Status: werr.Success(), LastEntryId: lastId}, nil
}

func (s *Server) CompactSegment(ctx context.Context, request *proto.CompactSegmentRequest) (*proto.CompactSegmentResponse, error) {
	meta, err := s.logStore.CompactSegment(ctx, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.CompactSegmentResponse{Status: werr.Status(err)}, nil
	}
	// meta 已经是 *proto.SegmentMetadata
	return &proto.CompactSegmentResponse{Status: werr.Success(), Metadata: meta}, nil
}

func (s *Server) GetSegmentLastAddConfirmed(ctx context.Context, request *proto.GetSegmentLastAddConfirmedRequest) (*proto.GetSegmentLastAddConfirmedResponse, error) {
	lac, err := s.logStore.GetSegmentLastAddConfirmed(ctx, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.GetSegmentLastAddConfirmedResponse{Status: werr.Status(err)}, nil
	}
	return &proto.GetSegmentLastAddConfirmedResponse{Status: werr.Success(), LastEntryId: lac}, nil
}

func (s *Server) GetSegmentBlockCount(ctx context.Context, request *proto.GetSegmentBlockCountRequest) (*proto.GetSegmentBlockCountResponse, error) {
	count, err := s.logStore.GetSegmentBlockCount(ctx, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.GetSegmentBlockCountResponse{Status: werr.Status(err)}, nil
	}
	return &proto.GetSegmentBlockCountResponse{Status: werr.Success(), BlockCount: count}, nil
}

func (s *Server) CleanSegment(ctx context.Context, request *proto.CleanSegmentRequest) (*proto.CleanSegmentResponse, error) {
	if err := s.logStore.CleanSegment(ctx, request.LogId, request.SegmentId, int(request.Flag)); err != nil {
		return &proto.CleanSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.CleanSegmentResponse{Status: werr.Success()}, nil
}

func (s *Server) UpdateLastAddConfirmed(ctx context.Context, request *proto.UpdateLastAddConfirmedRequest) (*proto.UpdateLastAddConfirmedResponse, error) {
	if err := s.logStore.UpdateLastAddConfirmed(ctx, request.LogId, request.SegmentId, request.LastAddConfirmed); err != nil {
		return &proto.UpdateLastAddConfirmedResponse{Status: werr.Status(err)}, nil
	}
	return &proto.UpdateLastAddConfirmedResponse{Status: werr.Success()}, nil
}

func (s *Server) SelectNodes(ctx context.Context, request *proto.SelectNodesRequest) (*proto.SelectNodesResponse, error) {
	discovery := s.serverNode.GetDiscovery()

	// Use the new protobuf-based approach
	var allServers []*proto.NodeMeta

	// If no filters provided, create a default filter
	filters := request.Filters
	if len(filters) == 0 {
		return &proto.SelectNodesResponse{Status: werr.Status(werr.ErrServiceNoFilterFound.WithCauseErrMsg("no filters provided"))}, nil
	}

	// Process each filter and collect results
	for _, filter := range filters {
		var servers []*proto.NodeMeta
		var err error

		// Select nodes using the specified strategy
		switch request.Strategy {
		case proto.StrategyType_RANDOM:
			servers, err = discovery.SelectRandom(filter, request.AffinityMode)
		case proto.StrategyType_SINGLE_AZ_SINGLE_RG:
			servers, err = discovery.SelectSingleAzSingleRg(filter, request.AffinityMode)
		case proto.StrategyType_SINGLE_AZ_MULTI_RG:
			servers, err = discovery.SelectSingleAzMultiRg(filter, request.AffinityMode)
		case proto.StrategyType_MULTI_AZ_SINGLE_RG:
			servers, err = discovery.SelectMultiAzSingleRg(filter, request.AffinityMode)
		case proto.StrategyType_MULTI_AZ_MULTI_RG:
			servers, err = discovery.SelectMultiAzMultiRg(filter, request.AffinityMode)
		case proto.StrategyType_CUSTOM:
			servers, err = discovery.SelectCustom(filter, request.AffinityMode)
		default:
			// Default to random
			servers, err = discovery.SelectRandom(filter, request.AffinityMode)
		}

		if err != nil {
			if request.AffinityMode == proto.AffinityMode_HARD {
				return &proto.SelectNodesResponse{Status: werr.Status(err)}, nil
			}
			// In soft mode, continue with other filters
			continue
		}

		allServers = append(allServers, servers...)
	}

	return &proto.SelectNodesResponse{
		Status:     werr.Success(),
		Nodes:      allServers,
		TotalCount: int32(len(allServers)),
	}, nil
}

// GetServiceAdvertiseAddrPort use for test only
func (s *Server) GetServiceAdvertiseAddrPort(ctx context.Context) string {
	// Get the actual service endpoint from the node metadata (which contains the resolved address)
	return s.serverNode.GetMeta().Endpoint
}

// GetAdvertiseAddrPort Use for test only
func (s *Server) GetAdvertiseAddrPort(ctx context.Context) string {
	// Get the actual gossip advertise address from the memberlist configuration
	ml := s.serverNode.GetMemberlist()
	config := ml.LocalNode()
	// Use the actual advertise address from memberlist, fall back to bind address if not set
	addr := config.Addr
	port := config.Port
	return fmt.Sprintf("%s:%d", addr, port)
}

// asyncJoinSeeds attempts to join seed nodes with retry mechanism in its own method [[memory:3527742]]
func asyncJoinSeeds(ctx context.Context, node *membership.ServerNode, seeds []string, name, adv string) {
	// TODO panic or always retry if join failed?
	maxRetries := 30                     // Maximum number of retry attempts
	retryInterval := 2 * time.Second     // Initial retry interval
	maxRetryInterval := 30 * time.Second // Maximum retry interval

	logger.Ctx(ctx).Info("Starting async seed joining",
		zap.String("node", name),
		zap.String("advertise", adv),
		zap.Strings("seeds", seeds))

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Async seed joining cancelled due to context cancellation",
				zap.String("node", name))
			return
		default:
		}

		err := node.Join(seeds)
		if err == nil {
			logger.Ctx(ctx).Info("Successfully joined cluster",
				zap.String("node", name),
				zap.String("advertise", adv),
				zap.Strings("seeds", seeds),
				zap.Int("attempt", attempt))
			return
		}

		logger.Ctx(ctx).Warn("Failed to join cluster, will retry",
			zap.Error(err),
			zap.String("node", name),
			zap.String("advertise", adv),
			zap.Strings("seeds", seeds),
			zap.Int("attempt", attempt),
			zap.Int("maxRetries", maxRetries),
			zap.Duration("nextRetry", retryInterval))

		// Wait before next retry with exponential backoff
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Async seed joining cancelled during retry wait",
				zap.String("node", name))
			return
		case <-time.After(retryInterval):
		}

		// Exponential backoff, capped at maxRetryInterval
		retryInterval = retryInterval * 2
		if retryInterval > maxRetryInterval {
			retryInterval = maxRetryInterval
		}
	}

	logger.Ctx(ctx).Error("Failed to join cluster after maximum retries",
		zap.String("node", name),
		zap.String("advertise", adv),
		zap.Strings("seeds", seeds),
		zap.Int("maxRetries", maxRetries))
}
