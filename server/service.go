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
	serverNode *membership.ServerNode
	bindPort   int // bind port for gossip
	seeds      []string

	// Advertise configuration for Docker bridge networking
	advertiseAddr       string
	advertiseGrpcPort   int
	advertiseGossipPort int

	logStore    LogStore
	servicePort int // service port for business
	grpcWG      sync.WaitGroup
	grpcErrChan chan error
	grpcServer  *grpc.Server
	listener    net.Listener

	ctx    context.Context
	cancel context.CancelFunc
}

// Config holds server configuration including advertise options
type Config struct {
	BindPort            int
	ServicePort         int
	AdvertiseAddr       string
	AdvertiseGrpcPort   int
	AdvertiseGossipPort int
	SeedNodes           []string
}

// NewServer creates a new server instance with same bind/advertise ip/port
func NewServer(ctx context.Context, configuration *config.Configuration, bindPort int, servicePort int, seeds []string) *Server {
	return NewServerWithConfig(ctx, configuration, &Config{
		BindPort:    bindPort,
		ServicePort: servicePort,
		SeedNodes:   seeds,
	})
}

// NewServerWithConfig creates a new server instance with custom configuration
func NewServerWithConfig(ctx context.Context, configuration *config.Configuration, serverConfig *Config) *Server {
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
		bindPort:    serverConfig.BindPort,
		seeds:       serverConfig.SeedNodes,
		servicePort: serverConfig.ServicePort,

		// advertise addr
		advertiseAddr:       serverConfig.AdvertiseAddr,
		advertiseGrpcPort:   serverConfig.AdvertiseGrpcPort,
		advertiseGossipPort: serverConfig.AdvertiseGossipPort,
	}
	s.logStore = NewLogStore(ctx, configuration, minioCli)
	return s
}

func (s *Server) Prepare() error {
	// start listener for business service
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.servicePort))
	if err != nil {
		return err
	}
	s.listener = l
	s.logStore.SetAddress(s.listener.Addr().String())

	// start server node for gossip
	node, _, err := startMembershipServerNode(context.TODO(), l.Addr().String(), "default", "default", s.bindPort, s.servicePort, s.seeds, s.advertiseAddr, s.advertiseGossipPort, s.advertiseGrpcPort)
	if err != nil {
		return err
	}
	s.serverNode = node
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

func (s *Server) GetServiceAddr(ctx context.Context) string {
	return fmt.Sprintf("%s:%d", s.listener.Addr().String(), s.servicePort)
}

func (s *Server) GetAdvertiseAddr(ctx context.Context) string {
	return fmt.Sprintf("%s:%d", s.serverNode.GetMemberlist().LocalNode().Addr.String(), int(s.serverNode.GetMemberlist().LocalNode().Port))
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

func startMembershipServerNode(ctx context.Context, name, rg, az string, bindPort int, servicePort int, seeds []string, advertiseAddr string, advertiseGossipPort, advertiseGrpcPort int) (*membership.ServerNode, string, error) {
	cfg := &membership.ServerConfig{
		NodeID:              name,
		ResourceGroup:       rg,
		AZ:                  az,
		BindAddr:            "0.0.0.0",
		BindPort:            bindPort,
		ServicePort:         servicePort,
		AdvertiseAddr:       advertiseAddr,
		AdvertiseGossipPort: advertiseGossipPort,
		AdvertiseGrpcPort:   advertiseGrpcPort,
		Tags:                map[string]string{"role": "demo"},
	}
	n, err := membership.NewServerNode(cfg)
	if err != nil {
		logger.Ctx(ctx).Error("create server failed", zap.Error(err))
		return nil, "", err
	}

	// Return advertise address (ip:port)
	adv := fmt.Sprintf("%s:%d", n.GetMemberlist().LocalNode().Addr.String(), int(n.GetMemberlist().LocalNode().Port))
	logger.Ctx(ctx).Info("NODE_READY", zap.String("name", name), zap.String("advertise", adv))

	if len(seeds) > 0 {
		if joinErr := n.Join(seeds); joinErr != nil {
			logger.Ctx(ctx).Error("join failed", zap.String("name", name), zap.String("advertise", adv), zap.Error(joinErr))
			return nil, "", joinErr
		}
	}
	return n, adv, nil
}
