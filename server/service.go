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
	"github.com/zilliztech/woodpecker/common/funcutil"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/membership"
	wpNet "github.com/zilliztech/woodpecker/common/net"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

type Server struct {
	cfg          *config.Configuration
	serverNode   *membership.ServerNode
	serverConfig *membership.ServerConfig // Configuration to be used for creating server node
	gossipSeeds  []string                 // Seeds for cluster joining
	logStore     LogStore
	grpcWG       sync.WaitGroup
	gossipWG     sync.WaitGroup // tracks asyncStartAndJoinSeeds goroutine
	grpcErrChan  chan error
	startupErrCh chan error // Channel to propagate async startup errors
	grpcServer   *grpc.Server
	listener     net.Listener

	ctx    context.Context
	cancel context.CancelFunc
}

// NewServer creates a new server instance with same bind/advertise ip/port
func NewServer(ctx context.Context, configuration *config.Configuration, bindPort int, servicePort int, gossipSeeds []string) *Server {
	return NewServerWithConfig(ctx, configuration, &membership.ServerConfig{
		NodeID:               "", // Will be set in Prepare()
		BindPort:             bindPort,
		ServicePort:          servicePort,
		AdvertisePort:        bindPort,    // Use same port for gossip advertise
		AdvertiseServicePort: servicePort, // Use same port for service advertise
		ResourceGroup:        "default",   // Default resource group
		AZ:                   "default",   // Default availability zone
		Tags:                 map[string]string{"role": "logstore"},
	}, gossipSeeds)
}

// NewServerWithConfig creates a new server instance with custom configuration
func NewServerWithConfig(ctx context.Context, configuration *config.Configuration, serverConfig *membership.ServerConfig, gossipSeeds []string) *Server {
	ctx, cancel := context.WithCancel(ctx)
	var storageCli storageclient.ObjectStorage
	if configuration.Woodpecker.Storage.IsStorageMinio() || configuration.Woodpecker.Storage.IsStorageService() {
		var err error
		storageCli, err = storageclient.NewObjectStorage(ctx, configuration)
		if err != nil {
			panic(err)
		}
	}
	s := &Server{
		cfg:          configuration,
		ctx:          ctx,
		cancel:       cancel,
		grpcErrChan:  make(chan error),
		startupErrCh: make(chan error, 1), // Buffered channel to avoid blocking
	}
	s.logStore = NewLogStore(ctx, configuration, storageCli)
	// Store the server config and seeds for later use in Prepare()
	s.serverConfig = serverConfig
	s.gossipSeeds = gossipSeeds
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

	// Start async join if seeds are provided
	if len(s.gossipSeeds) > 0 {
		s.gossipWG.Add(1)
		go func() {
			defer s.gossipWG.Done()
			s.asyncStartAndJoinSeeds(s.ctx, s.gossipSeeds)
		}()
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
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.cfg.Woodpecker.Logstore.GRPCConfig.GetServerMaxRecvSize()),
		grpc.MaxSendMsgSize(s.cfg.Woodpecker.Logstore.GRPCConfig.GetServerMaxSendSize()),
		grpc.ChainUnaryInterceptor(
			s.shutdownUnaryInterceptor(),
			otelgrpc.UnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			s.shutdownStreamInterceptor(),
			otelgrpc.StreamServerInterceptor(),
		),
	}
	s.grpcServer = grpc.NewServer(grpcOpts...)
	proto.RegisterLogStoreServer(s.grpcServer, s)
	funcutil.CheckGrpcReady(s.ctx, s.grpcErrChan)
	logger.Ctx(s.ctx).Info("start grpc server", zap.String("nodeID", s.serverConfig.NodeID), zap.String("address", s.listener.Addr().String()))
	if err := s.grpcServer.Serve(s.listener); err != nil {
		logger.Ctx(s.ctx).Error("grpc server failed", zap.Error(err))
		// Non-blocking send: during shutdown, init() has already consumed the
		// startup signal so nobody is reading grpcErrChan. A blocking send here
		// would prevent grpcWG.Done() from running, causing Stop() to hang.
		select {
		case s.grpcErrChan <- err:
		default:
		}
	}
	logger.Ctx(s.ctx).Info("grpc server stopped", zap.String("nodeID", s.serverConfig.NodeID), zap.String("address", s.logStore.GetAddress()))
}

func (s *Server) start() error {
	// start log store
	if err := s.logStore.Start(); err != nil {
		return err
	}
	logger.Ctx(s.ctx).Info("log store started", zap.String("nodeID", s.serverConfig.NodeID), zap.String("address", s.logStore.GetAddress()))
	return nil
}

func (s *Server) Stop() error {
	// 1. Stop accepting new connections by closing the listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			logger.Ctx(s.ctx).Warn("failed to close listener", zap.Error(err))
		}
	}

	// 2. Leave and shutdown the gossip cluster
	if s.serverNode != nil {
		leaveErr := s.serverNode.Leave()
		if leaveErr != nil {
			logger.Ctx(s.ctx).Error("server node leave failed", zap.Error(leaveErr))
		}
		shutdownErr := s.serverNode.Shutdown()
		if shutdownErr != nil {
			logger.Ctx(s.ctx).Error("server node shutdown failed", zap.Error(shutdownErr))
		}
	}

	// 3. GracefulStop with timeout — prevents deadlock when in-flight requests
	//    are blocked on resultCh that will never be notified after logStore.Stop()
	if s.grpcServer != nil {
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()
		t := time.NewTimer(shutdownGracePeriod)
		defer t.Stop()
		select {
		case <-stopped:
			logger.Ctx(s.ctx).Info("gRPC server gracefully stopped")
		case <-t.C:
			logger.Ctx(s.ctx).Warn("gRPC graceful stop timed out, forcing stop")
			s.grpcServer.Stop()
			<-stopped
		}
	}

	// 4. Wait for gRPC loop goroutine to finish
	s.grpcWG.Wait()

	// 5. Stop LogStore AFTER gRPC handlers have drained — avoids killing ack
	//    goroutines while AddEntry handlers are still waiting on resultCh
	if s.logStore != nil {
		if stopErr := s.logStore.Stop(); stopErr != nil {
			logger.Ctx(s.ctx).Error("log store stop failed", zap.Error(stopErr))
		}
	}

	// 6. Cancel server context
	s.cancel()

	// 7. Wait for gossip goroutine to finish
	s.gossipWG.Wait()

	logger.Ctx(s.ctx).Info("server stopped", zap.String("nodeID", s.serverConfig.NodeID), zap.String("address", s.logStore.GetAddress()))
	return nil
}

const shutdownGracePeriod = 10 * time.Second

// wrappedServerStream wraps a grpc.ServerStream with a custom context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

// shutdownUnaryInterceptor returns a gRPC unary interceptor that cancels handler
// contexts when the server context is cancelled (during shutdown).
func (s *Server) shutdownUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(s.ctx, func() { cancel() })
		defer stop()
		defer cancel()
		return handler(ctx, req)
	}
}

// shutdownStreamInterceptor returns a gRPC stream interceptor that cancels handler
// contexts when the server context is cancelled (during shutdown).
func (s *Server) shutdownStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, cancel := context.WithCancel(ss.Context())
		stop := context.AfterFunc(s.ctx, func() { cancel() })
		defer stop()
		defer cancel()
		return handler(srv, &wrappedServerStream{ServerStream: ss, ctx: ctx})
	}
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
	bufferedId, err := s.logStore.AddEntry(streamCtx, request.BucketName, request.RootPath, request.LogId, entry, resultCh)
	if err != nil {
		// entry add to buffer failed - send error response and close stream
		sendErr := serverStream.Send(&proto.AddEntryResponse{
			State:   proto.AddEntryState_Failed,
			EntryId: bufferedId,
			Status:  werr.Status(err),
		},
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
		Status:  werr.Success(),
	},
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
			Status:  werr.Status(err),
		},
		)
		return sendErr
	}
	// persist added entry success
	sendErr = serverStream.Send(&proto.AddEntryResponse{
		State:   proto.AddEntryState_Synced,
		EntryId: result.SyncedId,
		Status:  werr.Success(),
	},
	)
	return sendErr // Return nil for normal closure, not the context error
}

func (s *Server) GetBatchEntriesAdv(ctx context.Context, request *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error) {
	result, err := s.logStore.GetBatchEntriesAdv(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId, request.FromEntryId, request.MaxEntries, request.LastReadState)
	if err != nil {
		return &proto.GetBatchEntriesAdvResponse{
			Status: werr.Status(err),
		}, nil
	}
	return &proto.GetBatchEntriesAdvResponse{Status: werr.Success(), Result: result}, nil
}

func (s *Server) FenceSegment(ctx context.Context, request *proto.FenceSegmentRequest) (*proto.FenceSegmentResponse, error) {
	lastId, err := s.logStore.FenceSegment(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.FenceSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.FenceSegmentResponse{Status: werr.Success(), LastEntryId: lastId}, nil
}

func (s *Server) CompleteSegment(ctx context.Context, request *proto.CompleteSegmentRequest) (*proto.CompleteSegmentResponse, error) {
	lastId, err := s.logStore.CompleteSegment(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId, request.LastAddConfirmed)
	if err != nil {
		return &proto.CompleteSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.CompleteSegmentResponse{Status: werr.Success(), LastEntryId: lastId}, nil
}

func (s *Server) CompactSegment(ctx context.Context, request *proto.CompactSegmentRequest) (*proto.CompactSegmentResponse, error) {
	meta, err := s.logStore.CompactSegment(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.CompactSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.CompactSegmentResponse{Status: werr.Success(), Metadata: meta}, nil
}

func (s *Server) GetSegmentLastAddConfirmed(ctx context.Context, request *proto.GetSegmentLastAddConfirmedRequest) (*proto.GetSegmentLastAddConfirmedResponse, error) {
	lac, err := s.logStore.GetSegmentLastAddConfirmed(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.GetSegmentLastAddConfirmedResponse{Status: werr.Status(err)}, nil
	}
	return &proto.GetSegmentLastAddConfirmedResponse{Status: werr.Success(), LastEntryId: lac}, nil
}

func (s *Server) GetSegmentBlockCount(ctx context.Context, request *proto.GetSegmentBlockCountRequest) (*proto.GetSegmentBlockCountResponse, error) {
	count, err := s.logStore.GetSegmentBlockCount(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId)
	if err != nil {
		return &proto.GetSegmentBlockCountResponse{Status: werr.Status(err)}, nil
	}
	return &proto.GetSegmentBlockCountResponse{Status: werr.Success(), BlockCount: count}, nil
}

func (s *Server) CleanSegment(ctx context.Context, request *proto.CleanSegmentRequest) (*proto.CleanSegmentResponse, error) {
	if err := s.logStore.CleanSegment(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId, int(request.Flag)); err != nil {
		return &proto.CleanSegmentResponse{Status: werr.Status(err)}, nil
	}
	return &proto.CleanSegmentResponse{Status: werr.Success()}, nil
}

func (s *Server) UpdateLastAddConfirmed(ctx context.Context, request *proto.UpdateLastAddConfirmedRequest) (*proto.UpdateLastAddConfirmedResponse, error) {
	if err := s.logStore.UpdateLastAddConfirmed(ctx, request.BucketName, request.RootPath, request.LogId, request.SegmentId, request.LastAddConfirmed); err != nil {
		return &proto.UpdateLastAddConfirmedResponse{Status: werr.Status(err)}, nil
	}
	return &proto.UpdateLastAddConfirmedResponse{Status: werr.Success()}, nil
}

func (s *Server) SelectNodes(ctx context.Context, request *proto.SelectNodesRequest) (*proto.SelectNodesResponse, error) {
	if s.serverNode == nil {
		return &proto.SelectNodesResponse{Status: werr.Status(werr.ErrServiceInsufficientQuorum.WithCauseErrMsg("node not ready yet"))}, nil
	}
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

// GetServerNodeMemberlistStatus returns the server's ServerNode memberlist status
func (s *Server) GetServerNodeMemberlistStatus() string {
	if s.serverNode != nil {
		return s.serverNode.GetMemberlistStatus()
	}
	return "member not ready yet"
}

// GetServiceAdvertiseAddrPort use for test only
func (s *Server) GetServiceAdvertiseAddrPort(ctx context.Context) string {
	if s.serverNode == nil {
		return ""
	}
	// Get the actual service endpoint from the node metadata (which contains the resolved address)
	return s.serverNode.GetMeta().Endpoint
}

// GetAdvertiseAddrPort Use for test only
func (s *Server) GetAdvertiseAddrPort(ctx context.Context) string {
	return fmt.Sprintf("%s:%d", s.serverConfig.AdvertiseAddr, s.serverConfig.AdvertisePort)
}

// GetStartupErrCh returns the channel for async startup errors.
// The main program can listen to this channel to detect if the async
// server node creation (gossip port binding) failed.
func (s *Server) GetStartupErrCh() <-chan error {
	return s.startupErrCh
}

// asyncJoinSeeds continuously monitors and joins missing seed nodes with adaptive backoff [[memory:3527742]]
// It maintains a list of seed nodes and periodically checks which ones are not in memberlist
func (s *Server) asyncStartAndJoinSeeds(ctx context.Context, seeds []string) {
	// 1. Create server node directly using the stored config
	if err := s.waitAndStartCurrentNode(ctx); err != nil {
		// Send error to startup error channel (non-blocking due to buffered channel)
		select {
		case s.startupErrCh <- err:
		default:
		}
		logger.Ctx(ctx).Error("Failed to start server node, aborting async join",
			zap.String("nodeID", s.serverConfig.NodeID),
			zap.Error(err))
		return
	}

	// 2. Join the cluster seeds nodes
	s.monitorAndJoinSeeds(ctx, seeds)
}

func (s *Server) waitAndStartCurrentNode(ctx context.Context) error {
	// 1. Create server node directly using the stored config
	var node *membership.ServerNode
	var err error
	currentNodeID := s.serverConfig.NodeID
	retryInterval := 1 * time.Second
	const maxAttempts = 30 // Maximum retry attempts (30 seconds total)

	// 1.1 wait for hostname resolvable
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for hostname resolution: %w", ctx.Err())
		default:
		}

		resolvedIP := wpNet.ResolveAdvertiseAddr(s.serverConfig.AdvertiseAddr)
		if resolvedIP != nil {
			break
		}

		if attempt == maxAttempts-1 {
			return fmt.Errorf("failed to resolve hostname '%s' after %d attempts", s.serverConfig.AdvertiseAddr, maxAttempts)
		}

		logger.Ctx(ctx).Info("Waiting for hostname resolvable",
			zap.String("nodeID", currentNodeID),
			zap.String("hostname", s.serverConfig.AdvertiseAddr),
			zap.Int("attempt", attempt+1),
			zap.Int("maxAttempts", maxAttempts))
		time.Sleep(retryInterval)
	}

	// 1.2 start currentNode
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while creating server node: %w", ctx.Err())
		default:
		}

		logger.Ctx(ctx).Info("Attempting to create server node",
			zap.String("currentNodeID", currentNodeID),
			zap.Int("attempt", attempt+1),
			zap.Int("maxAttempts", maxAttempts),
			zap.Int("gossipPort", s.serverConfig.BindPort))
		node, err = membership.NewServerNode(s.serverConfig)
		if err != nil {
			logger.Ctx(ctx).Warn("server node create failed",
				zap.String("currentNodeID", currentNodeID),
				zap.Int("attempt", attempt+1),
				zap.Int("maxAttempts", maxAttempts),
				zap.Error(err))

			if attempt == maxAttempts-1 {
				return fmt.Errorf("failed to create server node after %d attempts: %w", maxAttempts, err)
			}

			time.Sleep(retryInterval)
			continue
		}

		logger.Ctx(ctx).Info("server node create success",
			zap.String("currentNodeID", currentNodeID),
			zap.Int("attempt", attempt+1),
			zap.String("initMemberlist", node.GetMemberlistStatus()))
		s.serverNode = node
		return nil
	}

	return fmt.Errorf("unexpected: exceeded max attempts for server node creation")
}

func (s *Server) monitorAndJoinSeeds(ctx context.Context, seeds []string) {
	currentNodeID := s.serverConfig.NodeID
	node := s.serverNode
	const (
		minBackoff     = 500 * time.Millisecond // Fastest check interval when issues detected
		normalBackoff  = 5 * time.Second        // Normal check interval when all healthy
		maxBackoff     = 10 * time.Second       // Maximum check interval
		initialBackoff = 2 * time.Second        // Initial backoff before first check
	)
	backoff := initialBackoff
	logger.Ctx(ctx).Info("Starting async seed joining loop",
		zap.String("currentNodeID", currentNodeID),
		zap.Strings("seeds", seeds))

	// Build seed hostname map for quick lookup
	seedHostnames := make(map[string]string) // hostname -> full_address
	for _, seed := range seeds {
		host, _, err := net.SplitHostPort(seed)
		if err == nil {
			seedHostnames[host] = seed
		}
	}

	// TODO use seed node leave event trigger
	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Async seed joining loop stopped",
				zap.String("currentNodeID", currentNodeID))
			return
		case <-time.After(backoff):
		}

		// Get current memberlist members
		members := node.GetMemberlist().Members()

		// Build map of current member names (hostnames)
		currentMembers := make(map[string]bool)
		for _, m := range members {
			currentMembers[m.Name] = true
		}

		// Find missing seed nodes
		var missingSeedAddrs []string
		for hostname, addr := range seedHostnames {
			if !currentMembers[hostname] {
				missingSeedAddrs = append(missingSeedAddrs, addr)
			}
		}

		// If all seeds are present, use longer backoff (normal mode)
		if len(missingSeedAddrs) == 0 {
			if backoff < normalBackoff {
				backoff = normalBackoff
				logger.Ctx(ctx).Debug("All seeds present, switching to normal backoff",
					zap.String("currentNodeID", currentNodeID),
					zap.Duration("backoff", backoff))
			}
			continue
		}

		// Some seeds are missing, try to join them
		logger.Ctx(ctx).Debug("Detected missing seed nodes, attempting to join",
			zap.String("currentNodeID", currentNodeID),
			zap.Strings("missingSeeds", missingSeedAddrs),
			zap.Int("totalMembers", len(members)))

		// Try to join missing seeds
		// Memberlist handles hostname resolution internally
		err := node.Join(missingSeedAddrs)
		if err != nil {
			// Join failed, use shorter backoff for faster retry
			backoff = minBackoff
			logger.Ctx(ctx).Debug("Failed to join missing seeds, will retry soon",
				zap.String("currentNodeID", currentNodeID),
				zap.Strings("missingSeeds", missingSeedAddrs),
				zap.Error(err),
				zap.Duration("nextRetry", backoff))
		} else {
			// Join succeeded, gradually increase backoff
			if backoff < maxBackoff {
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			logger.Ctx(ctx).Info("Successfully joined missing seeds",
				zap.String("currentNodeID", currentNodeID),
				zap.Strings("joinedSeeds", missingSeedAddrs),
				zap.Int("newMemberCount", len(node.GetMemberlist().Members())),
				zap.Duration("nextCheck", backoff))
		}
	}
}
