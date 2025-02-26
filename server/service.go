package server

import (
	"context"
	"net"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/proto"
)

type Server struct {
	logStore    LogStore
	grpcWG      sync.WaitGroup
	grpcErrChan chan error
	grpcServer  *grpc.Server
	listener    net.Listener
	ctx         context.Context
	cancel      context.CancelFunc
	etcdCli     *clientv3.Client
}

func NewServer(ctx context.Context, configuration *config.Configuration) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetRemoteEtcdClient(configuration.Etcd.GetEndpoints())
	if err != nil {
		panic(err)
	}
	minioCli, err := minio.NewMinioHandler(ctx, configuration)
	if err != nil {
		panic(err)
	}
	s := &Server{
		ctx:         ctx,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	s.logStore = NewLogStore(ctx, configuration, etcdCli, minioCli)
	return s
}

func (s *Server) Prepare() error {
	l, err := net.Listen("tcp", "0.0.0.0:52160") // TODO
	if err != nil {
		return err
	}
	s.listener = l
	return nil
}

func (s *Server) Run() error {
	if err := s.init(); err != nil {
		return err
	}
	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	etcdCli, err := clientv3.New(clientv3.Config{})
	if err != nil {
		return err
	}
	s.etcdCli = etcdCli
	s.logStore.SetEtcdClient(etcdCli)
	s.logStore.SetAddress(s.listener.Addr().String())
	err = s.startGrpc()
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpc() error {
	s.grpcWG.Add(1)
	go s.startGrpcLoop()
	// wait for grpc server loop start
	err := <-s.grpcErrChan
	return err
}

func (s *Server) startGrpcLoop() {
	defer s.grpcWG.Done()
	_, cancel := context.WithCancel(s.ctx)
	defer cancel()
	grpcOpts := []grpc.ServerOption{}
	s.grpcServer = grpc.NewServer(grpcOpts...)
	proto.RegisterLogStoreServer(s.grpcServer, s)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		s.grpcErrChan <- err
	}
}

func (s *Server) setEtcdClient(client *clientv3.Client) {
	s.logStore.SetEtcdClient(client)
}

func (s *Server) start() error {
	if err := s.logStore.Start(); err != nil {
		return err
	}
	err := s.logStore.Register(s.ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	return nil
}

// TODO should use unary-stream or stream-stream to impl async add request
func (s *Server) AddEntry(ctx context.Context, request *proto.AddEntryRequest) (*proto.AddEntryResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Server) ReadEntry(ctx context.Context, request *proto.ReadEntryRequest) (*proto.ReadEntryResponse, error) {
	//TODO implement me
	panic("implement me")
}
