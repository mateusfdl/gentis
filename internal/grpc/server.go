package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Server struct {
	gentisv1.UnimplementedGentisServiceServer

	config   *Config
	listener net.Listener
	grpcSrv  *grpc.Server
	engine   engine.Engine
	store    *transport.SessionStore
	sessions sync.Map
	nextID   atomic.Int32

	metrics             *metrics.Server
	connectionCount     atomic.Int64
	connectionsTotal    atomic.Int64
	disconnectionsTotal atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(address string, opts ...Option) *Server {
	cfg := defaultConfig(address)
	for _, opt := range opts {
		opt(cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())

	eng := cfg.Engine
	if eng == nil {
		eng = engine.New()
	}

	return &Server{
		config: cfg,
		engine: eng,
		store:  cfg.SessionStore,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) ConnectionCount() int64 {
	return s.connectionCount.Load()
}

func (s *Server) ConnectionsTotal() int64 {
	return s.connectionsTotal.Load()
}

func (s *Server) DisconnectionsTotal() int64 {
	return s.disconnectionsTotal.Load()
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}

	s.listener = listener
	s.grpcSrv = grpc.NewServer()
	gentisv1.RegisterGentisServiceServer(s.grpcSrv, s)

	if s.config.MetricsEnabled {
		collector := metrics.NewCollector(s.engine, s, "server")
		if s.config.Observer != nil {
			collector.SetObserver(s.config.Observer)
		}
		s.metrics = metrics.NewServer(s.config.MetricsAddr, collector)
		if err := s.metrics.Start(); err != nil {
			return fmt.Errorf("failed to start metrics server: %w", err)
		}
		log.Printf("Metrics server listening on %s", s.config.MetricsAddr)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.grpcSrv.Serve(listener)
	}()

	return nil
}

func (s *Server) Stop() error {
	s.cancel()

	if s.grpcSrv != nil {
		s.grpcSrv.GracefulStop()
	}

	if s.metrics != nil {
		if err := s.metrics.Stop(); err != nil {
			log.Printf("Error stopping metrics server: %v", err)
		}
	}

	s.wg.Wait()
	return nil
}

func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) getSession(id int) (*Session, bool) {
	val, ok := s.sessions.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Session), true
}
