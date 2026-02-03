package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
)

type Server struct {
	gentisv1.UnimplementedGentisServiceServer

	address  string
	listener net.Listener
	grpcSrv  *grpc.Server
	engine   engine.Engine
	sessions sync.Map
	nextID   atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(address string) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		address: address,
		engine:  engine.New(),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	s.listener = listener
	s.grpcSrv = grpc.NewServer()
	gentisv1.RegisterGentisServiceServer(s.grpcSrv, s)

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
