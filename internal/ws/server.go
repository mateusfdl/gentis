package ws

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gobwas/ws"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Server struct {
	config   *Config
	listener net.Listener
	httpSrv  *http.Server
	engine   engine.Engine
	store    *transport.SessionStore
	sessions sync.Map
	nextID   atomic.Int64

	connectionCount atomic.Int64

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

	store := cfg.SessionStore
	if store == nil {
		store = transport.NewSessionStore()
	}

	s := &Server{
		config: cfg,
		engine: eng,
		store:  store,
		ctx:    ctx,
		cancel: cancel,
	}
	s.nextID.Store(wsIDOffset)
	return s
}

func (s *Server) ConnectionCount() int64 {
	return s.connectionCount.Load()
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}
	s.listener = listener

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleUpgrade)

	s.httpSrv = &http.Server{
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpSrv.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("WebSocket server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	s.cancel()

	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("failed to shutdown WebSocket server: %w", err)
		}
	}

	s.wg.Wait()
	return nil
}

func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	sess := s.createSession()
	defer s.cleanupSession(sess)

	go s.runWriter(sess, conn)
	s.runReader(sess, conn)
}
