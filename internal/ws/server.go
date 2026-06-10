package ws

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"

	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Server struct {
	config   *Config
	listener net.Listener
	httpSrv  *http.Server
	engine   *engine.Engine
	store    *transport.SessionStore
	logger   *slog.Logger
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

	logger := cfg.Logger
	if logger == nil {
		logger = gentislog.Nop()
	}
	logger = logger.With("component", "ws")

	s := &Server{
		config: cfg,
		engine: eng,
		store:  store,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
	// ws session IDs are offset above the gRPC counter range so the two
	// transports never collide in a shared SessionStore.
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
	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(s.config.TLSCertFile, s.config.TLSKeyFile)
		if err != nil {
			listener.Close()
			return fmt.Errorf("failed to load TLS key pair: %w", err)
		}
		listener = tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{cert}})
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
			s.logger.Error("websocket serve error", "err", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	s.cancel()

	if s.httpSrv != nil {
		// Bounded shutdown timeout so a stuck hijacked connection cannot
		// block the whole test suite / caller indefinitely.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to shutdown WebSocket server: %w", err)
		}
	}

	s.wg.Wait()
	return nil
}

func (s *Server) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		s.logger.Warn("websocket upgrade failed", "remote_addr", r.RemoteAddr, "err", err)
		return
	}

	sess := s.createSession()
	defer s.cleanupSession(sess)

	go s.runWriter(sess, conn)
	s.runReader(sess, conn)
}
