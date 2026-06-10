package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Server struct {
	gentisv1.UnimplementedGentisServiceServer

	config    *Config
	listener  net.Listener
	grpcSrv   *grpc.Server
	engine    *engine.Engine
	store     *transport.SessionStore
	sessArena *arena.Arena
	sessions  sync.Map
	nextID    atomic.Int32

	logger              *slog.Logger
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

	// Arena slots physically hold at most arena.MaxSubscriptions entries;
	// anything past that would be dropped silently, so clamp the limit to
	// keep SUBSCRIPTION_LIMIT the single source of truth.
	if cfg.UseArena && (cfg.MaxSubscriptions <= 0 || cfg.MaxSubscriptions > arena.MaxSubscriptions) {
		cfg.MaxSubscriptions = arena.MaxSubscriptions
	}

	ctx, cancel := context.WithCancel(context.Background())

	eng := cfg.Engine
	if eng == nil {
		eng = engine.New()
	}

	logger := cfg.Logger
	if logger == nil {
		logger = gentislog.Nop()
	}
	logger = logger.With("component", "grpc")

	s := &Server{
		config: cfg,
		engine: eng,
		store:  cfg.SessionStore,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
	// When arena is on, counter-based IDs (arena-exhausted fallback)
	// must start ABOVE the arena range so they never collide with
	// arena-derived IDs (which occupy [1, MaxSessions]).
	if cfg.UseArena {
		maxSessions := cfg.MaxSessions
		if maxSessions <= 0 {
			maxSessions = 16384
		}
		s.nextID.Store(int32(maxSessions))
	}
	return s
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
	serverOpts := s.keepaliveOptions()
	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(s.config.TLSCertFile, s.config.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	s.grpcSrv = grpc.NewServer(serverOpts...)
	gentisv1.RegisterGentisServiceServer(s.grpcSrv, s)

	if s.config.UseArena {
		maxSessions := s.config.MaxSessions
		if maxSessions <= 0 {
			maxSessions = 16384
		}
		slotSize := int(unsafe.Sizeof(arena.SessionSlot{}))
		a, err := arena.New(slotSize, maxSessions)
		if err != nil {
			s.logger.Warn("grpc arena init failed, falling back to heap session state", "err", err)
		} else {
			s.sessArena = a
		}
	}

	if s.config.MetricsEnabled {
		// Sum gRPC's own active count with any extras registered via
		// WithExtraConnectionCounter (e.g. the WS server), so
		// `gentis_connections_active` reflects all live transports. The
		// total/disconnection counters stay tied to gRPC's own state.
		var connSrc metrics.ConnectionCounter = s
		if len(s.config.ExtraConnCounters) > 0 {
			connSrc = sumConnCounter{ConnectionCounter: s, extras: s.config.ExtraConnCounters}
		}
		collector := metrics.NewCollector(s.engine, connSrc, "server")
		if s.config.Observer != nil {
			collector.SetObserver(s.config.Observer)
		}
		s.metrics = metrics.NewServer(s.config.MetricsAddr, collector)
		if err := s.metrics.Start(); err != nil {
			return fmt.Errorf("failed to start metrics server: %w", err)
		}
		s.logger.Info("metrics server started", "addr", s.config.MetricsAddr)
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
			s.logger.Error("failed to stop metrics server", "error", err)
		}
	}

	s.wg.Wait()

	// Close the arena after all session cleanups have run
	// (GracefulStop drains in-flight streams
	//  Stream() → cleanupSession
	// → ArenaState.Close via the deferred path in Stream handler).
	if s.sessArena != nil {
		if err := s.sessArena.Close(); err != nil {
			s.logger.Error("failed to close session arena", "error", err)
		}
	}
	return nil
}

func (s *Server) getSession(id int) (*Session, bool) {
	val, ok := s.sessions.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Session), true
}

// sumConnCounter folds extra ConnectionCount() sources into the gauge
// reported by the primary metrics.ConnectionCounter. ConnectionsTotal()
// and DisconnectionsTotal() are inherited from the primary unchanged
// only the active gauge is summed. Used so `gentis_connections_active`
// reflects grpc + ws sessions while churn counters stay tied to grpc's
// own state
type sumConnCounter struct {
	metrics.ConnectionCounter // primary; promotes ConnectionsTotal/DisconnectionsTotal
	extras                    []ActiveConnCounter
}

func (s sumConnCounter) ConnectionCount() int64 {
	n := s.ConnectionCounter.ConnectionCount()
	for _, e := range s.extras {
		if e == nil {
			continue
		}
		n += e.ConnectionCount()
	}
	return n
}

func (s *Server) keepaliveOptions() []grpc.ServerOption {
	if s.config.PingInterval <= 0 {
		return nil
	}
	return []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    s.config.PingInterval,
			Timeout: 2 * s.config.PingInterval,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}
