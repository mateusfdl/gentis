package cli

import (
	"log/slog"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/config"
	"github.com/mateusfdl/gentis/internal/engine"
	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
	wsserver "github.com/mateusfdl/gentis/internal/ws"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the pub/sub server (gRPC + optional WebSocket)",
		Long: `Start the Gentis pub/sub server. Listens for gRPC client connections
and optionally exposes a WebSocket transport. Prometheus metrics are
served on a separate HTTP endpoint.`,
		RunE: runServe,
	}
}

func runServe(cmd *cobra.Command, _ []string) error {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return err
	}
	logger := buildLogger(cfg.Log)
	startDebugServer(cfg.Server.DebugAddr, logger)
	verifier := buildVerifier(cfg.Auth, logger)

	var obs *metrics.Observer
	if cfg.Metrics.Enabled {
		obs = metrics.NewObserver("server")
	}

	eng := engine.New(buildEngineOpts(cfg, logger, obs)...)
	store := newSessionStore(cfg.Server.Arena, cfg.Server.MaxSessions)

	// build (but don't start) the ws server first so its connection counter
	// can feed the grpc metrics collector; otherwise a ws-only run reads zero.
	wsSrv := buildWSServer(cfg.WebSocket, serverWSTransport(cfg.Server), logger, eng, store, obs, verifier)
	grpcSrv := grpcserver.New(cfg.Server.Addr, buildGRPCOpts(cfg, logger, eng, store, obs, verifier, wsSrv)...)

	logger.Info("starting gRPC server", "addr", cfg.Server.Addr)
	if err := grpcSrv.Start(); err != nil {
		return err
	}
	if wsSrv != nil {
		logger.Info("starting WebSocket server", "addr", cfg.WebSocket.Addr)
		if err := wsSrv.Start(); err != nil {
			grpcSrv.Stop()
			return err
		}
	}

	waitForShutdown(logger, func() error { return stopServe(eng, grpcSrv, wsSrv) })
	logger.Info("server stopped cleanly")
	return nil
}

func serverWSTransport(s config.Server) wsTransport {
	return wsTransport{
		pingInterval:     s.PingInterval,
		authDeadline:     s.AuthDeadline,
		maxMessageSize:   s.MaxMessageSize,
		maxSubscriptions: s.MaxSubscriptions,
		tls:              s.TLS,
	}
}

func buildGRPCOpts(cfg *config.Config, logger *slog.Logger, eng *engine.Engine, store *transport.SessionStore, obs *metrics.Observer, verifier auth.Verifier, wsSrv *wsserver.Server) []grpcserver.Option {
	s := cfg.Server
	opts := []grpcserver.Option{
		grpcserver.WithEngine(eng),
		grpcserver.WithSessionStore(store),
		grpcserver.WithLogger(logger),
		grpcserver.WithVerifier(verifier),
		grpcserver.WithPingInterval(s.PingInterval),
		grpcserver.WithAuthDeadline(s.AuthDeadline),
		grpcserver.WithMaxMessageSize(s.MaxMessageSize),
		grpcserver.WithMaxSubscriptions(s.MaxSubscriptions),
	}
	if s.TLS.Cert != "" {
		opts = append(opts, grpcserver.WithTLS(s.TLS.Cert, s.TLS.Key))
	}
	if s.Arena {
		opts = append(opts, grpcserver.WithArena(), grpcserver.WithMaxSessions(s.MaxSessions))
	}
	if cfg.Metrics.Enabled {
		opts = append(opts, grpcserver.WithMetrics(s.MetricsAddr), grpcserver.WithObserver(obs))
	}
	if wsSrv != nil {
		opts = append(opts, grpcserver.WithExtraConnectionCounter(wsSrv))
	}
	return opts
}

func stopServe(eng *engine.Engine, grpcSrv *grpcserver.Server, wsSrv *wsserver.Server) error {
	var firstErr error
	if wsSrv != nil {
		if err := wsSrv.Stop(); err != nil {
			firstErr = err
		}
	}
	if err := grpcSrv.Stop(); err != nil && firstErr == nil {
		firstErr = err
	}
	eng.Stop()
	return firstErr
}
