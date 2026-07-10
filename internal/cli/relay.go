package cli

import (
	"log/slog"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/config"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/relay"
	"github.com/mateusfdl/gentis/internal/transport"
	wsserver "github.com/mateusfdl/gentis/internal/ws"
	"github.com/spf13/cobra"
)

func newRelayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "relay",
		Short: "Start a relay (reverse proxy) to an upstream server",
		Long: `Start a Gentis relay that listens for local client connections and
proxies subscriptions and publishes to an upstream Gentis server.
Supports automatic reconnection with configurable backoff, message
deduplication, and channel routing.`,
		RunE: runRelay,
	}
}

func runRelay(cmd *cobra.Command, _ []string) error {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return err
	}
	if err := cfg.RelayReady(); err != nil {
		return err
	}

	logger := buildLogger(cfg.Log)
	verifier := buildVerifier(cfg.Auth, logger)

	var obs *metrics.Observer
	if cfg.Metrics.Enabled {
		obs = metrics.NewObserver("relay")
	}

	eng := engine.New(buildEngineOpts(cfg, logger, obs)...)
	store := newSessionStore(cfg.Relay.Arena, cfg.Relay.MaxSessions)

	relaySrv := relay.New(buildRelayOpts(cfg, logger, eng, store, obs, verifier)...)

	logger.Info("starting relay", "addr", cfg.Relay.Addr, "upstream", cfg.Relay.Upstream.Addr)
	if err := relaySrv.Start(); err != nil {
		return err
	}

	wsSrv := buildWSServer(cfg.WebSocket, relayWSTransport(cfg.Relay), logger, eng, store, obs, verifier)
	if wsSrv != nil {
		logger.Info("starting WebSocket server", "addr", cfg.WebSocket.Addr)
		if err := wsSrv.Start(); err != nil {
			relaySrv.Stop()
			return err
		}
	}

	waitForShutdown(logger, func() error { return stopRelay(eng, relaySrv, wsSrv) })
	logger.Info("relay stopped cleanly")
	return nil
}

func relayWSTransport(r config.Relay) wsTransport {
	return wsTransport{
		pingInterval:     r.PingInterval,
		authDeadline:     r.AuthDeadline,
		maxMessageSize:   r.MaxMessageSize,
		maxSubscriptions: r.MaxSubscriptions,
		tls:              r.TLS,
	}
}

func buildRelayOpts(cfg *config.Config, logger *slog.Logger, eng *engine.Engine, store *transport.SessionStore, obs *metrics.Observer, verifier auth.Verifier) []relay.Option {
	r := cfg.Relay
	opts := []relay.Option{
		relay.WithListenAddr(r.Addr),
		relay.WithUpstream(r.Upstream.Addr, r.Upstream.AuthToken),
		relay.WithReconnectPolicy(r.Reconnect.Initial, r.Reconnect.Max, r.Reconnect.Multiplier),
		relay.WithMaxRetries(r.Reconnect.MaxRetries),
		relay.WithBufferSize(r.BufferSize),
		relay.WithIncomingBuffer(r.IncomingBuffer),
		relay.WithFanoutWorkers(r.FanoutWorkers),
		relay.WithEngine(eng),
		relay.WithSessionStore(store),
		relay.WithLogger(logger),
		relay.WithVerifier(verifier),
		relay.WithPingInterval(r.PingInterval),
		relay.WithAuthDeadline(r.AuthDeadline),
		relay.WithMaxMessageSize(r.MaxMessageSize),
		relay.WithMaxSubscriptions(r.MaxSubscriptions),
	}
	if r.TLS.Cert != "" {
		opts = append(opts, relay.WithTLS(r.TLS.Cert, r.TLS.Key))
	}
	if r.Upstream.TLS {
		opts = append(opts, relay.WithUpstreamTLS(r.Upstream.CA))
	}
	if r.Arena {
		opts = append(opts, relay.WithArena(), relay.WithMaxSessions(r.MaxSessions))
	}
	if cfg.Metrics.Enabled {
		opts = append(opts, relay.WithMetrics(r.MetricsAddr), relay.WithObserver(obs))
	}
	return opts
}

func stopRelay(eng *engine.Engine, relaySrv *relay.Server, wsSrv *wsserver.Server) error {
	var firstErr error
	if wsSrv != nil {
		if err := wsSrv.Stop(); err != nil {
			firstErr = err
		}
	}
	if err := relaySrv.Stop(); err != nil && firstErr == nil {
		firstErr = err
	}
	eng.Stop()
	return firstErr
}
