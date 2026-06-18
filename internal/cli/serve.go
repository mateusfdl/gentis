package cli

import (
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the pub/sub server (gRPC + optional WebSocket)",
	Long: `Start the Gentis pub/sub server. Listens for gRPC client connections
and optionally exposes a WebSocket transport. Prometheus metrics are
served on a separate HTTP endpoint.`,
	RunE: runServe,
}

func init() {
	serveCmd.Flags().String("addr", "0.0.0.0:9000", "gRPC listen address (host:port)")
	serveCmd.Flags().String("metrics-addr", ":8080", "metrics/health HTTP server address")
	serveCmd.Flags().Bool("arena", false, "use mmap arena for session state (Linux only); applies to gRPC sessions")
	serveCmd.Flags().Int("max-sessions", 16384, "arena session capacity (only used when --arena is set)")
	serveCmd.Flags().Duration("ping-interval", 25*time.Second, "transport keepalive ping interval, 0 to disable")
	serveCmd.Flags().Duration("auth-deadline", 30*time.Second, "close sessions that have not authenticated within this window, 0 to disable")
	serveCmd.Flags().String("tls-cert", "", "TLS certificate file for gRPC and WebSocket listeners")
	serveCmd.Flags().String("tls-key", "", "TLS private key file for gRPC and WebSocket listeners")
	serveCmd.Flags().Int("max-message-size", 65536, "maximum publish payload size in bytes")
	serveCmd.Flags().Int("max-subscriptions", 16, "maximum subscriptions per session, 0 for unlimited")
	serveCmd.Flags().String("debug-addr", "", "pprof/debug HTTP server address (host:port), empty to disable")
	addAuthFlags(serveCmd)
	addWSFlags(serveCmd)
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	logger, err := buildLogger(cmd)
	if err != nil {
		return err
	}

	debugAddr, _ := cmd.Flags().GetString("debug-addr")
	startDebugServer(debugAddr, logger)

	addr, _ := cmd.Flags().GetString("addr")
	metricsAddr, _ := cmd.Flags().GetString("metrics-addr")
	metricsEnabled, _ := cmd.Flags().GetBool("metrics")

	verifier, err := buildVerifier(cmd, logger)
	if err != nil {
		return err
	}

	var obs *metrics.Observer
	if metricsEnabled {
		obs = metrics.NewObserver("server")
	}

	engOpts, err := buildEngineOpts(cmd, logger, obs)
	if err != nil {
		return err
	}
	eng := engine.New(engOpts...)

	arenaEnabled, _ := cmd.Flags().GetBool("arena")
	maxSessions, _ := cmd.Flags().GetInt("max-sessions")

	// when arena is on, ids land densely in [1, maxSessions] so a flat-
	// array store gives O(1) lookup with a single pointer-array gc scan.
	// otherwise the legacy sync.Map is fine, counter ids aren't dense.
	var store *transport.SessionStore
	if arenaEnabled {
		store = transport.NewFlatSessionStore(engine.SubscriberID(1), maxSessions)
	} else {
		store = transport.NewSessionStore()
	}

	// build (but don't start) the ws server first so we can hand its
	// connection counter to the grpc metrics collector. without this,
	// `gentis_connections_active` only reflects grpc sessions and reads
	// zero during a ws-only run.
	wsSrv := buildWSServer(cmd, logger, eng, store, obs, verifier)

	pingInterval, _ := cmd.Flags().GetDuration("ping-interval")
	authDeadline, _ := cmd.Flags().GetDuration("auth-deadline")
	maxMessageSize, _ := cmd.Flags().GetInt("max-message-size")
	maxSubscriptions, _ := cmd.Flags().GetInt("max-subscriptions")
	tlsCert, _ := cmd.Flags().GetString("tls-cert")
	tlsKey, _ := cmd.Flags().GetString("tls-key")
	if (tlsCert == "") != (tlsKey == "") {
		return errTLSIncomplete
	}

	grpcOpts := []grpcserver.Option{
		grpcserver.WithEngine(eng),
		grpcserver.WithSessionStore(store),
		grpcserver.WithLogger(logger),
		grpcserver.WithVerifier(verifier),
		grpcserver.WithPingInterval(pingInterval),
		grpcserver.WithAuthDeadline(authDeadline),
		grpcserver.WithMaxMessageSize(maxMessageSize),
		grpcserver.WithMaxSubscriptions(maxSubscriptions),
	}
	if tlsCert != "" {
		grpcOpts = append(grpcOpts, grpcserver.WithTLS(tlsCert, tlsKey))
	}
	if arenaEnabled {
		grpcOpts = append(grpcOpts,
			grpcserver.WithArena(),
			grpcserver.WithMaxSessions(maxSessions),
		)
	}
	if metricsEnabled {
		grpcOpts = append(grpcOpts,
			grpcserver.WithMetrics(metricsAddr),
			grpcserver.WithObserver(obs),
		)
	}
	if wsSrv != nil {
		grpcOpts = append(grpcOpts, grpcserver.WithExtraConnectionCounter(wsSrv))
	}

	grpcSrv := grpcserver.New(addr, grpcOpts...)

	logger.Info("starting gRPC server", "addr", addr)
	if err := grpcSrv.Start(); err != nil {
		return err
	}

	if wsSrv != nil {
		wsAddr, _ := cmd.Flags().GetString("ws-addr")
		logger.Info("starting WebSocket server", "addr", wsAddr)
		if err := wsSrv.Start(); err != nil {
			grpcSrv.Stop()
			return err
		}
	}

	waitForShutdown(logger, func() error {
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
	})

	logger.Info("server stopped cleanly")
	return nil
}
