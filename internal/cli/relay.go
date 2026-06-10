package cli

import (
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/relay"
	"github.com/mateusfdl/gentis/internal/transport"
	"github.com/spf13/cobra"
)

var relayCmd = &cobra.Command{
	Use:   "relay",
	Short: "Start a relay (reverse proxy) to an upstream server",
	Long: `Start a Gentis relay that listens for local client connections and
proxies subscriptions and publishes to an upstream Gentis server.
Supports automatic reconnection with configurable backoff, message
deduplication, and channel routing.`,
	RunE: runRelay,
}

func init() {
	f := relayCmd.Flags()

	f.String("addr", "127.0.0.1:9001", "relay gRPC listen address (host:port)")
	f.String("upstream", "", "upstream server address (required)")
	f.String("auth-token", "", "authentication token for upstream")
	f.String("metrics-addr", ":8081", "metrics/health HTTP server address")

	f.Duration("reconnect-initial", 100*time.Millisecond, "reconnect backoff initial delay")
	f.Duration("reconnect-max", 30*time.Second, "reconnect backoff maximum delay")
	f.Float64("reconnect-multiplier", 2.0, "reconnect backoff multiplier")
	f.Int("max-retries", 0, "maximum reconnect retries (0 = unlimited)")

	f.Int("buffer-size", 256, "per-session send buffer size")
	f.Int("incoming-buffer", 4096, "incoming message buffer from upstream")
	f.Int("relay-fanout-workers", 4, "relay-local parallel fanout goroutine count")

	f.Bool("arena", false, "use mmap arena for session state (Linux only); applies to relay sessions")
	f.Int("max-sessions", 16384, "arena session capacity (only used when --arena is set)")

	relayCmd.Flags().Duration("ping-interval", 25*time.Second, "transport keepalive ping interval, 0 to disable")
	f.Bool("upstream-tls", false, "dial the upstream over TLS")
	f.String("upstream-ca", "", "CA bundle for upstream TLS verification (empty = system roots)")
	f.Int("max-message-size", 65536, "maximum publish payload size in bytes")
	f.Int("max-subscriptions", 16, "maximum subscriptions per session, 0 for unlimited")
	addAuthFlags(relayCmd)
	addWSFlags(relayCmd)

	relayCmd.MarkFlagRequired("upstream")
	rootCmd.AddCommand(relayCmd)
}

func runRelay(cmd *cobra.Command, args []string) error {
	logger, err := buildLogger(cmd)
	if err != nil {
		return err
	}

	addr, _ := cmd.Flags().GetString("addr")
	upstream, _ := cmd.Flags().GetString("upstream")
	authToken, _ := cmd.Flags().GetString("auth-token")
	metricsAddr, _ := cmd.Flags().GetString("metrics-addr")
	metricsEnabled, _ := cmd.Flags().GetBool("metrics")

	verifier, err := buildVerifier(cmd, logger)
	if err != nil {
		return err
	}

	reconnectInitial, _ := cmd.Flags().GetDuration("reconnect-initial")
	reconnectMax, _ := cmd.Flags().GetDuration("reconnect-max")
	reconnectMult, _ := cmd.Flags().GetFloat64("reconnect-multiplier")
	maxRetries, _ := cmd.Flags().GetInt("max-retries")

	bufferSize, _ := cmd.Flags().GetInt("buffer-size")
	incomingBuffer, _ := cmd.Flags().GetInt("incoming-buffer")
	relayFanoutWorkers, _ := cmd.Flags().GetInt("relay-fanout-workers")
	pingInterval, _ := cmd.Flags().GetDuration("ping-interval")
	maxMessageSize, _ := cmd.Flags().GetInt("max-message-size")
	maxSubscriptions, _ := cmd.Flags().GetInt("max-subscriptions")

	var obs *metrics.Observer
	if metricsEnabled {
		obs = metrics.NewObserver("relay")
	}

	engOpts := buildEngineOpts(cmd, logger, obs)
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

	opts := []relay.Option{
		relay.WithListenAddr(addr),
		relay.WithUpstream(upstream, authToken),
		relay.WithReconnectPolicy(reconnectInitial, reconnectMax, reconnectMult),
		relay.WithMaxRetries(maxRetries),
		relay.WithBufferSize(bufferSize),
		relay.WithIncomingBuffer(incomingBuffer),
		relay.WithFanoutWorkers(relayFanoutWorkers),
		relay.WithEngine(eng),
		relay.WithSessionStore(store),
		relay.WithLogger(logger),
		relay.WithVerifier(verifier),
		relay.WithPingInterval(pingInterval),
		relay.WithMaxMessageSize(maxMessageSize),
		relay.WithMaxSubscriptions(maxSubscriptions),
	}
	upstreamTLS, _ := cmd.Flags().GetBool("upstream-tls")
	upstreamCA, _ := cmd.Flags().GetString("upstream-ca")
	if upstreamTLS {
		opts = append(opts, relay.WithUpstreamTLS(upstreamCA))
	}
	if arenaEnabled {
		opts = append(opts,
			relay.WithArena(),
			relay.WithMaxSessions(maxSessions),
		)
	}

	if metricsEnabled {
		opts = append(opts,
			relay.WithMetrics(metricsAddr),
			relay.WithObserver(obs),
		)
	}

	relaySrv := relay.New(opts...)

	logger.Info("starting relay", "addr", addr, "upstream", upstream)
	if err := relaySrv.Start(); err != nil {
		return err
	}

	wsSrv := buildWSServer(cmd, logger, eng, store, obs, verifier)
	if wsSrv != nil {
		wsAddr, _ := cmd.Flags().GetString("ws-addr")
		logger.Info("starting WebSocket server", "addr", wsAddr)
		if err := wsSrv.Start(); err != nil {
			relaySrv.Stop()
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
		if err := relaySrv.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
		eng.Stop()
		return firstErr
	})

	logger.Info("relay stopped cleanly")
	return nil
}
