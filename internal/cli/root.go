package cli

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/config"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
	wsserver "github.com/mateusfdl/gentis/internal/ws"
	"github.com/spf13/cobra"
)

func Execute(version, commit string) {
	root := newRootCmd(version, commit)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCmd(version, commit string) *cobra.Command {
	root := &cobra.Command{
		Use:   "gentis",
		Short: "A lightweight real-time pub/sub server",
		Long: `Gentis is a high-performance real-time pub/sub server with gRPC and
WebSocket transports, relay support for horizontal scaling, and
Prometheus metrics.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}
	root.PersistentFlags().String("config", "", "path to the gentis.yaml config file (built-in defaults when omitted)")
	root.SetVersionTemplate("gentis {{.Version}}\n")
	root.AddCommand(
		newServeCmd(),
		newRelayCmd(),
		newHealthCmd(),
		newVersionCmd(version, commit),
	)
	return root
}

// loadConfig reads the unified config document named by --config, or the
// built-in defaults when the flag is empty.
func loadConfig(cmd *cobra.Command) (*config.Config, error) {
	path, err := cmd.Flags().GetString("config")
	if err != nil {
		return nil, err
	}
	if path == "" {
		return config.Default()
	}
	return config.Load(path)
}

func buildLogger(cfg config.Log) *slog.Logger {
	return gentislog.New(gentislog.Config{
		Level:  cfg.Level,
		Format: cfg.Format,
		Output: os.Stderr,
	})
}

func buildEngineOpts(cfg *config.Config, logger *slog.Logger, obs *metrics.Observer) []engine.Option {
	opts := []engine.Option{engine.WithLogger(logger)}

	if cfg.Namespaces != nil {
		opts = append(opts, engine.WithNamespaces(cfg.Namespaces))
	}
	if cfg.Engine.Shards > 0 {
		opts = append(opts, engine.WithShards(cfg.Engine.Shards))
	}
	opts = append(opts,
		engine.WithFanoutThreshold(cfg.Engine.FanoutThreshold),
		engine.WithFanoutWorkers(cfg.Engine.FanoutWorkers),
	)
	if cfg.Engine.HistorySize > 0 {
		opts = append(opts, engine.WithHistory(cfg.Engine.HistorySize, cfg.Engine.HistoryTTL))
	}
	if obs != nil {
		opts = append(opts, engine.WithObserver(obs))
	}
	if cfg.GC.Pacer {
		opts = append(opts,
			engine.WithGCPacer(cfg.GC.MemLimit),
			engine.WithGCPacerSpikeGOGC(cfg.GC.SpikeGOGC),
			engine.WithGCPacerNormalGOGC(cfg.GC.NormalGOGC),
		)
	}

	return opts
}

// newSessionStore picks the session store implementation. When arena is on,
// ids land densely in [1, maxSessions] so a flat-array store gives O(1) lookup
// with a single pointer-array gc scan; otherwise the sync.Map store is fine.
func newSessionStore(arena bool, maxSessions int) *transport.SessionStore {
	if arena {
		return transport.NewFlatSessionStore(engine.SubscriberID(1), maxSessions)
	}
	return transport.NewSessionStore()
}

// wsTransport carries the transport tunables the WebSocket server shares with
// whichever command hosts it: server for serve, relay for relay.
type wsTransport struct {
	pingInterval     time.Duration
	authDeadline     time.Duration
	maxMessageSize   int
	maxSubscriptions int
	tls              config.TLS
}

func buildWSServer(ws config.WebSocket, tr wsTransport, logger *slog.Logger, eng *engine.Engine, store *transport.SessionStore, obs *metrics.Observer, verifier auth.Verifier) *wsserver.Server {
	if ws.Addr == "" {
		return nil
	}

	opts := []wsserver.Option{
		wsserver.WithEngine(eng),
		wsserver.WithSessionStore(store),
		wsserver.WithLogger(logger),
		wsserver.WithVerifier(verifier),
		wsserver.WithPingInterval(tr.pingInterval),
		wsserver.WithAuthDeadline(tr.authDeadline),
		wsserver.WithReadLimit(ws.ReadLimit),
		wsserver.WithWriteTimeout(ws.WriteTimeout),
		wsserver.WithSendBufferSize(ws.SendBuffer),
	}
	if tr.maxMessageSize > 0 {
		opts = append(opts, wsserver.WithMaxMessageSize(tr.maxMessageSize))
	}
	opts = append(opts, wsserver.WithMaxSubscriptions(tr.maxSubscriptions))
	if tr.tls.Cert != "" && tr.tls.Key != "" {
		opts = append(opts, wsserver.WithTLS(tr.tls.Cert, tr.tls.Key))
	}
	if obs != nil {
		opts = append(opts, wsserver.WithDeliveryLatencyObserver(func(d time.Duration) {
			obs.ObserveDeliveryLatency(d.Seconds())
		}))
	}

	return wsserver.New(ws.Addr, opts...)
}
