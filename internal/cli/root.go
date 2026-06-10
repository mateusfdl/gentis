package cli

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
	wsserver "github.com/mateusfdl/gentis/internal/ws"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gentis",
	Short: "A lightweight real-time pub/sub server",
	Long: `Gentis is a high-performance real-time pub/sub server with gRPC and
WebSocket transports, relay support for horizontal scaling, and
Prometheus metrics.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

var (
	buildVersion string
	buildCommit  string
)

func Execute(version, commit string) {
	buildVersion = version
	buildCommit = commit
	rootCmd.Version = version

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	pf := rootCmd.PersistentFlags()

	pf.String("log-level", "info", "log level (debug, info, warn, error)")
	pf.String("log-format", "text", "log format (text, json)")
	pf.Bool("metrics", true, "enable Prometheus metrics")
	pf.Bool("gc-pacer", false, "enable automatic GC tuning (spike detection + idle GC)")
	pf.Int64("gc-mem-limit", 0, "soft memory limit in bytes for GC pacer (0 = no limit)")
	pf.Int("gc-spike-gogc", 400, "GOGC value during detected activity spikes")
	pf.Int("gc-normal-gogc", 100, "GOGC value during normal operation")
	pf.Int("shards", 0, "engine shard count (0 = auto, rounded to power-of-2)")
	pf.Int("fanout-threshold", 100_000, "subscriber count to trigger parallel fanout")
	pf.Int("fanout-workers", 4, "parallel fanout goroutine count")

	rootCmd.SetVersionTemplate("gentis {{.Version}}\n")
}

func buildLogger(cmd *cobra.Command) (*slog.Logger, error) {
	levelStr, _ := cmd.Flags().GetString("log-level")
	formatStr, _ := cmd.Flags().GetString("log-format")

	level, err := gentislog.ParseLevel(levelStr)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}

	format, err := gentislog.ParseFormat(formatStr)
	if err != nil {
		return nil, fmt.Errorf("invalid log format: %w", err)
	}

	return gentislog.New(gentislog.Config{
		Level:  level,
		Format: format,
		Output: os.Stderr,
	}), nil
}

func buildEngineOpts(cmd *cobra.Command, logger *slog.Logger, obs *metrics.Observer) []engine.Option {
	var opts []engine.Option

	opts = append(opts, engine.WithLogger(logger))

	shards, _ := cmd.Flags().GetInt("shards")
	if shards > 0 {
		opts = append(opts, engine.WithShards(shards))
	}

	fanoutThreshold, _ := cmd.Flags().GetInt("fanout-threshold")
	opts = append(opts, engine.WithFanoutThreshold(fanoutThreshold))

	fanoutWorkers, _ := cmd.Flags().GetInt("fanout-workers")
	opts = append(opts, engine.WithFanoutWorkers(fanoutWorkers))

	if obs != nil {
		opts = append(opts, engine.WithObserver(obs))
	}

	gcPacer, _ := cmd.Flags().GetBool("gc-pacer")
	if gcPacer {
		gcMemLimit, _ := cmd.Flags().GetInt64("gc-mem-limit")
		opts = append(opts, engine.WithGCPacer(gcMemLimit))

		spikeGOGC, _ := cmd.Flags().GetInt("gc-spike-gogc")
		opts = append(opts, engine.WithGCPacerSpikeGOGC(spikeGOGC))

		normalGOGC, _ := cmd.Flags().GetInt("gc-normal-gogc")
		opts = append(opts, engine.WithGCPacerNormalGOGC(normalGOGC))
	}

	return opts
}

func buildWSServer(cmd *cobra.Command, logger *slog.Logger, eng *engine.Engine, store *transport.SessionStore, obs *metrics.Observer, verifier auth.Verifier) *wsserver.Server {
	wsAddr, _ := cmd.Flags().GetString("ws-addr")
	if wsAddr == "" {
		return nil
	}

	readLimit, _ := cmd.Flags().GetInt64("ws-read-limit")
	writeTimeout, _ := cmd.Flags().GetDuration("ws-write-timeout")
	sendBuffer, _ := cmd.Flags().GetInt("ws-send-buffer")

	pingInterval, _ := cmd.Flags().GetDuration("ping-interval")
	maxMessageSize, _ := cmd.Flags().GetInt("max-message-size")
	maxSubscriptions, _ := cmd.Flags().GetInt("max-subscriptions")
	tlsCert, _ := cmd.Flags().GetString("tls-cert")
	tlsKey, _ := cmd.Flags().GetString("tls-key")

	opts := []wsserver.Option{
		wsserver.WithEngine(eng),
		wsserver.WithSessionStore(store),
		wsserver.WithLogger(logger),
		wsserver.WithVerifier(verifier),
		wsserver.WithPingInterval(pingInterval),
		wsserver.WithReadLimit(readLimit),
		wsserver.WithWriteTimeout(writeTimeout),
		wsserver.WithSendBufferSize(sendBuffer),
	}
	if maxMessageSize > 0 {
		opts = append(opts, wsserver.WithMaxMessageSize(maxMessageSize))
	}
	opts = append(opts, wsserver.WithMaxSubscriptions(maxSubscriptions))
	if tlsCert != "" && tlsKey != "" {
		opts = append(opts, wsserver.WithTLS(tlsCert, tlsKey))
	}
	if obs != nil {
		opts = append(opts, wsserver.WithDeliveryLatencyObserver(func(d time.Duration) {
			obs.ObserveDeliveryLatency(d.Seconds())
		}))
	}

	return wsserver.New(wsAddr, opts...)
}

func addWSFlags(cmd *cobra.Command) {
	cmd.Flags().String("ws-addr", "", "WebSocket listen address (host:port), empty to disable")
	cmd.Flags().Int64("ws-read-limit", 65536, "WebSocket max message size in bytes")
	cmd.Flags().Duration("ws-write-timeout", 10*time.Second, "WebSocket write deadline")
	cmd.Flags().Int("ws-send-buffer", 256, "WebSocket per-session send buffer size")
}
