package cli

import (
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
	addWSFlags(serveCmd)
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	logger, err := buildLogger(cmd)
	if err != nil {
		return err
	}

	addr, _ := cmd.Flags().GetString("addr")
	metricsAddr, _ := cmd.Flags().GetString("metrics-addr")
	metricsEnabled, _ := cmd.Flags().GetBool("metrics")

	var obs *metrics.Observer
	if metricsEnabled {
		obs = metrics.NewObserver("server")
	}

	engOpts := buildEngineOpts(cmd, obs)
	eng := engine.New(engOpts...)
	store := transport.NewSessionStore()

	grpcOpts := []grpcserver.Option{
		grpcserver.WithEngine(eng),
		grpcserver.WithSessionStore(store),
		grpcserver.WithLogger(logger),
	}
	if metricsEnabled {
		grpcOpts = append(grpcOpts,
			grpcserver.WithMetrics(metricsAddr),
			grpcserver.WithObserver(obs),
		)
	}

	grpcSrv := grpcserver.New(addr, grpcOpts...)

	logger.Info("starting gRPC server", "addr", addr)
	if err := grpcSrv.Start(); err != nil {
		return err
	}

	wsSrv := buildWSServer(cmd, eng, store)
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
			if err := wsSrv.Stop(); err != nil && firstErr == nil {
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
