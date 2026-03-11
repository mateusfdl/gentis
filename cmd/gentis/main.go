package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/relay"
	"github.com/mateusfdl/gentis/internal/transport"
	wsserver "github.com/mateusfdl/gentis/internal/ws"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		os.Exit(runServe(os.Args[2:]))
	case "relay":
		os.Exit(runRelay(os.Args[2:]))
	case "health":
		os.Exit(runHealth(os.Args[2:]))
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Gentis - A lightweight real-time pub/sub server

Usage:
  gentis <command> [flags]

Commands:
  serve    Start the pub/sub server (gRPC + optional WebSocket)
  relay    Start a relay (reverse proxy) to an upstream server
  health   Check if a Gentis server is healthy

Run 'gentis <command> -h' for more information on a command.`)
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "0.0.0.0:9000", "gRPC listen address (host:port)")
	wsAddr := fs.String("ws-addr", "", "WebSocket listen address (host:port), empty to disable")
	metricsAddr := fs.String("metrics-addr", ":8080", "metrics server address")
	metricsEnabled := fs.Bool("metrics", true, "enable Prometheus metrics")
	gcPacer := fs.Bool("gc-pacer", false, "enable automatic GC tuning (spike detection + idle GC)")
	gcMemLimit := fs.Int64("gc-mem-limit", 0, "soft memory limit in bytes for GC pacer (0 = no limit)")
	fs.Parse(args)

	var obs *metrics.Observer
	engOpts := []engine.Option{}
	if *metricsEnabled {
		obs = metrics.NewObserver("server")
		engOpts = append(engOpts, engine.WithObserver(obs))
	}
	if *gcPacer {
		engOpts = append(engOpts, engine.WithGCPacer(*gcMemLimit))
	}

	eng := engine.New(engOpts...)
	store := transport.NewSessionStore()

	grpcOpts := []grpcserver.Option{
		grpcserver.WithEngine(eng),
		grpcserver.WithSessionStore(store),
	}
	if *metricsEnabled {
		grpcOpts = append(grpcOpts, grpcserver.WithMetrics(*metricsAddr))
		grpcOpts = append(grpcOpts, grpcserver.WithObserver(obs))
	}

	grpcSrv := grpcserver.New(*addr, grpcOpts...)

	log.Printf("Starting Gentis gRPC server on %s", *addr)
	if err := grpcSrv.Start(); err != nil {
		log.Printf("Failed to start gRPC server: %v", err)
		return 1
	}

	var wsSrv *wsserver.Server
	if *wsAddr != "" {
		wsSrv = wsserver.New(*wsAddr,
			wsserver.WithEngine(eng),
			wsserver.WithSessionStore(store),
		)

		log.Printf("Starting Gentis WebSocket server on %s", *wsAddr)
		if err := wsSrv.Start(); err != nil {
			log.Printf("Failed to start WebSocket server: %v", err)
			grpcSrv.Stop()
			return 1
		}
	}

	waitForShutdown(func() error {
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

	log.Println("Server stopped cleanly")
	return 0
}

func runRelay(args []string) int {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:9001", "relay listen address (host:port)")
	wsAddr := fs.String("ws-addr", "", "WebSocket listen address (host:port), empty to disable")
	upstream := fs.String("upstream", "", "upstream server address (required)")
	authToken := fs.String("auth-token", "", "authentication token for upstream")
	metricsAddr := fs.String("metrics-addr", ":8081", "metrics server address")
	metricsEnabled := fs.Bool("metrics", true, "enable Prometheus metrics")
	gcPacer := fs.Bool("gc-pacer", false, "enable automatic GC tuning (spike detection + idle GC)")
	gcMemLimit := fs.Int64("gc-mem-limit", 0, "soft memory limit in bytes for GC pacer (0 = no limit)")
	fs.Parse(args)

	if *upstream == "" {
		fmt.Fprintln(os.Stderr, "error: --upstream is required")
		fs.Usage()
		return 1
	}

	log.Printf("Starting Gentis relay on %s -> upstream %s", *addr, *upstream)

	var relayObs *metrics.Observer
	relayEngOpts := []engine.Option{}
	if *metricsEnabled {
		relayObs = metrics.NewObserver("relay")
		relayEngOpts = append(relayEngOpts, engine.WithObserver(relayObs))
	}
	if *gcPacer {
		relayEngOpts = append(relayEngOpts, engine.WithGCPacer(*gcMemLimit))
	}

	eng := engine.New(relayEngOpts...)
	store := transport.NewSessionStore()

	opts := []relay.Option{
		relay.WithListenAddr(*addr),
		relay.WithUpstream(*upstream, *authToken),
		relay.WithReconnectPolicy(100*time.Millisecond, 30*time.Second, 2.0),
		relay.WithEngine(eng),
		relay.WithSessionStore(store),
	}

	if *metricsEnabled {
		opts = append(opts, relay.WithMetrics(*metricsAddr))
		opts = append(opts, relay.WithObserver(relayObs))
	}

	relaySrv := relay.New(opts...)

	if err := relaySrv.Start(); err != nil {
		log.Printf("Failed to start relay: %v", err)
		return 1
	}

	var wsSrv *wsserver.Server
	if *wsAddr != "" {
		wsSrv = wsserver.New(*wsAddr,
			wsserver.WithEngine(eng),
			wsserver.WithSessionStore(store),
		)

		log.Printf("Starting Gentis WebSocket server on %s", *wsAddr)
		if err := wsSrv.Start(); err != nil {
			log.Printf("Failed to start WebSocket server: %v", err)
			relaySrv.Stop()
			return 1
		}
	}

	waitForShutdown(func() error {
		var firstErr error
		if wsSrv != nil {
			if err := wsSrv.Stop(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := relaySrv.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
		eng.Stop()
		return firstErr
	})

	log.Println("Relay stopped cleanly")
	return 0
}

func runHealth(args []string) int {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	addr := fs.String("addr", "http://localhost:8080", "health endpoint base URL")
	fs.Parse(args)

	resp, err := http.Get(*addr + "/health")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func waitForShutdown(stop func() error) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	if err := stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
