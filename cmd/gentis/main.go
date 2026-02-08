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

	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
	"github.com/mateusfdl/gentis/internal/relay"
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
  serve    Start the pub/sub server
  relay    Start a relay (reverse proxy) to an upstream server
  health   Check if a Gentis server is healthy

Run 'gentis <command> -h' for more information on a command.`)
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:9000", "listen address (host:port)")
	metricsAddr := fs.String("metrics-addr", ":8080", "metrics server address")
	metricsEnabled := fs.Bool("metrics", true, "enable Prometheus metrics")
	fs.Parse(args)

	log.Printf("Starting Gentis server on %s", *addr)

	var opts []grpcserver.Option
	if *metricsEnabled {
		opts = append(opts, grpcserver.WithMetrics(*metricsAddr))
	}

	srv := grpcserver.New(*addr, opts...)

	if err := srv.Start(); err != nil {
		log.Printf("Failed to start server: %v", err)
		return 1
	}

	waitForShutdown(func() error {
		return srv.Stop()
	})

	log.Println("Server stopped cleanly")
	return 0
}

func runRelay(args []string) int {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:9001", "relay listen address (host:port)")
	upstream := fs.String("upstream", "", "upstream server address (required)")
	authToken := fs.String("auth-token", "", "authentication token for upstream")
	metricsAddr := fs.String("metrics-addr", ":8081", "metrics server address")
	metricsEnabled := fs.Bool("metrics", true, "enable Prometheus metrics")
	fs.Parse(args)

	if *upstream == "" {
		fmt.Fprintln(os.Stderr, "error: --upstream is required")
		fs.Usage()
		return 1
	}

	log.Printf("Starting Gentis relay on %s -> upstream %s", *addr, *upstream)

	opts := []relay.Option{
		relay.WithListenAddr(*addr),
		relay.WithUpstream(*upstream, *authToken),
		relay.WithReconnectPolicy(100*time.Millisecond, 30*time.Second, 2.0),
	}

	if *metricsEnabled {
		opts = append(opts, relay.WithMetrics(*metricsAddr))
	}

	srv := relay.New(opts...)

	if err := srv.Start(); err != nil {
		log.Printf("Failed to start relay: %v", err)
		return 1
	}

	waitForShutdown(func() error {
		return srv.Stop()
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
