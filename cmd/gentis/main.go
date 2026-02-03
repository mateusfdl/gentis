// Gentis - A lightweight real-time pub/sub server
//
// Gentis is a high-performance pub/sub server inspired by Pusher,
// rewritten from Zig to Go with idiomatic patterns.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
)

const (
	defaultAddress = "127.0.0.1:9000"
)

func main() {
	address := flag.String("addr", defaultAddress, "gRPC server listen address (host:port)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Starting Gentis gRPC server on %s", *address)

	srv := grpcserver.New(*address)

	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("Server stopped cleanly")
}
