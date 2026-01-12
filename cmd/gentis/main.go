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

	"github.com/mateusfdl/gentis/internal/server"
)

const (
	defaultAddress = "127.0.0.1:9000"
)

func main() {
	address := flag.String("addr", defaultAddress, "Server listen address (host:port)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Starting Gentis server on %s", *address)

	srv := server.New(*address)

	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	log.Println("Server stopped cleanly")
}
