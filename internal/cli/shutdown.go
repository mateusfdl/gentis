package cli

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func waitForShutdown(logger *slog.Logger, stop func() error) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("received signal, initiating graceful shutdown", "signal", sig)

	if err := stop(); err != nil {
		logger.Error("error during shutdown", "error", err)
	}
}
