package cli

import (
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func startDebugServer(addr string, logger *slog.Logger) {
	if addr == "" {
		return
	}
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(10000)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Error("debug server stopped", "error", err)
		}
	}()
	logger.Info("debug server started", "addr", addr)
}
