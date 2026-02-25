package log

import (
	"context"
	"io"
	"log/slog"
	"os"
)

type Format int

const (
	FormatText Format = iota
	FormatJSON
)

type Config struct {
	Level  slog.Level
	Format Format
	Output io.Writer
}

func New(cfg Config) *slog.Logger {
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}

	opts := &slog.HandlerOptions{
		Level: cfg.Level,
	}

	var handler slog.Handler
	switch cfg.Format {
	case FormatJSON:
		handler = slog.NewJSONHandler(cfg.Output, opts)
	default:
		handler = slog.NewTextHandler(cfg.Output, opts)
	}

	return slog.New(handler)
}

type discardHandler struct{}

func (discardHandler) Enabled(_ context.Context, _ slog.Level) bool  { return false }
func (discardHandler) Handle(_ context.Context, _ slog.Record) error { return nil }
func (d discardHandler) WithAttrs(_ []slog.Attr) slog.Handler        { return d }
func (d discardHandler) WithGroup(_ string) slog.Handler              { return d }

func Nop() *slog.Logger {
	return slog.New(discardHandler{})
}
