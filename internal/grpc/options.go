package grpc

import (
	"log/slog"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Config struct {
	Address        string
	MetricsAddr    string
	MetricsEnabled bool
	Engine         *engine.Engine
	SessionStore   *transport.SessionStore
	Observer       *metrics.Observer
	Logger         *slog.Logger

	// Arena-backed session state (linux only). Default off. When enabled,
	// session state lives in an mmap arena slot instead of on the Go heap,
	// removing per-session State objects from GC scanning and enabling a
	// flat-array SessionStore lookup path. The session ID is derived from
	// the slot index so IDs land densely in [1, MaxSessions].
	UseArena    bool
	MaxSessions int // 0 = 16384 when UseArena is set
}

type Option func(*Config)

func WithMetrics(addr string) Option {
	return func(c *Config) {
		c.MetricsAddr = addr
		c.MetricsEnabled = true
	}
}

func WithEngine(e *engine.Engine) Option {
	return func(c *Config) {
		c.Engine = e
	}
}

func WithSessionStore(store *transport.SessionStore) Option {
	return func(c *Config) {
		c.SessionStore = store
	}
}

func WithObserver(obs *metrics.Observer) Option {
	return func(c *Config) {
		c.Observer = obs
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

func WithArena() Option {
	return func(c *Config) {
		c.UseArena = true
	}
}

// WithMaxSessions sets the arena session capacity. Only meaningful when
// WithArena() is also set. Default 16384 (~70 MB mmap reserve).
func WithMaxSessions(n int) Option {
	return func(c *Config) {
		c.MaxSessions = n
	}
}

func defaultConfig(address string) *Config {
	return &Config{
		Address:        address,
		MetricsEnabled: false,
	}
}
