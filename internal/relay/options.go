package relay

import (
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Config struct {
	ListenAddr         string
	Upstream           UpstreamConfig
	BufferSize         int
	IncomingBufferSize int
	FanoutWorkers      int
	ReconnectPolicy    ReconnectPolicy
	MetricsAddr        string
	MetricsEnabled     bool
	Engine             *engine.Engine
	SessionStore       *transport.SessionStore
	Observer           *metrics.Observer

	// Arena-backed session state (linux only). Default off. When enabled,
	// session state lives in an mmap arena slot instead of on the Go heap,
	// removing per-session State objects from GC scanning and enabling a
	// flat-array SessionStore lookup path. The session ID is derived from
	// the slot index so IDs land densely in [1, MaxSessions].
	UseArena    bool
	MaxSessions int // 0 = 16384 when UseArena is set
}

type UpstreamConfig struct {
	Address   string
	AuthToken string
}

type ReconnectPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	MaxRetries   int // 0 = unlimited
}

type Option func(*Config)

func WithListenAddr(addr string) Option {
	return func(c *Config) {
		c.ListenAddr = addr
	}
}

func WithUpstream(addr, authToken string) Option {
	return func(c *Config) {
		c.Upstream = UpstreamConfig{
			Address:   addr,
			AuthToken: authToken,
		}
	}
}

func WithBufferSize(size int) Option {
	return func(c *Config) {
		c.BufferSize = size
	}
}

func WithReconnectPolicy(initial, max time.Duration, multiplier float64) Option {
	return func(c *Config) {
		c.ReconnectPolicy = ReconnectPolicy{
			InitialDelay: initial,
			MaxDelay:     max,
			Multiplier:   multiplier,
		}
	}
}

func WithMaxRetries(n int) Option {
	return func(c *Config) {
		c.ReconnectPolicy.MaxRetries = n
	}
}

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

func WithIncomingBuffer(size int) Option {
	return func(c *Config) {
		c.IncomingBufferSize = size
	}
}

func WithFanoutWorkers(n int) Option {
	return func(c *Config) {
		c.FanoutWorkers = n
	}
}

// WithArena enables mmap arena-backed session state (linux only). On non-
// linux platforms or if arena creation fails, the relay silently falls
// back to heap *client.State.
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

func defaultConfig() *Config {
	return &Config{
		ListenAddr:         "127.0.0.1:9001",
		BufferSize:         256,
		IncomingBufferSize: 4096,
		FanoutWorkers:      4,
		ReconnectPolicy: ReconnectPolicy{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			MaxRetries:   0,
		},
	}
}
