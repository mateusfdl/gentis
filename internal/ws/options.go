package ws

import (
	"log/slog"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Config struct {
	Address        string
	Engine         *engine.Engine
	SessionStore   *transport.SessionStore
	Logger         *slog.Logger
	Verifier       auth.Verifier
	ReadLimit      int64
	WriteTimeout   time.Duration
	SendBufferSize int

	// TLSCertFile/TLSKeyFile enable TLS on the listener when both are
	// set.
	TLSCertFile string
	TLSKeyFile  string

	// MaxMessageSize bounds the publish payload in bytes. Oversized
	// publishes are rejected with MESSAGE_TOO_LARGE.
	MaxMessageSize int

	// MaxSubscriptions caps per-session channel subscriptions; the
	// excess subscribe is rejected with SUBSCRIPTION_LIMIT. Zero means
	// unlimited.
	MaxSubscriptions int

	// PingInterval is the cadence of server-initiated protocol pings. A
	// session is reaped when nothing arrives from the client for three
	// consecutive intervals (two unanswered pings). Zero disables
	// keepalive entirely.
	PingInterval time.Duration

	// OnDeliveryLatency, when set, observes the server-side latency from
	// message enqueue to socket write for delivered channel messages. nil
	// disables the measurement.
	OnDeliveryLatency func(time.Duration)
}

type Option func(*Config)

func defaultConfig(address string) *Config {
	return &Config{
		Address:          address,
		ReadLimit:        65536,
		WriteTimeout:     10 * time.Second,
		SendBufferSize:   256,
		Verifier:         auth.InsecureVerifier{},
		PingInterval:     25 * time.Second,
		MaxMessageSize:   65536,
		MaxSubscriptions: 16,
	}
}

func WithMaxSubscriptions(n int) Option {
	return func(c *Config) {
		c.MaxSubscriptions = n
	}
}

func WithMaxMessageSize(n int) Option {
	return func(c *Config) {
		c.MaxMessageSize = n
	}
}

func WithTLS(certFile, keyFile string) Option {
	return func(c *Config) {
		c.TLSCertFile = certFile
		c.TLSKeyFile = keyFile
	}
}

func WithPingInterval(d time.Duration) Option {
	return func(c *Config) {
		c.PingInterval = d
	}
}

func WithVerifier(v auth.Verifier) Option {
	return func(c *Config) {
		c.Verifier = v
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

func WithLogger(l *slog.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

func WithReadLimit(limit int64) Option {
	return func(c *Config) {
		c.ReadLimit = limit
	}
}

func WithWriteTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.WriteTimeout = d
	}
}

func WithSendBufferSize(size int) Option {
	return func(c *Config) {
		c.SendBufferSize = size
	}
}

// WithDeliveryLatencyObserver wires a callback that records the server-side
// enqueue-to-socket-write latency for delivered channel messages. Used to
// feed the gentis_delivery_latency_seconds histogram.
func WithDeliveryLatencyObserver(fn func(time.Duration)) Option {
	return func(c *Config) {
		c.OnDeliveryLatency = fn
	}
}
