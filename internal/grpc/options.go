package grpc

import (
	"log/slog"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
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
	Verifier       auth.Verifier

	// TLSCertFile/TLSKeyFile enable TLS on the listener when both are
	// set.
	TLSCertFile string
	TLSKeyFile  string

	// PingInterval drives HTTP/2 transport keepalive: the server pings an
	// idle connection every interval and closes it when the ack doesn't
	// arrive within two more. Zero disables keepalive.
	PingInterval time.Duration

	// Arena-backed session state (linux only). Default off. When enabled,
	// session state lives in an mmap arena slot instead of on the Go heap,
	// removing per-session State objects from GC scanning and enabling a
	// flat-array SessionStore lookup path. The session ID is derived from
	// the slot index so IDs land densely in [1, MaxSessions].
	UseArena    bool
	MaxSessions int // 0 = 16384 when UseArena is set

	// ExtraConnCounters are additional sources whose ConnectionCount() is
	// folded into this server's `gentis_connections_active` gauge. Used
	// to surface the WS server's session count alongside gRPC. Only the
	// active gauge is summed — `gentis_connections_total` /
	// `gentis_disconnections_total` continue to come from this server's
	// own counters, so churn metrics remain consistent with their
	// previous semantics.
	ExtraConnCounters []ActiveConnCounter
}

// ActiveConnCounter is the minimal interface needed by
// WithExtraConnectionCounter — just the live-session gauge. Implemented
// trivially by the WebSocket server (and anything else that exposes a
// ConnectionCount() method).
type ActiveConnCounter interface {
	ConnectionCount() int64
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

// WithExtraConnectionCounter folds an additional ConnectionCount() source
// (e.g. the websocket server's session counter) into the
// `gentis_connections_active` gauge. multiple calls accumulate. without
// this, a server only reports its own gRPC connection count, which reads
// zero during ws-only traffic.
func WithExtraConnectionCounter(c ActiveConnCounter) Option {
	return func(cfg *Config) {
		cfg.ExtraConnCounters = append(cfg.ExtraConnCounters, c)
	}
}

func defaultConfig(address string) *Config {
	return &Config{
		Address:        address,
		MetricsEnabled: false,
		Verifier:       auth.InsecureVerifier{},
		PingInterval:   25 * time.Second,
	}
}
