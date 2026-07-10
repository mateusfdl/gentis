// Package config loads the unified gentis.yaml document that is the single
// source of truth for every server setting. It mirrors the internal/namespace
// idioms: pointer-optional YAML fields so an unset key falls back to a default,
// strict decoding so a misspelled key fails loudly, and a validation pass that
// rejects out-of-range and contradictory values at one boundary. The namespace
// section is embedded inline, so a plain namespace file stays a valid subset.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
	"go.yaml.in/yaml/v3"
)

const defaultHMACSecretEnv = "GENTIS_AUTH_HMAC_SECRET"

var (
	ErrInvalid           = errors.New("config: invalid")
	ErrAuthConflict      = errors.New("config: auth.hmac_secret and auth.disabled are mutually exclusive")
	ErrAuthNotConfigured = errors.New("config: authentication not configured: set auth.hmac_secret, auth.hmac_secret_env, or auth.disabled")
	ErrTLSIncomplete     = errors.New("config: tls.cert and tls.key must be set together")
	ErrUpstreamRequired  = errors.New("config: relay.upstream.addr is required")
)

type Config struct {
	Log        Log
	Metrics    Metrics
	Engine     Engine
	GC         GC
	Auth       Auth
	WebSocket  WebSocket
	Server     Server
	Relay      Relay
	Namespaces *namespace.Registry
}

type Log struct {
	Level  slog.Level
	Format logs.Format
}

type Metrics struct {
	Enabled bool
}

type Engine struct {
	Shards          int
	FanoutThreshold int
	FanoutWorkers   int
	HistorySize     int
	HistoryTTL      time.Duration
}

type GC struct {
	Pacer      bool
	MemLimit   int64
	SpikeGOGC  int
	NormalGOGC int
}

type Auth struct {
	Secret   string
	Disabled bool
}

type TLS struct {
	Cert string
	Key  string
}

type WebSocket struct {
	Addr         string
	ReadLimit    int64
	WriteTimeout time.Duration
	SendBuffer   int
}

type Server struct {
	Addr             string
	MetricsAddr      string
	DebugAddr        string
	Arena            bool
	MaxSessions      int
	PingInterval     time.Duration
	AuthDeadline     time.Duration
	TLS              TLS
	MaxMessageSize   int
	MaxSubscriptions int
}

type Upstream struct {
	Addr      string
	AuthToken string
	TLS       bool
	CA        string
}

type Reconnect struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	MaxRetries int
}

type Relay struct {
	Addr             string
	Upstream         Upstream
	MetricsAddr      string
	Reconnect        Reconnect
	BufferSize       int
	IncomingBuffer   int
	FanoutWorkers    int
	Arena            bool
	MaxSessions      int
	PingInterval     time.Duration
	AuthDeadline     time.Duration
	TLS              TLS
	MaxMessageSize   int
	MaxSubscriptions int
}

// Defaults is the authoritative default for every setting, replacing the flag
// defaults that previously lived across the cobra commands. Load overlays the
// file on top of this.
func Defaults() Config {
	return Config{
		Log:     Log{Level: slog.LevelInfo, Format: logs.FormatText},
		Metrics: Metrics{Enabled: true},
		Engine: Engine{
			Shards:          0,
			FanoutThreshold: 100_000,
			FanoutWorkers:   4,
		},
		GC: GC{SpikeGOGC: 400, NormalGOGC: 100},
		WebSocket: WebSocket{
			ReadLimit:    65536,
			WriteTimeout: 10 * time.Second,
			SendBuffer:   256,
		},
		Server: Server{
			Addr:             "0.0.0.0:9000",
			MetricsAddr:      ":8080",
			MaxSessions:      16384,
			PingInterval:     25 * time.Second,
			AuthDeadline:     30 * time.Second,
			MaxMessageSize:   65536,
			MaxSubscriptions: 16,
		},
		Relay: Relay{
			Addr:        "127.0.0.1:9001",
			MetricsAddr: ":8081",
			Reconnect: Reconnect{
				Initial:    100 * time.Millisecond,
				Max:        30 * time.Second,
				Multiplier: 2.0,
			},
			BufferSize:       256,
			IncomingBuffer:   4096,
			FanoutWorkers:    4,
			MaxSessions:      16384,
			PingInterval:     25 * time.Second,
			AuthDeadline:     30 * time.Second,
			MaxMessageSize:   65536,
			MaxSubscriptions: 16,
		},
	}
}

// Load reads and validates the unified config document. Unknown keys and
// out-of-range or contradictory values fail loudly.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)

	var fy fileYAML
	if err := dec.Decode(&fy); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	cfg := Defaults()
	if err := fy.apply(&cfg); err != nil {
		return nil, err
	}

	if namespaceSectionPresent(fy.ConfigYAML) {
		reg, err := namespace.Build(fy.ConfigYAML)
		if err != nil {
			return nil, err
		}
		cfg.Namespaces = reg
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// RelayReady reports whether the config carries the settings a relay run
// additionally needs. It is separate from validate because an upstream address
// is meaningless for a serve run yet required for a relay run.
func (c *Config) RelayReady() error {
	if c.Relay.Upstream.Addr == "" {
		return ErrUpstreamRequired
	}
	return nil
}

func (c *Config) validate() error {
	if err := c.Engine.validate(); err != nil {
		return err
	}
	if err := c.GC.validate(); err != nil {
		return err
	}
	if err := c.Auth.validate(); err != nil {
		return err
	}
	if err := c.WebSocket.validate(); err != nil {
		return err
	}
	if err := c.Server.validate(); err != nil {
		return err
	}
	return c.Relay.validate()
}

func (e Engine) validate() error {
	if e.Shards < 0 {
		return fmt.Errorf("%w: engine.shards must be >= 0", ErrInvalid)
	}
	if e.FanoutThreshold < 0 {
		return fmt.Errorf("%w: engine.fanout_threshold must be >= 0", ErrInvalid)
	}
	if e.FanoutWorkers <= 0 {
		return fmt.Errorf("%w: engine.fanout_workers must be > 0", ErrInvalid)
	}
	if e.HistorySize < 0 {
		return fmt.Errorf("%w: engine.history_size must be >= 0", ErrInvalid)
	}
	if e.HistoryTTL < 0 {
		return fmt.Errorf("%w: engine.history_ttl must be >= 0", ErrInvalid)
	}
	if e.HistoryTTL > 0 && e.HistorySize <= 0 {
		return fmt.Errorf("%w: engine.history_ttl requires engine.history_size > 0", ErrInvalid)
	}
	return nil
}

func (g GC) validate() error {
	if g.MemLimit < 0 {
		return fmt.Errorf("%w: gc.mem_limit must be >= 0", ErrInvalid)
	}
	if g.SpikeGOGC <= 0 {
		return fmt.Errorf("%w: gc.spike_gogc must be > 0", ErrInvalid)
	}
	if g.NormalGOGC <= 0 {
		return fmt.Errorf("%w: gc.normal_gogc must be > 0", ErrInvalid)
	}
	return nil
}

func (a Auth) validate() error {
	switch {
	case a.Disabled && a.Secret != "":
		return ErrAuthConflict
	case !a.Disabled && a.Secret == "":
		return ErrAuthNotConfigured
	default:
		return nil
	}
}

func (w WebSocket) validate() error {
	if w.ReadLimit < 0 {
		return fmt.Errorf("%w: websocket.read_limit must be >= 0", ErrInvalid)
	}
	if w.WriteTimeout < 0 {
		return fmt.Errorf("%w: websocket.write_timeout must be >= 0", ErrInvalid)
	}
	if w.SendBuffer < 0 {
		return fmt.Errorf("%w: websocket.send_buffer must be >= 0", ErrInvalid)
	}
	return nil
}

func (s Server) validate() error {
	if s.MaxSessions < 0 {
		return fmt.Errorf("%w: server.max_sessions must be >= 0", ErrInvalid)
	}
	if s.PingInterval < 0 {
		return fmt.Errorf("%w: server.ping_interval must be >= 0", ErrInvalid)
	}
	if s.AuthDeadline < 0 {
		return fmt.Errorf("%w: server.auth_deadline must be >= 0", ErrInvalid)
	}
	if s.MaxMessageSize < 0 {
		return fmt.Errorf("%w: server.max_message_size must be >= 0", ErrInvalid)
	}
	if s.MaxSubscriptions < 0 {
		return fmt.Errorf("%w: server.max_subscriptions must be >= 0", ErrInvalid)
	}
	return s.TLS.validate("server")
}

func (r Relay) validate() error {
	if r.BufferSize <= 0 {
		return fmt.Errorf("%w: relay.buffer_size must be > 0", ErrInvalid)
	}
	if r.IncomingBuffer <= 0 {
		return fmt.Errorf("%w: relay.incoming_buffer must be > 0", ErrInvalid)
	}
	if r.FanoutWorkers <= 0 {
		return fmt.Errorf("%w: relay.fanout_workers must be > 0", ErrInvalid)
	}
	if r.MaxSessions < 0 {
		return fmt.Errorf("%w: relay.max_sessions must be >= 0", ErrInvalid)
	}
	if err := r.validateTransport(); err != nil {
		return err
	}
	if err := r.Reconnect.validate(); err != nil {
		return err
	}
	return r.TLS.validate("relay")
}

func (r Relay) validateTransport() error {
	if r.PingInterval < 0 {
		return fmt.Errorf("%w: relay.ping_interval must be >= 0", ErrInvalid)
	}
	if r.AuthDeadline < 0 {
		return fmt.Errorf("%w: relay.auth_deadline must be >= 0", ErrInvalid)
	}
	if r.MaxMessageSize < 0 {
		return fmt.Errorf("%w: relay.max_message_size must be >= 0", ErrInvalid)
	}
	if r.MaxSubscriptions < 0 {
		return fmt.Errorf("%w: relay.max_subscriptions must be >= 0", ErrInvalid)
	}
	return nil
}

func (rc Reconnect) validate() error {
	if rc.Initial < 0 {
		return fmt.Errorf("%w: relay.reconnect.initial must be >= 0", ErrInvalid)
	}
	if rc.Max < 0 {
		return fmt.Errorf("%w: relay.reconnect.max must be >= 0", ErrInvalid)
	}
	if rc.Multiplier <= 0 {
		return fmt.Errorf("%w: relay.reconnect.multiplier must be > 0", ErrInvalid)
	}
	if rc.MaxRetries < 0 {
		return fmt.Errorf("%w: relay.reconnect.max_retries must be >= 0", ErrInvalid)
	}
	return nil
}

func (t TLS) validate(section string) error {
	if (t.Cert == "") != (t.Key == "") {
		return fmt.Errorf("%w (%s)", ErrTLSIncomplete, section)
	}
	return nil
}
