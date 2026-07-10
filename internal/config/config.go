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

// Default returns the built-in configuration used when no file is supplied.
// The auth secret is still resolved from the default environment variable so a
// fileless run can authenticate.
func Default() (*Config, error) {
	cfg := Defaults()
	if err := applyAuth(nil, &cfg.Auth); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
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
