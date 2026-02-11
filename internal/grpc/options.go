package grpc

import (
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Config struct {
	Address        string
	MetricsAddr    string
	MetricsEnabled bool
	Engine         engine.Engine
	SessionStore   *transport.SessionStore
	Observer       *metrics.Observer
}

type Option func(*Config)

func WithMetrics(addr string) Option {
	return func(c *Config) {
		c.MetricsAddr = addr
		c.MetricsEnabled = true
	}
}

func WithEngine(e engine.Engine) Option {
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

func defaultConfig(address string) *Config {
	return &Config{
		Address:        address,
		MetricsEnabled: false,
	}
}
