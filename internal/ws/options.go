package ws

import (
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

type Config struct {
	Address        string
	Engine         *engine.Engine
	SessionStore   *transport.SessionStore
	ReadLimit      int64
	WriteTimeout   time.Duration
	SendBufferSize int

	// OnDeliveryLatency, when set, observes the server-side latency from
	// message enqueue to socket write for delivered channel messages. nil
	// disables the measurement.
	OnDeliveryLatency func(time.Duration)
}

type Option func(*Config)

func defaultConfig(address string) *Config {
	return &Config{
		Address:        address,
		ReadLimit:      65536,
		WriteTimeout:   10 * time.Second,
		SendBufferSize: 256,
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
