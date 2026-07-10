package config

import (
	"fmt"
	"time"
)

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
