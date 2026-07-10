package config

import (
	"fmt"
	"time"
)

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
