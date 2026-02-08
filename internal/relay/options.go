package relay

import "time"

type Config struct {
	ListenAddr      string
	Upstream        UpstreamConfig
	BufferSize      int
	ReconnectPolicy ReconnectPolicy
	MetricsAddr     string
	MetricsEnabled  bool
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

func defaultConfig() *Config {
	return &Config{
		ListenAddr: "127.0.0.1:9001",
		BufferSize: 256,
		ReconnectPolicy: ReconnectPolicy{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			MaxRetries:   0,
		},
	}
}
