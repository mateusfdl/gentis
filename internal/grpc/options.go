package grpc

type Config struct {
	Address        string
	MetricsAddr    string
	MetricsEnabled bool
}

type Option func(*Config)

func WithMetrics(addr string) Option {
	return func(c *Config) {
		c.MetricsAddr = addr
		c.MetricsEnabled = true
	}
}

func defaultConfig(address string) *Config {
	return &Config{
		Address:        address,
		MetricsEnabled: false,
	}
}
