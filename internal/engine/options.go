package engine

type Option func(*config)

type config struct {
	numShards     int
	enableMetrics bool
	observer      MetricsObserver
}

func defaultConfig() *config {
	return &config{
		numShards:     defaultNumShards,
		enableMetrics: true,
	}
}

func WithShards(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.numShards = n
		}
	}
}

func WithMetrics(enabled bool) Option {
	return func(c *config) {
		c.enableMetrics = enabled
	}
}

func WithObserver(obs MetricsObserver) Option {
	return func(c *config) {
		c.observer = obs
	}
}
