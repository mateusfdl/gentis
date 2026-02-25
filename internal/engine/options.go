package engine

type Option func(*config)

type config struct {
	numShards       int
	enableMetrics   bool
	observer        MetricsObserver
	fanoutThreshold int // min subscribers before parallel fan-out kicks in
	fanoutWorkers   int // number of parallel workers for fan-out
}

const (
	// defaultFanoutThreshold is set high to keep sequential fan-out as default.
	// The built-in delivery callback (sync.Map lookup + non-blocking channel send)
	// is fast enough that parallel fan-out overhead exceeds its benefit. Lower this
	// threshold when using heavier delivery callbacks (e.g., with encryption,
	// compression, or synchronous I/O per subscriber).
	defaultFanoutThreshold = 100_000
	defaultFanoutWorkers   = 4
)

func defaultConfig() *config {
	return &config{
		numShards:       defaultNumShards,
		enableMetrics:   true,
		fanoutThreshold: defaultFanoutThreshold,
		fanoutWorkers:   defaultFanoutWorkers,
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

// WithFanoutThreshold sets the minimum number of subscribers on a channel
// before publish switches to parallel fan-out. Below this threshold, fan-out
// is performed sequentially on the publisher's goroutine (lower latency for
// small channels). Set to 0 to always use parallel fan-out, or a very large
// value to disable it entirely.
func WithFanoutThreshold(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.fanoutThreshold = n
		}
	}
}

// WithFanoutWorkers sets the number of parallel goroutines used for fan-out
// when the subscriber count exceeds the fan-out threshold.
func WithFanoutWorkers(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.fanoutWorkers = n
		}
	}
}
