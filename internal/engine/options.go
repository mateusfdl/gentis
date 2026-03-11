package engine

type Option func(*config)

type config struct {
	numShards       int
	enableMetrics   bool
	observer        MetricsObserver
	fanoutThreshold int 
	fanoutWorkers   int 
	gcPacer         gcPacerConfig
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
		gcPacer:         defaultGCPacerConfig(),
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

func WithFanoutThreshold(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.fanoutThreshold = n
		}
	}
}

func WithFanoutWorkers(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.fanoutWorkers = n
		}
	}
}

// WithGCPacer enables automatic GC tuning that monitors engine activity rates
// and adjusts GOGC during spikes to reduce GC CPU overhead. It also triggers
// proactive GC during idle periods to reclaim spike garbage before the next
// spike arrives. The memoryLimit (in bytes) sets a soft memory cap as a safety
// net; pass 0 to leave the memory limit unchanged.
//
// NOTE: The pacer calls debug.SetGCPercent and debug.SetMemoryLimit which are
// process-global. Only one engine per process should enable the GC pacer. If a
// second pacer is created, it will log a warning and operate as a no-op.
func WithGCPacer(memoryLimit int64) Option {
	return func(c *config) {
		c.gcPacer.enabled = true
		c.gcPacer.memoryLimit = memoryLimit
	}
}

// WithGCPacerSpikeGOGC sets the GOGC value used during detected spikes.
// Default is 400 (4x normal headroom). Higher values trade more memory for
// less GC CPU overhead during spikes.
func WithGCPacerSpikeGOGC(gogc int) Option {
	return func(c *config) {
		if gogc > 0 {
			c.gcPacer.spikeGOGC = gogc
		}
	}
}

// WithGCPacerNormalGOGC sets the GOGC value used during normal operation.
// Default is 100 (Go's default).
func WithGCPacerNormalGOGC(gogc int) Option {
	return func(c *config) {
		if gogc > 0 {
			c.gcPacer.normalGOGC = gogc
		}
	}
}
