package engine

import (
	"log/slog"
	"runtime"
	"time"

	"github.com/mateusfdl/gentis/internal/namespace"
)

type Option func(*config)

type config struct {
	numShards       int
	observer        MetricsObserver
	fanoutThreshold int
	fanoutWorkers   int
	gcPacer         gcPacerConfig
	logger          *slog.Logger
	history         historyConfig
	namespaces      *namespace.Registry
}

type historyConfig struct {
	size int
	ttl  time.Duration
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
	numShards := nextPowerOf2(runtime.GOMAXPROCS(0) * 4)
	if numShards < defaultNumShards {
		numShards = defaultNumShards
	}

	return &config{
		numShards:       numShards,
		fanoutThreshold: defaultFanoutThreshold,
		fanoutWorkers:   defaultFanoutWorkers,
		gcPacer:         defaultGCPacerConfig(),
	}
}

func WithShards(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.numShards = nextPowerOf2(n)
		}
	}
}

// rounds n up to the nearest power of two.
// This is required for bitmask-based shard selection (h & (n-1))
// which avoids the cost of integer division/modulo.
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

func WithObserver(obs MetricsObserver) Option {
	return func(c *config) {
		c.observer = obs
	}
}

// WithHistory enables a bounded per-channel history ring of size entries.
// A non-zero ttl additionally expires entries via a background sweep.
// History is the basis for subscribe-time recovery by offset.
func WithHistory(size int, ttl time.Duration) Option {
	return func(c *config) {
		c.history = historyConfig{size: size, ttl: ttl}
	}
}

// WithNamespaces installs a channel namespace registry. Channel creation
// resolves the namespace once and caches its settings (history, subscriber
// cap) on the Channel; publish admission is checked via CheckPublish.
// Overrides WithHistory for namespaced configuration.
func WithNamespaces(r *namespace.Registry) Option {
	return func(c *config) {
		c.namespaces = r
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		c.logger = l
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
