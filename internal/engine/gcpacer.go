package engine

import (
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// gcPacerActive guards against multiple gcPacer instances per process.
// SetGCPercent and SetMemoryLimit are process-global, so only one pacer
// should be active at a time.
var gcPacerActive atomic.Bool

// gcPacer monitors engine activity rates and adjusts GC tuning parameters
// to optimize for spike workloads:
//
//   - During spikes (subscribe/publish rate > 2x baseline), it raises GOGC to
//     reduce GC frequency, trading temporary memory for CPU stability.
//   - Hysteresis prevents oscillation: spike mode is entered at spikeMultiple
//     (default 2.0x) but only exited when the ratio drops below
//     spikeExitRatio (default 0.7 × spikeMultiple = 1.4x).
//   - During idle periods (rate < 50% baseline for sustained duration), it
//     triggers an explicit GC + FreeOSMemory to reclaim spike garbage before
//     the next spike arrives.
//   - An optional memory limit acts as a safety net to prevent OOM even with
//     elevated GOGC.
type gcPacer struct {
	engine *Engine
	done   chan struct{}
	wg     sync.WaitGroup

	sampleInterval time.Duration
	spikeMultiple  float64
	spikeExitRatio float64 // rate / baseline to exit spike mode (< spikeMultiple for hysteresis)
	idleRatio      float64
	idleDuration   time.Duration
	spikeGOGC      int
	normalGOGC     int
	memoryLimit    int64

	// prevGOGC and prevMemoryLimit hold the process-global runtime settings
	// captured at construction so Stop can restore them. SetGCPercent and
	// SetMemoryLimit are process-global; leaving them changed would leak the
	// pacer's tuning into the rest of the process. memoryLimitSet records
	// whether the pacer actually changed the limit (only when configured).
	prevGOGC        int
	prevMemoryLimit int64
	memoryLimitSet  bool

	inSpike    atomic.Bool
	lastGCTime atomic.Int64
}

type gcPacerConfig struct {
	enabled       bool
	spikeMultiple float64
	idleRatio     float64
	idleDuration  time.Duration
	spikeGOGC     int
	normalGOGC    int
	memoryLimit   int64
}

func defaultGCPacerConfig() gcPacerConfig {
	return gcPacerConfig{
		enabled:       false,
		spikeMultiple: 2.0,
		idleRatio:     0.5,
		idleDuration:  5 * time.Second,
		spikeGOGC:     400,
		normalGOGC:    100,
		memoryLimit:   0,
	}
}

// spikeExitFactor is the fraction of spikeMultiple used as the exit threshold.
// This creates a hysteresis band: spike mode is entered at spikeMultiple but
// only exited at spikeExitFactor × spikeMultiple (e.g., enter at 2.0, exit at 1.4).
const spikeExitFactor = 0.7

// currentGCPercent reads the process GOGC setting without leaving it changed.
// debug has no getter, so this round-trips through SetGCPercent: the -1 call
// returns the current value (and disables GC for the nanoseconds until the
// second call restores it).
func currentGCPercent() int {
	prev := debug.SetGCPercent(-1)
	debug.SetGCPercent(prev)
	return prev
}

func newGCPacer(e *Engine, cfg gcPacerConfig) *gcPacer {
	if !gcPacerActive.CompareAndSwap(false, true) {
		e.logger.Warn("gc pacer already active, returning no-op pacer; " +
			"SetGCPercent/SetMemoryLimit are process-global so only one " +
			"pacer should run per process")
		return &gcPacer{done: make(chan struct{})}
	}

	p := &gcPacer{
		engine:         e,
		done:           make(chan struct{}),
		sampleInterval: 1 * time.Second,
		spikeMultiple:  cfg.spikeMultiple,
		spikeExitRatio: cfg.spikeMultiple * spikeExitFactor,
		idleRatio:      cfg.idleRatio,
		idleDuration:   cfg.idleDuration,
		spikeGOGC:      cfg.spikeGOGC,
		normalGOGC:     cfg.normalGOGC,
		memoryLimit:    cfg.memoryLimit,
		prevGOGC:       currentGCPercent(),
	}
	if cfg.memoryLimit > 0 {
		p.prevMemoryLimit = debug.SetMemoryLimit(cfg.memoryLimit)
		p.memoryLimitSet = true
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run()
	}()
	return p
}

func (p *gcPacer) Stop() {
	close(p.done)
	p.wg.Wait()
	// run() has returned, so nothing races these writes. Restore the
	// process-global runtime settings the pacer changed, then release the
	// guard so another pacer can be created (e.g., in tests that create and
	// stop multiple engines sequentially). A no-op pacer (engine == nil)
	// never changed anything and must not restore.
	if p.engine == nil {
		return
	}
	debug.SetGCPercent(p.prevGOGC)
	if p.memoryLimitSet {
		debug.SetMemoryLimit(p.prevMemoryLimit)
	}
	gcPacerActive.Store(false)
}

// pacerState holds the mutable state for the EMA-based sampling loop.
// Extracted from run() so tests can call sample() directly without timers.
type pacerState struct {
	baseline    float64
	prevOps     int64
	idleSince   time.Time
	initialized bool
}

// EMA smoothing factor: lower = more stable baseline.
const emaAlpha = 0.1

// sample reads the current ops counter, updates the EMA baseline, and
// adjusts GC parameters based on the ratio. Returns true if a proactive
// GC was triggered (for testing).
func (p *gcPacer) sample(st *pacerState) bool {
	var publishOps int64
	for i := range p.engine.shards {
		publishOps += p.engine.shards[i].publishCount.Load()
	}
	currentOps := p.engine.subscribeOps.Load() + publishOps

	if !st.initialized {
		st.prevOps = currentOps
		st.initialized = true
		return false
	}

	delta := float64(currentOps - st.prevOps)
	st.prevOps = currentOps

	if st.baseline == 0 {
		st.baseline = delta
	} else {
		st.baseline = emaAlpha*delta + (1-emaAlpha)*st.baseline
	}

	// Avoid division by zero with a minimum baseline
	if st.baseline < 10 {
		st.baseline = 10
	}

	ratio := delta / st.baseline

	switch {
	case ratio >= p.spikeMultiple && !p.inSpike.Load():
		// entering spike mode: widen GC headroom
		debug.SetGCPercent(p.spikeGOGC)
		p.inSpike.Store(true)
		st.idleSince = time.Time{} // reset idle timer

	case ratio < p.spikeExitRatio && p.inSpike.Load():
		// exiting spike mode: restore normal GC
		// Uses spikeExitRatio (< spikeMultiple) for hysteresis to prevent
		// oscillation when the rate hovers near the spike threshold.
		debug.SetGCPercent(p.normalGOGC)
		p.inSpike.Store(false)
		st.idleSince = time.Time{}

	case ratio < p.idleRatio:
		// possibly idle: track how long
		if st.idleSince.IsZero() {
			st.idleSince = time.Now()
		} else if time.Since(st.idleSince) >= p.idleDuration {
			// sustained idle: proactive GC to reclaim spike garbage
			lastGC := p.lastGCTime.Load()
			now := time.Now().UnixNano()
			// don't GC more than once per idle duration
			if now-lastGC > p.idleDuration.Nanoseconds() {
				runtime.GC()
				debug.FreeOSMemory()
				p.lastGCTime.Store(now)
			}
			st.idleSince = time.Time{}
			return true
		}

	default:
		st.idleSince = time.Time{}
	}
	return false
}

func (p *gcPacer) run() {
	ticker := time.NewTicker(p.sampleInterval)
	defer ticker.Stop()

	var st pacerState

	for {
		select {
		case <-p.done:
			// Stop restores the process-global GC settings after this
			// goroutine returns, so no restoration races here.
			return
		case <-ticker.C:
		}

		p.sample(&st)
	}
}
