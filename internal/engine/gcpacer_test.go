package engine

import (
	"runtime/debug"
	"testing"
	"time"
)

// newTestEngine creates a minimal Engine with controllable shard counters
// for the gcPacer to sample. Uses 1 shard to simplify testing.
func newTestEngine() *Engine {
	shards := make([]Shard, 1)
	shards[0].channels = make(map[string]*Channel)
	return &Engine{
		config:        defaultConfig(),
		shards:        shards,
		subscriptions: newSubscriptions(),
	}
}

// newTestPacer creates a gcPacer suitable for unit testing. It does NOT
// start the background goroutine — callers invoke p.sample() directly.
func newTestPacer(e *Engine) *gcPacer {
	return &gcPacer{
		engine:         e,
		done:           make(chan struct{}),
		sampleInterval: time.Second, // unused since we call sample() directly
		spikeMultiple:  2.0,
		spikeExitRatio: 2.0 * spikeExitFactor, // 1.4
		idleRatio:      0.5,
		idleDuration:   5 * time.Second,
		spikeGOGC:      400,
		normalGOGC:     100,
	}
}

// feedSamples calls p.sample() n times, adding opsPerTick to the engine's
// publish counter before each call. This builds a deterministic EMA baseline.
func feedSamples(p *gcPacer, e *Engine, st *pacerState, n int, opsPerTick int64) {
	for i := 0; i < n; i++ {
		e.shards[0].publishCount.Add(opsPerTick)
		p.sample(st)
	}
}

func TestGCPacerSpikeEntry(t *testing.T) {
	e := newTestEngine()
	p := newTestPacer(e)
	var st pacerState

	// Initialize the sampler.
	p.sample(&st) // sets initialized = true

	// Build a stable baseline with consistent ops.
	feedSamples(p, e, &st, 20, 100)

	if p.inSpike.Load() {
		t.Fatal("should not be in spike mode with steady-state ops")
	}

	// Now inject a large burst: 10x the baseline should easily exceed 2.0x.
	e.shards[0].publishCount.Add(10_000)
	p.sample(&st)

	if !p.inSpike.Load() {
		t.Error("expected pacer to enter spike mode after large op burst")
	}
}

func TestGCPacerSpikeExitHysteresis(t *testing.T) {
	// Verify that the pacer stays in spike mode when ratio is between
	// spikeExitRatio (1.4) and spikeMultiple (2.0).
	e := newTestEngine()
	p := newTestPacer(e)
	var st pacerState

	// Initialize and build baseline.
	p.sample(&st)
	feedSamples(p, e, &st, 30, 100)

	// Force spike entry.
	e.shards[0].publishCount.Add(10_000)
	p.sample(&st)
	if !p.inSpike.Load() {
		t.Fatal("precondition: expected spike mode")
	}

	p2 := newTestPacer(e)
	p2.inSpike.Store(true)
	st2 := pacerState{
		baseline:    100,
		prevOps:     e.shards[0].publishCount.Load(),
		initialized: true,
	}

	// Feed exactly 170 ops: ratio = 170/100 = 1.7 (between 1.4 and 2.0)
	e.shards[0].publishCount.Add(170)
	p2.sample(&st2)

	if !p2.inSpike.Load() {
		t.Error("expected pacer to remain in spike mode at ratio 1.7 (within hysteresis band)")
	}
}

func TestGCPacerSpikeExitBelowHysteresis(t *testing.T) {
	e := newTestEngine()
	p := newTestPacer(e)

	// Set up a known state: in spike, baseline = 100.
	p.inSpike.Store(true)
	st := pacerState{
		baseline:    100,
		prevOps:     e.shards[0].publishCount.Load(),
		initialized: true,
	}

	// Feed 50 ops: ratio = 50/100 = 0.5 (well below 1.4)
	e.shards[0].publishCount.Add(50)
	p.sample(&st)

	if p.inSpike.Load() {
		t.Error("expected pacer to exit spike mode when ratio drops below spikeExitRatio")
	}
}

func TestGCPacerNoOscillationAtBoundary(t *testing.T) {
	// Key test: ratio oscillates between 1.5 and 2.5 around the spike entry
	// threshold (2.0). Without hysteresis, GOGC would toggle every sample.
	// With hysteresis (exit at 1.4), once in spike mode we stay there.
	e := newTestEngine()
	p := newTestPacer(e)

	st := pacerState{
		baseline:    100,
		prevOps:     e.shards[0].publishCount.Load(),
		initialized: true,
	}

	// Enter spike: ratio = 250/100 = 2.5
	e.shards[0].publishCount.Add(250)
	p.sample(&st)
	if !p.inSpike.Load() {
		t.Fatal("expected spike entry at ratio 2.5")
	}

	transitions := 0
	wasInSpike := true

	// Now oscillate: alternate between ratio ~1.5 and ~2.5
	for i := 0; i < 20; i++ {
		// Recalibrate: set known prevOps and baseline for deterministic ratios
		st.prevOps = e.shards[0].publishCount.Load()
		st.baseline = 100

		if i%2 == 0 {
			// ratio = 150/100 = 1.5 (between exit 1.4 and entry 2.0)
			e.shards[0].publishCount.Add(150)
		} else {
			// ratio = 250/100 = 2.5 (above entry 2.0)
			e.shards[0].publishCount.Add(250)
		}
		p.sample(&st)

		inSpike := p.inSpike.Load()
		if inSpike != wasInSpike {
			transitions++
			wasInSpike = inSpike
		}
	}

	if transitions > 0 {
		t.Errorf("expected zero spike/normal transitions with oscillating ratio 1.5-2.5, got %d", transitions)
	}
}

func TestGCPacerStopRestoresGOGC(t *testing.T) {
	e := newTestEngine()

	// Ensure the global guard is clear.
	gcPacerActive.Store(false)

	p := &gcPacer{
		engine:         e,
		done:           make(chan struct{}),
		sampleInterval: 10 * time.Millisecond,
		spikeMultiple:  2.0,
		spikeExitRatio: 2.0 * spikeExitFactor,
		idleRatio:      0.5,
		idleDuration:   1 * time.Hour,
		spikeGOGC:      400,
		normalGOGC:     100,
	}

	// Force into spike mode — Stop() should restore normalGOGC.
	p.inSpike.Store(true)
	debug.SetGCPercent(400)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run()
	}()

	time.Sleep(20 * time.Millisecond)
	p.Stop()

	// SetGCPercent returns the previous value, which should be normalGOGC (100).
	prev := debug.SetGCPercent(100)
	if prev != 100 {
		t.Errorf("expected GOGC restored to 100 after Stop(), got %d", prev)
	}
}

func TestGCPacerDuplicateGuard(t *testing.T) {
	// Ensure the global guard is clear.
	gcPacerActive.Store(false)

	e := newTestEngine()
	cfg := defaultGCPacerConfig()
	cfg.enabled = true

	p1 := newGCPacer(e, cfg)
	defer p1.Stop()

	// Second pacer should be a no-op (engine == nil).
	p2 := newGCPacer(e, cfg)
	if p2.engine != nil {
		t.Error("expected second gcPacer to be a no-op (nil engine)")
	}

	// no-op pacer Stop() should not panic
	p2.Stop()
}

func TestGCPacerSpikeExitFactor(t *testing.T) {
	if spikeExitFactor != 0.7 {
		t.Errorf("expected spikeExitFactor = 0.7, got %f", spikeExitFactor)
	}

	cfg := defaultGCPacerConfig()
	exitRatio := cfg.spikeMultiple * spikeExitFactor
	if exitRatio != 1.4 {
		t.Errorf("expected default spikeExitRatio = 1.4, got %f", exitRatio)
	}
}
