package qos

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

// pumpBatch caps how many history items one pump iteration requests, so a
// huge backlog never materializes as one giant slice.
const pumpBatch = 64

// processStart anchors the monotonic clock the redelivery timer reads.
// time.Since keeps the monotonic reading time.Now().UnixNano() would strip,
// so a wall-clock step from NTP can neither stall nor storm redelivery.
var processStart = time.Now()

func monotonicNanos() int64 {
	return int64(time.Since(processStart))
}

// Recoverer is the slice of the engine the pump depends on.
type Recoverer interface {
	RecoverN(channel string, fromOffset, epoch uint64, max int) ([]engine.Delivery, bool)
}

// Consumer manages the at-least-once windows of one transport session.
// Sessions without QoS1 subscriptions pay one atomic load per delivery.
type Consumer struct {
	rec     Recoverer
	deliver func(engine.Delivery) bool
	sweeper *Sweeper
	logger  *slog.Logger
	now     func() int64

	mu      sync.RWMutex
	windows map[string]*Window
	stopped bool
	active  atomic.Bool

	// tickMu serializes Tick bodies and gives Stop its join: taking it
	// after setting stopped proves no tick is mid-delivery.
	tickMu sync.Mutex

	poisoned atomic.Int64
	lostGaps atomic.Int64
}

// NewConsumer wires a consumer to its history source and its transport
// delivery function. The sweeper drives redelivery ticks; nil opts out of
// background redelivery (callers then drive Tick themselves). A nil
// logger falls back to slog.Default.
func NewConsumer(rec Recoverer, deliver func(engine.Delivery) bool, sweeper *Sweeper, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		rec:     rec,
		deliver: deliver,
		sweeper: sweeper,
		logger:  logger,
		now:     monotonicNanos,
	}
}

// Subscribe registers an at-least-once window for a channel and joins the
// sweeper's tick set on first use. It never replaces an active window: a
// duplicate subscribe reports false and the existing window, with its
// inflight and confirm state, stays authoritative.
func (c *Consumer) Subscribe(channel string, w *Window) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.windows[channel] != nil {
		return false
	}
	if c.windows == nil {
		c.windows = make(map[string]*Window)
	}
	c.windows[channel] = w
	if !c.stopped && len(c.windows) == 1 && c.sweeper != nil {
		c.sweeper.add(c)
	}
	c.active.Store(true)
	return true
}

func (c *Consumer) Unsubscribe(channel string) {
	c.mu.Lock()
	delete(c.windows, channel)
	if len(c.windows) == 0 {
		c.active.Store(false)
		if c.sweeper != nil {
			c.sweeper.remove(c)
		}
	}
	c.mu.Unlock()
}

// Stop deregisters the consumer from its sweeper and joins any in-flight
// tick before returning, so no tick-driven pump can touch the transport
// afterward. Safe to call concurrently and more than once.
func (c *Consumer) Stop() {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		if c.sweeper != nil {
			c.sweeper.remove(c)
		}
	}
	c.mu.Unlock()

	c.tickMu.Lock()
	defer c.tickMu.Unlock()
}

func (c *Consumer) window(channel string) *Window {
	if !c.active.Load() {
		return nil
	}
	c.mu.RLock()
	w := c.windows[channel]
	c.mu.RUnlock()
	return w
}

// Deliver gates and enqueues a live delivery as one atomic step. Channels
// without a window pass straight to the transport. Returns false only when
// an admitted delivery could not be enqueued; deferred and duplicate
// deliveries report true because the pump owns them.
func (c *Consumer) Deliver(d engine.Delivery) bool {
	w := c.window(d.Channel)
	if w == nil {
		return c.deliver(d)
	}
	v := w.Admit(d.Offset, d.Epoch, len(d.Data), c.now(), func() bool {
		return c.deliver(d)
	})
	return v != Refused
}

// Confirm applies a cumulative confirm and pumps any deliveries the freed
// window now admits.
func (c *Consumer) Confirm(channel string, offset uint64) {
	w := c.window(channel)
	if w == nil {
		return
	}
	w.Confirm(offset, c.now())
	c.pump(channel, w)
}

func (c *Consumer) pump(channel string, w *Window) {
	for {
		from, epoch, room := w.PumpPoint()
		if room <= 0 || epoch == 0 {
			return
		}
		if room > pumpBatch {
			room = pumpBatch
		}

		batch, ok := c.rec.RecoverN(channel, from, epoch, room)
		if !ok {
			// History evicted the range; the engine contract says treat
			// this as a full resync. Re-baseline on the next live delivery
			// instead of retrying the same lost gap forever.
			c.lostGaps.Add(1)
			w.Reset()
			c.logger.Warn("qos gap unrecoverable, window re-baselined", "channel", channel, "from_offset", from)
			return
		}
		if len(batch) == 0 {
			return
		}

		now := c.now()
		for _, d := range batch {
			switch w.Admit(d.Offset, d.Epoch, len(d.Data), now, func() bool { return c.deliver(d) }) {
			case Admitted, Dup:
				continue
			default:
				return
			}
		}
	}
}

// Tick runs one redelivery pass over every window: timed-out heads are
// rescheduled and the pump resumes whatever the credit window admits.
// Normally driven by the shared sweeper; inert once the consumer stopped.
func (c *Consumer) Tick() {
	c.tickMu.Lock()
	defer c.tickMu.Unlock()

	if !c.active.Load() {
		return
	}
	c.mu.RLock()
	if c.stopped {
		c.mu.RUnlock()
		return
	}
	channels := make([]string, 0, len(c.windows))
	for ch := range c.windows {
		channels = append(channels, ch)
	}
	c.mu.RUnlock()

	now := c.now()
	for _, ch := range channels {
		w := c.window(ch)
		if w == nil {
			continue
		}
		action := w.CheckRedelivery(now)
		if action.Poisoned != 0 {
			c.poisoned.Add(1)
			c.logger.Warn("qos delivery poisoned after exhausting redeliveries", "channel", ch, "offset", action.Poisoned)
		}
		// Pump unconditionally: a refused enqueue with an empty window
		// leaves nothing inflight to time out, so the tick is the only
		// thing that can resume delivery.
		c.pump(ch, w)
	}
}
