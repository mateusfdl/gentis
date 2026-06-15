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
	rec      Recoverer
	deliver  func(engine.Delivery) bool
	interval time.Duration
	logger   *slog.Logger
	now      func() int64

	mu      sync.RWMutex
	windows map[string]*Window
	active  atomic.Bool

	poisoned atomic.Int64
	lostGaps atomic.Int64

	running bool
	stopped bool
	wg      sync.WaitGroup
	stop    chan struct{}
}

// NewConsumer wires a consumer to its history source and its transport
// delivery function. interval is the redelivery check cadence. A nil
// logger falls back to slog.Default.
func NewConsumer(rec Recoverer, deliver func(engine.Delivery) bool, interval time.Duration, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		rec:      rec,
		deliver:  deliver,
		interval: interval,
		logger:   logger,
		now:      monotonicNanos,
		stop:     make(chan struct{}),
	}
}

// Subscribe registers an at-least-once window for a channel and starts
// the redelivery loop on first use.
func (c *Consumer) Subscribe(channel string, w *Window) {
	c.mu.Lock()
	if c.windows == nil {
		c.windows = make(map[string]*Window)
	}
	c.windows[channel] = w
	if !c.stopped && !c.running {
		c.running = true
		c.wg.Add(1)
		go c.run()
	}
	c.mu.Unlock()
	c.active.Store(true)
}

func (c *Consumer) Unsubscribe(channel string) {
	c.mu.Lock()
	delete(c.windows, channel)
	if len(c.windows) == 0 {
		c.active.Store(false)
	}
	c.mu.Unlock()
}

// Stop terminates the redelivery loop and joins it before returning, so no
// pump can touch the transport afterward. Serializing against Subscribe under
// the same lock closes the start/stop race: a Subscribe that loses the race
// observes stopped and never launches the loop. Safe to call concurrently and
// more than once.
func (c *Consumer) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		c.wg.Wait()
		return
	}
	c.stopped = true
	close(c.stop)
	c.mu.Unlock()
	c.wg.Wait()
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

// Poisoned counts deliveries dropped after exhausting their redelivery
// budget.
func (c *Consumer) Poisoned() int64 {
	return c.poisoned.Load()
}

// LostGaps counts catch-up attempts that failed because history had
// already evicted the needed range.
func (c *Consumer) LostGaps() int64 {
	return c.lostGaps.Load()
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

func (c *Consumer) run() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.mu.RLock()
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
				// Pump unconditionally: a refused enqueue with an empty
				// window leaves nothing inflight to time out, so the tick
				// is the only thing that can resume delivery.
				c.pump(ch, w)
			}
		}
	}
}
