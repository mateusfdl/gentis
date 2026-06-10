package qos

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

// pumpBatch caps how many history items one pump iteration requests, so a
// huge backlog never materializes as one giant slice.
const pumpBatch = 64

type Gate int

const (
	// SendNow: enqueue the delivery immediately.
	SendNow Gate = iota
	// Deferred: the delivery is parked (window full or out of order); the
	// pump will fetch it from history later. Not a drop.
	Deferred
)

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

	mu      sync.RWMutex
	windows map[string]*Window
	active  atomic.Bool

	poisoned atomic.Int64
	lostGaps atomic.Int64

	startOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// NewConsumer wires a consumer to its history source and its transport
// delivery function. interval is the redelivery check cadence.
func NewConsumer(rec Recoverer, deliver func(engine.Delivery) bool, interval time.Duration) *Consumer {
	return &Consumer{
		rec:      rec,
		deliver:  deliver,
		interval: interval,
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
	c.mu.Unlock()
	c.active.Store(true)

	c.startOnce.Do(func() {
		c.done = make(chan struct{})
		go c.run()
	})
}

func (c *Consumer) Unsubscribe(channel string) {
	c.mu.Lock()
	delete(c.windows, channel)
	if len(c.windows) == 0 {
		c.active.Store(false)
	}
	c.mu.Unlock()
}

// Stop terminates the redelivery loop. Idempotent.
func (c *Consumer) Stop() {
	select {
	case <-c.stop:
		return
	default:
	}
	close(c.stop)
	if c.done != nil {
		<-c.done
	}
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

// Gate decides whether a live delivery may be sent right now. Deliveries
// on channels without a window always pass through.
func (c *Consumer) Gate(d engine.Delivery) Gate {
	w := c.window(d.Channel)
	if w == nil {
		return SendNow
	}
	switch w.Admit(d.Offset, d.Epoch, len(d.Data), time.Now().UnixNano()) {
	case Admitted:
		return SendNow
	default:
		return Deferred
	}
}

// Rollback reverts a Gate admission whose transport enqueue failed.
func (c *Consumer) Rollback(channel string, offset uint64) {
	if w := c.window(channel); w != nil {
		w.Rollback(offset)
	}
}

// Confirm applies a cumulative confirm and pumps any deliveries the freed
// window now admits.
func (c *Consumer) Confirm(channel string, offset uint64) {
	w := c.window(channel)
	if w == nil {
		return
	}
	w.Confirm(offset)
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
			c.lostGaps.Add(1)
			return
		}
		if len(batch) == 0 {
			return
		}

		now := time.Now().UnixNano()
		for _, d := range batch {
			switch w.Admit(d.Offset, d.Epoch, len(d.Data), now) {
			case Admitted:
				if !c.deliver(d) {
					w.Rollback(d.Offset)
					return
				}
			case Dup:
				continue
			case Full:
				return
			}
		}
	}
}

func (c *Consumer) run() {
	defer close(c.done)

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

			now := time.Now().UnixNano()
			for _, ch := range channels {
				w := c.window(ch)
				if w == nil {
					continue
				}
				action := w.CheckRedelivery(now)
				if action.Poisoned != 0 {
					c.poisoned.Add(1)
				}
				if action.ResendFrom != 0 {
					c.pump(ch, w)
				}
			}
		}
	}
}
