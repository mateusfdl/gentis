package qos

import (
	"sync"
	"time"
)

// Sweeper drives the redelivery checks of every registered consumer from
// one goroutine, so a fleet of QoS sessions costs one timer instead of a
// goroutine and a ticker per session. Consumers register themselves on
// their first window and deregister on their last, so sessions that never
// request at-least-once delivery are never swept at all.
type Sweeper struct {
	interval time.Duration

	mu        sync.Mutex
	consumers map[*Consumer]struct{}
	started   bool
	stopped   bool
	stop      chan struct{}
	wg        sync.WaitGroup
}

func NewSweeper(interval time.Duration) *Sweeper {
	return &Sweeper{
		interval:  interval,
		consumers: make(map[*Consumer]struct{}),
		stop:      make(chan struct{}),
	}
}

// add registers a consumer for sweeping and lazily starts the sweep loop
// on first use. Registration after Stop is refused so a late Subscribe
// cannot resurrect a drained sweeper.
func (s *Sweeper) add(c *Consumer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.consumers[c] = struct{}{}
	if !s.started {
		s.started = true
		s.wg.Add(1)
		go s.run()
	}
}

func (s *Sweeper) remove(c *Consumer) {
	s.mu.Lock()
	delete(s.consumers, c)
	s.mu.Unlock()
}

func (s *Sweeper) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.consumers)
}

// appendConsumers snapshots the registered consumers into buf so the sweep
// loop iterates without holding the lock: a Tick can block on the
// transport and must not stall registration.
func (s *Sweeper) appendConsumers(buf []*Consumer) []*Consumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.consumers {
		buf = append(buf, c)
	}
	return buf
}

func (s *Sweeper) run() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	var buf []*Consumer
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			buf = s.appendConsumers(buf[:0])
			for _, c := range buf {
				c.Tick()
			}
		}
	}
}

// Stop terminates the sweep loop and joins it before returning. Safe to
// call concurrently and more than once. Consumers are fenced individually
// by their own Stop; the sweeper only guarantees its loop is gone.
func (s *Sweeper) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.stopped = true
	if s.started {
		close(s.stop)
	}
	s.mu.Unlock()
	s.wg.Wait()
}
