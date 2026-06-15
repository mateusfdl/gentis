package qos

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

type sink struct {
	mu         sync.Mutex
	deliveries []engine.Delivery
	reject     bool
	failEvery  int
	attempts   int
}

func (s *sink) deliver(d engine.Delivery) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reject {
		return false
	}
	s.attempts++
	if s.failEvery > 0 && s.attempts%s.failEvery == 0 {
		return false
	}
	s.deliveries = append(s.deliveries, d)
	return true
}

func (s *sink) setReject(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reject = v
}

func (s *sink) offsets() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, len(s.deliveries))
	for i, d := range s.deliveries {
		out[i] = d.Offset
	}
	return out
}

func newQoSEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e := engine.New(engine.WithHistory(64, 0))
	t.Cleanup(e.Stop)
	return e
}

func TestSlowConsumerNeverDrops(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 10*time.Millisecond, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(2, 0, time.Minute, 2))

	for i := 0; i < 10; i++ {
		r := e.Publish("q", []byte{byte(i)}, 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
			c.Deliver(d)
			return true
		})
		if r.Offset != uint64(i+1) {
			t.Fatalf("publish %d offset = %d", i, r.Offset)
		}
	}

	if got := s.offsets(); len(got) != 2 {
		t.Fatalf("window 2: delivered %v, want first 2 only", got)
	}

	for confirmed := uint64(1); confirmed <= 10; confirmed++ {
		c.Confirm("q", confirmed)
	}

	got := s.offsets()
	if len(got) != 10 {
		t.Fatalf("delivered %d offsets %v, want all 10", len(got), got)
	}
	for i, off := range got {
		if off != uint64(i+1) {
			t.Fatalf("delivery %d = offset %d, want %d (ordered, no gaps)", i, off, i+1)
		}
	}
}

func TestRedeliveryAfterTimeout(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(5, 0, 20*time.Millisecond, 3))

	e.Publish("q", []byte("x"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		c.Deliver(d)
		return true
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.offsets()) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got := s.offsets()
	if len(got) < 2 || got[0] != 1 || got[1] != 1 {
		t.Fatalf("offsets = %v, want offset 1 redelivered", got)
	}
}

func TestRedeliveryDrivenByMonotonicClockNotWallClock(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond, nil)
	clock := int64(1000)
	c.now = func() int64 { return atomic.LoadInt64(&clock) }
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(5, 0, time.Millisecond, 3))

	deliver := func(_ engine.SubscriberID, d engine.Delivery) bool { return c.Deliver(d) }
	e.Publish("q", []byte("x"), 0, deliver)

	time.Sleep(120 * time.Millisecond)
	if got := s.offsets(); len(got) != 1 {
		t.Fatalf("offsets = %v, want exactly one delivery: a frozen clock must not trip the timeout despite real time passing", got)
	}

	atomic.StoreInt64(&clock, 1000+int64(time.Second))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.offsets()) >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("offsets = %v, want offset 1 redelivered once the injected clock advances past the timeout", s.offsets())
}

func TestPoisonAfterMaxRedeliveries(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(5, 0, 10*time.Millisecond, 1))

	e.Publish("q", []byte("bad"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		c.Deliver(d)
		return true
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Poisoned() == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Poisoned = %d, want 1", c.Poisoned())
}

func TestUnsubscribeRemovesWindow(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, time.Hour, nil)
	defer c.Stop()

	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))
	c.Unsubscribe("q")

	d := engine.Delivery{Channel: "q", Data: []byte("x"), Offset: 1, Epoch: 7}
	if !c.Deliver(d) {
		t.Fatal("Deliver after Unsubscribe must pass through")
	}
	if got := s.offsets(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("offsets = %v, want [1]", got)
	}
}

func TestIdleTickerDoesNoWorkAfterUnsubscribe(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond, nil)
	var nowCalls int64
	c.now = func() int64 { atomic.AddInt64(&nowCalls, 1); return 0 }
	defer c.Stop()

	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))
	c.Unsubscribe("q")

	time.Sleep(40 * time.Millisecond)
	before := atomic.LoadInt64(&nowCalls)
	time.Sleep(60 * time.Millisecond)
	after := atomic.LoadInt64(&nowCalls)

	if after != before {
		t.Fatalf("idle ticker kept working after Unsubscribe: now() calls %d -> %d, want no growth (tick body must short-circuit when inactive)", before, after)
	}
}

func TestDeliverWithoutWindowsIsPassthrough(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, time.Hour, nil)
	defer c.Stop()

	d := engine.Delivery{Channel: "any", Data: []byte("x"), Offset: 9, Epoch: 1}
	if !c.Deliver(d) {
		t.Fatal("Deliver with no windows must pass through")
	}
	if got := s.offsets(); len(got) != 1 || got[0] != 9 {
		t.Fatalf("offsets = %v, want [9]", got)
	}
}

func TestConcurrentConfirmKeepsStrictOrder(t *testing.T) {
	const total = 300

	e := engine.New(engine.WithHistory(total, 0))
	t.Cleanup(e.Stop)
	s := &sink{failEvery: 7}
	c := NewConsumer(e, s.deliver, 2*time.Millisecond, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(4, 0, time.Minute, 3))

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			got := s.offsets()
			if len(got) > 0 {
				c.Confirm("q", got[len(got)-1])
			}
			if len(got) == total {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for i := range total {
		e.Publish("q", []byte{byte(i)}, 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
			return c.Deliver(d)
		})
	}
	<-done

	got := s.offsets()
	if len(got) != total {
		t.Fatalf("delivered %d offsets, want %d", len(got), total)
	}
	for i, off := range got {
		if off != uint64(i+1) {
			t.Fatalf("delivery %d = offset %d, want %d (strict order, no loss, no dups)", i, off, i+1)
		}
	}
}

func TestTickerPumpResumesAfterRefusedDelivery(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(2, 0, time.Minute, 3))

	deliver := func(_ engine.SubscriberID, d engine.Delivery) bool { return c.Deliver(d) }
	e.Publish("q", []byte("a"), 0, deliver)
	c.Confirm("q", 1)

	s.setReject(true)
	e.Publish("q", []byte("b"), 0, deliver)
	s.setReject(false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := s.offsets(); len(got) == 2 && got[1] == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("offsets = %v, want [1 2]: refused delivery never resumed", s.offsets())
}

func TestLostGapResetsWindowAndKeepsFlowing(t *testing.T) {
	e := engine.New(engine.WithHistory(2, 0))
	t.Cleanup(e.Stop)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(1, 0, time.Minute, 3))

	deliver := func(_ engine.SubscriberID, d engine.Delivery) bool { return c.Deliver(d) }
	e.Publish("q", []byte("a"), 0, deliver)
	for i := range 5 {
		e.Publish("q", []byte{byte(i)}, 0, deliver)
	}

	c.Confirm("q", 1)
	if got := c.LostGaps(); got != 1 {
		t.Fatalf("LostGaps = %d, want 1 (offsets 2-4 evicted from history)", got)
	}

	e.Publish("q", []byte("fresh"), 0, deliver)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := s.offsets()
		if len(got) == 2 && got[0] == 1 && got[1] == 7 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("offsets = %v, want [1 7]: window never re-baselined after lost gap", s.offsets())
}

func TestSubscribeAfterStopDoesNotStartLoop(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, time.Hour, nil)

	c.Stop()
	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))

	c.mu.Lock()
	running := c.running
	c.mu.Unlock()
	if running {
		t.Fatal("Subscribe after Stop started the redelivery loop; Stop's join contract is unsound")
	}
}

func TestSubscribeStopConcurrent(t *testing.T) {
	for range 50 {
		e := newQoSEngine(t)
		c := NewConsumer(e, func(engine.Delivery) bool { return true }, time.Millisecond, nil)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))
		}()
		go func() {
			defer wg.Done()
			c.Stop()
		}()
		wg.Wait()
		c.Stop()
	}
}

func TestConsumerStopConcurrent(t *testing.T) {
	e := newQoSEngine(t)
	c := NewConsumer(e, func(engine.Delivery) bool { return true }, time.Hour, nil)
	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Stop()
		}()
	}
	wg.Wait()
}
