package qos

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

func TestSweeperDrivesRedelivery(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	sw := NewSweeper(5 * time.Millisecond)
	t.Cleanup(sw.Stop)
	c := NewConsumer(e, s.deliver, sw, nil)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(5, 0, 20*time.Millisecond, 3))

	e.Publish("q", []byte("x"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		return c.Deliver(d)
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := s.offsets(); len(got) >= 2 && got[0] == 1 && got[1] == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("offsets = %v, want offset 1 redelivered by the shared sweeper", s.offsets())
}

func TestSweeperSharedAcrossConsumers(t *testing.T) {
	e := newQoSEngine(t)
	s1, s2 := &sink{}, &sink{}
	sw := NewSweeper(5 * time.Millisecond)
	t.Cleanup(sw.Stop)
	c1 := NewConsumer(e, s1.deliver, sw, nil)
	defer c1.Stop()
	c2 := NewConsumer(e, s2.deliver, sw, nil)
	defer c2.Stop()

	e.Subscribe(1, "q")
	c1.Subscribe("q", NewWindow(5, 0, 20*time.Millisecond, 3))
	c2.Subscribe("q", NewWindow(5, 0, 20*time.Millisecond, 3))

	e.Publish("q", []byte("x"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		c1.Deliver(d)
		c2.Deliver(d)
		return true
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s1.offsets()) >= 2 && len(s2.offsets()) >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("offsets = %v / %v, want both consumers redelivered by one sweeper", s1.offsets(), s2.offsets())
}

func TestConsumerStopJoinsInflightTick(t *testing.T) {
	e := newQoSEngine(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls, clock atomic.Int64
	deliver := func(d engine.Delivery) bool {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		return true
	}
	c := NewConsumer(e, deliver, nil, nil)
	c.now = clock.Load

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(1, 0, time.Second, 3))
	e.Publish("q", []byte("x"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		return c.Deliver(d)
	})

	clock.Store(int64(2 * time.Second))
	tickDone := make(chan struct{})
	go func() {
		c.Tick()
		close(tickDone)
	}()
	<-entered

	stopDone := make(chan struct{})
	go func() {
		c.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop returned while a tick was still delivering to the transport")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-tickDone
	<-stopDone
}

func TestTickAfterStopDeliversNothing(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, nil, nil)

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(1, 0, time.Nanosecond, 3))
	e.Publish("q", []byte("x"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		return c.Deliver(d)
	})

	c.Stop()
	time.Sleep(time.Millisecond)
	c.Tick()

	if got := s.offsets(); len(got) != 1 {
		t.Fatalf("offsets = %v, want only the live delivery: Tick after Stop must be inert", got)
	}
}

func TestSubscribeAfterStopStaysUnregistered(t *testing.T) {
	sw := NewSweeper(time.Hour)
	t.Cleanup(sw.Stop)
	e := newQoSEngine(t)
	c := NewConsumer(e, func(engine.Delivery) bool { return true }, sw, nil)

	c.Stop()
	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))

	if got := sw.size(); got != 0 {
		t.Fatalf("sweeper holds %d consumers after Subscribe on a stopped consumer, want 0", got)
	}
}

func TestUnsubscribeLastWindowDeregisters(t *testing.T) {
	sw := NewSweeper(time.Hour)
	t.Cleanup(sw.Stop)
	e := newQoSEngine(t)
	c := NewConsumer(e, func(engine.Delivery) bool { return true }, sw, nil)
	defer c.Stop()

	c.Subscribe("a", NewWindow(1, 0, time.Minute, 1))
	c.Subscribe("b", NewWindow(1, 0, time.Minute, 1))
	if got := sw.size(); got != 1 {
		t.Fatalf("sweeper holds %d consumers, want 1", got)
	}

	c.Unsubscribe("a")
	if got := sw.size(); got != 1 {
		t.Fatalf("sweeper holds %d consumers with one window left, want 1", got)
	}
	c.Unsubscribe("b")
	if got := sw.size(); got != 0 {
		t.Fatalf("sweeper holds %d consumers after the last unsubscribe, want 0", got)
	}
}

func TestSweeperStopConcurrent(t *testing.T) {
	sw := NewSweeper(time.Millisecond)
	e := newQoSEngine(t)
	c := NewConsumer(e, func(engine.Delivery) bool { return true }, sw, nil)
	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sw.Stop()
		}()
	}
	wg.Wait()
}
