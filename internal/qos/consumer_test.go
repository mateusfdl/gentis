package qos

import (
	"sync"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

type sink struct {
	mu         sync.Mutex
	deliveries []engine.Delivery
	reject     bool
}

func (s *sink) deliver(d engine.Delivery) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reject {
		return false
	}
	s.deliveries = append(s.deliveries, d)
	return true
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

func gateAndDeliver(t *testing.T, c *Consumer, s *sink, d engine.Delivery) {
	t.Helper()
	if c.Gate(d) != SendNow {
		return
	}
	if !s.deliver(d) {
		c.Rollback(d.Channel, d.Offset)
	}
}

func TestSlowConsumerNeverDrops(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 10*time.Millisecond)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(2, 0, time.Minute, 2))

	for i := 0; i < 10; i++ {
		r := e.Publish("q", []byte{byte(i)}, 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
			gateAndDeliver(t, c, s, d)
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
	c := NewConsumer(e, s.deliver, 5*time.Millisecond)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(5, 0, 20*time.Millisecond, 3))

	e.Publish("q", []byte("x"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		gateAndDeliver(t, c, s, d)
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

func TestPoisonAfterMaxRedeliveries(t *testing.T) {
	e := newQoSEngine(t)
	s := &sink{}
	c := NewConsumer(e, s.deliver, 5*time.Millisecond)
	defer c.Stop()

	e.Subscribe(1, "q")
	c.Subscribe("q", NewWindow(5, 0, 10*time.Millisecond, 1))

	e.Publish("q", []byte("bad"), 0, func(_ engine.SubscriberID, d engine.Delivery) bool {
		gateAndDeliver(t, c, s, d)
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
	c := NewConsumer(e, s.deliver, time.Hour)
	defer c.Stop()

	c.Subscribe("q", NewWindow(1, 0, time.Minute, 1))
	c.Unsubscribe("q")

	d := engine.Delivery{Channel: "q", Data: []byte("x"), Offset: 1, Epoch: 7}
	if c.Gate(d) != SendNow {
		t.Fatal("Gate after Unsubscribe must pass through")
	}
}

func TestGateWithoutWindowsIsPassthrough(t *testing.T) {
	e := newQoSEngine(t)
	c := NewConsumer(e, func(engine.Delivery) bool { return true }, time.Hour)
	defer c.Stop()

	d := engine.Delivery{Channel: "any", Data: []byte("x"), Offset: 9, Epoch: 1}
	if c.Gate(d) != SendNow {
		t.Fatal("Gate with no windows must be SendNow")
	}
}
