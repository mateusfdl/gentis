package transport

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mateusfdl/gentis/internal/engine"
)

// stubSender records deliveries for assertion without any heavyweight
// message building. Only an atomic counter — no non-atomic state — so
// concurrent invocations under -race don't trip false positives.
type stubSender struct {
	calls atomic.Int64
}

func (s *stubSender) DeliverMessage(_ engine.Delivery) bool {
	s.calls.Add(1)
	return true
}

func TestSessionStore_LegacyRegisterDeliverUnregister(t *testing.T) {
	s := NewSessionStore()
	sender := &stubSender{}

	s.Register(42, sender)
	if !s.Deliver(42, engine.Delivery{Channel: "ch", Data: []byte("x")}) {
		t.Fatal("Deliver after Register returned false")
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("calls: want 1, got %d", got)
	}

	s.Unregister(42)
	if s.Deliver(42, engine.Delivery{Channel: "ch", Data: []byte("x")}) {
		t.Error("Deliver after Unregister should return false")
	}
}

func TestSessionStore_FlatInRangeUsesArray(t *testing.T) {
	s := NewFlatSessionStore(100, 16)
	sender := &stubSender{}

	s.Register(105, sender) // 105 is in [100, 116)
	// Sanity: slotFor returns ok=true.
	if idx, ok := s.slotFor(105); !ok || idx != 5 {
		t.Fatalf("slotFor(105): want (5, true), got (%d, %v)", idx, ok)
	}
	if !s.Deliver(105, engine.Delivery{Channel: "a", Data: []byte("b")}) {
		t.Fatal("in-range Deliver returned false")
	}
}

func TestSessionStore_FlatBelowBaseUsesOverflow(t *testing.T) {
	s := NewFlatSessionStore(100, 16)
	sender := &stubSender{}

	s.Register(50, sender) // below base → overflow
	if idx, ok := s.slotFor(50); ok {
		t.Fatalf("slotFor(50): expected overflow, got idx=%d ok=%v", idx, ok)
	}
	if !s.Deliver(50, engine.Delivery{Channel: "a", Data: []byte("b")}) {
		t.Fatal("below-base Deliver returned false")
	}

	s.Unregister(50)
	if s.Deliver(50, engine.Delivery{Channel: "a", Data: []byte("b")}) {
		t.Error("below-base Deliver after Unregister should return false")
	}
}

func TestSessionStore_FlatAboveRangeUsesOverflow(t *testing.T) {
	s := NewFlatSessionStore(100, 16)
	sender := &stubSender{}

	s.Register(200, sender) // above base+cap=116 → overflow
	if _, ok := s.slotFor(200); ok {
		t.Fatal("slotFor(200): expected overflow")
	}
	if !s.Deliver(200, engine.Delivery{Channel: "a", Data: []byte("b")}) {
		t.Fatal("above-range Deliver returned false")
	}
}

func TestSessionStore_FlatBoundaryIDs(t *testing.T) {
	const base = 100
	const cap = 16
	s := NewFlatSessionStore(base, cap)

	first := &stubSender{}
	last := &stubSender{}
	afterLast := &stubSender{}
	s.Register(base, first)            // idx 0 — in range
	s.Register(base+cap-1, last)       // idx cap-1 — last flat slot
	s.Register(base+cap, afterLast)    // one past — overflow

	if idx, ok := s.slotFor(base); !ok || idx != 0 {
		t.Errorf("first boundary: want idx=0 ok=true, got idx=%d ok=%v", idx, ok)
	}
	if idx, ok := s.slotFor(base + cap - 1); !ok || idx != cap-1 {
		t.Errorf("last boundary: want idx=%d ok=true, got idx=%d ok=%v", cap-1, idx, ok)
	}
	if _, ok := s.slotFor(base + cap); ok {
		t.Errorf("one-past boundary: want overflow, got flat")
	}

	// All three should deliver.
	s.Deliver(base, engine.Delivery{Channel: "x", Data: nil})
	s.Deliver(base+cap-1, engine.Delivery{Channel: "x", Data: nil})
	s.Deliver(base+cap, engine.Delivery{Channel: "x", Data: nil})
	for name, sender := range map[string]*stubSender{
		"first": first, "last": last, "afterLast": afterLast,
	} {
		if sender.calls.Load() != 1 {
			t.Errorf("%s: want 1 call, got %d", name, sender.calls.Load())
		}
	}
}

func TestSessionStore_DeliverUnknownIDReturnsFalse(t *testing.T) {
	s := NewFlatSessionStore(0, 8)
	if s.Deliver(3, engine.Delivery{Channel: "x", Data: nil}) {
		t.Error("Deliver on empty slot should return false")
	}
	if s.Deliver(99, engine.Delivery{Channel: "x", Data: nil}) {
		t.Error("Deliver on overflow miss should return false")
	}
}

func TestSessionStore_RegisterOverwrites(t *testing.T) {
	s := NewFlatSessionStore(0, 8)
	a := &stubSender{}
	b := &stubSender{}
	s.Register(1, a)
	s.Register(1, b) // overwrite
	s.Deliver(1, engine.Delivery{Channel: "x", Data: nil})
	if a.calls.Load() != 0 {
		t.Errorf("old sender should not have been called, got %d", a.calls.Load())
	}
	if b.calls.Load() != 1 {
		t.Errorf("new sender calls: want 1, got %d", b.calls.Load())
	}
}

func TestSessionStore_ZeroCapacityFallsBackToLegacy(t *testing.T) {
	// Capacity <= 0 should return a legacy map-only store.
	s := NewFlatSessionStore(0, 0)
	if len(s.arr) != 0 {
		t.Fatalf("expected legacy store, got flat array len=%d", len(s.arr))
	}
	// Still functional.
	sender := &stubSender{}
	s.Register(42, sender)
	s.Deliver(42, engine.Delivery{Channel: "x", Data: nil})
	if sender.calls.Load() != 1 {
		t.Errorf("legacy-mode call count: want 1, got %d", sender.calls.Load())
	}
}

func TestSessionStore_ConcurrentRegisterDeliver(t *testing.T) {
	// -race-oriented: many goroutines registering and delivering across
	// both the flat path and overflow path.
	s := NewFlatSessionStore(0, 64)
	const producers = 16
	const perProducer = 1000

	var wg sync.WaitGroup
	wg.Add(producers)
	for g := 0; g < producers; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				// Mix of flat (IDs 0..63) and overflow (IDs ≥ 64).
				id := engine.SubscriberID((g*perProducer + i) % 128)
				sender := &stubSender{}
				s.Register(id, sender)
				_ = s.Deliver(id, engine.Delivery{Channel: "ch", Data: nil})
				s.Unregister(id)
			}
		}(g)
	}
	wg.Wait()
}

func TestSessionStore_MixedFlatAndOverflow(t *testing.T) {
	// Exercise flat hits, overflow hits, flat misses, overflow misses all
	// in one go to catch any path-crossing bugs.
	s := NewFlatSessionStore(1000, 100)

	hitFlat := &stubSender{}
	hitOverflow := &stubSender{}
	s.Register(1050, hitFlat)
	s.Register(5000, hitOverflow)

	tests := []struct {
		id     engine.SubscriberID
		want   bool
		sender *stubSender
	}{
		{1050, true, hitFlat},
		{5000, true, hitOverflow},
		{1099, false, nil}, // flat miss
		{1100, false, nil}, // one past range — overflow miss
		{999, false, nil},  // below base — overflow miss
	}
	for _, tt := range tests {
		got := s.Deliver(tt.id, engine.Delivery{Channel: "x", Data: nil})
		if got != tt.want {
			t.Errorf("Deliver(%d): want %v, got %v", tt.id, tt.want, got)
		}
	}
	if hitFlat.calls.Load() != 1 {
		t.Errorf("hitFlat calls: %d", hitFlat.calls.Load())
	}
	if hitOverflow.calls.Load() != 1 {
		t.Errorf("hitOverflow calls: %d", hitOverflow.calls.Load())
	}
}
