package transport

import (
	"sync"
	"testing"

	"github.com/mateusfdl/gentis/internal/engine"
)

type fakeSender struct {
	mu       sync.Mutex
	received [][]byte
}

func (f *fakeSender) DeliverMessage(d engine.Delivery) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, d.Data)
	return true
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

func TestDeliverDropsStaleGenerationAfterSlotReuse(t *testing.T) {
	store := NewFlatSessionStore(engine.SubscriberID(1), 8)

	slot := engine.SubscriberID(3)

	idA := store.AllocID(slot)
	a := &fakeSender{}
	store.Register(idA, a)

	store.Unregister(idA)

	idB := store.AllocID(slot)
	if idB == idA {
		t.Fatalf("reused slot produced identical id %d; generation not stamped", idB)
	}
	b := &fakeSender{}
	store.Register(idB, b)

	if store.Deliver(idA, engine.Delivery{Channel: "foo", Data: []byte("stale")}) {
		t.Fatal("stale-generation delivery to a reused slot reported success")
	}
	if b.count() != 0 {
		t.Fatalf("reused session received %d stale messages, want 0", b.count())
	}

	if !store.Deliver(idB, engine.Delivery{Channel: "foo", Data: []byte("live")}) {
		t.Fatal("live delivery to the current session failed")
	}
	if b.count() != 1 {
		t.Fatalf("current session received %d messages, want 1", b.count())
	}
}

func TestDeliverValidatesAgainstRegisteredGenerationNotLiveCounter(t *testing.T) {
	store := NewFlatSessionStore(engine.SubscriberID(1), 8)

	slot := engine.SubscriberID(3)
	id := store.AllocID(slot)
	s := &fakeSender{}
	store.Register(id, s)

	idx, ok := store.slotFor(slot)
	if !ok {
		t.Fatalf("slot %d not in flat range", slot)
	}
	store.gen[idx].Add(1)

	if !store.Deliver(id, engine.Delivery{Channel: "foo", Data: []byte("x")}) {
		t.Fatal("live session lost its own message after the allocation counter advanced")
	}
	if s.count() != 1 {
		t.Fatalf("received %d messages, want 1", s.count())
	}
}

func TestAllocIDSkipsZeroGenerationOnWraparound(t *testing.T) {
	store := NewFlatSessionStore(engine.SubscriberID(1), 8)

	slot := engine.SubscriberID(3)
	idx, ok := store.slotFor(slot)
	if !ok {
		t.Fatalf("slot %d not in flat range", slot)
	}
	store.gen[idx].Store(^uint32(0))

	id := store.AllocID(slot)
	if g := uint32(uint64(id) >> genShift); g == 0 {
		t.Fatal("AllocID returned a bare id (gen=0) on wraparound; stale-delivery protection disabled")
	}
	if low := uint64(id) & idMask; low != uint64(slot) {
		t.Fatalf("AllocID corrupted the slot id: got low bits %d, want %d", low, slot)
	}
}

func TestDeliverBareIDStillWorks(t *testing.T) {
	store := NewFlatSessionStore(engine.SubscriberID(1), 8)

	id := engine.SubscriberID(2)
	s := &fakeSender{}
	store.Register(id, s)

	if !store.Deliver(id, engine.Delivery{Channel: "foo", Data: []byte("x")}) {
		t.Fatal("bare-id delivery failed")
	}
	if s.count() != 1 {
		t.Fatalf("received %d, want 1", s.count())
	}
}
