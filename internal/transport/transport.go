package transport

import (
	"sync"
	"sync/atomic"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
)

type Sender interface {
	DeliverMessage(d engine.Delivery) bool
}

// AddSubscriptionResult reports why a SessionState.AddSubscription call did or
// did not record a channel, so the caller can log capacity drops without the
// state layer reaching for a logger of its own.
type AddSubscriptionResult uint8

const (
	SubscriptionAdded AddSubscriptionResult = iota
	SubscriptionAlreadyPresent
	SubscriptionCapReached
)

// SessionState is the minimal per-connection state surface that the ws
// dispatch layer relies on. Both the heap-backed client.State and the
// arena-backed arena.ArenaState satisfy it.
type SessionState interface {
	IsAuthenticated() bool
	Authenticate(c auth.Claims) error
	Subject() string
	CanSubscribe(channel string) bool
	CanPublish(channel string) bool
	AddSubscription(channel string) AddSubscriptionResult
	RemoveSubscription(channel string)
	SubscriptionCount() int
}

// SessionStore maps SubscriberIDs to Sender implementations. It operates in
// one of two modes:
//
//   - Legacy map mode (NewSessionStore): all entries live in a sync.Map.
//     Retained for backward-compat and for transports that don't need the
//     GC-scan reduction.
//   - Flat-array mode (NewFlatSessionStore): a fixed-size
//     []atomic.Pointer[Sender] covers IDs in [baseID, baseID+cap). IDs
//     outside that range fall through to an overflow sync.Map.
//
// The flat array turns N scattered map-entry allocations into one
// contiguous pointer array — one big GC-scan region instead of many
// pointer-chasing map buckets.
const (
	genShift = 32
	idMask   = (uint64(1) << genShift) - 1
)

type SessionStore struct {
	baseID   uint64
	arr      []atomic.Pointer[Sender] // nil when in legacy map mode
	gen      []atomic.Uint32          // per-slot generation, parallel to arr
	overflow sync.Map                 // always used in legacy mode; fallback in flat mode
}

// NewSessionStore returns a legacy map-only store. Retained for backward-
// compat — all existing callsites that don't specify a capacity continue
// to get this behaviour.
func NewSessionStore() *SessionStore {
	return &SessionStore{}
}

// NewFlatSessionStore returns a store with a fixed-size array for IDs in
// [baseID, baseID+capacity). IDs outside that range fall through to an
// overflow sync.Map so correctness is preserved regardless of ID scale.
//
// Intended for transports with known dense ID ranges (e.g. gRPC sessions
// with arena-derived slot-index IDs, where the effective ID space is bounded
// by the arena's max-sessions capacity).
func NewFlatSessionStore(baseID engine.SubscriberID, capacity int) *SessionStore {
	if capacity <= 0 {
		return NewSessionStore()
	}
	return &SessionStore{
		baseID: uint64(baseID),
		arr:    make([]atomic.Pointer[Sender], capacity),
		gen:    make([]atomic.Uint32, capacity),
	}
}

// slotFor returns the array index for id if it falls within the flat
// range, or ok=false otherwise (overflow path). The generation bits in the
// high half of id are masked off before computing the slot.
func (s *SessionStore) slotFor(id engine.SubscriberID) (int, bool) {
	if len(s.arr) == 0 {
		return 0, false
	}
	n := (uint64(id) & idMask) - s.baseID
	if n >= uint64(len(s.arr)) {
		return 0, false
	}
	return int(n), true
}

// AllocID returns the identity a session occupying slotID should use with the
// engine and this store. For a flat-array slot it stamps a fresh generation
// into the high bits so a reused slot yields a distinct identity, letting
// Deliver reject in-flight messages addressed to a previous occupant. For
// overflow/legacy ids (monotonic, never reused) it returns slotID unchanged.
func (s *SessionStore) AllocID(slotID engine.SubscriberID) engine.SubscriberID {
	idx, ok := s.slotFor(slotID)
	if !ok {
		return slotID
	}
	g := s.gen[idx].Add(1)
	return engine.SubscriberID((uint64(g) << genShift) | (uint64(slotID) & idMask))
}

func (s *SessionStore) Register(id engine.SubscriberID, sender Sender) {
	if idx, ok := s.slotFor(id); ok {
		// atomic.Pointer[Sender] holds *Sender (pointer to interface). The
		// extra indirection vs storing the interface directly is one
		// pointer dereference on the hot Deliver path — cheap and lets us
		// use typed atomic operations.
		s.arr[idx].Store(&sender)
		return
	}
	s.overflow.Store(id, sender)
}

func (s *SessionStore) Unregister(id engine.SubscriberID) {
	if idx, ok := s.slotFor(id); ok {
		s.arr[idx].Store(nil)
		return
	}
	s.overflow.Delete(id)
}

func (s *SessionStore) Deliver(id engine.SubscriberID, d engine.Delivery) bool {
	if idx, ok := s.slotFor(id); ok {
		if g := uint32(uint64(id) >> genShift); g != 0 && g != s.gen[idx].Load() {
			return false
		}
		if p := s.arr[idx].Load(); p != nil {
			return (*p).DeliverMessage(d)
		}
		return false
	}
	val, ok := s.overflow.Load(id)
	if !ok {
		return false
	}
	return val.(Sender).DeliverMessage(d)
}
