//go:build linux

package arena

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/transport"
)

// ArenaState is a heap-allocated wrapper around a SessionSlot that lives
// inside an mmap'd Arena. Mutations are serialized by mu; IsAuthenticated
// is a lock-free atomic read of the underlying slot field so the dispatch
// hot path skips the mutex.
//
// the slot itself is gc-invisible.
// the wrapper (this struct right below your eyes) contains a
// single *SessionSlot pointer into arena memory plus a mutex and two
// atomics. all pointer-free aside from the slot reference itself.
type ArenaState struct {
	id    int
	arena *Arena
	slot  *SessionSlot
	idx   uint32
	mu    sync.RWMutex
	freed atomic.Bool

	// Allowlists are variable-length so they cannot live in the
	// fixed-layout slot; they stay on this heap wrapper. Usually nil
	// (full access), so the GC cost is two empty slice headers.
	channels []string
	pub      []string
}

// NewArenaState allocates a slot from the given arena and returns a wrapper.
// Returns ErrFull if the arena has no free slots. The session ID is supplied
// by the caller.
// NOTE: prefer NewArenaStateAuto when you want the ID to be
// deterministically derived from the slot index.
func NewArenaState(id int, a *Arena) (*ArenaState, error) {
	ptr, idx, err := a.Alloc()
	if err != nil {
		return nil, err
	}
	slot := (*SessionSlot)(ptr)
	slot.ID = uint64(id)
	return &ArenaState{
		id:    id,
		arena: a,
		slot:  slot,
		idx:   idx,
	}, nil
}

// NewArenaStateAuto allocates a slot and derives the session ID from the
// slot index: id = baseID + int(idx). This pins the ID into a dense range
// bounded by the arena capacity, which lets transports back their
// SessionStore with a flat array instead of a sync.Map.
//
// Returns ErrFull if the arena has no free slots.
func NewArenaStateAuto(a *Arena, baseID int) (*ArenaState, error) {
	ptr, idx, err := a.Alloc()
	if err != nil {
		return nil, err
	}
	id := baseID + int(idx)
	slot := (*SessionSlot)(ptr)
	slot.ID = uint64(id)
	return &ArenaState{
		id:    id,
		arena: a,
		slot:  slot,
		idx:   idx,
	}, nil
}

func (s *ArenaState) ID() int { return s.id }

func (s *ArenaState) IsAuthenticated() bool {
	return atomic.LoadUint32(&s.slot.Authenticated) == 1
}

func (s *ArenaState) Authenticate(c auth.Claims) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slot.SetSubject(c.Subject)
	if c.ExpiresAt.IsZero() {
		s.slot.ExpiresAt = 0
	} else {
		s.slot.ExpiresAt = c.ExpiresAt.Unix()
	}
	s.channels = c.Channels
	s.pub = c.Pub
	atomic.StoreUint32(&s.slot.Authenticated, 1)
	return nil
}

func (s *ArenaState) Subject() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slot.GetSubject()
}

func (s *ArenaState) ExpiresAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.slot.ExpiresAt == 0 {
		return time.Time{}
	}
	return time.Unix(s.slot.ExpiresAt, 0)
}

func (s *ArenaState) CanSubscribe(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := auth.Claims{Channels: s.channels}
	return c.CanSubscribe(channel)
}

func (s *ArenaState) CanPublish(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := auth.Claims{Pub: s.pub}
	return c.CanPublish(channel)
}

func (s *ArenaState) AddSubscription(channel string) transport.AddSubscriptionResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slot.AddSubscription(channel) {
		return transport.SubscriptionAdded
	}
	if s.slot.IsSubscribed(channel) {
		return transport.SubscriptionAlreadyPresent
	}
	return transport.SubscriptionCapReached
}

func (s *ArenaState) RemoveSubscription(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slot.RemoveSubscription(channel)
}

func (s *ArenaState) IsSubscribedTo(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slot.IsSubscribed(channel)
}

func (s *ArenaState) SubscriptionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int(s.slot.SubCount)
}

// Close returns the slot to the arena. It is idempotent, but the caller must
// not invoke any other method on this ArenaState once Close has run: the slot
// may be re-allocated to a different session, so a late read would observe
// another owner's state.
func (s *ArenaState) Close() {
	if !s.freed.CompareAndSwap(false, true) {
		return
	}
	s.arena.Free(s.idx)
}
