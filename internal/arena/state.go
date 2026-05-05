//go:build linux

package arena

import (
	"log"
	"sync"
	"sync/atomic"
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

func (s *ArenaState) Authenticate(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slot.SetAuthToken(token)
	atomic.StoreUint32(&s.slot.Authenticated, 1)
	return nil
}

func (s *ArenaState) AuthToken() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slot.GetAuthToken()
}

func (s *ArenaState) AddSubscription(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slot.AddSubscription(channel) {
		return
	}
	// false can mean "already subscribed" (no-op) or "cap hit" (log).
	// disambiguate by checking count after the call.
	if s.slot.SubCount >= MaxSubscriptions && !s.slot.IsSubscribed(channel) {
		log.Printf("arena session %d: subscription cap %d reached, dropped %q",
			s.id, MaxSubscriptions, channel)
	}
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

func (s *ArenaState) Close() {
	if !s.freed.CompareAndSwap(false, true) {
		return
	}
	s.arena.Free(s.idx)
}
