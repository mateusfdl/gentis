//go:build linux

package arena

import (
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/transport"
)

func newTestArena(t *testing.T, maxSlots int) *Arena {
	t.Helper()
	a, err := New(int(unsafe.Sizeof(SessionSlot{})), maxSlots)
	if err != nil {
		t.Fatalf("arena.New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestArenaState_InitialState(t *testing.T) {
	a := newTestArena(t, 4)

	s, err := NewArenaState(42, a)
	if err != nil {
		t.Fatalf("NewArenaState: %v", err)
	}

	if s.ID() != 42 {
		t.Errorf("ID: want 42, got %d", s.ID())
	}
	if s.IsAuthenticated() {
		t.Error("new state should not be authenticated")
	}
	if s.Subject() != "" {
		t.Errorf("Subject: want empty, got %q", s.Subject())
	}
	if s.SubscriptionCount() != 0 {
		t.Errorf("SubscriptionCount: want 0, got %d", s.SubscriptionCount())
	}
}

func TestArenaState_Authenticate(t *testing.T) {
	a := newTestArena(t, 1)
	s, err := NewArenaState(1, a)
	if err != nil {
		t.Fatalf("NewArenaState: %v", err)
	}

	claims := auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Unix(1700003600, 0),
		Channels:  []string{"chat-*"},
		Pub:       []string{"chat-1"},
	}
	if err := s.Authenticate(claims); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !s.IsAuthenticated() {
		t.Error("should be authenticated after Authenticate")
	}
	if got := s.Subject(); got != "user-1" {
		t.Errorf("Subject: want %q, got %q", "user-1", got)
	}
	if got := s.ExpiresAt(); !got.Equal(time.Unix(1700003600, 0)) {
		t.Errorf("ExpiresAt: want %v, got %v", time.Unix(1700003600, 0), got)
	}
	if !s.CanSubscribe("chat-9") {
		t.Error("CanSubscribe(chat-9): want true")
	}
	if s.CanSubscribe("news") {
		t.Error("CanSubscribe(news): want false")
	}
	if !s.CanPublish("chat-1") {
		t.Error("CanPublish(chat-1): want true")
	}
	if s.CanPublish("chat-2") {
		t.Error("CanPublish(chat-2): want false")
	}
}

func TestArenaState_AuthenticateOverwrite(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	_ = s.Authenticate(auth.Claims{Subject: "user-1"})
	_ = s.Authenticate(auth.Claims{Subject: "user-2"})

	if got := s.Subject(); got != "user-2" {
		t.Errorf("Subject: want %q, got %q", "user-2", got)
	}
	if got := s.ExpiresAt(); !got.IsZero() {
		t.Errorf("ExpiresAt: want zero time, got %v", got)
	}
}

func TestArenaState_Subscriptions(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	s.AddSubscription("alpha")
	s.AddSubscription("beta")

	if s.SubscriptionCount() != 2 {
		t.Errorf("count: want 2, got %d", s.SubscriptionCount())
	}
	if !s.IsSubscribedTo("alpha") {
		t.Error("should be subscribed to alpha")
	}
	if s.IsSubscribedTo("gamma") {
		t.Error("should not be subscribed to gamma")
	}

	s.RemoveSubscription("alpha")

	if s.IsSubscribedTo("alpha") {
		t.Error("should not be subscribed to alpha after removal")
	}
	if s.SubscriptionCount() != 1 {
		t.Errorf("count after remove: want 1, got %d", s.SubscriptionCount())
	}
}

func TestArenaState_DuplicateSubscription(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	s.AddSubscription("channel-a")
	s.AddSubscription("channel-a")

	if s.SubscriptionCount() != 1 {
		t.Errorf("count: want 1, got %d", s.SubscriptionCount())
	}
}

func TestArenaState_AddSubscriptionResults(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	if got := s.AddSubscription("channel-0"); got != transport.SubscriptionAdded {
		t.Errorf("first add: want SubscriptionAdded, got %v", got)
	}
	if got := s.AddSubscription("channel-0"); got != transport.SubscriptionAlreadyPresent {
		t.Errorf("duplicate add: want SubscriptionAlreadyPresent, got %v", got)
	}

	for i := 1; i < MaxSubscriptions; i++ {
		if got := s.AddSubscription(fmt.Sprintf("channel-%d", i)); got != transport.SubscriptionAdded {
			t.Errorf("add %d: want SubscriptionAdded, got %v", i, got)
		}
	}

	for i := 0; i < 3; i++ {
		if got := s.AddSubscription(fmt.Sprintf("overflow-%d", i)); got != transport.SubscriptionCapReached {
			t.Errorf("overflow add %d: want SubscriptionCapReached, got %v", i, got)
		}
	}

	if s.SubscriptionCount() != MaxSubscriptions {
		t.Errorf("count: want %d, got %d", MaxSubscriptions, s.SubscriptionCount())
	}
}

func TestArenaState_CloseIsIdempotent(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	s.Close()
	s.Close() // must not panic or double-free
}

func TestArenaState_CloseReleasesSlot(t *testing.T) {
	a := newTestArena(t, 1)

	// Exhaust the arena.
	s1, err := NewArenaState(1, a)
	if err != nil {
		t.Fatalf("first alloc: %v", err)
	}
	if _, err := NewArenaState(2, a); err == nil {
		t.Fatal("expected ErrFull on second alloc")
	}

	// after close, a new alloc should succeed.
	s1.Close()
	s2, err := NewArenaState(3, a)
	if err != nil {
		t.Fatalf("alloc after close: %v", err)
	}

	// reused slot should be zeroed.
	if s2.IsAuthenticated() {
		t.Error("reused slot should not be authenticated")
	}
	if s2.SubscriptionCount() != 0 {
		t.Errorf("reused slot SubscriptionCount: want 0, got %d", s2.SubscriptionCount())
	}
}

func TestArenaState_ConcurrentReaders(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	var wg sync.WaitGroup
	// 100 readers looping on the hot-path check.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = s.IsAuthenticated()
				_ = s.IsSubscribedTo("channel-x")
			}
		}()
	}

	// single writer flipping state.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 1000; j++ {
			_ = s.Authenticate(auth.Claims{Subject: "u"})
			s.AddSubscription("channel-x")
			s.RemoveSubscription("channel-x")
		}
	}()

	wg.Wait()
}

func TestArenaState_ConcurrentAddRemove(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.AddSubscription("ch")
		}()
		go func() {
			defer wg.Done()
			s.RemoveSubscription("ch")
		}()
	}
	wg.Wait()
}
