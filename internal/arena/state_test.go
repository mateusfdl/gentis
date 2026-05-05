//go:build linux

package arena

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"unsafe"
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
	if s.AuthToken() != "" {
		t.Errorf("AuthToken: want empty, got %q", s.AuthToken())
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

	if err := s.Authenticate("my-token"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !s.IsAuthenticated() {
		t.Error("should be authenticated after Authenticate")
	}
	if got := s.AuthToken(); got != "my-token" {
		t.Errorf("AuthToken: want %q, got %q", "my-token", got)
	}
}

func TestArenaState_AuthenticateOverwrite(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	_ = s.Authenticate("token-1")
	_ = s.Authenticate("token-2")

	if got := s.AuthToken(); got != "token-2" {
		t.Errorf("AuthToken: want %q, got %q", "token-2", got)
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

func TestArenaState_SubscriptionCapLogs(t *testing.T) {
	a := newTestArena(t, 1)
	s, _ := NewArenaState(1, a)

	var buf bytes.Buffer
	origOut := log.Writer()
	origFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(origOut)
		log.SetFlags(origFlags)
	})

	for i := 0; i < MaxSubscriptions+3; i++ {
		s.AddSubscription(fmt.Sprintf("channel-%d", i))
	}

	if s.SubscriptionCount() != MaxSubscriptions {
		t.Errorf("count: want %d, got %d", MaxSubscriptions, s.SubscriptionCount())
	}

	logs := buf.String()
	overflowLines := strings.Count(logs, "subscription cap")
	if overflowLines != 3 {
		t.Errorf("expected 3 overflow log lines, got %d:\n%s", overflowLines, logs)
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
			_ = s.Authenticate("t")
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
