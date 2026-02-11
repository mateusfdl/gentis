package client

import (
	"sort"
	"sync"
	"testing"
)

func TestNewState(t *testing.T) {
	s := NewState(42)

	if s.ID() != 42 {
		t.Errorf("expected ID 42, got %d", s.ID())
	}

	if s.IsAuthenticated() {
		t.Error("expected new state to be unauthenticated")
	}

	if s.AuthToken() != "" {
		t.Errorf("expected empty auth token, got %q", s.AuthToken())
	}

	if s.SubscriptionCount() != 0 {
		t.Errorf("expected 0 subscriptions, got %d", s.SubscriptionCount())
	}
}

func TestAuthenticate(t *testing.T) {
	s := NewState(1)

	if err := s.Authenticate("my-token"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !s.IsAuthenticated() {
		t.Error("expected authenticated after Authenticate()")
	}

	if s.AuthToken() != "my-token" {
		t.Errorf("expected token 'my-token', got %q", s.AuthToken())
	}
}

func TestAuthenticateEmptyToken(t *testing.T) {
	s := NewState(1)

	s.Authenticate("")

	if !s.IsAuthenticated() {
		t.Error("expected authenticated even with empty token")
	}

	if s.AuthToken() != "" {
		t.Errorf("expected empty token, got %q", s.AuthToken())
	}
}

func TestAuthenticateOverwrite(t *testing.T) {
	s := NewState(1)

	s.Authenticate("token-1")
	s.Authenticate("token-2")

	if s.AuthToken() != "token-2" {
		t.Errorf("expected token-2, got %q", s.AuthToken())
	}
}

func TestAddAndRemoveSubscription(t *testing.T) {
	s := NewState(1)

	s.AddSubscription("channel-a")
	s.AddSubscription("channel-b")

	if s.SubscriptionCount() != 2 {
		t.Errorf("expected 2 subscriptions, got %d", s.SubscriptionCount())
	}

	if !s.IsSubscribedTo("channel-a") {
		t.Error("expected to be subscribed to channel-a")
	}

	if !s.IsSubscribedTo("channel-b") {
		t.Error("expected to be subscribed to channel-b")
	}

	if s.IsSubscribedTo("channel-c") {
		t.Error("should not be subscribed to channel-c")
	}

	s.RemoveSubscription("channel-a")

	if s.IsSubscribedTo("channel-a") {
		t.Error("should not be subscribed to channel-a after removal")
	}

	if s.SubscriptionCount() != 1 {
		t.Errorf("expected 1 subscription, got %d", s.SubscriptionCount())
	}
}

func TestRemoveNonexistentSubscription(t *testing.T) {
	s := NewState(1)

	// Should not panic
	s.RemoveSubscription("nonexistent")

	if s.SubscriptionCount() != 0 {
		t.Errorf("expected 0 subscriptions, got %d", s.SubscriptionCount())
	}
}

func TestDuplicateSubscription(t *testing.T) {
	s := NewState(1)

	s.AddSubscription("channel-a")
	s.AddSubscription("channel-a")

	// map-based, so adding twice has no effect
	if s.SubscriptionCount() != 1 {
		t.Errorf("expected 1 subscription, got %d", s.SubscriptionCount())
	}
}

func TestGetSubscriptions(t *testing.T) {
	s := NewState(1)

	s.AddSubscription("alpha")
	s.AddSubscription("beta")
	s.AddSubscription("gamma")

	subs := s.GetSubscriptions()
	if len(subs) != 3 {
		t.Fatalf("expected 3 subscriptions, got %d", len(subs))
	}

	sort.Strings(subs)
	expected := []string{"alpha", "beta", "gamma"}
	for i, v := range expected {
		if subs[i] != v {
			t.Errorf("expected %s at index %d, got %s", v, i, subs[i])
		}
	}
}

func TestGetSubscriptionsEmpty(t *testing.T) {
	s := NewState(1)

	subs := s.GetSubscriptions()
	if len(subs) != 0 {
		t.Errorf("expected 0 subscriptions, got %d", len(subs))
	}
}

func TestGetSubscriptionsIsolation(t *testing.T) {
	s := NewState(1)
	s.AddSubscription("ch1")

	subs := s.GetSubscriptions()
	// Modifying the returned slice should not affect internal state
	subs[0] = "modified"

	if s.GetSubscriptions()[0] == "modified" {
		t.Error("GetSubscriptions should return a copy")
	}
}

func TestConcurrentAuthenticate(t *testing.T) {
	s := NewState(1)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			s.Authenticate(token)
		}("token")
	}

	wg.Wait()

	if !s.IsAuthenticated() {
		t.Error("expected authenticated after concurrent access")
	}
}

func TestConcurrentSubscriptions(t *testing.T) {
	s := NewState(1)
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.AddSubscription("channel")
			s.IsSubscribedTo("channel")
			s.SubscriptionCount()
			s.GetSubscriptions()
		}(i)
	}

	wg.Wait()

	if !s.IsSubscribedTo("channel") {
		t.Error("expected subscription after concurrent adds")
	}
}

func TestConcurrentAddAndRemove(t *testing.T) {
	s := NewState(1)
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
	// no race/panic ????
}
