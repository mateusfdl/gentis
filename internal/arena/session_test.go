package arena

import (
	"strings"
	"testing"
)

func TestSetGetAuthToken(t *testing.T) {
	var s SessionSlot
	s.SetAuthToken("my-secret-token")

	got := s.GetAuthToken()
	if got != "my-secret-token" {
		t.Fatalf("GetAuthToken() = %q, want %q", got, "my-secret-token")
	}
}

func TestAuthTokenTruncation(t *testing.T) {
	var s SessionSlot
	long := strings.Repeat("x", MaxAuthTokenLen+50)
	s.SetAuthToken(long)

	got := s.GetAuthToken()
	if len(got) != MaxAuthTokenLen {
		t.Fatalf("token len = %d, want %d", len(got), MaxAuthTokenLen)
	}
}

func TestAddRemoveSubscription(t *testing.T) {
	var s SessionSlot

	if !s.AddSubscription("chan-a") {
		t.Fatal("AddSubscription(chan-a) failed")
	}
	if !s.AddSubscription("chan-b") {
		t.Fatal("AddSubscription(chan-b) failed")
	}
	if !s.AddSubscription("chan-c") {
		t.Fatal("AddSubscription(chan-c) failed")
	}

	if s.SubCount != 3 {
		t.Fatalf("SubCount = %d, want 3", s.SubCount)
	}

	if !s.IsSubscribed("chan-a") || !s.IsSubscribed("chan-b") || !s.IsSubscribed("chan-c") {
		t.Fatal("IsSubscribed returned false for added channels")
	}

	if !s.RemoveSubscription("chan-b") {
		t.Fatal("RemoveSubscription(chan-b) failed")
	}

	if s.SubCount != 2 {
		t.Fatalf("SubCount = %d, want 2", s.SubCount)
	}
	if s.IsSubscribed("chan-b") {
		t.Fatal("chan-b still subscribed after removal")
	}
	if !s.IsSubscribed("chan-a") || !s.IsSubscribed("chan-c") {
		t.Fatal("remaining channels missing after removal")
	}
}

func TestSubscriptionOverflow(t *testing.T) {
	var s SessionSlot

	for i := range MaxSubscriptions {
		name := strings.Repeat("x", i+1)
		if !s.AddSubscription(name) {
			t.Fatalf("AddSubscription failed at i=%d", i)
		}
	}

	if s.AddSubscription("overflow") {
		t.Fatal("AddSubscription should return false at capacity")
	}
}

func TestDuplicateSubscription(t *testing.T) {
	var s SessionSlot

	if !s.AddSubscription("dup-chan") {
		t.Fatal("first AddSubscription failed")
	}
	if s.AddSubscription("dup-chan") {
		t.Fatal("duplicate AddSubscription should return false")
	}
	if s.SubCount != 1 {
		t.Fatalf("SubCount = %d, want 1", s.SubCount)
	}
}

func TestIsSubscribedEmpty(t *testing.T) {
	var s SessionSlot
	if s.IsSubscribed("anything") {
		t.Fatal("IsSubscribed should return false on empty slot")
	}
}

func TestClear(t *testing.T) {
	var s SessionSlot
	s.ID = 42
	s.Authenticated = 1
	s.SetAuthToken("token")
	s.AddSubscription("chan-1")
	s.AddSubscription("chan-2")

	s.Clear()

	if s.ID != 0 || s.Authenticated != 0 || s.TokenLen != 0 || s.SubCount != 0 {
		t.Fatal("Clear did not zero all fields")
	}
	if s.GetAuthToken() != "" {
		t.Fatal("auth token not cleared")
	}
}

func TestChannelNameTruncation(t *testing.T) {
	var s SessionSlot
	long := strings.Repeat("c", MaxChanNameLen+50)

	if !s.AddSubscription(long) {
		t.Fatal("AddSubscription failed")
	}

	if s.SubLens[0] != MaxChanNameLen {
		t.Fatalf("stored len = %d, want %d", s.SubLens[0], MaxChanNameLen)
	}

	truncated := long[:MaxChanNameLen]
	if !s.IsSubscribed(truncated) {
		t.Fatal("IsSubscribed with truncated name should match")
	}
}

func TestRemoveNonexistent(t *testing.T) {
	var s SessionSlot
	s.AddSubscription("exists")

	if s.RemoveSubscription("nope") {
		t.Fatal("RemoveSubscription should return false for nonexistent channel")
	}
}
