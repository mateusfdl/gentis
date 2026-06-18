package arena

import (
	"strings"
	"testing"
)

func TestSetGetSubject(t *testing.T) {
	var s SessionSlot
	s.SetSubject("user-42")

	got := s.GetSubject()
	if got != "user-42" {
		t.Fatalf("GetSubject() = %q, want %q", got, "user-42")
	}
}

func TestSubjectTruncation(t *testing.T) {
	var s SessionSlot
	long := strings.Repeat("x", MaxSubjectLen+50)
	s.SetSubject(long)

	got := s.GetSubject()
	if len(got) != MaxSubjectLen {
		t.Fatalf("subject len = %d, want %d", len(got), MaxSubjectLen)
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
