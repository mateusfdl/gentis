package pubsub

import (
	"testing"
)

func TestSubscribeAndUnsubscribe(t *testing.T) {
	ps := New()

	connID := ConnectionID(1)
	channel := "test-channel"

	// Subscribe
	if !ps.Subscribe(connID, channel) {
		t.Error("First subscribe should return true")
	}

	// Subscribe again (should return false)
	if ps.Subscribe(connID, channel) {
		t.Error("Second subscribe should return false")
	}

	// Check channel exists
	if !ps.ChannelExists(channel) {
		t.Error("Channel should exist after subscribe")
	}

	// Unsubscribe
	if !ps.Unsubscribe(connID, channel) {
		t.Error("Unsubscribe should return true")
	}

	// Channel should be cleaned up
	if ps.ChannelExists(channel) {
		t.Error("Channel should be deleted when empty")
	}

	// Unsubscribe again (should return false)
	if ps.Unsubscribe(connID, channel) {
		t.Error("Second unsubscribe should return false")
	}
}

func TestGetSubscribers(t *testing.T) {
	ps := New()

	conn1 := ConnectionID(1)
	conn2 := ConnectionID(2)
	conn3 := ConnectionID(3)
	channel := "test-channel"

	ps.Subscribe(conn1, channel)
	ps.Subscribe(conn2, channel)
	ps.Subscribe(conn3, channel)

	// Get all subscribers excluding conn1
	subs := ps.GetSubscribers(channel, conn1)

	if len(subs) != 2 {
		t.Errorf("Expected 2 subscribers, got %d", len(subs))
	}

	// Check that conn1 is not in the list
	for _, id := range subs {
		if id == conn1 {
			t.Error("conn1 should be excluded from subscribers")
		}
	}
}

func TestRemoveConnection(t *testing.T) {
	ps := New()

	connID := ConnectionID(1)
	channel1 := "channel1"
	channel2 := "channel2"

	ps.Subscribe(connID, channel1)
	ps.Subscribe(connID, channel2)

	if ps.ChannelCount() != 2 {
		t.Errorf("Expected 2 channels, got %d", ps.ChannelCount())
	}

	// Remove connection
	ps.RemoveConnection(connID)

	// Channels should be cleaned up
	if ps.ChannelCount() != 0 {
		t.Errorf("Expected 0 channels after removing connection, got %d", ps.ChannelCount())
	}
}

func TestChannelCount(t *testing.T) {
	ps := New()

	if ps.ChannelCount() != 0 {
		t.Error("Initial channel count should be 0")
	}

	ps.Subscribe(ConnectionID(1), "channel1")
	ps.Subscribe(ConnectionID(1), "channel2")
	ps.Subscribe(ConnectionID(2), "channel1")

	if ps.ChannelCount() != 2 {
		t.Errorf("Expected 2 channels, got %d", ps.ChannelCount())
	}
}

func TestTotalSubscriptions(t *testing.T) {
	ps := New()

	ps.Subscribe(ConnectionID(1), "channel1")
	ps.Subscribe(ConnectionID(2), "channel1")
	ps.Subscribe(ConnectionID(1), "channel2")

	total := ps.TotalSubscriptions()
	if total != 3 {
		t.Errorf("Expected 3 total subscriptions, got %d", total)
	}
}
