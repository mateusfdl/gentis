// Package pubsub implements the pub/sub channel management system.
package pubsub

import (
	"sync"
)

// ConnectionID represents a unique connection identifier.
type ConnectionID int

// Channel represents a pub/sub channel with its subscribers.
type Channel struct {
	name        string
	subscribers map[ConnectionID]struct{}
	mu          sync.RWMutex
}

// NewChannel creates a new channel.
func NewChannel(name string) *Channel {
	return &Channel{
		name:        name,
		subscribers: make(map[ConnectionID]struct{}),
	}
}

// Subscribe adds a connection to this channel.
// Returns true if newly subscribed, false if already subscribed.
func (c *Channel) Subscribe(connID ConnectionID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.subscribers[connID]; exists {
		return false
	}

	c.subscribers[connID] = struct{}{}
	return true
}

// Unsubscribe removes a connection from this channel.
// Returns true if was subscribed, false otherwise.
func (c *Channel) Unsubscribe(connID ConnectionID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.subscribers[connID]; !exists {
		return false
	}

	delete(c.subscribers, connID)
	return true
}

// GetSubscribers returns a slice of all subscribers, optionally excluding one.
func (c *Channel) GetSubscribers(excludeID ConnectionID) []ConnectionID {
	c.mu.RLock()
	defer c.mu.RUnlock()

	subscribers := make([]ConnectionID, 0, len(c.subscribers))
	for id := range c.subscribers {
		if id != excludeID {
			subscribers = append(subscribers, id)
		}
	}

	return subscribers
}

// SubscriberCount returns the number of subscribers.
func (c *Channel) SubscriberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subscribers)
}

// PubSub manages channels and subscriptions.
type PubSub struct {
	channels map[string]*Channel
	connSubs map[ConnectionID]map[string]struct{} // track what channels each connection is subscribed to
	mu       sync.RWMutex
}

// New creates a new PubSub instance.
func New() *PubSub {
	return &PubSub{
		channels: make(map[string]*Channel),
		connSubs: make(map[ConnectionID]map[string]struct{}),
	}
}

// Subscribe subscribes a connection to a channel.
// Creates the channel if it doesn't exist.
// Returns true if newly subscribed, false if already subscribed.
func (ps *PubSub) Subscribe(connID ConnectionID, channelName string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Get or create channel
	channel, exists := ps.channels[channelName]
	if !exists {
		channel = NewChannel(channelName)
		ps.channels[channelName] = channel
	}

	// Subscribe to channel
	newlySubscribed := channel.Subscribe(connID)

	// Track connection's subscriptions
	if newlySubscribed {
		if ps.connSubs[connID] == nil {
			ps.connSubs[connID] = make(map[string]struct{})
		}
		ps.connSubs[connID][channelName] = struct{}{}
	}

	return newlySubscribed
}

// Unsubscribe removes a connection from a channel.
// Deletes the channel if it becomes empty.
// Returns true if was subscribed, false otherwise.
func (ps *PubSub) Unsubscribe(connID ConnectionID, channelName string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	channel, exists := ps.channels[channelName]
	if !exists {
		return false
	}

	wasSubscribed := channel.Unsubscribe(connID)

	if wasSubscribed {
		// Remove from connection tracking
		if subs, ok := ps.connSubs[connID]; ok {
			delete(subs, channelName)
		}

		// Clean up empty channel
		if channel.SubscriberCount() == 0 {
			delete(ps.channels, channelName)
		}
	}

	return wasSubscribed
}

// GetSubscribers returns all subscribers to a channel, excluding the specified connection.
func (ps *PubSub) GetSubscribers(channelName string, excludeID ConnectionID) []ConnectionID {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	channel, exists := ps.channels[channelName]
	if !exists {
		return []ConnectionID{}
	}

	return channel.GetSubscribers(excludeID)
}

// RemoveConnection removes all subscriptions for a connection.
// This should be called when a connection closes.
func (ps *PubSub) RemoveConnection(connID ConnectionID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Get all channels this connection is subscribed to
	subs, exists := ps.connSubs[connID]
	if !exists {
		return
	}

	// Unsubscribe from all channels
	channelsToDelete := make([]string, 0)
	for channelName := range subs {
		if channel, ok := ps.channels[channelName]; ok {
			channel.Unsubscribe(connID)
			if channel.SubscriberCount() == 0 {
				channelsToDelete = append(channelsToDelete, channelName)
			}
		}
	}

	// Clean up empty channels
	for _, channelName := range channelsToDelete {
		delete(ps.channels, channelName)
	}

	// Remove connection tracking
	delete(ps.connSubs, connID)
}

// ChannelCount returns the number of active channels.
func (ps *PubSub) ChannelCount() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.channels)
}

// TotalSubscriptions returns the total number of subscriptions across all channels.
func (ps *PubSub) TotalSubscriptions() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	total := 0
	for _, channel := range ps.channels {
		total += channel.SubscriberCount()
	}
	return total
}

// ChannelExists checks if a channel exists.
func (ps *PubSub) ChannelExists(channelName string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	_, exists := ps.channels[channelName]
	return exists
}
