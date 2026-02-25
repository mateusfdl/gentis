package engine

import (
	"slices"
	"sync"
	"sync/atomic"
)

type Channel struct {
	name        string
	subscribers atomic.Pointer[[]SubscriberID]
	mu          sync.Mutex
}

func NewChannel(name string) *Channel {
	c := &Channel{name: name}
	empty := make([]SubscriberID, 0)
	c.subscribers.Store(&empty)
	return c
}

func (c *Channel) Subscribe(id SubscriberID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.subscribers.Load()
	if current == nil {
		current = &[]SubscriberID{}
	}

	if slices.Contains(*current, id) {
		return false
	}

	newSubs := make([]SubscriberID, len(*current)+1)
	copy(newSubs, *current)
	newSubs[len(*current)] = id

	c.subscribers.Store(&newSubs)

	return true
}

func (c *Channel) Unsubscribe(id SubscriberID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.subscribers.Load()
	if current == nil || len(*current) == 0 {
		return false
	}

	idx := -1
	for i, existing := range *current {
		if existing == id {
			idx = i
			break
		}
	}

	if idx == -1 {
		return false
	}

	newSubs := make([]SubscriberID, len(*current)-1)

	copy(newSubs[:idx], (*current)[:idx])
	copy(newSubs[idx:], (*current)[idx+1:])

	c.subscribers.Store(&newSubs)

	return true
}

func (c *Channel) Name() string {
	return c.name
}

func (c *Channel) Subscribers() []SubscriberID {
	ptr := c.subscribers.Load()

	if ptr == nil {
		return nil
	}

	return *ptr
}

func (c *Channel) SubscriberCount() int {
	ptr := c.subscribers.Load()

	if ptr == nil {
		return 0
	}

	return len(*ptr)
}
