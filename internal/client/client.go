package client

import (
	"sync"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/transport"
)

type State struct {
	id            int
	authenticated bool
	claims        auth.Claims
	subscriptions map[string]struct{}
	mu            sync.RWMutex
}

func NewState(id int) *State {
	return &State{
		id:            id,
		subscriptions: make(map[string]struct{}),
	}
}

func (s *State) ID() int {
	return s.id
}

func (s *State) Authenticate(c auth.Claims) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.authenticated = true
	s.claims = c
	return nil
}

func (s *State) IsAuthenticated() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authenticated
}

func (s *State) Subject() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.claims.Subject
}

func (s *State) Claims() auth.Claims {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.claims
}

func (s *State) CanSubscribe(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.claims.CanSubscribe(channel)
}

func (s *State) CanPublish(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.claims.CanPublish(channel)
}

func (s *State) AddSubscription(channel string) transport.AddSubscriptionResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[channel]; ok {
		return transport.SubscriptionAlreadyPresent
	}
	s.subscriptions[channel] = struct{}{}
	return transport.SubscriptionAdded
}

func (s *State) RemoveSubscription(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, channel)
}

func (s *State) IsSubscribedTo(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.subscriptions[channel]
	return exists
}

func (s *State) SubscriptionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscriptions)
}

func (s *State) GetSubscriptions() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs := make([]string, 0, len(s.subscriptions))
	for channel := range s.subscriptions {
		subs = append(subs, channel)
	}
	return subs
}
