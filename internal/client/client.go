package client

import (
	"sync"
)

type State struct {
	id            int
	authenticated bool
	authToken     string
	subscriptions map[string]struct{}
	mu            sync.RWMutex
}

func NewState(id int) *State {
	return &State{
		id:            id,
		authenticated: false,
		subscriptions: make(map[string]struct{}),
	}
}

func (s *State) ID() int {
	return s.id
}

func (s *State) Authenticate(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.authenticated = true
	s.authToken = token
	return nil
}

func (s *State) IsAuthenticated() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authenticated
}

func (s *State) AuthToken() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authToken
}

func (s *State) AddSubscription(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[channel] = struct{}{}
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
