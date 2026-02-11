package transport

import (
	"sync"

	"github.com/mateusfdl/gentis/internal/engine"
)

type Sender interface {
	DeliverMessage(channel string, data []byte) bool
}

type SessionStore struct {
	sessions sync.Map
}

func NewSessionStore() *SessionStore {
	return &SessionStore{}
}

func (s *SessionStore) Register(id engine.SubscriberID, sender Sender) {
	s.sessions.Store(id, sender)
}

func (s *SessionStore) Unregister(id engine.SubscriberID) {
	s.sessions.Delete(id)
}

func (s *SessionStore) Deliver(id engine.SubscriberID, channel string, data []byte) bool {
	val, ok := s.sessions.Load(id)
	if !ok {
		return false
	}
	return val.(Sender).DeliverMessage(channel, data)
}
