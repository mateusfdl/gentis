package engine

import "sync"

type subscriptions struct {
	index sync.Map
}

func newSubscriptions() *subscriptions {
	return &subscriptions{}
}

func (s *subscriptions) Add(id SubscriberID, channel string) {
	channelsI, _ := s.index.LoadOrStore(id, &sync.Map{})
	channels := channelsI.(*sync.Map)
	channels.Store(channel, struct{}{})
}

func (s *subscriptions) Remove(id SubscriberID, channel string) {
	channelsI, ok := s.index.Load(id)
	if !ok {
		return
	}
	channels := channelsI.(*sync.Map)
	channels.Delete(channel)

	// Clean up the outer entry if the inner map is now empty.
	empty := true
	channels.Range(func(_, _ any) bool {
		empty = false
		return false
	})
	if empty {
		s.index.CompareAndDelete(id, channelsI)
	}
}

func (s *subscriptions) RemoveAll(id SubscriberID) {
	s.index.Delete(id)
}

func (s *subscriptions) GetChannels(id SubscriberID) []string {
	channelsI, ok := s.index.Load(id)
	if !ok {
		return nil
	}

	channels := channelsI.(*sync.Map)
	result := make([]string, 0)
	channels.Range(func(key, _ any) bool {
		result = append(result, key.(string))
		return true
	})
	return result
}

func (s *subscriptions) Has(id SubscriberID, channel string) bool {
	channelsI, ok := s.index.Load(id)
	if !ok {
		return false
	}
	channels := channelsI.(*sync.Map)
	_, exists := channels.Load(channel)
	return exists
}

func (s *subscriptions) Count(id SubscriberID) int {
	channelsI, ok := s.index.Load(id)
	if !ok {
		return 0
	}

	count := 0
	channels := channelsI.(*sync.Map)
	channels.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
