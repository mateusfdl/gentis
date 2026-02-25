package engine

import (
	"maps"
	"sync"
)

const numSubShards = 32

type subShard struct {
	mu    sync.RWMutex
	index map[SubscriberID]map[string]struct{}
	peak  int

	// Pad to prevent false sharing between adjacent subShards (see Shard).
	_ [cacheLineSize]byte
}

// recreates index map to release old buckets when the load
// factor drops well below the high-water mark
func (sh *subShard) maybeRebuild() {
	if sh.peak > 64 && len(sh.index) < sh.peak/4 {
		rebuilt := make(map[SubscriberID]map[string]struct{}, len(sh.index))

		maps.Copy(rebuilt, sh.index)
		sh.index = rebuilt
		sh.peak = len(sh.index)
	}
}

type subscriptions struct {
	shards [numSubShards]subShard
}

func newSubscriptions() *subscriptions {
	s := &subscriptions{}
	for i := range s.shards {
		s.shards[i].index = make(map[SubscriberID]map[string]struct{})
	}
	return s
}

func (s *subscriptions) getShard(id SubscriberID) *subShard {
	return &s.shards[uint64(id)%numSubShards]
}

func (s *subscriptions) Add(id SubscriberID, channel string) {
	sh := s.getShard(id)
	sh.mu.Lock()
	channels, ok := sh.index[id]
	if !ok {
		channels = make(map[string]struct{})
		sh.index[id] = channels
		if len(sh.index) > sh.peak {
			sh.peak = len(sh.index)
		}
	}
	channels[channel] = struct{}{}
	sh.mu.Unlock()
}

func (s *subscriptions) Remove(id SubscriberID, channel string) {
	sh := s.getShard(id)
	sh.mu.Lock()
	channels, ok := sh.index[id]
	if !ok {
		sh.mu.Unlock()
		return
	}
	delete(channels, channel)
	if len(channels) == 0 {
		delete(sh.index, id)
		sh.maybeRebuild()
	}
	sh.mu.Unlock()
}

func (s *subscriptions) RemoveAll(id SubscriberID) {
	sh := s.getShard(id)
	sh.mu.Lock()
	delete(sh.index, id)
	sh.maybeRebuild()
	sh.mu.Unlock()
}

func (s *subscriptions) GetChannels(id SubscriberID) []string {
	sh := s.getShard(id)
	sh.mu.RLock()
	channels, ok := sh.index[id]
	if !ok {
		sh.mu.RUnlock()
		return nil
	}
	result := make([]string, 0, len(channels))
	for ch := range channels {
		result = append(result, ch)
	}
	sh.mu.RUnlock()
	return result
}

func (s *subscriptions) Has(id SubscriberID, channel string) bool {
	sh := s.getShard(id)
	sh.mu.RLock()
	channels, ok := sh.index[id]
	if !ok {
		sh.mu.RUnlock()
		return false
	}
	_, exists := channels[channel]
	sh.mu.RUnlock()
	return exists
}

func (s *subscriptions) Count(id SubscriberID) int {
	sh := s.getShard(id)
	sh.mu.RLock()
	channels, ok := sh.index[id]
	if !ok {
		sh.mu.RUnlock()
		return 0
	}
	count := len(channels)
	sh.mu.RUnlock()
	return count
}
