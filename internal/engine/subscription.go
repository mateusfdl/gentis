package engine

import (
	"maps"
	"slices"
	"sync"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

const numSubShards = 32

// flatThreshold is the max number of channels stored in a flat slice
// before promoting to a map. Most subscribers are on 1-5 channels, so
// the flat path avoids the overhead of a map (bucket allocation, hashing).
const flatThreshold = 8

// channelSet is a hybrid container that uses a flat slice for small sets
// and promotes to a map for larger ones. This eliminates the inner
// map[string]struct{} allocation for the overwhelmingly common case of
// subscribers on few channels.
type channelSet struct {
	flat []string            // used when len <= flatThreshold
	m    map[string]struct{} // used when len > flatThreshold; flat is nil when m is active
}

func (cs *channelSet) add(channel string) {
	if cs.m != nil {
		cs.m[channel] = struct{}{}
		return
	}
	// check for duplicate in flat
	if slices.Contains(cs.flat, channel) {
		return
	}
	if len(cs.flat) < flatThreshold {
		cs.flat = append(cs.flat, channel)
		return
	}
	// promote to map
	cs.m = make(map[string]struct{}, flatThreshold+1)
	for _, ch := range cs.flat {
		cs.m[ch] = struct{}{}
	}
	cs.m[channel] = struct{}{}
	cs.flat = nil
}

func (cs *channelSet) remove(channel string) {
	if cs.m != nil {
		delete(cs.m, channel)
		// Demote back to flat slice when the map shrinks to half the
		// promotion threshold. Using flatThreshold/2 (rather than
		// flatThreshold) provides hysteresis to prevent promote/demote
		// thrashing when the count oscillates near the boundary.
		if len(cs.m) <= flatThreshold/2 {
			cs.flat = make([]string, 0, len(cs.m))
			for ch := range cs.m {
				cs.flat = append(cs.flat, ch)
			}
			cs.m = nil
		}
		return
	}
	for i, ch := range cs.flat {
		if ch == channel {
			// swap-remove: order doesn't matter
			cs.flat[i] = cs.flat[len(cs.flat)-1]
			cs.flat = cs.flat[:len(cs.flat)-1]
			return
		}
	}
}

func (cs *channelSet) len() int {
	if cs.m != nil {
		return len(cs.m)
	}
	return len(cs.flat)
}

func (cs *channelSet) toSlice() []string {
	if cs.m != nil {
		result := make([]string, 0, len(cs.m))
		for ch := range cs.m {
			result = append(result, ch)
		}
		return result
	}
	result := make([]string, len(cs.flat))
	copy(result, cs.flat)
	return result
}

type subShard struct {
	mu    sync.RWMutex
	index map[SubscriberID]*channelSet
	peak  int

	// Pad to prevent false sharing between adjacent subShards (see Shard).
	_ [cacheline.Size]byte
}

// recreates index map to release old buckets when the load
// factor drops well below the high-water mark
func (sh *subShard) maybeRebuild() {
	if sh.peak > 64 && len(sh.index) < sh.peak/4 {
		rebuilt := make(map[SubscriberID]*channelSet, len(sh.index))

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
		s.shards[i].index = make(map[SubscriberID]*channelSet)
	}
	return s
}

func (s *subscriptions) getShard(id SubscriberID) *subShard {
	return &s.shards[uint64(id)%numSubShards]
}

func (s *subscriptions) Add(id SubscriberID, channel string) {
	sh := s.getShard(id)
	sh.mu.Lock()
	cs, ok := sh.index[id]
	if !ok {
		cs = &channelSet{}
		sh.index[id] = cs
		if len(sh.index) > sh.peak {
			sh.peak = len(sh.index)
		}
	}
	cs.add(channel)
	sh.mu.Unlock()
}

func (s *subscriptions) Remove(id SubscriberID, channel string) {
	sh := s.getShard(id)
	sh.mu.Lock()
	cs, ok := sh.index[id]
	if !ok {
		sh.mu.Unlock()
		return
	}
	cs.remove(channel)
	if cs.len() == 0 {
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
	cs, ok := sh.index[id]
	if !ok {
		sh.mu.RUnlock()
		return nil
	}
	result := cs.toSlice()
	sh.mu.RUnlock()
	return result
}
