package engine

import (
	"maps"
	"sync"
)

const defaultNumShards = 32

type Shard struct {
	mu       sync.RWMutex
	channels map[string]*channel
	peak     int
	_        [16]byte
}

// we can recreate the map to release old buckets when the load factor
// drops well below the high-water mark
func (s *Shard) maybeRebuild() {
	if s.peak > 64 && len(s.channels) < s.peak/4 {
		rebuilt := make(map[string]*channel, len(s.channels))

		maps.Copy(rebuilt, s.channels)

		s.channels = rebuilt
		s.peak = len(s.channels)
	}
}
