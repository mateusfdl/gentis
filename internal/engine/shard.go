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

// getShard returns the shard for a channel using inline FNV-1a hashing.
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func (e *engine) getShard(channel string) *Shard {
	h := uint32(2166136261)
	for i := 0; i < len(channel); i++ {
		h ^= uint32(channel[i])
		h *= 16777619
	}
	return &e.shards[h%uint32(len(e.shards))]
}

// we try to reclaim memory when a shard’s channel map has
// significantly shrunk after previously growing large.
//
// since maps do not shrink automatically(even gc will ignore it). Once a map grows and allocates
// buckets, deleting entries does not free that memory. The map will keep
// its internal bucket array for the rest of its lifetime even if not used, we don't release its pages to the OS.

// on heavy long-running workload we will keep the heap increasing and retaining the memory (kudos to "100 Go Mistakes and How to Avoid Them" btw).
//
// To work around this, we track a high-water mark (peak): the largest number
// of entries this map has ever held. When the current size drops far below
// that peak, we rebuild the map from scratch so the runtime can allocate a
// smaller bucket array and release the unused memory back to the OS.
//
//	peak = 1_000
//	len(channels) = 1_000
//
//	// Later, most channels disappear
//	len(channels) = 120
//
//	// Without a rebuild:
//	//   - map still holds buckets sized for ~1,000 entries
//	//   - memory remains allocated indefinitely
//
//	// With maybeRebuild:
//	//   - channels map is recreated with capacity ~120
//	//   - unused buckets can be reclaimed by the runtime
//
// The rebuild logic is intentionally conservative to avoid excessive
// copying and allocation churn:
//
//   - Small maps are ignored entirely (peak must exceed a minimum threshold)
//   - A rebuild only happens when the map shrinks below 25% of its peak
//
// This method MUST be called with the shard mutex held
func (s *Shard) maybeRebuild() {
	if s.peak > 64 && len(s.channels) < s.peak/4 {
		rebuilt := make(map[string]*channel, len(s.channels))

		maps.Copy(rebuilt, s.channels)

		s.channels = rebuilt
		s.peak = len(s.channels)
	}
}
