package engine

import (
	"hash/maphash"
	"maps"
	"sync"
	"sync/atomic"
)

const defaultNumShards = 32

// cacheLineSize is the typical CPU cache line size on modern x86-64 processors.
// Padding shards to this boundary prevents false sharing when different goroutines
// access adjacent shards in the slice.
const cacheLineSize = 64

type Shard struct {
	mu       sync.RWMutex
	channels map[string]*Channel
	peak     int

	// Per-shard counters avoid cross-core cache-line bouncing on publish.
	// Engine.Stats() sums across all shards (infrequent, ~once per Prometheus scrape).
	publishCount   atomic.Int64
	deliveredCount atomic.Int64
	droppedCount   atomic.Int64
	messageBytes   atomic.Int64

	// Pad to a multiple of the cache line size to prevent false sharing
	// between adjacent shards. Without this, two shards can share a cache
	// line, causing expensive cross-core invalidation when concurrent
	// goroutines access different shards.
	_ [cacheLineSize]byte
}

// getShard returns the shard for a channel using maphash.String which
// leverages AES-NI hardware acceleration on amd64 for fast, well-distributed
// hashing. The seed is initialized once at engine creation.
func (e *Engine) getShard(channel string) *Shard {
	h := maphash.String(e.hashSeed, channel)
	return &e.shards[h%uint64(len(e.shards))]
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
		rebuilt := make(map[string]*Channel, len(s.channels))

		maps.Copy(rebuilt, s.channels)

		s.channels = rebuilt
		s.peak = len(s.channels)
	}
}
