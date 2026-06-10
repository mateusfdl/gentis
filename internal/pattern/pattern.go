// Package pattern provides glob matching over channel names and a
// concurrency-safe lookup cache with random partial eviction. Shared by
// the relay router (channel routing) and the engine (wildcard
// subscriptions).
package pattern

import (
	"path"
	"strings"
	"sync"
)

// evictRatio is the fraction of cache entries to randomly evict when the
// cache exceeds its max size. Evicting ~25% avoids the thundering-herd
// problem of nuking the entire cache (every concurrent lookup would miss
// and recompute simultaneously).
const evictRatio = 4

// Match reports whether the glob pattern matches the name. A malformed
// pattern matches nothing.
func Match(pattern, name string) bool {
	matched, _ := path.Match(pattern, name)
	return matched
}

// IsPattern reports whether the string contains glob metacharacters and
// must be treated as a pattern rather than a literal channel name.
func IsPattern(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

// Cache memoizes pattern resolution results keyed by channel name. When
// full it evicts a random quarter of its entries; Go's randomized map
// iteration order provides the randomness without an external PRNG.
type Cache[V any] struct {
	mu      sync.RWMutex
	entries map[string]V
	maxSize int
}

func NewCache[V any](maxSize int) *Cache[V] {
	return &Cache[V]{
		entries: make(map[string]V),
		maxSize: maxSize,
	}
}

func (c *Cache[V]) Get(key string) (V, bool) {
	c.mu.RLock()
	v, ok := c.entries[key]
	c.mu.RUnlock()
	return v, ok
}

func (c *Cache[V]) Set(key string, v V) {
	c.mu.Lock()
	if len(c.entries) >= c.maxSize {
		toEvict := max(len(c.entries)/evictRatio, 1)
		evicted := 0
		for k := range c.entries {
			if evicted >= toEvict {
				break
			}
			delete(c.entries, k)
			evicted++
		}
	}
	c.entries[key] = v
	c.mu.Unlock()
}

func (c *Cache[V]) Clear() {
	c.mu.Lock()
	clear(c.entries)
	c.mu.Unlock()
}

func (c *Cache[V]) Len() int {
	c.mu.RLock()
	n := len(c.entries)
	c.mu.RUnlock()
	return n
}
