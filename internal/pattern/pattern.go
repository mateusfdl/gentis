// Package pattern provides star-only wildcard matching over channel names
// and a concurrency-safe lookup cache with random partial eviction. Shared
// by the relay router (channel routing) and the engine (wildcard
// subscriptions). The grammar has one metacharacter: * matches any
// sequence of bytes, including the empty one. The characters ? [ ] \ are
// reserved so the grammar can never silently diverge from the auth claim
// grammar (exact or trailing-star), which would turn a literal claim into
// a wildcard grant.
package pattern

import (
	"strings"
	"sync"
)

// evictRatio is the fraction of cache entries to randomly evict when the
// cache exceeds its max size. Evicting ~25% avoids the thundering-herd
// problem of nuking the entire cache (every concurrent lookup would miss
// and recompute simultaneously).
const evictRatio = 4

// Match reports whether the pattern matches the name. Greedy two-pointer
// star matching: on mismatch it backtracks to the last star and retries
// with the star consuming one more byte.
func Match(pattern, name string) bool {
	star, mark := -1, 0
	i, j := 0, 0
	for j < len(name) {
		switch {
		case i < len(pattern) && pattern[i] == '*':
			star, mark = i, j
			i++
		case i < len(pattern) && pattern[i] == name[j]:
			i++
			j++
		case star >= 0:
			mark++
			i, j = star+1, mark
		default:
			return false
		}
	}
	for i < len(pattern) && pattern[i] == '*' {
		i++
	}
	return i == len(pattern)
}

// IsPattern reports whether the string contains a star and must be
// treated as a pattern rather than a literal channel name.
func IsPattern(s string) bool {
	return strings.ContainsRune(s, '*')
}

// HasReserved reports whether the string contains a reserved
// metacharacter. Reserved characters are rejected in channel names,
// patterns, and auth claims alike.
func HasReserved(s string) bool {
	return strings.ContainsAny(s, `?[]\`)
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
	if maxSize < 1 {
		maxSize = 1
	}
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
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxSize {
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
