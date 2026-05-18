package relay

import (
	"path"
	"sync"
)

type RouteMode int

const (
	RouteModeRelay RouteMode = iota
	RouteModeLocal
	RouteModeBoth
)

const maxCacheSize = 10000

// evictRatio is the fraction of cache entries to randomly evict when the
// cache exceeds maxCacheSize. Evicting ~25% avoids the thundering-herd
// problem of nuking the entire cache (every concurrent Route call would
// miss and call resolve simultaneously).
const evictRatio = 4 // 1/4 = 25%

// Pre-allocated singleton results to avoid heap-allocating a new RouteResult
// on every cache miss. Since there are only 3 possible modes, we reuse these.
var (
	routeResultRelay = &RouteResult{Mode: RouteModeRelay}
	routeResultLocal = &RouteResult{Mode: RouteModeLocal}
	routeResultBoth  = &RouteResult{Mode: RouteModeBoth}
)

type ChannelPattern struct {
	Pattern string
	Mode    RouteMode
}

type RouteResult struct {
	Mode RouteMode
}

type Router struct {
	patterns []ChannelPattern
	cache    map[string]*RouteResult
	mu       sync.RWMutex
}

func NewRouter(patterns []ChannelPattern) *Router {
	return &Router{
		patterns: patterns,
		cache:    make(map[string]*RouteResult),
	}
}

func (r *Router) Route(channel string) *RouteResult {
	r.mu.RLock()
	if cached, ok := r.cache[channel]; ok {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	result := r.resolve(channel)

	r.mu.Lock()
	if len(r.cache) >= maxCacheSize {
		r.evictLocked()
	}
	r.cache[channel] = result
	r.mu.Unlock()

	return result
}

// evictLocked removes ~25% of cache entries randomly. Go's map iteration
// order is randomized, so simply deleting the first N entries from a range
// achieves random eviction without needing an external PRNG.
// Must be called with r.mu held for writing.
func (r *Router) evictLocked() {
	toEvict := len(r.cache) / evictRatio
	evicted := 0
	for key := range r.cache {
		if evicted >= toEvict {
			break
		}
		delete(r.cache, key)
		evicted++
	}
}

func (r *Router) resolve(channel string) *RouteResult {
	for _, p := range r.patterns {
		if r.matches(p.Pattern, channel) {
			return singletonResult(p.Mode)
		}
	}

	return routeResultRelay
}

// singletonResult returns the pre-allocated RouteResult for the given mode,
// eliminating a heap allocation per resolve call.
func singletonResult(mode RouteMode) *RouteResult {
	switch mode {
	case RouteModeRelay:
		return routeResultRelay
	case RouteModeLocal:
		return routeResultLocal
	case RouteModeBoth:
		return routeResultBoth
	default:
		return routeResultRelay
	}
}

func (r *Router) matches(pattern, channel string) bool {
	matched, _ := path.Match(pattern, channel)
	return matched
}
