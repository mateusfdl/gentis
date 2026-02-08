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
		r.cache = make(map[string]*RouteResult)
	}
	r.cache[channel] = result
	r.mu.Unlock()

	return result
}

func (r *Router) resolve(channel string) *RouteResult {
	for _, p := range r.patterns {
		if r.matches(p.Pattern, channel) {
			return &RouteResult{Mode: p.Mode}
		}
	}

	return &RouteResult{Mode: RouteModeRelay}
}

func (r *Router) matches(pattern, channel string) bool {
	matched, _ := path.Match(pattern, channel)
	return matched
}

func (r *Router) ClearCache() {
	r.mu.Lock()
	r.cache = make(map[string]*RouteResult)
	r.mu.Unlock()
}
