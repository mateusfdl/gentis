package relay

import (
	"github.com/mateusfdl/gentis/internal/pattern"
)

type RouteMode int

const (
	RouteModeRelay RouteMode = iota
	RouteModeLocal
	RouteModeBoth
)

const maxCacheSize = 10000

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
	cache    *pattern.Cache[*RouteResult]
}

func NewRouter(patterns []ChannelPattern) *Router {
	return &Router{
		patterns: patterns,
		cache:    pattern.NewCache[*RouteResult](maxCacheSize),
	}
}

func (r *Router) Route(channel string) *RouteResult {
	if cached, ok := r.cache.Get(channel); ok {
		return cached
	}

	result := r.resolve(channel)
	r.cache.Set(channel, result)
	return result
}

func (r *Router) resolve(channel string) *RouteResult {
	for _, p := range r.patterns {
		if pattern.Match(p.Pattern, channel) {
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
