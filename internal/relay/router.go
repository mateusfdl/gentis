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

type ChannelPattern struct {
	Pattern string
	Mode    RouteMode
}

type RouteResult struct {
	Mode RouteMode
}

type Router struct {
	patterns []ChannelPattern
	cache    sync.Map
}

func NewRouter(patterns []ChannelPattern) *Router {
	return &Router{
		patterns: patterns,
	}
}

func (r *Router) Route(channel string) *RouteResult {
	if cached, ok := r.cache.Load(channel); ok {
		return cached.(*RouteResult)
	}

	result := r.resolve(channel)
	r.cache.Store(channel, result)
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
	r.cache = sync.Map{}
}
