package engine

import (
	"slices"
	"sync"

	"github.com/mateusfdl/gentis/internal/pattern"
)

const patternCacheSize = 10000

// patternRegistry holds wildcard subscriptions for the whole engine. A
// pattern can match channels in any shard, so the registry is global
// rather than per-shard; publishes skip it entirely via one atomic
// counter when no patterns exist. Mutations are rare next to publishes,
// so every mutation invalidates the whole channel match cache.
type patternRegistry struct {
	mu    sync.RWMutex
	subs  map[string][]SubscriberID
	byID  map[SubscriberID][]string
	cache *pattern.Cache[[]SubscriberID]
}

func newPatternRegistry() *patternRegistry {
	return &patternRegistry{
		subs:  make(map[string][]SubscriberID),
		byID:  make(map[SubscriberID][]string),
		cache: pattern.NewCache[[]SubscriberID](patternCacheSize),
	}
}

func (p *patternRegistry) add(id SubscriberID, pat string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.Contains(p.subs[pat], id) {
		return false
	}
	p.subs[pat] = append(p.subs[pat], id)
	p.byID[id] = append(p.byID[id], pat)
	p.cache.Clear()
	return true
}

func (p *patternRegistry) remove(id SubscriberID, pat string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.removeLocked(id, pat)
}

func (p *patternRegistry) removeLocked(id SubscriberID, pat string) bool {
	ids := p.subs[pat]
	idx := slices.Index(ids, id)
	if idx < 0 {
		return false
	}
	ids = slices.Delete(ids, idx, idx+1)
	if len(ids) == 0 {
		delete(p.subs, pat)
	} else {
		p.subs[pat] = ids
	}
	pats := p.byID[id]
	if pidx := slices.Index(pats, pat); pidx >= 0 {
		pats = slices.Delete(pats, pidx, pidx+1)
		if len(pats) == 0 {
			delete(p.byID, id)
		} else {
			p.byID[id] = pats
		}
	}
	p.cache.Clear()
	return true
}

func (p *patternRegistry) removeAll(id SubscriberID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pats := slices.Clone(p.byID[id])
	for _, pat := range pats {
		p.removeLocked(id, pat)
	}
	return len(pats)
}

// subscribersFor returns the deduplicated wildcard subscribers whose
// patterns match the channel. Results are cached per channel; the Set
// happens under the registry read lock so a concurrent mutation (which
// clears the cache under the write lock) can never leave a stale entry.
// The returned slice is shared and must not be mutated.
func (p *patternRegistry) subscribersFor(channel string) []SubscriberID {
	if ids, ok := p.cache.Get(channel); ok {
		return ids
	}
	p.mu.RLock()
	var ids []SubscriberID
	for pat, subs := range p.subs {
		if !pattern.Match(pat, channel) {
			continue
		}
		for _, id := range subs {
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
	}
	p.cache.Set(channel, ids)
	p.mu.RUnlock()
	return ids
}
