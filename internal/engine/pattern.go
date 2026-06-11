package engine

import (
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/mateusfdl/gentis/internal/pattern"
)

const (
	patternCacheSize   = 10000
	patternCacheShards = 32

	// dedupMapThreshold is the candidate count past which subscribersFor
	// switches from slices.Contains to a map for deduplication.
	dedupMapThreshold = 32
)

// patternRegistry holds wildcard subscriptions for the whole engine. A
// pattern can match channels in any shard, so the registry is global
// rather than per-shard; publishes skip it entirely via one atomic
// counter when no patterns exist.
//
// Reads are lock-free: every mutation derives an immutable snapshot from
// the mutable maps and swaps it in atomically. Match results are memoized
// in sharded caches keyed by channel; a cached entry is valid only for
// the snapshot generation it was computed against, so a reader racing a
// mutation can never resurrect a stale result.
type patternRegistry struct {
	mu   sync.Mutex
	subs map[string][]SubscriberID
	byID map[SubscriberID][]string

	snap   atomic.Pointer[patternSnapshot]
	seed   maphash.Seed
	caches [patternCacheShards]*pattern.Cache[cachedMatch]
}

type patternSnapshot struct {
	gen     uint64
	entries []patternEntry
}

type patternEntry struct {
	pat  string
	subs []SubscriberID
}

type cachedMatch struct {
	gen uint64
	ids []SubscriberID
}

func newPatternRegistry() *patternRegistry {
	p := &patternRegistry{
		subs: make(map[string][]SubscriberID),
		byID: make(map[SubscriberID][]string),
		seed: maphash.MakeSeed(),
	}
	for i := range p.caches {
		p.caches[i] = pattern.NewCache[cachedMatch](patternCacheSize / patternCacheShards)
	}
	p.snap.Store(&patternSnapshot{})
	return p
}

// publishLocked derives a fresh snapshot from the mutable maps and clears
// the match caches. Caller must hold p.mu. The entries and their subs
// slices are never mutated after the Store, only replaced wholesale.
func (p *patternRegistry) publishLocked() {
	next := &patternSnapshot{gen: p.snap.Load().gen + 1}
	if len(p.subs) > 0 {
		next.entries = make([]patternEntry, 0, len(p.subs))
		for pat, ids := range p.subs {
			next.entries = append(next.entries, patternEntry{pat: pat, subs: slices.Clone(ids)})
		}
	}
	p.snap.Store(next)
	for _, c := range p.caches {
		c.Clear()
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
	p.publishLocked()
	return true
}

func (p *patternRegistry) remove(id SubscriberID, pat string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.removeLocked(id, pat) {
		return false
	}
	p.publishLocked()
	return true
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
	return true
}

func (p *patternRegistry) removeAll(id SubscriberID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pats := slices.Clone(p.byID[id])
	for _, pat := range pats {
		p.removeLocked(id, pat)
	}
	if len(pats) > 0 {
		p.publishLocked()
	}
	return len(pats)
}

// subscribersFor returns the deduplicated wildcard subscribers whose
// patterns match the channel. Lock-free: one snapshot load plus a sharded
// cache lookup on the hit path. The returned slice is shared and must not
// be mutated.
func (p *patternRegistry) subscribersFor(channel string) []SubscriberID {
	snap := p.snap.Load()
	if len(snap.entries) == 0 {
		return nil
	}
	c := p.caches[maphash.String(p.seed, channel)&(patternCacheShards-1)]
	if m, ok := c.Get(channel); ok && m.gen == snap.gen {
		return m.ids
	}
	var ids []SubscriberID
	var set map[SubscriberID]struct{}
	for _, e := range snap.entries {
		if !pattern.Match(e.pat, channel) {
			continue
		}
		for _, id := range e.subs {
			switch {
			case set != nil:
				if _, dup := set[id]; dup {
					continue
				}
				set[id] = struct{}{}
				ids = append(ids, id)
			case slices.Contains(ids, id):
			default:
				ids = append(ids, id)
				if len(ids) > dedupMapThreshold {
					set = make(map[SubscriberID]struct{}, 2*len(ids))
					for _, seen := range ids {
						set[seen] = struct{}{}
					}
				}
			}
		}
	}
	c.Set(channel, cachedMatch{gen: snap.gen, ids: ids})
	return ids
}
