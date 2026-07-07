package engine

import (
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mateusfdl/gentis/internal/namespace"
)

type Channel struct {
	name        string
	epoch       uint64
	offset      atomic.Uint64
	subscribers atomic.Pointer[[]SubscriberID]
	mu          sync.Mutex
	refs        atomic.Int32 // holds a ref outside RLock
	recycled    atomic.Bool
	pooled      atomic.Bool   // CAS guard: exactly one goroutine may pool this channel
	gen         atomic.Uint64 // incremented on each pool reuse to prevent ABA races
	hist        *history      // nil unless the engine enables history
	maxSubs     int           // namespace subscriber cap, 0 = unlimited

	// idleReap and lastActive drive the sweeper's idle reaping: a channel
	// with zero subscribers and no publish for idleReap is discarded,
	// history included. Zero idleReap opts the channel out.
	idleReap   time.Duration
	lastActive atomic.Int64

	fanout namespace.FanoutMode
	rr     atomic.Uint64 // round-robin rotation cursor

	// prios and topCohort exist only in priority mode: prios maps each
	// subscriber to its rank, topCohort caches the highest-rank cohort so
	// the publish path reads one pointer instead of sorting.
	prios     map[SubscriberID]int
	topCohort atomic.Pointer[[]SubscriberID]
}

// newEpoch never returns zero: zero is the "no identity" sentinel in
// PublishResult and the pooled-channel state.
func newEpoch() uint64 {
	for {
		if e := rand.Uint64(); e != 0 {
			return e
		}
	}
}

var channelPool = sync.Pool{
	New: func() any {
		return &Channel{}
	},
}

func NewChannel(name string) *Channel {
	c := channelPool.Get().(*Channel)
	c.gen.Add(1) // invalidates any stale returnToPool calls
	c.name = name
	c.epoch = newEpoch()
	c.offset.Store(0)
	c.refs.Store(0)
	c.recycled.Store(false)
	c.pooled.Store(false)
	empty := make([]SubscriberID, 0)
	c.subscribers.Store(&empty)
	return c
}

// recycleChannel marks a Channel for return to the pool. If no readers hold
// a reference (refs == 0), the channel is pooled immediately. Otherwise, the
// last reader to call Release will pool it. The caller must hold the shard
// write lock and must have already removed the channel from the shard map.
//
// We set recycled BEFORE attempting to pool so that in-flight readers (who
// hold a ref via Acquire) can still read the subscriber list and complete
// delivery. The pooled CAS ensures exactly one goroutine clears and pools
// the channel, even if Release() races with this function.
//
// A generation counter prevents ABA races: if the channel is pooled by
// Release(), re-acquired by NewChannel (which increments gen), and then
// this function's deferred returnToPool runs, the stale generation will
// cause it to no-op instead of clearing a live channel.
func recycleChannel(c *Channel) {
	g := c.gen.Load()
	c.recycled.Store(true)
	if c.refs.Load() == 0 {
		c.returnToPool(g)
	}
	// Otherwise the last Release() call will handle it.
}

// returnToPool clears the channel's fields and returns it to the pool.
// The CAS on pooled ensures this runs exactly once, preventing a race
// between recycleChannel and Release. The generation check prevents ABA
// races where a channel has been re-acquired from the pool by NewChannel
// (which increments gen) before a stale returnToPool call executes.
func (c *Channel) returnToPool(expectedGen uint64) {
	if c.gen.Load() != expectedGen {
		return // channel was reused by NewChannel; this call is stale
	}
	if !c.pooled.CompareAndSwap(false, true) {
		return
	}
	c.name = ""
	c.epoch = 0
	c.hist = nil
	c.maxSubs = 0
	c.idleReap = 0
	c.fanout = namespace.Broadcast
	c.rr.Store(0)
	c.prios = nil
	c.topCohort.Store(nil)
	c.subscribers.Store(nil)
	channelPool.Put(c)
}

// Acquire increments the reader reference count. Must be called under the
// shard's RLock before releasing it, so that the channel cannot be recycled
// between the map lookup and the Acquire call.
func (c *Channel) Acquire() { c.refs.Add(1) }

// Release decrements the reader reference count. If the channel was marked
// for recycling and this is the last reader, the channel is returned to the pool.
//
// The generation must be captured BEFORE the decrement: while this reader
// holds its ref the channel cannot be pooled, so the loaded gen is provably
// the generation the reader acquired. Loading it after the decrement races
// a concurrent recycle→pool→reuse cycle, and a Release preempted in that
// window would read the reused channel's fresh gen, defeat the ABA guard,
// and wipe a live channel back into the pool.
func (c *Channel) Release() {
	g := c.gen.Load()
	if c.refs.Add(-1) == 0 && c.recycled.Load() {
		c.returnToPool(g)
	}
}

func (c *Channel) Subscribe(id SubscriberID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.subscribers.Load()
	if current == nil {
		current = &[]SubscriberID{}
	}

	if slices.Contains(*current, id) {
		return false
	}

	needed := len(*current) + 1
	newSubs := getSubscriberSlice(needed)
	newSubs = append(newSubs, *current...)
	newSubs = append(newSubs, id)

	c.subscribers.Store(&newSubs)
	// Note: old slice is NOT returned to the pool because concurrent
	// publishers may still be iterating it via Subscribers(). The GC
	// will reclaim it once all readers finish.

	return true
}

func (c *Channel) Unsubscribe(id SubscriberID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.subscribers.Load()
	if current == nil || len(*current) == 0 {
		return false
	}

	idx := -1
	for i, existing := range *current {
		if existing == id {
			idx = i
			break
		}
	}

	if idx == -1 {
		return false
	}

	newLen := len(*current) - 1
	newSubs := getSubscriberSlice(newLen)
	newSubs = append(newSubs, (*current)[:idx]...)
	newSubs = append(newSubs, (*current)[idx+1:]...)

	c.subscribers.Store(&newSubs)
	// Note: old slice is NOT returned to the pool because concurrent
	// publishers may still be iterating it via Subscribers(). The GC
	// will reclaim it once all readers finish.

	return true
}

func (c *Channel) Subscribers() []SubscriberID {
	ptr := c.subscribers.Load()

	if ptr == nil {
		return nil
	}

	return *ptr
}

// setPriority records a subscriber's rank and rebuilds the cached top
// cohort. Caller must already hold the subscription serialization the
// engine provides (shard lock); c.mu guards against concurrent readers.
func (c *Channel) setPriority(id SubscriberID, prio int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prios == nil {
		c.prios = make(map[SubscriberID]int)
	}
	c.prios[id] = prio
	c.rebuildTopCohortLocked()
}

func (c *Channel) clearPriority(id SubscriberID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prios == nil {
		return
	}
	delete(c.prios, id)
	c.rebuildTopCohortLocked()
}

func (c *Channel) rebuildTopCohortLocked() {
	if len(c.prios) == 0 {
		empty := make([]SubscriberID, 0)
		c.topCohort.Store(&empty)
		return
	}
	found := false
	best := 0
	for _, p := range c.prios {
		if !found || p > best {
			best = p
			found = true
		}
	}
	cohort := make([]SubscriberID, 0, len(c.prios))
	for id, p := range c.prios {
		if p == best {
			cohort = append(cohort, id)
		}
	}
	slices.Sort(cohort)
	c.topCohort.Store(&cohort)
}

func (c *Channel) SubscriberCount() int {
	ptr := c.subscribers.Load()

	if ptr == nil {
		return 0
	}

	return len(*ptr)
}
