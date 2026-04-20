package relay

import (
	"encoding/binary"
	"hash/maphash"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

const numDedupShards = 16

// Deduplicator detects duplicate messages using time-windowed hashing.
// It uses a sharded map (instead of sync.Map) for better write performance
// and memory reclamation via maybeRebuild.
type Deduplicator struct {
	shards [numDedupShards]dedupShard
	seed   maphash.Seed
	ttl    time.Duration
	window time.Duration
	done   chan struct{}
	hits   atomic.Int64
	misses atomic.Int64
}

type dedupShard struct {
	mu   sync.RWMutex
	seen map[uint64]int64 // hash -> timestamp (UnixNano)
	peak int
	_    [cacheline.Size]byte // tail pad: keeps this shard's mutex off the next shard's cache line
}

func (sh *dedupShard) maybeRebuild() {
	if sh.peak > 64 && len(sh.seen) < sh.peak/4 {
		rebuilt := make(map[uint64]int64, len(sh.seen))
		maps.Copy(rebuilt, sh.seen)
		sh.seen = rebuilt
		sh.peak = len(sh.seen)
	}
}

func NewDeduplicator(ttl time.Duration) *Deduplicator {
	d := &Deduplicator{
		seed:   maphash.MakeSeed(),
		ttl:    ttl,
		window: ttl / 2,
		done:   make(chan struct{}),
	}
	for i := range d.shards {
		d.shards[i].seen = make(map[uint64]int64)
	}
	go d.cleanup()
	return d
}

// Check returns true if the message is unique (not a duplicate).
// Uses a Load-first pattern to avoid allocating on the common (duplicate) path.
func (d *Deduplicator) Check(channel string, data []byte) bool {
	key := d.createKey(channel, data)
	now := time.Now().UnixNano()

	sh := &d.shards[key%numDedupShards]

	// Fast path: read-lock check for existing entry
	sh.mu.RLock()
	if ts, ok := sh.seen[key]; ok {
		if now-ts < int64(d.ttl) {
			sh.mu.RUnlock()
			d.hits.Add(1)
			return false
		}
	}
	sh.mu.RUnlock()

	// Slow path: write-lock to insert or update.
	// Re-capture now after acquiring the write lock so stored timestamps
	// are fresh (the read-lock fast path above may have been delayed).
	sh.mu.Lock()
	now = time.Now().UnixNano()
	// Double-check after acquiring write lock
	if ts, ok := sh.seen[key]; ok {
		if now-ts < int64(d.ttl) {
			sh.mu.Unlock()
			d.hits.Add(1)
			return false
		}
		sh.seen[key] = now
		sh.mu.Unlock()
		d.misses.Add(1)
		return true
	}

	sh.seen[key] = now
	if len(sh.seen) > sh.peak {
		sh.peak = len(sh.seen)
	}
	sh.mu.Unlock()

	d.misses.Add(1)
	return true
}

func (d *Deduplicator) DedupHits() int64 {
	return d.hits.Load()
}

func (d *Deduplicator) DedupMisses() int64 {
	return d.misses.Load()
}

func (d *Deduplicator) Stop() {
	close(d.done)
}

// Len returns the total number of entries across all shards.
// Intended for testing and monitoring, not for hot-path use.
func (d *Deduplicator) Len() int {
	total := 0
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.RLock()
		total += len(sh.seen)
		sh.mu.RUnlock()
	}
	return total
}

// createKey uses maphash (hardware-accelerated on amd64) instead of fnv.New64a().
// No allocations: maphash.Bytes and maphash.String are single function calls.
func (d *Deduplicator) createKey(channel string, data []byte) uint64 {
	// Combine channel + data + time window into a single hash.
	// We use a two-step hash: hash(channel+data) mixed with the time window.
	var h maphash.Hash
	h.SetSeed(d.seed)
	h.WriteString(channel)
	h.Write(data)

	// Include time window to allow same message after TTL.
	// Guard against zero window (when TTL < 2s).
	windowSecs := int64(d.window.Seconds())
	if windowSecs == 0 {
		windowSecs = 1
	}
	window := time.Now().Unix() / windowSecs
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(window))
	h.Write(buf[:])

	return h.Sum64()
}

func (d *Deduplicator) cleanup() {
	ticker := time.NewTicker(d.ttl)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			cutoff := int64(d.ttl * 2)
			for i := range d.shards {
				sh := &d.shards[i]
				sh.mu.Lock()
				for key, ts := range sh.seen {
					if now-ts > cutoff {
						delete(sh.seen, key)
					}
				}
				sh.maybeRebuild()
				sh.mu.Unlock()
			}
		}
	}
}
