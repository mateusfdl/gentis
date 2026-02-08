package relay

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type Deduplicator struct {
	seen   sync.Map
	ttl    time.Duration
	window time.Duration
	done   chan struct{}
}

type dedupEntry struct {
	timestamp atomic.Int64 
}

func NewDeduplicator(ttl time.Duration) *Deduplicator {
	d := &Deduplicator{
		ttl:    ttl,
		window: ttl / 2,
		done:   make(chan struct{}),
	}
	go d.cleanup()
	return d
}

func (d *Deduplicator) Check(channel string, data []byte) bool {
	key := d.createKey(channel, data)
	now := time.Now().UnixNano()

	newEntry := &dedupEntry{}
	newEntry.timestamp.Store(now)

	if val, loaded := d.seen.LoadOrStore(key, newEntry); loaded {
		entry := val.(*dedupEntry)
		ts := entry.timestamp.Load()
		if now-ts < int64(d.ttl) {
			return false
		}
		entry.timestamp.Store(now)
	}
	return true
}

func (d *Deduplicator) Stop() {
	close(d.done)
}

func (d *Deduplicator) createKey(channel string, data []byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte(channel))
	h.Write(data)

	// Include time window to allow same message after TTL
	window := time.Now().Unix() / int64(d.window.Seconds())
	windowBytes := []byte{
		byte(window >> 56), byte(window >> 48),
		byte(window >> 40), byte(window >> 32),
		byte(window >> 24), byte(window >> 16),
		byte(window >> 8), byte(window),
	}
	h.Write(windowBytes)
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
			d.seen.Range(func(key, value any) bool {
				entry := value.(*dedupEntry)
				if now-entry.timestamp.Load() > cutoff {
					d.seen.Delete(key)
				}
				return true
			})
		}
	}
}
