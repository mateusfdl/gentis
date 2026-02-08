package relay

import (
	"hash/fnv"
	"sync"
	"time"
)

type Deduplicator struct {
	seen   sync.Map
	ttl    time.Duration
	window time.Duration
	done   chan struct{}
}

type dedupEntry struct {
	timestamp time.Time
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
	now := time.Now()

	if val, loaded := d.seen.LoadOrStore(key, &dedupEntry{timestamp: now}); loaded {
		entry := val.(*dedupEntry)
		if now.Sub(entry.timestamp) < d.ttl {
			return false
		}
		entry.timestamp = now
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
			now := time.Now()
			d.seen.Range(func(key, value any) bool {
				entry := value.(*dedupEntry)
				if now.Sub(entry.timestamp) > d.ttl*2 {
					d.seen.Delete(key)
				}
				return true
			})
		}
	}
}
