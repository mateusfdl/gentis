package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// history is a bounded per-channel ring of published messages, addressed by
// the channel's monotonic offset. Entries hold the same []byte handed to
// fanout: publishers must treat payloads as immutable after Publish, so the
// ring never copies on the hot path.
//
// Offsets in the ring are always contiguous: every publish appends, capacity
// overflow evicts from the tail, and the TTL sweep only trims the tail.
type history struct {
	mu         sync.Mutex
	entries    []historyItem
	head       int // index of the next write
	count      int
	ttl        time.Duration
	lastOffset uint64 // survives sweeps so "up to date" stays answerable
}

type historyItem struct {
	offset   uint64
	data     []byte
	storedAt int64
}

func newHistory(capacity int, ttl time.Duration) *history {
	return &history{
		entries: make([]historyItem, capacity),
		ttl:     ttl,
	}
}

// appendNext assigns the next offset from seq and appends under one lock:
// concurrent publishers would otherwise interleave assignment and append,
// regressing lastOffset and breaking the contiguity replay depends on.
func (h *history) appendNext(seq *atomic.Uint64, data []byte, now int64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	offset := seq.Add(1)
	h.entries[h.head] = historyItem{offset: offset, data: data, storedAt: now}
	h.head = (h.head + 1) % len(h.entries)
	if h.count < len(h.entries) {
		h.count++
	}
	h.lastOffset = offset
	return offset
}

// replayN returns the items with offset > fromOffset, oldest first. ok is
// false when the gap cannot be filled: the client is behind the retained
// tail (evicted or expired entries) or claims an offset the channel never
// assigned. max caps the returned items; zero means unbounded.
func (h *history) replayN(fromOffset uint64, max int) ([]historyItem, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if fromOffset == h.lastOffset {
		return nil, true
	}
	if fromOffset > h.lastOffset {
		return nil, false
	}
	if h.count == 0 {
		return nil, false
	}

	tail := (h.head - h.count + len(h.entries)) % len(h.entries)
	oldest := h.entries[tail].offset
	if fromOffset+1 < oldest {
		return nil, false
	}

	want := int(h.lastOffset - fromOffset)
	if max > 0 && max < want {
		want = max
	}
	items := make([]historyItem, 0, want)
	for i := 0; i < h.count; i++ {
		if max > 0 && len(items) >= max {
			break
		}
		item := h.entries[(tail+i)%len(h.entries)]
		if item.offset > fromOffset {
			items = append(items, item)
		}
	}
	return items, true
}

// sweep trims entries stored before now-ttl. A zero TTL disables expiry.
func (h *history) sweep(now int64) {
	if h.ttl <= 0 {
		return
	}
	cutoff := now - int64(h.ttl)

	h.mu.Lock()
	defer h.mu.Unlock()

	for h.count > 0 {
		tail := (h.head - h.count + len(h.entries)) % len(h.entries)
		if h.entries[tail].storedAt > cutoff {
			return
		}
		h.entries[tail] = historyItem{}
		h.count--
	}
}

func (h *history) size() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}
