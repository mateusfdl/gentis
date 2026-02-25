package engine

import "sync"

// fanoutResult holds per-chunk delivery counts, accumulated by parallel workers.
// Sized and aligned to avoid false sharing between workers writing to adjacent
// slots in the results array.
type fanoutResult struct {
	delivered int
	dropped   int
	_         [cacheLineSize - 16]byte // pad to cache line
}

// parallelFanout splits the subscriber slice into chunks and delivers in parallel.
// The calling goroutine processes the first chunk directly (saving one goroutine spawn),
// while additional goroutines handle the remaining chunks.
//
// This is beneficial when len(subscribers) is large AND the deliver callback has
// non-trivial per-call cost (e.g., sync.Map lookup + channel send). For trivially
// fast deliver callbacks, the goroutine coordination overhead may negate the benefit.
func (e *Engine) parallelFanout(
	subscribers []SubscriberID,
	channel string,
	data []byte,
	exclude SubscriberID,
	deliver DeliveryFunc,
) (delivered, dropped int) {
	n := len(subscribers)
	workers := e.config.fanoutWorkers

	// Determine chunk size: divide evenly, rounding up
	chunkSize := (n + workers - 1) / workers
	numChunks := (n + chunkSize - 1) / chunkSize

	results := make([]fanoutResult, numChunks)

	var wg sync.WaitGroup

	// Spawn workers for chunks 1..N (chunk 0 runs on the caller's goroutine)
	for i := 1; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}

		wg.Add(1)
		go func(idx int, chunk []SubscriberID) {
			defer wg.Done()
			var d, dr int
			for _, id := range chunk {
				if id == exclude {
					continue
				}
				if deliver(id, channel, data) {
					d++
				} else {
					dr++
				}
			}
			results[idx] = fanoutResult{delivered: d, dropped: dr}
		}(i, subscribers[start:end])
	}

	// Process chunk 0 on the caller's goroutine
	{
		end := chunkSize
		if end > n {
			end = n
		}
		chunk := subscribers[:end]
		var d, dr int
		for _, id := range chunk {
			if id == exclude {
				continue
			}
			if deliver(id, channel, data) {
				d++
			} else {
				dr++
			}
		}
		results[0] = fanoutResult{delivered: d, dropped: dr}
	}

	wg.Wait()

	for i := range numChunks {
		delivered += results[i].delivered
		dropped += results[i].dropped
	}
	return delivered, dropped
}
