package engine

import (
	"sync"
	"sync/atomic"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

// fanoutResult holds per-chunk delivery counts, accumulated by parallel workers.
// Sized and aligned to avoid false sharing between workers writing to adjacent
// slots in the results array.
type fanoutResult struct {
	delivered int
	dropped   int
	_         [cacheline.Size - 16]byte // pad to cache line
}

// fanoutJob describes a unit of work dispatched to the persistent worker pool.
type fanoutJob struct {
	chunk   []SubscriberID
	d       Delivery
	exclude SubscriberID
	deliver DeliveryFunc
	result  *fanoutResult
	wg      *sync.WaitGroup
}

// fanoutPool is a persistent goroutine pool that avoids per-publish goroutine
// spawn overhead. Workers are created once at engine startup and consume jobs
// from a shared channel, reusing their goroutine stacks across publishes.
type fanoutPool struct {
	jobs     chan fanoutJob
	done     chan struct{}
	stopped  atomic.Bool    // set when stop() is called, checked by submit
	workerWg sync.WaitGroup // tracks worker goroutine lifetimes
}

func newFanoutPool(workers int) *fanoutPool {
	p := &fanoutPool{
		// Buffer enough so that a single parallelFanout call can enqueue all
		// chunks without blocking. workers-1 is the max chunks dispatched
		// (chunk 0 runs on the caller goroutine).
		jobs: make(chan fanoutJob, workers*2),
		done: make(chan struct{}),
	}
	p.workerWg.Add(workers)
	for range workers {
		go p.worker()
	}
	return p
}

// submit sends a job to the worker pool. Returns false if the pool is
// shutting down, in which case the caller must handle the job's WaitGroup.
func (p *fanoutPool) submit(job fanoutJob) bool {
	if p.stopped.Load() {
		return false
	}
	select {
	case p.jobs <- job:
		return true
	case <-p.done:
		return false
	}
}

func (p *fanoutPool) worker() {
	defer p.workerWg.Done()
	for {
		select {
		case <-p.done:
			return
		case job := <-p.jobs:
			var d, dr int
			for _, id := range job.chunk {
				if id == job.exclude {
					continue
				}
				if job.deliver(id, job.d) {
					d++
				} else {
					dr++
				}
			}
			job.result.delivered = d
			job.result.dropped = dr
			job.wg.Done()
		}
	}
}

// stop signals all workers to exit, waits for them to finish, and drains
// any orphaned jobs left in the buffer (completing their WaitGroups so
// callers blocked on wg.Wait do not hang).
func (p *fanoutPool) stop() {
	p.stopped.Store(true)
	close(p.done)
	p.workerWg.Wait()

	// Drain orphaned jobs: workers may have exited via <-p.done before
	// consuming all buffered jobs.
	for {
		select {
		case job := <-p.jobs:
			job.wg.Done()
		default:
			return
		}
	}
}

// resultsPool avoids allocating a []fanoutResult on every parallelFanout call.
// The pool stores slices of the maximum worker count size; each call slices to
// the actual numChunks needed.
var resultsPool = sync.Pool{
	New: func() any {
		// Allocate for a generous default; callers with more workers will
		// fall through to make().
		s := make([]fanoutResult, 16)
		return &s
	},
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
	d Delivery,
	exclude SubscriberID,
	deliver DeliveryFunc,
) (delivered, dropped int) {
	n := len(subscribers)
	workers := e.config.fanoutWorkers

	// Determine chunk size: divide evenly, rounding up
	chunkSize := (n + workers - 1) / workers
	numChunks := (n + chunkSize - 1) / chunkSize

	// Get results slice from pool or allocate if too small
	var results []fanoutResult
	if ptr := resultsPool.Get().(*[]fanoutResult); len(*ptr) >= numChunks {
		results = (*ptr)[:numChunks]
		// zero the portion we'll use
		for i := range results {
			results[i].delivered = 0
			results[i].dropped = 0
		}
		defer func() { resultsPool.Put(ptr) }()
	} else {
		resultsPool.Put(ptr) // return the too-small slice to avoid leaking it
		results = make([]fanoutResult, numChunks)
	}

	var wg sync.WaitGroup

	// Dispatch chunks 1..N to the worker pool (chunk 0 runs on caller goroutine)
	if e.fanoutPool != nil {
		for i := 1; i < numChunks; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > n {
				end = n
			}
			wg.Add(1)
			if !e.fanoutPool.submit(fanoutJob{
				chunk:   subscribers[start:end],
				d:       d,
				exclude: exclude,
				deliver: deliver,
				result:  &results[i],
				wg:      &wg,
			}) {
				// Pool is shutting down; execute inline to avoid deadlock.
				var del, drp int
				for _, id := range subscribers[start:end] {
					if id == exclude {
						continue
					}
					if deliver(id, d) {
						del++
					} else {
						drp++
					}
				}
				results[i] = fanoutResult{delivered: del, dropped: drp}
				wg.Done()
			}
		}
	} else {
		// Fallback: spawn ad-hoc goroutines (for engines without a pool)
		for i := 1; i < numChunks; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > n {
				end = n
			}
			wg.Add(1)
			go func(idx int, chunk []SubscriberID) {
				defer wg.Done()
				var del, drp int
				for _, id := range chunk {
					if id == exclude {
						continue
					}
					if deliver(id, d) {
						del++
					} else {
						drp++
					}
				}
				results[idx] = fanoutResult{delivered: del, dropped: drp}
			}(i, subscribers[start:end])
		}
	}

	// Process chunk 0 on the caller's goroutine
	{
		end := chunkSize
		if end > n {
			end = n
		}
		chunk := subscribers[:end]
		var del, drp int
		for _, id := range chunk {
			if id == exclude {
				continue
			}
			if deliver(id, d) {
				del++
			} else {
				drp++
			}
		}
		results[0] = fanoutResult{delivered: del, dropped: drp}
	}

	wg.Wait()

	for i := range numChunks {
		delivered += results[i].delivered
		dropped += results[i].dropped
	}
	return delivered, dropped
}
