package engine

import (
	"sync"
	"testing"
	"time"
)

func TestFanoutPoolStopDoesNotOrphanSubmittedJobs(t *testing.T) {
	const submitters = 8

	for iter := range 300 {
		p := newFanoutPool(4)

		var jobs sync.WaitGroup
		jobs.Add(submitters)

		var results [submitters]fanoutResult
		var launched sync.WaitGroup
		launched.Add(submitters)
		for i := range submitters {
			go func(i int) {
				defer launched.Done()
				job := fanoutJob{
					deliver: func(SubscriberID, Delivery) bool { return true },
					result:  &results[i],
					wg:      &jobs,
				}
				if !p.submit(job) {
					jobs.Done()
				}
			}(i)
		}

		p.stop()
		launched.Wait()

		done := make(chan struct{})
		go func() {
			jobs.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: stop() orphaned an accepted job; wg.Wait hung", iter)
		}
	}
}
