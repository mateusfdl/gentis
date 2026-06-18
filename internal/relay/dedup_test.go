package relay

import (
	"sync"
	"testing"
	"time"
)

func dedupLen(d *Deduplicator) int {
	total := 0
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.RLock()
		total += len(sh.seen)
		sh.mu.RUnlock()
	}
	return total
}

func TestDedupFirstCallAllowed(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel", 7, 1) {
		t.Error("first Check should return true")
	}
}

func TestDedupDuplicateBlocked(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	d.Check("channel", 7, 1)

	if d.Check("channel", 7, 1) {
		t.Error("duplicate Check within TTL should return false")
	}
}

func TestDedupDifferentChannelsIndependent(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel-a", 7, 1) {
		t.Error("first check on channel-a should return true")
	}

	if !d.Check("channel-b", 7, 1) {
		t.Error("first check on channel-b should return true (different channel)")
	}
}

func TestDedupDifferentOffsetsIndependent(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel", 7, 1) {
		t.Error("first check with offset 1 should return true")
	}

	if !d.Check("channel", 7, 2) {
		t.Error("first check with offset 2 should return true (different offset)")
	}
}

func TestDedupDifferentEpochsIndependent(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel", 7, 1) {
		t.Error("first check with epoch 7 should return true")
	}

	if !d.Check("channel", 9, 1) {
		t.Error("first check with epoch 9 should return true (origin restarted)")
	}
}

func TestDedupAfterTTLExpiry(t *testing.T) {
	d := NewDeduplicator(2 * time.Second)
	defer d.Stop()

	if !d.Check("channel", 7, 1) {
		t.Fatal("first check should return true")
	}

	if d.Check("channel", 7, 1) {
		t.Fatal("duplicate within TTL should return false")
	}

	time.Sleep(3 * time.Second)

	if !d.Check("channel", 7, 1) {
		t.Error("check after TTL expiry should return true")
	}
}

func TestDedupStop(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	d.Stop()
}

func TestDedupConcurrentCheck(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	var wg sync.WaitGroup
	allowed := 0
	var mu sync.Mutex

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d.Check("channel", 7, 42) {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if allowed != 1 {
		t.Errorf("expected exactly one concurrent Check of the same identity to pass, got %d", allowed)
	}
}

func TestDedupCleanupRemovesExpired(t *testing.T) {
	d := NewDeduplicator(2 * time.Second)
	defer d.Stop()

	d.Check("channel", 7, 1)

	time.Sleep(7 * time.Second)

	count := dedupLen(d)

	if count > 0 {
		t.Errorf("expected 0 entries after cleanup, got %d", count)
	}
}

func TestDedupEmptyChannel(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("", 7, 1) {
		t.Error("first check with empty channel should return true")
	}

	if d.Check("", 7, 1) {
		t.Error("duplicate with empty channel should return false")
	}
}

func TestDedupMultipleDuplicates(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("ch", 7, 3) {
		t.Error("first check should return true")
	}

	for i := range 10 {
		if d.Check("ch", 7, 3) {
			t.Errorf("duplicate %d should return false", i)
		}
	}
}
