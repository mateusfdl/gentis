package relay

import (
	"sync"
	"testing"
	"time"
)

// TODO: Deduplicator.createKey divides by int64(d.window.Seconds()), which
// truncates to 0 when window < 1s (TTL < 2s), causing a panic.

func TestDedupFirstCallAllowed(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel", []byte("data")) {
		t.Error("first Check should return true")
	}
}

func TestDedupDuplicateBlocked(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	d.Check("channel", []byte("data"))

	if d.Check("channel", []byte("data")) {
		t.Error("duplicate Check within TTL should return false")
	}
}

func TestDedupDifferentChannelsIndependent(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel-a", []byte("data")) {
		t.Error("first check on channel-a should return true")
	}

	if !d.Check("channel-b", []byte("data")) {
		t.Error("first check on channel-b should return true (different channel)")
	}
}

func TestDedupDifferentDataIndependent(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel", []byte("data-1")) {
		t.Error("first check with data-1 should return true")
	}

	if !d.Check("channel", []byte("data-2")) {
		t.Error("first check with data-2 should return true (different data)")
	}
}

func TestDedupAfterTTLExpiry(t *testing.T) {
	d := NewDeduplicator(2 * time.Second)
	defer d.Stop()

	if !d.Check("channel", []byte("data")) {
		t.Fatal("first check should return true")
	}

	if d.Check("channel", []byte("data")) {
		t.Fatal("duplicate within TTL should return false")
	}

	time.Sleep(3 * time.Second)

	if !d.Check("channel", []byte("data")) {
		t.Error("check after TTL expiry should return true")
	}
}

func TestDedupStop(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	d.Stop()
	// Should not panic or hang
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
			if d.Check("channel", []byte("same-data")) {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if allowed == 0 {
		t.Error("expected at least one Check to return true")
	}
	if allowed == 100 {
		t.Error("expected dedup to block some concurrent duplicates")
	}
}

func TestDedupCleanupRemovesExpired(t *testing.T) {
	d := NewDeduplicator(2 * time.Second)
	defer d.Stop()

	d.Check("channel", []byte("data"))

	// Wait for cleanup to run: ticker fires every TTL (2s), cutoff = 2*TTL (4s)
	// Entry must be >4s old. Wait 5s for entry to expire, then up to 2s for ticker.
	time.Sleep(7 * time.Second)

	count := 0
	d.seen.Range(func(_, _ any) bool {
		count++
		return true
	})

	if count > 0 {
		t.Errorf("expected 0 entries after cleanup, got %d", count)
	}
}

func TestDedupEmptyData(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("channel", []byte{}) {
		t.Error("first check with empty data should return true")
	}

	if d.Check("channel", []byte{}) {
		t.Error("duplicate with empty data should return false")
	}
}

func TestDedupEmptyChannel(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("", []byte("data")) {
		t.Error("first check with empty channel should return true")
	}

	if d.Check("", []byte("data")) {
		t.Error("duplicate with empty channel should return false")
	}
}

func TestDedupMultipleDuplicates(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("ch", []byte("msg")) {
		t.Error("first check should return true")
	}

	for i := range 10 {
		if d.Check("ch", []byte("msg")) {
			t.Errorf("duplicate %d should return false", i)
		}
	}
}
