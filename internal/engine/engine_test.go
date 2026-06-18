package engine

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/namespace"
)

func engineSubscribers(e *Engine, channel string) []SubscriberID {
	s := e.getShard(channel)
	s.mu.RLock()
	ch := s.channels[channel]
	if ch != nil {
		ch.Acquire()
	}
	s.mu.RUnlock()

	if ch == nil {
		return nil
	}
	defer ch.Release()
	return ch.Subscribers()
}

func engineSubscriberCount(e *Engine, channel string) int {
	subs := engineSubscribers(e, channel)
	return len(subs)
}

func TestSubscribeAndUnsubscribe(t *testing.T) {
	e := New()

	if e.Subscribe(1, "test-channel") != nil {
		t.Error("expected first subscribe to return true")
	}

	if e.Subscribe(1, "test-channel") == nil {
		t.Error("expected duplicate subscribe to return false")
	}

	if int(e.Stats().Channels) != 1 {
		t.Errorf("expected 1 channel, got %d", int(e.Stats().Channels))
	}

	if engineSubscriberCount(e, "test-channel") != 1 {
		t.Errorf("expected 1 subscriber, got %d", engineSubscriberCount(e, "test-channel"))
	}

	if !e.Unsubscribe(1, "test-channel") {
		t.Error("expected unsubscribe to return true")
	}

	if e.Unsubscribe(1, "test-channel") {
		t.Error("expected second unsubscribe to return false")
	}

	if int(e.Stats().Channels) != 0 {
		t.Errorf("expected 0 channels after last unsubscribe, got %d", int(e.Stats().Channels))
	}
}

func TestGetSubscribers(t *testing.T) {
	e := New()

	e.Subscribe(1, "chat")
	e.Subscribe(2, "chat")
	e.Subscribe(3, "chat")

	subs := engineSubscribers(e, "chat")
	if len(subs) != 3 {
		t.Errorf("expected 3 subscribers, got %d", len(subs))
	}

	found := make(map[SubscriberID]bool)
	for _, id := range subs {
		found[id] = true
	}

	for _, id := range []SubscriberID{1, 2, 3} {
		if !found[id] {
			t.Errorf("expected subscriber %d to be in list", id)
		}
	}
}

func TestUnsubscribeAll(t *testing.T) {
	e := New()

	e.Subscribe(1, "channel-a")
	e.Subscribe(1, "channel-b")
	e.Subscribe(1, "channel-c")
	e.Subscribe(2, "channel-a")

	if int(e.Stats().TotalSubscribers) != 4 {
		t.Errorf("expected 4 subscriptions, got %d", int(e.Stats().TotalSubscribers))
	}

	e.UnsubscribeAll(1)

	if int(e.Stats().TotalSubscribers) != 1 {
		t.Errorf("expected 1 subscription after UnsubscribeAll, got %d", int(e.Stats().TotalSubscribers))
	}

	if engineSubscriberCount(e, "channel-a") != 1 {
		t.Errorf("expected 1 subscriber in channel-a, got %d", engineSubscriberCount(e, "channel-a"))
	}

	if int(e.Stats().Channels) != 1 {
		t.Errorf("expected 1 channel, got %d", int(e.Stats().Channels))
	}
}

func TestPublish(t *testing.T) {
	e := New()

	e.Subscribe(1, "news")
	e.Subscribe(2, "news")
	e.Subscribe(3, "news")

	delivered := make(map[SubscriberID]bool)
	var mu sync.Mutex

	result := e.Publish("news", []byte("hello"), 1, func(id SubscriberID, d Delivery) bool {
		mu.Lock()
		delivered[id] = true
		mu.Unlock()
		return true
	})

	if result.Delivered != 2 {
		t.Errorf("expected 2 delivered, got %d", result.Delivered)
	}

	if delivered[1] {
		t.Error("publisher (id=1) should be excluded")
	}

	if !delivered[2] || !delivered[3] {
		t.Error("subscribers 2 and 3 should receive message")
	}
}

func TestPublishWithDrops(t *testing.T) {
	e := New()

	e.Subscribe(1, "test")
	e.Subscribe(2, "test")

	result := e.Publish("test", []byte("data"), 0, func(id SubscriberID, d Delivery) bool {
		return id != 2
	})

	if result.Delivered != 1 {
		t.Errorf("expected 1 delivered, got %d", result.Delivered)
	}

	if result.Dropped != 1 {
		t.Errorf("expected 1 dropped, got %d", result.Dropped)
	}
}

func TestPublishToNonexistentChannel(t *testing.T) {
	e := New()

	result := e.Publish("nonexistent", []byte("data"), 0, func(id SubscriberID, d Delivery) bool {
		t.Error("delivery function should not be called")
		return true
	})

	if result.Delivered != 0 || result.Dropped != 0 {
		t.Error("expected zero deliveries for nonexistent channel")
	}
}

func TestStats(t *testing.T) {
	e := New()

	e.Subscribe(1, "ch1")
	e.Subscribe(2, "ch1")
	e.Subscribe(3, "ch2")

	e.Publish("ch1", []byte("msg"), 0, func(id SubscriberID, d Delivery) bool {
		return true
	})

	stats := e.Stats()

	if stats.Channels != 2 {
		t.Errorf("expected 2 channels, got %d", stats.Channels)
	}

	if stats.TotalSubscribers != 3 {
		t.Errorf("expected 3 subscribers, got %d", stats.TotalSubscribers)
	}

	if stats.MessagesPublished != 1 {
		t.Errorf("expected 1 message published, got %d", stats.MessagesPublished)
	}

	if stats.MessagesDelivered != 2 {
		t.Errorf("expected 2 messages delivered, got %d", stats.MessagesDelivered)
	}
}

func TestConcurrentSubscribe(t *testing.T) {
	e := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			e.Subscribe(SubscriberID(id), "concurrent-channel")
		}(i)
	}

	wg.Wait()

	if engineSubscriberCount(e, "concurrent-channel") != 100 {
		t.Errorf("expected 100 subscribers, got %d", engineSubscriberCount(e, "concurrent-channel"))
	}
}

func TestConcurrentPublish(t *testing.T) {
	e := New()

	for i := 0; i < 100; i++ {
		e.Subscribe(SubscriberID(i), "pub-channel")
	}

	var wg sync.WaitGroup
	var deliveries int64
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := e.Publish("pub-channel", []byte("msg"), 0, func(id SubscriberID, d Delivery) bool {
				return true
			})
			mu.Lock()
			deliveries += int64(result.Delivered)
			mu.Unlock()
		}()
	}

	wg.Wait()

	if deliveries != 50*99 {
		t.Errorf("expected %d total deliveries, got %d", 50*99, deliveries)
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	e := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			e.Subscribe(SubscriberID(id), "mixed-channel")
		}(i)
		go func(id int) {
			defer wg.Done()
			e.Unsubscribe(SubscriberID(id), "mixed-channel")
		}(i)
	}

	wg.Wait()
}

func TestWithShards(t *testing.T) {
	e := New(WithShards(64))

	if len(e.shards) != 64 {
		t.Errorf("expected 64 shards, got %d", len(e.shards))
	}
}

func TestNextPowerOf2HandlesLarge64BitInts(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires 64-bit int")
	}

	got := nextPowerOf2(1<<33 + 1)
	want := 1 << 34
	if got != want {
		t.Fatalf("nextPowerOf2(1<<33 + 1) = %d, want %d", got, want)
	}
}

func TestChannelAtomicSubscribers(t *testing.T) {
	ch := NewChannel("test")

	ch.Subscribe(1)
	ch.Subscribe(2)
	ch.Subscribe(3)

	subs := ch.Subscribers()
	if len(subs) != 3 {
		t.Errorf("expected 3 subscribers, got %d", len(subs))
	}

	ch.Unsubscribe(2)

	subs = ch.Subscribers()
	if len(subs) != 2 {
		t.Errorf("expected 2 subscribers after unsubscribe, got %d", len(subs))
	}

	found := make(map[SubscriberID]bool)
	for _, id := range subs {
		found[id] = true
	}

	if found[2] {
		t.Error("subscriber 2 should have been removed")
	}
}

func TestSubscriptionTracker(t *testing.T) {
	s := newSubscriptions()

	s.Add(1, "ch1")
	s.Add(1, "ch2")
	s.Add(1, "ch3")

	channels := s.GetChannels(1)
	if len(channels) != 3 {
		t.Errorf("expected 3 subscriptions, got %d", len(channels))
	}

	if !slices.Contains(channels, "ch2") {
		t.Error("expected subscription to ch2")
	}

	s.Remove(1, "ch2")

	channels = s.GetChannels(1)
	if slices.Contains(channels, "ch2") {
		t.Error("ch2 should be removed")
	}
	if len(channels) != 2 {
		t.Errorf("expected 2 channels, got %d", len(channels))
	}

	s.RemoveAll(1)

	channels = s.GetChannels(1)
	if len(channels) != 0 {
		t.Errorf("expected 0 subscriptions after RemoveAll, got %d", len(channels))
	}
}

func TestSubscribersNonexistentChannel(t *testing.T) {
	e := New()

	subs := engineSubscribers(e, "nonexistent")
	if subs != nil {
		t.Errorf("expected nil for nonexistent channel, got %v", subs)
	}
}

func TestSubscriberCountNonexistentChannel(t *testing.T) {
	e := New()

	if engineSubscriberCount(e, "nonexistent") != 0 {
		t.Errorf("expected 0 for nonexistent channel, got %d", engineSubscriberCount(e, "nonexistent"))
	}
}

func TestUnsubscribeAllNoSubscriptions(t *testing.T) {
	e := New()

	e.UnsubscribeAll(999)

	if int(e.Stats().TotalSubscribers) != 0 {
		t.Errorf("expected 0 subscriptions, got %d", int(e.Stats().TotalSubscribers))
	}
}

func TestStatsDropped(t *testing.T) {
	e := New()

	e.Subscribe(1, "ch")
	e.Subscribe(2, "ch")

	e.Publish("ch", []byte("msg"), 0, func(id SubscriberID, d Delivery) bool {
		return id != 2
	})

	stats := e.Stats()

	if stats.MessagesDropped != 1 {
		t.Errorf("expected 1 message dropped, got %d", stats.MessagesDropped)
	}

	if stats.MessagesDelivered != 1 {
		t.Errorf("expected 1 message delivered, got %d", stats.MessagesDelivered)
	}
}

func TestPublishNoExclude(t *testing.T) {
	e := New()

	e.Subscribe(1, "ch")
	e.Subscribe(2, "ch")
	e.Subscribe(3, "ch")

	result := e.Publish("ch", []byte("msg"), 0, func(id SubscriberID, d Delivery) bool {
		return true
	})

	if result.Delivered != 3 {
		t.Errorf("expected 3 delivered (no exclude), got %d", result.Delivered)
	}
}

func TestPublishResultChannel(t *testing.T) {
	e := New()

	e.Subscribe(1, "my-channel")

	result := e.Publish("my-channel", []byte("msg"), 0, func(id SubscriberID, d Delivery) bool {
		return true
	})

	if result.Channel != "my-channel" {
		t.Errorf("expected result.Channel 'my-channel', got %q", result.Channel)
	}
}

func TestPublishDeliveryFuncReceivesCorrectArgs(t *testing.T) {
	e := New()

	e.Subscribe(1, "test-ch")

	e.Publish("test-ch", []byte("payload"), 0, func(id SubscriberID, d Delivery) bool {
		if id != 1 {
			t.Errorf("expected subscriber ID 1, got %d", id)
		}
		if d.Channel != "test-ch" {
			t.Errorf("expected channel 'test-ch', got %q", d.Channel)
		}
		if string(d.Data) != "payload" {
			t.Errorf("expected data 'payload', got %q", string(d.Data))
		}
		return true
	})
}

func TestConcurrentPublishMultipleChannels(t *testing.T) {
	e := New()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			e.Subscribe(SubscriberID(j), "channel-"+string(rune('a'+i)))
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(ch string) {
			defer wg.Done()
			e.Publish(ch, []byte("msg"), 0, func(id SubscriberID, d Delivery) bool {
				return true
			})
		}("channel-" + string(rune('a'+i)))
	}

	wg.Wait()

	stats := e.Stats()
	if stats.MessagesPublished != 10 {
		t.Errorf("expected 10 messages published, got %d", stats.MessagesPublished)
	}
}

func TestWithShardsZero(t *testing.T) {
	e := New(WithShards(0))
	expected := defaultConfig().numShards

	if len(e.shards) != expected {
		t.Errorf("expected %d shards for zero value, got %d", expected, len(e.shards))
	}
}

func TestWithShardsNegative(t *testing.T) {
	e := New(WithShards(-1))
	expected := defaultConfig().numShards

	if len(e.shards) != expected {
		t.Errorf("expected %d shards for negative value, got %d", expected, len(e.shards))
	}
}

func TestChannelEmptyAfterCreation(t *testing.T) {
	ch := NewChannel("test")

	if ch.SubscriberCount() != 0 {
		t.Errorf("expected 0 subscribers for new channel, got %d", ch.SubscriberCount())
	}
}

func TestChannelNotEmptyAfterSubscribe(t *testing.T) {
	ch := NewChannel("test")
	ch.Subscribe(1)

	if ch.SubscriberCount() != 1 {
		t.Errorf("expected 1 subscriber, got %d", ch.SubscriberCount())
	}
}

func TestChannelSubscribeIdempotent(t *testing.T) {
	ch := NewChannel("test")

	if !ch.Subscribe(1) {
		t.Error("first subscribe should return true")
	}

	if ch.Subscribe(1) {
		t.Error("second subscribe should return false")
	}

	if ch.SubscriberCount() != 1 {
		t.Errorf("expected 1 subscriber, got %d", ch.SubscriberCount())
	}
}

func TestChannelUnsubscribeNonexistent(t *testing.T) {
	ch := NewChannel("test")

	if ch.Unsubscribe(999) {
		t.Error("unsubscribe non-existent should return false")
	}
}

func TestSubscriptionTrackerGetChannelsEmpty(t *testing.T) {
	s := newSubscriptions()

	channels := s.GetChannels(999)
	if channels != nil {
		t.Errorf("expected nil for unknown subscriber, got %v", channels)
	}
}

func TestSubscriptionTrackerRemoveUnknown(t *testing.T) {
	s := newSubscriptions()

	s.Remove(999, "ch")
	s.RemoveAll(999)
}

func TestMultipleSubscribersMultipleChannels(t *testing.T) {
	e := New()

	e.Subscribe(1, "ch1")
	e.Subscribe(1, "ch2")
	e.Subscribe(2, "ch1")
	e.Subscribe(2, "ch3")

	if int(e.Stats().Channels) != 3 {
		t.Errorf("expected 3 channels, got %d", int(e.Stats().Channels))
	}

	if int(e.Stats().TotalSubscribers) != 4 {
		t.Errorf("expected 4 subscriptions, got %d", int(e.Stats().TotalSubscribers))
	}

	e.UnsubscribeAll(1)

	if int(e.Stats().Channels) != 2 {
		t.Errorf("expected 2 channels after UnsubscribeAll(1), got %d", int(e.Stats().Channels))
	}

	if int(e.Stats().TotalSubscribers) != 2 {
		t.Errorf("expected 2 subscriptions, got %d", int(e.Stats().TotalSubscribers))
	}
}

func TestStatsAccumulate(t *testing.T) {
	e := New()

	e.Subscribe(1, "ch")
	e.Subscribe(2, "ch")

	deliver := func(id SubscriberID, d Delivery) bool { return true }

	e.Publish("ch", []byte("msg1"), 0, deliver)
	e.Publish("ch", []byte("msg2"), 0, deliver)
	e.Publish("ch", []byte("msg3"), 1, deliver)

	stats := e.Stats()

	if stats.MessagesPublished != 3 {
		t.Errorf("expected 3 published, got %d", stats.MessagesPublished)
	}

	if stats.MessagesDelivered != 5 {
		t.Errorf("expected 5 delivered, got %d", stats.MessagesDelivered)
	}
}

// TestConcurrentPublishDuringUnsubscribe exercises the channel refcount fix.
// Without the Acquire/Release mechanism in Publish, the race detector catches
// a use-after-free on the recycled Channel pointer.
func TestConcurrentPublishDuringUnsubscribe(t *testing.T) {
	const (
		numPublishers = 8
		iterations    = 5_000
	)

	e := New()
	deliver := func(id SubscriberID, d Delivery) bool { return true }

	var wg sync.WaitGroup

	// Publishers continuously publish to the channel.
	for p := 0; p < numPublishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				e.Publish("race-channel", []byte("msg"), 0, deliver)
			}
		}()
	}

	// Concurrently subscribe and unsubscribe the sole subscriber,
	// which triggers channel creation/recycling on each cycle.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			e.Subscribe(1, "race-channel")
			e.Unsubscribe(1, "race-channel")
		}
	}()

	wg.Wait()
}

// TestStopDuringPublish verifies that Engine.Stop() completes without
// deadlocking when a publish with parallel fan-out is in progress.
func TestStopDuringPublish(t *testing.T) {
	e := New(WithFanoutThreshold(0), WithFanoutWorkers(4))

	for i := 0; i < 100; i++ {
		e.Subscribe(SubscriberID(i), "stop-channel")
	}

	var publishing atomic.Bool
	publishing.Store(true)

	// Slow delivery callback to keep the fan-out in progress.
	deliver := func(id SubscriberID, d Delivery) bool {
		if publishing.Load() {
			time.Sleep(time.Microsecond)
		}
		return true
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Publish("stop-channel", []byte("msg"), 0, deliver)
	}()

	// Give the publish a moment to start fan-out, then stop.
	time.Sleep(time.Millisecond)
	publishing.Store(false)

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success — Stop() completed
	case <-time.After(5 * time.Second):
		t.Fatal("Engine.Stop() deadlocked")
	}

	wg.Wait()
}

func TestPublishAssignsMonotonicOffsetsWithinEpoch(t *testing.T) {
	e := New()
	defer e.Stop()

	e.Subscribe(1, "metrics")

	var deliveries []Delivery
	deliver := func(id SubscriberID, d Delivery) bool {
		deliveries = append(deliveries, d)
		return true
	}

	for i := range 3 {
		result := e.Publish("metrics", []byte("payload"), 0, deliver)
		if result.Offset != uint64(i+1) {
			t.Errorf("publish %d: expected result offset %d, got %d", i, i+1, result.Offset)
		}
		if result.Epoch == 0 {
			t.Errorf("publish %d: expected non-zero result epoch", i)
		}
	}

	if len(deliveries) != 3 {
		t.Fatalf("expected 3 deliveries, got %d", len(deliveries))
	}
	epoch := deliveries[0].Epoch
	for i, d := range deliveries {
		if d.Channel != "metrics" {
			t.Errorf("delivery %d: expected channel %q, got %q", i, "metrics", d.Channel)
		}
		if string(d.Data) != "payload" {
			t.Errorf("delivery %d: expected data %q, got %q", i, "payload", d.Data)
		}
		if d.Offset != uint64(i+1) {
			t.Errorf("delivery %d: expected offset %d, got %d", i, i+1, d.Offset)
		}
		if d.Epoch != epoch {
			t.Errorf("delivery %d: expected epoch %d, got %d", i, epoch, d.Epoch)
		}
	}
}

func TestChannelRecycleRegeneratesEpochAndRestartsOffset(t *testing.T) {
	e := New()
	defer e.Stop()

	deliver := func(id SubscriberID, d Delivery) bool { return true }

	e.Subscribe(1, "recycled")
	first := e.Publish("recycled", []byte("a"), 0, deliver)
	e.Unsubscribe(1, "recycled")

	e.Subscribe(1, "recycled")
	second := e.Publish("recycled", []byte("b"), 0, deliver)

	if second.Epoch == first.Epoch {
		t.Errorf("expected a new epoch after channel recycle, both are %d", first.Epoch)
	}
	if second.Offset != 1 {
		t.Errorf("expected offset to restart at 1 in the new epoch, got %d", second.Offset)
	}
}

func TestPublishToMissingChannelAssignsNoIdentity(t *testing.T) {
	e := New()
	defer e.Stop()

	result := e.Publish("missing", []byte("data"), 0, func(id SubscriberID, d Delivery) bool { return true })

	if result.Offset != 0 {
		t.Errorf("expected offset 0 for missing channel, got %d", result.Offset)
	}
	if result.Epoch != 0 {
		t.Errorf("expected zero epoch for missing channel, got %d", result.Epoch)
	}
}

func TestRecoverReplaysMissedPublications(t *testing.T) {
	e := New(WithHistory(8, 0))
	defer e.Stop()

	e.Subscribe(1, "hist-ch")

	var lastEpoch uint64
	for i := range 3 {
		r := e.Publish("hist-ch", []byte(fmt.Sprintf("m-%d", i)), 0, func(SubscriberID, Delivery) bool { return true })
		lastEpoch = r.Epoch
	}

	got, ok := e.Recover("hist-ch", 1, lastEpoch)
	if !ok {
		t.Fatal("Recover() ok = false, want true")
	}
	if len(got) != 2 {
		t.Fatalf("Recover() returned %d deliveries, want 2", len(got))
	}
	for i, d := range got {
		wantOffset := uint64(i + 2)
		if d.Offset != wantOffset {
			t.Errorf("delivery[%d].Offset = %d, want %d", i, d.Offset, wantOffset)
		}
		if d.Epoch != lastEpoch {
			t.Errorf("delivery[%d].Epoch = %d, want %d", i, d.Epoch, lastEpoch)
		}
		if d.Channel != "hist-ch" {
			t.Errorf("delivery[%d].Channel = %q, want %q", i, d.Channel, "hist-ch")
		}
		want := fmt.Sprintf("m-%d", i+1)
		if string(d.Data) != want {
			t.Errorf("delivery[%d].Data = %q, want %q", i, d.Data, want)
		}
	}
}

func TestRecoverRejectsEpochMismatch(t *testing.T) {
	e := New(WithHistory(8, 0))
	defer e.Stop()

	e.Subscribe(1, "hist-ch")
	r := e.Publish("hist-ch", []byte("x"), 0, func(SubscriberID, Delivery) bool { return true })

	if _, ok := e.Recover("hist-ch", 0, r.Epoch+1); ok {
		t.Fatal("Recover() with wrong epoch ok = true, want false")
	}
}

func TestRecoverWithoutHistoryIsUnrecoverable(t *testing.T) {
	e := New()
	defer e.Stop()

	e.Subscribe(1, "plain-ch")
	r := e.Publish("plain-ch", []byte("x"), 0, func(SubscriberID, Delivery) bool { return true })

	if _, ok := e.Recover("plain-ch", 0, r.Epoch); ok {
		t.Fatal("Recover() without history ok = true, want false")
	}
}

func TestRecoverMissingChannel(t *testing.T) {
	e := New(WithHistory(8, 0))
	defer e.Stop()

	if _, ok := e.Recover("ghost", 0, 1); ok {
		t.Fatal("Recover() on missing channel ok = true, want false")
	}
}

func TestRecoverContiguousUnderConcurrentPublishers(t *testing.T) {
	const goroutines, perGoroutine = 8, 400
	const total = goroutines * perGoroutine

	e := New(WithHistory(total, 0))
	defer e.Stop()

	e.Subscribe(1, "hot-ch")
	deliver := func(SubscriberID, Delivery) bool { return true }
	epoch := e.Publish("hot-ch", []byte("seed"), 0, deliver).Epoch

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				e.Publish("hot-ch", []byte("x"), 0, deliver)
			}
		}()
	}
	wg.Wait()

	got, ok := e.Recover("hot-ch", 1, epoch)
	if !ok {
		t.Fatal("Recover() ok = false, want true")
	}
	if len(got) != total {
		t.Fatalf("Recover() returned %d deliveries, want %d", len(got), total)
	}
	for i, d := range got {
		if want := uint64(i + 2); d.Offset != want {
			t.Fatalf("delivery[%d].Offset = %d, want %d", i, d.Offset, want)
		}
	}
}

func TestHistorySweeperExpiresEntries(t *testing.T) {
	e := New(WithHistory(8, 50*time.Millisecond))
	defer e.Stop()

	e.Subscribe(1, "ttl-ch")
	r := e.Publish("ttl-ch", []byte("x"), 0, func(SubscriberID, Delivery) bool { return true })

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := e.Recover("ttl-ch", 0, r.Epoch); !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("history entry never expired via TTL sweep")
}

func TestHistoryChannelSurvivesLastUnsubscribe(t *testing.T) {
	e := New(WithHistory(8, 0))
	defer e.Stop()

	e.Subscribe(1, "sticky-ch")
	r1 := e.Publish("sticky-ch", []byte("before"), 0, func(SubscriberID, Delivery) bool { return true })
	e.Unsubscribe(1, "sticky-ch")

	r2 := e.Publish("sticky-ch", []byte("after"), 0, func(SubscriberID, Delivery) bool { return true })
	if r2.Epoch != r1.Epoch {
		t.Fatalf("epoch changed across empty period: %d != %d", r2.Epoch, r1.Epoch)
	}
	if r2.Offset != 2 {
		t.Fatalf("offset = %d, want 2 (continuity across empty period)", r2.Offset)
	}

	got, ok := e.Recover("sticky-ch", 0, r1.Epoch)
	if !ok || len(got) != 2 {
		t.Fatalf("Recover = %d items ok=%v, want 2 items ok=true", len(got), ok)
	}
}

func TestSweeperReapsDrainedEmptyChannels(t *testing.T) {
	e := New(WithHistory(8, 30*time.Millisecond))
	defer e.Stop()

	e.Subscribe(1, "reap-ch")
	e.Publish("reap-ch", []byte("x"), 0, func(SubscriberID, Delivery) bool { return true })
	e.Unsubscribe(1, "reap-ch")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.Stats().Channels == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("drained empty channel never reaped, channels = %d", e.Stats().Channels)
}

func mustRegistry(reg *namespace.Registry, err error) *namespace.Registry {
	if err != nil {
		panic(err)
	}
	return reg
}

func idleReapRegistry(reap time.Duration) *namespace.Registry {
	return mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"metrics": {AllowPublish: true, HistorySize: 8, IdleReap: reap},
			"flow":    {AllowPublish: true, AllowWildcard: true, IdleReap: reap},
		},
	}))
}

func waitChannelCount(t *testing.T, e *Engine, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if int(e.Stats().Channels) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ChannelCount = %d, want %d", int(e.Stats().Channels), want)
}

func TestIdleReapDrainedHistoryChannel(t *testing.T) {
	e := New(WithNamespaces(idleReapRegistry(50 * time.Millisecond)))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.Subscribe(1, "metrics:cpu")
	r := e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	e.Unsubscribe(1, "metrics:cpu")

	if int(e.Stats().Channels) != 1 {
		t.Fatalf("ChannelCount = %d right after drain, want 1 (history channel survives)", int(e.Stats().Channels))
	}

	waitChannelCount(t, e, 0)

	if _, ok := e.Recover("metrics:cpu", 0, r.Epoch); ok {
		t.Fatal("Recover after idle reap must signal full resync")
	}
}

func TestIdleReapSkipsSubscribedChannel(t *testing.T) {
	e := New(WithNamespaces(idleReapRegistry(50 * time.Millisecond)))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.Subscribe(1, "metrics:cpu")
	e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)

	time.Sleep(250 * time.Millisecond)
	if int(e.Stats().Channels) != 1 {
		t.Fatalf("ChannelCount = %d, want 1 (subscribed channel must never be idle reaped)", int(e.Stats().Channels))
	}
}

func TestIdleReapPublishResetsClock(t *testing.T) {
	e := New(WithNamespaces(idleReapRegistry(120 * time.Millisecond)))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.Subscribe(1, "metrics:cpu")
	e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	e.Unsubscribe(1, "metrics:cpu")

	for range 10 {
		time.Sleep(40 * time.Millisecond)
		e.Publish("metrics:cpu", []byte("v"), 0, rec.deliver)
	}
	if int(e.Stats().Channels) != 1 {
		t.Fatalf("ChannelCount = %d while publishing kept the channel active, want 1", int(e.Stats().Channels))
	}

	waitChannelCount(t, e, 0)
}

func TestIdleReapMaterializedPatternChannel(t *testing.T) {
	e := New(WithNamespaces(idleReapRegistry(50 * time.Millisecond)))
	defer e.Stop()
	rec := newDeliveryRecorder()

	if err := e.SubscribePattern(1, "flow:*"); err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}
	if r := e.Publish("flow:x", []byte("v"), 0, rec.deliver); r.Delivered != 1 {
		t.Fatalf("Delivered = %d, want 1", r.Delivered)
	}
	if int(e.Stats().Channels) != 1 {
		t.Fatalf("ChannelCount = %d after materialization, want 1", int(e.Stats().Channels))
	}

	waitChannelCount(t, e, 0)

	if r := e.Publish("flow:x", []byte("v"), 0, rec.deliver); r.Delivered != 1 {
		t.Fatalf("Delivered = %d after reap, want 1 (channel rematerializes)", r.Delivered)
	}
}

func testRegistry() *namespace.Registry {
	return mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"chat": {HistorySize: 8, AllowPublish: true},
			"feed": {AllowPublish: false},
			"tiny": {AllowPublish: true, MaxSubscribers: 2},
		},
		Strict: true,
	}))
}

func TestSubscribeUnknownNamespace(t *testing.T) {
	e := New(WithNamespaces(testRegistry()))
	defer e.Stop()

	if err := e.Subscribe(1, "ghost:x"); !errors.Is(err, ErrUnknownNamespace) {
		t.Fatalf("Subscribe(ghost:x) = %v, want ErrUnknownNamespace", err)
	}
}

func TestSubscribeAlreadySubscribedError(t *testing.T) {
	e := New()
	defer e.Stop()

	if err := e.Subscribe(1, "ch"); err != nil {
		t.Fatalf("first Subscribe = %v, want nil", err)
	}
	if err := e.Subscribe(1, "ch"); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatalf("second Subscribe = %v, want ErrAlreadySubscribed", err)
	}
}

func TestSubscribeChannelFull(t *testing.T) {
	e := New(WithNamespaces(testRegistry()))
	defer e.Stop()

	if err := e.Subscribe(1, "tiny:room"); err != nil {
		t.Fatalf("Subscribe 1 = %v", err)
	}
	if err := e.Subscribe(2, "tiny:room"); err != nil {
		t.Fatalf("Subscribe 2 = %v", err)
	}
	if err := e.Subscribe(3, "tiny:room"); !errors.Is(err, ErrChannelFull) {
		t.Fatalf("Subscribe 3 = %v, want ErrChannelFull", err)
	}

	e.Unsubscribe(1, "tiny:room")
	if err := e.Subscribe(3, "tiny:room"); err != nil {
		t.Fatalf("Subscribe after slot freed = %v, want nil", err)
	}
}

func TestNamespaceHistorySettings(t *testing.T) {
	e := New(WithNamespaces(testRegistry()))
	defer e.Stop()

	e.Subscribe(1, "chat:room")
	r := e.Publish("chat:room", []byte("x"), 0, func(SubscriberID, Delivery) bool { return true })
	if _, ok := e.Recover("chat:room", 0, r.Epoch); !ok {
		t.Fatal("chat namespace has history, Recover must succeed")
	}

	e.Subscribe(1, "plain")
	r = e.Publish("plain", []byte("x"), 0, func(SubscriberID, Delivery) bool { return true })
	if _, ok := e.Recover("plain", 0, r.Epoch); ok {
		t.Fatal("default namespace has no history, Recover must fail")
	}
}

func TestCheckPublish(t *testing.T) {
	e := New(WithNamespaces(testRegistry()))
	defer e.Stop()

	tests := []struct {
		name    string
		channel string
		wantErr error
	}{
		{name: "default namespace allowed", channel: "plain", wantErr: nil},
		{name: "writable namespace allowed", channel: "chat:room", wantErr: nil},
		{name: "read only namespace denied", channel: "feed:news", wantErr: ErrPublishDenied},
		{name: "unknown namespace", channel: "ghost:x", wantErr: ErrUnknownNamespace},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := e.CheckPublish(tt.channel); !errors.Is(err, tt.wantErr) {
				t.Errorf("CheckPublish(%q) = %v, want %v", tt.channel, err, tt.wantErr)
			}
		})
	}
}

func TestCheckPublishWithoutRegistry(t *testing.T) {
	e := New()
	defer e.Stop()

	if err := e.CheckPublish("anything:goes"); err != nil {
		t.Fatalf("CheckPublish without registry = %v, want nil", err)
	}
}

func fanoutRegistry() *namespace.Registry {
	return mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"tasks":  {AllowPublish: true, Fanout: namespace.RoundRobin},
			"alerts": {AllowPublish: true, Fanout: namespace.Priority},
		},
	}))
}

type deliveryRecorder struct {
	mu        sync.Mutex
	byID      map[SubscriberID][]uint64
	delivered []SubscriberID
	refuse    map[SubscriberID]bool
}

func newDeliveryRecorder() *deliveryRecorder {
	return &deliveryRecorder{byID: make(map[SubscriberID][]uint64), refuse: make(map[SubscriberID]bool)}
}

func (r *deliveryRecorder) deliver(id SubscriberID, d Delivery) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refuse[id] {
		return false
	}
	r.byID[id] = append(r.byID[id], d.Offset)
	r.delivered = append(r.delivered, id)
	return true
}

func (r *deliveryRecorder) counts() map[SubscriberID]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[SubscriberID]int, len(r.byID))
	for id, offs := range r.byID {
		out[id] = len(offs)
	}
	return out
}

func (r *deliveryRecorder) deliveredIDs() []SubscriberID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SubscriberID, len(r.delivered))
	copy(out, r.delivered)
	return out
}

func TestRoundRobinStartsWithFirstSubscriber(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	for id := SubscriberID(1); id <= 2; id++ {
		if err := e.Subscribe(id, "tasks:q"); err != nil {
			t.Fatalf("subscribe %d: %v", id, err)
		}
	}

	r := e.Publish("tasks:q", []byte("job"), 0, rec.deliver)
	if r.Delivered != 1 || r.Dropped != 0 {
		t.Fatalf("publish result = delivered %d dropped %d, want delivered 1 dropped 0", r.Delivered, r.Dropped)
	}

	got := rec.deliveredIDs()
	want := []SubscriberID{1}
	if !slices.Equal(got, want) {
		t.Fatalf("delivered subscribers = %v, want %v", got, want)
	}
}

func TestRoundRobinDistributesEvenly(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	for id := SubscriberID(1); id <= 3; id++ {
		if err := e.Subscribe(id, "tasks:q"); err != nil {
			t.Fatalf("subscribe %d: %v", id, err)
		}
	}

	for i := 0; i < 6; i++ {
		r := e.Publish("tasks:q", []byte("job"), 0, rec.deliver)
		if r.Delivered != 1 {
			t.Fatalf("publish %d delivered %d, want exactly 1", i, r.Delivered)
		}
	}

	for id, n := range rec.counts() {
		if n != 2 {
			t.Fatalf("subscriber %d got %d jobs, want 2 each: %v", id, n, rec.counts())
		}
	}
}

func TestRoundRobinSkipsFailedSubscriber(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()
	rec.refuse[2] = true

	for id := SubscriberID(1); id <= 3; id++ {
		e.Subscribe(id, "tasks:q")
	}

	for i := 0; i < 6; i++ {
		r := e.Publish("tasks:q", []byte("job"), 0, rec.deliver)
		if r.Delivered != 1 {
			t.Fatalf("publish %d delivered %d, want 1 (failed subscriber skipped)", i, r.Delivered)
		}
	}

	counts := rec.counts()
	if counts[2] != 0 {
		t.Fatalf("refusing subscriber got %d jobs, want 0", counts[2])
	}
	if counts[1]+counts[3] != 6 {
		t.Fatalf("healthy subscribers got %d jobs total, want 6: %v", counts[1]+counts[3], counts)
	}
}

func TestRoundRobinExcludesPublisher(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.Subscribe(1, "tasks:q")
	e.Subscribe(2, "tasks:q")

	for i := 0; i < 4; i++ {
		e.Publish("tasks:q", []byte("job"), 1, rec.deliver)
	}

	counts := rec.counts()
	if counts[1] != 0 || counts[2] != 4 {
		t.Fatalf("counts = %v, want all 4 on subscriber 2 (publisher excluded)", counts)
	}
}

func TestRoundRobinAllRefusedDrops(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()
	rec.refuse[1] = true

	e.Subscribe(1, "tasks:q")

	r := e.Publish("tasks:q", []byte("job"), 0, rec.deliver)
	if r.Delivered != 0 || r.Dropped != 1 {
		t.Fatalf("delivered=%d dropped=%d, want 0/1", r.Delivered, r.Dropped)
	}
}

func TestRoundRobinSubscriberChurn(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.Subscribe(1, "tasks:q")
	e.Subscribe(2, "tasks:q")
	e.Publish("tasks:q", []byte("a"), 0, rec.deliver)
	e.Unsubscribe(2, "tasks:q")
	e.Publish("tasks:q", []byte("b"), 0, rec.deliver)
	e.Publish("tasks:q", []byte("c"), 0, rec.deliver)

	counts := rec.counts()
	if counts[1]+counts[2] != 3 {
		t.Fatalf("total deliveries = %d, want 3: %v", counts[1]+counts[2], counts)
	}
	if counts[1] < 2 {
		t.Fatalf("remaining subscriber got %d, want >= 2: %v", counts[1], counts)
	}
}

func TestPriorityDeliversToTopCohortOnly(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.SubscribePriority(1, "alerts:pager", 1)
	e.SubscribePriority(2, "alerts:pager", 5)
	e.SubscribePriority(3, "alerts:pager", 5)

	r := e.Publish("alerts:pager", []byte("alert"), 0, rec.deliver)
	if r.Delivered != 2 {
		t.Fatalf("delivered %d, want 2 (top cohort only)", r.Delivered)
	}

	counts := rec.counts()
	if counts[1] != 0 || counts[2] != 1 || counts[3] != 1 {
		t.Fatalf("counts = %v, want only subscribers 2 and 3", counts)
	}
}

func TestPriorityFallsBackOnDisconnect(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.SubscribePriority(1, "alerts:pager", 1)
	e.SubscribePriority(2, "alerts:pager", 5)

	e.Unsubscribe(2, "alerts:pager")

	r := e.Publish("alerts:pager", []byte("alert"), 0, rec.deliver)
	if r.Delivered != 1 {
		t.Fatalf("delivered %d, want 1 (standby takes over)", r.Delivered)
	}
	if rec.counts()[1] != 1 {
		t.Fatalf("counts = %v, want standby subscriber 1", rec.counts())
	}
}

func TestPriorityHigherJoinerTakesOver(t *testing.T) {
	e := New(WithNamespaces(fanoutRegistry()))
	defer e.Stop()
	rec := newDeliveryRecorder()

	e.SubscribePriority(1, "alerts:pager", 1)
	e.SubscribePriority(2, "alerts:pager", 9)

	e.Publish("alerts:pager", []byte("alert"), 0, rec.deliver)

	counts := rec.counts()
	if counts[1] != 0 || counts[2] != 1 {
		t.Fatalf("counts = %v, want only the higher-priority joiner", counts)
	}
}

func TestHistorySweepIntervalIgnoresEngineHistoryUnderNamespaces(t *testing.T) {
	reg := mustRegistry(namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
	}))
	cfg := defaultConfig()
	WithHistory(8, 10*time.Millisecond)(cfg)
	WithNamespaces(reg)(cfg)

	if got := historySweepInterval(cfg); got != 0 {
		t.Fatalf("historySweepInterval = %v, want 0 (namespaces own settings; no channel can have this history)", got)
	}
}

func TestStopConcurrent(t *testing.T) {
	e := New(WithHistory(8, time.Minute))

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Stop()
		}()
	}
	wg.Wait()
}
