package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubscribeAndUnsubscribe(t *testing.T) {
	e := New()

	if !e.Subscribe(1, "test-channel") {
		t.Error("expected first subscribe to return true")
	}

	if e.Subscribe(1, "test-channel") {
		t.Error("expected duplicate subscribe to return false")
	}

	if e.ChannelCount() != 1 {
		t.Errorf("expected 1 channel, got %d", e.ChannelCount())
	}

	if e.SubscriberCount("test-channel") != 1 {
		t.Errorf("expected 1 subscriber, got %d", e.SubscriberCount("test-channel"))
	}

	if !e.Unsubscribe(1, "test-channel") {
		t.Error("expected unsubscribe to return true")
	}

	if e.Unsubscribe(1, "test-channel") {
		t.Error("expected second unsubscribe to return false")
	}

	if e.ChannelCount() != 0 {
		t.Errorf("expected 0 channels after last unsubscribe, got %d", e.ChannelCount())
	}
}

func TestGetSubscribers(t *testing.T) {
	e := New()

	e.Subscribe(1, "chat")
	e.Subscribe(2, "chat")
	e.Subscribe(3, "chat")

	subs := e.Subscribers("chat")
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

	if e.TotalSubscriptions() != 4 {
		t.Errorf("expected 4 subscriptions, got %d", e.TotalSubscriptions())
	}

	e.UnsubscribeAll(1)

	if e.TotalSubscriptions() != 1 {
		t.Errorf("expected 1 subscription after UnsubscribeAll, got %d", e.TotalSubscriptions())
	}

	if e.SubscriberCount("channel-a") != 1 {
		t.Errorf("expected 1 subscriber in channel-a, got %d", e.SubscriberCount("channel-a"))
	}

	if e.ChannelCount() != 1 {
		t.Errorf("expected 1 channel, got %d", e.ChannelCount())
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

	if e.SubscriberCount("concurrent-channel") != 100 {
		t.Errorf("expected 100 subscribers, got %d", e.SubscriberCount("concurrent-channel"))
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

	if s.Count(1) != 3 {
		t.Errorf("expected 3 subscriptions, got %d", s.Count(1))
	}

	if !s.Has(1, "ch2") {
		t.Error("expected subscription to ch2")
	}

	s.Remove(1, "ch2")

	if s.Has(1, "ch2") {
		t.Error("ch2 should be removed")
	}

	channels := s.GetChannels(1)
	if len(channels) != 2 {
		t.Errorf("expected 2 channels, got %d", len(channels))
	}

	s.RemoveAll(1)

	if s.Count(1) != 0 {
		t.Error("expected 0 subscriptions after RemoveAll")
	}
}

func TestSubscribersNonexistentChannel(t *testing.T) {
	e := New()

	subs := e.Subscribers("nonexistent")
	if subs != nil {
		t.Errorf("expected nil for nonexistent channel, got %v", subs)
	}
}

func TestSubscriberCountNonexistentChannel(t *testing.T) {
	e := New()

	if e.SubscriberCount("nonexistent") != 0 {
		t.Errorf("expected 0 for nonexistent channel, got %d", e.SubscriberCount("nonexistent"))
	}
}

func TestUnsubscribeAllNoSubscriptions(t *testing.T) {
	e := New()

	e.UnsubscribeAll(999)

	if e.TotalSubscriptions() != 0 {
		t.Errorf("expected 0 subscriptions, got %d", e.TotalSubscriptions())
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

func TestChannelName(t *testing.T) {
	ch := NewChannel("my-channel")

	if ch.Name() != "my-channel" {
		t.Errorf("expected 'my-channel', got %q", ch.Name())
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

func TestSubscriptionTrackerHasUnknown(t *testing.T) {
	s := newSubscriptions()

	if s.Has(999, "ch") {
		t.Error("expected false for unknown subscriber")
	}
}

func TestSubscriptionTrackerCountUnknown(t *testing.T) {
	s := newSubscriptions()

	if s.Count(999) != 0 {
		t.Errorf("expected 0 for unknown subscriber, got %d", s.Count(999))
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

	if e.ChannelCount() != 3 {
		t.Errorf("expected 3 channels, got %d", e.ChannelCount())
	}

	if e.TotalSubscriptions() != 4 {
		t.Errorf("expected 4 subscriptions, got %d", e.TotalSubscriptions())
	}

	e.UnsubscribeAll(1)

	if e.ChannelCount() != 2 {
		t.Errorf("expected 2 channels after UnsubscribeAll(1), got %d", e.ChannelCount())
	}

	if e.TotalSubscriptions() != 2 {
		t.Errorf("expected 2 subscriptions, got %d", e.TotalSubscriptions())
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
