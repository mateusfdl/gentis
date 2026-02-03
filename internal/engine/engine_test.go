package engine

import (
	"sync"
	"testing"
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

	result := e.Publish("news", []byte("hello"), 1, func(id SubscriberID, ch string, data []byte) bool {
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

	result := e.Publish("test", []byte("data"), 0, func(id SubscriberID, ch string, data []byte) bool {
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

	result := e.Publish("nonexistent", []byte("data"), 0, func(id SubscriberID, ch string, data []byte) bool {
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

	e.Publish("ch1", []byte("msg"), 0, func(id SubscriberID, ch string, data []byte) bool {
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
			result := e.Publish("pub-channel", []byte("msg"), 0, func(id SubscriberID, ch string, data []byte) bool {
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

	eng := e.(*engine)
	if len(eng.shards) != 64 {
		t.Errorf("expected 64 shards, got %d", len(eng.shards))
	}
}

func TestChannelAtomicSubscribers(t *testing.T) {
	ch := newChannel("test")

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
