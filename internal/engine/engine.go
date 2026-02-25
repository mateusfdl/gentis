package engine

import (
	"sync/atomic"
	"time"
)

type SubscriberID uint64

type DeliveryFunc func(id SubscriberID, channel string, data []byte) bool

// MetricsObserver receives histogram observations from the engine.
// Implementations should be thread-safe.
type MetricsObserver interface {
	ObservePublishDuration(seconds float64)
	ObservePublishFanout(count float64)
}

type PublishResult struct {
	Channel   string
	Delivered int
	Dropped   int
}

type EngineStats struct {
	Channels          int64
	TotalSubscribers  int64
	MessagesPublished int64
	MessagesDelivered int64
	MessagesDropped   int64
	SubscribeOps      int64
	UnsubscribeOps    int64
	MessageBytes      int64
}

type Engine interface {
	Subscribe(id SubscriberID, channel string) bool
	Unsubscribe(id SubscriberID, channel string) bool
	UnsubscribeAll(id SubscriberID)
	Publish(channel string, data []byte, exclude SubscriberID, deliver DeliveryFunc) PublishResult
	Subscribers(channel string) []SubscriberID
	ChannelCount() int
	SubscriberCount(channel string) int
	TotalSubscriptions() int
	Stats() EngineStats
}

type engine struct {
	config        *config
	shards        []Shard
	subscriptions *subscriptions
	observer      MetricsObserver

	channelCount      atomic.Int64
	publishCount      atomic.Int64
	deliveredCount    atomic.Int64
	droppedCount      atomic.Int64
	subscriptionCount atomic.Int64
	subscribeOps      atomic.Int64
	unsubscribeOps    atomic.Int64
	messageBytes      atomic.Int64
}

func New(opts ...Option) Engine {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	shards := make([]Shard, cfg.numShards)
	for i := range shards {
		shards[i].channels = make(map[string]*Channel)
	}

	e := &engine{
		config:        cfg,
		shards:        shards,
		subscriptions: newSubscriptions(),
		observer:      cfg.observer,
	}
	return e
}

func (e *engine) Subscribe(id SubscriberID, channelName string) bool {
	e.subscribeOps.Add(1)

	s := e.getShard(channelName)
	s.mu.Lock()

	ch, ok := s.channels[channelName]
	if !ok {
		ch = NewChannel(channelName)
		s.channels[channelName] = ch
		if len(s.channels) > s.peak {
			s.peak = len(s.channels)
		}
		e.channelCount.Add(1)
	}

	subscribed := ch.Subscribe(id)
	s.mu.Unlock()

	if subscribed {
		e.subscriptions.Add(id, channelName)
		e.subscriptionCount.Add(1)
	}

	return subscribed
}

func (e *engine) Unsubscribe(id SubscriberID, channelName string) bool {
	e.unsubscribeOps.Add(1)

	s := e.getShard(channelName)
	s.mu.Lock()

	ch, ok := s.channels[channelName]
	if !ok {
		s.mu.Unlock()
		return false
	}

	if !ch.Unsubscribe(id) {
		s.mu.Unlock()
		return false
	}

	if ch.SubscriberCount() == 0 {
		delete(s.channels, channelName)
		e.channelCount.Add(-1)
		s.maybeRebuild()
	}
	s.mu.Unlock()

	e.subscriptions.Remove(id, channelName)
	e.subscriptionCount.Add(-1)
	return true
}

func (e *engine) UnsubscribeAll(id SubscriberID) {
	channels := e.subscriptions.GetChannels(id)
	if len(channels) > 0 {
		e.unsubscribeOps.Add(int64(len(channels)))
	}
	for _, channelName := range channels {
		s := e.getShard(channelName)
		s.mu.Lock()
		ch, ok := s.channels[channelName]
		if ok {
			if ch.Unsubscribe(id) {
				e.subscriptionCount.Add(-1)
			}
			if ch.SubscriberCount() == 0 {
				delete(s.channels, channelName)
				e.channelCount.Add(-1)
				s.maybeRebuild()
			}
		}
		s.mu.Unlock()
	}
	e.subscriptions.RemoveAll(id)
}

func (e *engine) Publish(channel string, data []byte, exclude SubscriberID, deliver DeliveryFunc) PublishResult {
	e.publishCount.Add(1)
	e.messageBytes.Add(int64(len(data)))

	observed := e.observer != nil
	var start time.Time
	if observed {
		start = time.Now()
	}

	result := PublishResult{Channel: channel}

	ch := e.getChannel(channel)
	if ch == nil {
		if observed {
			e.observer.ObservePublishDuration(time.Since(start).Seconds())
			e.observer.ObservePublishFanout(0)
		}
		return result
	}

	subscribers := ch.Subscribers()

	// Use parallel fan-out for high-subscriber channels to reduce
	// publish latency by distributing delivery across multiple goroutines.
	if len(subscribers) >= e.config.fanoutThreshold && e.config.fanoutWorkers > 1 {
		result.Delivered, result.Dropped = e.parallelFanout(subscribers, channel, data, exclude, deliver)
	} else {
		for _, id := range subscribers {
			if id == exclude {
				continue
			}
			if deliver(id, channel, data) {
				result.Delivered++
			} else {
				result.Dropped++
			}
		}
	}

	delivered := int64(result.Delivered)
	dropped := int64(result.Dropped)
	if delivered > 0 {
		e.deliveredCount.Add(delivered)
	}
	if dropped > 0 {
		e.droppedCount.Add(dropped)
	}

	if observed {
		e.observer.ObservePublishDuration(time.Since(start).Seconds())
		e.observer.ObservePublishFanout(float64(result.Delivered + result.Dropped))
	}

	return result
}

func (e *engine) Subscribers(channel string) []SubscriberID {
	ch := e.getChannel(channel)
	if ch == nil {
		return nil
	}
	return ch.Subscribers()
}

func (e *engine) ChannelCount() int {
	return int(e.channelCount.Load())
}

func (e *engine) SubscriberCount(channel string) int {
	ch := e.getChannel(channel)
	if ch == nil {
		return 0
	}
	return ch.SubscriberCount()
}

func (e *engine) TotalSubscriptions() int {
	return int(e.subscriptionCount.Load())
}

func (e *engine) Stats() EngineStats {
	return EngineStats{
		Channels:          e.channelCount.Load(),
		TotalSubscribers:  e.subscriptionCount.Load(),
		MessagesPublished: e.publishCount.Load(),
		MessagesDelivered: e.deliveredCount.Load(),
		MessagesDropped:   e.droppedCount.Load(),
		SubscribeOps:      e.subscribeOps.Load(),
		UnsubscribeOps:    e.unsubscribeOps.Load(),
		MessageBytes:      e.messageBytes.Load(),
	}
}

func (e *engine) getChannel(name string) *Channel {
	s := e.getShard(name)
	s.mu.RLock()
	ch := s.channels[name]
	s.mu.RUnlock()
	return ch
}

var _ Engine = (*engine)(nil)
