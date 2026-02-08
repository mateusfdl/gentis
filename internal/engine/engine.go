package engine

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

type SubscriberID uint64

type DeliveryFunc func(id SubscriberID, channel string, data []byte) bool

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

const defaultNumShards = 32

type shard struct {
	channels sync.Map
	_        [56]byte
}

type engine struct {
	config        *config
	shards        []shard
	subscriptions *subscriptions

	channelCount      atomic.Int64
	publishCount      atomic.Int64
	deliveredCount    atomic.Int64
	droppedCount      atomic.Int64
	subscriptionCount atomic.Int64
}

func New(opts ...Option) Engine {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	e := &engine{
		config:        cfg,
		shards:        make([]shard, cfg.numShards),
		subscriptions: newSubscriptions(),
	}
	return e
}

func (e *engine) getShard(channel string) *shard {
	h := fnv.New32a()
	h.Write([]byte(channel))
	return &e.shards[h.Sum32()%uint32(len(e.shards))]
}

func (e *engine) getChannel(name string) *channel {
	shard := e.getShard(name)
	if ch, ok := shard.channels.Load(name); ok {
		return ch.(*channel)
	}
	return nil
}

func (e *engine) getOrCreateChannel(name string) *channel {
	shard := e.getShard(name)

	if ch, ok := shard.channels.Load(name); ok {
		return ch.(*channel)
	}

	newCh := newChannel(name)
	actual, loaded := shard.channels.LoadOrStore(name, newCh)

	if !loaded {
		e.channelCount.Add(1)
	}

	return actual.(*channel)
}

func (e *engine) deleteChannelIfEmpty(name string) {
	shard := e.getShard(name)
	if ch, ok := shard.channels.Load(name); ok {
		if ch.(*channel).TryDelete() {
			if shard.channels.CompareAndDelete(name, ch) {
				e.channelCount.Add(-1)
			}
		}
	}
}

func (e *engine) Subscribe(id SubscriberID, channelName string) bool {
	ch := e.getOrCreateChannel(channelName)

	if !ch.Subscribe(id) {
		return false
	}

	e.subscriptions.Add(id, channelName)
	e.subscriptionCount.Add(1)

	return true
}

func (e *engine) Unsubscribe(id SubscriberID, channelName string) bool {
	ch := e.getChannel(channelName)
	if ch == nil {
		return false
	}

	if !ch.Unsubscribe(id) {
		return false
	}

	e.subscriptions.Remove(id, channelName)
	e.subscriptionCount.Add(-1)
	e.deleteChannelIfEmpty(channelName)
	return true
}

func (e *engine) UnsubscribeAll(id SubscriberID) {
	channels := e.subscriptions.GetChannels(id)
	for _, channelName := range channels {
		ch := e.getChannel(channelName)
		if ch != nil {
			if ch.Unsubscribe(id) {
				e.subscriptionCount.Add(-1)
			}
			e.deleteChannelIfEmpty(channelName)
		}
	}
	e.subscriptions.RemoveAll(id)
}

func (e *engine) Publish(channel string, data []byte, exclude SubscriberID, deliver DeliveryFunc) PublishResult {
	e.publishCount.Add(1)

	result := PublishResult{Channel: channel}

	ch := e.getChannel(channel)
	if ch == nil {
		return result
	}

	subscribers := ch.Subscribers()

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

	e.deliveredCount.Add(int64(result.Delivered))
	e.droppedCount.Add(int64(result.Dropped))

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
	}
}

var _ Engine = (*engine)(nil)
