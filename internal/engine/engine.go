package engine

import (
	"maps"
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
	mu       sync.RWMutex
	channels map[string]*channel
	peak     int
	_        [16]byte
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

	shards := make([]shard, cfg.numShards)
	for i := range shards {
		shards[i].channels = make(map[string]*channel)
	}

	e := &engine{
		config:        cfg,
		shards:        shards,
		subscriptions: newSubscriptions(),
	}
	return e
}

func (e *engine) Subscribe(id SubscriberID, channelName string) bool {
	s := e.getShard(channelName)
	s.mu.Lock()

	ch, ok := s.channels[channelName]
	if !ok {
		ch = newChannel(channelName)
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

// --- Private helpers ---

// getShard returns the shard for a channel using inline FNV-1a hashing.
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func (e *engine) getShard(channel string) *shard {
	h := uint32(2166136261)
	for i := 0; i < len(channel); i++ {
		h ^= uint32(channel[i])
		h *= 16777619
	}
	return &e.shards[h%uint32(len(e.shards))]
}

func (e *engine) getChannel(name string) *channel {
	s := e.getShard(name)
	s.mu.RLock()
	ch := s.channels[name]
	s.mu.RUnlock()
	return ch
}

// maybeRebuild recreates the map to release old buckets when the load factor
// drops well below the high-water mark. Must be called with s.mu held for writing.
func (s *shard) maybeRebuild() {
	if s.peak > 64 && len(s.channels) < s.peak/4 {
		rebuilt := make(map[string]*channel, len(s.channels))

		maps.Copy(rebuilt, s.channels)

		s.channels = rebuilt
		s.peak = len(s.channels)
	}
}

var _ Engine = (*engine)(nil)
