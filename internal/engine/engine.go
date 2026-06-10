package engine

import (
	"hash/maphash"
	"log/slog"
	"sync/atomic"
	"time"

	gentislog "github.com/mateusfdl/gentis/internal/logs"
)

type SubscriberID uint64

// Delivery is one publication as handed to a subscriber: the payload plus
// the identity the engine assigned to it. Offsets are monotonic per channel
// and only comparable within the same epoch.
type Delivery struct {
	Channel string
	Data    []byte
	Offset  uint64
	Epoch   uint64
}

type DeliveryFunc func(id SubscriberID, d Delivery) bool

type MetricsObserver interface {
	ObservePublishDuration(seconds float64)
	ObservePublishFanout(count float64)
}

type PublishResult struct {
	Channel   string
	Offset    uint64
	Epoch     uint64
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

type Engine struct {
	config        *config
	shards        []Shard
	subscriptions *subscriptions
	observer      MetricsObserver
	pacer         *gcPacer
	fanoutPool    *fanoutPool
	hashSeed      maphash.Seed
	logger        *slog.Logger

	channelCount      atomic.Int64
	subscriptionCount atomic.Int64
	subscribeOps      atomic.Int64
	unsubscribeOps    atomic.Int64
}

func New(opts ...Option) *Engine {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	shards := make([]Shard, cfg.numShards)
	for i := range shards {
		shards[i].channels = make(map[string]*Channel)
	}

	logger := cfg.logger
	if logger == nil {
		logger = gentislog.Nop()
	}
	logger = logger.With("component", "engine")

	e := &Engine{
		config:        cfg,
		shards:        shards,
		subscriptions: newSubscriptions(),
		observer:      cfg.observer,
		hashSeed:      maphash.MakeSeed(),
		logger:        logger,
	}

	if cfg.gcPacer.enabled {
		e.pacer = newGCPacer(e, cfg.gcPacer)
	}

	if cfg.fanoutWorkers > 1 {
		e.fanoutPool = newFanoutPool(cfg.fanoutWorkers)
	}

	logger.Info("engine initialized",
		"shards", cfg.numShards,
		"fanout_threshold", cfg.fanoutThreshold,
		"fanout_workers", cfg.fanoutWorkers,
		"gc_pacer", cfg.gcPacer.enabled,
	)

	return e
}

func (e *Engine) Subscribe(id SubscriberID, channelName string) bool {
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

func (e *Engine) Unsubscribe(id SubscriberID, channelName string) bool {
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
		recycleChannel(ch)
	}
	s.mu.Unlock()

	e.subscriptions.Remove(id, channelName)
	e.subscriptionCount.Add(-1)
	return true
}

func (e *Engine) UnsubscribeAll(id SubscriberID) {
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
				recycleChannel(ch)
			}
		}
		s.mu.Unlock()
	}
	e.subscriptions.RemoveAll(id)
}

func (e *Engine) Publish(channel string, data []byte, exclude SubscriberID, deliver DeliveryFunc) PublishResult {
	// Accumulate on per-shard counters to avoid cross-core cache-line bouncing.
	s := e.getShard(channel)
	s.publishCount.Add(1)
	s.messageBytes.Add(int64(len(data)))

	observed := e.observer != nil
	var start time.Time
	if observed {
		start = time.Now()
	}

	result := PublishResult{Channel: channel}

	s.mu.RLock()
	ch := s.channels[channel]
	if ch != nil {
		ch.Acquire()
	}
	s.mu.RUnlock()

	if ch == nil {
		if observed {
			e.observer.ObservePublishDuration(time.Since(start).Seconds())
			e.observer.ObservePublishFanout(0)
		}
		return result
	}
	defer ch.Release()

	d := Delivery{
		Channel: channel,
		Data:    data,
		Offset:  ch.offset.Add(1),
		Epoch:   ch.epoch,
	}
	result.Offset = d.Offset
	result.Epoch = d.Epoch

	subscribers := ch.Subscribers()

	// Use parallel fan-out for high-subscriber channels to reduce
	// publish latency by distributing delivery across multiple goroutines.
	if len(subscribers) >= e.config.fanoutThreshold && e.config.fanoutWorkers > 1 {
		result.Delivered, result.Dropped = e.parallelFanout(subscribers, d, exclude, deliver)
	} else {
		for _, id := range subscribers {
			if id == exclude {
				continue
			}
			if deliver(id, d) {
				result.Delivered++
			} else {
				result.Dropped++
			}
		}
	}

	delivered := int64(result.Delivered)
	dropped := int64(result.Dropped)
	if delivered > 0 {
		s.deliveredCount.Add(delivered)
	}
	if dropped > 0 {
		s.droppedCount.Add(dropped)
	}

	if observed {
		e.observer.ObservePublishDuration(time.Since(start).Seconds())
		e.observer.ObservePublishFanout(float64(result.Delivered + result.Dropped))
	}

	return result
}

func (e *Engine) Subscribers(channel string) []SubscriberID {
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

func (e *Engine) ChannelCount() int {
	return int(e.channelCount.Load())
}

func (e *Engine) SubscriberCount(channel string) int {
	s := e.getShard(channel)
	s.mu.RLock()
	ch := s.channels[channel]
	if ch != nil {
		ch.Acquire()
	}
	s.mu.RUnlock()

	if ch == nil {
		return 0
	}
	defer ch.Release()
	return ch.SubscriberCount()
}

func (e *Engine) TotalSubscriptions() int {
	return int(e.subscriptionCount.Load())
}

func (e *Engine) Stats() EngineStats {
	var published, delivered, dropped, msgBytes int64
	for i := range e.shards {
		published += e.shards[i].publishCount.Load()
		delivered += e.shards[i].deliveredCount.Load()
		dropped += e.shards[i].droppedCount.Load()
		msgBytes += e.shards[i].messageBytes.Load()
	}
	return EngineStats{
		Channels:          e.channelCount.Load(),
		TotalSubscribers:  e.subscriptionCount.Load(),
		MessagesPublished: published,
		MessagesDelivered: delivered,
		MessagesDropped:   dropped,
		SubscribeOps:      e.subscribeOps.Load(),
		UnsubscribeOps:    e.unsubscribeOps.Load(),
		MessageBytes:      msgBytes,
	}
}

// Stop shuts down background goroutines (GC pacer, fanout workers).
// Safe to call even if no background tasks are running.
func (e *Engine) Stop() {
	if e.pacer != nil {
		e.pacer.Stop()
	}
	if e.fanoutPool != nil {
		e.fanoutPool.stop()
	}
}
