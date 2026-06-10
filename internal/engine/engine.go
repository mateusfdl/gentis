package engine

import (
	"errors"
	"hash/maphash"
	"log/slog"
	"sync/atomic"
	"time"

	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
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

var (
	ErrAlreadySubscribed = errors.New("engine: already subscribed to channel")
	ErrUnknownNamespace  = errors.New("engine: unknown channel namespace")
	ErrChannelFull       = errors.New("engine: channel subscriber limit reached")
	ErrPublishDenied     = errors.New("engine: publish not allowed in namespace")
)

type Engine struct {
	config        *config
	shards        []Shard
	subscriptions *subscriptions
	observer      MetricsObserver
	pacer         *gcPacer
	fanoutPool    *fanoutPool
	hashSeed      maphash.Seed
	logger        *slog.Logger

	sweepStop chan struct{}
	sweepDone chan struct{}

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

	if interval := historySweepInterval(cfg); interval > 0 {
		e.sweepStop = make(chan struct{})
		e.sweepDone = make(chan struct{})
		go e.runHistorySweeper(interval)
	}

	logger.Info("engine initialized",
		"shards", cfg.numShards,
		"fanout_threshold", cfg.fanoutThreshold,
		"fanout_workers", cfg.fanoutWorkers,
		"gc_pacer", cfg.gcPacer.enabled,
	)

	return e
}

func (e *Engine) Subscribe(id SubscriberID, channelName string) error {
	return e.SubscribePriority(id, channelName, 0)
}

// SubscribePriority subscribes with a consumer rank. The rank only
// matters in priority fan-out namespaces, where the highest-rank cohort
// receives deliveries and everyone else is standby.
func (e *Engine) SubscribePriority(id SubscriberID, channelName string, prio int) error {
	e.subscribeOps.Add(1)

	s := e.getShard(channelName)
	s.mu.Lock()

	ch, ok := s.channels[channelName]
	if !ok {
		settings, known := e.channelSettings(channelName)
		if !known {
			s.mu.Unlock()
			return ErrUnknownNamespace
		}
		ch = NewChannel(channelName)
		if settings.HistorySize > 0 {
			ch.hist = newHistory(settings.HistorySize, settings.HistoryTTL)
		}
		ch.maxSubs = settings.MaxSubscribers
		ch.fanout = settings.Fanout
		s.channels[channelName] = ch
		if len(s.channels) > s.peak {
			s.peak = len(s.channels)
		}
		e.channelCount.Add(1)
	}

	if ch.maxSubs > 0 && ch.SubscriberCount() >= ch.maxSubs {
		s.mu.Unlock()
		return ErrChannelFull
	}

	if !ch.Subscribe(id) {
		s.mu.Unlock()
		return ErrAlreadySubscribed
	}
	if ch.fanout == namespace.Priority {
		ch.setPriority(id, prio)
	}
	s.mu.Unlock()

	e.subscriptions.Add(id, channelName)
	e.subscriptionCount.Add(1)
	return nil
}

// roundRobinDeliver hands the publication to exactly one subscriber,
// rotating through the set. A subscriber that refuses (full buffer) is
// skipped and the next one is tried; the message counts as dropped only
// when every candidate refused.
func roundRobinDeliver(ch *Channel, subs []SubscriberID, d Delivery, exclude SubscriberID, deliver DeliveryFunc) (int, int) {
	n := uint64(len(subs))
	if n == 0 {
		return 0, 0
	}
	start := ch.rr.Add(1)
	tried := 0
	for i := uint64(0); i < n; i++ {
		id := subs[(start+i)%n]
		if id == exclude {
			continue
		}
		tried++
		if deliver(id, d) {
			return 1, 0
		}
	}
	if tried == 0 {
		return 0, 0
	}
	return 0, 1
}

// channelSettings resolves the namespace settings governing a new channel.
// Without a registry the engine-wide history option applies to every
// channel and nothing is ever unknown.
func (e *Engine) channelSettings(channelName string) (namespace.Settings, bool) {
	if e.config.namespaces != nil {
		return e.config.namespaces.Resolve(channelName)
	}
	return namespace.Settings{
		HistorySize:  e.config.history.size,
		HistoryTTL:   e.config.history.ttl,
		AllowPublish: true,
	}, true
}

// QoSPolicy reports whether a channel's namespace offers at-least-once
// delivery and with what redelivery parameters.
func (e *Engine) QoSPolicy(channel string) (enabled bool, timeout time.Duration, maxRedeliveries int) {
	if e.config.namespaces == nil {
		return false, 0, 0
	}
	s, ok := e.config.namespaces.Resolve(channel)
	if !ok || s.QoS != namespace.AtLeastOnce {
		return false, 0, 0
	}
	return true, s.RedeliveryTimeout, s.MaxRedeliveries
}

// CheckPublish reports whether the namespace admits publishes on the
// channel. Without a registry every publish is admitted at zero cost.
func (e *Engine) CheckPublish(channel string) error {
	if e.config.namespaces == nil {
		return nil
	}
	s, ok := e.config.namespaces.Resolve(channel)
	if !ok {
		return ErrUnknownNamespace
	}
	if !s.AllowPublish {
		return ErrPublishDenied
	}
	return nil
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
	if ch.fanout == namespace.Priority {
		ch.clearPriority(id)
	}

	// Channels with history outlive their last subscriber so reconnecting
	// clients can recover; the TTL sweeper reaps them once drained.
	if ch.SubscriberCount() == 0 && ch.hist == nil {
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
				if ch.fanout == namespace.Priority {
					ch.clearPriority(id)
				}
				e.subscriptionCount.Add(-1)
			}
			if ch.SubscriberCount() == 0 && ch.hist == nil {
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

	if ch.hist != nil {
		ch.hist.append(d.Offset, data, time.Now().UnixNano())
	}

	subscribers := ch.Subscribers()

	switch ch.fanout {
	case namespace.RoundRobin:
		result.Delivered, result.Dropped = roundRobinDeliver(ch, subscribers, d, exclude, deliver)
	case namespace.Priority:
		if cohort := ch.topCohort.Load(); cohort != nil {
			subscribers = *cohort
		}
		fallthrough
	default:
		// Use parallel fan-out for high-subscriber channels to reduce
		// publish latency by distributing delivery across multiple
		// goroutines.
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
	if e.sweepStop != nil {
		close(e.sweepStop)
		<-e.sweepDone
		e.sweepStop = nil
	}
	if e.pacer != nil {
		e.pacer.Stop()
	}
	if e.fanoutPool != nil {
		e.fanoutPool.stop()
	}
}

// Recover returns the publications a client missed since fromOffset on the
// given channel, oldest first. ok is false when the gap cannot be served
// exactly: history disabled, the channel is gone, the epoch changed, or the
// requested range was evicted or expired. Callers must treat false as a
// full-resync signal.
func (e *Engine) Recover(channel string, fromOffset, epoch uint64) ([]Delivery, bool) {
	return e.RecoverN(channel, fromOffset, epoch, 0)
}

// RecoverN is Recover with a result cap: at most max items are returned
// (zero means unbounded). Used by the QoS pump to fetch only what the
// credit window admits.
func (e *Engine) RecoverN(channel string, fromOffset, epoch uint64, max int) ([]Delivery, bool) {
	s := e.getShard(channel)
	s.mu.RLock()
	ch := s.channels[channel]
	if ch != nil {
		ch.Acquire()
	}
	s.mu.RUnlock()

	if ch == nil {
		return nil, false
	}
	defer ch.Release()

	if ch.hist == nil || ch.epoch != epoch {
		return nil, false
	}

	items, ok := ch.hist.replayN(fromOffset, max)
	if !ok {
		return nil, false
	}

	out := make([]Delivery, len(items))
	for i, item := range items {
		out[i] = Delivery{
			Channel: channel,
			Data:    item.data,
			Offset:  item.offset,
			Epoch:   ch.epoch,
		}
	}
	return out, true
}

// historySweepInterval derives the sweep cadence from the smallest positive
// history TTL in play, or zero when no TTL exists and no sweeper is needed.
func historySweepInterval(cfg *config) time.Duration {
	minTTL := time.Duration(0)
	consider := func(size int, ttl time.Duration) {
		if size > 0 && ttl > 0 && (minTTL == 0 || ttl < minTTL) {
			minTTL = ttl
		}
	}
	consider(cfg.history.size, cfg.history.ttl)
	if cfg.namespaces != nil {
		for _, s := range cfg.namespaces.All() {
			consider(s.HistorySize, s.HistoryTTL)
		}
	}
	if minTTL == 0 {
		return 0
	}
	interval := minTTL / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

// runHistorySweeper trims expired history entries across all shards. One
// goroutine for the whole engine: per-channel sweep work is a tail trim
// under a short lock, so a single sweeper scales fine and avoids
// per-channel timers.
func (e *Engine) runHistorySweeper(interval time.Duration) {
	defer close(e.sweepDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.sweepStop:
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			for i := range e.shards {
				s := &e.shards[i]
				var drained []string
				s.mu.RLock()
				for name, ch := range s.channels {
					if ch.hist == nil {
						continue
					}
					ch.hist.sweep(now)
					if ch.SubscriberCount() == 0 && ch.hist.size() == 0 {
						drained = append(drained, name)
					}
				}
				s.mu.RUnlock()

				if len(drained) == 0 {
					continue
				}
				s.mu.Lock()
				for _, name := range drained {
					ch, ok := s.channels[name]
					if !ok || ch.SubscriberCount() != 0 || ch.hist.size() != 0 {
						continue
					}
					delete(s.channels, name)
					e.channelCount.Add(-1)
					s.maybeRebuild()
					recycleChannel(ch)
				}
				s.mu.Unlock()
			}
		}
	}
}
