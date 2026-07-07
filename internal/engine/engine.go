package engine

import (
	"errors"
	"hash/maphash"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
	"github.com/mateusfdl/gentis/internal/pattern"
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

	// Frame memoizes the wire encoding shared across this publish's
	// fan-out, so N subscribers encode the payload once. Nil on cold
	// paths (replay, recovery) where there is a single recipient.
	Frame *EncodedFrame
}

// DeliveryFunc fans a publication out to one subscriber, returning whether the
// delivery was accepted (false counts as dropped). Within one Publish the
// engine never delivers the same id from two goroutines at once, even when
// parallel fan-out splits the subscriber set. Across Publishes there is no
// such exclusivity: concurrent publishers on one channel invoke the func for
// the same id concurrently, so any per-id state an implementation touches
// must be synchronized (the built-in senders use a channel send or a CAS
// ring, which are).
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
	ErrWildcardDenied    = errors.New("engine: wildcard subscriptions not allowed in namespace")
)

type Engine struct {
	config        *config
	shards        []Shard
	subscriptions *subscriptions
	patterns      *patternRegistry
	observer      MetricsObserver
	pacer         *gcPacer
	fanoutPool    *fanoutPool
	hashSeed      maphash.Seed
	logger        *slog.Logger

	sweepStop chan struct{}
	sweepDone chan struct{}
	stopOnce  sync.Once

	channelCount      atomic.Int64
	subscriptionCount atomic.Int64
	patternSubs       atomic.Int64
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
		patterns:      newPatternRegistry(cfg.namespaces != nil),
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

// Subscribe registers id on a channel. Calls for the same subscriber id
// (Subscribe, Unsubscribe, UnsubscribeAll) must be serialized by the
// caller: the channel registry and the reverse subscription index are
// updated non-atomically, and an UnsubscribeAll racing a Subscribe for one
// id can strand a ghost subscription that pins the channel forever.
// Transports satisfy this by driving every per-session op from the
// session's dispatch goroutine and cleaning up only after it exits.
// Different ids are fully concurrent.
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
		ch = e.createChannelLocked(s, channelName, settings)
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

// SubscribePattern subscribes to every channel a glob pattern matches,
// present and future. The pattern's namespace prefix must be literal and
// must allow wildcards. Pattern deliveries are broadcast-only and count
// against the same subscription accounting as exact ones.
func (e *Engine) SubscribePattern(id SubscriberID, pat string) error {
	if e.config.namespaces != nil {
		ns, _, _ := namespace.Split(pat)
		if pattern.IsPattern(ns) {
			return ErrWildcardDenied
		}
		settings, known := e.config.namespaces.Resolve(pat)
		if !known {
			return ErrUnknownNamespace
		}
		if !settings.AllowWildcard {
			return ErrWildcardDenied
		}
	}

	e.subscribeOps.Add(1)
	if !e.patterns.add(id, pat) {
		return ErrAlreadySubscribed
	}
	e.patternSubs.Add(1)
	e.subscriptionCount.Add(1)
	return nil
}

func (e *Engine) UnsubscribePattern(id SubscriberID, pat string) bool {
	if !e.patterns.remove(id, pat) {
		return false
	}
	e.unsubscribeOps.Add(1)
	e.subscriptionCount.Add(-1)
	if e.patternSubs.Add(-1) == 0 {
		e.reapEmptyChannels()
	}
	return true
}

// reapEmptyChannels drops channels that only existed to serve pattern
// subscribers: no exact subscribers and no history. Called when the last
// pattern subscription goes away; history-bearing channels stay under the
// TTL sweeper's authority.
func (e *Engine) reapEmptyChannels() {
	for i := range e.shards {
		s := &e.shards[i]
		s.mu.Lock()
		for name, ch := range s.channels {
			if ch.SubscriberCount() == 0 && ch.hist == nil {
				delete(s.channels, name)
				e.channelCount.Add(-1)
				s.maybeRebuild()
				recycleChannel(ch)
			}
		}
		s.mu.Unlock()
	}
}

// createChannelLocked builds a channel from its namespace settings and
// inserts it into the shard. The caller must hold the shard write lock.
func (e *Engine) createChannelLocked(s *Shard, name string, settings namespace.Settings) *Channel {
	ch := NewChannel(name)
	if settings.HistorySize > 0 {
		ch.hist = newHistory(settings.HistorySize, settings.HistoryTTL)
	}
	ch.maxSubs = settings.MaxSubscribers
	ch.fanout = settings.Fanout
	ch.idleReap = settings.IdleReap
	ch.lastActive.Store(time.Now().UnixNano())
	s.channels[name] = ch
	if len(s.channels) > s.peak {
		s.peak = len(s.channels)
	}
	e.channelCount.Add(1)
	return ch
}

// materializeChannel creates the channel a pattern-matched publish needs
// so offsets, epoch, and history behave exactly as if an exact subscriber
// existed. Returns nil when the namespace is unknown under strict mode.
// The returned channel is acquired; the caller must Release it.
func (e *Engine) materializeChannel(channel string) *Channel {
	settings, known := e.channelSettings(channel)
	if !known {
		return nil
	}
	s := e.getShard(channel)
	s.mu.Lock()
	ch, ok := s.channels[channel]
	if !ok {
		ch = e.createChannelLocked(s, channel, settings)
	}
	ch.Acquire()
	s.mu.Unlock()
	return ch
}

// deliverPatterns fans the publication out to the wildcard subscribers in
// patternIDs (the match set Publish already resolved for this channel),
// skipping anyone the exact fan-out already reached. exact must be the
// same snapshot the exact fan-out used, so a subscriber churning between
// the two passes is never double-delivered or skipped. Returns counts
// instead of mutating the result so Publish never takes the result's
// address: that would demote the exact fan-out loop's counter increments
// from registers to memory.
func (e *Engine) deliverPatterns(exact, patternIDs []SubscriberID, d Delivery, exclude SubscriberID, deliver DeliveryFunc) (delivered, dropped int) {
	if len(patternIDs) == 0 {
		return 0, 0
	}
	reached := func(id SubscriberID) bool { return slices.Contains(exact, id) }
	if len(exact) > 32 && len(patternIDs) > 1 {
		set := make(map[SubscriberID]struct{}, len(exact))
		for _, id := range exact {
			set[id] = struct{}{}
		}
		reached = func(id SubscriberID) bool { _, ok := set[id]; return ok }
	}
	for _, id := range patternIDs {
		if id == exclude || reached(id) {
			continue
		}
		if deliver(id, d) {
			delivered++
		} else {
			dropped++
		}
	}
	return delivered, dropped
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
	start := ch.rr.Add(1) - 1
	tried := 0
	for i := range n {
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
	// clients can recover; the TTL sweeper reaps them once drained. The
	// idle clock restarts at drain so a busy channel is not reaped the
	// instant its last subscriber leaves.
	if ch.SubscriberCount() == 0 {
		if ch.hist == nil {
			delete(s.channels, channelName)
			e.channelCount.Add(-1)
			s.maybeRebuild()
			recycleChannel(ch)
		} else if ch.idleReap > 0 {
			ch.lastActive.Store(time.Now().UnixNano())
		}
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
			if ch.SubscriberCount() == 0 {
				if ch.hist == nil {
					delete(s.channels, channelName)
					e.channelCount.Add(-1)
					s.maybeRebuild()
					recycleChannel(ch)
				} else if ch.idleReap > 0 {
					ch.lastActive.Store(time.Now().UnixNano())
				}
			}
		}
		s.mu.Unlock()
	}
	e.subscriptions.RemoveAll(id)

	if n := e.patterns.removeAll(id); n > 0 {
		e.unsubscribeOps.Add(int64(n))
		e.subscriptionCount.Add(int64(-n))
		if e.patternSubs.Add(int64(-n)) == 0 {
			e.reapEmptyChannels()
		}
	}
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

	if ch == nil && e.patternSubs.Load() != 0 && len(e.patterns.subscribersFor(channel)) != 0 {
		ch = e.materializeChannel(channel)
		// The last pattern unsubscribe may have reaped between the load
		// above and the materialization; without this re-check the fresh
		// channel would leak until the next reap cycle.
		if ch != nil && e.patternSubs.Load() == 0 {
			e.reapEmptyChannels()
		}
	}
	if ch == nil {
		if observed {
			e.observer.ObservePublishDuration(time.Since(start).Seconds())
			e.observer.ObservePublishFanout(0)
		}
		return result
	}
	defer ch.Release()

	var offset uint64
	if ch.hist != nil {
		now := time.Now().UnixNano()
		offset = ch.hist.appendNext(&ch.offset, data, now)
		if ch.idleReap > 0 {
			ch.lastActive.Store(now)
		}
	} else {
		offset = ch.offset.Add(1)
		if ch.idleReap > 0 {
			ch.lastActive.Store(time.Now().UnixNano())
		}
	}
	d := Delivery{
		Channel: channel,
		Data:    data,
		Offset:  offset,
		Epoch:   ch.epoch,
	}
	result.Offset = d.Offset
	result.Epoch = d.Epoch

	subscribers := ch.Subscribers()

	// Resolved once here (a sharded cache hit after the first publish) and
	// reused by both the frame decision and the pattern fan-out below.
	var patternIDs []SubscriberID
	if e.patternSubs.Load() != 0 {
		patternIDs = e.patterns.subscribersFor(channel)
	}

	switch ch.fanout {
	case namespace.RoundRobin:
		result.Delivered, result.Dropped = roundRobinDeliver(ch, subscribers, d, exclude, deliver)
	case namespace.Priority:
		if cohort := ch.topCohort.Load(); cohort != nil {
			subscribers = *cohort
		}
		fallthrough
	default:
		// Share one wire encoding across the fan-out: with 2+ recipients
		// the channel_message payload is identical, so encoding it once
		// removes the per-subscriber marshal and its encoder-pool
		// contention. Pattern recipients count: a broadcast reaching one
		// exact and one wildcard subscriber still marshals once. A single
		// recipient gets no shared frame and pays nothing.
		if len(subscribers)+len(patternIDs) > 1 {
			d.Frame = &EncodedFrame{}
		}
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

	if len(patternIDs) > 0 {
		pd, pdrop := e.deliverPatterns(subscribers, patternIDs, d, exclude, deliver)
		result.Delivered += pd
		result.Dropped += pdrop
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
	e.stopOnce.Do(func() {
		if e.sweepStop != nil {
			close(e.sweepStop)
			<-e.sweepDone
		}
		if e.pacer != nil {
			e.pacer.Stop()
		}
		if e.fanoutPool != nil {
			e.fanoutPool.stop()
		}
	})
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
// history TTL or idle_reap in play, or zero when neither exists and no
// sweeper is needed.
func historySweepInterval(cfg *config) time.Duration {
	minTTL := time.Duration(0)
	consider := func(d time.Duration) {
		if d > 0 && (minTTL == 0 || d < minTTL) {
			minTTL = d
		}
	}
	if cfg.namespaces != nil {
		// Namespaces own channel settings; the engine-wide history config
		// is ignored by channelSettings and must not spawn a sweeper.
		for _, s := range cfg.namespaces.All() {
			if s.HistorySize > 0 {
				consider(s.HistoryTTL)
			}
			consider(s.IdleReap)
		}
	} else if cfg.history.size > 0 {
		consider(cfg.history.ttl)
	}
	if minTTL == 0 {
		return 0
	}
	return max(minTTL/2, 10*time.Millisecond)
}

// runHistorySweeper trims expired history entries across all shards and
// reaps drained channels: history-bearing ones once their entries expire,
// idle_reap ones once they sit subscriber-less without a publish past
// their deadline. One goroutine for the whole engine: per-channel sweep
// work is a tail trim under a short lock, so a single sweeper scales fine
// and avoids per-channel timers.
func (e *Engine) runHistorySweeper(interval time.Duration) {
	defer close(e.sweepDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	reapable := func(ch *Channel, now int64) bool {
		if ch.SubscriberCount() != 0 {
			return false
		}
		if ch.hist != nil && ch.hist.size() == 0 {
			return true
		}
		return ch.idleReap > 0 && now-ch.lastActive.Load() > int64(ch.idleReap)
	}

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
					if ch.hist != nil {
						ch.hist.sweep(now)
					}
					if reapable(ch, now) {
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
					if !ok || !reapable(ch, now) {
						continue
					}
					e.logger.Debug("channel reaped", "channel", name)
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
