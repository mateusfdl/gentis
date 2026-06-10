package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/pattern"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/ringbuf"
	"github.com/mateusfdl/gentis/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func sendRingCap(bufferSize int) int {
	if bufferSize < 1 {
		return 1
	}
	c := 1
	for c < bufferSize {
		c <<= 1
	}
	return c
}

type incomingMsg struct {
	channel string
	data    []byte
}

type Server struct {
	gentisv1.UnimplementedGentisServiceServer

	config    *Config
	engine    *engine.Engine
	store     *transport.SessionStore
	sessArena *arena.Arena // nil when arena is disabled
	upstream  *Upstream
	router    *Router
	dedup     *Deduplicator

	incomingCh chan incomingMsg

	listener            net.Listener
	grpcSrv             *grpc.Server
	sessions            sync.Map
	nextID              atomic.Int32
	logger              *slog.Logger
	metrics             *metrics.Server
	connectionCount     atomic.Int64
	connectionsTotal    atomic.Int64
	disconnectionsTotal atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Session struct {
	id       int
	subID    engine.SubscriberID
	state    transport.SessionState
	sendRing *ringbuf.PointerRing[gentisv1.ServerMessage]
	wakeCh   chan struct{}
	drainCh  chan struct{}
	relay    *Server
	ctx      context.Context
	cancel   context.CancelFunc
	subsMu   sync.RWMutex
	channels map[string]struct{}

	expiryTimer *time.Timer

	// qosc gates deliveries for at-least-once subscriptions. Sessions
	// without QoS1 windows pay a single atomic load per delivery.
	qosc *qos.Consumer

	dropsFull atomic.Int64
}

const redeliveryCheckInterval = 200 * time.Millisecond

func (sess *Session) DeliverMessage(d engine.Delivery) bool {
	return sess.qosc.Deliver(d)
}

func (sess *Session) produce(d engine.Delivery) bool {
	msg := getServerMsg(d)
	if !sess.sendRing.TryProduce(msg) {
		putServerMsg(msg)
		sess.dropsFull.Add(1)
		sess.relay.logger.Warn("message dropped, send buffer full", "channel", d.Channel, "session_id", sess.id)
		return false
	}
	sess.wake()
	return true
}

func (sess *Session) wake() {
	select {
	case sess.wakeCh <- struct{}{}:
	default:
	}
}

var _ transport.Sender = (*Session)(nil)

func New(opts ...Option) *Server {
	config := defaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Arena slots physically hold at most arena.MaxSubscriptions entries;
	// anything past that would be dropped silently, so clamp the limit to
	// keep SUBSCRIPTION_LIMIT the single source of truth.
	if config.UseArena && (config.MaxSubscriptions <= 0 || config.MaxSubscriptions > arena.MaxSubscriptions) {
		config.MaxSubscriptions = arena.MaxSubscriptions
	}

	ctx, cancel := context.WithCancel(context.Background())

	eng := config.Engine
	if eng == nil {
		eng = engine.New()
	}

	logger := config.Logger
	if logger == nil {
		logger = gentislog.Nop()
	}
	logger = logger.With("component", "relay")

	s := &Server{
		config:     config,
		engine:     eng,
		store:      config.SessionStore,
		router:     NewRouter(nil),
		dedup:      NewDeduplicator(5 * time.Second),
		incomingCh: make(chan incomingMsg, config.IncomingBufferSize),
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
	}

	// When arena is on, counter-based IDs (arena-exhausted fallback)
	// must start ABOVE the arena range so they never collide with
	// arena-derived IDs (which occupy [1, MaxSessions]).
	if config.UseArena {
		maxSessions := config.MaxSessions
		if maxSessions <= 0 {
			maxSessions = 16384
		}
		s.nextID.Store(int32(maxSessions))
	}

	s.upstream = NewUpstream(config.Upstream, config.ReconnectPolicy, s.enqueueUpstreamMessage, logger)

	return s
}

func (s *Server) ConnectionCount() int64 {
	return s.connectionCount.Load()
}

func (s *Server) ConnectionsTotal() int64 {
	return s.connectionsTotal.Load()
}

func (s *Server) DisconnectionsTotal() int64 {
	return s.disconnectionsTotal.Load()
}

func (s *Server) IsUpstreamConnected() bool {
	return s.upstream.IsConnected()
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	s.grpcSrv = grpc.NewServer(s.keepaliveOptions()...)
	gentisv1.RegisterGentisServiceServer(s.grpcSrv, s)

	if s.config.UseArena {
		maxSessions := s.config.MaxSessions
		if maxSessions <= 0 {
			maxSessions = 16384
		}
		slotSize := int(unsafe.Sizeof(arena.SessionSlot{}))
		a, err := arena.New(slotSize, maxSessions)
		if err != nil {
			s.logger.Warn("relay arena init failed, falling back to heap session state", "err", err)
		} else {
			s.sessArena = a
		}
	}

	if err := s.upstream.Start(); err != nil {
		listener.Close()
		if s.sessArena != nil {
			s.sessArena.Close()
		}
		return fmt.Errorf("failed to connect to upstream: %w", err)
	}

	if s.config.MetricsEnabled {
		collector := metrics.NewCollector(s.engine, s, "relay")
		collector.SetDedupStats(s.dedup)
		collector.SetUpstreamStatus(s)
		if s.config.Observer != nil {
			collector.SetObserver(s.config.Observer)
		}
		s.metrics = metrics.NewServer(s.config.MetricsAddr, collector)
		if err := s.metrics.Start(); err != nil {
			listener.Close()
			s.upstream.Stop()
			return fmt.Errorf("failed to start metrics server: %w", err)
		}
		s.logger.Info("metrics server started", "addr", s.config.MetricsAddr)
	}

	for range s.config.FanoutWorkers {
		s.wg.Go(func() {
			s.fanoutWorker()
		})
	}

	s.wg.Go(func() {
		if err := s.grpcSrv.Serve(listener); err != nil {
			s.logger.Error("grpc serve error", "err", err)
		}
	})

	return nil
}

func (s *Server) Stop() error {
	s.cancel()
	s.grpcSrv.GracefulStop()
	s.upstream.Stop()
	close(s.incomingCh)
	s.dedup.Stop()

	if s.metrics != nil {
		if err := s.metrics.Stop(); err != nil {
			s.logger.Error("failed to stop metrics server", "err", err)
		}
	}

	s.wg.Wait()

	// Close the arena after all session cleanups have run (GracefulStop
	// drains in-flight streams which triggers Stream() → cleanupSession
	// → ArenaState.Close via the deferred path in Stream handler).
	if s.sessArena != nil {
		if err := s.sessArena.Close(); err != nil {
			s.logger.Error("failed to close session arena", "err", err)
		}
	}
	return nil
}

func (s *Server) Stream(stream gentisv1.GentisService_StreamServer) error {
	// Session contexts are rooted in the server context, not the stream
	// context: client cancellation must surface through Recv so in-flight
	// messages drain in order, while ctx.Done stays reserved for
	// server-initiated closes (credential expiry, shutdown).
	sess := s.createSession(s.ctx)
	if sess == nil {
		return fmt.Errorf("failed to create session")
	}
	defer s.cleanupSession(sess)

	go sess.runSender(stream)

	// Recv runs in its own goroutine so the dispatch loop can also exit
	// on session cancellation (credential expiry). Returning from this
	// handler cancels the stream context, which unblocks Recv.
	recvCh := make(chan *gentisv1.ClientMessage)
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case recvCh <- msg:
			case <-sess.ctx.Done():
				return
			}
		}
	}()

	for {
		// Drain pending traffic before honoring cancellation so a client
		// that publishes and immediately closes doesn't lose its last
		// writes to the select race.
		select {
		case msg := <-recvCh:
			sess.handleMessage(msg)
			continue
		default:
		}

		select {
		case <-sess.ctx.Done():
			for {
				select {
				case msg := <-recvCh:
					sess.handleMessage(msg)
				default:
					return nil
				}
			}
		case err := <-recvErr:
			if err == io.EOF {
				return nil
			}
			return err
		case msg := <-recvCh:
			sess.handleMessage(msg)
		}
	}
}

func (s *Server) createSession(parentCtx context.Context) *Session {
	ctx, cancel := context.WithCancel(parentCtx)

	// Prefer arena-derived IDs. NewArenaStateAuto allocates a slot and
	// returns an ArenaState whose ID is derived from the slot index,
	// keeping the ID space dense and bounded by MaxSessions — the
	// property that lets the flat SessionStore avoid a sync.Map fallback.
	var state transport.SessionState
	var id int
	if s.sessArena != nil {
		if as, err := arena.NewArenaStateAuto(s.sessArena, 1); err == nil {
			state = as
			id = as.ID()
		} else {
			// Arena exhausted. Fall back to a counter-allocated ID that
			// lives ABOVE the arena range (see New: nextID is initialized
			// to MaxSessions). The session still works; it just won't
			// benefit from arena state or flat-store lookup.
			id = int(s.nextID.Add(1))
			s.logger.Warn("relay arena alloc failed, falling back to heap",
				"err", err, "session_id", id)
			state = client.NewState(id)
		}
	} else {
		id = int(s.nextID.Add(1))
		state = client.NewState(id)
	}

	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](sendRingCap(s.config.BufferSize))
	if err != nil {
		s.logger.Error("relay send ring alloc failed", "err", err)
		cancel()
		if as, ok := state.(*arena.ArenaState); ok {
			as.Close()
		}
		return nil
	}

	subID := engine.SubscriberID(id)
	if s.store != nil {
		subID = s.store.AllocID(subID)
	}

	sess := &Session{
		id:       id,
		subID:    subID,
		state:    state,
		sendRing: ring,
		wakeCh:   make(chan struct{}, 1),
		drainCh:  make(chan struct{}, 1),
		relay:    s,
		ctx:      ctx,
		cancel:   cancel,
		channels: make(map[string]struct{}),
	}
	sess.qosc = qos.NewConsumer(s.engine, sess.produce, redeliveryCheckInterval)

	s.sessions.Store(id, sess)
	s.connectionCount.Add(1)
	s.connectionsTotal.Add(1)
	if s.store != nil {
		s.store.Register(subID, sess)
	}
	s.logger.Debug("session created", "session_id", id)
	return sess
}

func (s *Server) cleanupSession(sess *Session) {
	sess.qosc.Stop()
	if sess.expiryTimer != nil {
		sess.expiryTimer.Stop()
	}
	sess.cancel()
	s.sessions.Delete(sess.id)
	s.connectionCount.Add(-1)
	s.disconnectionsTotal.Add(1)
	if s.store != nil {
		s.store.Unregister(sess.subID)
	}
	s.engine.UnsubscribeAll(sess.subID)

	sess.subsMu.RLock()
	channels := make([]string, 0, len(sess.channels))
	for ch := range sess.channels {
		channels = append(channels, ch)
	}
	sess.subsMu.RUnlock()

	for _, ch := range channels {
		err := s.upstream.Unsubscribe(ch)
		if err != nil {
			s.logger.Warn("upstream unsubscribe failed during session cleanup", "channel", ch, "err", err)
		}
	}

	// If state came from the arena, return its slot.
	if as, ok := sess.state.(*arena.ArenaState); ok {
		as.Close()
	}
	s.logger.Debug("session closed", "session_id", sess.id)
}

func (s *Server) getSession(id int) (*Session, bool) {
	val, ok := s.sessions.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Session), true
}

func (s *Server) enqueueUpstreamMessage(channel string, data []byte) {
	select {
	case s.incomingCh <- incomingMsg{channel: channel, data: data}:
	case <-s.ctx.Done():
	}
}

func (s *Server) fanoutWorker() {
	for msg := range s.incomingCh {
		s.onUpstreamMessage(msg.channel, msg.data)
	}
}

func (s *Server) onUpstreamMessage(channel string, data []byte) {
	if !s.dedup.Check(channel, data) {
		return
	}

	if s.store != nil {
		s.engine.Publish(channel, data, 0, s.store.Deliver)
		return
	}

	s.engine.Publish(channel, data, 0, func(id engine.SubscriberID, d engine.Delivery) bool {
		sess, ok := s.getSession(int(id))
		if !ok {
			return false
		}
		return sess.DeliverMessage(d)
	})
}

func (sess *Session) runSender(stream gentisv1.GentisService_StreamServer) {
	defer sess.drainSendRing()
	for {
		for {
			msg, ok := sess.sendRing.TryConsume()
			if !ok {
				break
			}
			if err := stream.Send(msg); err != nil {
				// Outbound is dead but inbound may still hold client
				// messages; let the dispatch loop drain them until Recv
				// errors instead of cancelling the whole session.
				putServerMsgIfPooled(msg)
				return
			}
			putServerMsgIfPooled(msg)
			sess.signalDrain()
		}
		select {
		case <-sess.ctx.Done():
			return
		case <-sess.wakeCh:
		}
	}
}

func (sess *Session) drainSendRing() {
	for {
		msg, ok := sess.sendRing.TryConsume()
		if !ok {
			return
		}
		putServerMsgIfPooled(msg)
	}
}

const maxChannelNameLen = 256

func (sess *Session) handleMessage(msg *gentisv1.ClientMessage) {
	reqID := msg.Id
	switch m := msg.Message.(type) {
	case *gentisv1.ClientMessage_Connect:
		sess.handleConnect(m.Connect, reqID)
	case *gentisv1.ClientMessage_Ping:
		sess.handlePing(reqID)
	default:
		if !sess.state.IsAuthenticated() {
			sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "not authenticated", reqID)
			return
		}

		switch m := msg.Message.(type) {
		case *gentisv1.ClientMessage_Refresh:
			sess.handleRefresh(m.Refresh, reqID)
		case *gentisv1.ClientMessage_Confirm:
			sess.qosc.Confirm(m.Confirm.Channel, m.Confirm.Offset)
		case *gentisv1.ClientMessage_Subscribe:
			sess.handleSubscribe(m.Subscribe, reqID)
		case *gentisv1.ClientMessage_Unsubscribe:
			sess.handleUnsubscribe(m.Unsubscribe, reqID)
		case *gentisv1.ClientMessage_Publish:
			sess.handlePublish(m.Publish, reqID)
		default:
			sess.sendError(gentisv1.ErrorCode_ERROR_CODE_UNKNOWN_MESSAGE, "unknown message type", reqID)
		}
	}
}

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen
}

func (sess *Session) handleConnect(req *gentisv1.ConnectRequest, reqID string) {
	claims, err := sess.relay.config.Verifier.Verify(req.AuthToken)
	if err != nil {
		sess.relay.logger.Debug("authentication failed", "err", err)
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "authentication failed", reqID)
		return
	}
	sess.state.Authenticate(claims)
	sess.scheduleExpiry(claims.ExpiresAt)

	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId: fmt.Sprintf("relay-conn-%d", sess.id),
			},
		},
	})
}

// scheduleExpiry arms (or re-arms) the timer that cancels the session when
// its credentials lapse. Only the dispatch loop calls this, so no locking
// is needed. A zero expiry disables enforcement.
func (sess *Session) scheduleExpiry(exp time.Time) {
	if sess.expiryTimer != nil {
		sess.expiryTimer.Stop()
		sess.expiryTimer = nil
	}
	if exp.IsZero() {
		return
	}
	sess.expiryTimer = time.AfterFunc(time.Until(exp), func() {
		sess.relay.logger.Debug("session credentials expired", "session_id", sess.id)
		sess.cancel()
	})
}

func (sess *Session) handleRefresh(req *gentisv1.RefreshRequest, reqID string) {
	claims, err := sess.relay.config.Verifier.Verify(req.AuthToken)
	if err != nil {
		sess.relay.logger.Debug("refresh failed", "err", err)
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "authentication failed", reqID)
		return
	}
	if claims.Subject != sess.state.Subject() {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "refresh subject mismatch", reqID)
		return
	}
	sess.state.Authenticate(claims)
	sess.scheduleExpiry(claims.ExpiresAt)

	var exp uint64
	if !claims.ExpiresAt.IsZero() {
		exp = uint64(claims.ExpiresAt.Unix())
	}
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Refreshed{
			Refreshed: &gentisv1.RefreshResponse{ExpiresAt: exp},
		},
	})
}

func (sess *Session) handleSubscribe(req *gentisv1.SubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !sess.state.CanSubscribe(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "subscribe not allowed on channel", reqID)
		return
	}

	if max := sess.relay.config.MaxSubscriptions; max > 0 && sess.state.SubscriptionCount() >= max {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_SUBSCRIPTION_LIMIT, "subscription limit reached", reqID)
		return
	}

	if pattern.IsPattern(req.Channel) {
		sess.handleSubscribePattern(req, reqID)
		return
	}

	route := sess.relay.router.Route(req.Channel)

	if err := sess.relay.engine.SubscribePriority(sess.subID, req.Channel, int(req.Priority)); err != nil {
		sess.sendError(subscribeErrorCode(err), err.Error(), reqID)
		return
	}

	sess.state.AddSubscription(req.Channel)

	if req.MaxUnconfirmed != nil {
		enabled, timeout, maxRedeliveries := sess.relay.engine.QoSPolicy(req.Channel)
		if !enabled {
			sess.relay.engine.Unsubscribe(sess.subID, req.Channel)
			sess.state.RemoveSubscription(req.Channel)
			sess.sendError(gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "namespace does not offer at-least-once delivery", reqID)
			return
		}
		w := qos.NewWindow(int(req.MaxUnconfirmed.Count), int64(req.MaxUnconfirmed.Bytes), timeout, maxRedeliveries)
		sess.qosc.Subscribe(req.Channel, w)
	}

	sess.subsMu.Lock()
	sess.channels[req.Channel] = struct{}{}
	sess.subsMu.Unlock()

	var replay []engine.Delivery
	recovered := false
	if req.Recover != nil {
		replay, recovered = sess.relay.engine.Recover(req.Channel, req.Recover.Offset, req.Recover.Epoch)
	}

	if route.Mode == RouteModeRelay || route.Mode == RouteModeBoth {
		sess.relay.upstream.Subscribe(req.Channel)
	}

	resp := &gentisv1.SubscribedResponse{Channel: req.Channel}
	if req.Recover != nil {
		resp.Recovered = recovered
	}
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: resp,
		},
	})
	for _, d := range replay {
		sess.DeliverMessage(d)
	}
}

// handleSubscribePattern registers a wildcard subscription on the local
// engine and forwards the pattern verbatim to the upstream, which resolves
// it against its own channel space. Patterns are broadcast-only and
// replayless, so credit windows and recovery points are rejected up front.
func (sess *Session) handleSubscribePattern(req *gentisv1.SubscribeRequest, reqID string) {
	if req.MaxUnconfirmed != nil || req.Recover != nil {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "wildcard subscriptions do not support qos or recovery", reqID)
		return
	}

	route := sess.relay.router.Route(req.Channel)

	if err := sess.relay.engine.SubscribePattern(sess.subID, req.Channel); err != nil {
		sess.sendError(subscribeErrorCode(err), err.Error(), reqID)
		return
	}

	sess.state.AddSubscription(req.Channel)
	sess.subsMu.Lock()
	sess.channels[req.Channel] = struct{}{}
	sess.subsMu.Unlock()

	if route.Mode == RouteModeRelay || route.Mode == RouteModeBoth {
		sess.relay.upstream.Subscribe(req.Channel)
	}

	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: &gentisv1.SubscribedResponse{Channel: req.Channel},
		},
	})
}

func (sess *Session) unsubscribe(channel string) bool {
	if pattern.IsPattern(channel) {
		return sess.relay.engine.UnsubscribePattern(sess.subID, channel)
	}
	return sess.relay.engine.Unsubscribe(sess.subID, channel)
}

func (sess *Session) handleUnsubscribe(req *gentisv1.UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !sess.unsubscribe(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED, "Not subscribed to channel", reqID)
		return
	}

	sess.state.RemoveSubscription(req.Channel)
	sess.qosc.Unsubscribe(req.Channel)

	sess.subsMu.Lock()
	delete(sess.channels, req.Channel)
	sess.subsMu.Unlock()

	route := sess.relay.router.Route(req.Channel)
	if route.Mode == RouteModeRelay || route.Mode == RouteModeBoth {
		sess.relay.upstream.Unsubscribe(req.Channel)
	}

	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Unsubscribed{
			Unsubscribed: &gentisv1.UnsubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
}

func (sess *Session) handlePublish(req *gentisv1.PublishRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !sess.state.CanPublish(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "publish not allowed on channel", reqID)
		return
	}

	if max := sess.relay.config.MaxMessageSize; max > 0 && len(req.Data) > max {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE, "message exceeds max size", reqID)
		return
	}

	if err := sess.relay.engine.CheckPublish(req.Channel); err != nil {
		sess.sendError(publishErrorCode(err), err.Error(), reqID)
		return
	}

	route := sess.relay.router.Route(req.Channel)

	var result engine.PublishResult
	if route.Mode == RouteModeLocal || route.Mode == RouteModeBoth {
		if sess.relay.store != nil {
			result = sess.relay.engine.Publish(req.Channel, req.Data, sess.subID, sess.relay.store.Deliver)
		} else {
			result = sess.relay.engine.Publish(req.Channel, req.Data, sess.subID, func(id engine.SubscriberID, d engine.Delivery) bool {
				other, ok := sess.relay.getSession(int(id))
				if !ok {
					return false
				}
				return other.DeliverMessage(d)
			})
		}
	}

	if route.Mode == RouteModeRelay || route.Mode == RouteModeBoth {
		sess.relay.upstream.Publish(req.Channel, req.Data)
	}

	// Acks are opt-in via the correlation id and describe the relay-local
	// fanout only; upstream forwarding stays fire-and-forget.
	if reqID == "" {
		return
	}
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Published{
			Published: &gentisv1.PublishResponse{
				Channel:   req.Channel,
				Offset:    result.Offset,
				Epoch:     result.Epoch,
				Delivered: uint32(result.Delivered),
				Dropped:   uint32(result.Dropped),
			},
		},
	})
}

func (sess *Session) handlePing(reqID string) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Pong{
			Pong: &gentisv1.PongResponse{},
		},
	})
}

func (sess *Session) send(msg *gentisv1.ServerMessage) {
	for !sess.sendRing.TryProduce(msg) {
		select {
		case <-sess.ctx.Done():
			return
		case <-sess.drainCh:
		}
	}
	sess.wake()
}

func (sess *Session) signalDrain() {
	select {
	case sess.drainCh <- struct{}{}:
	default:
	}
}

func (sess *Session) sendError(code gentisv1.ErrorCode, message string, reqID string) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Error{
			Error: &gentisv1.ErrorResponse{
				Code:    code,
				Message: message,
			},
		},
	})
}

func (s *Server) keepaliveOptions() []grpc.ServerOption {
	if s.config.PingInterval <= 0 {
		return nil
	}
	return []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    s.config.PingInterval,
			Timeout: 2 * s.config.PingInterval,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

func subscribeErrorCode(err error) gentisv1.ErrorCode {
	switch {
	case errors.Is(err, engine.ErrAlreadySubscribed):
		return gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED
	case errors.Is(err, engine.ErrUnknownNamespace):
		return gentisv1.ErrorCode_ERROR_CODE_CHANNEL_NOT_FOUND
	case errors.Is(err, engine.ErrChannelFull):
		return gentisv1.ErrorCode_ERROR_CODE_SUBSCRIPTION_LIMIT
	case errors.Is(err, engine.ErrWildcardDenied):
		return gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	default:
		return gentisv1.ErrorCode_ERROR_CODE_INTERNAL
	}
}

func publishErrorCode(err error) gentisv1.ErrorCode {
	switch {
	case errors.Is(err, engine.ErrUnknownNamespace):
		return gentisv1.ErrorCode_ERROR_CODE_CHANNEL_NOT_FOUND
	case errors.Is(err, engine.ErrPublishDenied):
		return gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	default:
		return gentisv1.ErrorCode_ERROR_CODE_INTERNAL
	}
}
