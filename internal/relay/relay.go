package relay

import (
	"context"
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
	"github.com/mateusfdl/gentis/internal/ringbuf"
	"github.com/mateusfdl/gentis/internal/transport"
	"google.golang.org/grpc"
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

	dropsFull atomic.Int64
}

func (sess *Session) DeliverMessage(channel string, data []byte) bool {
	msg := getServerMsg(channel, data)
	if !sess.sendRing.TryProduce(msg) {
		putServerMsg(msg)
		sess.dropsFull.Add(1)
		sess.relay.logger.Warn("message dropped, send buffer full", "channel", channel, "session_id", sess.id)
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

	s.grpcSrv = grpc.NewServer()
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
	sess := s.createSession(stream.Context())
	if sess == nil {
		return fmt.Errorf("failed to create session")
	}
	defer s.cleanupSession(sess)

	go sess.runSender(stream)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		sess.handleMessage(msg)
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

	s.engine.Publish(channel, data, 0, func(id engine.SubscriberID, ch string, d []byte) bool {
		sess, ok := s.getSession(int(id))
		if !ok {
			return false
		}
		return sess.DeliverMessage(ch, d)
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
				putServerMsgIfPooled(msg)
				sess.cancel()
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
	sess.state.Authenticate(req.AuthToken)

	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId: fmt.Sprintf("relay-conn-%d", sess.id),
			},
		},
	})
}

func (sess *Session) handleSubscribe(req *gentisv1.SubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	route := sess.relay.router.Route(req.Channel)

	if !sess.relay.engine.Subscribe(sess.subID, req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED, "already subscribed to channel", reqID)
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
			Subscribed: &gentisv1.SubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
}

func (sess *Session) handleUnsubscribe(req *gentisv1.UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !sess.relay.engine.Unsubscribe(sess.subID, req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED, "Not subscribed to channel", reqID)
		return
	}

	sess.state.RemoveSubscription(req.Channel)

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

	route := sess.relay.router.Route(req.Channel)

	if route.Mode == RouteModeLocal || route.Mode == RouteModeBoth {
		if sess.relay.store != nil {
			sess.relay.engine.Publish(req.Channel, req.Data, sess.subID, sess.relay.store.Deliver)
		} else {
			sess.relay.engine.Publish(req.Channel, req.Data, sess.subID, func(id engine.SubscriberID, ch string, d []byte) bool {
				other, ok := sess.relay.getSession(int(id))
				if !ok {
					return false
				}
				return other.DeliverMessage(ch, d)
			})
		}
	}

	if route.Mode == RouteModeRelay || route.Mode == RouteModeBoth {
		sess.relay.upstream.Publish(req.Channel, req.Data)
	}
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
