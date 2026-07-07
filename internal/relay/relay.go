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
	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/metrics"
	"github.com/mateusfdl/gentis/internal/protocol"
	"github.com/mateusfdl/gentis/internal/protocol/pbcode"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/ringbuf"
	"github.com/mateusfdl/gentis/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

func sendRingCap(bufferSize int) int {
	if bufferSize < 2 {
		return 2
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
	offset  uint64
	epoch   uint64
}

type Server struct {
	gentisv1.UnimplementedGentisServiceServer

	config    *Config
	engine    *engine.Engine
	store     *transport.SessionStore
	sweeper   *qos.Sweeper
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

	// senderDone is closed when runSender exits, releasing any send()
	// blocked on a full ring that no longer has a consumer.
	senderDone chan struct{}

	relay    *Server
	ctx      context.Context
	cancel   context.CancelFunc
	subsMu   sync.RWMutex
	channels map[string]struct{}

	expiryTimer *time.Timer

	// authTimer cancels the session if it never authenticates. Armed at
	// creation, fires exactly once and no-ops when the session
	// authenticated in time.
	authTimer *time.Timer

	// qosc gates deliveries for at-least-once subscriptions. Sessions
	// without QoS1 windows pay a single atomic load per delivery.
	qosc *qos.Consumer

	logger    *slog.Logger
	deliverFn engine.DeliveryFunc
	hooks     *protocol.Hooks

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
		sweeper:    qos.NewSweeper(redeliveryCheckInterval),
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

	serverOpts := s.keepaliveOptions()
	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(s.config.TLSCertFile, s.config.TLSKeyFile)
		if err != nil {
			listener.Close()
			return fmt.Errorf("failed to load relay TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	s.grpcSrv = grpc.NewServer(serverOpts...)
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
	s.sweeper.Stop()

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
		id:         id,
		subID:      subID,
		state:      state,
		sendRing:   ring,
		wakeCh:     make(chan struct{}, 1),
		drainCh:    make(chan struct{}, 1),
		senderDone: make(chan struct{}),
		relay:      s,
		ctx:        ctx,
		cancel:     cancel,
		channels:   make(map[string]struct{}),
	}
	sess.logger = s.logger.With("session_id", id)
	sess.qosc = qos.NewConsumer(s.engine, sess.produce, s.sweeper, sess.logger)
	if s.store != nil {
		sess.deliverFn = s.store.Deliver
	} else {
		sess.deliverFn = func(id engine.SubscriberID, d engine.Delivery) bool {
			other, ok := s.getSession(int(id))
			if !ok {
				return false
			}
			return other.DeliverMessage(d)
		}
	}
	sess.hooks = &protocol.Hooks{
		OnSubscribed: func(channel string) {
			sess.subsMu.Lock()
			sess.channels[channel] = struct{}{}
			sess.subsMu.Unlock()
			if m := s.router.Route(channel).Mode; m == RouteModeRelay || m == RouteModeBoth {
				s.upstream.Subscribe(channel)
			}
		},
		OnUnsubscribed: func(channel string) {
			sess.subsMu.Lock()
			delete(sess.channels, channel)
			sess.subsMu.Unlock()
			if m := s.router.Route(channel).Mode; m == RouteModeRelay || m == RouteModeBoth {
				s.upstream.Unsubscribe(channel)
			}
		},
		PublishPlan: func(channel string) (local, forward bool) {
			m := s.router.Route(channel).Mode
			return m == RouteModeLocal || m == RouteModeBoth, m == RouteModeRelay || m == RouteModeBoth
		},
		ForwardPublish: func(channel string, data []byte) {
			if err := s.upstream.Publish(channel, data); err != nil {
				s.logger.Warn("publish dropped, upstream unavailable", "channel", channel, "err", err)
			}
		},
	}
	if d := s.config.AuthDeadline; d > 0 {
		sess.authTimer = time.AfterFunc(d, func() {
			if sess.state.IsAuthenticated() {
				return
			}
			s.logger.Warn("session reaped, auth deadline", "session_id", sess.id, "deadline", d)
			sess.cancel()
		})
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
	sess.qosc.Stop()
	if sess.expiryTimer != nil {
		sess.expiryTimer.Stop()
	}
	if sess.authTimer != nil {
		sess.authTimer.Stop()
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

func (s *Server) enqueueUpstreamMessage(channel string, data []byte, offset, epoch uint64) {
	select {
	case s.incomingCh <- incomingMsg{channel: channel, data: data, offset: offset, epoch: epoch}:
	case <-s.ctx.Done():
	}
}

func (s *Server) fanoutWorker() {
	for msg := range s.incomingCh {
		s.onUpstreamMessage(msg)
	}
}

func (s *Server) onUpstreamMessage(msg incomingMsg) {
	channel, data := msg.channel, msg.data
	if msg.offset != 0 && !s.dedup.Check(channel, msg.epoch, msg.offset) {
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
	// senderDone unblocks any send() waiting on ring drain: after the
	// outbound side dies nobody consumes the ring, so a blocked send in
	// the dispatch loop would wedge the handler forever. The session
	// itself stays alive to drain inbound traffic until Recv errors.
	defer close(sess.senderDone)
	defer sess.drainSendRing()
	for {
		for {
			msg, ok := sess.sendRing.TryConsume()
			if !ok {
				break
			}
			if err := stream.Send(msg); err != nil {
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

func (sess *Session) handleMessage(msg *gentisv1.ClientMessage) {
	reqID := msg.Id
	switch m := msg.Message.(type) {
	case *gentisv1.ClientMessage_Connect:
		protocol.Connect(sess, protocol.ConnectRequest{
			AuthToken:       m.Connect.AuthToken,
			ProtocolVersion: m.Connect.ProtocolVersion,
		}, reqID)
	case *gentisv1.ClientMessage_Ping:
		protocol.Ping(sess, reqID)
	case *gentisv1.ClientMessage_Refresh:
		protocol.Refresh(sess, protocol.RefreshRequest{AuthToken: m.Refresh.AuthToken}, reqID)
	case *gentisv1.ClientMessage_Confirm:
		protocol.Confirm(sess, m.Confirm.Channel, m.Confirm.Offset, reqID)
	case *gentisv1.ClientMessage_Subscribe:
		protocol.Subscribe(sess, pbcode.ToSubscribe(m.Subscribe), reqID)
	case *gentisv1.ClientMessage_Unsubscribe:
		protocol.Unsubscribe(sess, m.Unsubscribe.Channel, reqID)
	case *gentisv1.ClientMessage_Publish:
		protocol.Publish(sess, protocol.PublishRequest{Channel: m.Publish.Channel, Data: m.Publish.Data}, reqID)
	default:
		protocol.Unknown(sess, reqID)
	}
}

// Session implements protocol.Session so the shared core can drive it.
func (sess *Session) State() transport.SessionState     { return sess.state }
func (sess *Session) Engine() protocol.Engine           { return sess.relay.engine }
func (sess *Session) QoS() protocol.Consumer            { return sess.qosc }
func (sess *Session) SubscriberID() engine.SubscriberID { return sess.subID }
func (sess *Session) Verifier() auth.Verifier           { return sess.relay.config.Verifier }
func (sess *Session) Logger() *slog.Logger              { return sess.logger }
func (sess *Session) MaxSubscriptions() int             { return sess.relay.config.MaxSubscriptions }
func (sess *Session) MaxMessageSize() int               { return sess.relay.config.MaxMessageSize }
func (sess *Session) DeliverFunc() engine.DeliveryFunc  { return sess.deliverFn }
func (sess *Session) ScheduleExpiry(exp time.Time)      { sess.scheduleExpiry(exp) }
func (sess *Session) Hooks() *protocol.Hooks            { return sess.hooks }

// SetProtocolVersion is a no-op: the relay listener does not negotiate or
// batch, so the negotiated version never leaves the core.
func (sess *Session) SetProtocolVersion(uint32) {}

func (sess *Session) SendError(code protocol.ErrorCode, message, reqID string) {
	sess.sendError(pbcode.From(code), message, reqID)
}

func (sess *Session) SendConnected(reqID string, version uint32) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId: fmt.Sprintf("relay-conn-%d", sess.id),
			},
		},
	})
}

func (sess *Session) SendRefreshed(reqID string, expiresAt uint64) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Refreshed{
			Refreshed: &gentisv1.RefreshResponse{ExpiresAt: expiresAt},
		},
	})
}

func (sess *Session) SendSubscribed(reqID, channel string, recovered, didRecover bool) {
	resp := &gentisv1.SubscribedResponse{Channel: channel}
	if didRecover {
		resp.Recovered = recovered
	}
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: resp,
		},
	})
}

func (sess *Session) SendUnsubscribed(reqID, channel string) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Unsubscribed{
			Unsubscribed: &gentisv1.UnsubscribedResponse{Channel: channel},
		},
	})
}

func (sess *Session) SendPublished(reqID, channel string, r engine.PublishResult) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Published{
			Published: &gentisv1.PublishResponse{
				Channel:   channel,
				Offset:    r.Offset,
				Epoch:     r.Epoch,
				Delivered: uint32(r.Delivered),
				Dropped:   uint32(r.Dropped),
			},
		},
	})
}

func (sess *Session) SendPong(reqID string) {
	sess.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Pong{
			Pong: &gentisv1.PongResponse{},
		},
	})
}

var _ protocol.Session = (*Session)(nil)

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

func (sess *Session) send(msg *gentisv1.ServerMessage) {
	for !sess.sendRing.TryProduce(msg) {
		select {
		case <-sess.ctx.Done():
			return
		case <-sess.senderDone:
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
