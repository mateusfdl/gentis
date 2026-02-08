package relay

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/metrics"
	"google.golang.org/grpc"
)

const sendBufferSize = 256

type Server struct {
	gentisv1.UnimplementedGentisServiceServer

	config   *Config
	engine   engine.Engine
	upstream *Upstream
	router   *Router
	dedup    *Deduplicator

	listener        net.Listener
	grpcSrv         *grpc.Server
	sessions        sync.Map
	nextID          atomic.Int32
	metrics         *metrics.Server
	connectionCount atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Session struct {
	id       int
	state    *client.State
	sendCh   chan *gentisv1.ServerMessage
	relay    *Server
	ctx      context.Context
	cancel   context.CancelFunc
	subsMu   sync.RWMutex
	channels map[string]struct{}
}

func New(opts ...Option) *Server {
	config := defaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		config: config,
		engine: engine.New(),
		router: NewRouter(nil),
		dedup:  NewDeduplicator(5 * time.Second),
		ctx:    ctx,
		cancel: cancel,
	}

	s.upstream = NewUpstream(config.Upstream, config.ReconnectPolicy, s.onUpstreamMessage)

	return s
}

// ConnectionCount returns the current number of active connections.
func (s *Server) ConnectionCount() int64 {
	return s.connectionCount.Load()
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	s.grpcSrv = grpc.NewServer()
	gentisv1.RegisterGentisServiceServer(s.grpcSrv, s)

	if err := s.upstream.Start(); err != nil {
		listener.Close()
		return fmt.Errorf("failed to connect to upstream: %w", err)
	}

	if s.config.MetricsEnabled {
		collector := metrics.NewCollector(s.engine, s, "relay")
		s.metrics = metrics.NewServer(s.config.MetricsAddr, collector)
		if err := s.metrics.Start(); err != nil {
			listener.Close()
			s.upstream.Stop()
			return fmt.Errorf("failed to start metrics server: %w", err)
		}
		log.Printf("Metrics server listening on %s", s.config.MetricsAddr)
	}

	s.wg.Go(func() {
		if err := s.grpcSrv.Serve(listener); err != nil {
			log.Printf("grpc server error: %v", err)
		}
	})

	log.Printf("relay server listening on %s, upstream: %s", s.config.ListenAddr, s.config.Upstream.Address)
	return nil
}

func (s *Server) Stop() error {
	s.cancel()
	s.grpcSrv.GracefulStop()
	s.upstream.Stop()
	s.dedup.Stop()

	if s.metrics != nil {
		if err := s.metrics.Stop(); err != nil {
			log.Printf("Error stopping metrics server: %v", err)
		}
	}

	s.wg.Wait()
	return nil
}

func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) Stream(stream gentisv1.GentisService_StreamServer) error {
	sess := s.createSession(stream.Context())
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
	id := int(s.nextID.Add(1))
	ctx, cancel := context.WithCancel(parentCtx)

	sess := &Session{
		id:       id,
		state:    client.NewState(id),
		sendCh:   make(chan *gentisv1.ServerMessage, sendBufferSize),
		relay:    s,
		ctx:      ctx,
		cancel:   cancel,
		channels: make(map[string]struct{}),
	}

	s.sessions.Store(id, sess)
	s.connectionCount.Add(1)
	return sess
}

func (s *Server) cleanupSession(sess *Session) {
	sess.cancel()
	s.sessions.Delete(sess.id)
	s.connectionCount.Add(-1)
	s.engine.UnsubscribeAll(engine.SubscriberID(sess.id))

	sess.subsMu.RLock()
	channels := make([]string, 0, len(sess.channels))
	for ch := range sess.channels {
		channels = append(channels, ch)
	}
	sess.subsMu.RUnlock()

	for _, ch := range channels {
		err := s.upstream.Unsubscribe(ch)
		if err != nil {
			log.Printf("failed to delete the following channel: %v", ch)
		}
	}
}

func (s *Server) getSession(id int) (*Session, bool) {
	val, ok := s.sessions.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Session), true
}

func (s *Server) onUpstreamMessage(channel string, data []byte) {
	if !s.dedup.Check(channel, data) {
		return
	}

	msg := &gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_ChannelMessage{
			ChannelMessage: &gentisv1.ChannelMessage{
				Channel: channel,
				Data:    data,
			},
		},
	}

	s.engine.Publish(channel, data, 0, func(id engine.SubscriberID, _ string, _ []byte) bool {
		sess, ok := s.getSession(int(id))
		if !ok {
			return false
		}
		select {
		case sess.sendCh <- msg:
			return true
		default:
			return false
		}
	})
}

func (sess *Session) runSender(stream gentisv1.GentisService_StreamServer) {
	for {
		select {
		case <-sess.ctx.Done():
			return
		case msg := <-sess.sendCh:
			if err := stream.Send(msg); err != nil {
				return
			}
		}
	}
}

func (sess *Session) handleMessage(msg *gentisv1.ClientMessage) {
	switch m := msg.Message.(type) {
	case *gentisv1.ClientMessage_Connect:
		sess.handleConnect(m.Connect)
	case *gentisv1.ClientMessage_Subscribe:
		sess.handleSubscribe(m.Subscribe)
	case *gentisv1.ClientMessage_Unsubscribe:
		sess.handleUnsubscribe(m.Unsubscribe)
	case *gentisv1.ClientMessage_Publish:
		sess.handlePublish(m.Publish)
	case *gentisv1.ClientMessage_Ping:
		sess.handlePing()
	default:
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_UNKNOWN_MESSAGE, "Unknown message type")
	}
}

func (sess *Session) handleConnect(req *gentisv1.ConnectRequest) {
	sess.state.Authenticate(req.AuthToken)

	sess.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId: fmt.Sprintf("relay-conn-%d", sess.id),
			},
		},
	})
}

func (sess *Session) handleSubscribe(req *gentisv1.SubscribeRequest) {
	route := sess.relay.router.Route(req.Channel)

	if !sess.relay.engine.Subscribe(engine.SubscriberID(sess.id), req.Channel) {
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
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: &gentisv1.SubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
}

func (sess *Session) handleUnsubscribe(req *gentisv1.UnsubscribeRequest) {
	if !sess.relay.engine.Unsubscribe(engine.SubscriberID(sess.id), req.Channel) {
		sess.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED, "Not subscribed to channel")
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
		Message: &gentisv1.ServerMessage_Unsubscribed{
			Unsubscribed: &gentisv1.UnsubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
}

func (sess *Session) handlePublish(req *gentisv1.PublishRequest) {
	route := sess.relay.router.Route(req.Channel)

	msg := &gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_ChannelMessage{
			ChannelMessage: &gentisv1.ChannelMessage{
				Channel: req.Channel,
				Data:    req.Data,
			},
		},
	}

	if route.Mode == RouteModeLocal || route.Mode == RouteModeBoth {
		sess.relay.engine.Publish(req.Channel, req.Data, engine.SubscriberID(sess.id), func(id engine.SubscriberID, _ string, _ []byte) bool {
			other, ok := sess.relay.getSession(int(id))
			if !ok {
				return false
			}
			select {
			case other.sendCh <- msg:
				return true
			default:
				return false
			}
		})
	}

	if route.Mode == RouteModeRelay || route.Mode == RouteModeBoth {
		sess.relay.upstream.Publish(req.Channel, req.Data)
	}
}

func (sess *Session) handlePing() {
	sess.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Pong{
			Pong: &gentisv1.PongResponse{},
		},
	})
}

func (sess *Session) send(msg *gentisv1.ServerMessage) {
	select {
	case sess.sendCh <- msg:
	case <-sess.ctx.Done():
	}
}

func (sess *Session) sendError(code gentisv1.ErrorCode, message string) {
	sess.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Error{
			Error: &gentisv1.ErrorResponse{
				Code:    code,
				Message: message,
			},
		},
	})
}
