package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/protocol"
	"github.com/mateusfdl/gentis/internal/protocol/pbcode"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/ringbuf"
	"github.com/mateusfdl/gentis/internal/transport"
)

const (
	sendRingCapacity = 256

	// redeliveryCheckInterval is how often a session scans its QoS
	// windows for overdue unconfirmed deliveries.
	redeliveryCheckInterval = 200 * time.Millisecond
)

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

	engine    *engine.Engine
	server    *Server
	logger    *slog.Logger
	deliverFn engine.DeliveryFunc
	ctx       context.Context
	cancel    context.CancelFunc

	expiryTimer *time.Timer

	// authTimer cancels the session if it never authenticates. Armed at
	// creation, fires exactly once and no-ops when the session
	// authenticated in time.
	authTimer *time.Timer

	// qosc gates deliveries for at-least-once subscriptions. Sessions
	// without QoS1 windows pay a single atomic load per delivery.
	qosc *qos.Consumer

	// protoVersion is the negotiated protocol: 1 sends one message per
	// frame, 2 may pack consecutive deliveries into BatchMessage frames.
	protoVersion atomic.Uint32
}

func (s *Session) DeliverMessage(d engine.Delivery) bool {
	return s.qosc.Deliver(d)
}

func (s *Session) produce(d engine.Delivery) bool {
	msg := getServerMsg(d)
	if !s.sendRing.TryProduce(msg) {
		putServerMsg(msg)
		s.logger.Warn("message dropped, send buffer full", "channel", d.Channel)
		return false
	}
	s.wake()
	// No ring occupancy in this log: Len reads the consumer-owned tail
	// and would race from the producer side.
	s.logger.Debug("ring produce", "channel", d.Channel)
	return true
}

// scheduleExpiry arms (or re-arms) the timer that cancels the session when
// its credentials lapse. Only the dispatch loop calls this, so no locking
// is needed. A zero expiry disables enforcement.
func (s *Session) scheduleExpiry(exp time.Time) {
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	if exp.IsZero() {
		return
	}
	s.expiryTimer = time.AfterFunc(time.Until(exp), func() {
		s.logger.Debug("session credentials expired")
		s.cancel()
	})
}

var _ transport.Sender = (*Session)(nil)

// Session implements protocol.Session so the shared core can drive it.
func (s *Session) State() transport.SessionState     { return s.state }
func (s *Session) Engine() protocol.Engine           { return s.engine }
func (s *Session) QoS() protocol.Consumer            { return s.qosc }
func (s *Session) SubscriberID() engine.SubscriberID { return s.subID }
func (s *Session) Verifier() auth.Verifier           { return s.server.config.Verifier }
func (s *Session) Logger() *slog.Logger              { return s.logger }
func (s *Session) MaxSubscriptions() int             { return s.server.config.MaxSubscriptions }
func (s *Session) MaxMessageSize() int               { return s.server.config.MaxMessageSize }
func (s *Session) DeliverFunc() engine.DeliveryFunc  { return s.deliverFn }
func (s *Session) ScheduleExpiry(exp time.Time)      { s.scheduleExpiry(exp) }
func (s *Session) SetProtocolVersion(v uint32)       { s.protoVersion.Store(v) }
func (s *Session) Hooks() *protocol.Hooks            { return nil }

func (s *Session) SendError(code protocol.ErrorCode, message, reqID string) {
	s.sendError(pbcode.From(code), message, reqID)
}

func (s *Session) SendConnected(reqID string, version uint32) {
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId:    fmt.Sprintf("conn-%d", s.id),
				ProtocolVersion: version,
			},
		},
	})
}

func (s *Session) SendRefreshed(reqID string, expiresAt uint64) {
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Refreshed{
			Refreshed: &gentisv1.RefreshResponse{ExpiresAt: expiresAt},
		},
	})
}

func (s *Session) SendSubscribed(reqID, channel string, recovered, didRecover bool) {
	resp := &gentisv1.SubscribedResponse{Channel: channel}
	if didRecover {
		resp.Recovered = recovered
	}
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: resp,
		},
	})
}

func (s *Session) SendUnsubscribed(reqID, channel string) {
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Unsubscribed{
			Unsubscribed: &gentisv1.UnsubscribedResponse{Channel: channel},
		},
	})
}

func (s *Session) SendPublished(reqID, channel string, r engine.PublishResult) {
	s.send(&gentisv1.ServerMessage{
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

func (s *Session) SendPong(reqID string) {
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Pong{
			Pong: &gentisv1.PongResponse{},
		},
	})
}

var _ protocol.Session = (*Session)(nil)

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
			// lives ABOVE the arena range (nextID is initialized to
			// MaxSessions in New). The session still works; it just
			// won't benefit from arena state or flat-store lookup.
			id = int(s.nextID.Add(1))
			s.logger.Warn("grpc arena alloc failed, falling back to heap",
				"err", err, "session_id", id)
			state = client.NewState(id)
		}
	} else {
		id = int(s.nextID.Add(1))
		state = client.NewState(id)
	}

	sendRing, err := ringbuf.NewPointer[gentisv1.ServerMessage](sendRingCapacity)
	if err != nil {
		cancel()
		if as, ok := state.(*arena.ArenaState); ok {
			as.Close()
		}
		s.logger.Error("grpc send ring alloc failed", "err", err)
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
		sendRing:   sendRing,
		wakeCh:     make(chan struct{}, 1),
		drainCh:    make(chan struct{}, 1),
		senderDone: make(chan struct{}),
		engine:     s.engine,
		server:     s,
		logger:     s.logger.With("session_id", id),
		ctx:        ctx,
		cancel:     cancel,
	}
	sess.qosc = qos.NewConsumer(s.engine, sess.produce, redeliveryCheckInterval, sess.logger)
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
	if d := s.config.AuthDeadline; d > 0 {
		sess.authTimer = time.AfterFunc(d, func() {
			if sess.state.IsAuthenticated() {
				return
			}
			sess.logger.Warn("session reaped, auth deadline", "deadline", d)
			sess.cancel()
		})
	}

	s.sessions.Store(id, sess)
	s.connectionCount.Add(1)
	s.connectionsTotal.Add(1)
	if s.store != nil {
		s.store.Register(subID, sess)
	}
	sess.logger.Debug("session created")
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

	// If state came from the arena, return its slot.
	if as, ok := sess.state.(*arena.ArenaState); ok {
		as.Close()
	}
	sess.logger.Debug("session closed")
}

func (s *Session) send(msg *gentisv1.ServerMessage) {
	for !s.sendRing.TryProduce(msg) {
		select {
		case <-s.ctx.Done():
			return
		case <-s.senderDone:
			return
		case <-s.drainCh:
		}
	}
	s.wake()
}

func (s *Session) wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *Session) signalDrain() {
	select {
	case s.drainCh <- struct{}{}:
	default:
	}
}

func (s *Session) sendError(code gentisv1.ErrorCode, message string, reqID string) {
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Error{
			Error: &gentisv1.ErrorResponse{
				Code:    code,
				Message: message,
			},
		},
	})
}
