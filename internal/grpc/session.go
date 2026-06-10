package grpc

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
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
	engine   *engine.Engine
	server   *Server
	logger   *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc

	expiryTimer *time.Timer

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
	if s.logger.Enabled(s.ctx, slog.LevelDebug) {
		s.logger.Debug("ring produce", "channel", d.Channel, "ring_len", s.sendRing.Len(), "ring_cap", s.sendRing.Cap())
	}
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
		id:       id,
		subID:    subID,
		state:    state,
		sendRing: sendRing,
		wakeCh:   make(chan struct{}, 1),
		drainCh:  make(chan struct{}, 1),
		engine:   s.engine,
		server:   s,
		logger:   s.logger.With("session_id", id),
		ctx:      ctx,
		cancel:   cancel,
	}
	sess.qosc = qos.NewConsumer(s.engine, sess.produce, redeliveryCheckInterval)

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
