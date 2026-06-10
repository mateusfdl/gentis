package ws

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/transport"
)

const wsIDOffset int64 = 1 << 31

type Session struct {
	id          int
	state       *client.State
	sendCh      chan *ServerMessage
	engine      *engine.Engine
	store       *transport.SessionStore
	server      *Server
	ctx         context.Context
	cancel      context.CancelFunc
	expiryTimer *time.Timer
	lastRecv    atomic.Int64

	// qosc gates deliveries for at-least-once subscriptions. Sessions
	// without QoS1 windows pay a single atomic load per delivery.
	qosc *qos.Consumer

	// protoVersion is the negotiated protocol: 1 sends one message per
	// frame, 2 may pack consecutive deliveries into array frames.
	protoVersion atomic.Uint32
}

const redeliveryCheckInterval = 200 * time.Millisecond

func (s *Session) DeliverMessage(d engine.Delivery) bool {
	if s.qosc.Gate(d) == qos.Deferred {
		return true
	}
	if !s.produce(d) {
		s.qosc.Rollback(d.Channel, d.Offset)
		return false
	}
	return true
}

func (s *Session) produce(d engine.Delivery) bool {
	msg := getWSMsg(d)
	if s.server.config.OnDeliveryLatency != nil {
		msg.enqueuedAt = time.Now()
	}
	select {
	case s.sendCh <- msg:
		return true
	default:
		putWSMsg(msg)
		s.server.logger.Warn("message dropped, send buffer full", "channel", d.Channel, "session_id", s.id)
		return false
	}
}

var _ transport.Sender = (*Session)(nil)

func (s *Server) createSession() *Session {
	id := int(s.nextID.Add(1))
	ctx, cancel := context.WithCancel(s.ctx)

	sess := &Session{
		id:     id,
		state:  client.NewState(id),
		sendCh: make(chan *ServerMessage, s.config.SendBufferSize),
		engine: s.engine,
		store:  s.store,
		server: s,
		ctx:    ctx,
		cancel: cancel,
	}
	sess.lastRecv.Store(time.Now().UnixNano())
	sess.qosc = qos.NewConsumer(s.engine, sess.produce, redeliveryCheckInterval)

	s.sessions.Store(id, sess)
	s.connectionCount.Add(1)
	s.store.Register(engine.SubscriberID(id), sess)
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
	s.store.Unregister(engine.SubscriberID(sess.id))
	s.engine.UnsubscribeAll(engine.SubscriberID(sess.id))
	s.logger.Debug("session closed", "session_id", sess.id)
}

func (s *Session) send(msg *ServerMessage) {
	select {
	case s.sendCh <- msg:
	case <-s.ctx.Done():
	}
}

func (s *Session) sendError(code string, message string, reqID string) {
	s.send(&ServerMessage{
		ID: reqID,
		Error: &ErrorResponse{
			Code:    code,
			Message: message,
		},
	})
}
