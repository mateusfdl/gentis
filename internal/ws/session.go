package ws

import (
	"context"

	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

const wsIDOffset int64 = 1 << 31

type Session struct {
	id     int
	state  *client.State
	sendCh chan *ServerMessage
	engine *engine.Engine
	store  *transport.SessionStore
	server *Server
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *Session) DeliverMessage(channel string, data []byte) bool {
	msg := getWSMsg(channel, data)
	select {
	case s.sendCh <- msg:
		return true
	default:
		putWSMsg(msg)
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

	s.sessions.Store(id, sess)
	s.connectionCount.Add(1)
	s.store.Register(engine.SubscriberID(id), sess)
	return sess
}

func (s *Server) cleanupSession(sess *Session) {
	sess.cancel()
	s.sessions.Delete(sess.id)
	s.connectionCount.Add(-1)
	s.store.Unregister(engine.SubscriberID(sess.id))
	s.engine.UnsubscribeAll(engine.SubscriberID(sess.id))
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
