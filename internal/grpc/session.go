package grpc

import (
	"context"
	"log/slog"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

const sendBufferSize = 256

type Session struct {
	id     int
	state  *client.State
	sendCh chan *gentisv1.ServerMessage
	engine engine.Engine
	server *Server
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *Session) DeliverMessage(channel string, data []byte) bool {
	msg := getServerMsg(channel, data)
	select {
	case s.sendCh <- msg:
		return true
	default:
		putServerMsg(msg)
		s.logger.Warn("message dropped, send buffer full", "channel", channel)
		return false
	}
}

var _ transport.Sender = (*Session)(nil)

func (s *Server) createSession(parentCtx context.Context) *Session {
	id := int(s.nextID.Add(1))
	ctx, cancel := context.WithCancel(parentCtx)

	sess := &Session{
		id:     id,
		state:  client.NewState(id),
		sendCh: make(chan *gentisv1.ServerMessage, sendBufferSize),
		engine: s.engine,
		server: s,
		logger: s.logger.With("session_id", id),
		ctx:    ctx,
		cancel: cancel,
	}

	s.sessions.Store(id, sess)
	s.connectionCount.Add(1)
	s.connectionsTotal.Add(1)
	if s.store != nil {
		s.store.Register(engine.SubscriberID(id), sess)
	}
	sess.logger.Info("session created")
	return sess
}

func (s *Server) cleanupSession(sess *Session) {
	sess.cancel()
	s.sessions.Delete(sess.id)
	s.connectionCount.Add(-1)
	s.disconnectionsTotal.Add(1)
	if s.store != nil {
		s.store.Unregister(engine.SubscriberID(sess.id))
	}
	s.engine.UnsubscribeAll(engine.SubscriberID(sess.id))
	sess.logger.Info("session closed")
}

func (s *Session) send(msg *gentisv1.ServerMessage) {
	select {
	case s.sendCh <- msg:
	case <-s.ctx.Done():
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
