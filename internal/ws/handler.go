package ws

import (
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

// Session implements MessageHandler so it can be used with DispatchMessage.
func (s *Session) ID() int                               { return s.id }
func (s *Session) State() transport.SessionState         { return s.state }
func (s *Session) Engine() *engine.Engine                { return s.engine }
func (s *Session) Store() *transport.SessionStore        { return s.store }
func (s *Session) Verifier() auth.Verifier               { return s.server.config.Verifier }
func (s *Session) Subject() string                       { return s.state.Subject() }
func (s *Session) Send(msg *ServerMessage)               { s.send(msg) }
func (s *Session) SendError(code, message, reqID string) { s.sendError(code, message, reqID) }

// ScheduleExpiry arms (or re-arms) the timer that force-closes the session
// when its credentials lapse. Only the dispatch goroutine calls this, so no
// locking is needed. A zero expiry disables enforcement.
func (s *Session) ScheduleExpiry(exp time.Time) {
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	if exp.IsZero() {
		return
	}
	s.expiryTimer = time.AfterFunc(time.Until(exp), func() {
		s.server.logger.Debug("session credentials expired", "session_id", s.id)
		s.cancel()
	})
}

var _ MessageHandler = (*Session)(nil)

func (s *Server) runReader(sess *Session, conn net.Conn) {
	for {
		// using a short read deadline to check context cancellation periodically
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))

		data, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			if sess.ctx.Err() != nil {
				return
			}

			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}

			var closeErr wsutil.ClosedError
			if errors.As(err, &closeErr) {
				if closeErr.Code == ws.StatusNormalClosure || closeErr.Code == ws.StatusGoingAway {
					return
				}
			}

			s.logger.Debug("websocket read error", "session_id", sess.id, "err", err)
			return
		}

		DispatchMessage(sess, data, s.config.ReadLimit)
	}
}

func (s *Server) runWriter(sess *Session, conn net.Conn) {
	for {
		select {
		case <-sess.ctx.Done():
			closeBody := ws.NewCloseFrameBody(ws.StatusGoingAway, "server shutting down")
			frame := ws.NewCloseFrame(closeBody)
			ws.WriteFrame(conn, frame)
			conn.Close()
			return
		case msg := <-sess.sendCh:
			data, err := json.Marshal(msg)
			isChannelMsg := msg.ChannelMessage != nil
			enqueuedAt := msg.enqueuedAt
			if isChannelMsg {
				putWSMsg(msg)
			}
			if err != nil {
				s.logger.Error("websocket marshal error", "session_id", sess.id, "err", err)
				continue
			}

			conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			err = wsutil.WriteServerMessage(conn, ws.OpText, data)
			conn.SetWriteDeadline(time.Time{})

			if err != nil {
				sess.cancel()
				return
			}

			if isChannelMsg && s.config.OnDeliveryLatency != nil && !enqueuedAt.IsZero() {
				s.config.OnDeliveryLatency(time.Since(enqueuedAt))
			}
		}
	}
}
