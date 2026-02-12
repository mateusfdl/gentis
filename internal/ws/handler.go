package ws

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/engine"
)

const maxChannelNameLen = 256

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

			log.Printf("ws read error (session %d): %v", sess.id, err)
			return
		}

		if int64(len(data)) > s.config.ReadLimit {
			sess.sendError(ErrorCodeInvalidPayload, "message too large", "")
			continue
		}

		var msg ClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			sess.sendError(ErrorCodeInvalidPayload, "invalid JSON", "")
			continue
		}

		sess.handleMessage(&msg)
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
			if msg.ChannelMessage != nil {
				putWSMsg(msg)
			}
			if err != nil {
				log.Printf("ws marshal error (session %d): %v", sess.id, err)
				continue
			}

			conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			err = wsutil.WriteServerMessage(conn, ws.OpText, data)
			conn.SetWriteDeadline(time.Time{})

			if err != nil {
				sess.cancel()
				return
			}
		}
	}
}

func (sess *Session) handleMessage(msg *ClientMessage) {
	reqID := msg.ID
	switch {
	case msg.Connect != nil:
		sess.handleConnect(msg.Connect, reqID)
	case msg.Ping != nil:
		sess.handlePing(reqID)
	default:
		if !sess.state.IsAuthenticated() {
			sess.sendError(ErrorCodeNotAuthenticated, "not authenticated", reqID)
			return
		}

		switch {
		case msg.Subscribe != nil:
			sess.handleSubscribe(msg.Subscribe, reqID)
		case msg.Unsubscribe != nil:
			sess.handleUnsubscribe(msg.Unsubscribe, reqID)
		case msg.Publish != nil:
			sess.handlePublish(msg.Publish, reqID)
		default:
			sess.sendError(ErrorCodeUnknownMessage, "unknown message type", reqID)
		}
	}
}

func (sess *Session) handleConnect(req *ConnectRequest, reqID string) {
	sess.state.Authenticate(req.AuthToken)

	sess.send(&ServerMessage{
		ID: reqID,
		Connected: &ConnectedResponse{
			ConnectionID: fmt.Sprintf("ws-conn-%d", sess.id),
		},
	})
}

func (sess *Session) handlePing(reqID string) {
	sess.send(&ServerMessage{
		ID:   reqID,
		Pong: &PongResponse{},
	})
}

func (sess *Session) handleSubscribe(req *SubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	if !sess.engine.Subscribe(engine.SubscriberID(sess.id), req.Channel) {
		sess.sendError(ErrorCodeAlreadySubscribed, "already subscribed to channel", reqID)
		return
	}

	sess.state.AddSubscription(req.Channel)
	sess.send(&ServerMessage{
		ID: reqID,
		Subscribed: &SubscribedResponse{
			Channel: req.Channel,
		},
	})
}

func (sess *Session) handleUnsubscribe(req *UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	if !sess.engine.Unsubscribe(engine.SubscriberID(sess.id), req.Channel) {
		sess.sendError(ErrorCodeNotSubscribed, "not subscribed to channel", reqID)
		return
	}

	sess.state.RemoveSubscription(req.Channel)
	sess.send(&ServerMessage{
		ID: reqID,
		Unsubscribed: &UnsubscribedResponse{
			Channel: req.Channel,
		},
	})
}

func (sess *Session) handlePublish(req *PublishRequest, reqID string) {
	if !validateChannel(req.Channel) {
		sess.sendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	sess.engine.Publish(req.Channel, []byte(req.Data), engine.SubscriberID(sess.id), sess.store.Deliver)
}

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen
}
