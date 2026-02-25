package grpc

import (
	"fmt"
	"io"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
)

const maxChannelNameLen = 256

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

func (s *Session) runSender(stream gentisv1.GentisService_StreamServer) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.sendCh:
			err := stream.Send(msg)
			if msg.GetChannelMessage() != nil {
				putServerMsg(msg)
			}
			if err != nil {
				s.cancel()
				return
			}
		}
	}
}

func (s *Session) handleMessage(msg *gentisv1.ClientMessage) {
	reqID := msg.Id
	switch m := msg.Message.(type) {
	case *gentisv1.ClientMessage_Connect:
		s.handleConnect(m.Connect, reqID)
	case *gentisv1.ClientMessage_Ping:
		s.handlePing(reqID)
	default:
		if !s.state.IsAuthenticated() {
			s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "not authenticated", reqID)
			return
		}

		switch m := msg.Message.(type) {
		case *gentisv1.ClientMessage_Subscribe:
			s.handleSubscribe(m.Subscribe, reqID)
		case *gentisv1.ClientMessage_Unsubscribe:
			s.handleUnsubscribe(m.Unsubscribe, reqID)
		case *gentisv1.ClientMessage_Publish:
			s.handlePublish(m.Publish, reqID)
		default:
			s.sendError(gentisv1.ErrorCode_ERROR_CODE_UNKNOWN_MESSAGE, "unknown message type", reqID)
		}
	}
}

func (s *Session) handleConnect(req *gentisv1.ConnectRequest, reqID string) {
	s.state.Authenticate(req.AuthToken)

	connID := fmt.Sprintf("conn-%d", s.id)
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId: connID,
			},
		},
	})
	s.logger.Debug("client connected", "connection_id", connID)
}

func (s *Session) handleSubscribe(req *gentisv1.SubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		s.logger.Debug("invalid channel name", "channel", req.Channel)
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !s.engine.Subscribe(engine.SubscriberID(s.id), req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED, "already subscribed to channel", reqID)
		return
	}

	s.state.AddSubscription(req.Channel)
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: &gentisv1.SubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
	s.logger.Debug("subscribed", "channel", req.Channel)
}

func (s *Session) handleUnsubscribe(req *gentisv1.UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		s.logger.Debug("invalid channel name", "channel", req.Channel)
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !s.engine.Unsubscribe(engine.SubscriberID(s.id), req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED, "Not subscribed to channel", reqID)
		return
	}

	s.state.RemoveSubscription(req.Channel)
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Unsubscribed{
			Unsubscribed: &gentisv1.UnsubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
	s.logger.Debug("unsubscribed", "channel", req.Channel)
}

func (s *Session) handlePublish(req *gentisv1.PublishRequest, reqID string) {
	if !validateChannel(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if s.server.store != nil {
		s.engine.Publish(req.Channel, req.Data, engine.SubscriberID(s.id), s.server.store.Deliver)
		return
	}

	s.engine.Publish(req.Channel, req.Data, engine.SubscriberID(s.id), func(id engine.SubscriberID, ch string, d []byte) bool {
		other, ok := s.server.getSession(int(id))
		if !ok {
			return false
		}
		msg := getServerMsg(ch, d)
		select {
		case other.sendCh <- msg:
			return true
		default:
			putServerMsg(msg)
			return false
		}
	})
}

func (s *Session) handlePing(reqID string) {
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Pong{
			Pong: &gentisv1.PongResponse{},
		},
	})
}

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen
}
