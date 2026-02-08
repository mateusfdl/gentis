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
			if err := stream.Send(msg); err != nil {
				s.cancel()
				return
			}
		}
	}
}

func (s *Session) handleMessage(msg *gentisv1.ClientMessage) {
	switch m := msg.Message.(type) {
	case *gentisv1.ClientMessage_Connect:
		s.handleConnect(m.Connect)
	case *gentisv1.ClientMessage_Ping:
		s.handlePing()
	default:
		if !s.state.IsAuthenticated() {
			s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "not authenticated")
			return
		}

		switch m := msg.Message.(type) {
		case *gentisv1.ClientMessage_Subscribe:
			s.handleSubscribe(m.Subscribe)
		case *gentisv1.ClientMessage_Unsubscribe:
			s.handleUnsubscribe(m.Unsubscribe)
		case *gentisv1.ClientMessage_Publish:
			s.handlePublish(m.Publish)
		default:
			s.sendError(gentisv1.ErrorCode_ERROR_CODE_UNKNOWN_MESSAGE, "unknown message type")
		}
	}
}

func (s *Session) handleConnect(req *gentisv1.ConnectRequest) {
	s.state.Authenticate(req.AuthToken)

	s.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId: fmt.Sprintf("conn-%d", s.id),
			},
		},
	})
}

func (s *Session) handleSubscribe(req *gentisv1.SubscribeRequest) {
	if !validateChannel(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name")
		return
	}

	if !s.engine.Subscribe(engine.SubscriberID(s.id), req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED, "already subscribed to channel")
		return
	}

	s.state.AddSubscription(req.Channel)
	s.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: &gentisv1.SubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
}

func (s *Session) handleUnsubscribe(req *gentisv1.UnsubscribeRequest) {
	if !validateChannel(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name")
		return
	}

	if !s.engine.Unsubscribe(engine.SubscriberID(s.id), req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED, "Not subscribed to channel")
		return
	}

	s.state.RemoveSubscription(req.Channel)
	s.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Unsubscribed{
			Unsubscribed: &gentisv1.UnsubscribedResponse{
				Channel: req.Channel,
			},
		},
	})
}

func (s *Session) handlePublish(req *gentisv1.PublishRequest) {
	if !validateChannel(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name")
		return
	}

	chMsg := &gentisv1.ChannelMessage{
		Channel: req.Channel,
		Data:    req.Data,
	}

	s.engine.Publish(req.Channel, req.Data, engine.SubscriberID(s.id), func(id engine.SubscriberID, _ string, _ []byte) bool {
		other, ok := s.server.getSession(int(id))
		if !ok {
			return false
		}
		msg := &gentisv1.ServerMessage{
			Message: &gentisv1.ServerMessage_ChannelMessage{
				ChannelMessage: chMsg,
			},
		}
		select {
		case other.sendCh <- msg:
			return true
		default:
			return false
		}
	})
}

func (s *Session) handlePing() {
	s.send(&gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_Pong{
			Pong: &gentisv1.PongResponse{},
		},
	})
}

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen
}
