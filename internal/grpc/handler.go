package grpc

import (
	"errors"
	"fmt"
	"io"

	"github.com/mateusfdl/gentis/internal/qos"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/pattern"
)

const (
	maxChannelNameLen = 256

	// serverProtocolVersion is the highest protocol this server speaks.
	serverProtocolVersion = 2

	// maxBatchSize caps how many deliveries one BatchMessage frame packs.
	maxBatchSize = 64
)

func (s *Server) Stream(stream gentisv1.GentisService_StreamServer) error {
	// Session contexts are rooted in the server context, not the stream
	// context: client cancellation must surface through Recv so in-flight
	// messages drain in order, while ctx.Done stays reserved for
	// server-initiated closes (credential expiry, shutdown).
	sess := s.createSession(s.ctx)
	if sess == nil {
		return fmt.Errorf("failed to create session")
	}
	defer s.cleanupSession(sess)

	go sess.runSender(stream)

	// Recv runs in its own goroutine so the dispatch loop can also exit
	// on session cancellation (credential expiry). Returning from this
	// handler cancels the stream context, which unblocks Recv.
	recvCh := make(chan *gentisv1.ClientMessage)
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case recvCh <- msg:
			case <-sess.ctx.Done():
				return
			}
		}
	}()

	for {
		// Drain pending traffic before honoring cancellation so a client
		// that publishes and immediately closes doesn't lose its last
		// writes to the select race.
		select {
		case msg := <-recvCh:
			sess.handleMessage(msg)
			continue
		default:
		}

		select {
		case <-sess.ctx.Done():
			for {
				select {
				case msg := <-recvCh:
					sess.handleMessage(msg)
				default:
					return nil
				}
			}
		case err := <-recvErr:
			if err == io.EOF {
				return nil
			}
			return err
		case msg := <-recvCh:
			sess.handleMessage(msg)
		}
	}
}

func (s *Session) runSender(stream gentisv1.GentisService_StreamServer) {
	// senderDone unblocks any send() waiting on ring drain: after the
	// outbound side dies nobody consumes the ring, so a blocked send in
	// the dispatch loop would wedge the handler forever. The session
	// itself stays alive to drain inbound traffic until Recv errors.
	defer close(s.senderDone)
	defer s.drainSendRing()
	var pending []*gentisv1.ServerMessage
	for {
		batching := s.protoVersion.Load() >= 2
		for {
			msg, ok := s.sendRing.TryConsume()
			if !ok {
				break
			}
			if batching && msg.GetChannelMessage() != nil && msg.Id == "" {
				pending = append(pending, msg)
				if len(pending) >= maxBatchSize {
					if !s.flushPending(stream, &pending) {
						return
					}
				}
				continue
			}
			if !s.flushPending(stream, &pending) {
				putServerMsgIfPooled(msg)
				return
			}
			if err := stream.Send(msg); err != nil {
				putServerMsgIfPooled(msg)
				return
			}
			putServerMsgIfPooled(msg)
			s.signalDrain()
		}
		if !s.flushPending(stream, &pending) {
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-s.wakeCh:
		}
	}
}

// flushPending sends accumulated consecutive deliveries: as-is when there
// is one, packed into a single BatchMessage frame when there are more.
func (s *Session) flushPending(stream gentisv1.GentisService_StreamServer, pending *[]*gentisv1.ServerMessage) bool {
	msgs := *pending
	switch len(msgs) {
	case 0:
		return true
	case 1:
		err := stream.Send(msgs[0])
		putServerMsgIfPooled(msgs[0])
		*pending = msgs[:0]
		if err != nil {
			return false
		}
		s.signalDrain()
		return true
	}

	env := getBatchMsg()
	batch := env.GetBatch()
	for _, m := range msgs {
		batch.Messages = append(batch.Messages, m.GetChannelMessage())
	}
	err := stream.Send(env)
	for _, m := range msgs {
		putServerMsgIfPooled(m)
		s.signalDrain()
	}
	putBatchMsg(env)
	*pending = msgs[:0]
	return err == nil
}

func (s *Session) drainSendRing() {
	for {
		msg, ok := s.sendRing.TryConsume()
		if !ok {
			return
		}
		putServerMsgIfPooled(msg)
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
		case *gentisv1.ClientMessage_Refresh:
			s.handleRefresh(m.Refresh, reqID)
		case *gentisv1.ClientMessage_Confirm:
			s.qosc.Confirm(m.Confirm.Channel, m.Confirm.Offset)
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
	claims, err := s.server.config.Verifier.Verify(req.AuthToken)
	if err != nil {
		s.logger.Debug("authentication failed", "err", err)
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "authentication failed", reqID)
		return
	}
	if s.state.IsAuthenticated() && claims.Subject != s.state.Subject() {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "connect subject mismatch", reqID)
		return
	}
	s.state.Authenticate(claims)
	s.scheduleExpiry(claims.ExpiresAt)

	version := min(req.ProtocolVersion, serverProtocolVersion)
	if version == 0 {
		version = 1
	}
	s.protoVersion.Store(version)

	connID := fmt.Sprintf("conn-%d", s.id)
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Connected{
			Connected: &gentisv1.ConnectedResponse{
				ConnectionId:    connID,
				ProtocolVersion: version,
			},
		},
	})
	s.logger.Debug("client connected", "connection_id", connID)
}

func (s *Session) handleRefresh(req *gentisv1.RefreshRequest, reqID string) {
	claims, err := s.server.config.Verifier.Verify(req.AuthToken)
	if err != nil {
		s.logger.Debug("refresh failed", "err", err)
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "authentication failed", reqID)
		return
	}
	if claims.Subject != s.state.Subject() {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED, "refresh subject mismatch", reqID)
		return
	}
	s.state.Authenticate(claims)
	s.scheduleExpiry(claims.ExpiresAt)

	var exp uint64
	if !claims.ExpiresAt.IsZero() {
		exp = uint64(claims.ExpiresAt.Unix())
	}
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Refreshed{
			Refreshed: &gentisv1.RefreshResponse{ExpiresAt: exp},
		},
	})
}

func (s *Session) handleSubscribe(req *gentisv1.SubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		s.logger.Debug("invalid channel name", "channel", req.Channel)
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !s.state.CanSubscribe(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "subscribe not allowed on channel", reqID)
		return
	}

	if max := s.server.config.MaxSubscriptions; max > 0 && s.state.SubscriptionCount() >= max {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_SUBSCRIPTION_LIMIT, "subscription limit reached", reqID)
		return
	}

	if pattern.IsPattern(req.Channel) {
		s.handleSubscribePattern(req, reqID)
		return
	}

	// The window is installed and pinned before live fanout starts:
	// deliveries must never bypass the gate, and a live publish racing
	// the replay must not baseline the window past the recover point.
	if req.MaxUnconfirmed != nil {
		enabled, timeout, maxRedeliveries := s.engine.QoSPolicy(req.Channel)
		if !enabled {
			s.sendError(gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "namespace does not offer at-least-once delivery", reqID)
			return
		}
		w := qos.NewWindow(int(req.MaxUnconfirmed.Count), int64(req.MaxUnconfirmed.Bytes), timeout, maxRedeliveries)
		if req.Recover != nil {
			w.Baseline(req.Recover.Offset, req.Recover.Epoch)
		}
		s.qosc.Subscribe(req.Channel, w)
	}

	if err := s.engine.SubscribePriority(s.subID, req.Channel, int(req.Priority)); err != nil {
		s.qosc.Unsubscribe(req.Channel)
		s.sendError(subscribeErrorCode(err), err.Error(), reqID)
		return
	}

	s.state.AddSubscription(req.Channel)

	resp := &gentisv1.SubscribedResponse{Channel: req.Channel}
	var replay []engine.Delivery
	if req.Recover != nil {
		deliveries, ok := s.engine.Recover(req.Channel, req.Recover.Offset, req.Recover.Epoch)
		resp.Recovered = ok
		replay = deliveries
	}

	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: resp,
		},
	})
	for _, d := range replay {
		s.DeliverMessage(d)
	}
	s.logger.Debug("subscribed", "channel", req.Channel)
}

// handleSubscribePattern registers a wildcard subscription. Patterns are
// broadcast-only and replayless, so credit windows and recovery points are
// rejected up front.
func (s *Session) handleSubscribePattern(req *gentisv1.SubscribeRequest, reqID string) {
	if req.MaxUnconfirmed != nil || req.Recover != nil {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "wildcard subscriptions do not support qos or recovery", reqID)
		return
	}

	if err := s.engine.SubscribePattern(s.subID, req.Channel); err != nil {
		s.sendError(subscribeErrorCode(err), err.Error(), reqID)
		return
	}

	s.state.AddSubscription(req.Channel)
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Subscribed{
			Subscribed: &gentisv1.SubscribedResponse{Channel: req.Channel},
		},
	})
	s.logger.Debug("subscribed", "pattern", req.Channel)
}

func (s *Session) unsubscribe(channel string) bool {
	if pattern.IsPattern(channel) {
		return s.engine.UnsubscribePattern(s.subID, channel)
	}
	return s.engine.Unsubscribe(s.subID, channel)
}

func (s *Session) handleUnsubscribe(req *gentisv1.UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		s.logger.Debug("invalid channel name", "channel", req.Channel)
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD, "invalid channel name", reqID)
		return
	}

	if !s.unsubscribe(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED, "Not subscribed to channel", reqID)
		return
	}

	s.state.RemoveSubscription(req.Channel)
	s.qosc.Unsubscribe(req.Channel)
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

	if !s.state.CanPublish(req.Channel) {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "publish not allowed on channel", reqID)
		return
	}

	if max := s.server.config.MaxMessageSize; max > 0 && len(req.Data) > max {
		s.sendError(gentisv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE, "message exceeds max size", reqID)
		return
	}

	if err := s.engine.CheckPublish(req.Channel); err != nil {
		s.sendError(publishErrorCode(err), err.Error(), reqID)
		return
	}

	var result engine.PublishResult
	if s.server.store != nil {
		result = s.engine.Publish(req.Channel, req.Data, s.subID, s.server.store.Deliver)
	} else {
		result = s.engine.Publish(req.Channel, req.Data, s.subID, func(id engine.SubscriberID, d engine.Delivery) bool {
			other, ok := s.server.getSession(int(id))
			if !ok {
				return false
			}
			return other.DeliverMessage(d)
		})
	}

	// Acks are opt-in: only clients that correlate publishes with an id
	// pay for the response message.
	if reqID == "" {
		return
	}
	s.send(&gentisv1.ServerMessage{
		Id: reqID,
		Message: &gentisv1.ServerMessage_Published{
			Published: &gentisv1.PublishResponse{
				Channel:   req.Channel,
				Offset:    result.Offset,
				Epoch:     result.Epoch,
				Delivered: uint32(result.Delivered),
				Dropped:   uint32(result.Dropped),
			},
		},
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

func subscribeErrorCode(err error) gentisv1.ErrorCode {
	switch {
	case errors.Is(err, engine.ErrAlreadySubscribed):
		return gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED
	case errors.Is(err, engine.ErrUnknownNamespace):
		return gentisv1.ErrorCode_ERROR_CODE_CHANNEL_NOT_FOUND
	case errors.Is(err, engine.ErrChannelFull):
		return gentisv1.ErrorCode_ERROR_CODE_SUBSCRIPTION_LIMIT
	case errors.Is(err, engine.ErrWildcardDenied):
		return gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	default:
		return gentisv1.ErrorCode_ERROR_CODE_INTERNAL
	}
}

func publishErrorCode(err error) gentisv1.ErrorCode {
	switch {
	case errors.Is(err, engine.ErrUnknownNamespace):
		return gentisv1.ErrorCode_ERROR_CODE_CHANNEL_NOT_FOUND
	case errors.Is(err, engine.ErrPublishDenied):
		return gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	default:
		return gentisv1.ErrorCode_ERROR_CODE_INTERNAL
	}
}
