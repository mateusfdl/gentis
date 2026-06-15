package protocol

import (
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/pattern"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/transport"
)

// Connect and Ping are the only ops that run unauthenticated; every other
// op self-gates through authed so a transport dispatcher cannot forget
// the check.
func authed(s Session, reqID string) bool {
	if s.State().IsAuthenticated() {
		return true
	}
	s.SendError(CodeNotAuthenticated, "not authenticated", reqID)
	return false
}

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen && !pattern.HasReserved(name)
}

func Connect(s Session, req ConnectRequest, reqID string) {
	claims, err := s.Verifier().Verify(req.AuthToken)
	if err != nil {
		s.Logger().Debug("authentication failed", "err", err)
		s.SendError(CodeNotAuthenticated, "authentication failed", reqID)
		return
	}
	if s.State().IsAuthenticated() && claims.Subject != s.State().Subject() {
		s.SendError(CodeNotAuthenticated, "connect subject mismatch", reqID)
		return
	}
	s.State().Authenticate(claims)
	s.ScheduleExpiry(claims.ExpiresAt)

	version := min(req.ProtocolVersion, ServerProtocolVersion)
	if version == 0 {
		version = 1
	}
	s.SetProtocolVersion(version)
	s.SendConnected(reqID, version)
	s.Logger().Debug("client connected")
}

func Refresh(s Session, req RefreshRequest, reqID string) {
	if !authed(s, reqID) {
		return
	}
	claims, err := s.Verifier().Verify(req.AuthToken)
	if err != nil {
		s.Logger().Debug("refresh failed", "err", err)
		s.SendError(CodeNotAuthenticated, "authentication failed", reqID)
		return
	}
	if claims.Subject != s.State().Subject() {
		s.SendError(CodeNotAuthenticated, "refresh subject mismatch", reqID)
		return
	}
	s.State().Authenticate(claims)
	s.ScheduleExpiry(claims.ExpiresAt)

	var exp uint64
	if !claims.ExpiresAt.IsZero() {
		exp = uint64(claims.ExpiresAt.Unix())
	}
	s.SendRefreshed(reqID, exp)
}

func Ping(s Session, reqID string) {
	s.SendPong(reqID)
}

func Confirm(s Session, channel string, offset uint64, reqID string) {
	if !authed(s, reqID) {
		return
	}
	s.QoS().Confirm(channel, offset)
}

func Unknown(s Session, reqID string) {
	if !authed(s, reqID) {
		return
	}
	s.SendError(CodeUnknownMessage, "unknown message type", reqID)
}

func Subscribe(s Session, req SubscribeRequest, reqID string) {
	if !authed(s, reqID) {
		return
	}
	if !validateChannel(req.Channel) {
		s.Logger().Debug("invalid channel name", "channel", req.Channel)
		s.SendError(CodeInvalidPayload, "invalid channel name", reqID)
		return
	}
	if !s.State().CanSubscribe(req.Channel) {
		s.SendError(CodePermissionDenied, "subscribe not allowed on channel", reqID)
		return
	}
	if max := s.MaxSubscriptions(); max > 0 && s.State().SubscriptionCount() >= max {
		s.SendError(CodeSubscriptionLimit, "subscription limit reached", reqID)
		return
	}

	if pattern.IsPattern(req.Channel) {
		subscribePattern(s, req, reqID)
		return
	}

	// The window is installed and pinned before live fanout starts:
	// deliveries must never bypass the gate, and a live publish racing
	// the replay must not baseline the window past the recover point.
	if req.HasWindow {
		enabled, timeout, maxRedeliveries := s.Engine().QoSPolicy(req.Channel)
		if !enabled {
			s.SendError(CodePermissionDenied, "namespace does not offer at-least-once delivery", reqID)
			return
		}
		w := qos.NewWindow(int(req.Window.Count), int64(req.Window.Bytes), timeout, maxRedeliveries)
		if req.HasRecover {
			w.Baseline(req.Recover.Offset, req.Recover.Epoch)
		}
		s.QoS().Subscribe(req.Channel, w)
	}

	if err := s.Engine().SubscribePriority(s.SubscriberID(), req.Channel, int(req.Priority)); err != nil {
		s.QoS().Unsubscribe(req.Channel)
		s.SendError(SubscribeErrorCode(err), err.Error(), reqID)
		return
	}

	if s.State().AddSubscription(req.Channel) == transport.SubscriptionCapReached {
		s.Logger().Warn("subscription cap reached, channel dropped from session state", "channel", req.Channel)
	}

	var replay []engine.Delivery
	recovered := false
	if req.HasRecover {
		replay, recovered = s.Engine().Recover(req.Channel, req.Recover.Offset, req.Recover.Epoch)
	}

	if h := s.Hooks(); h != nil && h.OnSubscribed != nil {
		h.OnSubscribed(req.Channel)
	}

	s.SendSubscribed(reqID, req.Channel, recovered, req.HasRecover)
	for _, d := range replay {
		s.QoS().Deliver(d)
	}
	s.Logger().Debug("subscribed", "channel", req.Channel)
}

// subscribePattern registers a wildcard subscription. Patterns are
// broadcast-only and replayless, so credit windows and recovery points are
// rejected up front and the qos consumer is never touched.
func subscribePattern(s Session, req SubscribeRequest, reqID string) {
	if req.HasWindow || req.HasRecover {
		s.SendError(CodeInvalidPayload, "wildcard subscriptions do not support qos or recovery", reqID)
		return
	}

	if err := s.Engine().SubscribePattern(s.SubscriberID(), req.Channel); err != nil {
		s.SendError(SubscribeErrorCode(err), err.Error(), reqID)
		return
	}

	if s.State().AddSubscription(req.Channel) == transport.SubscriptionCapReached {
		s.Logger().Warn("subscription cap reached, channel dropped from session state", "channel", req.Channel)
	}
	if h := s.Hooks(); h != nil && h.OnSubscribed != nil {
		h.OnSubscribed(req.Channel)
	}
	s.SendSubscribed(reqID, req.Channel, false, false)
	s.Logger().Debug("subscribed", "pattern", req.Channel)
}

func Unsubscribe(s Session, channel, reqID string) {
	if !authed(s, reqID) {
		return
	}
	if !validateChannel(channel) {
		s.Logger().Debug("invalid channel name", "channel", channel)
		s.SendError(CodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	removed := false
	if pattern.IsPattern(channel) {
		removed = s.Engine().UnsubscribePattern(s.SubscriberID(), channel)
	} else {
		removed = s.Engine().Unsubscribe(s.SubscriberID(), channel)
	}
	if !removed {
		s.SendError(CodeNotSubscribed, "not subscribed to channel", reqID)
		return
	}

	s.State().RemoveSubscription(channel)
	s.QoS().Unsubscribe(channel)
	if h := s.Hooks(); h != nil && h.OnUnsubscribed != nil {
		h.OnUnsubscribed(channel)
	}
	s.SendUnsubscribed(reqID, channel)
	s.Logger().Debug("unsubscribed", "channel", channel)
}

func Publish(s Session, req PublishRequest, reqID string) {
	if !authed(s, reqID) {
		return
	}
	if !validateChannel(req.Channel) || pattern.IsPattern(req.Channel) {
		s.SendError(CodeInvalidPayload, "invalid channel name", reqID)
		return
	}
	if !s.State().CanPublish(req.Channel) {
		s.SendError(CodePermissionDenied, "publish not allowed on channel", reqID)
		return
	}
	if max := s.MaxMessageSize(); max > 0 && len(req.Data) > max {
		s.SendError(CodeMessageTooLarge, "message exceeds max size", reqID)
		return
	}
	if err := s.Engine().CheckPublish(req.Channel); err != nil {
		s.SendError(PublishErrorCode(err), err.Error(), reqID)
		return
	}

	local, forward := true, false
	if h := s.Hooks(); h != nil && h.PublishPlan != nil {
		local, forward = h.PublishPlan(req.Channel)
	}
	var result engine.PublishResult
	if local {
		result = s.Engine().Publish(req.Channel, req.Data, s.SubscriberID(), s.DeliverFunc())
	}
	if forward {
		s.Hooks().ForwardPublish(req.Channel, req.Data)
	}

	// Acks are opt-in: only clients that correlate publishes with an id
	// pay for the response message. The ack describes the local fanout
	// only; forwarding stays fire-and-forget.
	if reqID == "" {
		return
	}
	s.SendPublished(reqID, req.Channel, result)
}
