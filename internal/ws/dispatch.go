package ws

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/pattern"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/transport"
)

const (
	maxChannelNameLen = 256

	// serverProtocolVersion is the highest protocol this server speaks.
	serverProtocolVersion = 2

	// maxBatchSize caps how many deliveries one array frame packs.
	maxBatchSize = 64

	// maxBatchBytes caps the payload bytes one array frame accumulates,
	// so a burst of large messages cannot balloon a single frame.
	maxBatchBytes = 1 << 20
)

// MessageHandler is the minimal interface needed by DispatchMessage.
type MessageHandler interface {
	ID() int
	State() transport.SessionState
	Engine() *engine.Engine
	Store() *transport.SessionStore
	Verifier() auth.Verifier
	Subject() string
	ScheduleExpiry(exp time.Time)
	MaxMessageSize() int
	MaxSubscriptions() int
	Deliver(d engine.Delivery)
	Consumer() *qos.Consumer
	SetProtocolVersion(v uint32)
	Send(msg *ServerMessage)
	SendError(code string, message string, reqID string)
}

// DispatchMessage unmarshals a raw JSON payload and routes it to the
// appropriate handler.
func DispatchMessage(h MessageHandler, data []byte, readLimit int64) {
	if int64(len(data)) > readLimit {
		h.SendError(ErrorCodeMessageTooLarge, "message too large", "")
		return
	}

	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		h.SendError(ErrorCodeInvalidPayload, "invalid JSON", "")
		return
	}

	dispatchParsed(h, &msg)
}

func dispatchParsed(h MessageHandler, msg *ClientMessage) {
	reqID := msg.ID
	switch {
	case msg.Connect != nil:
		handleConnect(h, msg.Connect, reqID)
	case msg.Ping != nil:
		handlePing(h, reqID)
	default:
		if !h.State().IsAuthenticated() {
			h.SendError(ErrorCodeNotAuthenticated, "not authenticated", reqID)
			return
		}

		switch {
		case msg.Refresh != nil:
			handleRefresh(h, msg.Refresh, reqID)
		case msg.Confirm != nil:
			h.Consumer().Confirm(msg.Confirm.Channel, msg.Confirm.Offset)
		case msg.Subscribe != nil:
			handleSubscribe(h, msg.Subscribe, reqID)
		case msg.Unsubscribe != nil:
			handleUnsubscribe(h, msg.Unsubscribe, reqID)
		case msg.Publish != nil:
			handlePublish(h, msg.Publish, reqID)
		default:
			h.SendError(ErrorCodeUnknownMessage, "unknown message type", reqID)
		}
	}
}

func handleConnect(h MessageHandler, req *ConnectRequest, reqID string) {
	claims, err := h.Verifier().Verify(req.AuthToken)
	if err != nil {
		h.SendError(ErrorCodeNotAuthenticated, "authentication failed", reqID)
		return
	}
	if h.State().IsAuthenticated() && claims.Subject != h.Subject() {
		h.SendError(ErrorCodeNotAuthenticated, "connect subject mismatch", reqID)
		return
	}
	h.State().Authenticate(claims)
	h.ScheduleExpiry(claims.ExpiresAt)

	version := min(req.ProtocolVersion, serverProtocolVersion)
	if version == 0 {
		version = 1
	}
	h.SetProtocolVersion(version)

	h.Send(&ServerMessage{
		ID: reqID,
		Connected: &ConnectedResponse{
			ConnectionID:    fmt.Sprintf("ws-conn-%d", h.ID()),
			ProtocolVersion: version,
		},
	})
}

func handleRefresh(h MessageHandler, req *RefreshRequest, reqID string) {
	claims, err := h.Verifier().Verify(req.AuthToken)
	if err != nil {
		h.SendError(ErrorCodeNotAuthenticated, "authentication failed", reqID)
		return
	}
	if claims.Subject != h.Subject() {
		h.SendError(ErrorCodeNotAuthenticated, "refresh subject mismatch", reqID)
		return
	}
	h.State().Authenticate(claims)
	h.ScheduleExpiry(claims.ExpiresAt)

	var exp uint64
	if !claims.ExpiresAt.IsZero() {
		exp = uint64(claims.ExpiresAt.Unix())
	}
	h.Send(&ServerMessage{
		ID:        reqID,
		Refreshed: &RefreshResponse{ExpiresAt: exp},
	})
}

func handlePing(h MessageHandler, reqID string) {
	h.Send(&ServerMessage{
		ID:   reqID,
		Pong: &PongResponse{},
	})
}

func handleSubscribe(h MessageHandler, req *SubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		h.SendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	if !h.State().CanSubscribe(req.Channel) {
		h.SendError(ErrorCodePermissionDenied, "subscribe not allowed on channel", reqID)
		return
	}

	if max := h.MaxSubscriptions(); max > 0 && h.State().SubscriptionCount() >= max {
		h.SendError(ErrorCodeSubscriptionLimit, "subscription limit reached", reqID)
		return
	}

	if pattern.IsPattern(req.Channel) {
		handleSubscribePattern(h, req, reqID)
		return
	}

	// The window is installed and pinned before live fanout starts:
	// deliveries must never bypass the gate, and a live publish racing
	// the replay must not baseline the window past the recover point.
	if req.MaxUnconfirmed != nil {
		enabled, timeout, maxRedeliveries := h.Engine().QoSPolicy(req.Channel)
		if !enabled {
			h.SendError(ErrorCodePermissionDenied, "namespace does not offer at-least-once delivery", reqID)
			return
		}
		w := qos.NewWindow(int(req.MaxUnconfirmed.Count), int64(req.MaxUnconfirmed.Bytes), timeout, maxRedeliveries)
		if req.Recover != nil {
			w.Baseline(req.Recover.Offset, req.Recover.Epoch)
		}
		h.Consumer().Subscribe(req.Channel, w)
	}

	if err := h.Engine().SubscribePriority(engine.SubscriberID(h.ID()), req.Channel, int(req.Priority)); err != nil {
		h.Consumer().Unsubscribe(req.Channel)
		h.SendError(subscribeErrorCode(err), err.Error(), reqID)
		return
	}

	h.State().AddSubscription(req.Channel)

	resp := &SubscribedResponse{Channel: req.Channel}
	var replay []engine.Delivery
	if req.Recover != nil {
		deliveries, ok := h.Engine().Recover(req.Channel, req.Recover.Offset, req.Recover.Epoch)
		resp.Recovered = &ok
		replay = deliveries
	}

	h.Send(&ServerMessage{
		ID:         reqID,
		Subscribed: resp,
	})
	for _, d := range replay {
		h.Deliver(d)
	}
}

// handleSubscribePattern registers a wildcard subscription. Patterns are
// broadcast-only and replayless, so credit windows and recovery points are
// rejected up front.
func handleSubscribePattern(h MessageHandler, req *SubscribeRequest, reqID string) {
	if req.MaxUnconfirmed != nil || req.Recover != nil {
		h.SendError(ErrorCodeInvalidPayload, "wildcard subscriptions do not support qos or recovery", reqID)
		return
	}

	if err := h.Engine().SubscribePattern(engine.SubscriberID(h.ID()), req.Channel); err != nil {
		h.SendError(subscribeErrorCode(err), err.Error(), reqID)
		return
	}

	h.State().AddSubscription(req.Channel)
	h.Send(&ServerMessage{
		ID:         reqID,
		Subscribed: &SubscribedResponse{Channel: req.Channel},
	})
}

func unsubscribe(h MessageHandler, channel string) bool {
	if pattern.IsPattern(channel) {
		return h.Engine().UnsubscribePattern(engine.SubscriberID(h.ID()), channel)
	}
	return h.Engine().Unsubscribe(engine.SubscriberID(h.ID()), channel)
}

func handleUnsubscribe(h MessageHandler, req *UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		h.SendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	if !unsubscribe(h, req.Channel) {
		h.SendError(ErrorCodeNotSubscribed, "not subscribed to channel", reqID)
		return
	}

	h.State().RemoveSubscription(req.Channel)
	h.Consumer().Unsubscribe(req.Channel)
	h.Send(&ServerMessage{
		ID: reqID,
		Unsubscribed: &UnsubscribedResponse{
			Channel: req.Channel,
		},
	})
}

func handlePublish(h MessageHandler, req *PublishRequest, reqID string) {
	if !validateChannel(req.Channel) {
		h.SendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	if !h.State().CanPublish(req.Channel) {
		h.SendError(ErrorCodePermissionDenied, "publish not allowed on channel", reqID)
		return
	}

	if max := h.MaxMessageSize(); max > 0 && len(req.Data) > max {
		h.SendError(ErrorCodeMessageTooLarge, "message exceeds max size", reqID)
		return
	}

	if err := h.Engine().CheckPublish(req.Channel); err != nil {
		h.SendError(publishErrorCode(err), err.Error(), reqID)
		return
	}

	result := h.Engine().Publish(req.Channel, []byte(req.Data), engine.SubscriberID(h.ID()), h.Store().Deliver)

	// Acks are opt-in: only clients that correlate publishes with an id
	// pay for the response message.
	if reqID == "" {
		return
	}
	h.Send(&ServerMessage{
		ID: reqID,
		Published: &PublishResponse{
			Channel:   req.Channel,
			Offset:    result.Offset,
			Epoch:     result.Epoch,
			Delivered: uint32(result.Delivered),
			Dropped:   uint32(result.Dropped),
		},
	})
}

func subscribeErrorCode(err error) string {
	switch {
	case errors.Is(err, engine.ErrAlreadySubscribed):
		return ErrorCodeAlreadySubscribed
	case errors.Is(err, engine.ErrUnknownNamespace):
		return ErrorCodeChannelNotFound
	case errors.Is(err, engine.ErrChannelFull):
		return ErrorCodeSubscriptionLimit
	case errors.Is(err, engine.ErrWildcardDenied):
		return ErrorCodePermissionDenied
	default:
		return ErrorCodeUnknownMessage
	}
}

func publishErrorCode(err error) string {
	switch {
	case errors.Is(err, engine.ErrUnknownNamespace):
		return ErrorCodeChannelNotFound
	case errors.Is(err, engine.ErrPublishDenied):
		return ErrorCodePermissionDenied
	default:
		return ErrorCodeUnknownMessage
	}
}

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen
}
