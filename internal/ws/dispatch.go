package ws

import (
	"encoding/json"
	"fmt"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

const maxChannelNameLen = 256

// MessageHandler is the minimal interface needed by DispatchMessage.
type MessageHandler interface {
	ID() int
	State() transport.SessionState
	Engine() *engine.Engine
	Store() *transport.SessionStore
	Send(msg *ServerMessage)
	SendError(code string, message string, reqID string)
}

// DispatchMessage unmarshals a raw JSON payload and routes it to the
// appropriate handler.
func DispatchMessage(h MessageHandler, data []byte, readLimit int64) {
	if int64(len(data)) > readLimit {
		h.SendError(ErrorCodeInvalidPayload, "message too large", "")
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
	h.State().Authenticate(req.AuthToken)
	h.Send(&ServerMessage{
		ID: reqID,
		Connected: &ConnectedResponse{
			ConnectionID: fmt.Sprintf("ws-conn-%d", h.ID()),
		},
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

	if !h.Engine().Subscribe(engine.SubscriberID(h.ID()), req.Channel) {
		h.SendError(ErrorCodeAlreadySubscribed, "already subscribed to channel", reqID)
		return
	}

	h.State().AddSubscription(req.Channel)
	h.Send(&ServerMessage{
		ID: reqID,
		Subscribed: &SubscribedResponse{
			Channel: req.Channel,
		},
	})
}

func handleUnsubscribe(h MessageHandler, req *UnsubscribeRequest, reqID string) {
	if !validateChannel(req.Channel) {
		h.SendError(ErrorCodeInvalidPayload, "invalid channel name", reqID)
		return
	}

	if !h.Engine().Unsubscribe(engine.SubscriberID(h.ID()), req.Channel) {
		h.SendError(ErrorCodeNotSubscribed, "not subscribed to channel", reqID)
		return
	}

	h.State().RemoveSubscription(req.Channel)
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

func validateChannel(name string) bool {
	return len(name) > 0 && len(name) <= maxChannelNameLen
}
