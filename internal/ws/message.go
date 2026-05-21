package ws

import (
	"encoding/json"
	"time"
)

const (
	ErrorCodeUnknownMessage    = "UNKNOWN_MESSAGE"
	ErrorCodeInvalidPayload    = "INVALID_PAYLOAD"
	ErrorCodeNotAuthenticated  = "NOT_AUTHENTICATED"
	ErrorCodeAlreadySubscribed = "ALREADY_SUBSCRIBED"
	ErrorCodeNotSubscribed     = "NOT_SUBSCRIBED"
)

type ClientMessage struct {
	ID          string              `json:"id,omitempty"`
	Connect     *ConnectRequest     `json:"connect,omitempty"`
	Subscribe   *SubscribeRequest   `json:"subscribe,omitempty"`
	Unsubscribe *UnsubscribeRequest `json:"unsubscribe,omitempty"`
	Publish     *PublishRequest     `json:"publish,omitempty"`
	Ping        *PingRequest        `json:"ping,omitempty"`
}

type ConnectRequest struct {
	AuthToken string `json:"auth_token"`
}

type SubscribeRequest struct {
	Channel string `json:"channel"`
}

type UnsubscribeRequest struct {
	Channel string `json:"channel"`
}

type PublishRequest struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type PingRequest struct{}

type ServerMessage struct {
	ID             string                 `json:"id,omitempty"`
	Connected      *ConnectedResponse     `json:"connected,omitempty"`
	Subscribed     *SubscribedResponse    `json:"subscribed,omitempty"`
	Unsubscribed   *UnsubscribedResponse  `json:"unsubscribed,omitempty"`
	ChannelMessage *ChannelMessagePayload `json:"channel_message,omitempty"`
	Pong           *PongResponse          `json:"pong,omitempty"`
	Error          *ErrorResponse         `json:"error,omitempty"`

	enqueuedAt time.Time
}

type ConnectedResponse struct {
	ConnectionID string `json:"connection_id"`
}

type SubscribedResponse struct {
	Channel string `json:"channel"`
}

type UnsubscribedResponse struct {
	Channel string `json:"channel"`
}

type ChannelMessagePayload struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type PongResponse struct{}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
