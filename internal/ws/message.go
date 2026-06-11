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
	ErrorCodePermissionDenied  = "PERMISSION_DENIED"
	ErrorCodeMessageTooLarge   = "MESSAGE_TOO_LARGE"
	ErrorCodeSubscriptionLimit = "SUBSCRIPTION_LIMIT"
	ErrorCodeChannelNotFound   = "CHANNEL_NOT_FOUND"
	ErrorCodeInternal          = "INTERNAL"
)

type ClientMessage struct {
	ID          string              `json:"id,omitempty"`
	Connect     *ConnectRequest     `json:"connect,omitempty"`
	Subscribe   *SubscribeRequest   `json:"subscribe,omitempty"`
	Unsubscribe *UnsubscribeRequest `json:"unsubscribe,omitempty"`
	Publish     *PublishRequest     `json:"publish,omitempty"`
	Ping        *PingRequest        `json:"ping,omitempty"`
	Refresh     *RefreshRequest     `json:"refresh,omitempty"`
	Confirm     *ConfirmRequest     `json:"confirm,omitempty"`
}

type ConnectRequest struct {
	AuthToken       string `json:"auth_token"`
	ProtocolVersion uint32 `json:"protocol_version,omitempty"`
}

type SubscribeRequest struct {
	Channel        string             `json:"channel"`
	Recover        *RecoverPoint      `json:"recover,omitempty"`
	MaxUnconfirmed *UnconfirmedWindow `json:"max_unconfirmed,omitempty"`
	Priority       int32              `json:"priority,omitempty"`
}

type UnconfirmedWindow struct {
	Count uint32 `json:"count"`
	Bytes uint64 `json:"bytes"`
}

type ConfirmRequest struct {
	Channel string `json:"channel"`
	Offset  uint64 `json:"offset"`
}

type RecoverPoint struct {
	Offset uint64 `json:"offset"`
	Epoch  uint64 `json:"epoch,string"`
}

type UnsubscribeRequest struct {
	Channel string `json:"channel"`
}

type PublishRequest struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type PingRequest struct{}

type RefreshRequest struct {
	AuthToken string `json:"auth_token"`
}

type ServerMessage struct {
	ID             string                 `json:"id,omitempty"`
	Connected      *ConnectedResponse     `json:"connected,omitempty"`
	Subscribed     *SubscribedResponse    `json:"subscribed,omitempty"`
	Unsubscribed   *UnsubscribedResponse  `json:"unsubscribed,omitempty"`
	ChannelMessage *ChannelMessagePayload `json:"channel_message,omitempty"`
	Pong           *PongResponse          `json:"pong,omitempty"`
	Error          *ErrorResponse         `json:"error,omitempty"`
	Published      *PublishResponse       `json:"published,omitempty"`
	Refreshed      *RefreshResponse       `json:"refreshed,omitempty"`

	enqueuedAt time.Time
}

type ConnectedResponse struct {
	ConnectionID    string `json:"connection_id"`
	ProtocolVersion uint32 `json:"protocol_version"`
}

type SubscribedResponse struct {
	Channel   string `json:"channel"`
	Recovered *bool  `json:"recovered,omitempty"`
}

type UnsubscribedResponse struct {
	Channel string `json:"channel"`
}

type PublishResponse struct {
	Channel   string `json:"channel"`
	Offset    uint64 `json:"offset"`
	Epoch     uint64 `json:"epoch,string"`
	Delivered uint32 `json:"delivered"`
	Dropped   uint32 `json:"dropped"`
}

type ChannelMessagePayload struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
	Offset  uint64          `json:"offset,omitempty"`
	Epoch   uint64          `json:"epoch,omitempty,string"`
}

type RefreshResponse struct {
	ExpiresAt uint64 `json:"expires_at"`
}

type PongResponse struct{}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
