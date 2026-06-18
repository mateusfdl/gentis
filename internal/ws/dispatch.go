package ws

import (
	"encoding/json"

	"github.com/mateusfdl/gentis/internal/protocol"
)

const (
	// maxBatchSize caps how many deliveries one array frame packs.
	maxBatchSize = 64

	// maxBatchBytes caps the payload bytes one array frame accumulates,
	// so a burst of large messages cannot balloon a single frame.
	maxBatchBytes = 1 << 20

	// writeBufferSize is the per-connection bufio.Writer capacity. The
	// writer drains every queued message into this buffer and flushes once,
	// so a spike costs one write syscall per bufferSize-worth of frames
	// instead of one syscall per frame. Held for the connection's lifetime.
	writeBufferSize = 16 << 10

	// maxDrainFrames caps how many frames one drain pass buffers before it
	// flushes and yields back to the run loop, so a relentless producer
	// cannot starve keepalive pings or cancellation in the writer.
	maxDrainFrames = 256
)

// DispatchMessage unmarshals a raw JSON payload and routes it to the
// shared protocol core.
func DispatchMessage(s *Session, data []byte, readLimit int64) {
	if int64(len(data)) > readLimit {
		s.SendError(protocol.CodeMessageTooLarge, "message too large", "")
		return
	}

	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.SendError(protocol.CodeInvalidPayload, "invalid JSON", "")
		return
	}

	dispatchParsed(s, &msg)
}

func dispatchParsed(s *Session, msg *ClientMessage) {
	reqID := msg.ID
	switch {
	case msg.Connect != nil:
		protocol.Connect(s, protocol.ConnectRequest{
			AuthToken:       msg.Connect.AuthToken,
			ProtocolVersion: msg.Connect.ProtocolVersion,
		}, reqID)
	case msg.Ping != nil:
		protocol.Ping(s, reqID)
	case msg.Refresh != nil:
		protocol.Refresh(s, protocol.RefreshRequest{AuthToken: msg.Refresh.AuthToken}, reqID)
	case msg.Confirm != nil:
		protocol.Confirm(s, msg.Confirm.Channel, msg.Confirm.Offset, reqID)
	case msg.Subscribe != nil:
		protocol.Subscribe(s, toSubscribe(msg.Subscribe), reqID)
	case msg.Unsubscribe != nil:
		protocol.Unsubscribe(s, msg.Unsubscribe.Channel, reqID)
	case msg.Publish != nil:
		protocol.Publish(s, protocol.PublishRequest{Channel: msg.Publish.Channel, Data: msg.Publish.Data}, reqID)
	default:
		protocol.Unknown(s, reqID)
	}
}

func toSubscribe(req *SubscribeRequest) protocol.SubscribeRequest {
	out := protocol.SubscribeRequest{Channel: req.Channel, Priority: req.Priority}
	if req.MaxUnconfirmed != nil {
		out.HasWindow = true
		out.Window = protocol.Window{Count: req.MaxUnconfirmed.Count, Bytes: req.MaxUnconfirmed.Bytes}
	}
	if req.Recover != nil {
		out.HasRecover = true
		out.Recover = protocol.RecoverPoint{Offset: req.Recover.Offset, Epoch: req.Recover.Epoch}
	}
	return out
}

func wsErrorCode(code protocol.ErrorCode) string {
	switch code {
	case protocol.CodeUnknownMessage:
		return ErrorCodeUnknownMessage
	case protocol.CodeInvalidPayload:
		return ErrorCodeInvalidPayload
	case protocol.CodeNotAuthenticated:
		return ErrorCodeNotAuthenticated
	case protocol.CodeAlreadySubscribed:
		return ErrorCodeAlreadySubscribed
	case protocol.CodeNotSubscribed:
		return ErrorCodeNotSubscribed
	case protocol.CodePermissionDenied:
		return ErrorCodePermissionDenied
	case protocol.CodeMessageTooLarge:
		return ErrorCodeMessageTooLarge
	case protocol.CodeSubscriptionLimit:
		return ErrorCodeSubscriptionLimit
	case protocol.CodeChannelNotFound:
		return ErrorCodeChannelNotFound
	case protocol.CodeInternal:
		return ErrorCodeInternal
	default:
		panic("ws: unmapped protocol error code")
	}
}
