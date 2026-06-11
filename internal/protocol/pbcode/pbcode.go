// Package pbcode maps the neutral protocol error codes to the proto wire
// enum shared by the gRPC and relay transports. It lives outside the core
// so internal/protocol never imports generated wire types.
package pbcode

import (
	"fmt"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/protocol"
)

// ToSubscribe converts the proto subscribe request shared by the gRPC and
// relay transports into the neutral form.
func ToSubscribe(req *gentisv1.SubscribeRequest) protocol.SubscribeRequest {
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

func From(c protocol.ErrorCode) gentisv1.ErrorCode {
	switch c {
	case protocol.CodeUnknownMessage:
		return gentisv1.ErrorCode_ERROR_CODE_UNKNOWN_MESSAGE
	case protocol.CodeInvalidPayload:
		return gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD
	case protocol.CodeNotAuthenticated:
		return gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED
	case protocol.CodeAlreadySubscribed:
		return gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED
	case protocol.CodeNotSubscribed:
		return gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED
	case protocol.CodePermissionDenied:
		return gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	case protocol.CodeMessageTooLarge:
		return gentisv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE
	case protocol.CodeSubscriptionLimit:
		return gentisv1.ErrorCode_ERROR_CODE_SUBSCRIPTION_LIMIT
	case protocol.CodeChannelNotFound:
		return gentisv1.ErrorCode_ERROR_CODE_CHANNEL_NOT_FOUND
	case protocol.CodeInternal:
		return gentisv1.ErrorCode_ERROR_CODE_INTERNAL
	default:
		panic(fmt.Sprintf("pbcode: unmapped protocol error code %d", c))
	}
}
