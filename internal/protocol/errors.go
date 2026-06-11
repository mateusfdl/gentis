package protocol

import (
	"errors"

	"github.com/mateusfdl/gentis/internal/engine"
)

// ErrorCode is the neutral protocol error taxonomy. Each transport maps
// it to its wire enum in exactly one place, so a new code that misses a
// mapping fails loudly there instead of silently drifting per transport.
type ErrorCode uint8

const (
	CodeUnknownMessage ErrorCode = iota
	CodeInvalidPayload
	CodeNotAuthenticated
	CodeAlreadySubscribed
	CodeNotSubscribed
	CodePermissionDenied
	CodeMessageTooLarge
	CodeSubscriptionLimit
	CodeChannelNotFound
	CodeInternal
)

func SubscribeErrorCode(err error) ErrorCode {
	switch {
	case errors.Is(err, engine.ErrAlreadySubscribed):
		return CodeAlreadySubscribed
	case errors.Is(err, engine.ErrUnknownNamespace):
		return CodeChannelNotFound
	case errors.Is(err, engine.ErrChannelFull):
		return CodeSubscriptionLimit
	case errors.Is(err, engine.ErrWildcardDenied):
		return CodePermissionDenied
	default:
		return CodeInternal
	}
}

func PublishErrorCode(err error) ErrorCode {
	switch {
	case errors.Is(err, engine.ErrUnknownNamespace):
		return CodeChannelNotFound
	case errors.Is(err, engine.ErrPublishDenied):
		return CodePermissionDenied
	default:
		return CodeInternal
	}
}
