// Package protocol is the transport-agnostic half of the client protocol:
// one state machine for connect, refresh, subscribe, unsubscribe, publish,
// confirm, and ping, driven by gRPC, WebSocket, and relay sessions alike.
// Transports decode their wire format into the neutral request types and
// implement Session; everything that previously drifted between the three
// handler copies (auth gating, QoS ordering, error codes) lives here once.
package protocol

import (
	"log/slog"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/transport"
)

const (
	maxChannelNameLen = 256

	// ServerProtocolVersion is the highest protocol the server speaks.
	ServerProtocolVersion = 2
)

// Engine is the slice of the pub/sub engine the protocol ops drive.
// *engine.Engine satisfies it; tests use a recording fake.
type Engine interface {
	QoSPolicy(channel string) (enabled bool, timeout time.Duration, maxRedeliveries int)
	SubscribePriority(id engine.SubscriberID, channel string, prio int) error
	SubscribePattern(id engine.SubscriberID, pat string) error
	Unsubscribe(id engine.SubscriberID, channel string) bool
	UnsubscribePattern(id engine.SubscriberID, pat string) bool
	Recover(channel string, fromOffset, epoch uint64) ([]engine.Delivery, bool)
	CheckPublish(channel string) error
	Publish(channel string, data []byte, exclude engine.SubscriberID, deliver engine.DeliveryFunc) engine.PublishResult
}

// Consumer is the slice of qos.Consumer the ops drive. *qos.Consumer
// satisfies it; a fake lets tests pin the install-before-subscribe and
// rollback-on-error ordering directly.
type Consumer interface {
	Subscribe(channel string, w *qos.Window)
	Unsubscribe(channel string)
	Confirm(channel string, offset uint64)
	Deliver(d engine.Delivery) bool
}

// Session is the one surface a transport session exposes to the ops. The
// Send methods encode and enqueue a response in the transport's wire
// format; the adapter owns its connection-id prefix and error code enum.
type Session interface {
	State() transport.SessionState
	Engine() Engine
	QoS() Consumer
	SubscriberID() engine.SubscriberID
	Verifier() auth.Verifier
	Logger() *slog.Logger
	MaxSubscriptions() int
	MaxMessageSize() int

	// DeliverFunc is the fanout target for Engine.Publish, built once at
	// session creation so publish pays no per-call closure allocation.
	DeliverFunc() engine.DeliveryFunc

	ScheduleExpiry(exp time.Time)
	SetProtocolVersion(v uint32)

	SendConnected(reqID string, version uint32)
	SendRefreshed(reqID string, expiresAt uint64)
	SendSubscribed(reqID, channel string, recovered, didRecover bool)
	SendUnsubscribed(reqID, channel string)
	SendPublished(reqID, channel string, r engine.PublishResult)
	SendPong(reqID string)
	SendError(code ErrorCode, message, reqID string)

	// Hooks splices transport routing side effects into the shared ops.
	// nil for transports without any (grpc, ws).
	Hooks() *Hooks
}

// Hooks lets a transport splice routing side effects into the shared ops.
// A nil *Hooks (or nil field) means the op runs purely locally. Built once
// per session; the dispatch loop is single-threaded so hooks need no
// internal synchronization beyond what they touch.
type Hooks struct {
	// OnSubscribed runs after the local subscribe (and recovery read)
	// committed, before the Subscribed response is sent.
	OnSubscribed func(channel string)
	// OnUnsubscribed runs after local teardown, before the response.
	OnUnsubscribed func(channel string)
	// PublishPlan decides fanout participation; nil means local-only.
	PublishPlan func(channel string) (local, forward bool)
	// ForwardPublish ships the payload upstream when PublishPlan said so.
	ForwardPublish func(channel string, data []byte)
}
