package protocol

import (
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/client"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/qos"
	"github.com/mateusfdl/gentis/internal/transport"
)

type recorder struct {
	events []string
}

func (r *recorder) add(format string, args ...any) {
	r.events = append(r.events, fmt.Sprintf(format, args...))
}

type fakeEngine struct {
	rec           *recorder
	subscribeErr  error
	patternErr    error
	checkErr      error
	unsubscribeOK bool
	recoverItems  []engine.Delivery
	recoverOK     bool
	publishResult engine.PublishResult
	qosEnabled    bool
}

func (f *fakeEngine) QoSPolicy(channel string) (bool, time.Duration, int) {
	return f.qosEnabled, time.Second, 3
}

func (f *fakeEngine) SubscribePriority(id engine.SubscriberID, channel string, prio int) error {
	f.rec.add("engine.subscribe %s prio=%d", channel, prio)
	return f.subscribeErr
}

func (f *fakeEngine) SubscribePattern(id engine.SubscriberID, pat string) error {
	f.rec.add("engine.subscribePattern %s", pat)
	return f.patternErr
}

func (f *fakeEngine) Unsubscribe(id engine.SubscriberID, channel string) bool {
	f.rec.add("engine.unsubscribe %s", channel)
	return f.unsubscribeOK
}

func (f *fakeEngine) UnsubscribePattern(id engine.SubscriberID, pat string) bool {
	f.rec.add("engine.unsubscribePattern %s", pat)
	return f.unsubscribeOK
}

func (f *fakeEngine) Recover(channel string, fromOffset, epoch uint64) ([]engine.Delivery, bool) {
	f.rec.add("engine.recover %s from=%d epoch=%d", channel, fromOffset, epoch)
	return f.recoverItems, f.recoverOK
}

func (f *fakeEngine) CheckPublish(channel string) error {
	return f.checkErr
}

func (f *fakeEngine) Publish(channel string, data []byte, exclude engine.SubscriberID, deliver engine.DeliveryFunc) engine.PublishResult {
	f.rec.add("engine.publish %s %s", channel, data)
	return f.publishResult
}

type fakeConsumer struct {
	rec *recorder
}

func (f *fakeConsumer) Subscribe(channel string, w *qos.Window) {
	f.rec.add("qos.subscribe %s", channel)
}

func (f *fakeConsumer) Unsubscribe(channel string) {
	f.rec.add("qos.unsubscribe %s", channel)
}

func (f *fakeConsumer) Confirm(channel string, offset uint64) {
	f.rec.add("qos.confirm %s offset=%d", channel, offset)
}

func (f *fakeConsumer) Deliver(d engine.Delivery) bool {
	f.rec.add("qos.deliver %s offset=%d", d.Channel, d.Offset)
	return true
}

type stubVerifier struct {
	claims map[string]auth.Claims
}

func (v stubVerifier) Verify(token string) (auth.Claims, error) {
	c, ok := v.claims[token]
	if !ok {
		return auth.Claims{}, auth.ErrBadSignature
	}
	return c, nil
}

type fakeSession struct {
	rec      *recorder
	state    transport.SessionState
	eng      *fakeEngine
	consumer *fakeConsumer
	verifier auth.Verifier
	hooks    *Hooks
	maxSubs  int
	maxMsg   int
	version  uint32
}

func (s *fakeSession) State() transport.SessionState     { return s.state }
func (s *fakeSession) Engine() Engine                    { return s.eng }
func (s *fakeSession) QoS() Consumer                     { return s.consumer }
func (s *fakeSession) SubscriberID() engine.SubscriberID { return 1 }
func (s *fakeSession) Verifier() auth.Verifier           { return s.verifier }
func (s *fakeSession) Logger() *slog.Logger              { return gentislog.Nop() }
func (s *fakeSession) MaxSubscriptions() int             { return s.maxSubs }
func (s *fakeSession) MaxMessageSize() int               { return s.maxMsg }
func (s *fakeSession) Hooks() *Hooks                     { return s.hooks }

func (s *fakeSession) DeliverFunc() engine.DeliveryFunc {
	return func(id engine.SubscriberID, d engine.Delivery) bool { return true }
}

func (s *fakeSession) ScheduleExpiry(exp time.Time) {
	s.rec.add("session.scheduleExpiry zero=%v", exp.IsZero())
}

func (s *fakeSession) SetProtocolVersion(v uint32) {
	s.version = v
}

func (s *fakeSession) SendConnected(reqID string, version uint32) {
	s.rec.add("send.connected v=%d req=%s", version, reqID)
}

func (s *fakeSession) SendRefreshed(reqID string, expiresAt uint64) {
	s.rec.add("send.refreshed exp=%d", expiresAt)
}

func (s *fakeSession) SendSubscribed(reqID, channel string, recovered, didRecover bool) {
	s.rec.add("send.subscribed %s recovered=%v didRecover=%v", channel, recovered, didRecover)
}

func (s *fakeSession) SendUnsubscribed(reqID, channel string) {
	s.rec.add("send.unsubscribed %s", channel)
}

func (s *fakeSession) SendPublished(reqID, channel string, r engine.PublishResult) {
	s.rec.add("send.published %s offset=%d delivered=%d", channel, r.Offset, r.Delivered)
}

func (s *fakeSession) SendPong(reqID string) {
	s.rec.add("send.pong req=%s", reqID)
}

func (s *fakeSession) SendError(code ErrorCode, message, reqID string) {
	s.rec.add("send.error code=%d msg=%s", code, message)
}

func newFakeSession() *fakeSession {
	rec := &recorder{}
	return &fakeSession{
		rec:      rec,
		state:    client.NewState(1),
		eng:      &fakeEngine{rec: rec, unsubscribeOK: true, qosEnabled: true},
		consumer: &fakeConsumer{rec: rec},
		verifier: stubVerifier{claims: map[string]auth.Claims{
			"token-a": {Subject: "alice"},
			"token-b": {Subject: "bob"},
		}},
		maxSubs: 16,
		maxMsg:  1024,
	}
}

func authenticate(t *testing.T, s *fakeSession) {
	t.Helper()
	Connect(s, ConnectRequest{AuthToken: "token-a", ProtocolVersion: 2}, "c1")
	if !s.state.IsAuthenticated() {
		t.Fatal("Connect with valid token must authenticate")
	}
	s.rec.events = nil
}

func lastEvent(t *testing.T, s *fakeSession) string {
	t.Helper()
	if len(s.rec.events) == 0 {
		t.Fatal("no events recorded")
	}
	return s.rec.events[len(s.rec.events)-1]
}

func TestAuthGateMatrix(t *testing.T) {
	gated := map[string]func(s *fakeSession){
		"refresh":     func(s *fakeSession) { Refresh(s, RefreshRequest{AuthToken: "token-a"}, "r") },
		"confirm":     func(s *fakeSession) { Confirm(s, "ch", 1, "r") },
		"subscribe":   func(s *fakeSession) { Subscribe(s, SubscribeRequest{Channel: "ch"}, "r") },
		"unsubscribe": func(s *fakeSession) { Unsubscribe(s, "ch", "r") },
		"publish":     func(s *fakeSession) { Publish(s, PublishRequest{Channel: "ch", Data: []byte("x")}, "r") },
		"unknown":     func(s *fakeSession) { Unknown(s, "r") },
	}
	for name, op := range gated {
		t.Run(name, func(t *testing.T) {
			s := newFakeSession()
			op(s)
			want := fmt.Sprintf("send.error code=%d msg=not authenticated", CodeNotAuthenticated)
			if got := lastEvent(t, s); got != want {
				t.Errorf("unauthenticated %s event = %q, want %q", name, got, want)
			}
		})
	}

	t.Run("ping passes unauthenticated", func(t *testing.T) {
		s := newFakeSession()
		Ping(s, "p1")
		if got := lastEvent(t, s); got != "send.pong req=p1" {
			t.Errorf("event = %q, want pong", got)
		}
	})
}

func TestConnectNegotiatesVersion(t *testing.T) {
	tests := []struct {
		requested uint32
		want      uint32
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{99, 2},
	}
	for _, tt := range tests {
		s := newFakeSession()
		Connect(s, ConnectRequest{AuthToken: "token-a", ProtocolVersion: tt.requested}, "c")
		if s.version != tt.want {
			t.Errorf("requested %d: version = %d, want %d", tt.requested, s.version, tt.want)
		}
		want := fmt.Sprintf("send.connected v=%d req=c", tt.want)
		if got := lastEvent(t, s); got != want {
			t.Errorf("requested %d: event = %q, want %q", tt.requested, got, want)
		}
	}
}

func TestConnectRejectsBadToken(t *testing.T) {
	s := newFakeSession()
	Connect(s, ConnectRequest{AuthToken: "nope"}, "c")
	want := fmt.Sprintf("send.error code=%d msg=authentication failed", CodeNotAuthenticated)
	if got := lastEvent(t, s); got != want {
		t.Errorf("event = %q, want %q", got, want)
	}
	if s.state.IsAuthenticated() {
		t.Fatal("bad token must not authenticate")
	}
}

func TestConnectSubjectGuard(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Connect(s, ConnectRequest{AuthToken: "token-a"}, "c2")
	if got := lastEvent(t, s); got != "send.connected v=1 req=c2" {
		t.Errorf("same-subject reconnect event = %q, want connected", got)
	}

	Connect(s, ConnectRequest{AuthToken: "token-b"}, "c3")
	want := fmt.Sprintf("send.error code=%d msg=connect subject mismatch", CodeNotAuthenticated)
	if got := lastEvent(t, s); got != want {
		t.Errorf("subject swap event = %q, want %q", got, want)
	}
}

func TestRefreshSubjectGuard(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Refresh(s, RefreshRequest{AuthToken: "token-b"}, "r1")
	want := fmt.Sprintf("send.error code=%d msg=refresh subject mismatch", CodeNotAuthenticated)
	if got := lastEvent(t, s); got != want {
		t.Errorf("event = %q, want %q", got, want)
	}

	Refresh(s, RefreshRequest{AuthToken: "token-a"}, "r2")
	if got := lastEvent(t, s); got != "send.refreshed exp=0" {
		t.Errorf("event = %q, want refreshed", got)
	}
}

func TestSubscribeInstallsWindowBeforeEngineSubscribe(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Subscribe(s, SubscribeRequest{
		Channel:    "jobs:q",
		HasWindow:  true,
		Window:     Window{Count: 4},
		HasRecover: true,
		Recover:    RecoverPoint{Offset: 7, Epoch: 9},
	}, "s1")

	want := []string{
		"qos.subscribe jobs:q",
		"engine.subscribe jobs:q prio=0",
		"engine.recover jobs:q from=7 epoch=9",
		"send.subscribed jobs:q recovered=false didRecover=true",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}
}

func TestSubscribeRollsBackWindowOnEngineError(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	s.eng.subscribeErr = engine.ErrChannelFull

	Subscribe(s, SubscribeRequest{Channel: "jobs:q", HasWindow: true, Window: Window{Count: 4}}, "s1")

	want := []string{
		"qos.subscribe jobs:q",
		"engine.subscribe jobs:q prio=0",
		"qos.unsubscribe jobs:q",
		fmt.Sprintf("send.error code=%d msg=%s", CodeSubscriptionLimit, engine.ErrChannelFull.Error()),
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}
	if s.state.SubscriptionCount() != 0 {
		t.Fatal("failed subscribe must not register in session state")
	}
}

func TestSubscribeQoSDeniedOutsideNamespace(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	s.eng.qosEnabled = false

	Subscribe(s, SubscribeRequest{Channel: "ch", HasWindow: true, Window: Window{Count: 4}}, "s1")

	want := fmt.Sprintf("send.error code=%d msg=namespace does not offer at-least-once delivery", CodePermissionDenied)
	if got := lastEvent(t, s); got != want {
		t.Errorf("event = %q, want %q", got, want)
	}
}

func TestSubscribeReplayDeliveredAfterResponse(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	s.eng.recoverOK = true
	s.eng.recoverItems = []engine.Delivery{
		{Channel: "ch", Offset: 8},
		{Channel: "ch", Offset: 9},
	}

	Subscribe(s, SubscribeRequest{Channel: "ch", HasRecover: true, Recover: RecoverPoint{Offset: 7, Epoch: 1}}, "s1")

	want := []string{
		"engine.subscribe ch prio=0",
		"engine.recover ch from=7 epoch=1",
		"send.subscribed ch recovered=true didRecover=true",
		"qos.deliver ch offset=8",
		"qos.deliver ch offset=9",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}
}

func TestSubscribePatternRejectsQoSAndRecovery(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Subscribe(s, SubscribeRequest{Channel: "metrics:*", HasWindow: true, Window: Window{Count: 4}}, "s1")
	want := fmt.Sprintf("send.error code=%d msg=wildcard subscriptions do not support qos or recovery", CodeInvalidPayload)
	if got := lastEvent(t, s); got != want {
		t.Errorf("window event = %q, want %q", got, want)
	}

	Subscribe(s, SubscribeRequest{Channel: "metrics:*", HasRecover: true, Recover: RecoverPoint{Offset: 1}}, "s2")
	if got := lastEvent(t, s); got != want {
		t.Errorf("recover event = %q, want %q", got, want)
	}

	for _, ev := range s.rec.events {
		if ev == "qos.subscribe metrics:*" {
			t.Fatal("pattern subscribe must never touch the qos consumer")
		}
	}
}

func TestSubscribePatternRoutesToEnginePattern(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Subscribe(s, SubscribeRequest{Channel: "metrics:*"}, "s1")

	want := []string{
		"engine.subscribePattern metrics:*",
		"send.subscribed metrics:* recovered=false didRecover=false",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}
}

func TestSubscribeChecksOrder(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Subscribe(s, SubscribeRequest{Channel: "a?b"}, "s1")
	want := fmt.Sprintf("send.error code=%d msg=invalid channel name", CodeInvalidPayload)
	if got := lastEvent(t, s); got != want {
		t.Errorf("reserved char event = %q, want %q", got, want)
	}

	s.state.Authenticate(auth.Claims{Subject: "alice", Channels: []string{"allowed"}})
	Subscribe(s, SubscribeRequest{Channel: "denied"}, "s2")
	want = fmt.Sprintf("send.error code=%d msg=subscribe not allowed on channel", CodePermissionDenied)
	if got := lastEvent(t, s); got != want {
		t.Errorf("permission event = %q, want %q", got, want)
	}

	s.maxSubs = 1
	s.state.AddSubscription("allowed")
	Subscribe(s, SubscribeRequest{Channel: "allowed"}, "s3")
	want = fmt.Sprintf("send.error code=%d msg=subscription limit reached", CodeSubscriptionLimit)
	if got := lastEvent(t, s); got != want {
		t.Errorf("limit event = %q, want %q", got, want)
	}
}

func TestUnsubscribe(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	s.state.AddSubscription("ch")

	Unsubscribe(s, "ch", "u1")
	want := []string{
		"engine.unsubscribe ch",
		"qos.unsubscribe ch",
		"send.unsubscribed ch",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}

	s.rec.events = nil
	s.eng.unsubscribeOK = false
	Unsubscribe(s, "ch", "u2")
	wantErr := fmt.Sprintf("send.error code=%d msg=not subscribed to channel", CodeNotSubscribed)
	if got := lastEvent(t, s); got != wantErr {
		t.Errorf("event = %q, want %q", got, wantErr)
	}
}

func TestUnsubscribePattern(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Unsubscribe(s, "metrics:*", "u1")
	if s.rec.events[0] != "engine.unsubscribePattern metrics:*" {
		t.Errorf("first event = %q, want engine.unsubscribePattern", s.rec.events[0])
	}
}

func TestPublish(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	s.eng.publishResult = engine.PublishResult{Offset: 3, Delivered: 2}

	Publish(s, PublishRequest{Channel: "ch", Data: []byte("x")}, "p1")
	want := []string{
		"engine.publish ch x",
		"send.published ch offset=3 delivered=2",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}
}

func TestPublishFireAndForgetSkipsAck(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Publish(s, PublishRequest{Channel: "ch", Data: []byte("x")}, "")
	want := []string{"engine.publish ch x"}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("events = %v, want %v", s.rec.events, want)
	}
}

func TestPublishValidation(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Publish(s, PublishRequest{Channel: "jobs:*", Data: []byte("x")}, "p1")
	want := fmt.Sprintf("send.error code=%d msg=invalid channel name", CodeInvalidPayload)
	if got := lastEvent(t, s); got != want {
		t.Errorf("pattern target event = %q, want %q", got, want)
	}

	Publish(s, PublishRequest{Channel: "ch", Data: make([]byte, 2048)}, "p2")
	want = fmt.Sprintf("send.error code=%d msg=message exceeds max size", CodeMessageTooLarge)
	if got := lastEvent(t, s); got != want {
		t.Errorf("oversize event = %q, want %q", got, want)
	}

	s.eng.checkErr = engine.ErrPublishDenied
	Publish(s, PublishRequest{Channel: "ch", Data: []byte("x")}, "p3")
	want = fmt.Sprintf("send.error code=%d msg=%s", CodePermissionDenied, engine.ErrPublishDenied.Error())
	if got := lastEvent(t, s); got != want {
		t.Errorf("check event = %q, want %q", got, want)
	}
}

func TestPublishPlanCombinations(t *testing.T) {
	tests := []struct {
		name    string
		local   bool
		forward bool
		want    []string
	}{
		{"local only", true, false, []string{"engine.publish ch x", "send.published ch offset=3 delivered=2"}},
		{"forward only", false, true, []string{"hook.forward ch x", "send.published ch offset=0 delivered=0"}},
		{"both", true, true, []string{"engine.publish ch x", "hook.forward ch x", "send.published ch offset=3 delivered=2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newFakeSession()
			authenticate(t, s)
			s.eng.publishResult = engine.PublishResult{Offset: 3, Delivered: 2}
			s.hooks = &Hooks{
				PublishPlan: func(channel string) (bool, bool) { return tt.local, tt.forward },
				ForwardPublish: func(channel string, data []byte) {
					s.rec.add("hook.forward %s %s", channel, data)
				},
			}

			Publish(s, PublishRequest{Channel: "ch", Data: []byte("x")}, "p1")
			if !slices.Equal(s.rec.events, tt.want) {
				t.Errorf("events = %v, want %v", s.rec.events, tt.want)
			}
		})
	}
}

func TestSubscribeHooksFireBeforeResponse(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	s.hooks = &Hooks{
		OnSubscribed:   func(channel string) { s.rec.add("hook.subscribed %s", channel) },
		OnUnsubscribed: func(channel string) { s.rec.add("hook.unsubscribed %s", channel) },
	}

	Subscribe(s, SubscribeRequest{Channel: "ch"}, "s1")
	want := []string{
		"engine.subscribe ch prio=0",
		"hook.subscribed ch",
		"send.subscribed ch recovered=false didRecover=false",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("subscribe events = %v, want %v", s.rec.events, want)
	}

	s.rec.events = nil
	Subscribe(s, SubscribeRequest{Channel: "metrics:*"}, "s2")
	want = []string{
		"engine.subscribePattern metrics:*",
		"hook.subscribed metrics:*",
		"send.subscribed metrics:* recovered=false didRecover=false",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("pattern events = %v, want %v", s.rec.events, want)
	}

	s.rec.events = nil
	Unsubscribe(s, "ch", "u1")
	want = []string{
		"engine.unsubscribe ch",
		"qos.unsubscribe ch",
		"hook.unsubscribed ch",
		"send.unsubscribed ch",
	}
	if !slices.Equal(s.rec.events, want) {
		t.Errorf("unsubscribe events = %v, want %v", s.rec.events, want)
	}
}

func TestConfirm(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)

	Confirm(s, "ch", 42, "")
	if got := lastEvent(t, s); got != "qos.confirm ch offset=42" {
		t.Errorf("event = %q, want confirm", got)
	}
}

func TestUnknown(t *testing.T) {
	s := newFakeSession()
	authenticate(t, s)
	Unknown(s, "x")
	want := fmt.Sprintf("send.error code=%d msg=unknown message type", CodeUnknownMessage)
	if got := lastEvent(t, s); got != want {
		t.Errorf("event = %q, want %q", got, want)
	}
}

func TestSubscribeErrorCodeTable(t *testing.T) {
	tests := []struct {
		err  error
		want ErrorCode
	}{
		{engine.ErrAlreadySubscribed, CodeAlreadySubscribed},
		{engine.ErrUnknownNamespace, CodeChannelNotFound},
		{engine.ErrChannelFull, CodeSubscriptionLimit},
		{engine.ErrWildcardDenied, CodePermissionDenied},
		{fmt.Errorf("boom"), CodeInternal},
	}
	for _, tt := range tests {
		if got := SubscribeErrorCode(tt.err); got != tt.want {
			t.Errorf("SubscribeErrorCode(%v) = %d, want %d", tt.err, got, tt.want)
		}
	}
}

func TestPublishErrorCodeTable(t *testing.T) {
	tests := []struct {
		err  error
		want ErrorCode
	}{
		{engine.ErrUnknownNamespace, CodeChannelNotFound},
		{engine.ErrPublishDenied, CodePermissionDenied},
		{fmt.Errorf("boom"), CodeInternal},
	}
	for _, tt := range tests {
		if got := PublishErrorCode(tt.err); got != tt.want {
			t.Errorf("PublishErrorCode(%v) = %d, want %d", tt.err, got, tt.want)
		}
	}
}
