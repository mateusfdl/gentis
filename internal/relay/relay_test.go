package relay

import (
	"context"
	"net"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func startUpstream(t *testing.T) (string, func()) {
	t.Helper()
	addr := freeAddr(t)
	srv := grpcserver.New(addr)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start upstream: %v", err)
	}
	return addr, func() { srv.Stop() }
}

func startRelay(t *testing.T, upstreamAddr string) (string, func()) {
	t.Helper()
	addr := freeAddr(t)
	r := New(
		WithListenAddr(addr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithReconnectPolicy(50*time.Millisecond, 1*time.Second, 2.0),
	)
	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	return addr, func() { r.Stop() }
}

func connectClient(t *testing.T, addr string) (gentisv1.GentisService_StreamClient, func()) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	client := gentisv1.NewGentisServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream, err := client.Stream(ctx)
	if err != nil {
		cancel()
		conn.Close()
		t.Fatalf("failed to open stream: %v", err)
	}
	return stream, func() {
		stream.CloseSend()
		cancel()
		conn.Close()
	}
}

func authenticate(t *testing.T, stream gentisv1.GentisService_StreamClient, token string) {
	t.Helper()
	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: token},
		},
	})
	if err != nil {
		t.Fatalf("failed to send connect: %v", err)
	}
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetConnected() == nil {
		t.Fatalf("expected ConnectedResponse, got %T", msg.Message)
	}
}

func recvWithTimeout(t *testing.T, stream gentisv1.GentisService_StreamClient, timeout time.Duration) *gentisv1.ServerMessage {
	t.Helper()
	type result struct {
		msg *gentisv1.ServerMessage
		err error
	}
	ch := make(chan result, 1)
	go func() {
		msg, err := stream.Recv()
		ch <- result{msg, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv error: %v", r.err)
		}
		return r.msg
	case <-time.After(timeout):
		t.Fatal("recv timed out")
		return nil
	}
}

func TestRelayStartStop(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	if relayAddr == "" {
		t.Fatal("expected non-empty relay address")
	}
}

func TestRelayConnectAndPing(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected Pong, got %T", msg.Message)
	}
}

func TestRelayAuthenticate(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "my-token")
}

func TestRelaySubscribeUnauthenticated(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Errorf("expected NOT_AUTHENTICATED, got %v", errResp.Code)
	}
}

func TestRelaySubscribe(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "relay-ch"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	sub := msg.GetSubscribed()
	if sub == nil {
		t.Fatalf("expected SubscribedResponse, got %T", msg.Message)
	}
	if sub.Channel != "relay-ch" {
		t.Errorf("expected channel 'relay-ch', got %q", sub.Channel)
	}
}

func TestRelaySubscribeAlreadySubscribed(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	recvWithTimeout(t, stream, 2*time.Second)

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetError() == nil {
		t.Fatalf("expected error for duplicate subscribe, got %T", msg.Message)
	}
	if msg.GetError().Code != gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED {
		t.Errorf("expected ALREADY_SUBSCRIBED, got %v", msg.GetError().Code)
	}
}

func TestRelayUnsubscribe(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	recvWithTimeout(t, stream, 2*time.Second)

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "ch"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	unsub := msg.GetUnsubscribed()
	if unsub == nil {
		t.Fatalf("expected UnsubscribedResponse, got %T", msg.Message)
	}
	if unsub.Channel != "ch" {
		t.Errorf("expected channel 'ch', got %q", unsub.Channel)
	}
}

func TestRelayUnsubscribeNotSubscribed(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "ch"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetError() == nil {
		t.Fatalf("expected error, got %T", msg.Message)
	}
	if msg.GetError().Code != gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED {
		t.Errorf("expected NOT_SUBSCRIBED, got %v", msg.GetError().Code)
	}
}

func TestRelaySubscribeInvalidChannel(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: ""},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetError() == nil {
		t.Fatalf("expected error, got %T", msg.Message)
	}
	if msg.GetError().Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", msg.GetError().Code)
	}
}

func TestRelayPublishInvalidChannel(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "", Data: []byte("data")},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetError() == nil {
		t.Fatalf("expected error, got %T", msg.Message)
	}
	if msg.GetError().Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", msg.GetError().Code)
	}
}

func TestRelayE2EPublishViaUpstream(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	relaySub, closeRelaySub := connectClient(t, relayAddr)
	defer closeRelaySub()
	authenticate(t, relaySub, "token")
	relaySub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "e2e-channel"},
		},
	})
	recvWithTimeout(t, relaySub, 2*time.Second)

	time.Sleep(200 * time.Millisecond)

	upstreamPub, closeUpstreamPub := connectClient(t, upstreamAddr)
	defer closeUpstreamPub()
	authenticate(t, upstreamPub, "token")
	upstreamPub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "e2e-channel"},
		},
	})
	recvWithTimeout(t, upstreamPub, 2*time.Second)

	upstreamPub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: "e2e-channel",
				Data:    []byte("upstream-msg"),
			},
		},
	})

	msg := recvWithTimeout(t, relaySub, 3*time.Second)
	chMsg := msg.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("relay subscriber: expected ChannelMessage, got %T", msg.Message)
	}
	if chMsg.Channel != "e2e-channel" {
		t.Errorf("expected channel 'e2e-channel', got %q", chMsg.Channel)
	}
	if string(chMsg.Data) != "upstream-msg" {
		t.Errorf("expected data 'upstream-msg', got %q", string(chMsg.Data))
	}
}

func TestRelayLocalPublishBetweenRelayClients(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
	)

	r.router = NewRouter([]ChannelPattern{
		{Pattern: "local-*", Mode: RouteModeLocal},
	})

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	time.Sleep(100 * time.Millisecond)

	sub, closeSub := connectClient(t, relayAddr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "local-chat"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, relayAddr)
	defer closePub()
	authenticate(t, pub, "token")
	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "local-chat"},
		},
	})
	recvWithTimeout(t, pub, 2*time.Second)

	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: "local-chat",
				Data:    []byte("local-msg"),
			},
		},
	})

	msg := recvWithTimeout(t, sub, 2*time.Second)
	chMsg := msg.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("expected ChannelMessage, got %T", msg.Message)
	}
	if string(chMsg.Data) != "local-msg" {
		t.Errorf("expected 'local-msg', got %q", string(chMsg.Data))
	}
}

func TestRelayConnectionCount(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
	)

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	time.Sleep(100 * time.Millisecond)

	if r.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections, got %d", r.ConnectionCount())
	}

	c1, close1 := connectClient(t, relayAddr)
	defer close1()
	authenticate(t, c1, "token")

	c1.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	recvWithTimeout(t, c1, 2*time.Second)

	if r.ConnectionCount() != 1 {
		t.Errorf("expected 1 connection, got %d", r.ConnectionCount())
	}
}

func TestUpstreamSubscriptionRefCounting(t *testing.T) {
	handler := func(channel string, data []byte) {}
	u := NewUpstream(
		UpstreamConfig{Address: "127.0.0.1:0"},
		ReconnectPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 1 * time.Second, Multiplier: 2.0},
		handler,
		gentislog.Nop(),
	)

	u.Subscribe("ch1")
	u.Subscribe("ch1")
	u.Subscribe("ch1")

	val, ok := u.subscriptions.Load("ch1")
	if !ok {
		t.Fatal("expected subscription entry for ch1")
	}
	ref := val.(*subscriptionRef)
	if ref.count != 3 {
		t.Errorf("expected ref count 3, got %d", ref.count)
	}

	u.Unsubscribe("ch1")
	if ref.count != 2 {
		t.Errorf("expected ref count 2 after one unsubscribe, got %d", ref.count)
	}

	u.Unsubscribe("ch1")
	u.Unsubscribe("ch1")

	_, ok = u.subscriptions.Load("ch1")
	if ok {
		t.Error("expected subscription entry to be removed after all unsubscribes")
	}
}

func TestUpstreamUnsubscribeNonexistent(t *testing.T) {
	handler := func(channel string, data []byte) {}
	u := NewUpstream(
		UpstreamConfig{Address: "127.0.0.1:0"},
		ReconnectPolicy{},
		handler,
		gentislog.Nop(),
	)

	err := u.Unsubscribe("nonexistent")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUpstreamPublishNotConnected(t *testing.T) {
	handler := func(channel string, data []byte) {}
	u := NewUpstream(
		UpstreamConfig{Address: "127.0.0.1:0"},
		ReconnectPolicy{},
		handler,
		gentislog.Nop(),
	)

	err := u.Publish("ch", []byte("data"))
	if err == nil {
		t.Error("expected error when publishing while not connected")
	}
}

func TestUpstreamIsConnected(t *testing.T) {
	handler := func(channel string, data []byte) {}
	u := NewUpstream(
		UpstreamConfig{Address: "127.0.0.1:0"},
		ReconnectPolicy{},
		handler,
		gentislog.Nop(),
	)

	if u.IsConnected() {
		t.Error("expected not connected initially")
	}
}

func TestDedupIntegration(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)
	defer d.Stop()

	if !d.Check("ch1", []byte("msg1")) {
		t.Error("first message should pass")
	}
	if !d.Check("ch1", []byte("msg2")) {
		t.Error("different data should pass")
	}
	if !d.Check("ch2", []byte("msg1")) {
		t.Error("different channel should pass")
	}

	if d.Check("ch1", []byte("msg1")) {
		t.Error("duplicate should be blocked")
	}
}

func TestRouterIntegration(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "local-*", Mode: RouteModeLocal},
		{Pattern: "both-*", Mode: RouteModeBoth},
	})

	result := r.Route("local-chat")
	if result.Mode != RouteModeLocal {
		t.Errorf("expected RouteModeLocal, got %d", result.Mode)
	}

	result = r.Route("both-chat")
	if result.Mode != RouteModeBoth {
		t.Errorf("expected RouteModeBoth, got %d", result.Mode)
	}

	result = r.Route("other")
	if result.Mode != RouteModeRelay {
		t.Errorf("expected RouteModeRelay, got %d", result.Mode)
	}
}

func TestRelayWithMetrics(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr := freeAddr(t)
	metricsAddr := freeAddr(t)

	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "token"),
		WithMetrics(metricsAddr),
	)

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay with metrics: %v", err)
	}
	defer r.Stop()
}

func TestRelayMaxRetries(t *testing.T) {
	r := New(
		WithListenAddr("127.0.0.1:0"),
		WithUpstream("127.0.0.1:0", "token"),
		WithMaxRetries(3),
	)

	if r.config.ReconnectPolicy.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", r.config.ReconnectPolicy.MaxRetries)
	}
}

func TestRelayLocalPublishAck(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
	)

	r.router = NewRouter([]ChannelPattern{
		{Pattern: "local-*", Mode: RouteModeLocal},
	})

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	time.Sleep(100 * time.Millisecond)

	sub, closeSub := connectClient(t, relayAddr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "local-acked"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, relayAddr)
	defer closePub()
	authenticate(t, pub, "token")

	pub.Send(&gentisv1.ClientMessage{
		Id: "rel-1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "local-acked", Data: []byte("x")},
		},
	})

	msg := recvWithTimeout(t, pub, 2*time.Second)
	if msg.Id != "rel-1" {
		t.Errorf("expected correlation id 'rel-1', got %q", msg.Id)
	}
	ack := msg.GetPublished()
	if ack == nil {
		t.Fatalf("expected PublishResponse, got %T", msg.Message)
	}
	if ack.Channel != "local-acked" {
		t.Errorf("expected channel 'local-acked', got %q", ack.Channel)
	}
	if ack.Offset != 1 {
		t.Errorf("expected offset 1, got %d", ack.Offset)
	}
	if ack.Epoch == 0 {
		t.Error("expected non-zero epoch")
	}
	if ack.Delivered != 1 {
		t.Errorf("expected delivered 1, got %d", ack.Delivered)
	}
	if ack.Dropped != 0 {
		t.Errorf("expected dropped 0, got %d", ack.Dropped)
	}
}
