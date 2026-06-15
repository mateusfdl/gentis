package relay

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	grpcserver "github.com/mateusfdl/gentis/internal/grpc"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
	"github.com/mateusfdl/gentis/internal/ringbuf"
	"github.com/mateusfdl/gentis/internal/testcert"
	"github.com/mateusfdl/gentis/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

func waitUpstreamConnected(t *testing.T, r *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.IsUpstreamConnected() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("relay never connected to upstream")
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
	waitUpstreamConnected(t, r)
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

	waitUpstreamConnected(t, r)

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

	waitUpstreamConnected(t, r)

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
	handler := func(channel string, data []byte, offset, epoch uint64) {}
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
	handler := func(channel string, data []byte, offset, epoch uint64) {}
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
	handler := func(channel string, data []byte, offset, epoch uint64) {}
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
	handler := func(channel string, data []byte, offset, epoch uint64) {}
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

	if !d.Check("ch1", 7, 1) {
		t.Error("first message should pass")
	}
	if !d.Check("ch1", 7, 2) {
		t.Error("different offset should pass")
	}
	if !d.Check("ch2", 7, 1) {
		t.Error("different channel should pass")
	}

	if d.Check("ch1", 7, 1) {
		t.Error("duplicate identity should be blocked")
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

	waitUpstreamConnected(t, r)

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

func TestRelayUnauthenticatedSessionClosedAtAuthDeadline(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithAuthDeadline(100*time.Millisecond),
	)
	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	client, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	client.Send(&gentisv1.ClientMessage{
		Id:      "p1",
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg := recvWithTimeout(t, client, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected PongResponse, got %T", msg.Message)
	}

	errCh := make(chan error, 1)
	go func() {
		for {
			if _, err := client.Recv(); err != nil {
				errCh <- err
				return
			}
		}
	}()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("unauthenticated session still alive past the auth deadline")
	}
}

func TestRelayAuthenticatedSessionSurvivesAuthDeadline(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithAuthDeadline(100*time.Millisecond),
	)
	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	client, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, client, "token")

	time.Sleep(300 * time.Millisecond)

	client.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg := recvWithTimeout(t, client, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("expected SubscribedResponse, got %T", msg.Message)
	}
}

func TestRelayPublishToPatternRejected(t *testing.T) {
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

	client, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, client, "token")

	client.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "jobs:*", Data: []byte("x")},
		},
	})

	msg := recvWithTimeout(t, client, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestRelaySubscribeReservedCharRejected(t *testing.T) {
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

	client, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, client, "token")

	client.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "jobs:cpu?"},
		},
	})

	msg := recvWithTimeout(t, client, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestRelayPermissionChecks(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	secret := []byte("relay-secret")
	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithVerifier(auth.NewHMACVerifier(secret)),
	)

	r.router = NewRouter([]ChannelPattern{
		{Pattern: "local-*", Mode: RouteModeLocal},
	})

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	waitUpstreamConnected(t, r)

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
		Channels:  []string{"local-allowed"},
		Pub:       []string{},
	})
	authenticate(t, stream, token)

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "local-forbidden"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("subscribe outside allowlist: got %v, want PERMISSION_DENIED", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "p1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "local-allowed", Data: []byte("x")},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	errResp = msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("publish with empty pub allowlist: got %v, want PERMISSION_DENIED", msg.Message)
	}
}

func TestUpstreamTLS(t *testing.T) {
	certFile, keyFile := testcert.Generate(t)
	upstreamAddr := freeAddr(t)
	upstream := grpcserver.New(upstreamAddr, grpcserver.WithTLS(certFile, keyFile))
	if err := upstream.Start(); err != nil {
		t.Fatalf("start upstream: %v", err)
	}
	defer upstream.Stop()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithUpstreamTLS(certFile),
		WithBufferSize(256),
		WithReconnectPolicy(50*time.Millisecond, 1*time.Second, 2.0),
	)
	if err := r.Start(); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer r.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.IsUpstreamConnected() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("relay never connected to TLS upstream")
}

func TestUpstreamTLSWithoutCAFailsClosed(t *testing.T) {
	certFile, keyFile := testcert.Generate(t)
	upstreamAddr := freeAddr(t)
	upstream := grpcserver.New(upstreamAddr, grpcserver.WithTLS(certFile, keyFile))
	if err := upstream.Start(); err != nil {
		t.Fatalf("start upstream: %v", err)
	}
	defer upstream.Stop()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithReconnectPolicy(50*time.Millisecond, 200*time.Millisecond, 2.0),
		WithMaxRetries(3),
	)
	err := r.Start()
	if err == nil {
		defer r.Stop()
		time.Sleep(500 * time.Millisecond)
		if r.IsUpstreamConnected() {
			t.Fatal("plaintext relay should not connect to TLS upstream")
		}
	}
}

func TestUpstreamReconnectRecoversGap(t *testing.T) {
	upstreamAddr := freeAddr(t)
	upstreamEng := engine.New(engine.WithHistory(64, 0))
	defer upstreamEng.Stop()
	upstreamStore := transport.NewSessionStore()

	upstream := grpcserver.New(upstreamAddr,
		grpcserver.WithEngine(upstreamEng),
		grpcserver.WithSessionStore(upstreamStore),
	)
	if err := upstream.Start(); err != nil {
		t.Fatalf("start upstream: %v", err)
	}

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithReconnectPolicy(50*time.Millisecond, 200*time.Millisecond, 2.0),
		WithFanoutWorkers(1),
	)
	r.router = NewRouter([]ChannelPattern{
		{Pattern: "up-*", Mode: RouteModeRelay},
	})
	if err := r.Start(); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer r.Stop()

	client, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, client, "token")
	client.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "up-rec"},
		},
	})
	recvWithTimeout(t, client, 2*time.Second)
	time.Sleep(200 * time.Millisecond)

	upstreamEng.Publish("up-rec", []byte("m-1"), 0, upstreamStore.Deliver)

	msg := recvWithTimeout(t, client, 3*time.Second)
	cm := msg.GetChannelMessage()
	if cm == nil || string(cm.Data) != "m-1" {
		t.Fatalf("expected live m-1 through relay, got %v", msg.Message)
	}

	upstream.Stop()
	upstreamEng.Publish("up-rec", []byte("m-2"), 0, upstreamStore.Deliver)
	upstreamEng.Publish("up-rec", []byte("m-3"), 0, upstreamStore.Deliver)

	upstream2 := grpcserver.New(upstreamAddr,
		grpcserver.WithEngine(upstreamEng),
		grpcserver.WithSessionStore(upstreamStore),
	)
	if err := upstream2.Start(); err != nil {
		t.Fatalf("restart upstream: %v", err)
	}
	defer upstream2.Stop()

	for _, want := range []string{"m-2", "m-3"} {
		msg := recvWithTimeout(t, client, 5*time.Second)
		cm := msg.GetChannelMessage()
		if cm == nil || string(cm.Data) != want {
			t.Fatalf("expected recovered %q through relay, got %v", want, msg.Message)
		}
	}
}

func TestRelayWildcardSubscribeViaUpstream(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	relaySub, closeRelaySub := connectClient(t, relayAddr)
	defer closeRelaySub()
	authenticate(t, relaySub, "token")
	relaySub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "metrics:*"},
		},
	})
	subResp := recvWithTimeout(t, relaySub, 2*time.Second).GetSubscribed()
	if subResp == nil {
		t.Fatal("expected SubscribedResponse for pattern subscribe")
	}
	if subResp.Channel != "metrics:*" {
		t.Fatalf("subscribed channel = %q, want metrics:*", subResp.Channel)
	}

	time.Sleep(200 * time.Millisecond)

	upstreamPub, closeUpstreamPub := connectClient(t, upstreamAddr)
	defer closeUpstreamPub()
	authenticate(t, upstreamPub, "token")
	upstreamPub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: "metrics:cpu",
				Data:    []byte("wild"),
			},
		},
	})

	msg := recvWithTimeout(t, relaySub, 3*time.Second)
	chMsg := msg.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("relay subscriber: expected ChannelMessage, got %T", msg.Message)
	}
	if chMsg.Channel != "metrics:cpu" {
		t.Errorf("expected channel metrics:cpu, got %q", chMsg.Channel)
	}
	if string(chMsg.Data) != "wild" {
		t.Errorf("expected data 'wild', got %q", string(chMsg.Data))
	}
}

func TestRelayWildcardUnsubscribe(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, stream, "token")
	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "metrics:*"},
		},
	})
	recvWithTimeout(t, stream, 2*time.Second)

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "metrics:*"},
		},
	})
	unsub := recvWithTimeout(t, stream, 2*time.Second).GetUnsubscribed()
	if unsub == nil {
		t.Fatal("expected UnsubscribedResponse")
	}
	if unsub.Channel != "metrics:*" {
		t.Fatalf("unsubscribed channel = %q, want metrics:*", unsub.Channel)
	}
}

type failSendStream struct {
	gentisv1.GentisService_StreamServer
}

func (f *failSendStream) Send(*gentisv1.ServerMessage) error {
	return errors.New("broken pipe")
}

func TestSendDoesNotBlockAfterSenderDies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](sendRingCap(256))
	if err != nil {
		t.Fatalf("ring alloc: %v", err)
	}
	sess := &Session{
		sendRing:   ring,
		wakeCh:     make(chan struct{}, 1),
		drainCh:    make(chan struct{}, 1),
		senderDone: make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
	sess.send(&gentisv1.ServerMessage{Id: "1"})

	go sess.runSender(&failSendStream{})
	<-sess.senderDone

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range sendRingCap(256) + 8 {
			sess.send(&gentisv1.ServerMessage{Id: fmt.Sprintf("m-%d", i)})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("send() blocked after sender death; dispatch loop would wedge")
	}
}

func TestConnectRejectsSubjectChange(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	secret := []byte("relay-secret")
	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithVerifier(auth.NewHMACVerifier(secret)),
	)
	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer r.Stop()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()

	authenticate(t, stream, auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	stream.Send(&gentisv1.ClientMessage{
		Id: "c2",
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: auth.SignHS256(secret, auth.Claims{
				Subject:   "user-2",
				ExpiresAt: time.Now().Add(time.Hour),
			})},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Fatalf("expected NOT_AUTHENTICATED on connect subject change, got %v", msg.Message)
	}
}

func TestUpstreamRecoveryDeliversRepeatedPayloads(t *testing.T) {
	upstreamAddr := freeAddr(t)
	upstreamEng := engine.New(engine.WithHistory(64, 0))
	defer upstreamEng.Stop()
	upstreamStore := transport.NewSessionStore()

	upstream := grpcserver.New(upstreamAddr,
		grpcserver.WithEngine(upstreamEng),
		grpcserver.WithSessionStore(upstreamStore),
	)
	if err := upstream.Start(); err != nil {
		t.Fatalf("start upstream: %v", err)
	}

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithReconnectPolicy(50*time.Millisecond, 200*time.Millisecond, 2.0),
		WithFanoutWorkers(1),
	)
	r.router = NewRouter([]ChannelPattern{
		{Pattern: "up-*", Mode: RouteModeRelay},
	})
	if err := r.Start(); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer r.Stop()

	client, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, client, "token")
	client.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "up-dup"},
		},
	})
	recvWithTimeout(t, client, 2*time.Second)
	time.Sleep(200 * time.Millisecond)

	upstreamEng.Publish("up-dup", []byte("tick"), 0, upstreamStore.Deliver)
	msg := recvWithTimeout(t, client, 3*time.Second)
	if cm := msg.GetChannelMessage(); cm == nil || string(cm.Data) != "tick" {
		t.Fatalf("expected live tick through relay, got %v", msg.Message)
	}

	upstream.Stop()
	upstreamEng.Publish("up-dup", []byte("tick"), 0, upstreamStore.Deliver)
	upstreamEng.Publish("up-dup", []byte("tick"), 0, upstreamStore.Deliver)

	upstream2 := grpcserver.New(upstreamAddr,
		grpcserver.WithEngine(upstreamEng),
		grpcserver.WithSessionStore(upstreamStore),
	)
	if err := upstream2.Start(); err != nil {
		t.Fatalf("restart upstream: %v", err)
	}
	defer upstream2.Stop()

	for i := range 2 {
		msg := recvWithTimeout(t, client, 5*time.Second)
		cm := msg.GetChannelMessage()
		if cm == nil || string(cm.Data) != "tick" {
			t.Fatalf("recovered message %d: got %v, want repeated payload %q", i, msg.Message, "tick")
		}
	}
}

func TestRelayTLSListener(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	certFile, keyFile := testcert.Generate(t)
	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithTLS(certFile, keyFile),
	)
	if err := r.Start(); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer r.Stop()

	creds, err := credentials.NewClientTLSFromFile(certFile, "")
	if err != nil {
		t.Fatalf("client creds: %v", err)
	}
	conn, err := grpc.NewClient(relayAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := gentisv1.NewGentisServiceClient(conn).Stream(ctx)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected Pong over TLS relay listener, got %T", msg.Message)
	}
}

type fakeClientStream struct {
	gentisv1.GentisService_StreamClient
}

func TestHandleDisconnectIgnoresStaleStream(t *testing.T) {
	u := NewUpstream(
		UpstreamConfig{Address: "127.0.0.1:1"},
		ReconnectPolicy{InitialDelay: time.Hour, MaxDelay: time.Hour, Multiplier: 2},
		nil,
		gentislog.Nop(),
	)
	fresh := &fakeClientStream{}
	stale := &fakeClientStream{}
	u.stream = fresh
	u.connected.Store(true)

	u.handleDisconnect(stale)

	if !u.connected.Load() {
		t.Fatal("stale stream error must not mark the fresh connection down")
	}
	u.streamMu.Lock()
	defer u.streamMu.Unlock()
	if u.stream != fresh {
		t.Fatal("stale stream error must not null the fresh stream")
	}
}

func TestRelayWildcardSubscribeRejectsQoSAndRecovery(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	relayAddr, stopRelay := startRelay(t, upstreamAddr)
	defer stopRelay()

	stream, closeClient := connectClient(t, relayAddr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel:        "metrics:*",
				MaxUnconfirmed: &gentisv1.UnconfirmedWindow{Count: 4},
			},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if errResp := msg.GetError(); errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Fatalf("pattern subscribe with qos: got %v, want INVALID_PAYLOAD", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "s2",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel: "metrics:*",
				Recover: &gentisv1.RecoverPoint{Offset: 3, Epoch: 7},
			},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if errResp := msg.GetError(); errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Fatalf("pattern subscribe with recover: got %v, want INVALID_PAYLOAD", msg.Message)
	}
}

func TestRelayQoSConfirmFlow(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	reg := namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"jobs": {
				AllowPublish:      true,
				HistorySize:       64,
				QoS:               namespace.AtLeastOnce,
				RedeliveryTimeout: 5 * time.Second,
				MaxRedeliveries:   3,
			},
		},
	})
	eng := engine.New(engine.WithNamespaces(reg))
	defer eng.Stop()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithEngine(eng),
		WithSessionStore(transport.NewSessionStore()),
	)
	r.router = NewRouter([]ChannelPattern{
		{Pattern: "jobs:*", Mode: RouteModeLocal},
	})
	if err := r.Start(); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer r.Stop()

	sub, closeSub := connectClient(t, relayAddr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel:        "jobs:emails",
				MaxUnconfirmed: &gentisv1.UnconfirmedWindow{Count: 2},
			},
		},
	})
	if recvWithTimeout(t, sub, 2*time.Second).GetSubscribed() == nil {
		t.Fatal("subscribe failed")
	}

	pub, closePub := connectClient(t, relayAddr)
	defer closePub()
	authenticate(t, pub, "token")
	for i := 1; i <= 10; i++ {
		pub.Send(&gentisv1.ClientMessage{
			Id: fmt.Sprintf("p%d", i),
			Message: &gentisv1.ClientMessage_Publish{
				Publish: &gentisv1.PublishRequest{Channel: "jobs:emails", Data: []byte(fmt.Sprintf("job-%d", i))},
			},
		})
		if recvWithTimeout(t, pub, 2*time.Second).GetPublished() == nil {
			t.Fatalf("publish %d not acked", i)
		}
	}

	var got []uint64
	for len(got) < 10 {
		msg := recvWithTimeout(t, sub, 3*time.Second)
		cm := msg.GetChannelMessage()
		if cm == nil {
			continue
		}
		if string(cm.Data) != fmt.Sprintf("job-%d", cm.Offset) {
			t.Fatalf("delivery offset %d carries %q, want job-%d", cm.Offset, cm.Data, cm.Offset)
		}
		got = append(got, cm.Offset)

		sub.Send(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Confirm{
				Confirm: &gentisv1.ConfirmRequest{Channel: "jobs:emails", Offset: cm.Offset},
			},
		})
	}

	for i, off := range got {
		if off != uint64(i+1) {
			t.Fatalf("deliveries = %v, want 1..10 in order without gaps or dups", got)
		}
	}
}

func TestRelayRecoverOnSubscribe(t *testing.T) {
	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	eng := engine.New(engine.WithHistory(16, 0))
	defer eng.Stop()

	relayAddr := freeAddr(t)
	r := New(
		WithListenAddr(relayAddr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithEngine(eng),
		WithSessionStore(transport.NewSessionStore()),
	)
	r.router = NewRouter([]ChannelPattern{
		{Pattern: "local-*", Mode: RouteModeLocal},
	})
	if err := r.Start(); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer r.Stop()

	holder, closeHolder := connectClient(t, relayAddr)
	defer closeHolder()
	authenticate(t, holder, "token")
	holder.Send(&gentisv1.ClientMessage{
		Id: "h1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "local-hist"},
		},
	})
	if recvWithTimeout(t, holder, 2*time.Second).GetSubscribed() == nil {
		t.Fatal("holder subscribe failed")
	}

	pub, closePub := connectClient(t, relayAddr)
	defer closePub()
	authenticate(t, pub, "token")
	var epoch uint64
	for i := 1; i <= 3; i++ {
		pub.Send(&gentisv1.ClientMessage{
			Id: fmt.Sprintf("p%d", i),
			Message: &gentisv1.ClientMessage_Publish{
				Publish: &gentisv1.PublishRequest{Channel: "local-hist", Data: []byte(fmt.Sprintf("m-%d", i))},
			},
		})
		ack := recvWithTimeout(t, pub, 2*time.Second).GetPublished()
		if ack == nil {
			t.Fatalf("publish %d not acked", i)
		}
		epoch = ack.Epoch
	}

	sub, closeSub := connectClient(t, relayAddr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel: "local-hist",
				Recover: &gentisv1.RecoverPoint{Offset: 1, Epoch: epoch},
			},
		},
	})
	subResp := recvWithTimeout(t, sub, 2*time.Second).GetSubscribed()
	if subResp == nil || !subResp.Recovered {
		t.Fatalf("subscribe with recover: got %+v, want Recovered=true", subResp)
	}

	for _, want := range []string{"m-2", "m-3"} {
		msg := recvWithTimeout(t, sub, 3*time.Second)
		cm := msg.GetChannelMessage()
		if cm == nil || string(cm.Data) != want {
			t.Fatalf("replay: got %v, want %q", msg.Message, want)
		}
	}
}

func TestSendRingCapFloorsToMinimum(t *testing.T) {
	cases := []struct {
		bufferSize int
		want       int
	}{
		{bufferSize: -1, want: 2},
		{bufferSize: 0, want: 2},
		{bufferSize: 1, want: 2},
		{bufferSize: 2, want: 2},
		{bufferSize: 3, want: 4},
		{bufferSize: 256, want: 256},
		{bufferSize: 257, want: 512},
	}
	for _, tc := range cases {
		got := sendRingCap(tc.bufferSize)
		if got != tc.want {
			t.Fatalf("sendRingCap(%d) = %d, want %d", tc.bufferSize, got, tc.want)
		}
		if _, err := ringbuf.NewPointer[gentisv1.ServerMessage](got); err != nil {
			t.Fatalf("sendRingCap(%d) produced unusable capacity %d: %v", tc.bufferSize, got, err)
		}
	}
}
