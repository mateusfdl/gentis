package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/namespace"
	"github.com/mateusfdl/gentis/internal/ringbuf"
	"github.com/mateusfdl/gentis/internal/testcert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestServer(t *testing.T) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	return addr, func() {
		srv.Stop()
	}
}

func connectClient(t *testing.T, addr string) (gentisv1.GentisService_StreamClient, func()) {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := gentisv1.NewGentisServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

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

func authenticate(t *testing.T, stream gentisv1.GentisService_StreamClient, token string) string {
	t.Helper()

	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: token},
		},
	})
	if err != nil {
		t.Fatalf("failed to send connect: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to recv connected: %v", err)
	}

	connected := msg.GetConnected()
	if connected == nil {
		t.Fatalf("expected ConnectedResponse, got %T", msg.Message)
	}

	return connected.ConnectionId
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

func TestServerStartStop(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	if addr == "" {
		t.Fatal("expected non-empty address")
	}
}

func TestConnect(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	connID := authenticate(t, stream, "test-token")
	if connID == "" {
		t.Error("expected non-empty connection ID")
	}
}

func TestPing(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{
			Ping: &gentisv1.PingRequest{},
		},
	})
	if err != nil {
		t.Fatalf("failed to send ping: %v", err)
	}

	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected PongResponse, got %T", msg.Message)
	}
}

func TestSubscribeUnauthenticated(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "test"},
		},
	})
	if err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Errorf("expected NOT_AUTHENTICATED, got %v", errResp.Code)
	}
}

func TestPublishUnauthenticated(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "test", Data: []byte("data")},
		},
	})
	if err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Errorf("expected NOT_AUTHENTICATED, got %v", errResp.Code)
	}
}

func TestUnsubscribeUnauthenticated(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "test"},
		},
	})
	if err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Errorf("expected NOT_AUTHENTICATED, got %v", errResp.Code)
	}
}

func TestSubscribe(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	err := stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "my-channel"},
		},
	})
	if err != nil {
		t.Fatalf("failed to send subscribe: %v", err)
	}

	msg := recvWithTimeout(t, stream, 2*time.Second)
	sub := msg.GetSubscribed()
	if sub == nil {
		t.Fatalf("expected SubscribedResponse, got %T", msg.Message)
	}

	if sub.Channel != "my-channel" {
		t.Errorf("expected channel 'my-channel', got %q", sub.Channel)
	}
}

func TestSubscribeAlreadySubscribed(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
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
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_ALREADY_SUBSCRIBED {
		t.Errorf("expected ALREADY_SUBSCRIBED, got %v", errResp.Code)
	}
}

func TestSubscribeInvalidChannel(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: ""},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestUnsubscribe(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
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

func TestUnsubscribeNotSubscribed(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "ch"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED {
		t.Errorf("expected NOT_SUBSCRIBED, got %v", errResp.Code)
	}
}

func TestUnsubscribeInvalidChannel(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: ""},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestPublishInvalidChannel(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "", Data: []byte("data")},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}

	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestUnauthenticatedSessionClosedAtAuthDeadline(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithAuthDeadline(100*time.Millisecond))
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	stream.Send(&gentisv1.ClientMessage{
		Id:      "p1",
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected PongResponse, got %T", msg.Message)
	}

	errCh := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
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

func TestAuthenticatedSessionSurvivesAuthDeadline(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithAuthDeadline(100*time.Millisecond))
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")
	time.Sleep(300 * time.Millisecond)

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("expected SubscribedResponse, got %T", msg.Message)
	}
}

func TestPublishToPatternRejected(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "jobs:*", Data: []byte("data")},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestSubscribeReservedCharRejected(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "jobs:cpu?"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("expected INVALID_PAYLOAD, got %v", errResp.Code)
	}
}

func TestPublishAndReceive(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")

	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "news"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")

	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "news"},
		},
	})
	recvWithTimeout(t, pub, 2*time.Second)

	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: "news",
				Data:    []byte("hello world"),
			},
		},
	})

	msg := recvWithTimeout(t, sub, 2*time.Second)
	chMsg := msg.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("expected ChannelMessage, got %T", msg.Message)
	}

	if chMsg.Channel != "news" {
		t.Errorf("expected channel 'news', got %q", chMsg.Channel)
	}

	if string(chMsg.Data) != "hello world" {
		t.Errorf("expected data 'hello world', got %q", string(chMsg.Data))
	}
}

func TestPublisherExcludedFromOwnMessage(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	recvWithTimeout(t, stream, 2*time.Second)

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "ch", Data: []byte("msg")},
		},
	})

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{
			Ping: &gentisv1.PingRequest{},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected PongResponse (publisher excluded), got %T", msg.Message)
	}
}

func TestMultipleSubscribers(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	const numSubscribers = 5

	subscribers := make([]gentisv1.GentisService_StreamClient, numSubscribers)
	cleanups := make([]func(), numSubscribers)

	for i := range numSubscribers {
		s, c := connectClient(t, addr)
		subscribers[i] = s
		cleanups[i] = c
		defer c()

		authenticate(t, s, "token")

		s.Send(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: &gentisv1.SubscribeRequest{Channel: "broadcast"},
			},
		})
		recvWithTimeout(t, s, 2*time.Second)
	}

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")

	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "broadcast"},
		},
	})
	recvWithTimeout(t, pub, 2*time.Second)

	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "broadcast", Data: []byte("fanout")},
		},
	})

	for i, sub := range subscribers {
		msg := recvWithTimeout(t, sub, 2*time.Second)
		chMsg := msg.GetChannelMessage()
		if chMsg == nil {
			t.Fatalf("subscriber %d: expected ChannelMessage, got %T", i, msg.Message)
		}
		if string(chMsg.Data) != "fanout" {
			t.Errorf("subscriber %d: expected 'fanout', got %q", i, string(chMsg.Data))
		}
	}
}

func TestSubscribeMultipleChannels(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	channels := []string{"ch1", "ch2", "ch3"}
	for _, ch := range channels {
		stream.Send(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: &gentisv1.SubscribeRequest{Channel: ch},
			},
		})
		msg := recvWithTimeout(t, stream, 2*time.Second)
		sub := msg.GetSubscribed()
		if sub == nil {
			t.Fatalf("expected SubscribedResponse for %s, got %T", ch, msg.Message)
		}
		if sub.Channel != ch {
			t.Errorf("expected channel %s, got %s", ch, sub.Channel)
		}
	}
}

func TestPublishToSpecificChannel(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub1, close1 := connectClient(t, addr)
	defer close1()
	authenticate(t, sub1, "token")
	sub1.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch-a"},
		},
	})
	recvWithTimeout(t, sub1, 2*time.Second)

	sub2, close2 := connectClient(t, addr)
	defer close2()
	authenticate(t, sub2, "token")
	sub2.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch-b"},
		},
	})
	recvWithTimeout(t, sub2, 2*time.Second)

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")
	pub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "ch-a", Data: []byte("for-a")},
		},
	})

	msg := recvWithTimeout(t, sub1, 2*time.Second)
	if msg.GetChannelMessage() == nil {
		t.Fatalf("sub1: expected ChannelMessage, got %T", msg.Message)
	}

	sub2.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg2 := recvWithTimeout(t, sub2, 2*time.Second)
	if msg2.GetPong() == nil {
		t.Fatalf("sub2: expected Pong (not on ch-a), got %T", msg2.Message)
	}
}

func TestConcurrentPublishers(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "concurrent"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	const numPublishers = 10
	var wg sync.WaitGroup

	for i := range numPublishers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pub, closePub := connectClient(t, addr)
			defer closePub()
			authenticate(t, pub, "token")
			pub.Send(&gentisv1.ClientMessage{
				Message: &gentisv1.ClientMessage_Subscribe{
					Subscribe: &gentisv1.SubscribeRequest{Channel: "concurrent"},
				},
			})
			recvWithTimeout(t, pub, 2*time.Second)

			pub.Send(&gentisv1.ClientMessage{
				Message: &gentisv1.ClientMessage_Publish{
					Publish: &gentisv1.PublishRequest{
						Channel: "concurrent",
						Data:    []byte(fmt.Sprintf("msg-%d", id)),
					},
				},
			})
		}(i)
	}

	wg.Wait()

	received := 0
	for range numPublishers {
		msg := recvWithTimeout(t, sub, 3*time.Second)
		if msg.GetChannelMessage() != nil {
			received++
		}
	}

	if received != numPublishers {
		t.Errorf("expected %d messages, received %d", numPublishers, received)
	}
}

func TestSubscribeUnsubscribeResubscribe(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("expected SubscribedResponse, got %T", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "ch"},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetUnsubscribed() == nil {
		t.Fatalf("expected UnsubscribedResponse, got %T", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("expected SubscribedResponse on resubscribe, got %T", msg.Message)
	}
}

func TestServerWithMetrics(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	metricsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for metrics: %v", err)
	}
	metricsAddr := metricsLis.Addr().String()
	metricsLis.Close()

	srv := New(addr, WithMetrics(metricsAddr))
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server with metrics: %v", err)
	}
	defer srv.Stop()
}

func TestPublishAckCarriesOffsetAndFanout(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "acked"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")

	for i := range 2 {
		pub.Send(&gentisv1.ClientMessage{
			Id: fmt.Sprintf("req-%d", i),
			Message: &gentisv1.ClientMessage_Publish{
				Publish: &gentisv1.PublishRequest{Channel: "acked", Data: []byte("payload")},
			},
		})

		msg := recvWithTimeout(t, pub, 2*time.Second)
		if msg.Id != fmt.Sprintf("req-%d", i) {
			t.Errorf("publish %d: expected correlation id %q, got %q", i, fmt.Sprintf("req-%d", i), msg.Id)
		}
		ack := msg.GetPublished()
		if ack == nil {
			t.Fatalf("publish %d: expected PublishResponse, got %T", i, msg.Message)
		}
		if ack.Channel != "acked" {
			t.Errorf("publish %d: expected channel 'acked', got %q", i, ack.Channel)
		}
		if ack.Offset != uint64(i+1) {
			t.Errorf("publish %d: expected offset %d, got %d", i, i+1, ack.Offset)
		}
		if ack.Epoch == 0 {
			t.Errorf("publish %d: expected non-zero epoch", i)
		}
		if ack.Delivered != 1 {
			t.Errorf("publish %d: expected delivered 1, got %d", i, ack.Delivered)
		}
		if ack.Dropped != 0 {
			t.Errorf("publish %d: expected dropped 0, got %d", i, ack.Dropped)
		}
	}

	delivery := recvWithTimeout(t, sub, 2*time.Second)
	chMsg := delivery.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("expected ChannelMessage, got %T", delivery.Message)
	}
	if chMsg.Offset != 1 {
		t.Errorf("expected delivery offset 1, got %d", chMsg.Offset)
	}
	if chMsg.Epoch == 0 {
		t.Error("expected non-zero delivery epoch")
	}
}

func TestPublishWithoutCorrelationIDGetsNoAck(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "silent", Data: []byte("x")},
		},
	})
	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected PongResponse (no ack without id), got %T", msg.Message)
	}
}

func TestPublishAckToSubscriberlessChannel(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Id: "lonely",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "nobody", Data: []byte("x")},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	ack := msg.GetPublished()
	if ack == nil {
		t.Fatalf("expected PublishResponse, got %T", msg.Message)
	}
	if ack.Offset != 0 || ack.Epoch != 0 || ack.Delivered != 0 || ack.Dropped != 0 {
		t.Errorf("expected zero-identity ack for subscriberless channel, got offset=%d epoch=%d delivered=%d dropped=%d",
			ack.Offset, ack.Epoch, ack.Delivered, ack.Dropped)
	}
}

func startVerifierServer(t *testing.T, secret []byte) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithVerifier(auth.NewHMACVerifier(secret)))
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	return addr, func() { srv.Stop() }
}

func TestConnectRejectsInvalidToken(t *testing.T) {
	addr, cleanup := startVerifierServer(t, []byte("grpc-secret"))
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	stream.Send(&gentisv1.ClientMessage{
		Id: "c1",
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: "garbage"},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil {
		t.Fatalf("expected ErrorResponse, got %T", msg.Message)
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Errorf("error code = %v, want NOT_AUTHENTICATED", errResp.Code)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	errResp = msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Fatalf("subscribe after failed connect: got %v, want NOT_AUTHENTICATED error", msg.Message)
	}
}

func TestConnectAcceptsSignedToken(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	authenticate(t, stream, token)

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("expected SubscribedResponse, got %T", msg.Message)
	}
}

func TestConnectRejectsExpiredToken(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(-time.Minute),
	})
	stream.Send(&gentisv1.ClientMessage{
		Id: "c1",
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: token},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Fatalf("expected NOT_AUTHENTICATED error, got %v", msg.Message)
	}
}

func TestPermissionChecks(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
		Channels:  []string{"allowed-*"},
		Pub:       []string{"allowed-pub"},
	})
	authenticate(t, stream, token)

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "forbidden"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("subscribe outside allowlist: got %v, want PERMISSION_DENIED", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "s2",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "allowed-1"},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("subscribe inside allowlist: got %v, want Subscribed", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "p1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "allowed-1", Data: []byte("x")},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	errResp = msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("publish outside pub allowlist: got %v, want PERMISSION_DENIED", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "p2",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "allowed-pub", Data: []byte("x")},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPublished() == nil {
		t.Fatalf("publish inside pub allowlist: got %v, want PublishResponse", msg.Message)
	}
}

func TestExpiredSessionDisconnects(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Second),
	})
	authenticate(t, stream, token)

	deadline := time.After(4 * time.Second)
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				done <- err
				return
			}
		}
	}()

	select {
	case <-done:
	case <-deadline:
		t.Fatal("session not disconnected after token expiry")
	}
}

func TestRefreshExtendsSession(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	short := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(2 * time.Second),
	})
	authenticate(t, stream, short)

	renewedExp := time.Now().Add(time.Hour)
	renewed := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: renewedExp,
	})
	stream.Send(&gentisv1.ClientMessage{
		Id: "r1",
		Message: &gentisv1.ClientMessage_Refresh{
			Refresh: &gentisv1.RefreshRequest{AuthToken: renewed},
		},
	})

	msg := recvWithTimeout(t, stream, 2*time.Second)
	refreshed := msg.GetRefreshed()
	if refreshed == nil {
		t.Fatalf("expected RefreshResponse, got %v", msg.Message)
	}
	if refreshed.ExpiresAt != uint64(renewedExp.Unix()) {
		t.Errorf("expires_at = %d, want %d", refreshed.ExpiresAt, renewedExp.Unix())
	}

	time.Sleep(2500 * time.Millisecond)

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("expected Pong after refresh outlived original expiry, got %v", msg.Message)
	}
}

func TestRefreshRejectsBadToken(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	authenticate(t, stream, token)

	stream.Send(&gentisv1.ClientMessage{
		Id: "r1",
		Message: &gentisv1.ClientMessage_Refresh{
			Refresh: &gentisv1.RefreshRequest{AuthToken: "garbage"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Fatalf("expected NOT_AUTHENTICATED, got %v", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPong() == nil {
		t.Fatalf("session should survive failed refresh, got %v", msg.Message)
	}
}

func TestRefreshRejectsSubjectChange(t *testing.T) {
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()

	authenticate(t, stream, auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	stream.Send(&gentisv1.ClientMessage{
		Id: "r1",
		Message: &gentisv1.ClientMessage_Refresh{
			Refresh: &gentisv1.RefreshRequest{AuthToken: auth.SignHS256(secret, auth.Claims{
				Subject:   "user-2",
				ExpiresAt: time.Now().Add(time.Hour),
			})},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_AUTHENTICATED {
		t.Fatalf("expected NOT_AUTHENTICATED on subject change, got %v", msg.Message)
	}
}

func TestTLSServer(t *testing.T) {
	certFile, keyFile := testcert.Generate(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithTLS(certFile, keyFile))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	creds, err := credentials.NewClientTLSFromFile(certFile, "")
	if err != nil {
		t.Fatalf("client creds: %v", err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
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
		t.Fatalf("expected Pong over TLS, got %T", msg.Message)
	}
}

func TestTLSServerRejectsPlaintext(t *testing.T) {
	certFile, keyFile := testcert.Generate(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithTLS(certFile, keyFile))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := gentisv1.NewGentisServiceClient(conn).Stream(ctx)
	if err == nil {
		if _, err = stream.Recv(); err == nil {
			t.Fatal("expected plaintext client to fail against TLS server")
		}
	}
}

func TestPublishMessageTooLarge(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithMaxMessageSize(1024))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Id: "big",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "ch", Data: make([]byte, 2048)},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE {
		t.Fatalf("oversized publish: got %v, want MESSAGE_TOO_LARGE", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "fits",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "ch", Data: make([]byte, 1024)},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetPublished() == nil {
		t.Fatalf("max-size publish: got %v, want PublishResponse", msg.Message)
	}
}

func TestSubscriptionLimit(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithMaxSubscriptions(2))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	for i := range 2 {
		stream.Send(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Subscribe{
				Subscribe: &gentisv1.SubscribeRequest{Channel: fmt.Sprintf("ch-%d", i)},
			},
		})
		msg := recvWithTimeout(t, stream, 2*time.Second)
		if msg.GetSubscribed() == nil {
			t.Fatalf("subscribe %d: got %v, want Subscribed", i, msg.Message)
		}
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "over",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch-2"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_SUBSCRIPTION_LIMIT {
		t.Fatalf("subscribe over limit: got %v, want SUBSCRIPTION_LIMIT", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "ch-0"},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetUnsubscribed() == nil {
		t.Fatalf("unsubscribe: got %v", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ch-2"},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetSubscribed() == nil {
		t.Fatalf("subscribe after freeing slot: got %v, want Subscribed", msg.Message)
	}
}

func startHistoryServer(t *testing.T) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	eng := engine.New(engine.WithHistory(64, 0))
	srv := New(addr, WithEngine(eng))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return addr, func() {
		srv.Stop()
		eng.Stop()
	}
}

func TestSubscribeWithRecovery(t *testing.T) {
	addr, cleanup := startHistoryServer(t)
	defer cleanup()

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")

	sub, closeSub := connectClient(t, addr)
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "rec-ch"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub.Send(&gentisv1.ClientMessage{
		Id: "p1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "rec-ch", Data: []byte("m-1")},
		},
	})
	first := recvWithTimeout(t, sub, 2*time.Second).GetChannelMessage()
	if first == nil || first.Offset != 1 {
		t.Fatalf("expected live delivery offset 1, got %+v", first)
	}
	epoch := first.Epoch

	closeSub()
	ack := recvWithTimeout(t, pub, 2*time.Second).GetPublished()
	if ack == nil {
		t.Fatalf("expected publish ack")
	}

	for i := 2; i <= 3; i++ {
		pub.Send(&gentisv1.ClientMessage{
			Id: fmt.Sprintf("p%d", i),
			Message: &gentisv1.ClientMessage_Publish{
				Publish: &gentisv1.PublishRequest{Channel: "rec-ch", Data: []byte(fmt.Sprintf("m-%d", i))},
			},
		})
		if recvWithTimeout(t, pub, 2*time.Second).GetPublished() == nil {
			t.Fatalf("publish %d not acked", i)
		}
	}

	sub2, closeSub2 := connectClient(t, addr)
	defer closeSub2()
	authenticate(t, sub2, "token")
	sub2.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel: "rec-ch",
				Recover: &gentisv1.RecoverPoint{Offset: 1, Epoch: epoch},
			},
		},
	})

	msg := recvWithTimeout(t, sub2, 2*time.Second)
	subscribed := msg.GetSubscribed()
	if subscribed == nil {
		t.Fatalf("expected Subscribed, got %v", msg.Message)
	}
	if !subscribed.Recovered {
		t.Fatal("Subscribed.Recovered = false, want true")
	}

	for i := 2; i <= 3; i++ {
		cm := recvWithTimeout(t, sub2, 2*time.Second).GetChannelMessage()
		if cm == nil {
			t.Fatalf("expected replayed ChannelMessage %d", i)
		}
		if cm.Channel != "rec-ch" || cm.Offset != uint64(i) || cm.Epoch != epoch || string(cm.Data) != fmt.Sprintf("m-%d", i) {
			t.Errorf("replay %d = {channel:%q offset:%d epoch:%d data:%q}, want {rec-ch %d %d m-%d}",
				i, cm.Channel, cm.Offset, cm.Epoch, cm.Data, i, epoch, i)
		}
	}
}

func TestSubscribeWithRecoveryEpochMismatch(t *testing.T) {
	addr, cleanup := startHistoryServer(t)
	defer cleanup()

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")
	pub.Send(&gentisv1.ClientMessage{
		Id: "p1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "mismatch-ch", Data: []byte("x")},
		},
	})

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel: "mismatch-ch",
				Recover: &gentisv1.RecoverPoint{Offset: 0, Epoch: 12345},
			},
		},
	})

	msg := recvWithTimeout(t, sub, 2*time.Second)
	subscribed := msg.GetSubscribed()
	if subscribed == nil {
		t.Fatalf("expected Subscribed, got %v", msg.Message)
	}
	if subscribed.Recovered {
		t.Fatal("Subscribed.Recovered = true with wrong epoch, want false")
	}
}

func TestNamespacePolicies(t *testing.T) {
	reg := namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"feed": {AllowPublish: false},
		},
		Strict: true,
	})
	eng := engine.New(engine.WithNamespaces(reg))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithEngine(eng))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		srv.Stop()
		eng.Stop()
	}()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "ghost:x"},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_CHANNEL_NOT_FOUND {
		t.Fatalf("subscribe unknown namespace: got %v, want CHANNEL_NOT_FOUND", msg.Message)
	}

	stream.Send(&gentisv1.ClientMessage{
		Id: "p1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "feed:news", Data: []byte("x")},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	errResp = msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("publish read-only namespace: got %v, want PERMISSION_DENIED", msg.Message)
	}
}

func TestQoSSlowConsumerNoDrops(t *testing.T) {
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
		Strict: false,
	})
	eng := engine.New(engine.WithNamespaces(reg))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv := New(addr, WithEngine(eng))
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		srv.Stop()
		eng.Stop()
	}()

	sub, closeSub := connectClient(t, addr)
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

	pub, closePub := connectClient(t, addr)
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

func TestQoSRejectedOutsideNamespace(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Id: "s1",
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel:        "plain",
				MaxUnconfirmed: &gentisv1.UnconfirmedWindow{Count: 4},
			},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	errResp := msg.GetError()
	if errResp == nil || errResp.Code != gentisv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("qos subscribe without namespace support: got %v, want PERMISSION_DENIED", msg.Message)
	}
}

func connectWithVersion(t *testing.T, stream gentisv1.GentisService_StreamClient, version uint32) uint32 {
	t.Helper()
	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: "token", ProtocolVersion: version},
		},
	})
	msg := recvWithTimeout(t, stream, 2*time.Second)
	connected := msg.GetConnected()
	if connected == nil {
		t.Fatalf("expected ConnectedResponse, got %T", msg.Message)
	}
	return connected.ProtocolVersion
}

func TestProtocolVersionNegotiation(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	tests := []struct {
		name      string
		advertise uint32
		want      uint32
	}{
		{name: "legacy client without version", advertise: 0, want: 1},
		{name: "version 1 client", advertise: 1, want: 1},
		{name: "version 2 client", advertise: 2, want: 2},
		{name: "future client capped at server max", advertise: 9, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, closeClient := connectClient(t, addr)
			defer closeClient()
			if got := connectWithVersion(t, stream, tt.advertise); got != tt.want {
				t.Errorf("negotiated version = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBatchedDelivery(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	if v := connectWithVersion(t, sub, 2); v != 2 {
		t.Fatalf("negotiated %d, want 2", v)
	}
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "burst"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")

	const total = 50
	for i := 1; i <= total; i++ {
		pub.Send(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Publish{
				Publish: &gentisv1.PublishRequest{Channel: "burst", Data: []byte(fmt.Sprintf("m-%d", i))},
			},
		})
	}

	received := 0
	batches := 0
	var offsets []uint64
	for received < total {
		msg := recvWithTimeout(t, sub, 3*time.Second)
		if b := msg.GetBatch(); b != nil {
			if len(b.Messages) < 2 {
				t.Fatalf("batch frame with %d messages, batches must hold 2+", len(b.Messages))
			}
			batches++
			for _, cm := range b.Messages {
				offsets = append(offsets, cm.Offset)
				received++
			}
			continue
		}
		if cm := msg.GetChannelMessage(); cm != nil {
			offsets = append(offsets, cm.Offset)
			received++
		}
	}

	if batches == 0 {
		t.Fatal("burst of 50 produced zero batch frames for a v2 client")
	}
	for i, off := range offsets {
		if off != uint64(i+1) {
			t.Fatalf("offsets = %v..., want 1..%d in order", offsets[:i+1], total)
		}
	}
}

func TestLegacyClientNeverSeesBatches(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "legacy-burst"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")

	const total = 30
	for i := 1; i <= total; i++ {
		pub.Send(&gentisv1.ClientMessage{
			Message: &gentisv1.ClientMessage_Publish{
				Publish: &gentisv1.PublishRequest{Channel: "legacy-burst", Data: []byte("x")},
			},
		})
	}

	for received := 0; received < total; {
		msg := recvWithTimeout(t, sub, 3*time.Second)
		if msg.GetBatch() != nil {
			t.Fatal("legacy client received a batch frame")
		}
		if msg.GetChannelMessage() != nil {
			received++
		}
	}
}

func TestWildcardSubscribeDeliversMatchingChannels(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "metrics:*"},
		},
	})
	subResp := recvWithTimeout(t, sub, 2*time.Second).GetSubscribed()
	if subResp == nil {
		t.Fatal("expected SubscribedResponse for pattern subscribe")
	}
	if subResp.Channel != "metrics:*" {
		t.Fatalf("subscribed channel = %q, want metrics:*", subResp.Channel)
	}

	pub, closePub := connectClient(t, addr)
	defer closePub()
	authenticate(t, pub, "token")
	pub.Send(&gentisv1.ClientMessage{
		Id: "p1",
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{Channel: "metrics:cpu", Data: []byte("v")},
		},
	})
	ack := recvWithTimeout(t, pub, 2*time.Second).GetPublished()
	if ack == nil {
		t.Fatal("expected PublishResponse")
	}
	if ack.Delivered != 1 {
		t.Fatalf("ack.Delivered = %d, want 1", ack.Delivered)
	}

	delivery := recvWithTimeout(t, sub, 2*time.Second).GetChannelMessage()
	if delivery == nil {
		t.Fatal("expected ChannelMessage")
	}
	if delivery.Channel != "metrics:cpu" {
		t.Errorf("delivery channel = %q, want metrics:cpu", delivery.Channel)
	}
	if delivery.Offset != 1 {
		t.Errorf("delivery offset = %d, want 1", delivery.Offset)
	}
	if string(delivery.Data) != "v" {
		t.Errorf("delivery data = %q, want v", delivery.Data)
	}
}

func TestWildcardUnsubscribeStopsDelivery(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	sub, closeSub := connectClient(t, addr)
	defer closeSub()
	authenticate(t, sub, "token")
	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "metrics:*"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "metrics:*"},
		},
	})
	unsub := recvWithTimeout(t, sub, 2*time.Second).GetUnsubscribed()
	if unsub == nil {
		t.Fatal("expected UnsubscribedResponse")
	}
	if unsub.Channel != "metrics:*" {
		t.Fatalf("unsubscribed channel = %q, want metrics:*", unsub.Channel)
	}

	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Unsubscribe{
			Unsubscribe: &gentisv1.UnsubscribeRequest{Channel: "metrics:*"},
		},
	})
	errResp := recvWithTimeout(t, sub, 2*time.Second).GetError()
	if errResp == nil {
		t.Fatal("expected ErrorResponse for double unsubscribe")
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_NOT_SUBSCRIBED {
		t.Errorf("error code = %v, want NOT_SUBSCRIBED", errResp.Code)
	}
}

func TestWildcardSubscribeRejectsQoSAndRecovery(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
	defer closeClient()
	authenticate(t, stream, "token")

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel:        "metrics:*",
				MaxUnconfirmed: &gentisv1.UnconfirmedWindow{Count: 8},
			},
		},
	})
	errResp := recvWithTimeout(t, stream, 2*time.Second).GetError()
	if errResp == nil {
		t.Fatal("expected ErrorResponse for pattern subscribe with qos window")
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("error code = %v, want INVALID_PAYLOAD", errResp.Code)
	}

	stream.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{
				Channel: "metrics:*",
				Recover: &gentisv1.RecoverPoint{Offset: 1, Epoch: 1},
			},
		},
	})
	errResp = recvWithTimeout(t, stream, 2*time.Second).GetError()
	if errResp == nil {
		t.Fatal("expected ErrorResponse for pattern subscribe with recover")
	}
	if errResp.Code != gentisv1.ErrorCode_ERROR_CODE_INVALID_PAYLOAD {
		t.Errorf("error code = %v, want INVALID_PAYLOAD", errResp.Code)
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
	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](sendRingCapacity)
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
		for i := range sendRingCapacity + 8 {
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
	secret := []byte("grpc-secret")
	addr, cleanup := startVerifierServer(t, secret)
	defer cleanup()

	stream, closeClient := connectClient(t, addr)
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

	stream.Send(&gentisv1.ClientMessage{
		Id: "c3",
		Message: &gentisv1.ClientMessage_Connect{
			Connect: &gentisv1.ConnectRequest{AuthToken: auth.SignHS256(secret, auth.Claims{
				Subject:   "user-1",
				ExpiresAt: time.Now().Add(time.Hour),
			})},
		},
	})
	msg = recvWithTimeout(t, stream, 2*time.Second)
	if msg.GetConnected() == nil {
		t.Fatalf("same-subject reconnect must stay idempotent, got %v", msg.Message)
	}
}

type recordingStream struct {
	gentisv1.GentisService_StreamServer
	mu       sync.Mutex
	frames   []int
	messages int
}

func (r *recordingStream) Send(m *gentisv1.ServerMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b := m.GetBatch(); b != nil {
		size := 0
		for _, cm := range b.Messages {
			size += len(cm.Data)
		}
		r.frames = append(r.frames, size)
		r.messages += len(b.Messages)
		return nil
	}
	r.frames = append(r.frames, len(m.GetChannelMessage().GetData()))
	r.messages++
	return nil
}

func (r *recordingStream) snapshot() (frames []int, messages int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.frames...), r.messages
}

func TestBatchingCapsFrameBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](sendRingCapacity)
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
	sess.protoVersion.Store(2)

	const total = 64
	payload := make([]byte, 64*1024)
	for i := range total {
		if !ring.TryProduce(getServerMsg(engine.Delivery{Channel: "big", Data: payload, Offset: uint64(i + 1), Epoch: 7})) {
			t.Fatalf("ring full at %d", i)
		}
	}

	rec := &recordingStream{}
	go sess.runSender(rec)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, n := rec.snapshot(); n == total {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-sess.senderDone

	frames, n := rec.snapshot()
	if n != total {
		t.Fatalf("delivered %d messages, want %d", n, total)
	}
	for i, size := range frames {
		if size > maxBatchBytes {
			t.Fatalf("frame %d carries %d payload bytes, want <= %d (default grpc client recv limit is 4MiB)", i, size, maxBatchBytes)
		}
	}
}
