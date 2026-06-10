package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/auth"
	"google.golang.org/grpc"
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
