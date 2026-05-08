package grpc

import (
	"net"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

func skipIfArenaUnsupported(t *testing.T) {
	t.Helper()
	a, err := arena.New(1, 1)
	if err != nil {
		t.Skipf("arena not supported on this platform: %v", err)
	}
	a.Close()
}

func startArenaTestServer(t *testing.T, maxSessions int) (*Server, string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	store := transport.NewFlatSessionStore(engine.SubscriberID(1), maxSessions)

	srv := New(addr,
		WithArena(),
		WithMaxSessions(maxSessions),
		WithSessionStore(store),
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	return srv, addr, func() { srv.Stop() }
}

func TestGRPCArenaEndToEnd(t *testing.T) {
	skipIfArenaUnsupported(t)

	_, addr, cleanup := startArenaTestServer(t, 64)
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
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: "news",
				Data:    []byte("arena-hello"),
			},
		},
	})

	msg := recvWithTimeout(t, sub, 2*time.Second)
	chMsg := msg.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("expected ChannelMessage, got %T", msg.Message)
	}
	if string(chMsg.Data) != "arena-hello" {
		t.Errorf("expected 'arena-hello', got %q", string(chMsg.Data))
	}
}

func TestGRPCArenaSlotReuse(t *testing.T) {
	skipIfArenaUnsupported(t)

	const cap = 4
	const rounds = 3

	_, addr, cleanup := startArenaTestServer(t, cap)
	defer cleanup()

	for round := 0; round < rounds; round++ {
		streams := make([]gentisv1.GentisService_StreamClient, cap)
		closers := make([]func(), cap)
		for i := 0; i < cap; i++ {
			s, c := connectClient(t, addr)
			streams[i] = s
			closers[i] = c
			authenticate(t, s, "token")
		}

		for _, s := range streams {
			s.Send(&gentisv1.ClientMessage{
				Message: &gentisv1.ClientMessage_Ping{Ping: &gentisv1.PingRequest{}},
			})
			msg := recvWithTimeout(t, s, 2*time.Second)
			if msg.GetPong() == nil {
				t.Fatalf("round %d: expected Pong, got %T", round, msg.Message)
			}
		}

		for _, c := range closers {
			c()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestGRPCArenaFullFallback(t *testing.T) {
	skipIfArenaUnsupported(t)

	_, addr, cleanup := startArenaTestServer(t, 1)
	defer cleanup()

	s1, close1 := connectClient(t, addr)
	defer close1()
	authenticate(t, s1, "token")

	s2, close2 := connectClient(t, addr)
	defer close2()
	authenticate(t, s2, "token")

	s1.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "fb"},
		},
	})
	recvWithTimeout(t, s1, 2*time.Second)

	s2.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Publish{
			Publish: &gentisv1.PublishRequest{
				Channel: "fb",
				Data:    []byte("from-heap-fallback"),
			},
		},
	})

	msg := recvWithTimeout(t, s1, 2*time.Second)
	chMsg := msg.GetChannelMessage()
	if chMsg == nil {
		t.Fatalf("expected ChannelMessage, got %T", msg.Message)
	}
	if string(chMsg.Data) != "from-heap-fallback" {
		t.Errorf("expected 'from-heap-fallback', got %q", string(chMsg.Data))
	}
}

func TestGRPCArenaIDRange(t *testing.T) {
	skipIfArenaUnsupported(t)

	const cap = 4
	srv, addr, cleanup := startArenaTestServer(t, cap)
	defer cleanup()

	streams := make([]gentisv1.GentisService_StreamClient, cap)
	closers := make([]func(), cap)
	for i := 0; i < cap; i++ {
		s, c := connectClient(t, addr)
		streams[i] = s
		closers[i] = c
		authenticate(t, s, "token")
	}
	defer func() {
		for _, c := range closers {
			c()
		}
	}()

	sFallback, closeFB := connectClient(t, addr)
	defer closeFB()
	authenticate(t, sFallback, "token")

	time.Sleep(50 * time.Millisecond) // let server register all sessions
	arenaIDs := 0
	fallbackIDs := 0
	srv.sessions.Range(func(k, v any) bool {
		id, ok := k.(int)
		if !ok {
			return true
		}
		if id >= 1 && id <= cap {
			arenaIDs++
		} else if id > cap {
			fallbackIDs++
		}
		return true
	})
	if arenaIDs != cap {
		t.Errorf("expected %d arena-range IDs, got %d", cap, arenaIDs)
	}
	if fallbackIDs != 1 {
		t.Errorf("expected 1 fallback ID, got %d", fallbackIDs)
	}
}
