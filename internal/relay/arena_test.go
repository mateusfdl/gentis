package relay

import (
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/arena"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

// skipIfArenaUnsupported skips the test when running on a platform where
// arena.New returns ErrUnsupported (non-linux).
func skipIfArenaUnsupported(t *testing.T) {
	t.Helper()
	a, err := arena.New(1, 1)
	if err != nil {
		t.Skipf("arena not supported on this platform: %v", err)
	}
	a.Close()
}

// startArenaRelay starts a relay with WithArena() pointing at the given
// upstream address. Returns the relay, its listen address, and a cleanup.
//
// The relay's router is overridden to route ALL channels locally so tests
// can exercise arena delivery without having to round-trip through the
// upstream server.
func startArenaRelay(t *testing.T, upstreamAddr string, maxSessions int) (*Server, string, func()) {
	t.Helper()
	addr := freeAddr(t)

	// Flat store sized to match the arena so arena-derived IDs land in
	// the dense range and exercise the O(1) fast path.
	store := transport.NewFlatSessionStore(engine.SubscriberID(1), maxSessions)

	r := New(
		WithListenAddr(addr),
		WithUpstream(upstreamAddr, "relay-token"),
		WithBufferSize(256),
		WithReconnectPolicy(50*time.Millisecond, 1*time.Second, 2.0),
		WithArena(),
		WithMaxSessions(maxSessions),
		WithSessionStore(store),
	)

	// Route every channel locally (default is RouteModeRelay which sends
	// to upstream). Must be set before Start() because router is captured
	// by the grpc server goroutine.
	r.router = NewRouter([]ChannelPattern{
		{Pattern: "*", Mode: RouteModeLocal},
	})

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	return r, addr, func() { r.Stop() }
}

// TestRelayArenaEndToEnd verifies that a relay running with WithArena()
// completes a full connect/authenticate/subscribe/publish/receive cycle
// (relay-local delivery; upstream is involved only for connect auth).
func TestRelayArenaEndToEnd(t *testing.T) {
	skipIfArenaUnsupported(t)

	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	_, relayAddr, stopRelay := startArenaRelay(t, upstreamAddr, 64)
	defer stopRelay()

	sub, closeSub := connectClient(t, relayAddr)
	defer closeSub()
	authenticate(t, sub, "token")

	sub.Send(&gentisv1.ClientMessage{
		Message: &gentisv1.ClientMessage_Subscribe{
			Subscribe: &gentisv1.SubscribeRequest{Channel: "news"},
		},
	})
	recvWithTimeout(t, sub, 2*time.Second)

	pub, closePub := connectClient(t, relayAddr)
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

// TestRelayArenaSlotReuse exercises the Close → Alloc path: opens and
// closes the arena capacity multiple times, verifying sessions still
// work after slot reuse.
func TestRelayArenaSlotReuse(t *testing.T) {
	skipIfArenaUnsupported(t)

	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	const cap = 4
	const rounds = 3

	_, relayAddr, stopRelay := startArenaRelay(t, upstreamAddr, cap)
	defer stopRelay()

	for round := 0; round < rounds; round++ {
		streams := make([]gentisv1.GentisService_StreamClient, cap)
		closers := make([]func(), cap)
		for i := 0; i < cap; i++ {
			s, c := connectClient(t, relayAddr)
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

// TestRelayArenaFullFallback sets the arena cap to 1 and opens 2 concurrent
// sessions. The second must fall back to a heap *client.State without
// failing. Both sessions should still route pub/sub correctly.
func TestRelayArenaFullFallback(t *testing.T) {
	skipIfArenaUnsupported(t)

	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	_, relayAddr, stopRelay := startArenaRelay(t, upstreamAddr, 1)
	defer stopRelay()

	s1, close1 := connectClient(t, relayAddr)
	defer close1()
	authenticate(t, s1, "token")

	s2, close2 := connectClient(t, relayAddr)
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

// TestRelayArenaIDRange verifies that arena-allocated session IDs land in
// the [1, maxSessions] range and heap-fallback IDs land above it.
func TestRelayArenaIDRange(t *testing.T) {
	skipIfArenaUnsupported(t)

	upstreamAddr, stopUpstream := startUpstream(t)
	defer stopUpstream()

	const cap = 4
	srv, relayAddr, stopRelay := startArenaRelay(t, upstreamAddr, cap)
	defer stopRelay()

	closers := make([]func(), cap)
	for i := 0; i < cap; i++ {
		s, c := connectClient(t, relayAddr)
		closers[i] = c
		authenticate(t, s, "token")
	}
	defer func() {
		for _, c := range closers {
			c()
		}
	}()

	sFallback, closeFB := connectClient(t, relayAddr)
	defer closeFB()
	authenticate(t, sFallback, "token")

	time.Sleep(50 * time.Millisecond)
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
