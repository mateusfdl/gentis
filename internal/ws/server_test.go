package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

func startTestServer(t *testing.T) (addr string, stop func()) {
	t.Helper()

	eng := engine.New()
	store := transport.NewSessionStore()

	opts := []Option{
		WithEngine(eng),
		WithSessionStore(store),
		WithReadLimit(1024),
		WithSendBufferSize(64),
	}

	srv := New("127.0.0.1:0", opts...)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var once sync.Once
	stopFn := func() {
		once.Do(func() {
			srv.Stop()
			time.Sleep(50 * time.Millisecond)
		})
	}
	t.Cleanup(stopFn)
	t.Cleanup(func() { eng.Stop() })

	return srv.listener.Addr().String(), stopFn
}

func dialWS(t *testing.T, addr string) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, _, err := ws.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return conn
}

func sendJSON(t *testing.T, conn net.Conn, msg any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := wsutil.WriteClientMessage(conn, ws.OpText, data); err != nil {
		t.Fatalf("WriteClientMessage: %v", err)
	}
}

func readJSON(t *testing.T, conn net.Conn, dst any) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, _, err := wsutil.ReadServerData(conn)
	if err != nil {
		t.Fatalf("ReadServerData: %v", err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
}

func authenticate(t *testing.T, conn net.Conn) {
	t.Helper()
	sendJSON(t, conn, map[string]any{
		"id":      "auth-1",
		"connect": map[string]any{"auth_token": "test-token"},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected response, got %+v", resp)
	}
}

func subscribe(t *testing.T, conn net.Conn, channel string) {
	t.Helper()
	sendJSON(t, conn, map[string]any{
		"id":        "sub-1",
		"subscribe": map[string]any{"channel": channel},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("expected Subscribed response, got %+v", resp)
	}
}

func TestConnectAuth(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id":      "req-1",
		"connect": map[string]any{"auth_token": "my-token"},
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)

	if resp.ID != "req-1" {
		t.Fatalf("resp ID = %q, want %q", resp.ID, "req-1")
	}
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}
}

func TestPing(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id":   "ping-1",
		"ping": map[string]any{},
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)

	if resp.ID != "ping-1" {
		t.Fatalf("resp ID = %q, want %q", resp.ID, "ping-1")
	}
	if resp.Pong == nil {
		t.Fatalf("expected Pong, got %+v", resp)
	}
}

func TestSubscribePublish(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	connA := dialWS(t, addr)
	defer connA.Close()
	authenticate(t, connA)
	subscribe(t, connA, "news")

	connB := dialWS(t, addr)
	defer connB.Close()
	authenticate(t, connB)

	sendJSON(t, connB, map[string]any{
		"id":      "pub-1",
		"publish": map[string]any{"channel": "news", "data": "hello"},
	})

	var msg ServerMessage
	readJSON(t, connA, &msg)

	if msg.ChannelMessage == nil {
		t.Fatalf("expected ChannelMessage, got %+v", msg)
	}
	if msg.ChannelMessage.Channel != "news" {
		t.Fatalf("channel = %q, want %q", msg.ChannelMessage.Channel, "news")
	}
}

func TestSubscribeUnsubscribe(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	connA := dialWS(t, addr)
	defer connA.Close()
	authenticate(t, connA)
	subscribe(t, connA, "ch")

	sendJSON(t, connA, map[string]any{
		"id":          "unsub-1",
		"unsubscribe": map[string]any{"channel": "ch"},
	})
	var unsubResp ServerMessage
	readJSON(t, connA, &unsubResp)
	if unsubResp.Unsubscribed == nil {
		t.Fatalf("expected Unsubscribed, got %+v", unsubResp)
	}

	connB := dialWS(t, addr)
	defer connB.Close()
	authenticate(t, connB)

	sendJSON(t, connB, map[string]any{
		"id":      "pub-1",
		"publish": map[string]any{"channel": "ch", "data": "nope"},
	})

	connA.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := wsutil.ReadServerData(connA)
	if err == nil {
		t.Fatal("expected timeout, got a message after unsubscribe")
	}
}

func TestNotAuthenticated(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id":        "sub-1",
		"subscribe": map[string]any{"channel": "ch"},
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)

	if resp.Error == nil {
		t.Fatalf("expected Error, got %+v", resp)
	}
	if resp.Error.Code != ErrorCodeNotAuthenticated {
		t.Fatalf("error code = %q, want %q", resp.Error.Code, ErrorCodeNotAuthenticated)
	}
}

func TestCleanDisconnect(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	authenticate(t, conn)
	subscribe(t, conn, "ch")

	conn.Close()

	time.Sleep(100 * time.Millisecond)

	conn2 := dialWS(t, addr)
	defer conn2.Close()
	authenticate(t, conn2)
}

func TestMultipleConnections(t *testing.T) {
	addr, stop := startTestServer(t)

	const n = 5
	conns := make([]net.Conn, n)
	for i := range n {
		conns[i] = dialWS(t, addr)
		authenticate(t, conns[i])
		subscribe(t, conns[i], "broadcast")
	}

	time.Sleep(50 * time.Millisecond)

	pub := dialWS(t, addr)
	authenticate(t, pub)

	sendJSON(t, pub, map[string]any{
		"id":      "pub-1",
		"publish": map[string]any{"channel": "broadcast", "data": "hi all"},
	})

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(c net.Conn, idx int) {
			defer wg.Done()
			var msg ServerMessage
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			data, _, err := wsutil.ReadServerData(c)
			if err != nil {
				t.Errorf("conn %d read: %v", idx, err)
				return
			}
			json.Unmarshal(data, &msg)
			if msg.ChannelMessage == nil {
				t.Errorf("conn %d: expected ChannelMessage, got %+v", idx, msg)
			}
		}(conns[i], i)
	}
	wg.Wait()

	pub.Close()
	for _, c := range conns {
		c.Close()
	}
	stop()
}

func TestServerStop(t *testing.T) {
	addr, stop := startTestServer(t)

	conn := dialWS(t, addr)
	authenticate(t, conn)

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5 seconds")
	}

	conn.Close()
}

func TestAlreadySubscribed(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)
	subscribe(t, conn, "ch")

	sendJSON(t, conn, map[string]any{
		"id":        "sub-dup",
		"subscribe": map[string]any{"channel": "ch"},
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)

	if resp.Error == nil || resp.Error.Code != ErrorCodeAlreadySubscribed {
		t.Fatalf("expected ALREADY_SUBSCRIBED error, got %+v", resp)
	}
}

func TestUnknownMessage(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{
		"id": "wat",
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)

	if resp.Error == nil || resp.Error.Code != ErrorCodeUnknownMessage {
		fmt.Printf("resp: %+v\n", resp)
		t.Fatalf("expected UNKNOWN_MESSAGE error, got %+v", resp)
	}
}

func TestPublishAck(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	sub := dialWS(t, addr)
	defer sub.Close()
	authenticate(t, sub)
	subscribe(t, sub, "acked")

	pub := dialWS(t, addr)
	defer pub.Close()
	authenticate(t, pub)

	for i := range 2 {
		sendJSON(t, pub, map[string]any{
			"id":      fmt.Sprintf("pub-%d", i),
			"publish": map[string]any{"channel": "acked", "data": "x"},
		})

		var msg ServerMessage
		readJSON(t, pub, &msg)

		if msg.ID != fmt.Sprintf("pub-%d", i) {
			t.Errorf("publish %d: id = %q, want %q", i, msg.ID, fmt.Sprintf("pub-%d", i))
		}
		if msg.Published == nil {
			t.Fatalf("publish %d: expected Published, got %+v", i, msg)
		}
		if msg.Published.Channel != "acked" {
			t.Errorf("publish %d: channel = %q, want %q", i, msg.Published.Channel, "acked")
		}
		if msg.Published.Offset != uint64(i+1) {
			t.Errorf("publish %d: offset = %d, want %d", i, msg.Published.Offset, i+1)
		}
		if msg.Published.Epoch == 0 {
			t.Errorf("publish %d: expected non-zero epoch", i)
		}
		if msg.Published.Delivered != 1 {
			t.Errorf("publish %d: delivered = %d, want 1", i, msg.Published.Delivered)
		}
		if msg.Published.Dropped != 0 {
			t.Errorf("publish %d: dropped = %d, want 0", i, msg.Published.Dropped)
		}
	}

	var delivery ServerMessage
	readJSON(t, sub, &delivery)
	if delivery.ChannelMessage == nil {
		t.Fatalf("expected ChannelMessage, got %+v", delivery)
	}
	if delivery.ChannelMessage.Offset != 1 {
		t.Errorf("delivery offset = %d, want 1", delivery.ChannelMessage.Offset)
	}
	if delivery.ChannelMessage.Epoch == 0 {
		t.Error("expected non-zero delivery epoch")
	}
}

func TestPublishWithoutIDGetsNoAck(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{
		"publish": map[string]any{"channel": "silent", "data": "x"},
	})
	sendJSON(t, conn, map[string]any{
		"id":   "ping-1",
		"ping": map[string]any{},
	})

	var msg ServerMessage
	readJSON(t, conn, &msg)
	if msg.Pong == nil {
		t.Fatalf("expected Pong (no ack without id), got %+v", msg)
	}
}
