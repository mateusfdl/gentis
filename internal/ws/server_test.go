package ws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/engine"
	gentislog "github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
	"github.com/mateusfdl/gentis/internal/testcert"
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

func startVerifierTestServer(t *testing.T, secret []byte) string {
	t.Helper()

	eng := engine.New()
	store := transport.NewSessionStore()

	srv := New("127.0.0.1:0",
		WithEngine(eng),
		WithSessionStore(store),
		WithVerifier(auth.NewHMACVerifier(secret)),
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	return srv.listener.Addr().String()
}

func TestConnectRejectsInvalidToken(t *testing.T) {
	addr := startVerifierTestServer(t, []byte("ws-secret"))
	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": "garbage"},
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeNotAuthenticated {
		t.Fatalf("expected NOT_AUTHENTICATED error, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":        "s1",
		"subscribe": map[string]any{"channel": "ch"},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeNotAuthenticated {
		t.Fatalf("subscribe after failed connect: got %+v, want NOT_AUTHENTICATED", resp)
	}
}

func TestConnectAcceptsSignedToken(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	sendJSON(t, conn, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": token},
	})

	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":        "s1",
		"subscribe": map[string]any{"channel": "ch"},
	})
	readJSON(t, conn, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("expected Subscribed, got %+v", resp)
	}
}

func TestPermissionChecks(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Hour),
		Channels:  []string{"allowed-*"},
		Pub:       []string{"allowed-pub"},
	})
	sendJSON(t, conn, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": token},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":        "s1",
		"subscribe": map[string]any{"channel": "forbidden"},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodePermissionDenied {
		t.Fatalf("subscribe outside allowlist: got %+v, want PERMISSION_DENIED", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":        "s2",
		"subscribe": map[string]any{"channel": "allowed-1"},
	})
	readJSON(t, conn, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("subscribe inside allowlist: got %+v, want Subscribed", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":      "p1",
		"publish": map[string]any{"channel": "allowed-1", "data": json.RawMessage(`"x"`)},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodePermissionDenied {
		t.Fatalf("publish outside pub allowlist: got %+v, want PERMISSION_DENIED", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":      "p2",
		"publish": map[string]any{"channel": "allowed-pub", "data": json.RawMessage(`"x"`)},
	})
	readJSON(t, conn, &resp)
	if resp.Published == nil {
		t.Fatalf("publish inside pub allowlist: got %+v, want Published ack", resp)
	}
}

func TestExpiredSessionDisconnects(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	token := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(time.Second),
	})
	sendJSON(t, conn, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": token},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	for {
		if _, _, err := wsutil.ReadServerData(conn); err != nil {
			return
		}
	}
}

func TestRefreshExtendsSession(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	short := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: time.Now().Add(2 * time.Second),
	})
	sendJSON(t, conn, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": short},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	renewedExp := time.Now().Add(time.Hour)
	renewed := auth.SignHS256(secret, auth.Claims{
		Subject:   "user-1",
		ExpiresAt: renewedExp,
	})
	sendJSON(t, conn, map[string]any{
		"id":      "r1",
		"refresh": map[string]any{"auth_token": renewed},
	})
	readJSON(t, conn, &resp)
	if resp.Refreshed == nil {
		t.Fatalf("expected Refreshed, got %+v", resp)
	}
	if resp.Refreshed.ExpiresAt != uint64(renewedExp.Unix()) {
		t.Errorf("expires_at = %d, want %d", resp.Refreshed.ExpiresAt, renewedExp.Unix())
	}

	time.Sleep(2500 * time.Millisecond)

	sendJSON(t, conn, map[string]any{"id": "p1", "ping": map[string]any{}})
	readJSON(t, conn, &resp)
	if resp.Pong == nil {
		t.Fatalf("expected Pong after refresh outlived original expiry, got %+v", resp)
	}
}

func startKeepaliveTestServer(t *testing.T, interval time.Duration) (*Server, string) {
	t.Helper()

	eng := engine.New()
	store := transport.NewSessionStore()

	srv := New("127.0.0.1:0",
		WithEngine(eng),
		WithSessionStore(store),
		WithPingInterval(interval),
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	return srv, srv.listener.Addr().String()
}

func waitForConnectionCount(t *testing.T, srv *Server, want int64, deadline time.Duration) {
	t.Helper()
	stop := time.Now().Add(deadline)
	for time.Now().Before(stop) {
		if srv.ConnectionCount() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("ConnectionCount = %d, want %d after %v", srv.ConnectionCount(), want, deadline)
}

func TestKeepaliveReapsUnresponsiveClient(t *testing.T) {
	srv, addr := startKeepaliveTestServer(t, 150*time.Millisecond)

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)
	waitForConnectionCount(t, srv, 1, time.Second)

	// Stop reading entirely: pings go unanswered, the session must be
	// reaped after the missed-pong budget.
	waitForConnectionCount(t, srv, 0, 3*time.Second)
}

func TestKeepaliveKeepsResponsiveClientAlive(t *testing.T) {
	srv, addr := startKeepaliveTestServer(t, 150*time.Millisecond)

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)
	waitForConnectionCount(t, srv, 1, time.Second)

	// Reading with wsutil answers protocol pings with pongs, so the
	// session must survive well past the reap budget.
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		wsutil.ReadServerData(conn)
	}

	if got := srv.ConnectionCount(); got != 1 {
		t.Fatalf("ConnectionCount = %d, want 1 (responsive client reaped)", got)
	}

	sendJSON(t, conn, map[string]any{"id": "p1", "ping": map[string]any{}})
	var resp ServerMessage
	for resp.Pong == nil {
		readJSON(t, conn, &resp)
	}
}

func TestTLSServer(t *testing.T) {
	certFile, keyFile := testcert.Generate(t)

	eng := engine.New()
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0",
		WithEngine(eng),
		WithSessionStore(store),
		WithTLS(certFile, keyFile),
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})
	addr := srv.listener.Addr().String()

	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatalf("read cert: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		t.Fatal("bad cert pem")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dialer := ws.Dialer{
		TLSConfig: &tls.Config{RootCAs: roots},
	}
	conn, _, _, err := dialer.Dial(ctx, "wss://"+addr+"/ws")
	if err != nil {
		t.Fatalf("wss dial: %v", err)
	}
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": "t"},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected over TLS, got %+v", resp)
	}
}

func TestPublishMessageTooLarge(t *testing.T) {
	eng := engine.New()
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0",
		WithEngine(eng),
		WithSessionStore(store),
		WithMaxMessageSize(1024),
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	conn := dialWS(t, srv.listener.Addr().String())
	defer conn.Close()
	authenticate(t, conn)

	big := make([]byte, 2048)
	for i := range big {
		big[i] = 'a'
	}
	sendJSON(t, conn, map[string]any{
		"id":      "big",
		"publish": map[string]any{"channel": "ch", "data": json.RawMessage(`"` + string(big) + `"`)},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeMessageTooLarge {
		t.Fatalf("oversized publish: got %+v, want MESSAGE_TOO_LARGE", resp)
	}

	sendJSON(t, conn, map[string]any{"id": "p", "ping": map[string]any{}})
	readJSON(t, conn, &resp)
	if resp.Pong == nil {
		t.Fatalf("session should survive oversized publish, got %+v", resp)
	}
}

func TestSubscriptionLimit(t *testing.T) {
	eng := engine.New()
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0",
		WithEngine(eng),
		WithSessionStore(store),
		WithMaxSubscriptions(1),
	)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	conn := dialWS(t, srv.listener.Addr().String())
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{"id": "s0", "subscribe": map[string]any{"channel": "ch-0"}})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("first subscribe: got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{"id": "s1", "subscribe": map[string]any{"channel": "ch-1"}})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeSubscriptionLimit {
		t.Fatalf("over-limit subscribe: got %+v, want SUBSCRIPTION_LIMIT", resp)
	}
}

func TestSubscribeWithRecovery(t *testing.T) {
	eng := engine.New(engine.WithHistory(64, 0))
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0", WithEngine(eng), WithSessionStore(store))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})
	addr := srv.listener.Addr().String()

	pub := dialWS(t, addr)
	defer pub.Close()
	authenticate(t, pub)

	sub := dialWS(t, addr)
	authenticate(t, sub)
	sendJSON(t, sub, map[string]any{"id": "s", "subscribe": map[string]any{"channel": "rec-ws"}})
	var resp ServerMessage
	readJSON(t, sub, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("subscribe failed: %+v", resp)
	}

	sendJSON(t, pub, map[string]any{
		"id":      "p1",
		"publish": map[string]any{"channel": "rec-ws", "data": json.RawMessage(`"m-1"`)},
	})
	readJSON(t, sub, &resp)
	if resp.ChannelMessage == nil || resp.ChannelMessage.Offset != 1 {
		t.Fatalf("expected live delivery offset 1, got %+v", resp)
	}
	epoch := resp.ChannelMessage.Epoch
	sub.Close()

	var ack ServerMessage
	readJSON(t, pub, &ack)
	if ack.Published == nil {
		t.Fatalf("expected ack, got %+v", ack)
	}

	for i := 2; i <= 3; i++ {
		sendJSON(t, pub, map[string]any{
			"id":      fmt.Sprintf("p%d", i),
			"publish": map[string]any{"channel": "rec-ws", "data": json.RawMessage(fmt.Sprintf(`"m-%d"`, i))},
		})
		readJSON(t, pub, &ack)
		if ack.Published == nil {
			t.Fatalf("publish %d not acked: %+v", i, ack)
		}
	}

	sub2 := dialWS(t, addr)
	defer sub2.Close()
	authenticate(t, sub2)
	sendJSON(t, sub2, map[string]any{
		"id": "s2",
		"subscribe": map[string]any{
			"channel": "rec-ws",
			"recover": map[string]any{"offset": 1, "epoch": fmt.Sprintf("%d", epoch)},
		},
	})

	readJSON(t, sub2, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("expected Subscribed, got %+v", resp)
	}
	if resp.Subscribed.Recovered == nil || !*resp.Subscribed.Recovered {
		t.Fatalf("Subscribed.Recovered = %v, want true", resp.Subscribed.Recovered)
	}

	for i := 2; i <= 3; i++ {
		readJSON(t, sub2, &resp)
		cm := resp.ChannelMessage
		if cm == nil {
			t.Fatalf("expected replayed message %d, got %+v", i, resp)
		}
		if cm.Channel != "rec-ws" || cm.Offset != uint64(i) || cm.Epoch != epoch || string(cm.Data) != fmt.Sprintf(`"m-%d"`, i) {
			t.Errorf("replay %d = %+v, want offset %d epoch %d data \"m-%d\"", i, cm, i, epoch, i)
		}
	}
}

func TestSubscribeWithRecoveryUnrecoverable(t *testing.T) {
	eng := engine.New(engine.WithHistory(64, 0))
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0", WithEngine(eng), WithSessionStore(store))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	conn := dialWS(t, srv.listener.Addr().String())
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{
		"id": "s",
		"subscribe": map[string]any{
			"channel": "ghost-ch",
			"recover": map[string]any{"offset": 5, "epoch": "999"},
		},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("expected Subscribed, got %+v", resp)
	}
	if resp.Subscribed.Recovered == nil || *resp.Subscribed.Recovered {
		t.Fatalf("Subscribed.Recovered = %v, want false", resp.Subscribed.Recovered)
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
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0", WithEngine(eng), WithSessionStore(store))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	conn := dialWS(t, srv.listener.Addr().String())
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{"id": "s1", "subscribe": map[string]any{"channel": "ghost:x"}})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeChannelNotFound {
		t.Fatalf("subscribe unknown namespace: got %+v, want CHANNEL_NOT_FOUND", resp)
	}

	sendJSON(t, conn, map[string]any{"id": "s2", "subscribe": map[string]any{"channel": "feed:news"}})
	readJSON(t, conn, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("subscribe read-only namespace: got %+v, want Subscribed", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":      "p1",
		"publish": map[string]any{"channel": "feed:news", "data": json.RawMessage(`"x"`)},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodePermissionDenied {
		t.Fatalf("publish read-only namespace: got %+v, want PERMISSION_DENIED", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":      "p2",
		"publish": map[string]any{"channel": "ghost:x", "data": json.RawMessage(`"x"`)},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeChannelNotFound {
		t.Fatalf("publish unknown namespace: got %+v, want CHANNEL_NOT_FOUND", resp)
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
	})
	eng := engine.New(engine.WithNamespaces(reg))
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0", WithEngine(eng), WithSessionStore(store))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})
	addr := srv.listener.Addr().String()

	sub := dialWS(t, addr)
	defer sub.Close()
	authenticate(t, sub)
	sendJSON(t, sub, map[string]any{
		"id": "s",
		"subscribe": map[string]any{
			"channel":         "jobs:emails",
			"max_unconfirmed": map[string]any{"count": 2},
		},
	})
	var resp ServerMessage
	readJSON(t, sub, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("subscribe failed: %+v", resp)
	}

	pub := dialWS(t, addr)
	defer pub.Close()
	authenticate(t, pub)
	for i := 1; i <= 6; i++ {
		sendJSON(t, pub, map[string]any{
			"id":      fmt.Sprintf("p%d", i),
			"publish": map[string]any{"channel": "jobs:emails", "data": json.RawMessage(fmt.Sprintf(`"job-%d"`, i))},
		})
		var ack ServerMessage
		readJSON(t, pub, &ack)
		if ack.Published == nil {
			t.Fatalf("publish %d not acked: %+v", i, ack)
		}
	}

	var got []uint64
	for len(got) < 6 {
		readJSON(t, sub, &resp)
		cm := resp.ChannelMessage
		if cm == nil {
			continue
		}
		got = append(got, cm.Offset)
		sendJSON(t, sub, map[string]any{
			"confirm": map[string]any{"channel": "jobs:emails", "offset": cm.Offset},
		})
	}

	for i, off := range got {
		if off != uint64(i+1) {
			t.Fatalf("deliveries = %v, want 1..6 in order without gaps or dups", got)
		}
	}
}

func TestBatchedDelivery(t *testing.T) {
	addr, _ := startTestServer(t)

	sub := dialWS(t, addr)
	defer sub.Close()
	sendJSON(t, sub, map[string]any{
		"id":      "c1",
		"connect": map[string]any{"auth_token": "t", "protocol_version": 2},
	})
	var resp ServerMessage
	readJSON(t, sub, &resp)
	if resp.Connected == nil || resp.Connected.ProtocolVersion != 2 {
		t.Fatalf("expected protocol_version 2, got %+v", resp)
	}
	sendJSON(t, sub, map[string]any{"id": "s", "subscribe": map[string]any{"channel": "ws-burst"}})
	readJSON(t, sub, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("subscribe failed: %+v", resp)
	}

	pub := dialWS(t, addr)
	defer pub.Close()
	authenticate(t, pub)
	const total = 40
	for i := 1; i <= total; i++ {
		sendJSON(t, pub, map[string]any{
			"publish": map[string]any{"channel": "ws-burst", "data": json.RawMessage(fmt.Sprintf(`"m-%d"`, i))},
		})
	}

	received := 0
	arrays := 0
	var offsets []uint64
	for received < total {
		sub.SetReadDeadline(time.Now().Add(3 * time.Second))
		data, _, err := wsutil.ReadServerData(sub)
		if err != nil {
			t.Fatalf("read: %v (received %d)", err, received)
		}
		if len(data) > 0 && data[0] == '[' {
			var batch []ServerMessage
			if err := json.Unmarshal(data, &batch); err != nil {
				t.Fatalf("unmarshal array frame: %v", err)
			}
			if len(batch) < 2 {
				t.Fatalf("array frame with %d messages, batches must hold 2+", len(batch))
			}
			arrays++
			for _, m := range batch {
				if m.ChannelMessage == nil {
					t.Fatalf("array frame contains non-delivery: %+v", m)
				}
				offsets = append(offsets, m.ChannelMessage.Offset)
				received++
			}
			continue
		}
		var m ServerMessage
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if m.ChannelMessage != nil {
			offsets = append(offsets, m.ChannelMessage.Offset)
			received++
		}
	}

	if arrays == 0 {
		t.Fatal("burst of 40 produced zero array frames for a v2 client")
	}
	for i, off := range offsets {
		if off != uint64(i+1) {
			t.Fatalf("offsets = %v..., want 1..%d in order", offsets[:i+1], total)
		}
	}
}

func TestLegacyWSClientNeverSeesArrayFrames(t *testing.T) {
	addr, _ := startTestServer(t)

	sub := dialWS(t, addr)
	defer sub.Close()
	authenticate(t, sub)
	sendJSON(t, sub, map[string]any{"id": "s", "subscribe": map[string]any{"channel": "ws-legacy"}})
	var resp ServerMessage
	readJSON(t, sub, &resp)
	if resp.Subscribed == nil {
		t.Fatalf("subscribe failed: %+v", resp)
	}

	pub := dialWS(t, addr)
	defer pub.Close()
	authenticate(t, pub)
	const total = 20
	for i := 1; i <= total; i++ {
		sendJSON(t, pub, map[string]any{
			"publish": map[string]any{"channel": "ws-legacy", "data": json.RawMessage(`"x"`)},
		})
	}

	for received := 0; received < total; {
		sub.SetReadDeadline(time.Now().Add(3 * time.Second))
		data, _, err := wsutil.ReadServerData(sub)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(data) > 0 && data[0] == '[' {
			t.Fatal("legacy client received an array frame")
		}
		var m ServerMessage
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if m.ChannelMessage != nil {
			received++
		}
	}
}

func TestWildcardSubscribePublish(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	connA := dialWS(t, addr)
	defer connA.Close()
	authenticate(t, connA)
	subscribe(t, connA, "metrics:*")

	connB := dialWS(t, addr)
	defer connB.Close()
	authenticate(t, connB)

	sendJSON(t, connB, map[string]any{
		"id":      "pub-1",
		"publish": map[string]any{"channel": "metrics:cpu", "data": "v"},
	})

	var msg ServerMessage
	readJSON(t, connA, &msg)
	if msg.ChannelMessage == nil {
		t.Fatalf("expected ChannelMessage, got %+v", msg)
	}
	if msg.ChannelMessage.Channel != "metrics:cpu" {
		t.Fatalf("channel = %q, want metrics:cpu", msg.ChannelMessage.Channel)
	}
	if string(msg.ChannelMessage.Data) != `"v"` {
		t.Fatalf("data = %s, want \"v\"", msg.ChannelMessage.Data)
	}
}

func TestWildcardUnsubscribe(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)
	subscribe(t, conn, "metrics:*")

	sendJSON(t, conn, map[string]any{
		"id":          "unsub-1",
		"unsubscribe": map[string]any{"channel": "metrics:*"},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Unsubscribed == nil {
		t.Fatalf("expected Unsubscribed, got %+v", resp)
	}
	if resp.Unsubscribed.Channel != "metrics:*" {
		t.Fatalf("channel = %q, want metrics:*", resp.Unsubscribed.Channel)
	}

	sendJSON(t, conn, map[string]any{
		"id":          "unsub-2",
		"unsubscribe": map[string]any{"channel": "metrics:*"},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil {
		t.Fatalf("expected Error for double unsubscribe, got %+v", resp)
	}
	if resp.Error.Code != ErrorCodeNotSubscribed {
		t.Fatalf("error code = %q, want %q", resp.Error.Code, ErrorCodeNotSubscribed)
	}
}

func TestWildcardSubscribeRejectsQoS(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{
		"id": "sub-1",
		"subscribe": map[string]any{
			"channel":         "metrics:*",
			"max_unconfirmed": map[string]any{"count": 8},
		},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Error == nil {
		t.Fatalf("expected Error for pattern subscribe with qos, got %+v", resp)
	}
	if resp.Error.Code != ErrorCodeInvalidPayload {
		t.Fatalf("error code = %q, want %q", resp.Error.Code, ErrorCodeInvalidPayload)
	}
}

func TestConnectRejectsSubjectChange(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id": "c1",
		"connect": map[string]any{"auth_token": auth.SignHS256(secret, auth.Claims{
			Subject:   "user-1",
			ExpiresAt: time.Now().Add(time.Hour),
		})},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id": "c2",
		"connect": map[string]any{"auth_token": auth.SignHS256(secret, auth.Claims{
			Subject:   "user-2",
			ExpiresAt: time.Now().Add(time.Hour),
		})},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeNotAuthenticated {
		t.Fatalf("expected NOT_AUTHENTICATED on connect subject change, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id": "c3",
		"connect": map[string]any{"auth_token": auth.SignHS256(secret, auth.Claims{
			Subject:   "user-1",
			ExpiresAt: time.Now().Add(time.Hour),
		})},
	})
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("same-subject reconnect must stay idempotent, got %+v", resp)
	}
}

func TestWildcardSubscribeDeniedNamespace(t *testing.T) {
	reg := namespace.NewRegistry(namespace.Config{
		Default: namespace.Settings{AllowPublish: true},
		Namespaces: map[string]namespace.Settings{
			"metrics": {AllowPublish: true},
		},
	})
	eng := engine.New(engine.WithNamespaces(reg))
	store := transport.NewSessionStore()
	srv := New("127.0.0.1:0", WithEngine(eng), WithSessionStore(store))
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Stop()
	})

	conn := dialWS(t, srv.listener.Addr().String())
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{"id": "s1", "subscribe": map[string]any{"channel": "metrics:*"}})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodePermissionDenied {
		t.Fatalf("wildcard subscribe in denied namespace: got %+v, want PERMISSION_DENIED", resp)
	}
}

func TestDrainBatchCapsFrameBytes(t *testing.T) {
	sess := &Session{sendCh: make(chan *ServerMessage, maxBatchSize*2)}

	payload := make([]byte, 256*1024)
	first := getWSMsg(engine.Delivery{Channel: "big", Data: payload, Offset: 1, Epoch: 7})
	for i := 2; i <= maxBatchSize; i++ {
		sess.sendCh <- getWSMsg(engine.Delivery{Channel: "big", Data: payload, Offset: uint64(i), Epoch: 7})
	}

	batch, _ := drainBatch(sess, first)

	size := 0
	for _, m := range batch {
		size += len(m.ChannelMessage.Data)
	}
	if size > maxBatchBytes {
		t.Fatalf("batch carries %d payload bytes, want <= %d", size, maxBatchBytes)
	}
	if len(batch) >= maxBatchSize {
		t.Fatalf("batch packed %d messages of 256KiB each; byte budget never applied", len(batch))
	}
}

func TestWriteFrameSalvagesBatchAroundBadPayload(t *testing.T) {
	srv := &Server{
		config: &Config{WriteTimeout: time.Second},
		logger: gentislog.Nop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &Session{id: 1, ctx: ctx, cancel: cancel}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	batch := []*ServerMessage{
		getWSMsg(engine.Delivery{Channel: "c", Data: []byte(`"ok-1"`), Offset: 1, Epoch: 7}),
		getWSMsg(engine.Delivery{Channel: "c", Data: []byte("\xff not json"), Offset: 2, Epoch: 7}),
		getWSMsg(engine.Delivery{Channel: "c", Data: []byte(`"ok-3"`), Offset: 3, Epoch: 7}),
	}

	done := make(chan bool, 1)
	go func() { done <- srv.writeFrame(sess, server, batch) }()

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := wsutil.ReadServerText(client)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if ok := <-done; !ok {
		t.Fatal("writeFrame = false, want true")
	}

	var got []ServerMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("frame is not a JSON array: %v (%q)", err, data)
	}
	if len(got) != 2 {
		t.Fatalf("frame carries %d messages, want the 2 valid ones (got %q)", len(got), data)
	}
	if got[0].ChannelMessage.Offset != 1 || got[1].ChannelMessage.Offset != 3 {
		t.Fatalf("salvaged offsets = (%d, %d), want (1, 3)", got[0].ChannelMessage.Offset, got[1].ChannelMessage.Offset)
	}
}

type closeRecordingConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *closeRecordingConn) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }
func (c *closeRecordingConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *closeRecordingConn) SetWriteDeadline(time.Time) error { return nil }

func TestRunWriterClosesConnOnWriteError(t *testing.T) {
	srv := &Server{
		config: &Config{WriteTimeout: time.Second},
		logger: gentislog.Nop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &Session{id: 1, ctx: ctx, cancel: cancel, sendCh: make(chan *ServerMessage, 1)}
	sess.send(&ServerMessage{ID: "x", Pong: &PongResponse{}})

	conn := &closeRecordingConn{closed: make(chan struct{})}
	go srv.runWriter(sess, conn)

	select {
	case <-conn.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never closed after write error; fd leaks")
	}
}

func TestRefreshRejectsBadToken(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id": "c1",
		"connect": map[string]any{"auth_token": auth.SignHS256(secret, auth.Claims{
			Subject:   "user-1",
			ExpiresAt: time.Now().Add(time.Hour),
		})},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id":      "r1",
		"refresh": map[string]any{"auth_token": "garbage"},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeNotAuthenticated {
		t.Fatalf("refresh with bad token: got %+v, want NOT_AUTHENTICATED", resp)
	}
}

func TestRefreshRejectsSubjectChange(t *testing.T) {
	secret := []byte("ws-secret")
	addr := startVerifierTestServer(t, secret)
	conn := dialWS(t, addr)
	defer conn.Close()

	sendJSON(t, conn, map[string]any{
		"id": "c1",
		"connect": map[string]any{"auth_token": auth.SignHS256(secret, auth.Claims{
			Subject:   "user-1",
			ExpiresAt: time.Now().Add(time.Hour),
		})},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Connected == nil {
		t.Fatalf("expected Connected, got %+v", resp)
	}

	sendJSON(t, conn, map[string]any{
		"id": "r1",
		"refresh": map[string]any{"auth_token": auth.SignHS256(secret, auth.Claims{
			Subject:   "user-2",
			ExpiresAt: time.Now().Add(time.Hour),
		})},
	})
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeNotAuthenticated {
		t.Fatalf("refresh with subject change: got %+v, want NOT_AUTHENTICATED", resp)
	}
}

func TestWildcardSubscribeRejectsRecovery(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn := dialWS(t, addr)
	defer conn.Close()
	authenticate(t, conn)

	sendJSON(t, conn, map[string]any{
		"id": "sub-1",
		"subscribe": map[string]any{
			"channel": "metrics:*",
			"recover": map[string]any{"offset": 3, "epoch": "7"},
		},
	})
	var resp ServerMessage
	readJSON(t, conn, &resp)
	if resp.Error == nil || resp.Error.Code != ErrorCodeInvalidPayload {
		t.Fatalf("pattern subscribe with recover: got %+v, want INVALID_PAYLOAD", resp)
	}
}
