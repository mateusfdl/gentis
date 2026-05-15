package relay

import (
	"context"
	"testing"
	"time"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/ringbuf"
)

func newParkSession(t *testing.T, capacity int) *Session {
	t.Helper()
	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](capacity)
	if err != nil {
		t.Fatalf("NewPointer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Session{
		sendRing: ring,
		drainCh:  make(chan struct{}, 1),
		wakeCh:   make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func ctrlMsg() *gentisv1.ServerMessage {
	return &gentisv1.ServerMessage{Id: "ctrl"}
}

func TestSendParksUntilDrain(t *testing.T) {
	s := newParkSession(t, 2)

	s.send(ctrlMsg())
	s.send(ctrlMsg())

	done := make(chan struct{})
	go func() {
		s.send(ctrlMsg())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("send returned while ring full; expected it to park")
	case <-time.After(50 * time.Millisecond):
	}

	if _, ok := s.sendRing.TryConsume(); !ok {
		t.Fatal("expected to consume a buffered message")
	}
	s.signalDrain()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send did not unpark after a slot was drained")
	}
}

func TestSendUnblocksOnContextCancel(t *testing.T) {
	s := newParkSession(t, 2)

	s.send(ctrlMsg())
	s.send(ctrlMsg())

	done := make(chan struct{})
	go func() {
		s.send(ctrlMsg())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("send returned while ring full; expected it to park")
	case <-time.After(50 * time.Millisecond):
	}

	s.cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send did not unblock after context cancel")
	}
}
