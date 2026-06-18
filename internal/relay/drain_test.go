package relay

import (
	"testing"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/ringbuf"
)

func TestDrainSendRing(t *testing.T) {
	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](4)
	if err != nil {
		t.Fatalf("NewPointer: %v", err)
	}
	sess := &Session{sendRing: ring}

	for range 3 {
		if !ring.TryProduce(getServerMsg(engine.Delivery{Channel: "ch", Data: []byte("x")})) {
			t.Fatal("TryProduce failed during setup")
		}
	}
	sess.drainSendRing()

	if _, ok := ring.TryConsume(); ok {
		t.Fatal("send ring should be empty after drain")
	}
}
