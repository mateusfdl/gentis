package grpc

import (
	"testing"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/ringbuf"
)

func TestDrainSendRing(t *testing.T) {
	ring, err := ringbuf.NewPointer[gentisv1.ServerMessage](4)
	if err != nil {
		t.Fatalf("NewPointer: %v", err)
	}
	sess := &Session{sendRing: ring}

	for range 3 {
		if !ring.TryProduce(getServerMsg("ch", []byte("x"))) {
			t.Fatal("TryProduce failed during setup")
		}
	}
	if ring.Len() != 3 {
		t.Fatalf("setup Len = %d, want 3", ring.Len())
	}

	sess.drainSendRing()

	if ring.Len() != 0 {
		t.Fatalf("after drain Len = %d, want 0", ring.Len())
	}
}
