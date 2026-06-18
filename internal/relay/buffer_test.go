package relay

import (
	"context"
	"testing"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
)

func TestSendRingHonorsBufferSize(t *testing.T) {
	cases := []struct {
		name       string
		bufferSize int
		wantCap    int
	}{
		{"exact power of two", 1024, 1024},
		{"rounds up to next power of two", 300, 512},
		{"default", 256, 256},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(WithBufferSize(tc.bufferSize))
			sess := s.createSession(context.Background())
			if sess == nil {
				t.Fatal("createSession returned nil")
			}
			msg := gentisv1.ServerMessage{}
			for range tc.wantCap {
				if !sess.sendRing.TryProduce(&msg) {
					t.Fatalf("TryProduce failed before expected capacity %d", tc.wantCap)
				}
			}
			if sess.sendRing.TryProduce(&msg) {
				t.Fatalf("TryProduce succeeded beyond expected capacity %d", tc.wantCap)
			}
		})
	}
}
