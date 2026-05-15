package relay

import (
	"context"
	"testing"
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
			if got := sess.sendRing.Cap(); got != tc.wantCap {
				t.Fatalf("sendRing.Cap() = %d, want %d", got, tc.wantCap)
			}
		})
	}
}
