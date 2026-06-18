package engine

import (
	"sync"
	"testing"
)

func TestEncodedFrameLoadMissBeforeStore(t *testing.T) {
	frame := &EncodedFrame{}
	if b, ok := frame.Load(); ok {
		t.Fatalf("Load on fresh frame = (%q, true), want miss", b)
	}
}

func TestEncodedFrameConcurrentStoreConverges(t *testing.T) {
	frame := &EncodedFrame{}

	const writers = 64
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := range writers {
		go func() {
			defer wg.Done()
			if b, ok := frame.Load(); !ok {
				frame.Store([]byte{byte(i)})
			} else {
				_ = b
			}
		}()
	}
	wg.Wait()

	first, ok := frame.Load()
	if !ok {
		t.Fatal("no value stored after concurrent writers")
	}
	for range 8 {
		b, _ := frame.Load()
		if string(b) != string(first) {
			t.Fatalf("Load returned %q then %q; winner not stable", first, b)
		}
	}
}
