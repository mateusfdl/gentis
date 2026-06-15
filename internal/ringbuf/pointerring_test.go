package ringbuf

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewPointer(t *testing.T) {
	r, err := NewPointer[int](8)
	if err != nil {
		t.Fatalf("NewPointer(8): %v", err)
	}
	if r.Cap() != 8 {
		t.Fatalf("Cap() = %d, want 8", r.Cap())
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
}

func TestNewPointerInvalid(t *testing.T) {
	for _, cap := range []int{0, -1, 1, 3, 5, 7, 10} {
		_, err := NewPointer[int](cap)
		if err != ErrCapacity {
			t.Fatalf("NewPointer(%d) err = %v, want ErrCapacity", cap, err)
		}
	}
}

func TestPointerRingSingleProducerConsumer(t *testing.T) {
	r, _ := NewPointer[int](8)
	v := 42

	if !r.TryProduce(&v) {
		t.Fatal("TryProduce failed")
	}

	got, ok := r.TryConsume()
	if !ok {
		t.Fatal("TryConsume returned false")
	}
	if *got != v {
		t.Fatalf("got %d, want %d", *got, v)
	}
}

func TestPointerRingRejectsNil(t *testing.T) {
	r, _ := NewPointer[int](4)
	if r.TryProduce(nil) {
		t.Fatal("TryProduce should reject nil")
	}
}

func TestPointerRingFull(t *testing.T) {
	r, _ := NewPointer[int](4)
	values := []int{1, 2, 3, 4, 5}

	for i := range 4 {
		if !r.TryProduce(&values[i]) {
			t.Fatal("TryProduce failed before ring is full")
		}
	}

	if r.TryProduce(&values[4]) {
		t.Fatal("TryProduce should return false on full ring")
	}
}

func TestPointerRingEmpty(t *testing.T) {
	r, _ := NewPointer[int](4)

	_, ok := r.TryConsume()
	if ok {
		t.Fatal("TryConsume should return false on empty ring")
	}
}

func TestPointerRingWraparound(t *testing.T) {
	r, _ := NewPointer[int](8)

	for i := range 1000 {
		v := i
		if !r.TryProduce(&v) {
			t.Fatalf("TryProduce failed at i=%d", i)
		}

		got, ok := r.TryConsume()
		if !ok {
			t.Fatalf("TryConsume failed at i=%d", i)
		}
		if *got != i {
			t.Fatalf("i=%d: got %d", i, *got)
		}
	}
}

func TestPointerRingClearsConsumedSlot(t *testing.T) {
	r, _ := NewPointer[int](2)
	v := 42

	if !r.TryProduce(&v) {
		t.Fatal("TryProduce failed")
	}
	if _, ok := r.TryConsume(); !ok {
		t.Fatal("TryConsume returned false")
	}
	if got := r.slots[0].ptr.Load(); got != nil {
		t.Fatalf("slot retained pointer %v, want nil", *got)
	}
}

func TestPointerRingLenRaceFree(t *testing.T) {
	const n = 10000
	r, _ := NewPointer[int](1024)
	done := make(chan struct{})

	go func() {
		for i := range n {
			v := i
			for !r.TryProduce(&v) {
				runtime.Gosched()
			}
		}
	}()

	go func() {
		consumed := 0
		for consumed < n {
			if _, ok := r.TryConsume(); ok {
				consumed++
			} else {
				runtime.Gosched()
			}
		}
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			_ = r.Len()
		}
	}
}

func TestPointerRingMPSC(t *testing.T) {
	const (
		producers   = 100
		msgsPerProd = 100
	)

	r, _ := NewPointer[int](1024)
	var wg sync.WaitGroup
	var produced atomic.Int64

	wg.Add(producers)
	for p := range producers {
		go func(id int) {
			defer wg.Done()
			for i := range msgsPerProd {
				v := new(int)
				*v = id*msgsPerProd + i
				for !r.TryProduce(v) {
					runtime.Gosched()
				}
				produced.Add(1)
			}
		}(p)
	}

	total := producers * msgsPerProd
	consumed := 0
	done := make(chan struct{})

	go func() {
		for consumed < total {
			if _, ok := r.TryConsume(); ok {
				consumed++
			} else {
				runtime.Gosched()
			}
		}
		close(done)
	}()

	wg.Wait()
	<-done

	if consumed != total {
		t.Fatalf("consumed = %d, want %d", consumed, total)
	}
}
