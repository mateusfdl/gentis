//go:build linux

package arena

import (
	"sync"
	"testing"
	"unsafe"
)

const testSlotSize = 256

func TestNewArena(t *testing.T) {
	a, err := New(testSlotSize, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
}

func TestAllocValid(t *testing.T) {
	a, _ := New(testSlotSize, 100)
	defer a.Close()

	ptr, idx, err := a.Alloc()
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	if ptr == nil {
		t.Fatal("Alloc returned nil pointer")
	}
	if idx != 0 {
		t.Fatalf("first alloc idx = %d, want 0", idx)
	}

	// verify pointer is within arena bounds
	base := uintptr(a.base)
	got := uintptr(ptr)
	end := base + uintptr(a.maxSlots)*a.slotSize
	if got < base || got >= end {
		t.Fatalf("pointer %x outside arena [%x, %x)", got, base, end)
	}
}

func TestAllocFills(t *testing.T) {
	const max = 10
	a, _ := New(testSlotSize, max)
	defer a.Close()

	for i := range max {
		_, idx, err := a.Alloc()
		if err != nil {
			t.Fatalf("Alloc %d: %v", i, err)
		}
		if idx != uint32(i) {
			t.Fatalf("idx = %d, want %d", idx, i)
		}
	}

	_, _, err := a.Alloc()
	if err != ErrFull {
		t.Fatalf("err = %v, want ErrFull", err)
	}
}

func TestFreeRealloc(t *testing.T) {
	a, _ := New(testSlotSize, 4)
	defer a.Close()

	_, idx0, _ := a.Alloc()
	_, idx1, _ := a.Alloc()

	a.Free(idx0)

	_, idx2, err := a.Alloc()
	if err != nil {
		t.Fatalf("Alloc after free: %v", err)
	}
	if idx2 != idx0 {
		t.Fatalf("realloc idx = %d, want %d (freed slot)", idx2, idx0)
	}
	_ = idx1
}

func TestFreeZeros(t *testing.T) {
	a, _ := New(testSlotSize, 4)
	defer a.Close()

	ptr, idx, _ := a.Alloc()

	mem := unsafe.Slice((*byte)(ptr), testSlotSize)
	for i := range mem {
		mem[i] = 0xAA
	}

	a.Free(idx)

	ptr2, _, _ := a.Alloc()
	mem2 := unsafe.Slice((*byte)(ptr2), testSlotSize)
	for i, b := range mem2 {
		if b != 0 {
			t.Fatalf("byte[%d] = 0x%02X, want 0x00", i, b)
		}
	}
}

func TestConcurrentAlloc(t *testing.T) {
	const (
		maxSlots   = 1000
		goroutines = 8
		perG       = maxSlots / goroutines
	)

	a, _ := New(testSlotSize, maxSlots)
	defer a.Close()

	var mu sync.Mutex
	seen := make(map[uint32]bool, maxSlots)
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perG {
				_, idx, err := a.Alloc()
				if err != nil {
					return // arena full
				}
				mu.Lock()
				if seen[idx] {
					t.Errorf("duplicate index: %d", idx)
				}
				seen[idx] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if len(seen) != maxSlots {
		t.Fatalf("unique indices = %d, want %d", len(seen), maxSlots)
	}
}

func TestSlotPtr(t *testing.T) {
	a, _ := New(testSlotSize, 4)
	defer a.Close()

	ptr0, idx0, _ := a.Alloc()
	ptr1, idx1, _ := a.Alloc()

	if a.SlotPtr(idx0) != ptr0 {
		t.Fatal("SlotPtr(idx0) != original alloc pointer")
	}
	if a.SlotPtr(idx1) != ptr1 {
		t.Fatal("SlotPtr(idx1) != original alloc pointer")
	}
}

func TestClose(t *testing.T) {
	a, _ := New(testSlotSize, 100)

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}
}

func TestAllocAfterClose(t *testing.T) {
	a, _ := New(testSlotSize, 4)
	a.Close()

	_, _, err := a.Alloc()
	if err != ErrClosed {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestSessionSlotInArena(t *testing.T) {
	slotSize := int(unsafe.Sizeof(SessionSlot{}))
	a, err := New(slotSize, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()

	ptr, _, err := a.Alloc()
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	s := (*SessionSlot)(ptr)
	s.ID = 42
	s.SetSubject("arena-subject")
	s.AddSubscription("test-channel")

	if s.ID != 42 {
		t.Fatalf("ID = %d, want 42", s.ID)
	}
	if s.GetSubject() != "arena-subject" {
		t.Fatalf("subject = %q, want %q", s.GetSubject(), "arena-subject")
	}
	if !s.IsSubscribed("test-channel") {
		t.Fatal("not subscribed to test-channel")
	}
}
