//go:build linux

package arena

import (
	"testing"
	"unsafe"
)

const benchArenaSlots = 8192

func BenchmarkArenaAlloc(b *testing.B) {
	slotSize := int(unsafe.Sizeof(SessionSlot{}))
	a, err := New(slotSize, benchArenaSlots)
	if err != nil {
		b.Fatal(err)
	}
	defer a.Close()

	indices := make([]uint32, benchArenaSlots)
	for i := range indices {
		_, idx, _ := a.Alloc()
		indices[i] = idx
	}
	for _, idx := range indices {
		a.Free(idx)
	}

	b.ResetTimer()
	for b.Loop() {
		_, idx, _ := a.Alloc()
		a.Free(idx)
	}
}

func BenchmarkArenaAllocFree(b *testing.B) {
	slotSize := int(unsafe.Sizeof(SessionSlot{}))
	a, err := New(slotSize, benchArenaSlots)
	if err != nil {
		b.Fatal(err)
	}
	defer a.Close()

	indices := make([]uint32, benchArenaSlots)
	for i := range indices {
		_, idx, _ := a.Alloc()
		indices[i] = idx
	}
	for _, idx := range indices {
		a.Free(idx)
	}

	b.ResetTimer()
	for b.Loop() {
		_, idx, _ := a.Alloc()
		a.Free(idx)
	}
}

func BenchmarkArenaAllocConcurrent(b *testing.B) {
	slotSize := int(unsafe.Sizeof(SessionSlot{}))
	a, err := New(slotSize, benchArenaSlots)
	if err != nil {
		b.Fatal(err)
	}
	defer a.Close()

	indices := make([]uint32, benchArenaSlots)
	for i := range indices {
		_, idx, _ := a.Alloc()
		indices[i] = idx
	}
	for _, idx := range indices {
		a.Free(idx)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, idx, err := a.Alloc()
			if err == nil {
				a.Free(idx)
			}
		}
	})
}

func BenchmarkSessionSlotAddSubscription(b *testing.B) {
	var s SessionSlot
	b.ResetTimer()
	for b.Loop() {
		s.SubCount = 0
		s.AddSubscription("benchmark-channel")
	}
}

func BenchmarkSessionSlotIsSubscribed(b *testing.B) {
	var s SessionSlot
	for i := range 8 {
		s.AddSubscription("chan-" + string(rune('a'+i)))
	}

	b.ResetTimer()
	for b.Loop() {
		s.IsSubscribed("chan-d")
	}
}
