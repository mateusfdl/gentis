//go:build linux

package arena

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

var (
	ErrClosed = errors.New("arena: closed")
	ErrFull   = errors.New("arena: no free slots")
)

// Arena is an mmap-backed slab allocator. All slots are the same size and
// live in a contiguous memory region that the Go GC does not scan.
// Allocation fast path: atomic bump pointer (single Add).
// Allocation slow path: mutex-guarded free list (reuses returned slots).
type Arena struct {
	base     unsafe.Pointer
	rawMem   []byte
	slotSize uintptr
	maxSlots uint32
	offset   atomic.Uint32
	free     []uint32
	inFree   []bool // membership guard, parallel to slot indices; guarded by freeMu
	freeMu   sync.Mutex
	closed   atomic.Bool
}

// New creates an arena that can hold maxSlots slots of slotSize bytes each.
// The backing memory is allocated via mmap(MAP_ANONYMOUS|MAP_PRIVATE) and
// is invisible to the Go garbage collector.
func New(slotSize, maxSlots int) (*Arena, error) {
	if slotSize <= 0 || maxSlots <= 0 {
		return nil, errors.New("arena: slotSize and maxSlots must be positive")
	}

	totalSize := uintptr(slotSize) * uintptr(maxSlots)
	mem, err := syscall.Mmap(
		-1, 0, int(totalSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANONYMOUS|syscall.MAP_PRIVATE,
	)
	if err != nil {
		return nil, err
	}

	return &Arena{
		base:     unsafe.Pointer(&mem[0]),
		rawMem:   mem,
		slotSize: uintptr(slotSize),
		maxSlots: uint32(maxSlots),
		free:     make([]uint32, 0, 64),
		inFree:   make([]bool, maxSlots),
	}, nil
}

func (a *Arena) Alloc() (unsafe.Pointer, uint32, error) {
	if a.closed.Load() {
		return nil, 0, ErrClosed
	}

	if idx, ok := a.popFree(); ok {
		return a.slotPtr(idx), idx, nil
	}

	idx := a.offset.Add(1) - 1
	if idx < a.maxSlots {
		return a.slotPtr(idx), idx, nil
	}
	a.offset.Add(^uint32(0)) // decrement rollback

	// The bump region is exhausted, but a Free may have returned a slot
	// after our first popFree and before the rollback. Re-check so a
	// concurrent release is not lost to a spurious ErrFull.
	if idx, ok := a.popFree(); ok {
		return a.slotPtr(idx), idx, nil
	}
	return nil, 0, ErrFull
}

func (a *Arena) popFree() (uint32, bool) {
	a.freeMu.Lock()
	defer a.freeMu.Unlock()
	n := len(a.free)
	if n == 0 {
		return 0, false
	}
	idx := a.free[n-1]
	a.free = a.free[:n-1]
	a.inFree[idx] = false
	return idx, true
}

func (a *Arena) Free(idx uint32) {
	if idx >= a.maxSlots {
		return
	}

	a.freeMu.Lock()
	// Reject a slot that is already on the free list (double free) or was
	// never handed out by the bump allocator: clearing and re-listing it
	// would alias a live slot to two owners.
	if a.inFree[idx] || idx >= a.offset.Load() {
		a.freeMu.Unlock()
		return
	}
	a.inFree[idx] = true
	a.freeMu.Unlock()

	ptr := a.slotPtr(idx)
	mem := unsafe.Slice((*byte)(ptr), a.slotSize)
	clear(mem)

	a.freeMu.Lock()
	a.free = append(a.free, idx)
	a.freeMu.Unlock()
}

func (a *Arena) SlotPtr(idx uint32) unsafe.Pointer {
	return a.slotPtr(idx)
}

func (a *Arena) SlotSize() uintptr {
	return a.slotSize
}

func (a *Arena) MaxSlots() uint32 {
	return a.maxSlots
}

func (a *Arena) slotPtr(idx uint32) unsafe.Pointer {
	return unsafe.Add(a.base, uintptr(idx)*a.slotSize)
}

func (a *Arena) Close() error {
	if !a.closed.CompareAndSwap(false, true) {
		return nil
	}
	return syscall.Munmap(a.rawMem)
}
