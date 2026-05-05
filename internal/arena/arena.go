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
	}, nil
}

func (a *Arena) Alloc() (unsafe.Pointer, uint32, error) {
	if a.closed.Load() {
		return nil, 0, ErrClosed
	}

	a.freeMu.Lock()
	if n := len(a.free); n > 0 {
		idx := a.free[n-1]
		a.free = a.free[:n-1]
		a.freeMu.Unlock()
		return a.slotPtr(idx), idx, nil
	}
	a.freeMu.Unlock()

	idx := a.offset.Add(1) - 1
	if idx >= a.maxSlots {
		a.offset.Add(^uint32(0)) // decrement rollback
		return nil, 0, ErrFull
	}

	return a.slotPtr(idx), idx, nil
}

func (a *Arena) Free(idx uint32) {
	if idx >= a.maxSlots {
		return
	}

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
