package ringbuf

import (
	"errors"
	"runtime"
	"sync/atomic"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

var ErrCapacity = errors.New("ringbuf: capacity must be a power of 2 and at least 2")

const spinsBeforeYield = 16

type pointerSlot[T any] struct {
	seq atomic.Uint64
	ptr atomic.Pointer[T]
}

// PointerRing is a bounded MPSC queue: TryProduce is safe from any number
// of goroutines, TryConsume must only ever run from one goroutine at a
// time (the unsynchronized tail assumes a single consumer). Transports
// satisfy this by consuming only from the sender goroutine, including the
// post-loop drain that runs after the loop exits on the same goroutine.
type PointerRing[T any] struct {
	head atomic.Uint64
	_    [cacheline.Size - 8]byte

	tail atomic.Uint64
	_    [cacheline.Size - 8]byte

	mask  uint64
	cap   uint64
	slots []pointerSlot[T]
}

func NewPointer[T any](capacity int) (*PointerRing[T], error) {
	if capacity < 2 || capacity&(capacity-1) != 0 {
		return nil, ErrCapacity
	}

	r := &PointerRing[T]{
		mask:  uint64(capacity - 1),
		cap:   uint64(capacity),
		slots: make([]pointerSlot[T], capacity),
	}

	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}

	return r, nil
}

func (r *PointerRing[T]) TryProduce(v *T) bool {
	if v == nil {
		return false
	}

	spins := 0
	for {
		pos := r.head.Load()
		slot := &r.slots[pos&r.mask]
		seq := slot.seq.Load()

		diff := int64(seq) - int64(pos)
		if diff == 0 {
			if r.head.CompareAndSwap(pos, pos+1) {
				slot.ptr.Store(v)
				slot.seq.Store(pos + 1)
				return true
			}
		} else if diff < 0 {
			return false
		}

		spins++
		if spins >= spinsBeforeYield {
			spins = 0
			runtime.Gosched()
		}
	}
}

func (r *PointerRing[T]) TryConsume() (*T, bool) {
	pos := r.tail.Load()
	slot := &r.slots[pos&r.mask]
	seq := slot.seq.Load()

	if seq != pos+1 {
		return nil, false
	}

	v := slot.ptr.Load()
	slot.ptr.Store(nil)
	slot.seq.Store(pos + r.cap)
	r.tail.Store(pos + 1)

	return v, true
}
