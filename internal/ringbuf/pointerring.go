package ringbuf

import (
	"errors"
	"runtime"
	"sync/atomic"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

var ErrCapacity = errors.New("ringbuf: capacity must be a positive power of 2")

type pointerSlot[T any] struct {
	seq atomic.Uint64
	ptr atomic.Pointer[T]
}

type PointerRing[T any] struct {
	head atomic.Uint64
	_    [cacheline.Size - 8]byte

	tail uint64
	_    [cacheline.Size - 8]byte

	mask  uint64
	cap   uint64
	slots []pointerSlot[T]
}

func NewPointer[T any](capacity int) (*PointerRing[T], error) {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
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
		runtime.Gosched()
	}
}

func (r *PointerRing[T]) TryConsume() (*T, bool) {
	pos := r.tail
	slot := &r.slots[pos&r.mask]
	seq := slot.seq.Load()

	if seq != pos+1 {
		return nil, false
	}

	v := slot.ptr.Load()
	slot.ptr.Store(nil)
	slot.seq.Store(pos + r.cap)
	r.tail = pos + 1

	return v, true
}

func (r *PointerRing[T]) Len() int {
	head := r.head.Load()
	return int(head - r.tail)
}

func (r *PointerRing[T]) Cap() int {
	return int(r.cap)
}
