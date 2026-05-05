//go:build !linux

package arena

import (
	"errors"
	"unsafe"
)

var (
	ErrUnsupported = errors.New("arena: mmap not available on this platform")
	ErrClosed      = ErrUnsupported
	ErrFull        = ErrUnsupported
)

type Arena struct{}

func New(slotSize, maxSlots int) (*Arena, error)             { return nil, ErrUnsupported }
func (a *Arena) Alloc() (unsafe.Pointer, uint32, error)      { return nil, 0, ErrUnsupported }
func (a *Arena) Free(idx uint32)                             {}
func (a *Arena) SlotPtr(idx uint32) unsafe.Pointer           { return nil }
func (a *Arena) SlotSize() uintptr                           { return 0 }
func (a *Arena) MaxSlots() uint32                            { return 0 }
func (a *Arena) Close() error                                { return ErrUnsupported }
