package ringbuf

import (
	"testing"
	"unsafe"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

func TestPointerRingCursorsOnSeparateCacheLines(t *testing.T) {
	headToTail := unsafe.Offsetof(PointerRing[int]{}.tail) - unsafe.Offsetof(PointerRing[int]{}.head)
	if headToTail < cacheline.Size {
		t.Fatalf("PointerRing head/tail offset gap %d is below one cache line %d", headToTail, cacheline.Size)
	}
}
