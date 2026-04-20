package relay

import (
	"testing"
	"unsafe"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

func TestDedupShardTailPadIsolatesNeighbors(t *testing.T) {
	size := unsafe.Sizeof(dedupShard{})
	hotEnd := unsafe.Offsetof(dedupShard{}.peak) + unsafe.Sizeof(dedupShard{}.peak)
	gap := size - hotEnd
	if gap < cacheline.Size {
		t.Fatalf("dedupShard inter-element gap %d is below one cache line %d; adjacent shards may false-share", gap, cacheline.Size)
	}
}
