package engine

import (
	"testing"
	"unsafe"

	"github.com/mateusfdl/gentis/internal/cacheline"
)

func TestShardLayoutSeparatesReadPathFromCounters(t *testing.T) {
	size := unsafe.Sizeof(Shard{})
	if size%cacheline.Size != 0 {
		t.Fatalf("Shard size %d is not a multiple of cache line %d", size, cacheline.Size)
	}

	readEnd := unsafe.Offsetof(Shard{}.peak) + unsafe.Sizeof(Shard{}.peak)
	if readEnd > cacheline.Size {
		t.Fatalf("read-path fields end at offset %d, spilling past the first cache line %d", readEnd, cacheline.Size)
	}

	counters := unsafe.Offsetof(Shard{}.publishCount)
	if counters%cacheline.Size != 0 {
		t.Fatalf("counters start at offset %d, not a cache-line boundary %d", counters, cacheline.Size)
	}
}

func TestSubShardTailPadIsolatesNeighbors(t *testing.T) {
	size := unsafe.Sizeof(subShard{})
	hotEnd := unsafe.Offsetof(subShard{}.peak) + unsafe.Sizeof(subShard{}.peak)
	gap := size - hotEnd
	if gap < cacheline.Size {
		t.Fatalf("subShard inter-element gap %d is below one cache line %d; adjacent shards may false-share", gap, cacheline.Size)
	}
}

func TestFanoutResultIsCacheLineSized(t *testing.T) {
	size := unsafe.Sizeof(fanoutResult{})
	if size%cacheline.Size != 0 {
		t.Fatalf("fanoutResult size %d is not a multiple of cache line %d; adjacent worker slots may false-share", size, cacheline.Size)
	}
}
