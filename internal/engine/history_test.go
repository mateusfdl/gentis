package engine

import (
	"fmt"
	"testing"
	"time"
)

func appendN(h *history, from, to uint64, storedAt int64) {
	for off := from; off <= to; off++ {
		h.append(off, []byte(fmt.Sprintf("msg-%d", off)), storedAt)
	}
}

func TestHistoryReplay(t *testing.T) {
	tests := []struct {
		name        string
		capacity    int
		setup       func(h *history)
		fromOffset  uint64
		wantOffsets []uint64
		wantOK      bool
	}{
		{
			name:        "replay all from zero",
			capacity:    8,
			setup:       func(h *history) { appendN(h, 1, 5, 100) },
			fromOffset:  0,
			wantOffsets: []uint64{1, 2, 3, 4, 5},
			wantOK:      true,
		},
		{
			name:        "replay tail after partial consumption",
			capacity:    8,
			setup:       func(h *history) { appendN(h, 1, 5, 100) },
			fromOffset:  3,
			wantOffsets: []uint64{4, 5},
			wantOK:      true,
		},
		{
			name:        "up to date client gets empty ok",
			capacity:    8,
			setup:       func(h *history) { appendN(h, 1, 5, 100) },
			fromOffset:  5,
			wantOffsets: nil,
			wantOK:      true,
		},
		{
			name:        "client ahead of channel is unrecoverable",
			capacity:    8,
			setup:       func(h *history) { appendN(h, 1, 5, 100) },
			fromOffset:  7,
			wantOffsets: nil,
			wantOK:      false,
		},
		{
			name:        "empty ring with no publishes is up to date",
			capacity:    8,
			setup:       func(h *history) {},
			fromOffset:  0,
			wantOffsets: nil,
			wantOK:      true,
		},
		{
			name:        "wrap keeps newest entries recoverable",
			capacity:    4,
			setup:       func(h *history) { appendN(h, 1, 6, 100) },
			fromOffset:  2,
			wantOffsets: []uint64{3, 4, 5, 6},
			wantOK:      true,
		},
		{
			name:       "wrap evicts oldest entries",
			capacity:   4,
			setup:      func(h *history) { appendN(h, 1, 6, 100) },
			fromOffset: 1,
			wantOK:     false,
		},
		{
			name:     "swept gap is unrecoverable",
			capacity: 8,
			setup: func(h *history) {
				appendN(h, 1, 3, 100)
				h.sweep(100 + int64(time.Minute) + 1)
				appendN(h, 4, 5, 100+int64(2*time.Minute))
			},
			fromOffset:  1,
			wantOffsets: nil,
			wantOK:      false,
		},
		{
			name:     "sweep keeps contiguous tail recoverable",
			capacity: 8,
			setup: func(h *history) {
				appendN(h, 1, 3, 100)
				h.sweep(100 + int64(time.Minute) + 1)
				appendN(h, 4, 5, 100+int64(2*time.Minute))
			},
			fromOffset:  3,
			wantOffsets: []uint64{4, 5},
			wantOK:      true,
		},
		{
			name:     "fully swept ring is up to date at last offset",
			capacity: 8,
			setup: func(h *history) {
				appendN(h, 1, 5, 100)
				h.sweep(100 + int64(time.Minute) + 1)
			},
			fromOffset:  5,
			wantOffsets: nil,
			wantOK:      true,
		},
		{
			name:     "fully swept ring is unrecoverable behind last offset",
			capacity: 8,
			setup: func(h *history) {
				appendN(h, 1, 5, 100)
				h.sweep(100 + int64(time.Minute) + 1)
			},
			fromOffset: 4,
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHistory(tt.capacity, time.Minute)
			tt.setup(h)

			items, ok := h.replay(tt.fromOffset)
			if ok != tt.wantOK {
				t.Fatalf("replay(%d) ok = %v, want %v", tt.fromOffset, ok, tt.wantOK)
			}
			if len(items) != len(tt.wantOffsets) {
				t.Fatalf("replay(%d) returned %d items, want %d", tt.fromOffset, len(items), len(tt.wantOffsets))
			}
			for i, item := range items {
				if item.offset != tt.wantOffsets[i] {
					t.Errorf("item[%d].offset = %d, want %d", i, item.offset, tt.wantOffsets[i])
				}
				want := fmt.Sprintf("msg-%d", tt.wantOffsets[i])
				if string(item.data) != want {
					t.Errorf("item[%d].data = %q, want %q", i, item.data, want)
				}
			}
		})
	}
}

func TestHistoryStoresSameBackingSlice(t *testing.T) {
	h := newHistory(4, 0)
	payload := []byte("zero-copy")
	h.append(1, payload, 100)

	items, ok := h.replay(0)
	if !ok || len(items) != 1 {
		t.Fatalf("replay = %d items ok=%v, want 1 item ok=true", len(items), ok)
	}
	if &items[0].data[0] != &payload[0] {
		t.Error("history copied the payload; it must store the same backing slice")
	}
}

func TestHistorySweepWithoutTTLKeepsEverything(t *testing.T) {
	h := newHistory(4, 0)
	appendN(h, 1, 3, 100)
	h.sweep(int64(time.Hour))

	items, ok := h.replay(0)
	if !ok || len(items) != 3 {
		t.Fatalf("replay = %d items ok=%v, want 3 items ok=true", len(items), ok)
	}
}
