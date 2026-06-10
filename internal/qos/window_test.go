package qos

import (
	"testing"
	"time"
)

func sendOK() bool { return true }

func admitN(t *testing.T, w *Window, from, to uint64, size int, now int64) {
	t.Helper()
	for off := from; off <= to; off++ {
		if v := w.Admit(off, 7, size, now, sendOK); v != Admitted {
			t.Fatalf("Admit(%d) = %v, want Admitted", off, v)
		}
	}
}

func TestWindowAdmit(t *testing.T) {
	tests := []struct {
		name  string
		setup func(w *Window)
		off   uint64
		size  int
		want  Verdict
	}{
		{
			name:  "first admit sets baseline at any offset",
			setup: func(w *Window) {},
			off:   57,
			size:  10,
			want:  Admitted,
		},
		{
			name:  "next in order admitted",
			setup: func(w *Window) { w.Admit(1, 7, 10, 0, sendOK) },
			off:   2,
			size:  10,
			want:  Admitted,
		},
		{
			name:  "duplicate offset rejected",
			setup: func(w *Window) { w.Admit(1, 7, 10, 0, sendOK); w.Admit(2, 7, 10, 0, sendOK) },
			off:   2,
			size:  10,
			want:  Dup,
		},
		{
			name:  "gap defers to pump",
			setup: func(w *Window) { w.Admit(1, 7, 10, 0, sendOK) },
			off:   5,
			size:  10,
			want:  Full,
		},
		{
			name: "count budget exhausted",
			setup: func(w *Window) {
				w.Admit(1, 7, 10, 0, sendOK)
				w.Admit(2, 7, 10, 0, sendOK)
				w.Admit(3, 7, 10, 0, sendOK)
			},
			off:  4,
			size: 10,
			want: Full,
		},
		{
			name:  "byte budget exhausted",
			setup: func(w *Window) { w.Admit(1, 7, 90, 0, sendOK) },
			off:   2,
			size:  20,
			want:  Full,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWindow(3, 100, time.Second, 2)
			tt.setup(w)
			if got := w.Admit(tt.off, 7, tt.size, 0, sendOK); got != tt.want {
				t.Errorf("Admit(%d, size %d) = %v, want %v", tt.off, tt.size, got, tt.want)
			}
		})
	}
}

func TestWindowConfirmFreesBudget(t *testing.T) {
	w := NewWindow(2, 1000, time.Second, 2)
	admitN(t, w, 1, 2, 10, 0)

	if v := w.Admit(3, 7, 10, 0, sendOK); v != Full {
		t.Fatalf("Admit(3) with full window = %v, want Full", v)
	}

	w.Confirm(1)
	if v := w.Admit(3, 7, 10, 0, sendOK); v != Admitted {
		t.Fatalf("Admit(3) after confirming 1 = %v, want Admitted", v)
	}
}

func TestWindowCumulativeConfirm(t *testing.T) {
	w := NewWindow(10, 1000, time.Second, 2)
	admitN(t, w, 1, 5, 10, 0)

	w.Confirm(4)

	count, bytes := w.Inflight()
	if count != 1 || bytes != 10 {
		t.Fatalf("Inflight after Confirm(4) = (%d, %d), want (1, 10)", count, bytes)
	}
}

func TestWindowRedelivery(t *testing.T) {
	timeout := time.Second
	w := NewWindow(10, 1000, timeout, 2)
	admitN(t, w, 1, 3, 10, 0)

	if a := w.CheckRedelivery(int64(timeout) / 2); a.ResendFrom != 0 || a.Poisoned != 0 {
		t.Fatalf("CheckRedelivery before timeout = %+v, want none", a)
	}

	a := w.CheckRedelivery(int64(timeout) + 1)
	if a.ResendFrom != 1 || a.Poisoned != 0 {
		t.Fatalf("first redelivery = %+v, want ResendFrom 1", a)
	}

	admitN(t, w, 1, 3, 10, int64(timeout)+1)

	a = w.CheckRedelivery(2*int64(timeout) + 2)
	if a.ResendFrom != 1 || a.Poisoned != 0 {
		t.Fatalf("second redelivery = %+v, want ResendFrom 1", a)
	}

	admitN(t, w, 1, 3, 10, 2*int64(timeout)+2)

	a = w.CheckRedelivery(3*int64(timeout) + 3)
	if a.Poisoned != 1 {
		t.Fatalf("after max redeliveries = %+v, want Poisoned 1", a)
	}
	if a.ResendFrom != 2 {
		t.Fatalf("after poisoning offset 1 = %+v, want ResendFrom 2", a)
	}
}

func TestWindowConfirmResetsRedeliveryAttempts(t *testing.T) {
	timeout := time.Second
	w := NewWindow(10, 1000, timeout, 1)
	admitN(t, w, 1, 2, 10, 0)

	a := w.CheckRedelivery(int64(timeout) + 1)
	if a.ResendFrom != 1 {
		t.Fatalf("first redelivery = %+v, want ResendFrom 1", a)
	}
	admitN(t, w, 1, 2, 10, int64(timeout)+1)

	w.Confirm(1)

	a = w.CheckRedelivery(2*int64(timeout) + 2)
	if a.Poisoned != 0 {
		t.Fatalf("redelivery of fresh oldest = %+v, want no poison (attempts reset on confirm)", a)
	}
	if a.ResendFrom != 2 {
		t.Fatalf("redelivery of fresh oldest = %+v, want ResendFrom 2", a)
	}
}

func TestWindowRefusedAdmitCommitsNothing(t *testing.T) {
	w := NewWindow(10, 1000, time.Second, 2)
	admitN(t, w, 1, 1, 10, 0)

	if v := w.Admit(2, 7, 10, 0, func() bool { return false }); v != Refused {
		t.Fatalf("Admit(2) with failing send = %v, want Refused", v)
	}
	count, bytes := w.Inflight()
	if count != 1 || bytes != 10 {
		t.Fatalf("Inflight after refused admit = (%d, %d), want (1, 10)", count, bytes)
	}

	if v := w.Admit(2, 7, 10, 0, sendOK); v != Admitted {
		t.Fatalf("Admit(2) retry = %v, want Admitted", v)
	}
	count, _ = w.Inflight()
	if count != 2 {
		t.Fatalf("Inflight = %d, want 2", count)
	}
}

func TestWindowResetDropsBaseline(t *testing.T) {
	w := NewWindow(10, 1000, time.Second, 2)
	admitN(t, w, 1, 3, 10, 0)

	w.Reset()

	count, bytes := w.Inflight()
	if count != 0 || bytes != 0 {
		t.Fatalf("Inflight after Reset = (%d, %d), want (0, 0)", count, bytes)
	}
	if v := w.Admit(42, 9, 10, 0, sendOK); v != Admitted {
		t.Fatalf("Admit(42) after Reset = %v, want Admitted (re-baseline)", v)
	}
	from, epoch, _ := w.PumpPoint()
	if from != 42 || epoch != 9 {
		t.Fatalf("PumpPoint after re-baseline = (%d, %d), want (42, 9)", from, epoch)
	}
}

func TestWindowPumpPoint(t *testing.T) {
	w := NewWindow(3, 1000, time.Second, 2)
	admitN(t, w, 5, 6, 10, 0)

	from, epoch, room := w.PumpPoint()
	if from != 6 || epoch != 7 || room != 1 {
		t.Fatalf("PumpPoint = (%d, %d, %d), want (6, 7, 1)", from, epoch, room)
	}

	w.Confirm(6)
	from, _, room = w.PumpPoint()
	if from != 6 || room != 3 {
		t.Fatalf("PumpPoint after confirm = (%d, _, %d), want (6, 3)", from, room)
	}
}
