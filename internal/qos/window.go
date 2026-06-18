// Package qos implements at-least-once delivery on top of the engine's
// per-channel history: a credit window of unconfirmed deliveries per
// subscription, cumulative confirms, and timed redelivery with a poison
// cap. QoS0 subscriptions never touch any of this.
package qos

import (
	"sync"
	"time"
)

type Verdict int

const (
	// Admitted: deliver now; the window has room and the offset is next
	// in order.
	Admitted Verdict = iota
	// Full: do not deliver now. Either the credit window is exhausted or
	// the offset is ahead of the consumer's cursor; the pump will fetch
	// it from history once confirms free the window.
	Full
	// Dup: the offset was already delivered; drop silently.
	Dup
	// Refused: the offset was next in order but the transport could not
	// enqueue it. Nothing was committed; the pump retries from history.
	Refused
)

// RedeliveryAction tells the caller what to do after a redelivery check.
// ResendFrom is the offset to pump from when non-zero. Poisoned is the
// offset dropped after exhausting its redelivery budget, zero when none.
type RedeliveryAction struct {
	ResendFrom uint64
	Poisoned   uint64
}

// Window tracks the unconfirmed deliveries of one subscription. All
// methods are safe for concurrent use; the lock is per-subscription so
// publishers only contend with that subscriber's own confirms.
type Window struct {
	mu sync.Mutex

	maxCount        int
	maxBytes        int64
	timeout         time.Duration
	maxRedeliveries int

	epoch     uint64
	baselined bool
	delivered uint64
	confirmed uint64

	inflight      []entry
	inflightBytes int64

	attempts       int
	attemptsOffset uint64
	oldestAt       int64
}

type entry struct {
	offset uint64
	size   int64
}

func NewWindow(maxCount int, maxBytes int64, timeout time.Duration, maxRedeliveries int) *Window {
	return &Window{
		maxCount:        maxCount,
		maxBytes:        maxBytes,
		timeout:         timeout,
		maxRedeliveries: maxRedeliveries,
	}
}

// Admit runs send while holding the window lock, so admission order is
// enqueue order across the live path, the confirm pump, and the redelivery
// pump. The window state only commits after send succeeds: a cumulative
// confirm can never cover an offset that did not reach the transport.
//
// A delivery whose epoch differs from the baselined one is a recreated
// channel: offsets restarted, so the cursor is re-baselined to this delivery
// and the stale inflight tail is dropped, instead of misreading the new
// stream as duplicates of the old epoch.
func (w *Window) Admit(offset, epoch uint64, size int, now int64, send func() bool) Verdict {
	w.mu.Lock()
	defer w.mu.Unlock()

	if offset == 0 {
		return Dup
	}

	if !w.baselined {
		w.baselined = true
		w.epoch = epoch
		w.delivered = offset - 1
		w.confirmed = offset - 1
	} else if epoch != w.epoch {
		w.epoch = epoch
		w.delivered = offset - 1
		w.confirmed = offset - 1
		w.inflight = w.inflight[:0]
		w.inflightBytes = 0
		w.attempts = 0
		w.attemptsOffset = 0
	}

	if offset <= w.delivered {
		return Dup
	}
	if offset != w.delivered+1 {
		return Full
	}
	if w.maxCount > 0 && len(w.inflight) >= w.maxCount {
		return Full
	}
	if w.maxBytes > 0 && len(w.inflight) > 0 && w.inflightBytes+int64(size) > w.maxBytes {
		return Full
	}

	if !send() {
		return Refused
	}

	if len(w.inflight) == 0 {
		w.oldestAt = now
	}
	w.inflight = append(w.inflight, entry{offset: offset, size: int64(size)})
	w.inflightBytes += int64(size)
	w.delivered = offset
	return Admitted
}

// Baseline pins the window cursor to a recover point before any delivery
// flows, so replay and live fanout serialize against it instead of racing
// to set the baseline. No-op once the window is baselined.
func (w *Window) Baseline(offset, epoch uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.baselined {
		return
	}
	w.baselined = true
	w.epoch = epoch
	w.delivered = offset
	w.confirmed = offset
}

// Reset drops the baseline after an unrecoverable gap so the next live
// delivery re-baselines the window and the subscription keeps flowing.
func (w *Window) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.baselined = false
	w.epoch = 0
	w.delivered = 0
	w.confirmed = 0
	w.inflight = w.inflight[:0]
	w.inflightBytes = 0
	w.attempts = 0
	w.attemptsOffset = 0
}

// Confirm applies a cumulative confirm: everything up to and including
// offset is acknowledged. Trimming the head restamps the redelivery clock:
// the remaining tail has only been the oldest unconfirmed work since now,
// so a healthy pipelined consumer never trips the timeout.
func (w *Window) Confirm(offset uint64, now int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if offset > w.delivered {
		offset = w.delivered
	}
	if offset <= w.confirmed {
		return
	}
	w.confirmed = offset

	trimmed := 0
	for _, e := range w.inflight {
		if e.offset > offset {
			break
		}
		w.inflightBytes -= e.size
		trimmed++
	}
	w.inflight = w.inflight[trimmed:]
	if trimmed > 0 {
		w.oldestAt = now
	}
}

// CheckRedelivery inspects the oldest unconfirmed delivery. When it has
// been waiting past the timeout the whole unconfirmed range is scheduled
// for resend; when its redelivery budget is spent it is dropped as poison
// and the rest is resent.
func (w *Window) CheckRedelivery(now int64) RedeliveryAction {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.inflight) == 0 || w.timeout <= 0 {
		return RedeliveryAction{}
	}
	if now-w.oldestAt < int64(w.timeout) {
		return RedeliveryAction{}
	}

	oldest := w.inflight[0].offset
	if w.attemptsOffset != oldest {
		w.attemptsOffset = oldest
		w.attempts = 0
	}

	var action RedeliveryAction
	if w.attempts >= w.maxRedeliveries {
		action.Poisoned = oldest
		w.confirmed = oldest
		w.attempts = 0
		w.attemptsOffset = 0
	} else {
		w.attempts++
	}

	w.inflight = w.inflight[:0]
	w.inflightBytes = 0
	w.delivered = w.confirmed
	w.oldestAt = now
	action.ResendFrom = w.confirmed + 1
	return action
}

// PumpPoint snapshots where catch-up delivery should resume: the last
// delivered offset, the epoch the window is pinned to, and how many more
// deliveries the credit window admits.
func (w *Window) PumpPoint() (from, epoch uint64, room int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.baselined {
		return 0, 0, 0
	}
	room = w.maxCount - len(w.inflight)
	if w.maxCount == 0 {
		room = 1 << 20
	}
	return w.delivered, w.epoch, room
}
