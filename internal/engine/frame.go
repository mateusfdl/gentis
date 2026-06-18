package engine

import "sync/atomic"

// EncodedFrame memoizes the wire encoding of a single publication so a
// fan-out to N subscribers serializes the payload once instead of once per
// subscriber. The same EncodedFrame is shared across every Delivery copy of
// one publish; the first subscriber's writer to encode stores the result and
// the rest reuse the immutable bytes. Only allocated when fan-out exceeds one
// recipient, so single-subscriber paths pay nothing.
//
// Safe for concurrent use. Encoding is not strictly single-shot: a brief race
// on a cold frame may encode a few times, but every encoding is byte-identical
// and every reader is lock-free, which collapses the json encoder-pool
// contention that per-subscriber marshaling caused.
type EncodedFrame struct {
	bytes atomic.Pointer[[]byte]
}

// Load returns the shared wire bytes if they have been encoded, else ok=false.
func (f *EncodedFrame) Load() (data []byte, ok bool) {
	if p := f.bytes.Load(); p != nil {
		return *p, true
	}
	return nil, false
}

// Store publishes the wire bytes for reuse. Concurrent stores all carry
// byte-identical encodings, so whichever wins is correct. The slice must not
// be mutated after Store.
func (f *EncodedFrame) Store(data []byte) {
	f.bytes.Store(&data)
}
