package transport_test

import (
	"fmt"
	"testing"

	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

// BenchmarkSessionStoreRegister isolates Register cost: legacy sync.Map
// vs flat-array with hits in-range. Expected: flat is O(1) with zero
// allocation; legacy allocates an entry per Register.
func BenchmarkSessionStoreRegister(b *testing.B) {
	sender := &microSender{}

	b.Run("legacy-map", func(b *testing.B) {
		s := transport.NewSessionStore()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.Register(engine.SubscriberID(i), sender)
		}
	})

	b.Run("flat-array", func(b *testing.B) {
		// Capacity must match b.N to keep every Register in the flat path.
		b.StopTimer()
		s := transport.NewFlatSessionStore(0, b.N+1)
		b.StartTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.Register(engine.SubscriberID(i), sender)
		}
	})
}

// BenchmarkSessionStoreDeliver measures the hot-path lookup cost: flat
// array should be strictly faster than sync.Map.Load by avoiding the
// interface-boxed hash lookup.
func BenchmarkSessionStoreDeliver(b *testing.B) {
	const N = 10_000
	sender := &microSender{}

	b.Run("legacy-map", func(b *testing.B) {
		s := transport.NewSessionStore()
		for i := 0; i < N; i++ {
			s.Register(engine.SubscriberID(i), sender)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.Deliver(engine.SubscriberID(i%N), engine.Delivery{Channel: "ch"})
		}
	})

	b.Run("flat-array", func(b *testing.B) {
		s := transport.NewFlatSessionStore(0, N)
		for i := 0; i < N; i++ {
			s.Register(engine.SubscriberID(i), sender)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.Deliver(engine.SubscriberID(i%N), engine.Delivery{Channel: "ch"})
		}
	})
}

// BenchmarkSessionStoreRegisterDeliverUnregister measures the full
// session-lifecycle roundtrip, parametrized by population size. Captures
// the net benefit across Register+Deliver+Unregister.
func BenchmarkSessionStoreRegisterDeliverUnregister(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("N=%d/legacy-map", n), func(b *testing.B) {
			s := transport.NewSessionStore()
			sender := &microSender{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := engine.SubscriberID(i % n)
				s.Register(id, sender)
				s.Deliver(id, engine.Delivery{Channel: "ch", Data: nil})
				s.Unregister(id)
			}
		})
		b.Run(fmt.Sprintf("N=%d/flat-array", n), func(b *testing.B) {
			s := transport.NewFlatSessionStore(0, n)
			sender := &microSender{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := engine.SubscriberID(i % n)
				s.Register(id, sender)
				s.Deliver(id, engine.Delivery{Channel: "ch", Data: nil})
				s.Unregister(id)
			}
		})
	}
}

// microSender is a zero-state Sender for micro-benchmarking — no atomic
// counter, no deliverable message building. Just satisfies the
// interface so we can measure pure store overhead.
type microSender struct{}

func (microSender) DeliverMessage(_ engine.Delivery) bool { return true }
