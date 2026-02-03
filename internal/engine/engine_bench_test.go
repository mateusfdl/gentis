package engine

import (
	"fmt"
	"testing"
)

func BenchmarkSubscribe(b *testing.B) {
	e := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Subscribe(SubscriberID(i), "bench-channel")
	}
}

func BenchmarkUnsubscribe(b *testing.B) {
	e := New()
	for i := 0; i < b.N; i++ {
		e.Subscribe(SubscriberID(i), "bench-channel")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Unsubscribe(SubscriberID(i), "bench-channel")
	}
}

func BenchmarkGetSubscribers(b *testing.B) {
	for _, numSubs := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			ch := newChannel("bench")
			for i := 0; i < numSubs; i++ {
				ch.Subscribe(SubscriberID(i))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ch.Subscribers()
			}
		})
	}
}

func BenchmarkPublish(b *testing.B) {
	for _, numSubs := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			e := New()
			for i := 0; i < numSubs; i++ {
				e.Subscribe(SubscriberID(i), "bench-channel")
			}

			data := []byte("benchmark message payload")
			deliver := func(id SubscriberID, ch string, d []byte) bool {
				return true
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.Publish("bench-channel", data, 0, deliver)
			}
		})
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	for _, numSubs := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			e := New()
			for i := 0; i < numSubs; i++ {
				e.Subscribe(SubscriberID(i), "bench-channel")
			}

			data := []byte("benchmark message payload")
			deliver := func(id SubscriberID, ch string, d []byte) bool {
				return true
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					e.Publish("bench-channel", data, 0, deliver)
				}
			})
		})
	}
}

func BenchmarkSubscribeParallel(b *testing.B) {
	e := New()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := SubscriberID(0)
		for pb.Next() {
			id++
			e.Subscribe(id, "bench-channel")
		}
	})
}

func BenchmarkMixedOperations(b *testing.B) {
	e := New()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := SubscriberID(0)
		for pb.Next() {
			id++
			switch id % 10 {
			case 0:
				e.Subscribe(id, "bench-channel")
			case 1:
				e.Unsubscribe(id-10, "bench-channel")
			default:
				e.Publish("bench-channel", []byte("msg"), id, func(SubscriberID, string, []byte) bool {
					return true
				})
			}
		}
	})
}

func BenchmarkChannelSubscribe(b *testing.B) {
	ch := newChannel("bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Subscribe(SubscriberID(i))
	}
}

func BenchmarkChannelUnsubscribe(b *testing.B) {
	ch := newChannel("bench")
	for i := 0; i < b.N; i++ {
		ch.Subscribe(SubscriberID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Unsubscribe(SubscriberID(i))
	}
}

func BenchmarkUnsubscribeAll(b *testing.B) {
	for _, numChannels := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("channels=%d", numChannels), func(b *testing.B) {
			e := New()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := SubscriberID(i)
				for j := 0; j < numChannels; j++ {
					e.Subscribe(id, fmt.Sprintf("channel-%d", j))
				}
				e.UnsubscribeAll(id)
			}
		})
	}
}

func BenchmarkShardDistribution(b *testing.B) {
	for _, numShards := range []int{1, 8, 32, 64} {
		b.Run(fmt.Sprintf("shards=%d", numShards), func(b *testing.B) {
			e := New(WithShards(numShards))

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := SubscriberID(0)
				for pb.Next() {
					id++
					e.Subscribe(id, fmt.Sprintf("channel-%d", id%100))
				}
			})
		})
	}
}

func BenchmarkSubscriberSlicePool(b *testing.B) {
	b.Run("with-pool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := AcquireSubscriberSlice()
			*s = append(*s, 1, 2, 3, 4, 5)
			ReleaseSubscriberSlice(s)
		}
	})

	b.Run("without-pool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := make([]SubscriberID, 0, 128)
			s = append(s, 1, 2, 3, 4, 5)
			_ = s
		}
	})
}
