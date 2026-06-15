package engine

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/mateusfdl/gentis/internal/namespace"
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
			ch := NewChannel("bench")
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
			deliver := func(id SubscriberID, d Delivery) bool {
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
			deliver := func(id SubscriberID, d Delivery) bool {
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

func BenchmarkPublishParallelWithPatterns(b *testing.B) {
	for _, numPatterns := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("patterns=%d", numPatterns), func(b *testing.B) {
			e := New()
			defer e.Stop()
			for i := 0; i < 10; i++ {
				e.Subscribe(SubscriberID(i), "bench:hot")
			}
			for i := 0; i < numPatterns; i++ {
				e.SubscribePattern(SubscriberID(1000+i), fmt.Sprintf("bench:p%d*", i))
			}

			data := []byte("benchmark message payload")
			deliver := func(id SubscriberID, d Delivery) bool {
				return true
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					e.Publish("bench:hot", data, 0, deliver)
				}
			})
		})
	}
}

func BenchmarkPublishParallelPatternsMultiChannel(b *testing.B) {
	e := New()
	defer e.Stop()
	for i := 0; i < 16; i++ {
		e.SubscribePattern(SubscriberID(1000+i), fmt.Sprintf("bench:p%d*", i))
	}
	channels := make([]string, 64)
	for i := range channels {
		channels[i] = fmt.Sprintf("bench:hot%d", i)
		e.Subscribe(SubscriberID(i), channels[i])
		e.Publish(channels[i], []byte("warm"), 0, func(SubscriberID, Delivery) bool { return true })
	}

	data := []byte("benchmark message payload")
	deliver := func(id SubscriberID, d Delivery) bool {
		return true
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.Publish(channels[i&63], data, 0, deliver)
			i++
		}
	})
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
				e.Publish("bench-channel", []byte("msg"), id, func(SubscriberID, Delivery) bool {
					return true
				})
			}
		}
	})
}

func BenchmarkChannelSubscribe(b *testing.B) {
	ch := NewChannel("bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Subscribe(SubscriberID(i))
	}
}

func BenchmarkChannelUnsubscribe(b *testing.B) {
	ch := NewChannel("bench")
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

func BenchmarkPublishParallelFanout(b *testing.B) {
	for _, numSubs := range []int{1000, 5000, 10000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			e := New(WithFanoutThreshold(500), WithFanoutWorkers(4))
			b.Cleanup(e.Stop)
			for i := 0; i < numSubs; i++ {
				e.Subscribe(SubscriberID(i), "bench-channel")
			}

			data := []byte("benchmark message payload")
			deliver := func(id SubscriberID, d Delivery) bool {
				return true
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.Publish("bench-channel", data, 0, deliver)
			}
		})
	}
}

func BenchmarkPublishSequentialVsParallel(b *testing.B) {
	numSubs := 5000
	data := []byte("benchmark message payload")
	deliver := func(id SubscriberID, d Delivery) bool {
		return true
	}

	b.Run("sequential", func(b *testing.B) {
		e := New(WithFanoutThreshold(numSubs + 1)) // force sequential
		for i := 0; i < numSubs; i++ {
			e.Subscribe(SubscriberID(i), "bench-channel")
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e.Publish("bench-channel", data, 0, deliver)
		}
	})

	for _, workers := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("parallel/workers=%d", workers), func(b *testing.B) {
			e := New(WithFanoutThreshold(0), WithFanoutWorkers(workers))
			b.Cleanup(e.Stop)
			for i := 0; i < numSubs; i++ {
				e.Subscribe(SubscriberID(i), "bench-channel")
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.Publish("bench-channel", data, 0, deliver)
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

func BenchmarkPublishRoundRobin(b *testing.B) {
	for _, subs := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			e := New(WithNamespaces(mustRegistry(namespace.NewRegistry(namespace.Config{
				Default: namespace.Settings{AllowPublish: true, Fanout: namespace.RoundRobin},
			}))))
			defer e.Stop()

			for i := 1; i <= subs; i++ {
				e.Subscribe(SubscriberID(i), "rr-bench")
			}
			var sink atomic.Int64
			deliver := func(SubscriberID, Delivery) bool {
				sink.Add(1)
				return true
			}
			data := make([]byte, 64)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.Publish("rr-bench", data, 0, deliver)
			}
		})
	}
}
