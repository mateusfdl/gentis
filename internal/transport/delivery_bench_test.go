package transport_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
	"github.com/mateusfdl/gentis/internal/transport"
)

type benchSender struct {
	sendCh chan *gentisv1.ServerMessage
}

func (s *benchSender) DeliverMessage(channel string, data []byte) bool {
	msg := &gentisv1.ServerMessage{
		Message: &gentisv1.ServerMessage_ChannelMessage{
			ChannelMessage: &gentisv1.ChannelMessage{
				Channel: channel,
				Data:    data,
			},
		},
	}
	select {
	case s.sendCh <- msg:
		return true
	default:
		return false
	}
}

func setupDeliveryBench(b *testing.B, numSubs int) (*engine.Engine, *transport.SessionStore, context.CancelFunc) {
	b.Helper()

	eng := engine.New()
	store := transport.NewSessionStore()
	ctx, cancel := context.WithCancel(context.Background())

	for i := 1; i <= numSubs; i++ {
		id := engine.SubscriberID(i)
		sender := &benchSender{sendCh: make(chan *gentisv1.ServerMessage, 256)}
		store.Register(id, sender)
		eng.Subscribe(id, "bench-channel")

		go func(ch <-chan *gentisv1.ServerMessage) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
			}
		}(sender.sendCh)
	}

	return eng, store, cancel
}

func BenchmarkDelivery(b *testing.B) {
	for _, numSubs := range []int{10, 100, 500, 1000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			eng, store, cancel := setupDeliveryBench(b, numSubs)
			defer cancel()

			data := []byte(`{"msg":"benchmark payload data"}`)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eng.Publish("bench-channel", data, 0, store.Deliver)
			}
		})
	}
}

func BenchmarkDeliveryParallel(b *testing.B) {
	for _, numSubs := range []int{100, 500} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			eng, store, cancel := setupDeliveryBench(b, numSubs)
			defer cancel()

			data := []byte(`{"msg":"benchmark payload data"}`)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					eng.Publish("bench-channel", data, 0, store.Deliver)
				}
			})
		})
	}
}

// --- Pooled variants ---

var benchMsgPool = sync.Pool{
	New: func() any {
		return &gentisv1.ServerMessage{
			Message: &gentisv1.ServerMessage_ChannelMessage{
				ChannelMessage: &gentisv1.ChannelMessage{},
			},
		}
	},
}

type pooledBenchSender struct {
	sendCh chan *gentisv1.ServerMessage
}

func (s *pooledBenchSender) DeliverMessage(channel string, data []byte) bool {
	msg := benchMsgPool.Get().(*gentisv1.ServerMessage)
	cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
	cm.ChannelMessage.Channel = channel
	cm.ChannelMessage.Data = data
	select {
	case s.sendCh <- msg:
		return true
	default:
		cm.ChannelMessage.Data = nil
		benchMsgPool.Put(msg)
		return false
	}
}

func setupPooledDeliveryBench(b *testing.B, numSubs int) (*engine.Engine, *transport.SessionStore, context.CancelFunc) {
	b.Helper()

	eng := engine.New()
	store := transport.NewSessionStore()
	ctx, cancel := context.WithCancel(context.Background())

	for i := 1; i <= numSubs; i++ {
		id := engine.SubscriberID(i)
		sender := &pooledBenchSender{sendCh: make(chan *gentisv1.ServerMessage, 256)}
		store.Register(id, sender)
		eng.Subscribe(id, "bench-channel")

		go func(ch <-chan *gentisv1.ServerMessage) {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-ch:
					cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
					cm.ChannelMessage.Data = nil
					benchMsgPool.Put(msg)
				}
			}
		}(sender.sendCh)
	}

	return eng, store, cancel
}

func BenchmarkDeliveryPooled(b *testing.B) {
	for _, numSubs := range []int{10, 100, 500, 1000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			eng, store, cancel := setupPooledDeliveryBench(b, numSubs)
			defer cancel()

			data := []byte(`{"msg":"benchmark payload data"}`)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eng.Publish("bench-channel", data, 0, store.Deliver)
			}
		})
	}
}

func BenchmarkDeliveryPooledParallel(b *testing.B) {
	for _, numSubs := range []int{100, 500} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			eng, store, cancel := setupPooledDeliveryBench(b, numSubs)
			defer cancel()

			data := []byte(`{"msg":"benchmark payload data"}`)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					eng.Publish("bench-channel", data, 0, store.Deliver)
				}
			})
		})
	}
}

// --- Parallel fanout delivery benchmarks ---

func setupPooledFanoutBench(b *testing.B, numSubs int, threshold int, workers int) (*engine.Engine, *transport.SessionStore, context.CancelFunc) {
	b.Helper()

	eng := engine.New(engine.WithFanoutThreshold(threshold), engine.WithFanoutWorkers(workers))
	b.Cleanup(eng.Stop)
	store := transport.NewSessionStore()
	ctx, cancel := context.WithCancel(context.Background())

	for i := 1; i <= numSubs; i++ {
		id := engine.SubscriberID(i)
		sender := &pooledBenchSender{sendCh: make(chan *gentisv1.ServerMessage, 256)}
		store.Register(id, sender)
		eng.Subscribe(id, "bench-channel")

		go func(ch <-chan *gentisv1.ServerMessage) {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-ch:
					cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
					cm.ChannelMessage.Data = nil
					benchMsgPool.Put(msg)
				}
			}
		}(sender.sendCh)
	}

	return eng, store, cancel
}

func BenchmarkDeliveryPooledFanout(b *testing.B) {
	numSubs := 5000
	data := []byte(`{"msg":"benchmark payload data"}`)

	b.Run("sequential", func(b *testing.B) {
		eng, store, cancel := setupPooledFanoutBench(b, numSubs, numSubs+1, 1)
		defer cancel()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			eng.Publish("bench-channel", data, 0, store.Deliver)
		}
	})

	for _, workers := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("parallel/workers=%d", workers), func(b *testing.B) {
			eng, store, cancel := setupPooledFanoutBench(b, numSubs, 0, workers)
			defer cancel()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eng.Publish("bench-channel", data, 0, store.Deliver)
			}
		})
	}
}
