package ws

import (
	"testing"

	"github.com/mateusfdl/gentis/internal/engine"
)

func benchFanout(b *testing.B, subscribers int, shared bool) {
	b.Helper()
	data := []byte(`{"event":"tick","seq":1234567,"payload":"some representative body"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d := engine.Delivery{Channel: "room", Data: data, Offset: 1, Epoch: 7}
		if shared {
			d.Frame = &engine.EncodedFrame{}
		}
		for range subscribers {
			m := getWSMsg(d)
			if _, err := messageBytes(m); err != nil {
				b.Fatal(err)
			}
			putWSMsg(m)
		}
	}
}

func BenchmarkFanoutEncodePerSubscriber(b *testing.B) { benchFanout(b, 1600, false) }
func BenchmarkFanoutEncodeShared(b *testing.B)        { benchFanout(b, 1600, true) }
