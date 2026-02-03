package engine

import "sync"

var subscriberSlicePool = sync.Pool{
	New: func() any {
		s := make([]SubscriberID, 0, 128)
		return &s
	},
}

func AcquireSubscriberSlice() *[]SubscriberID {
	return subscriberSlicePool.Get().(*[]SubscriberID)
}

func ReleaseSubscriberSlice(s *[]SubscriberID) {
	if s == nil {
		return
	}
	if cap(*s) > 4096 {
		return
	}
	*s = (*s)[:0]
	subscriberSlicePool.Put(s)
}

type MessageEnvelope struct {
	Channel string
	Data    []byte
}

var envelopePool = sync.Pool{
	New: func() any {
		return &MessageEnvelope{}
	},
}

func AcquireEnvelope(channel string, data []byte) *MessageEnvelope {
	e := envelopePool.Get().(*MessageEnvelope)
	e.Channel = channel
	e.Data = data
	return e
}

func ReleaseEnvelope(e *MessageEnvelope) {
	if e == nil {
		return
	}
	e.Channel = ""
	e.Data = nil
	envelopePool.Put(e)
}
