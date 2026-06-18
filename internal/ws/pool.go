package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/mateusfdl/gentis/internal/engine"
)

var wsMsgPool = sync.Pool{
	New: func() any {
		return &ServerMessage{
			ChannelMessage: &ChannelMessagePayload{},
		}
	},
}

func getWSMsg(d engine.Delivery) *ServerMessage {
	msg := wsMsgPool.Get().(*ServerMessage)
	msg.ChannelMessage.Channel = d.Channel
	msg.ChannelMessage.Data = json.RawMessage(d.Data)
	msg.ChannelMessage.Offset = d.Offset
	msg.ChannelMessage.Epoch = d.Epoch
	msg.frame = d.Frame
	return msg
}

func putWSMsg(msg *ServerMessage) {
	msg.ChannelMessage.Channel = ""
	msg.ChannelMessage.Data = nil
	msg.ChannelMessage.Offset = 0
	msg.ChannelMessage.Epoch = 0
	msg.frame = nil
	msg.enqueuedAt = time.Time{}
	wsMsgPool.Put(msg)
}
