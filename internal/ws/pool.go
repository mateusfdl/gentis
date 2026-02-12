package ws

import (
	"encoding/json"
	"sync"
)

var wsMsgPool = sync.Pool{
	New: func() any {
		return &ServerMessage{
			ChannelMessage: &ChannelMessagePayload{},
		}
	},
}

func getWSMsg(channel string, data []byte) *ServerMessage {
	msg := wsMsgPool.Get().(*ServerMessage)
	msg.ChannelMessage.Channel = channel
	msg.ChannelMessage.Data = json.RawMessage(data)
	return msg
}

func putWSMsg(msg *ServerMessage) {
	msg.ChannelMessage.Data = nil
	wsMsgPool.Put(msg)
}
