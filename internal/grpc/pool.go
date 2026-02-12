package grpc

import (
	"sync"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
)

var serverMsgPool = sync.Pool{
	New: func() any {
		return &gentisv1.ServerMessage{
			Message: &gentisv1.ServerMessage_ChannelMessage{
				ChannelMessage: &gentisv1.ChannelMessage{},
			},
		}
	},
}

func getServerMsg(channel string, data []byte) *gentisv1.ServerMessage {
	msg := serverMsgPool.Get().(*gentisv1.ServerMessage)
	cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
	cm.ChannelMessage.Channel = channel
	cm.ChannelMessage.Data = data
	return msg
}

func putServerMsg(msg *gentisv1.ServerMessage) {
	cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
	cm.ChannelMessage.Data = nil
	serverMsgPool.Put(msg)
}
