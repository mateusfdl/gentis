package grpc

import (
	"sync"

	gentisv1 "github.com/mateusfdl/gentis/api/gen/gentis/v1"
	"github.com/mateusfdl/gentis/internal/engine"
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

func getServerMsg(d engine.Delivery) *gentisv1.ServerMessage {
	msg := serverMsgPool.Get().(*gentisv1.ServerMessage)
	cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
	cm.ChannelMessage.Channel = d.Channel
	cm.ChannelMessage.Data = d.Data
	cm.ChannelMessage.Offset = d.Offset
	cm.ChannelMessage.Epoch = d.Epoch
	return msg
}

func putServerMsg(msg *gentisv1.ServerMessage) {
	cm := msg.Message.(*gentisv1.ServerMessage_ChannelMessage)
	cm.ChannelMessage.Channel = ""
	cm.ChannelMessage.Data = nil
	cm.ChannelMessage.Offset = 0
	cm.ChannelMessage.Epoch = 0
	serverMsgPool.Put(msg)
}

func putServerMsgIfPooled(msg *gentisv1.ServerMessage) {
	if _, ok := msg.Message.(*gentisv1.ServerMessage_ChannelMessage); ok {
		putServerMsg(msg)
	}
}

var batchMsgPool = sync.Pool{
	New: func() any {
		return &gentisv1.ServerMessage{
			Message: &gentisv1.ServerMessage_Batch{
				Batch: &gentisv1.BatchMessage{},
			},
		}
	},
}

func getBatchMsg() *gentisv1.ServerMessage {
	return batchMsgPool.Get().(*gentisv1.ServerMessage)
}

func putBatchMsg(msg *gentisv1.ServerMessage) {
	batch := msg.GetBatch()
	for i := range batch.Messages {
		batch.Messages[i] = nil
	}
	batch.Messages = batch.Messages[:0]
	batchMsgPool.Put(msg)
}
