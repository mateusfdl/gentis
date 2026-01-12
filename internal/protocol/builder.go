package protocol

import (
	"encoding/binary"
)

type Builder struct{}

func NewBuilder() *Builder {
	return &Builder{}
}

// CONNECT message : [token_len (1)][token (variable)]
func (b *Builder) BuildConnect(authToken string) ([]byte, error) {
	if len(authToken) > MaxAuthTokenLength {
		return nil, ErrDataTooLarge
	}

	payload := make([]byte, 1+len(authToken))
	payload[0] = byte(len(authToken))
	copy(payload[1:], authToken)

	return b.buildMessage(TypeConnect, payload)
}

// BuildConnected -> CONNECTED: [conn_id_len (1)][conn_id (variable)]
func (b *Builder) BuildConnected(connectionID string) ([]byte, error) {
	return b.buildStringMessage(TypeConnected, connectionID)
}

// BuildSubscribe -> SUBSCRIBE: [channel_len (1)][channel (variable)]
func (b *Builder) BuildSubscribe(channel string) ([]byte, error) {
	return b.buildStringMessage(TypeSubscribe, channel)
}

// BuildSubscribed -> SUBSCRIBED: [channel_len (1)][channel (variable)]
func (b *Builder) BuildSubscribed(channel string) ([]byte, error) {
	return b.buildStringMessage(TypeSubscribed, channel)
}

// BuildUnsubscribe -> UNSUBSCRIBE: [channel_len (1)][channel (variable)]
func (b *Builder) BuildUnsubscribe(channel string) ([]byte, error) {
	return b.buildStringMessage(TypeUnsubscribe, channel)
}

// BuildUnsubscribed -> UNSUBSCRIBED: [channel_len (1)][channel (variable)]
func (b *Builder) BuildUnsubscribed(channel string) ([]byte, error) {
	return b.buildStringMessage(TypeUnsubscribed, channel)
}

// BuildPublish -> PUBLISH: [channel_len (1)][channel (variable)][data_len (2)][data (variable)]
func (b *Builder) BuildPublish(channel string, data []byte) ([]byte, error) {
	if len(channel) > MaxChannelLength {
		return nil, ErrChannelTooLong
	}
	if len(data) > 0xFFFF {
		return nil, ErrDataTooLarge
	}

	payload := make([]byte, 1+len(channel)+2+len(data))
	payload[0] = byte(len(channel))
	copy(payload[1:], channel)
	binary.BigEndian.PutUint16(payload[1+len(channel):], uint16(len(data)))
	copy(payload[1+len(channel)+2:], data)

	return b.buildMessage(TypePublish, payload)
}

// BuildMessage creates a MESSAGE (channel message delivery).
// Format: [channel_len (1)][channel (variable)][data_len (2)][data (variable)]
func (b *Builder) BuildMessage(channel string, data []byte) ([]byte, error) {
	if len(channel) > MaxChannelLength {
		return nil, ErrChannelTooLong
	}
	if len(data) > 0xFFFF {
		return nil, ErrDataTooLarge
	}

	payload := make([]byte, 1+len(channel)+2+len(data))
	payload[0] = byte(len(channel))
	copy(payload[1:], channel)
	binary.BigEndian.PutUint16(payload[1+len(channel):], uint16(len(data)))
	copy(payload[1+len(channel)+2:], data)

	return b.buildMessage(TypeMessage, payload)
}

// BuildError creates an ERROR message: [error_code (1)][msg_len (2)][msg (variable)]
func (b *Builder) BuildError(code uint8, message string) ([]byte, error) {
	if len(message) > MaxErrorMsgLength {
		return nil, ErrDataTooLarge
	}

	payload := make([]byte, 1+2+len(message))
	payload[0] = code
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(message)))
	copy(payload[3:], message)

	return b.buildMessage(TypeError, payload)
}

func (b *Builder) BuildPing() ([]byte, error) {
	return b.buildMessage(TypePing, nil)
}

func (b *Builder) BuildPong() ([]byte, error) {
	return b.buildMessage(TypePong, nil)
}

func (b *Builder) buildStringMessage(msgType MessageType, str string) ([]byte, error) {
	if len(str) > MaxChannelLength {
		return nil, ErrChannelTooLong
	}

	payload := make([]byte, 1+len(str))
	payload[0] = byte(len(str))
	copy(payload[1:], str)

	return b.buildMessage(msgType, payload)
}

func (b *Builder) buildMessage(msgType MessageType, payload []byte) ([]byte, error) {
	if len(payload) > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}

	msg := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint16(msg[0:2], uint16(msgType))
	binary.BigEndian.PutUint16(msg[2:4], uint16(len(payload)))
	copy(msg[4:], payload)

	return msg, nil
}
