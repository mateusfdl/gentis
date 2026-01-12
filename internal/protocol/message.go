// Package protocol implements the binary message protocol for Gentis.
//
// Message format (Big-Endian):
// ┌─────────────┬─────────────┬─────────────────────────┐
// │ Type ID (2) │ Length (2)  │ Payload (variable)      │
// └─────────────┴─────────────┴─────────────────────────┘
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type MessageType uint16

const (
	TypeConnect      MessageType = 1
	TypeConnected    MessageType = 2
	TypeSubscribe    MessageType = 3
	TypeSubscribed   MessageType = 4
	TypeUnsubscribe  MessageType = 5
	TypeUnsubscribed MessageType = 6
	TypePublish      MessageType = 7
	TypeMessage      MessageType = 8
	TypeError        MessageType = 9
	TypePing         MessageType = 10
	TypePong         MessageType = 11
)

const (
	MaxMessageSize     = 65535 // Maximum payload size (uint16 max)
	MaxChannelLength   = 256
	MaxErrorMsgLength  = 1024
	MaxAuthTokenLength = 255
	HeaderSize         = 4 // 2 bytes type + 2 bytes length
)

var (
	ErrMessageTooLarge    = errors.New("message exceeds maximum size")
	ErrInvalidMessageType = errors.New("invalid message type")
	ErrIncompleteMessage  = errors.New("incomplete message")
	ErrChannelTooLong     = errors.New("channel name too long")
	ErrDataTooLarge       = errors.New("data payload too large")
	ErrInvalidPayload     = errors.New("invalid payload format")
)

type Message struct {
	Type    MessageType
	Payload []byte
}

func (mt MessageType) String() string {
	switch mt {
	case TypeConnect:
		return "CONNECT"
	case TypeConnected:
		return "CONNECTED"
	case TypeSubscribe:
		return "SUBSCRIBE"
	case TypeSubscribed:
		return "SUBSCRIBED"
	case TypeUnsubscribe:
		return "UNSUBSCRIBE"
	case TypeUnsubscribed:
		return "UNSUBSCRIBED"
	case TypePublish:
		return "PUBLISH"
	case TypeMessage:
		return "MESSAGE"
	case TypeError:
		return "ERROR"
	case TypePing:
		return "PING"
	case TypePong:
		return "PONG"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", mt)
	}
}

// ParseConnect extracts the auth token from a <-CONNECT : [token_len (1)][token (variable)]
func ParseConnect(payload []byte) (string, error) {
	if len(payload) < 1 {
		return "", ErrInvalidPayload
	}
	tokenLen := int(payload[0])
	if len(payload) < 1+tokenLen {
		return "", ErrInvalidPayload
	}
	return string(payload[1 : 1+tokenLen]), nil
}

// ParseSubscribe extracts the channel name from a <-SUBSCRIBE: [channel_len (1)][channel (variable)]
func ParseSubscribe(payload []byte) (string, error) {
	return parseStringPayload(payload)
}

// ParseUnsubscribe extracts the channel name from an <-UNSUBSCRIBE: [channel_len (1)][channel (variable)]
func ParseUnsubscribe(payload []byte) (string, error) {
	return parseStringPayload(payload)
}

// PublishData represents the parsed data from a PUBLISH message.
type PublishData struct {
	Channel string
	Data    []byte
}

// ParsePublish extracts channel and data from a PUBLISH message payload.
// Format: [channel_len (1)][channel (variable)][data_len (2)][data (variable)]
func ParsePublish(payload []byte) (*PublishData, error) {
	if len(payload) < 1 {
		return nil, ErrInvalidPayload
	}

	channelLen := int(payload[0])
	if len(payload) < 1+channelLen+2 {
		return nil, ErrInvalidPayload
	}

	channel := string(payload[1 : 1+channelLen])
	dataLen := binary.BigEndian.Uint16(payload[1+channelLen : 1+channelLen+2])

	if len(payload) < 1+channelLen+2+int(dataLen) {
		return nil, ErrInvalidPayload
	}

	data := payload[1+channelLen+2 : 1+channelLen+2+int(dataLen)]

	return &PublishData{
		Channel: channel,
		Data:    data,
	}, nil
}

type ErrorData struct {
	Code    uint8
	Message string
}

// ParseError extracts error code and message from an ERROR message payload.
// Format: [error_code (1)][msg_len (2)][msg (variable)]
func ParseError(payload []byte) (*ErrorData, error) {
	if len(payload) < 3 {
		return nil, ErrInvalidPayload
	}

	code := payload[0]
	msgLen := binary.BigEndian.Uint16(payload[1:3])

	if len(payload) < 3+int(msgLen) {
		return nil, ErrInvalidPayload
	}

	msg := string(payload[3 : 3+msgLen])

	return &ErrorData{
		Code:    code,
		Message: msg,
	}, nil
}

func parseStringPayload(payload []byte) (string, error) {
	if len(payload) < 1 {
		return "", ErrInvalidPayload
	}
	strLen := int(payload[0])
	if len(payload) < 1+strLen {
		return "", ErrInvalidPayload
	}
	return string(payload[1 : 1+strLen]), nil
}

// ReadMessage reads a complete message from a reader.
// This is a blocking read that waits for a complete message.
func ReadMessage(r io.Reader) (*Message, error) {
	// header (4 bytes)
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	msgType := MessageType(binary.BigEndian.Uint16(header[0:2]))
	payloadLen := binary.BigEndian.Uint16(header[2:4])

	if payloadLen > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &Message{
		Type:    msgType,
		Payload: payload,
	}, nil
}

func WriteMessage(w io.Writer, msgType MessageType, payload []byte) error {
	if len(payload) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint16(header[0:2], uint16(msgType))
	binary.BigEndian.PutUint16(header[2:4], uint16(len(payload)))

	if _, err := w.Write(header); err != nil {
		return err
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	return nil
}
