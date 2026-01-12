package protocol

import (
	"bytes"
	"testing"
)

func TestBuildAndParseConnect(t *testing.T) {
	builder := NewBuilder()

	token := "my-auth-token"
	msg, err := builder.BuildConnect(token)
	if err != nil {
		t.Fatalf("BuildConnect failed: %v", err)
	}

	// Parse the message
	reader := bytes.NewReader(msg[HeaderSize:])
	parsedToken, err := ParseConnect(msg[HeaderSize:])
	if err != nil {
		t.Fatalf("ParseConnect failed: %v", err)
	}

	if parsedToken != token {
		t.Errorf("Expected token %q, got %q", token, parsedToken)
	}

	_ = reader
}

func TestBuildAndParseSubscribe(t *testing.T) {
	builder := NewBuilder()

	channel := "test-channel"
	msg, err := builder.BuildSubscribe(channel)
	if err != nil {
		t.Fatalf("BuildSubscribe failed: %v", err)
	}

	parsedChannel, err := ParseSubscribe(msg[HeaderSize:])
	if err != nil {
		t.Fatalf("ParseSubscribe failed: %v", err)
	}

	if parsedChannel != channel {
		t.Errorf("Expected channel %q, got %q", channel, parsedChannel)
	}
}

func TestBuildAndParsePublish(t *testing.T) {
	builder := NewBuilder()

	channel := "test-channel"
	data := []byte("Hello, World!")

	msg, err := builder.BuildPublish(channel, data)
	if err != nil {
		t.Fatalf("BuildPublish failed: %v", err)
	}

	pub, err := ParsePublish(msg[HeaderSize:])
	if err != nil {
		t.Fatalf("ParsePublish failed: %v", err)
	}

	if pub.Channel != channel {
		t.Errorf("Expected channel %q, got %q", channel, pub.Channel)
	}

	if !bytes.Equal(pub.Data, data) {
		t.Errorf("Expected data %q, got %q", data, pub.Data)
	}
}

func TestBuildAndParseError(t *testing.T) {
	builder := NewBuilder()

	code := uint8(42)
	message := "Something went wrong"

	msg, err := builder.BuildError(code, message)
	if err != nil {
		t.Fatalf("BuildError failed: %v", err)
	}

	errData, err := ParseError(msg[HeaderSize:])
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if errData.Code != code {
		t.Errorf("Expected code %d, got %d", code, errData.Code)
	}

	if errData.Message != message {
		t.Errorf("Expected message %q, got %q", message, errData.Message)
	}
}

func TestMessageTypeString(t *testing.T) {
	tests := []struct {
		msgType  MessageType
		expected string
	}{
		{TypeConnect, "CONNECT"},
		{TypeConnected, "CONNECTED"},
		{TypeSubscribe, "SUBSCRIBE"},
		{TypePublish, "PUBLISH"},
		{TypeMessage, "MESSAGE"},
		{TypeError, "ERROR"},
		{TypePing, "PING"},
		{TypePong, "PONG"},
	}

	for _, tt := range tests {
		if got := tt.msgType.String(); got != tt.expected {
			t.Errorf("MessageType(%d).String() = %q, want %q", tt.msgType, got, tt.expected)
		}
	}
}

func TestReadWriteMessage(t *testing.T) {
	testCases := []struct {
		name    string
		msgType MessageType
		payload []byte
	}{
		{"Ping", TypePing, nil},
		{"Pong", TypePong, nil},
		{"Connect", TypeConnect, []byte{5, 't', 'o', 'k', 'e', 'n'}},
		{"Subscribe", TypeSubscribe, []byte{4, 'c', 'h', 'a', 't'}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Write message
			err := WriteMessage(&buf, tc.msgType, tc.payload)
			if err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			// Read message
			msg, err := ReadMessage(&buf)
			if err != nil {
				t.Fatalf("ReadMessage failed: %v", err)
			}

			if msg.Type != tc.msgType {
				t.Errorf("Expected type %v, got %v", tc.msgType, msg.Type)
			}

			if !bytes.Equal(msg.Payload, tc.payload) {
				t.Errorf("Expected payload %v, got %v", tc.payload, msg.Payload)
			}
		})
	}
}
