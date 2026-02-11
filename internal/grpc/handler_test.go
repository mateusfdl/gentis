package grpc

import (
	"strings"
	"testing"
)

func TestValidateChannel(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		valid   bool
	}{
		{"valid short name", "ch", true},
		{"valid single char", "a", true},
		{"valid max length", strings.Repeat("a", maxChannelNameLen), true},
		{"empty string", "", false},
		{"too long", strings.Repeat("a", maxChannelNameLen+1), false},
		{"with special chars", "chat-room_1.public", true},
		{"with spaces", "chat room", true},
		{"unicode", "канал", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateChannel(tt.channel)
			if got != tt.valid {
				t.Errorf("validateChannel(%q) = %v, want %v", tt.channel, got, tt.valid)
			}
		})
	}
}
