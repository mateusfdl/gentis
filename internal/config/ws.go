package config

import (
	"fmt"
	"time"
)

type WebSocket struct {
	Addr         string
	ReadLimit    int64
	WriteTimeout time.Duration
	SendBuffer   int
}

func (w WebSocket) validate() error {
	if w.ReadLimit < 0 {
		return fmt.Errorf("%w: websocket.read_limit must be >= 0", ErrInvalid)
	}

	if w.WriteTimeout < 0 {
		return fmt.Errorf("%w: websocket.write_timeout must be >= 0", ErrInvalid)
	}

	if w.SendBuffer < 0 {
		return fmt.Errorf("%w: websocket.send_buffer must be >= 0", ErrInvalid)
	}

	return nil
}
