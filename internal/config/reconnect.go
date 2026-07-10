package config

import (
	"fmt"
	"time"
)

type Reconnect struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	MaxRetries int
}

func (rc Reconnect) validate() error {
	if rc.Initial < 0 {
		return fmt.Errorf("%w: relay.reconnect.initial must be >= 0", ErrInvalid)
	}

	if rc.Max < 0 {
		return fmt.Errorf("%w: relay.reconnect.max must be >= 0", ErrInvalid)
	}

	if rc.Multiplier <= 0 {
		return fmt.Errorf("%w: relay.reconnect.multiplier must be > 0", ErrInvalid)
	}

	if rc.MaxRetries < 0 {
		return fmt.Errorf("%w: relay.reconnect.max_retries must be >= 0", ErrInvalid)
	}

	return nil
}
