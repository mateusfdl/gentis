package config

import (
	"fmt"
	"time"
)

type Engine struct {
	Shards          int
	FanoutThreshold int
	FanoutWorkers   int
	HistorySize     int
	HistoryTTL      time.Duration
}

func (e Engine) validate() error {
	if e.Shards < 0 {
		return fmt.Errorf("%w: engine.shards must be >= 0", ErrInvalid)
	}

	if e.FanoutThreshold < 0 {
		return fmt.Errorf("%w: engine.fanout_threshold must be >= 0", ErrInvalid)
	}

	if e.FanoutWorkers <= 0 {
		return fmt.Errorf("%w: engine.fanout_workers must be > 0", ErrInvalid)
	}

	if e.HistorySize < 0 {
		return fmt.Errorf("%w: engine.history_size must be >= 0", ErrInvalid)
	}

	if e.HistoryTTL < 0 {
		return fmt.Errorf("%w: engine.history_ttl must be >= 0", ErrInvalid)
	}

	if e.HistoryTTL > 0 && e.HistorySize <= 0 {
		return fmt.Errorf("%w: engine.history_ttl requires engine.history_size > 0", ErrInvalid)
	}

	return nil
}
