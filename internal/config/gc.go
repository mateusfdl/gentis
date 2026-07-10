package config

import "fmt"

type GC struct {
	Pacer      bool
	MemLimit   int64
	SpikeGOGC  int
	NormalGOGC int
}

func (g GC) validate() error {
	if g.MemLimit < 0 {
		return fmt.Errorf("%w: gc.mem_limit must be >= 0", ErrInvalid)
	}

	if g.SpikeGOGC <= 0 {
		return fmt.Errorf("%w: gc.spike_gogc must be > 0", ErrInvalid)
	}

	if g.NormalGOGC <= 0 {
		return fmt.Errorf("%w: gc.normal_gogc must be > 0", ErrInvalid)
	}

	return nil
}
