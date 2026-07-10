package config

import (
	"log/slog"

	"github.com/mateusfdl/gentis/internal/logs"
)

type Log struct {
	Level  slog.Level
	Format logs.Format
}
