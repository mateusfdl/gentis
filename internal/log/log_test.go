package log

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNew_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Level: slog.LevelInfo, Format: FormatText, Output: &buf})

	logger.Info("hello", "key", "value")

	out := buf.String()
	if !strings.Contains(out, "hello") {
		t.Errorf("expected output to contain 'hello', got: %s", out)
	}
	if !strings.Contains(out, "key=value") {
		t.Errorf("expected output to contain 'key=value', got: %s", out)
	}
}

func TestNew_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Level: slog.LevelInfo, Format: FormatJSON, Output: &buf})

	logger.Info("hello", "key", "value")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("expected valid JSON, got error: %v, output: %s", err, buf.String())
	}
	if m["msg"] != "hello" {
		t.Errorf("expected msg='hello', got: %v", m["msg"])
	}
	if m["key"] != "value" {
		t.Errorf("expected key='value', got: %v", m["key"])
	}
}

func TestNew_LevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Level: slog.LevelInfo, Format: FormatText, Output: &buf})

	logger.Debug("should not appear")
	if buf.Len() != 0 {
		t.Errorf("expected no output for debug at info level, got: %s", buf.String())
	}

	logger.Info("should appear")
	if buf.Len() == 0 {
		t.Error("expected output for info at info level")
	}
}

func TestNop(t *testing.T) {
	logger := Nop()

	// Should not panic and should produce no output
	logger.Info("should be discarded")
	logger.Error("also discarded")
	logger.Debug("also discarded")
}

func TestNop_Enabled(t *testing.T) {
	logger := Nop()
	if logger.Enabled(t.Context(), slog.LevelError) {
		t.Error("Nop logger should never be enabled")
	}
}

func TestNew_WithAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Level: slog.LevelInfo, Format: FormatText, Output: &buf})

	child := logger.With("component", "grpc")
	child.Info("started")

	out := buf.String()
	if !strings.Contains(out, "component=grpc") {
		t.Errorf("expected output to contain 'component=grpc', got: %s", out)
	}
	if !strings.Contains(out, "started") {
		t.Errorf("expected output to contain 'started', got: %s", out)
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  slog.Level
		err   bool
	}{
		{"debug", slog.LevelDebug, false},
		{"DEBUG", slog.LevelDebug, false},
		{"info", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, false},
		{"warning", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"invalid", slog.LevelInfo, true},
	}

	for _, tt := range tests {
		got, err := ParseLevel(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("ParseLevel(%q): error = %v, want error = %v", tt.input, err, tt.err)
		}
		if got != tt.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input string
		want  Format
		err   bool
	}{
		{"text", FormatText, false},
		{"TEXT", FormatText, false},
		{"json", FormatJSON, false},
		{"JSON", FormatJSON, false},
		{"invalid", FormatText, true},
	}

	for _, tt := range tests {
		got, err := ParseFormat(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("ParseFormat(%q): error = %v, want error = %v", tt.input, err, tt.err)
		}
		if got != tt.want {
			t.Errorf("ParseFormat(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
