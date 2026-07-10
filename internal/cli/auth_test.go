package cli

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/mateusfdl/gentis/internal/config"
)

func TestBuildVerifier(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	secret := buildVerifier(config.Auth{Secret: "s3cret"}, logger)
	if got := fmt.Sprintf("%T", secret); got != "*auth.HMACVerifier" {
		t.Fatalf("verifier = %s, want *auth.HMACVerifier", got)
	}

	disabled := buildVerifier(config.Auth{Disabled: true}, logger)
	if got := fmt.Sprintf("%T", disabled); got != "auth.InsecureVerifier" {
		t.Fatalf("verifier = %s, want auth.InsecureVerifier", got)
	}
}
