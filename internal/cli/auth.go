package cli

import (
	"errors"
	"log/slog"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/spf13/cobra"
)

var (
	errAuthNotConfigured = errors.New("authentication not configured: set --auth-hmac-secret or explicitly disable with --auth-disabled")
	errAuthConflict      = errors.New("--auth-hmac-secret and --auth-disabled are mutually exclusive")
	errTLSIncomplete     = errors.New("--tls-cert and --tls-key must be set together")
)

func addAuthFlags(cmd *cobra.Command) {
	cmd.Flags().String("auth-hmac-secret", "", "HS256 secret for verifying client JWTs")
	cmd.Flags().Bool("auth-disabled", false, "accept any token without verification (dev only)")
}

func buildVerifier(cmd *cobra.Command, logger *slog.Logger) (auth.Verifier, error) {
	secret, _ := cmd.Flags().GetString("auth-hmac-secret")
	disabled, _ := cmd.Flags().GetBool("auth-disabled")

	switch {
	case disabled && secret != "":
		return nil, errAuthConflict
	case disabled:
		logger.Warn("authentication disabled, all tokens accepted")
		return auth.InsecureVerifier{}, nil
	case secret == "":
		return nil, errAuthNotConfigured
	default:
		return auth.NewHMACVerifier([]byte(secret)), nil
	}
}
