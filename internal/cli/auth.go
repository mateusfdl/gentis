package cli

import (
	"log/slog"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/mateusfdl/gentis/internal/config"
)

// buildVerifier constructs the token verifier from the already-validated auth
// config: the config loader guarantees exactly one of secret or disabled is set.
func buildVerifier(a config.Auth, logger *slog.Logger) auth.Verifier {
	if a.Disabled {
		logger.Warn("authentication disabled, all tokens accepted")
		return auth.InsecureVerifier{}
	}
	return auth.NewHMACVerifier([]byte(a.Secret))
}
