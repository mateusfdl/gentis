package cli

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/mateusfdl/gentis/internal/auth"
	"github.com/spf13/cobra"
)

func newAuthFlagCmd(args ...string) *cobra.Command {
	cmd := &cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { return nil }}
	addAuthFlags(cmd)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
	return cmd
}

func TestBuildVerifier(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantType any
		wantErr  error
	}{
		{
			name:     "secret yields hmac verifier",
			args:     []string{"--auth-hmac-secret", "s3cret"},
			wantType: &auth.HMACVerifier{},
		},
		{
			name:     "disabled yields insecure verifier",
			args:     []string{"--auth-disabled"},
			wantType: auth.InsecureVerifier{},
		},
		{
			name:    "no choice is an error",
			args:    []string{},
			wantErr: errAuthNotConfigured,
		},
		{
			name:    "both flags is an error",
			args:    []string{"--auth-hmac-secret", "s3cret", "--auth-disabled"},
			wantErr: errAuthConflict,
		},
	}

	logger := slog.New(slog.DiscardHandler)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newAuthFlagCmd(tt.args...)
			v, err := buildVerifier(cmd, logger)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("buildVerifier() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildVerifier() error = %v, want nil", err)
			}
			switch tt.wantType.(type) {
			case *auth.HMACVerifier:
				if _, ok := v.(*auth.HMACVerifier); !ok {
					t.Fatalf("verifier = %T, want *auth.HMACVerifier", v)
				}
			case auth.InsecureVerifier:
				if _, ok := v.(auth.InsecureVerifier); !ok {
					t.Fatalf("verifier = %T, want auth.InsecureVerifier", v)
				}
			}
		})
	}
}
