package cli

import (
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

func newHealthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check if a Gentis server is healthy",
		RunE:  runHealth,
	}
	cmd.Flags().String("addr", "http://localhost:8080", "health endpoint base URL")
	cmd.Flags().Duration("timeout", 5*time.Second, "HTTP request timeout")
	return cmd
}

func runHealth(cmd *cobra.Command, _ []string) error {
	addr, err := cmd.Flags().GetString("addr")
	if err != nil {
		return err
	}
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(addr + "/health")
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned status %d", resp.StatusCode)
	}

	fmt.Println("ok")
	return nil
}
