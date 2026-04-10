package cli

import (
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check if a Gentis server is healthy",
	RunE:  runHealth,
}

func init() {
	healthCmd.Flags().String("addr", "http://localhost:8080", "health endpoint base URL")
	healthCmd.Flags().Duration("timeout", 5*time.Second, "HTTP request timeout")
	rootCmd.AddCommand(healthCmd)
}

func runHealth(cmd *cobra.Command, args []string) error {
	addr, _ := cmd.Flags().GetString("addr")
	timeout, _ := cmd.Flags().GetDuration("timeout")

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
