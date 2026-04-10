package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version and build information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("gentis %s (commit: %s)\n", buildVersion, buildCommit)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
